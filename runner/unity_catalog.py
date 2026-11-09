"""Unity Catalog publish from the runner (ADR-027).

Configured by environment, set by the generated orchestration.tf (Lambda)
or by `clavesa pipeline run` (local compute against a cloud warehouse):

  CLAVESA_DATABRICKS_HOST          https://dbc-....cloud.databricks.com
  CLAVESA_DATABRICKS_CLIENT_ID     service principal application ID
  CLAVESA_DATABRICKS_SECRET        secret reference (arn:aws:secretsmanager:..., env:VAR)
  CLAVESA_DATABRICKS_CLIENT_SECRET the secret itself (local runs only)
  CLAVESA_UC_CATALOG / CLAVESA_UC_SCHEMA   where outputs go
  CLAVESA_UC_PUBLISH / CLAVESA_UC_LINEAGE  "true" / "false"
  CLAVESA_DATABRICKS_JOBS          JSON list of job IDs started after success
  CLAVESA_UC_READERS               JSON list of principals granted SELECT on
                                   every table clavesa registers

Stdlib only (urllib), plus boto3 for an arn: secret, which the runner
image already has. Every REST call follows the shapes P0 established
(docs/ideas/unity-catalog-publish.md, "The exact calls").
"""

from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class UCError(RuntimeError):
    """A Unity Catalog step failed; the message names the fix."""


def _log(msg: str) -> None:
    print(f"[clavesa] unity catalog: {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def config() -> dict[str, Any] | None:
    """The publish configuration, or None when the pipeline is not connected."""
    host = os.environ.get("CLAVESA_DATABRICKS_HOST", "").rstrip("/")
    if not host:
        return None
    warehouse = os.environ.get("CLAVESA_WAREHOUSE", "")
    if warehouse and not (warehouse.startswith("s3://") or warehouse.startswith("s3a://")):
        # ADR-014 / ADR-024: a local warehouse has no Unity Catalog; its
        # files are on a laptop disk Databricks cannot read.
        _log("not published: the workspace warehouse is local (Unity Catalog needs the cloud warehouse)")
        return None
    try:
        jobs = [int(j) for j in json.loads(os.environ.get("CLAVESA_DATABRICKS_JOBS") or "[]")]
    except (ValueError, TypeError) as exc:
        raise UCError(f"CLAVESA_DATABRICKS_JOBS is not a JSON list of job IDs: {exc}") from exc
    try:
        readers = [str(r) for r in json.loads(os.environ.get("CLAVESA_UC_READERS") or "[]") if str(r)]
    except (ValueError, TypeError) as exc:
        raise UCError(f"CLAVESA_UC_READERS is not a JSON list of principals: {exc}") from exc
    return {
        "host": host,
        "client_id": os.environ.get("CLAVESA_DATABRICKS_CLIENT_ID", ""),
        "catalog": os.environ.get("CLAVESA_UC_CATALOG", ""),
        "schema": os.environ.get("CLAVESA_UC_SCHEMA", ""),
        "publish": os.environ.get("CLAVESA_UC_PUBLISH", "true") == "true",
        "lineage": os.environ.get("CLAVESA_UC_LINEAGE", "true") == "true",
        "jobs": jobs,
        "readers": readers,
    }


def _client_secret() -> str:
    direct = os.environ.get("CLAVESA_DATABRICKS_CLIENT_SECRET")
    if direct:
        return direct
    ref = os.environ.get("CLAVESA_DATABRICKS_SECRET", "")
    if ref.startswith("arn:aws:secretsmanager:"):
        import boto3  # noqa: PLC0415

        region = ref.split(":")[3]
        return boto3.client("secretsmanager", region_name=region).get_secret_value(SecretId=ref)["SecretString"].strip()
    if ref.startswith("env:"):
        val = os.environ.get(ref[4:], "")
        if val:
            return val
    raise UCError(
        "no Databricks client secret: CLAVESA_DATABRICKS_SECRET must be an arn:aws:secretsmanager: ARN "
        "(cloud) or CLAVESA_DATABRICKS_CLIENT_SECRET must be set (local)"
    )


# ---------------------------------------------------------------------------
# REST client
# ---------------------------------------------------------------------------


class Client:
    def __init__(self, cfg: dict[str, Any]):
        self.host = cfg["host"]
        self.client_id = cfg["client_id"]
        self._secret: str | None = None
        self._token: str | None = None
        self._exp = 0.0

    def token(self) -> str:
        if self._token and time.time() < self._exp - 60:
            return self._token
        if self._secret is None:
            self._secret = _client_secret()
        body = urllib.parse.urlencode({"grant_type": "client_credentials", "scope": "all-apis"}).encode()
        req = urllib.request.Request(self.host + "/oidc/v1/token", data=body, method="POST")
        basic = base64.b64encode(f"{self.client_id}:{self._secret}".encode()).decode()
        req.add_header("Authorization", "Basic " + basic)
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                tok = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            raise UCError(f"service principal sign-in failed: HTTP {exc.code} {exc.read()[:300]!r}") from exc
        self._token = tok["access_token"]
        self._exp = time.time() + int(tok.get("expires_in", 3600))
        return self._token

    def call(self, method: str, path: str, body: Any = None, ok404: bool = False) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.host + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self.token())
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            if ok404 and exc.code == 404:
                return None
            detail = exc.read()[:500].decode("utf-8", "replace")
            raise UCError(f"{method} {path}: HTTP {exc.code}: {detail}") from exc
        return json.loads(raw) if raw else {}


# ---------------------------------------------------------------------------
# Tables (slice 4)
# ---------------------------------------------------------------------------

_TYPE_NAMES = {
    "byte": "BYTE", "short": "SHORT", "integer": "INT", "long": "LONG",
    "float": "FLOAT", "double": "DOUBLE", "string": "STRING", "binary": "BINARY",
    "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP_NTZ", "array": "ARRAY", "map": "MAP",
    "struct": "STRUCT", "void": "NULL", "variant": "VARIANT",
}


def columns_from_schema(schema) -> list[dict[str, Any]]:
    """Unity Catalog column list from a Spark StructType (P0: type_json is the StructField JSON)."""
    cols = []
    for pos, field in enumerate(schema.fields):
        dt = field.dataType
        type_name_raw = dt.typeName()
        col: dict[str, Any] = {
            "name": field.name,
            "type_text": dt.simpleString(),
            "type_json": field.json(),
            "position": pos,
            "nullable": bool(field.nullable),
        }
        if type_name_raw.startswith("decimal"):
            col["type_name"] = "DECIMAL"
            col["type_precision"] = dt.precision
            col["type_scale"] = dt.scale
        else:
            col["type_name"] = _TYPE_NAMES.get(type_name_raw, type_name_raw.upper())
        cols.append(col)
    return cols


def _columns_signature(cols: list[dict[str, Any]]) -> list[tuple[str, str]]:
    return [(str(c.get("name", "")).lower(), str(c.get("type_text", "")).lower()) for c in cols]


def full_name(cfg: dict[str, Any], table_id: str, uc_table: str = "") -> str:
    """The Unity Catalog name an output writes.

    `uc_table` (the output's `unity_catalog_table`, ADR-027 section 1) wins:
    a full `<catalog>.<schema>.<table>` as given, `<schema>.<table>` in the
    connection's catalog, or `<table>` in the pipeline's schema. Without it,
    `<uc catalog>.<uc schema>.<table>` for a clavesa Glue table id
    `<db>.<table>`.
    """
    parts = [p for p in (uc_table or "").split(".") if p]
    if len(parts) == 3:
        return ".".join(parts)
    if len(parts) == 2:
        return f"{cfg['catalog']}.{parts[0]}.{parts[1]}"
    table = parts[0] if parts else table_id.rsplit(".", 1)[-1]
    return f"{cfg['catalog']}.{cfg['schema']}.{table}"


def s3_uri(location: str) -> str:
    return "s3://" + location.split("://", 1)[1] if location.startswith("s3a://") else location


def check_step_paused(client: Client, job_id: int, table: str) -> None:
    """ADR-027 provisional decision 7, now enforced: the Databricks job
    whose step this output replaces must not run any more, or two writers
    corrupt the table (S3 Delta tolerates one writer). Paused means: every
    schedule, trigger and continuous setting the job has is PAUSED, and no
    run of it is active. A job with none of the three runs only when
    something starts it, so the active-run check is what guards it."""
    job = client.call("GET", f"/api/2.1/jobs/get?job_id={int(job_id)}", ok404=True)
    if job is None:
        raise UCError(f"{table}: the replaced Databricks job {job_id} does not exist or the service principal "
                      "cannot see it (CAN_VIEW on the job)")
    settings = job.get("settings") or {}
    name = settings.get("name") or str(job_id)
    unpaused = [k for k in ("schedule", "trigger", "continuous")
                if isinstance(settings.get(k), dict) and settings[k].get("pause_status", "UNPAUSED") != "PAUSED"]
    if unpaused:
        raise UCError(
            f"{table}: Databricks job {job_id} ({name!r}) still has an unpaused {', '.join(unpaused)}. "
            f"It writes the table clavesa now writes; pause it in Databricks (Jobs, {name}, Pause) "
            f"before clavesa writes {table}. Nothing was written.")
    runs = client.call("GET", f"/api/2.1/jobs/runs/list?job_id={int(job_id)}&active_only=true&limit=5")
    active = [r.get("run_id") for r in (runs or {}).get("runs") or []]
    if active:
        raise UCError(f"{table}: Databricks job {job_id} ({name!r}) has active runs {active}; wait for them "
                      f"or cancel them before clavesa writes {table}. Nothing was written.")
    _log(f"replaced Databricks job {job_id} ({name!r}) is paused and idle")


def prepare(client: Client, cfg: dict[str, Any], spark, table_id: str, spec: dict[str, Any] | None = None) -> dict[str, Any]:
    """Before the write: decide where the output goes (ADR-027 section 2).

    - The table does not exist in Unity Catalog: clavesa writes its own
      location (the output's uc_location, else the warehouse) and
      registers a new external table after the write.
    - It is an EXTERNAL Delta table: clavesa takes it over in place. The
      Glue table is pointed at the same location first, so every write
      mode (replace, append, merge, replace_partitions) lands in the
      team's table.
    - It is MANAGED (or not Delta): refused; outside writes to managed
      tables are out of scope. The fix is the switch in
      docs/cookbook/replace-a-databricks-step.md.

    With `replaces_job` set, the job must be paused first.
    """
    spec = spec or {}
    name = full_name(cfg, table_id, spec.get("uc_table", ""))
    if spec.get("replaces_job"):
        check_step_paused(client, spec["replaces_job"], name)
    existing = client.call("GET", "/api/2.1/unity-catalog/tables/" + urllib.parse.quote(name), ok404=True)
    if existing is None:
        want = s3_uri(spec.get("uc_location", "")).rstrip("/")
        ctx: dict[str, Any] = {"full_name": name, "state": "new"}
        if want:
            if spark.catalog.tableExists(table_id):
                current = s3_uri(spark.sql(f"DESCRIBE DETAIL {table_id}").collect()[0]["location"]).rstrip("/")
                if current != want:
                    raise UCError(
                        f"{name}: the output's unity_catalog_location is {want}, but this pipeline's table "
                        f"{table_id} is already at {current}. Reset the clavesa table (clavesa pipeline reset) "
                        "so the next run creates it at the new location.")
            ctx["create_location"] = want
        return ctx
    ttype = existing.get("table_type", "")
    fmt = existing.get("data_source_format", "")
    if ttype != "EXTERNAL" or fmt != "DELTA":
        raise UCError(
            f"{name} is a {ttype or '?'} {fmt or '?'} table. Clavesa can only write external Delta tables "
            "(ADR-027 section 2). Run clavesa next to it in shadow mode and switch with the runbook in "
            "docs/cookbook/replace-a-databricks-step.md (the shadow table takes the name; the managed one is "
            "kept, renamed, for roll-back), or give this output another name."
        )
    location = s3_uri(existing.get("storage_location", ""))
    if not location:
        raise UCError(f"{name} has no storage_location")
    owner_pipeline = (existing.get("properties") or {}).get("clavesa.pipeline")
    if owner_pipeline is None:
        # Unity Catalog drops REST-set properties when Databricks writes the
        # table and does not re-read them after an outside write; the Delta
        # log has them (mark_delta).
        try:
            props = spark.sql(f"DESCRIBE DETAIL delta.`{'s3a://' + location.split('://', 1)[1]}`").collect()[0]["properties"] or {}
            owner_pipeline = props.get("clavesa.pipeline")
        except Exception:  # noqa: BLE001  (no Delta log yet, or unreadable: treat as not clavesa's)
            owner_pipeline = None
    if owner_pipeline not in (None, os.environ.get("CLAVESA_PIPELINE", "")):
        raise UCError(
            f"{name} is already written by clavesa pipeline {owner_pipeline!r}; "
            "two pipelines may not write one table (ADR-016)"
        )
    if owner_pipeline is None and spec.get("mode", "replace") == "replace":
        # A table someone else built up (usually day by day) would lose
        # everything but this run's rows. Refuse unless the mode says so.
        partitioned = existing.get("partition_columns") or [
            c.get("name") for c in existing.get("columns") or [] if c.get("partition_index") is not None]
        if partitioned:
            raise UCError(
                f"{name} is partitioned by {partitioned} and was not created by clavesa. Mode 'replace' would "
                "overwrite all of it with this run's rows. Use mode 'replace_partitions' (or 'merge'/'append').")
    if not spark.catalog.tableExists(table_id):
        spark.sql(f"CREATE TABLE {table_id} USING DELTA LOCATION '{location}'")
        _log(f"taking over external table {name} at {location}")
    else:
        current = s3_uri(spark.sql(f"DESCRIBE DETAIL {table_id}").collect()[0]["location"])
        if current.rstrip("/") != location.rstrip("/"):
            raise UCError(
                f"{name} is at {location}, but this pipeline's table {table_id} is already at {current}. "
                f"Reset the clavesa table (clavesa pipeline reset) so the next run writes the Unity Catalog table."
            )
    return {"full_name": name, "state": "external", "location": location, "columns": existing.get("columns") or []}


def mark_delta(spark, table_id: str) -> None:
    """Record which clavesa pipeline and node write the table in the Delta
    log itself (TBLPROPERTIES), not only in the Unity Catalog registration.

    Found in the switch rehearsal: once Databricks writes the table (the
    history copy), Unity Catalog re-reads its properties from the Delta log
    and the ones clavesa set through REST are gone, and with them the
    one-pipeline-per-table guard. A metadata-only commit, made only when a
    value is missing or differs."""
    want = {"clavesa.pipeline": os.environ.get("CLAVESA_PIPELINE", ""), "clavesa.node": os.environ.get("CLAVESA_NODE", "")}
    have = spark.sql(f"DESCRIBE DETAIL {table_id}").collect()[0]["properties"] or {}
    if all(have.get(k) == v for k, v in want.items()):
        return
    props = ", ".join(f"'{k}' = '{v}'" for k, v in want.items())
    spark.sql(f"ALTER TABLE {table_id} SET TBLPROPERTIES ({props})")


def publish(client: Client, cfg: dict[str, Any], spark, table_id: str, ctx: dict[str, Any]) -> dict[str, Any]:
    """After the write: register a new table, or check a taken-over one.

    Returns {"full_name", "action", "url"} for the run record.
    """
    name = ctx["full_name"]
    mark_delta(spark, table_id)
    schema = spark.table(table_id).schema
    cols = columns_from_schema(schema)
    catalog, uc_schema, table = name.split(".")
    if ctx["state"] == "new":
        location = s3_uri(spark.sql(f"DESCRIBE DETAIL {table_id}").collect()[0]["location"])
        client.call("POST", "/api/2.1/unity-catalog/tables", {
            "name": table,
            "catalog_name": catalog,
            "schema_name": uc_schema,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": location,
            "columns": cols,
            "properties": {
                "clavesa.pipeline": os.environ.get("CLAVESA_PIPELINE", ""),
                "clavesa.node": os.environ.get("CLAVESA_NODE", ""),
                "clavesa.glue-table": table_id,
            },
        })
        _log(f"registered {name} at {location}")
        action = "registered"
        grant_readers(client, cfg, name)
    else:
        action = "written"
        if _columns_signature(ctx.get("columns") or []) != _columns_signature(cols):
            # ADR-027 section 3, provisional decision 4: readers already see
            # the new columns (schema comes from the Delta log); only the
            # Catalog Explorer list lags, and the API cannot change it.
            _log(
                f"{name}: the columns changed; readers see them now, Catalog Explorer lists the old ones until an "
                f"admin runs: REPAIR TABLE {name} SYNC METADATA"
            )
            action = "written (column list stale)"
        else:
            _log(f"wrote {name} in place")
    return {"full_name": name, "action": action, "url": explorer_url(cfg, name)}


def grant_readers(client: Client, cfg: dict[str, Any], name: str) -> None:
    """SELECT on a table clavesa registered, for the connection's readers.

    A new table has only its owner's grants; a SQL warehouse user who
    reads the replaced step's table would get a permission error on the
    new one. Readers still need USE CATALOG and USE SCHEMA on its parents,
    which the team (or --create-catalog's Terraform) grants once."""
    readers = cfg.get("readers") or []
    if not readers:
        return
    client.call("PATCH", "/api/2.1/unity-catalog/permissions/table/" + urllib.parse.quote(name),
                {"changes": [{"principal": r, "add": ["SELECT"]} for r in readers]})
    _log(f"granted SELECT on {name} to {', '.join(readers)}")


def explorer_url(cfg: dict[str, Any], name: str) -> str:
    return cfg["host"] + "/explore/data/" + "/".join(urllib.parse.quote(p) for p in name.split("."))


# ---------------------------------------------------------------------------
# Reading Unity Catalog tables (ADR-027 provisional decision 2)
# ---------------------------------------------------------------------------


def get_table(client: Client, name: str) -> dict[str, Any]:
    t = client.call("GET", "/api/2.1/unity-catalog/tables/" + urllib.parse.quote(name), ok404=True)
    if t is None:
        raise UCError(f"{name}: no such table, or the service principal lacks USE CATALOG / USE SCHEMA / SELECT on it")
    return t


def bucket_of(location: str) -> str:
    return s3_uri(location).split("://", 1)[1].split("/", 1)[0]


def vend_read(client: Client, table: dict[str, Any]) -> dict[str, str]:
    """Short-lived S3 credentials to read one table (Unity Catalog
    credential vending). Needs external data access on the metastore and
    EXTERNAL USE SCHEMA on the table's schema."""
    out = client.call("POST", "/api/2.1/unity-catalog/temporary-table-credentials",
                      {"table_id": table["table_id"], "operation": "READ"})
    aws = out.get("aws_temp_credentials") or {}
    if not aws.get("access_key_id"):
        raise UCError(f"{table.get('full_name')}: credential vending returned no AWS credentials: {list(out)}")
    return aws


_S3A_KEYS = ("aws.credentials.provider", "access.key", "secret.key", "session.token")


def set_bucket_credentials(spark, bucket: str, aws: dict[str, str] | None) -> None:
    """Point S3A at vended credentials for one bucket, or back at the
    runner's own credentials (aws=None). The FileSystem cache is closed so
    the next read opens the bucket with the new credentials."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    prefix = f"fs.s3a.bucket.{bucket}."
    if aws is None:
        for k in _S3A_KEYS:
            conf.unset(prefix + k)
    else:
        conf.set(prefix + "aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        conf.set(prefix + "access.key", aws["access_key_id"])
        conf.set(prefix + "secret.key", aws["secret_access_key"])
        conf.set(prefix + "session.token", aws.get("session_token", ""))
    jvm.org.apache.hadoop.fs.FileSystem.closeAll()


def open_table(spark, client: Client, name: str):
    """A DataFrame over a Unity Catalog Delta table, and how it was opened.

    An EXTERNAL table is read from its folder with the runner's own
    credentials (its IAM role needs read on the prefix). A MANAGED table
    is read with vended credentials, set for its bucket; the caller must
    not read other paths in that bucket with the runner's own credentials
    while they are set, and calls set_bucket_credentials(spark, bucket,
    None) when done.
    """
    t = get_table(client, name)
    if t.get("data_source_format") != "DELTA":
        raise UCError(f"{name} is {t.get('data_source_format')}; clavesa reads Delta tables only")
    location = s3_uri(t.get("storage_location") or "")
    if not location:
        raise UCError(f"{name} has no storage location (a view or a table on Default Storage cannot be read from outside)")
    path = "s3a://" + location.split("://", 1)[1]
    how = "own credentials"
    if t.get("table_type") == "MANAGED":
        set_bucket_credentials(spark, bucket_of(location), vend_read(client, t))
        how = "vended credentials"
    _log(f"reading {name} ({t.get('table_type')}) at {location} with {how}")
    return spark.read.format("delta").load(path), {"table": t, "location": location, "vended": how != "own credentials"}


# ---------------------------------------------------------------------------
# Shadow comparison (docs/cookbook/replace-a-databricks-step.md)
# ---------------------------------------------------------------------------


def _profile(spark, client: Client, name: str, partition: str, sums: list[str], where: str) -> tuple[dict, dict]:
    """Rows and key sums per partition, collected to the driver, and the
    schema. Collecting closes the read before the next table is opened,
    so two tables in one bucket can use different credentials."""
    from pyspark.sql import functions as F  # noqa: PLC0415

    df, info = open_table(spark, client, name)
    cols = {f.name.lower(): f for f in df.schema.fields}
    missing = [c for c in [partition, *sums] if c.lower() not in cols]
    if missing:
        raise UCError(f"{name} has no column(s) {missing}")
    if where:
        df = df.where(where)
    aggs = [F.count(F.lit(1)).alias("rows")] + [
        # Each value as decimal(38,6) first: a decimal sum is exact, so the
        # two engines' different summing orders give the same number (a
        # double sum differs in the last bits).
        F.sum(F.col(c).cast("decimal(38,6)")).cast("string").alias("sum_" + c) for c in sums]
    rows = {str(r[partition]): r.asDict() for r in df.groupBy(partition).agg(*aggs).collect()}
    schema = {f.name.lower(): f.dataType.simpleString() for f in df.schema.fields}
    if info["vended"]:
        set_bucket_credentials(spark, bucket_of(info["location"]), None)
    return rows, schema


def compare(spark, client: Client, existing: str, shadow: str, partition: str, sums: list[str],
            where: str = "") -> dict[str, Any]:
    """Per partition: row count and the sum of each key column in the
    existing table (the Databricks step's) and the shadow table
    (clavesa's). Pass only when every partition is in both and every number
    is equal, and the columns match."""
    a, schema_a = _profile(spark, client, existing, partition, sums, where)
    b, schema_b = _profile(spark, client, shadow, partition, sums, where)
    parts = []
    for p in sorted(set(a) | set(b)):
        ra, rb = a.get(p), b.get(p)
        row: dict[str, Any] = {"partition": p}
        if ra is None or rb is None:
            row["status"] = "missing in " + ("existing" if ra is None else "shadow")
        else:
            diffs = [k for k in ["rows", *("sum_" + c for c in sums)] if ra.get(k) != rb.get(k)]
            row["status"] = "match" if not diffs else "differs: " + ", ".join(diffs)
        for k in ["rows", *("sum_" + c for c in sums)]:
            row[k] = [None if ra is None else ra.get(k), None if rb is None else rb.get(k)]
        parts.append(row)
    schema_diff = sorted(
        [f"{c}: {schema_a.get(c)} vs {schema_b.get(c)}" for c in set(schema_a) | set(schema_b)
         if schema_a.get(c) != schema_b.get(c)])
    ok = bool(parts) and all(r["status"] == "match" for r in parts) and not schema_diff
    return {"pass": ok, "existing": existing, "shadow": shadow, "partition": partition, "sums": sums,
            "where": where, "partitions": parts, "schema_diff": schema_diff}


# ---------------------------------------------------------------------------
# Lineage (slice 5)
# ---------------------------------------------------------------------------

_SAFE = re.compile(r"[^A-Za-z0-9_]")


def _meta_name(*parts: str) -> str:
    return _SAFE.sub("_", "_".join(p for p in parts if p))[:255]


def _upsert_metadata(client: Client, body: dict[str, Any]) -> None:
    try:
        client.call("POST", "/api/2.0/lineage-tracking/external-metadata", body)
    except UCError as exc:
        if "ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc):
            raise
        name = urllib.parse.quote(body["name"])
        client.call(
            "PATCH",
            f"/api/2.0/lineage-tracking/external-metadata/{name}?update_mask=columns,properties,url,entity_type",
            body,
        )


def _add_edge(client: Client, source: dict[str, Any], target: dict[str, Any], columns: list[dict[str, str]], props: dict[str, str]) -> None:
    body: dict[str, Any] = {"source": source, "target": target, "properties": props}
    if columns:
        body["columns"] = columns
    try:
        client.call("POST", "/api/2.0/lineage-tracking/external-lineage", body)
    except UCError as exc:
        if "ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc):
            raise


def _input_endpoint(client: Client, cfg: dict[str, Any], alias: str, src: Any) -> tuple[dict[str, Any] | None, str]:
    """The lineage endpoint for one transform input, and a label for logs.

    A table that exists in Unity Catalog links as a table; everything else
    (S3, HTTP, a Glue-only table) becomes an external metadata object
    (P0: system_type OTHER; there is no S3 or HTTP type).
    """
    pipeline = os.environ.get("CLAVESA_PIPELINE", "")
    url, kind = "", "path"
    if isinstance(src, str):
        if "/" not in src and src.count(".") == 1:
            db, table = src.split(".")
            schema = db.split("__", 1)[1] if "__" in db else db
            uc_name = f"{cfg['catalog']}.{schema}.{table}"
            if client.call("GET", "/api/2.1/unity-catalog/tables/" + urllib.parse.quote(uc_name), ok404=True):
                return {"table": {"name": uc_name}}, uc_name
            kind, url = "glue table", src
        else:
            url = src
    elif isinstance(src, dict):
        kind = str(src.get("kind") or "path")
        if kind == "uc_table" and src.get("table"):
            return {"table": {"name": src["table"]}}, src["table"]
        if kind == "http":
            url = src.get("url", "")
        elif kind == "s3":
            url = f"s3://{src.get('bucket', '')}/{src.get('prefix', '')}"
        else:
            url = str(src.get("path") or src.get("table") or src.get("table_id") or "")
    name = _meta_name("clavesa", pipeline, "in", alias)
    body = {
        "name": name,
        "system_type": "OTHER",
        "entity_type": f"clavesa source ({kind})",
        "properties": {"clavesa.pipeline": pipeline, "clavesa.input": alias, "clavesa.kind": kind},
    }
    if url.startswith("http") or url.startswith("s3://"):
        body["url"] = url
    else:
        body["properties"]["clavesa.location"] = url
    _upsert_metadata(client, body)
    return {"external_metadata": {"name": name}}, name


def push_lineage(client: Client, cfg: dict[str, Any], inputs: dict[str, Any], output_name: str, columns: list[str]) -> None:
    """ADR-027 section 5: input -> clavesa step -> output table.

    The clavesa step is an external metadata object, so every edge has one
    on one side, which the API requires (P0).
    """
    pipeline = os.environ.get("CLAVESA_PIPELINE", "")
    node = os.environ.get("CLAVESA_NODE", "")
    step = _meta_name("clavesa", pipeline, node)
    _upsert_metadata(client, {
        "name": step,
        "system_type": "OTHER",
        "entity_type": "clavesa transform",
        "columns": columns,
        "properties": {"clavesa.pipeline": pipeline, "clavesa.node": node},
    })
    props = {"clavesa.pipeline": pipeline, "clavesa.node": node}
    _add_edge(client, {"external_metadata": {"name": step}}, {"table": {"name": output_name}},
              [{"source": c, "target": c} for c in columns], props)
    labels = []
    for alias, src in inputs.items():
        endpoint, label = _input_endpoint(client, cfg, alias, src)
        if endpoint is not None:
            _add_edge(client, endpoint, {"external_metadata": {"name": step}}, [], props)
            labels.append(label)
    _log(f"lineage: {', '.join(labels) or '(no inputs)'} -> {step} -> {output_name}")


# ---------------------------------------------------------------------------
# Downstream jobs (slice 6)
# ---------------------------------------------------------------------------


def start_jobs(client: Client, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """ADR-027 section 6, option A: start each declared job once, after a
    successful run. Returns one record per job for the run page."""
    started = []
    for job_id in cfg.get("jobs") or []:
        rec: dict[str, Any] = {"job_id": job_id}
        try:
            out = client.call("POST", "/api/2.1/jobs/run-now", {"job_id": job_id})
            rec["run_id"] = out.get("run_id")
            run = client.call("GET", f"/api/2.1/jobs/runs/get?run_id={rec['run_id']}")
            rec["run_page_url"] = run.get("run_page_url", "")
            _log(f"started Databricks job {job_id}: run {rec['run_id']} {rec['run_page_url']}")
        except UCError as exc:
            rec["error"] = str(exc)
            _log(f"could not start Databricks job {job_id}: {exc}")
        started.append(rec)
    return started
