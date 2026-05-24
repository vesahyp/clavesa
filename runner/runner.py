"""
Clavesa transform runner — PySpark.

One engine, identical semantics across local, Lambda, Fargate, EMR Serverless.
The same transform script that runs in preview mode runs unchanged in production.

Preview mode (CLAVESA_PREVIEW=1):
  - Reads input rows from CLAVESA_PREVIEW_INPUT_<ALIAS> env vars (JSON arrays).
  - Each input is registered as a temp view named <alias>.
  - One of:
      * CLAVESA_SQL — a SparkSQL query string. Single output, key "default".
      * CLAVESA_PYTHON_SCRIPT — a script defining
            transform(spark, inputs) -> dict[str, DataFrame]
        where inputs is dict[str, pyspark.sql.DataFrame].
  - Writes {"<output>": [rows...], ...} JSON to stdout.

Production mode: handler() is the Lambda entry point.
  Event shape (also used for local CLI invocation):
      {
        "inputs":  {"alias": "s3://bucket/path/" | "/local/path/", ...},
        "outputs": {"key":   "s3://bucket/path/" | "/local/path/", ...}
      }
  Reads logic from CLAVESA_LOGIC_S3_PATH (s3:// or local), reads each input
  as Parquet, runs the transform, writes outputs as Parquet. Returns
  {"status": "ok", "outputs": {key: path}}.

S3 vs local: any path that starts with "s3://" routes through boto3; anything
else is treated as a local filesystem path. This lets the same handler() back
both Lambda invocations and local `clavesa pipeline run` commands.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sys
import time
import types
import uuid
from typing import Any

# Module-level SparkSession so warm starts (UI preview server reusing the
# container, Lambda warm invocations) skip the ~3-5s JVM boot.
_SPARK = None


def _spark():
    """Lazy SparkSession with Iceberg + catalog + S3 config baked in.

    Iceberg catalog naming (per ADR-013):
      catalog name (Spark side) = "clavesa"
      catalog impl              = GlueCatalog when CLAVESA_WAREHOUSE is set
                                  to s3://...; HadoopCatalog otherwise (local
                                  dev / preview)
      warehouse path            = CLAVESA_WAREHOUSE env, defaulting to
                                  /tmp/clavesa-warehouse for local runs

    Lambda-specific knobs (driver.bindAddress, ui.enabled) match SoAL —
    Lambda's hostname doesn't resolve to a bindable interface.
    """
    global _SPARK
    if _SPARK is None:
        from pyspark.sql import SparkSession  # noqa: PLC0415

        warehouse = os.environ.get("CLAVESA_WAREHOUSE", "/tmp/clavesa-warehouse")
        is_s3 = warehouse.startswith("s3://")

        builder = (
            SparkSession.builder.appName("clavesa-runner")
            .master(os.environ.get("CLAVESA_SPARK_MASTER", "local[*]"))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.showConsoleProgress", "false")
            # Iceberg session extensions — enables CALL syntax, MERGE INTO, etc.
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.clavesa", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.clavesa.warehouse", warehouse)
        )

        if is_s3:
            # Cloud: Glue Data Catalog as the metastore so Athena can query
            # tables natively (ADR-013). S3FileIO is Iceberg-AWS's S3 client.
            builder = (
                builder.config(
                    "spark.sql.catalog.clavesa.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog",
                ).config(
                    "spark.sql.catalog.clavesa.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO",
                )
            )
        else:
            # Local dev / preview: Hadoop catalog — file-based, no metastore.
            # Tables live as directories under the warehouse path.
            builder = builder.config(
                "spark.sql.catalog.clavesa.type", "hadoop"
            )

        # Plain spark.read.parquet("s3://...") uses hadoop-aws regardless
        # of where the Iceberg warehouse lives. ADR-017 slice 3 made
        # local pipelines able to read s3:// sources, so this scheme
        # mapping has to apply unconditionally — not gated on `is_s3`,
        # which only describes the warehouse target.
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            ).config(
                "spark.hadoop.fs.s3.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            ).config(
                "spark.hadoop.fs.s3n.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
        )
        # Test-infra override: point S3A at a custom endpoint (moto,
        # MinIO) when CLAVESA_S3_ENDPOINT is set. Wires path-style
        # addressing too — virtual-host buckets don't work against
        # arbitrary localhost endpoints.
        s3_endpoint = os.environ.get("CLAVESA_S3_ENDPOINT")
        if s3_endpoint:
            builder = (
                builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                        "true" if s3_endpoint.startswith("https://") else "false")
            )

        _SPARK = builder.getOrCreate()
        _SPARK.sparkContext.setLogLevel("ERROR")
    return _SPARK


def _load_script(source: str) -> types.ModuleType:
    mod = types.ModuleType("_user_transform")
    exec(compile(source, "<clavesa_script>", "exec"), mod.__dict__)  # noqa: S102
    return mod


# ---------------------------------------------------------------------------
# Preview-mode helpers (in-memory rows ↔ DataFrames)
# ---------------------------------------------------------------------------


def _df_to_rows(df) -> list[dict[str, Any]]:
    pdf = df.toPandas()
    return json.loads(pdf.to_json(orient="records", date_format="iso"))


def _normalise_output(value) -> list[dict[str, Any]]:
    from pyspark.sql import DataFrame  # noqa: PLC0415

    if isinstance(value, DataFrame):
        return _df_to_rows(value)
    if isinstance(value, list):
        return value
    raise TypeError(
        f"transform() output values must be a Spark DataFrame or list of dicts, got {type(value)}"
    )


def _register_inputs(spark, inputs: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    dataframes: dict[str, Any] = {}
    for alias, rows in inputs.items():
        if not rows:
            df = spark.createDataFrame([], schema="struct<>")
        else:
            df = spark.createDataFrame(rows)
        df.createOrReplaceTempView(alias)
        dataframes[alias] = df
    return dataframes


def run_preview() -> None:
    prefix = "CLAVESA_PREVIEW_INPUT_"
    inputs: dict[str, list] = {}
    for key, val in os.environ.items():
        if key.startswith(prefix):
            alias = key[len(prefix):].lower()
            inputs[alias] = json.loads(val)

    sql = os.environ.get("CLAVESA_SQL", "").strip()
    script = os.environ.get("CLAVESA_PYTHON_SCRIPT", "")

    if not sql and not script:
        print("{}", flush=True)
        return

    spark = _spark()
    dataframes = _register_inputs(spark, inputs)

    if sql:
        result_df = spark.sql(sql)
        result = {"default": _df_to_rows(result_df)}
    else:
        mod = _load_script(script)
        if not hasattr(mod, "transform"):
            raise RuntimeError(
                "User script must define a top-level 'transform(spark, inputs)' function."
            )
        raw = mod.transform(spark, dataframes)
        if not isinstance(raw, dict):
            raise TypeError(f"transform() must return a dict, got {type(raw)}")
        result = {k: _normalise_output(v) for k, v in raw.items()}

    print(json.dumps(result), flush=True)


# ---------------------------------------------------------------------------
# Production-mode helpers (S3/local I/O)
# ---------------------------------------------------------------------------


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _split_s3(path: str) -> tuple[str, str]:
    """s3://bucket/key/parts → ('bucket', 'key/parts'). Trailing slash kept."""
    assert _is_s3(path), f"not an s3 path: {path}"
    rest = path[len("s3://"):]
    bucket, _, key = rest.partition("/")
    return bucket, key


def _read_text(path: str) -> str:
    if _is_s3(path):
        import boto3  # noqa: PLC0415

        bucket, key = _split_s3(path)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return body.decode("utf-8")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _looks_like_path(s: str) -> bool:
    """Heuristic: paths have slashes; Iceberg table identifiers don't."""
    return "/" in s or s.startswith("s3://")


# ---------------------------------------------------------------------------
# Incremental processing — partition listing + watermarks (v0.12+)
# ---------------------------------------------------------------------------


def _list_partition_tree(bucket: str, prefix: str, partition_names: list[str]) -> list[tuple[tuple[str, ...], str]]:
    """Walk an S3 partition tree (Hive-style) and return (cursor, full_prefix)
    pairs for every leaf partition.

    For partition_names = ["day", "hour"] under prefix
    "logs/year=2026/month=04/", recursively lists:
      logs/year=2026/month=04/day=*/hour=*/
    Returns e.g. [(("2026-04-26", "00"), "logs/.../day=2026-04-26/hour=00/"), ...].

    Ordering: lexicographic on the cursor tuple. Matches Python tuple
    comparison so callers can do `cursor > watermark` directly.
    """
    import boto3  # noqa: PLC0415

    s3 = boto3.client("s3")

    def walk(cur_prefix: str, remaining: list[str]) -> list[tuple[tuple[str, ...], str]]:
        if not remaining:
            return [((), cur_prefix)]
        head, *tail = remaining
        out: list[tuple[tuple[str, ...], str]] = []
        token = f"{head}="
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=cur_prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []) or []:
                sub = cp["Prefix"]
                last = sub.rstrip("/").rsplit("/", 1)[-1]
                if not last.startswith(token):
                    continue
                value = last[len(token):]
                for cur, leaf in walk(sub, tail):
                    out.append(((value,) + cur, leaf))
        return out

    return sorted(walk(prefix, partition_names), key=lambda x: x[0])


def _watermark_uri(alias: str) -> str:
    """Resolve the watermark URI for a (consumer, input) pair. Pipeline-shared
    so transforms reading the same source see the same cursor.

    Cloud (s3:// base): a per-alias JSON object under the pipeline bucket.
    Local (file:// or plain path): the local pipeline-run flow sets a
    filesystem path so `pipeline run` doesn't need S3 to track watermarks
    against transform upstreams. ADR-014 local-cloud parity.
    """
    base = os.environ.get("CLAVESA_WATERMARKS", "")
    if not base:
        raise RuntimeError("CLAVESA_WATERMARKS env var not set")
    if not base.endswith("/"):
        base += "/"
    return f"{base}{alias}.json"


def _read_watermark(uri: str) -> tuple[str, ...] | None:
    """Returns the stored cursor as a tuple, or None when no watermark exists.
    Treats any read error other than NoSuchKey / FileNotFoundError as fatal:
    better to fail loud than reprocess silently."""
    if uri.startswith("s3://"):
        import boto3  # noqa: PLC0415
        from botocore.exceptions import ClientError  # noqa: PLC0415

        bucket, key = _split_s3(uri)
        try:
            body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                return None
            raise
        payload = json.loads(body)
    else:
        path = uri[len("file://"):] if uri.startswith("file://") else uri
        try:
            with open(path, "rb") as f:
                payload = json.loads(f.read())
        except FileNotFoundError:
            return None
    cursor = payload.get("cursor")
    if not isinstance(cursor, list):
        return None
    return tuple(str(x) for x in cursor)


def _write_watermark(uri: str, cursor: tuple[str, ...]) -> None:
    import datetime as _dt  # noqa: PLC0415

    payload = {
        "cursor": list(cursor),
        "updated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }
    body = json.dumps(payload).encode("utf-8")
    if uri.startswith("s3://"):
        import boto3  # noqa: PLC0415

        bucket, key = _split_s3(uri)
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return
    path = uri[len("file://"):] if uri.startswith("file://") else uri
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(body)


def _resolve_initial_cursor(start_from: str, partitions: list[tuple[tuple[str, ...], str]]) -> tuple[str, ...] | None:
    """Translate a start_from declaration into an effective watermark when none
    is stored yet.

    "all"    → None (no filter; first run reads everything).
    "now"    → the current max partition (skip backfill).
    literal  → tuple-encoded literal (e.g. "2026-04-26" → ("2026-04-26",)).
    """
    if start_from in ("", "all"):
        return None
    if start_from == "now":
        return partitions[-1][0] if partitions else None
    # Literal: take "/"-separated segments. start_from = "2026-04-26" gives
    # the tuple ("2026-04-26",); "2026-04-26/14" gives ("2026-04-26", "14").
    return tuple(start_from.split("/"))


def _download_http_to_tmp(url: str, headers: dict | None = None) -> str:
    """Download an http(s) URL into a unique /tmp file and return the path.

    ADR-017 slice 1: stdlib only (no extra dep), no caching. Slice 2
    adds optional headers for credentialed fetches.

    Filename component preserved so Spark's format dispatch (which keys
    off path extension for some formats) still works. Hash prefix
    incorporates the headers so an authed fetch and an unauthed fetch
    of the same URL don't share a cache entry — typically they return
    different bytes.
    """
    import hashlib  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    base = url.rsplit("/", 1)[-1] or "data"
    cache_key = url
    if headers:
        # Sort so dict ordering doesn't change the digest. Header values
        # may be sensitive; only the digest hits disk.
        cache_key += "\n" + "\n".join(f"{k}={headers[k]}" for k in sorted(headers))
    digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()[:12]
    dest = f"/tmp/clavesa-src-{digest}-{base}"
    if not os.path.exists(dest):
        tmp = dest + ".part"
        req = urllib.request.Request(url, headers=headers or {})  # noqa: S310
        with urllib.request.urlopen(req) as resp, open(tmp, "wb") as f:  # noqa: S310
            while True:
                chunk = resp.read(64 * 1024)
                if not chunk:
                    break
                f.write(chunk)
        os.replace(tmp, dest)
    return dest


def _resolve_http_headers(credential: dict | None) -> dict | None:
    """Resolve a credential descriptor (inlined by the orchestration
    emitter) into the headers dict the request needs.

    Descriptor shape (slice 2):
        {
          "kind": "header",
          "header_name": "Authorization",
          "value_prefix": "Bearer ",
          "secret": "env:STRIPE_KEY"           # or file:/abs/path or arn:aws:secretsmanager:...
        }

    Returns None when credential is None or empty (anonymous fetch).
    Raises RuntimeError on backend resolution failures — the runner
    fails the run rather than fetching with a bad header (which would
    produce a confusing downstream error).
    """
    if not credential:
        return None
    kind = credential.get("kind", "")
    if kind != "header":
        raise RuntimeError(f"unsupported credential kind {kind!r} (slice 2: only header)")
    secret_value = _resolve_secret(credential.get("secret", ""))
    header_name = credential.get("header_name", "")
    if not header_name:
        raise RuntimeError("credential missing header_name")
    value_prefix = credential.get("value_prefix", "") or ""
    return {header_name: value_prefix + secret_value}


def _resolve_secret(ref: str) -> str:
    """Translate a slice-2 secret reference into the actual secret value.

    Backends:
      env:VAR              — read os.environ[VAR]
      file:<absolute-path> — read the file's full contents (rstripped of
                             trailing whitespace, since editors love
                             adding trailing newlines)
      arn:aws:secretsmanager:... — fetch via boto3 secretsmanager:GetSecretValue

    Cloud-only backends (arn:) require boto3 and IAM. Local-only backends
    (env:/file:) are rejected at orchestration emit time for cloud
    pipelines (see service.SyncOrchestration); the runner doesn't
    re-validate, just fetches.
    """
    if ref.startswith("env:"):
        var = ref[len("env:"):]
        if var not in os.environ:
            raise RuntimeError(f"credential env var {var!r} is not set")
        return os.environ[var]
    if ref.startswith("file:"):
        path = ref[len("file:"):]
        with open(path, "r", encoding="utf-8") as f:
            return f.read().rstrip("\n\r")
    if ref.startswith("arn:aws:secretsmanager:"):
        import boto3  # noqa: PLC0415

        # ARN like arn:aws:secretsmanager:eu-north-1:123:secret:foo
        # boto3 accepts the full ARN as SecretId.
        client = boto3.client("secretsmanager")
        resp = client.get_secret_value(SecretId=ref)
        # SecretString first; fall back to SecretBinary for non-text secrets.
        if "SecretString" in resp:
            return resp["SecretString"]
        return resp["SecretBinary"].decode("utf-8")
    raise RuntimeError(f"unknown secret backend in reference {ref!r}")


def _is_hive_partitioned(path: str) -> bool:
    """True when the immediate children of a prefix-style `path` are
    Hive-style partition directories (`name=value/`).

    Such a layout must be read with Spark's default partition discovery
    so the partition keys surface as columns — `recursiveFileLookup`
    finds the files but silently drops those columns (e.g. CloudFront
    logs laid out `…/day=26/hour=NN/` lose `hour`). Best-effort: any
    listing error returns False, falling back to recursiveFileLookup,
    which at least still finds the files.
    """
    try:
        if _is_s3(path):
            import boto3  # noqa: PLC0415

            bucket, key = _split_s3(path)
            if key and not key.endswith("/"):
                key += "/"
            resp = boto3.client("s3").list_objects_v2(
                Bucket=bucket, Prefix=key, Delimiter="/", MaxKeys=100
            )
            children = [
                cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
                for cp in resp.get("CommonPrefixes", [])
            ]
        else:
            children = [e.name for e in os.scandir(path) if e.is_dir()]
        return any("=" in c for c in children)
    except Exception:  # noqa: BLE001
        return False


def _read_path_format(spark, path: str, fmt: str):
    """Dispatch a Spark read by source format. Defaults match what most users
    expect for ad-hoc CSV/JSON: header on, schema inferred. Anything more
    specific (custom delimiters, schema enforcement) should ride the source's
    HCL `read_options` once we thread that through — for now, sane defaults.

    recursiveFileLookup is on for prefix-style paths (anything ending in
    "/") so a registered s3 source pointing at e.g. `s3://b/events/`
    picks up `events/2024/jan.json` without users having to encode the
    partition layout in the prefix. It is held OFF for Hive-partitioned
    prefixes (`day=26/hour=NN/…`) so Spark's default partition discovery
    runs and the partition keys surface as columns. Single-file paths
    bypass both so Spark's per-extension dispatch still works.
    """
    recurse = path.endswith("/") and not _is_hive_partitioned(path)
    if fmt in ("parquet", ""):
        reader = spark.read
        if recurse:
            reader = reader.option("recursiveFileLookup", "true")
        return reader.parquet(path)
    if fmt == "csv":
        reader = spark.read.option("header", "true").option("inferSchema", "true")
        if recurse:
            reader = reader.option("recursiveFileLookup", "true")
        return reader.csv(path)
    if fmt == "json" or fmt == "ndjson":
        reader = spark.read
        if recurse:
            reader = reader.option("recursiveFileLookup", "true")
        return reader.json(path)
    raise RuntimeError(f"unsupported source format {fmt!r} for path {path!r}")


def _resolve_input(spark, alias: str, src: Any, backfill: dict[str, Any] | None = None) -> tuple[Any, dict[str, Any] | None]:
    """Returns (DataFrame, watermark_advance_record).

    watermark_advance_record is None when the input doesn't track a watermark
    (string-form path, Iceberg table, or empty partitioned source). When set,
    it carries {"uri": ..., "new_cursor": (...,)} for the runner to commit
    after all outputs succeed.

    `backfill`, when set, switches the partitioned_path branch to read the
    closed [from_cursor, to_cursor] window instead of the watermark-based
    incremental window. The watermark is NEITHER read nor advanced — backfill
    runs go to a parallel staging table that the user inspects + promotes
    separately, leaving production state untouched.
    """
    if isinstance(src, str):
        if _looks_like_path(src):
            return spark.read.parquet(src), None
        return spark.table(src), None

    if not isinstance(src, dict):
        raise TypeError(f"input {alias!r}: unsupported descriptor type {type(src).__name__}")

    kind = src.get("kind", "")
    if kind == "http":
        # ADR-017 slice 1+2: workspace source registry, http kind. Slice 2
        # adds optional header auth resolved at request time from the
        # workspace credentials registry. The orchestration emitter
        # inlines header_name + value_prefix; the runner resolves the
        # secret reference (env: / file: / arn:) and assembles the
        # header value before issuing the request.
        url = src["url"]
        fmt = str(src.get("format") or "parquet").lower()
        headers = _resolve_http_headers(src.get("credentials"))
        local_path = _download_http_to_tmp(url, headers=headers)
        return _read_path_format(spark, local_path, fmt), None

    if kind == "s3":
        # ADR-017 slice 3: same-account S3 reads via Spark's S3A.
        # The Spark builder in `_spark()` already maps the s3:// scheme
        # onto S3AFileSystem with DefaultAWSCredentialsProviderChain,
        # so the credentials picked up here come from (in order) env
        # vars, ~/.aws, EC2 / Lambda instance role. Cross-account reads
        # via assume-role land in a later slice.
        bucket = src["bucket"]
        prefix = str(src.get("prefix") or "")
        path = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"
        fmt = str(src.get("format") or "parquet").lower()
        return _read_path_format(spark, path, fmt), None

    if kind == "path":
        # `clavesa pipeline run` emits this when the upstream source
        # declares a non-Parquet format. Routes the read by the source's
        # declared `format` attr — without this, every CSV/JSON source would
        # fail at footer-read time pretending to be Parquet.
        path = src["path"]
        fmt = str(src.get("format") or "parquet").lower()
        return _read_path_format(spark, path, fmt), None

    if kind == "iceberg_table_incremental":
        # v0.24.0: snapshot-bounded read on an Iceberg upstream. Same
        # watermark machinery as `partitioned_path` (best-effort
        # advance after outputs commit), keyed on a single Iceberg
        # snapshot id instead of a partition cursor. Used when a
        # downstream transform should process only rows committed
        # since its last successful run, not the whole upstream table.
        #
        # Descriptor:
        #   {"kind": "iceberg_table_incremental",
        #    "table": "clavesa.<db>.<table>",
        #    "alias": "<consumer_node>__<input_alias>"}
        #
        # alias scopes the watermark file so two consumers reading the
        # same upstream don't share state (each advances at its own
        # pace; producer commits land in both watermark files
        # independently).
        table = src["table"]
        alias_key = str(src.get("alias") or alias)
        # `.history` reflects the current lineage of the table; the last
        # row (most recent made_current_at) is the head. `.snapshots`
        # includes orphaned snapshots from prior createOrReplace cycles,
        # so MAX(committed_at) there can return a snapshot that isn't
        # actually the table's current root — which would then fail the
        # incremental scan with a confusing "not a parent ancestor"
        # error. Using `.history` is the contract-correct way to ask
        # "what is the current head".
        snapshots_query = (
            f"SELECT snapshot_id FROM {table}.history "
            f"ORDER BY made_current_at DESC LIMIT 1"
        )
        current_row = spark.sql(snapshots_query).first()
        if current_row is None:
            print(
                f"[clavesa] input {alias!r}: upstream {table} has no snapshots yet; skipping run",
                file=sys.stderr,
            )
            return None, None
        current_snapshot = int(current_row[0])
        watermark_uri = _watermark_uri(alias_key)
        stored = _read_watermark(watermark_uri)
        last_snapshot: int | None = None
        if stored is not None and len(stored) == 1:
            try:
                last_snapshot = int(stored[0])
            except ValueError:
                last_snapshot = None
        if last_snapshot is None:
            # First run for this (consumer, upstream) pair: full read,
            # advance watermark to the current snapshot. Same semantic
            # as start_from="all" on a partitioned source.
            print(
                f"[clavesa] input {alias!r}: first incremental run on {table}; reading full snapshot {current_snapshot}",
                file=sys.stderr,
            )
            return spark.table(table), {
                "uri": watermark_uri,
                "new_cursor": (str(current_snapshot),),
            }
        if last_snapshot == current_snapshot:
            # No new commits since last run. Skip.
            print(
                f"[clavesa] input {alias!r}: upstream {table} unchanged since snapshot {current_snapshot}; skipping run",
                file=sys.stderr,
            )
            return None, None
        # Try the snapshot-bounded read. If the watermark snapshot is no
        # longer in the table's lineage (a `mode = "replace"` upstream
        # rewrote the tree, garbage collection expired the old snapshot,
        # etc.), Iceberg raises "Starting snapshot ... is not a parent
        # ancestor of end snapshot". Fall back to a full read +
        # watermark reset — same semantic as first-run, bounded by the
        # upstream's current size. Done as a try/except instead of an
        # ancestor pre-check because Iceberg's `.history` view doesn't
        # cleanly distinguish in-lineage from orphaned snapshots after a
        # createOrReplace rewrite; the exception is the authoritative
        # signal.
        # Walk the parent chain of `current_snapshot` to confirm
        # `last_snapshot` is in lineage. A `mode = "replace"` upstream
        # re-roots the table on every run, so the previous watermark
        # snapshot ends up orphaned — its row still exists in
        # `<table>.snapshots` but it isn't an ancestor of the new
        # current. An incremental read would fail with "Starting
        # snapshot ... is not a parent ancestor of end snapshot"; pre-
        # detecting via the parent chain gives us a clean fallback
        # (full read + watermark reset) instead of a confusing runtime
        # error halfway through the transform.
        parents_rows = spark.sql(
            f"SELECT snapshot_id, parent_id FROM {table}.snapshots"
        ).collect()
        parents: dict[int, int | None] = {}
        for row in parents_rows:
            pid = row[1]
            parents[int(row[0])] = int(pid) if pid is not None else None
        cursor_node: int | None = current_snapshot
        found = False
        while cursor_node is not None:
            if cursor_node == last_snapshot:
                found = True
                break
            cursor_node = parents.get(cursor_node)
        if not found:
            print(
                f"[clavesa] input {alias!r}: upstream {table} was rewritten "
                f"(watermark snapshot {last_snapshot} is not an ancestor of current snapshot {current_snapshot}); "
                f"falling back to full read and re-stamping watermark",
                file=sys.stderr,
            )
            return spark.table(table), {
                "uri": watermark_uri,
                "new_cursor": (str(current_snapshot),),
            }
        df = (
            spark.read
            .option("start-snapshot-id", str(last_snapshot))
            .option("end-snapshot-id", str(current_snapshot))
            .table(table)
        )
        print(
            f"[clavesa] input {alias!r}: reading {table} snapshot range ({last_snapshot}, {current_snapshot}]",
            file=sys.stderr,
        )
        return df, {
            "uri": watermark_uri,
            "new_cursor": (str(current_snapshot),),
        }

    if kind == "partitioned_path":
        path = src["path"]
        partition_names = list(src.get("partitions") or [])
        start_from = str(src.get("start_from") or "all")
        if not path.startswith("s3://"):
            raise RuntimeError(f"partitioned_path inputs must be s3://...; got {path!r}")
        bucket, prefix = _split_s3(path)
        if not prefix.endswith("/"):
            prefix += "/"
        all_partitions = _list_partition_tree(bucket, prefix, partition_names)

        if backfill is not None:
            from_cursor = tuple(backfill.get("from_cursor") or ())
            to_cursor = tuple(backfill.get("to_cursor") or ())
            if not from_cursor or not to_cursor:
                raise RuntimeError(f"input {alias!r}: backfill requires both from_cursor and to_cursor")
            new_partitions = [(c, p) for c, p in all_partitions if from_cursor <= c <= to_cursor]
            if not new_partitions:
                print(f"[clavesa] input {alias!r}: 0 partitions in backfill window [{from_cursor!r}, {to_cursor!r}]; skipping run", file=sys.stderr)
                return None, None
            paths = [f"s3://{bucket}/{leaf}" for _, leaf in new_partitions]
            print(f"[clavesa] input {alias!r}: backfill reading {len(paths)} partitions ({new_partitions[0][0]} → {new_partitions[-1][0]})", file=sys.stderr)
            df = spark.read.option("basePath", f"s3://{bucket}/{prefix}").parquet(*paths)
            return df, None

        watermark_uri = _watermark_uri(alias)
        cursor = _read_watermark(watermark_uri)
        if cursor is None:
            cursor = _resolve_initial_cursor(start_from, all_partitions)

        if cursor is None:
            new_partitions = all_partitions
        else:
            new_partitions = [(c, p) for c, p in all_partitions if c > cursor]

        if not new_partitions:
            print(f"[clavesa] input {alias!r}: 0 new partitions since cursor {cursor!r}; skipping run", file=sys.stderr)
            return None, None

        new_max = new_partitions[-1][0]
        paths = [f"s3://{bucket}/{leaf}" for _, leaf in new_partitions]
        print(f"[clavesa] input {alias!r}: reading {len(paths)} partitions ({new_partitions[0][0]} → {new_max})", file=sys.stderr)
        # basePath tells Spark to derive the Hive partition columns
        # (year/month/day/hour/…) from each leaf path's tail relative to
        # the prefix. Without it, multi-path reads drop the partition
        # columns entirely and the output table schema diverges from a
        # full-prefix read — appending to a table created in full-read
        # mode then fails with INCOMPATIBLE_DATA_FOR_TABLE.
        df = spark.read.option("basePath", f"s3://{bucket}/{prefix}").parquet(*paths)
        return df, {"uri": watermark_uri, "new_cursor": new_max}

    raise RuntimeError(f"input {alias!r}: unknown kind {kind!r}")


def _resolve_output(key: str, dest: Any) -> dict[str, Any]:
    """Returns {kind: "path"|"iceberg_table", target: str, mode: "replace"|"append"|"merge", merge_keys: [...]}.

    String forms map to the existing semantics:
      "" or "<id>" → iceberg_table, mode=replace
      "/path" or "s3://..." → path, mode=replace
    Dict form (v0.12+) carries an explicit mode. `mode = "merge"` requires
    a non-empty `merge_keys` list naming the columns that uniquely identify
    a row in the target.
    """
    if isinstance(dest, str):
        if dest and _looks_like_path(dest):
            return {"kind": "path", "target": dest, "mode": "replace", "merge_keys": []}
        target = dest if dest else _table_id_for(key)
        return {"kind": "iceberg_table", "target": target, "mode": "replace", "merge_keys": []}

    if not isinstance(dest, dict):
        raise TypeError(f"output {key!r}: unsupported descriptor type {type(dest).__name__}")

    kind = dest.get("kind", "iceberg_table")
    merge_keys = list(dest.get("merge_keys") or [])
    # When merge_keys is declared and mode is unset, default to merge —
    # saves users from picking the right semantics every time.
    mode = dest.get("mode") or ("merge" if merge_keys else "replace")
    if mode not in ("replace", "append", "merge"):
        raise RuntimeError(f"output {key!r}: unsupported mode {mode!r}")
    if mode == "merge" and not merge_keys:
        raise RuntimeError(f"output {key!r}: mode='merge' requires non-empty merge_keys")
    target = dest.get("target") or dest.get("table_id") or dest.get("path") or ""
    if kind == "iceberg_table" and not target:
        target = _table_id_for(key)
    stats = bool(dest.get("stats"))
    return {
        "kind": kind,
        "target": target,
        "mode": mode,
        "merge_keys": merge_keys,
        "stats": stats,
    }


def _glue_db() -> str:
    """Encode the (workspace_catalog, pipeline_schema) env pair into the
    flat Glue Data Catalog database name the runner writes to.

    Mirrors `internal/identutil.EncodeGlueDatabase` on the Go side — the
    Python and Go encoders MUST stay byte-identical, since the runner
    writes to the same Glue DB the catalog handler reads from.

    Both CLAVESA_CATALOG and CLAVESA_SCHEMA are required as of
    v0.18.0. The pre-v0.18 legacy fallback (empty catalog →
    `clavesa_<schema>`) was removed once the only production user
    (cloudfront-analytics) migrated to the encoded form. CLAVESA_SCHEMA
    falls back to a sanitized CLAVESA_PIPELINE only as a defensive
    last resort — orchestration always sets both.
    """
    catalog = os.environ["CLAVESA_CATALOG"]
    schema = os.environ.get("CLAVESA_SCHEMA") or os.environ.get("CLAVESA_PIPELINE", "default")
    return f"{catalog.replace('-', '_')}__{schema.replace('-', '_')}"


# Workspace system observability schema (ADR-016 "Workspace system
# catalog"). Hard-coded here because the schema name is a domain-grouping
# convention, not a per-pipeline knob — every pipeline writes to the
# same `pipelines` schema under the system catalog, distinguished by the
# `pipeline` column on each row. Future system schemas (`query`,
# `billing`, `access`) introduce their own constants when they ship.
_SYSTEM_SCHEMA = "pipelines"


def _system_glue_db() -> str:
    """Encode the workspace system catalog into its Glue DB name —
    `<system_catalog>__pipelines`. Mirrors the user-DB encoder above so
    Glue's flat-namespace flavor of the three-level address matches what
    the workspace module creates (`aws_glue_catalog_database.system_pipelines`).

    Falls back to `<catalog>__pipelines` if CLAVESA_SYSTEM_CATALOG is
    unset — defensive only; orchestration always sets it for v0.20+.
    """
    system_catalog = os.environ.get("CLAVESA_SYSTEM_CATALOG")
    if not system_catalog:
        system_catalog = os.environ["CLAVESA_CATALOG"] + "_system"
    return f"{system_catalog.replace('-', '_')}__{_SYSTEM_SCHEMA}"


def _table_id_for(output_key: str) -> str:
    """Auto-generated Iceberg table identifier for a transform output.

    Three-segment Spark identifier: `clavesa.<glue_db>.<table>` —
    first segment is the Spark catalog name (configured in `_spark()` —
    always literal `clavesa` since Iceberg's SparkCatalog can't
    filter Glue by namespace prefix); second is the Glue DB name from
    `_glue_db()` which encodes ADR-016's (catalog, schema) pair; third
    is `<node>__<output_key>`.
    """
    node = os.environ.get("CLAVESA_NODE", "node")
    return f"clavesa.{_glue_db()}.{node}__{output_key}"


def _evolve_target_schema(spark, target: str, staging: str) -> list[str]:
    """ALTER TABLE target ADD COLUMN for every column in staging missing
    from target. Returns the list of added column names (empty when the
    schemas already match by name).

    Iceberg supports schema evolution natively — added columns read back
    NULL on existing target rows, populate from the staging values on
    matched / inserted rows. Type widening and column drops are NOT
    handled here; those surface loudly from the downstream MERGE / INSERT
    instead of silently corrupting data.
    """
    target_fields = {f.name: f.dataType for f in spark.table(target).schema}
    staging_schema = spark.table(staging).schema
    added: list[str] = []
    for field in staging_schema:
        if field.name in target_fields:
            continue
        spark.sql(
            f"ALTER TABLE {target} ADD COLUMN {field.name} {field.dataType.simpleString()}"
        )
        added.append(field.name)
    if added:
        print(
            f"[clavesa] backfill_promote: evolved {target} with {len(added)} new column(s): {added}",
            file=sys.stderr,
        )
    return added


def _run_operation(event: dict[str, Any]) -> dict[str, Any]:
    """Execute a non-transform control-plane operation against Iceberg.

    Operations:
      backfill_promote: MERGE INTO target USING staging ON <merge_keys> ...
                       (mode=merge|append + opts.{force_dedup,allow_duplicates})
                       ALTER TABLE target ADD COLUMN for any staging-only
                       columns before the merge — Iceberg schema evolution.
                       Drops staging on success.
      backfill_discard: DROP TABLE staging.

    All operations run via SparkSQL — same engine that wrote the staging
    table in the first place, same MERGE semantics the runner already
    uses for `mode = "merge"` outputs in the transform path.
    """
    op = event["_operation"]
    spark = _spark()

    if op == "backfill_discard":
        staging = event["staging"]
        spark.sql(f"DROP TABLE IF EXISTS {staging}")
        return {"status": "ok", "operation": op, "staging_dropped": staging}

    if op == "backfill_promote":
        staging = event["staging"]
        target = event["target"]
        mode = event.get("mode", "merge")
        merge_keys = list(event.get("merge_keys") or [])
        force_dedup = event.get("force_dedup", "")
        allow_dupes = bool(event.get("allow_duplicates"))

        # Evolve the target schema to absorb any new columns the user added
        # to the transform between the canonical run and the backfill —
        # otherwise SparkSQL's `MERGE … UPDATE SET *` silently drops
        # columns it can't resolve on the target side, and positional
        # `INSERT INTO target SELECT *` errors with arity mismatch.
        # Iceberg supports `ALTER TABLE … ADD COLUMN` natively; existing
        # rows in target read back NULL for the added columns.
        columns_added = _evolve_target_schema(spark, target, staging)

        if mode == "merge":
            if not merge_keys:
                raise RuntimeError("backfill_promote merge: merge_keys required")
            on = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
            sql = (
                f"MERGE INTO {target} t USING {staging} s ON {on} "
                f"WHEN MATCHED THEN UPDATE SET * "
                f"WHEN NOT MATCHED THEN INSERT *"
            )
            spark.sql(sql)
        elif mode == "append":
            if force_dedup:
                on = f"t.{force_dedup} = s.{force_dedup}"
                sql = (
                    f"MERGE INTO {target} t USING {staging} s ON {on} "
                    f"WHEN MATCHED THEN UPDATE SET * "
                    f"WHEN NOT MATCHED THEN INSERT *"
                )
                spark.sql(sql)
            elif allow_dupes:
                # DataFrame writer with mergeSchema=true so name-based
                # column resolution holds even when the schemas drifted
                # in either direction — positional `INSERT INTO target
                # SELECT *` would error on arity mismatch.
                spark.table(staging).writeTo(target).option("mergeSchema", "true").append()
            else:
                raise RuntimeError(
                    "backfill_promote append: append-mode targets need force_dedup or allow_duplicates"
                )
        else:
            raise RuntimeError(f"backfill_promote: unsupported mode {mode!r}")

        # Promotion succeeded — drop the staging table so the next backfill
        # for the same node doesn't accumulate stale staging artifacts.
        spark.sql(f"DROP TABLE IF EXISTS {staging}")
        return {
            "status": "ok",
            "operation": op,
            "target": target,
            "staging_dropped": staging,
            "columns_added": columns_added,
        }

    raise RuntimeError(f"unknown _operation: {op!r}")


def _apply_snapshot_props(writer, props):
    """Stamp clavesa provenance into the Iceberg snapshot summary.

    Each key becomes a `snapshot-property.<key>` write option, which Iceberg
    copies verbatim into the snapshot's `summary` map. The table timeline then
    shows whether an append came from a backfill, an event trigger, a
    schedule, or a manual run. Empty values are skipped — a snapshot written
    outside clavesa carries no `clavesa.*` keys, and that absence reads
    as "external / manual" downstream.
    """
    for key, val in props.items():
        if val:
            writer = writer.option(f"snapshot-property.{key}", str(val))
    return writer


def _run_transform(event, context, run_id=""):  # noqa: ARG001
    """Inner body of handler() — runs one transform invocation. Extracted so
    handler() can wrap it in timing + node_runs telemetry without nesting
    the transform logic in a try/finally that obscures the data path.

    Event shape (v0.12+):
      {
        "inputs":  {"alias": <descriptor>, ...},
        "outputs": {"key":   <descriptor>, ...}
      }

    Each descriptor is either:
      - string: an S3 URI / local path / Iceberg table id (existing semantics).
      - dict (input):  {"kind": "partitioned_path", "path": "s3://...",
                        "partitions": [...], "start_from": "..."}
        Runner walks the partition tree, filters by stored watermark, reads
        only new partitions, and advances the watermark on success.
      - dict (output): {"kind": "iceberg_table", "table_id": "<id>"|"",
                        "mode": "replace"|"append"}
        Mode "append" switches the writer from createOrReplace to append.

    Output routing (per ADR-013):
      - Path (contains "/"): plain Parquet at the destination.
      - Empty / identifier:  Iceberg table at `clavesa.<glue_db>.<node>__<key>`,
        where `<glue_db>` follows the ADR-016 `_glue_db()` encoding of
        the (workspace_catalog, pipeline_schema) env pair.

    Skip semantics: when every partitioned input has zero new partitions, the
    handler returns {"status": "skipped"} without invoking the user transform
    or touching outputs. Watermarks unchanged.
    """
    inputs = event.get("inputs", {}) or {}
    outputs = event.get("outputs", {}) or {}
    language = os.environ.get("CLAVESA_LANGUAGE", "sql")
    logic_path = os.environ.get("CLAVESA_LOGIC_S3_PATH")
    if not logic_path:
        raise RuntimeError("CLAVESA_LOGIC_S3_PATH is not set")

    # Backfill mode (Gate 1). When `_backfill.node` matches this Lambda's
    # CLAVESA_NODE, swap the cursor window + redirect outputs to the
    # staging table. Non-target nodes in the same SFN execution see
    # `_backfill.node` mismatch and skip — backfill is per-node by design,
    # not per-pipeline. `_backfill.target_outputs` (a map of output_key →
    # staging table id) overrides the canonical target per output; mode
    # is forced to "replace" so re-runs of the same backfill rewrite the
    # staging cleanly. Watermarks are neither read nor advanced.
    backfill_full = event.get("_backfill")
    my_node = os.environ.get("CLAVESA_NODE", "")
    backfill: dict[str, Any] | None = None
    if isinstance(backfill_full, dict):
        if backfill_full.get("node") == my_node:
            backfill = backfill_full
        else:
            return {"status": "skipped", "reason": f"backfill targets {backfill_full.get('node')!r}, this node is {my_node!r}"}

    # Provenance stamped into every Iceberg snapshot this run writes. A
    # backfill is self-evident from the `_backfill` block; other runs carry a
    # `_trigger` value set by whichever path started them — `manual` (CLI /
    # ad-hoc), `event` (SQS poller), `scheduled` (EventBridge rule). Cloud
    # runs receive the SFN execution input under `_execution_input`; local
    # runs get `_trigger` on the event directly.
    if backfill is not None:
        trigger = "backfill"
    else:
        exec_input = event.get("_execution_input")
        trigger = (
            event.get("_trigger")
            or (exec_input.get("_trigger") if isinstance(exec_input, dict) else None)
            or ""
        )
    snapshot_props = {"clavesa.trigger": trigger, "clavesa.run-id": run_id}

    logic = _read_text(logic_path)
    spark = _spark()

    # Resolve inputs. Track watermark advances to commit after outputs succeed.
    input_dfs: dict[str, Any] = {}
    pending_watermarks: list[dict[str, Any]] = []
    saw_partitioned = False
    for alias, src in inputs.items():
        if isinstance(src, dict) and src.get("kind") == "partitioned_path":
            saw_partitioned = True
        df, advance = _resolve_input(spark, alias, src, backfill=backfill)
        if df is None:
            # Empty partitioned input — skip the entire run.
            return {"status": "skipped", "reason": f"input {alias!r} has no new partitions"}
        df.createOrReplaceTempView(alias)
        input_dfs[alias] = df
        if advance is not None:
            pending_watermarks.append(advance)

    # Run the transform.
    if language == "sql":
        result = {"default": spark.sql(logic)}
    elif language == "python":
        mod = _load_script(logic)
        if not hasattr(mod, "transform"):
            raise RuntimeError(
                "User script must define a top-level 'transform(spark, inputs)' function."
            )
        raw = mod.transform(spark, input_dfs)
        if not isinstance(raw, dict):
            raise TypeError(f"transform() must return a dict, got {type(raw)}")
        result = raw
    else:
        raise RuntimeError(f"unsupported CLAVESA_LANGUAGE: {language!r}")

    # Write outputs.
    written: dict[str, str] = {}
    backfill_targets = (backfill or {}).get("target_outputs", {}) if backfill else {}
    for key, df in result.items():
        spec = _resolve_output(key, outputs.get(key, ""))
        if backfill and key in backfill_targets:
            # Redirect this output to its staging table; always replace so
            # backfill retries rewrite the staging cleanly.
            spec = {**spec, "kind": "iceberg_table", "target": backfill_targets[key],
                    "mode": "replace", "merge_keys": []}
        if spec["kind"] == "path":
            df.write.mode("overwrite").parquet(spec["target"])
        else:
            table_id = spec["target"]
            db_part = table_id.rsplit(".", 1)[0]
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {db_part}")
            if spec["mode"] == "merge":
                # First run: no target yet, MERGE has nothing to match
                # against. Create the table and skip MERGE for this run.
                if not spark.catalog.tableExists(table_id):
                    _apply_snapshot_props(df.writeTo(table_id), snapshot_props).create()
                else:
                    staging = f"__merge_src_{key}"
                    df.createOrReplaceTempView(staging)
                    on_clause = " AND ".join(
                        f"t.{col} = s.{col}" for col in spec["merge_keys"]
                    )
                    spark.sql(
                        f"MERGE INTO {table_id} t USING {staging} s "
                        f"ON {on_clause} "
                        f"WHEN MATCHED THEN UPDATE SET * "
                        f"WHEN NOT MATCHED THEN INSERT *"
                    )
            else:
                writer = _apply_snapshot_props(df.writeTo(table_id), snapshot_props)
                if spec["mode"] == "append":
                    # Iceberg's .append() fails on a non-existent table; .create()
                    # fails when one exists. Branch on whether the table is already
                    # registered in the catalog.
                    if spark.catalog.tableExists(table_id):
                        writer.append()
                    else:
                        writer.create()
                else:
                    writer.createOrReplace()
        written[key] = spec["target"]

        # Per-output opt-in column statistics (v0.24+). Computed off the
        # source DataFrame, which is exactly the post-commit table state
        # for replace-mode outputs and the rows this run contributed for
        # append/merge — best-effort; a stats-write failure logs but does
        # not mask the transform outcome.
        if spec.get("stats") and spec["kind"] == "iceberg_table" and not (backfill and key in backfill_targets):
            try:
                _emit_column_stats(
                    spark=spark,
                    df=df,
                    run_id=run_id,
                    output_key=key,
                    table_identifier=spec["target"],
                )
            except Exception as stats_exc:  # noqa: BLE001
                print(
                    f"[clavesa] column_stats write failed for {spec['target']!r}: {stats_exc!r}",
                    file=sys.stderr,
                )

    # Advance watermarks. Best-effort atomicity: outputs committed first, so a
    # failure here causes the next run to reprocess the same partitions
    # (at-least-once on the input side). Mode="replace" outputs absorb the
    # duplicate; "append" outputs would dupe rows. Document, don't solve.
    for adv in pending_watermarks:
        _write_watermark(adv["uri"], adv["new_cursor"])

    response: dict[str, Any] = {"status": "ok", "outputs": written}
    if saw_partitioned:
        response["watermarks_advanced"] = [
            {"uri": adv["uri"], "cursor": list(adv["new_cursor"])}
            for adv in pending_watermarks
        ]
    return response


# ---------------------------------------------------------------------------
# node_runs metadata capture (option A in TODO bucket A: runner self-reports
# at end of every invocation). Failures here never mask the original outcome —
# we'd rather lose a metadata row than fail a successful transform run.
# ---------------------------------------------------------------------------


def _build_node_run_row(
    *,
    run_id: str,
    started_ms: int,
    ended_ms: int,
    status: str,
    error_class: str | None,
    error_msg: str | None,
    cold_start: bool,
    context: Any,
    sf_execution_arn: str | None = None,
    env: dict[str, str] | None = None,
    output_rows: int | None = None,
) -> dict[str, Any]:
    """Pure helper: construct one node_runs row from invocation telemetry.

    Kept side-effect-free so it's unit-testable without Spark. ``env``
    defaults to ``os.environ`` but tests pass a dict to assert behavior
    around missing values.

    ``sf_execution_arn`` comes from the orchestration's SFN payload —
    when present, the row joins to the EventBridge-populated runs table
    on this column. Falls back to empty string when invoked outside of
    Step Functions (local CLI runs, ad-hoc Lambda invocations).
    """
    env_map: dict[str, str] = dict(os.environ if env is None else env)

    pipeline = env_map.get("CLAVESA_PIPELINE", "default")
    node = env_map.get("CLAVESA_NODE", "node")
    compute_target = (
        "lambda" if env_map.get("AWS_LAMBDA_FUNCTION_NAME") else "local"
    )

    memory_mb: int | None = None
    request_id: str | None = None
    if context is not None:
        raw = getattr(context, "memory_limit_in_mb", None)
        if raw is not None:
            try:
                memory_mb = int(raw)
            except (TypeError, ValueError):
                memory_mb = None
        rid = getattr(context, "aws_request_id", None)
        if rid:
            request_id = str(rid)

    started_iso = _dt.datetime.fromtimestamp(
        started_ms / 1000, tz=_dt.timezone.utc
    )
    ended_iso = _dt.datetime.fromtimestamp(
        ended_ms / 1000, tz=_dt.timezone.utc
    )

    # Truncate long error messages so a runaway traceback doesn't blow up
    # the Iceberg manifest. 4 KB matches the typical Lambda log truncation
    # threshold and is plenty for the message itself; full traces are still
    # in CloudWatch.
    if error_msg is not None and len(error_msg) > 4096:
        error_msg = error_msg[:4093] + "..."

    # Triage metadata: which build of the runner produced this row, and
    # which `?ref=vX.Y.Z` was the orchestration module pinned to. Both come
    # from env (set by the Lambda function in cloud, by service.RunPipeline
    # in local). Empty strings when unset — older runners that didn't pass
    # the env still join cleanly via the schema-evolution writer option.
    runner_image_digest = env_map.get("CLAVESA_RUNNER_IMAGE_DIGEST", "")
    module_version = env_map.get("CLAVESA_MODULE_VERSION", "")

    return {
        "run_id": run_id,
        "pipeline": pipeline,
        "node": node,
        "started_at": started_iso,
        "ended_at": ended_iso,
        "duration_ms": max(ended_ms - started_ms, 0),
        "status": status,
        "compute_target": compute_target,
        "memory_mb": memory_mb,
        "cold_start": cold_start,
        "lambda_request_id": request_id,
        "sf_execution_arn": sf_execution_arn or "",
        "error_class": error_class,
        "error_msg": error_msg,
        "runner_image_digest": runner_image_digest,
        "module_version": module_version,
        # Sum of added-records across every Iceberg-mode output for this
        # invocation. Sourced from each output's snapshot summary at write
        # time — no extra .count() scan, just a manifest read. None when
        # the run had no Iceberg outputs (path-mode destinations, skipped
        # runs) or when summary capture failed.
        "output_rows": output_rows,
    }


def _system_table_location(table_name: str) -> str | None:
    """Absolute S3 LOCATION for a workspace system-catalog table.

    Cloud only: CLAVESA_SYSTEM_WAREHOUSE is set by the transform
    module to `s3://<bucket>/_system/pipelines/`. Returning a concrete
    location at create time pins `node_runs` / `tables` to the workspace-
    shared prefix rather than letting GlueCatalog default them under the
    invoking pipeline's per-pipeline `_warehouse/`. Returns None for
    local/preview (HadoopCatalog default is fine — single warehouse).
    """
    base = os.environ.get("CLAVESA_SYSTEM_WAREHOUSE")
    if not base:
        return None
    return base.rstrip("/") + "/" + table_name + "/"


def _node_runs_table_id() -> str:
    """clavesa.<system_glue_db>.node_runs — workspace-wide observability
    table (ADR-016 "Workspace system catalog", v0.20.0). Every pipeline
    appends here; the `pipeline` column distinguishes rows. Pre-v0.20
    runs landed in the per-pipeline `<glue_db>.node_runs` instead.
    """
    return f"clavesa.{_system_glue_db()}.node_runs"


def _node_runs_schema():
    """Explicit Iceberg schema. Without this, createDataFrame infers types
    from the first row — ``error_msg=None`` would be inferred as void and
    refuse subsequent string values, and Iceberg refuses to evolve void→
    string in the same commit.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline", StringType(), False),
        StructField("node", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("status", StringType(), False),
        StructField("compute_target", StringType(), True),
        StructField("memory_mb", IntegerType(), True),
        StructField("cold_start", BooleanType(), True),
        StructField("lambda_request_id", StringType(), True),
        # Joins to <pipeline>.runs.sf_execution_arn (EventBridge writer).
        # Empty string when the runner was invoked outside of Step Functions.
        StructField("sf_execution_arn", StringType(), True),
        StructField("error_class", StringType(), True),
        StructField("error_msg", StringType(), True),
        # Triage columns added in the bucket-A item-1 slice. Both nullable
        # so older tables that haven't evolved yet still satisfy the
        # writer's schema-merge contract.
        StructField("runner_image_digest", StringType(), True),
        StructField("module_version", StringType(), True),
        # Output rowcount summed across all Iceberg-mode outputs for this
        # invocation. Nullable: path-mode-only runs and skipped runs leave
        # it null; older tables not carrying the column read as null too.
        StructField("output_rows", LongType(), True),
    ])


def _record_node_run(row: dict[str, Any]) -> None:
    """Append one row to the pipeline's node_runs Iceberg table.

    Creates the namespace and table on first call; appends thereafter.
    `mergeSchema=true` lets us evolve the schema forward (added columns
    backfill as null on older snapshots) without an explicit ALTER TABLE
    when the runner adds columns between releases.
    """
    spark = _spark()
    table_id = _node_runs_table_id()
    db_part = table_id.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {db_part}")

    df = spark.createDataFrame([row], schema=_node_runs_schema())
    writer = df.writeTo(table_id).option("mergeSchema", "true")
    if spark.catalog.tableExists(table_id):
        writer.append()
    else:
        location = _system_table_location("node_runs")
        if location:
            writer = writer.tableProperty("location", location)
        writer.create()


def _runs_table_id() -> str:
    """clavesa.<system_glue_db>.runs — workspace-wide rollup table.
    Every pipeline's executions land here; `pipeline` column filters.

    Cloud writes go through the runs_writer Lambda
    (`modules/orchestration/aws/runs_writer/`), which is also pointed at
    the workspace system DB as of v0.20.0. Local has no SFN, so
    service.RunPipeline drives the same write through this runner mode.
    Schema and column order must stay in lockstep with the cloud writer
    or LocalProvider.Runs() will project mismatched values.
    """
    return f"clavesa.{_system_glue_db()}.runs"


def _runs_schema():
    """Explicit Iceberg schema mirroring runs_writer/index.py::_ensure_table.
    Same NOT-NULL discipline as _node_runs_schema for the same reason: an
    inferred void column blocks subsequent string appends.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline", StringType(), False),
        StructField("sf_execution_arn", StringType(), True),
        StructField("status", StringType(), False),
        StructField("trigger", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("failed_step", StringType(), True),
        StructField("error_class", StringType(), True),
        StructField("error_msg", StringType(), True),
    ])


def _record_run(row: dict[str, Any]) -> None:
    """Append one row to the pipeline's runs Iceberg table.

    Same create-or-append branching as _record_node_run.
    """
    spark = _spark()
    table_id = _runs_table_id()
    db_part = table_id.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {db_part}")

    df = spark.createDataFrame([row], schema=_runs_schema())
    writer = df.writeTo(table_id).option("mergeSchema", "true")
    if spark.catalog.tableExists(table_id):
        writer.append()
    else:
        writer.create()


def _tables_table_id() -> str:
    """clavesa.<system_glue_db>.tables — workspace-wide denormalized
    snapshot metadata across every Iceberg output produced by every
    pipeline. Lives in the system catalog's `pipelines` schema alongside
    runs / node_runs (ADR-016 v0.20.0); `pipeline` column filters per
    pipeline.
    """
    return f"clavesa.{_system_glue_db()}.tables"


def _tables_schema():
    """One row per Iceberg-output write. Append-only history — query
    `MAX(snapshot_ts) GROUP BY table_id` for current-state.

    Mirrors the shape Databricks exposes via `system.information_schema`
    + `<table>.snapshots`, but flattened across an entire pipeline so a
    single SELECT answers "what tables does this pipeline produce, and
    when were they last refreshed?".
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("pipeline", StringType(), False),
        StructField("node", StringType(), False),
        StructField("output_key", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("table_id", StringType(), False),
        StructField("snapshot_id", LongType(), True),
        StructField("snapshot_ts", TimestampType(), True),
        StructField("row_count", LongType(), True),
        StructField("file_count", IntegerType(), True),
        StructField("total_bytes", LongType(), True),
        # Joins to node_runs.run_id so users can answer "which invocation
        # produced this snapshot?" without scanning logs.
        StructField("last_writer_run_id", StringType(), True),
    ])


def _record_table_state(run_id: str, output_key: str, table_id: str) -> int | None:
    """Append one row to <pipeline>.tables describing the latest snapshot
    of `table_id`. Called after every Iceberg-mode output write.

    Reads Iceberg's metadata-only `<table>.snapshots` view to source
    snapshot_id / committed_at / summary — no data-file scan, just a
    manifest read. Cheap.

    Returns the latest snapshot's added-records count (or None if
    summary didn't carry it) so the caller can accumulate output_rows
    across all outputs for the node_runs row. Returning None on early
    exit (no snapshots yet) keeps the caller's accumulation simple.

    Best-effort: a write failure logs to stderr and returns. Losing a
    metadata row is strictly better than failing a transform whose data
    already committed.
    """
    spark = _spark()

    # Iceberg's <table>.snapshots is its own metadata namespace; quote
    # to avoid confusion with the literal 'snapshots' table name.
    rows = spark.sql(
        f"SELECT snapshot_id, committed_at, summary "
        f"FROM {table_id}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not rows:
        return

    snap = rows[0]
    summary = snap["summary"] or {}

    def _int_or_none(key: str) -> int | None:
        raw = summary.get(key)
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    pipeline = os.environ.get("CLAVESA_PIPELINE", "default")
    node = os.environ.get("CLAVESA_NODE", "node")
    table_name = table_id.rsplit(".", 1)[-1]

    file_count_raw = _int_or_none("total-data-files")
    row = {
        "pipeline": pipeline,
        "node": node,
        "output_key": output_key,
        "table_name": table_name,
        "table_id": table_id,
        "snapshot_id": int(snap["snapshot_id"]),
        "snapshot_ts": snap["committed_at"],
        "row_count": _int_or_none("total-records"),
        "file_count": file_count_raw,
        "total_bytes": _int_or_none("total-files-size"),
        "last_writer_run_id": run_id,
    }

    tables_id = _tables_table_id()
    db_part = tables_id.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {db_part}")

    df = spark.createDataFrame([row], schema=_tables_schema())
    writer = df.writeTo(tables_id).option("mergeSchema", "true")
    if spark.catalog.tableExists(tables_id):
        writer.append()
    else:
        location = _system_table_location("tables")
        if location:
            writer = writer.tableProperty("location", location)
        writer.create()

    # Net rows this commit contributed = added-records - deleted-records.
    #
    # Why the subtraction: Iceberg's `added-records` counts rows in the
    # new data files. For an append that's literally "new rows". For a
    # COW merge, Iceberg rewrites entire data files in place — so
    # `added-records` = "rows in the rewritten files" even when none of
    # them actually changed value. An idempotent merge over a full-read
    # dim_* transform (read 1k rows of enriched, GROUP BY → 80 distinct
    # keys, MERGE into a dim with the same 80 keys) reports
    # added=80/deleted=80, which used to surface as "80 rows written"
    # despite the table being byte-identical afterwards.
    #
    # `added - deleted` collapses both cases honestly: append gives
    # N - 0 = N; pure-update merge gives N - N = 0; insert-of-K into
    # existing-N gives (N+K) - N = K. Replace (full overwrite) can
    # come back negative when the new snapshot has fewer rows than the
    # old; we clamp to >= 0 there since "Rows written: -3" is more
    # confusing than informative.
    added = _int_or_none("added-records")
    deleted = _int_or_none("deleted-records")
    if added is None:
        return None
    net = added - (deleted or 0)
    return net if net > 0 else 0


# ---------------------------------------------------------------------------
# column_stats — opt-in per-column profile (null %, distinct count, top-K,
# min/max, percentiles) computed off the source DataFrame at write time and
# appended to the workspace system catalog. Drives the catalog page's "is
# this column worth graphing?" affordance.
# ---------------------------------------------------------------------------

# High-cardinality cutoff for top-K. Above this distinct-value count, the
# top-10 list is noise: the long tail dwarfs each bucket and the runner
# pays for an extra group-by per column. The UI shows a "high cardinality"
# badge instead and the row's top_10 stays empty.
_COLUMN_STATS_TOPK_CUTOFF = 1000
_COLUMN_STATS_TOPK_LIMIT = 10
# Cap the per-output top-K compute to bound cost on very wide tables. The
# row's top_10 is left empty (not the same as "skipped due to cardinality")
# for any column past this cap — the UI doesn't distinguish today, the
# distinction matters when wide-table billing data shows up.
_COLUMN_STATS_TOPK_MAX_COLUMNS = 20


def _column_stats_table_id() -> str:
    """clavesa.<system_glue_db>.column_stats — workspace-wide opt-in
    column profile table (v0.24+). Multi-writer; `pipeline` / `node` /
    `output_key` columns scope each row, and `snapshot_id` joins to the
    Iceberg snapshot the stats describe. Lives alongside node_runs /
    runs / tables in the system catalog's `pipelines` schema (ADR-016).
    """
    return f"clavesa.{_system_glue_db()}.column_stats"


def _column_stats_schema():
    """Explicit Iceberg schema. Uniform across column types (min/max
    stringified, percentiles as nullable doubles, top_10 as a nested
    array<struct>) so a single schema serves every dtype the runner
    might profile.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        ArrayType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    top_struct = StructType([
        StructField("value", StringType(), True),
        StructField("count", LongType(), True),
    ])
    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline", StringType(), False),
        StructField("node", StringType(), False),
        StructField("output_key", StringType(), False),
        StructField("table_identifier", StringType(), False),
        StructField("snapshot_id", LongType(), True),
        StructField("snapshot_ts", TimestampType(), True),
        StructField("column_name", StringType(), False),
        StructField("column_type", StringType(), False),
        StructField("row_count", LongType(), True),
        StructField("null_count", LongType(), True),
        StructField("null_pct", DoubleType(), True),
        StructField("approx_count_distinct", LongType(), True),
        StructField("min_value", StringType(), True),
        StructField("max_value", StringType(), True),
        StructField("approx_p50", DoubleType(), True),
        StructField("approx_p95", DoubleType(), True),
        StructField("top_10", ArrayType(top_struct), True),
        StructField("computed_at", TimestampType(), False),
    ])


def _is_numeric_type(spark_type) -> bool:
    """approx_percentile + numeric min/max only make sense on numeric
    types. Decimal counts (Iceberg / Spark store amounts as DecimalType
    routinely)."""
    from pyspark.sql.types import (  # noqa: PLC0415
        ByteType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
    )

    return isinstance(
        spark_type,
        (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType),
    )


def _read_latest_snapshot(spark, table_identifier):
    """Latest (snapshot_id, committed_at) from <table>.snapshots, or
    (None, None) if no snapshots are visible yet — defensive: the write
    we just performed should always produce one, but a brand-new table
    where the catalog hasn't refreshed gets the safe (None, None)."""
    try:
        rows = spark.sql(
            f"SELECT snapshot_id, committed_at "
            f"FROM {table_identifier}.snapshots "
            f"ORDER BY committed_at DESC LIMIT 1"
        ).collect()
    except Exception:  # noqa: BLE001
        return None, None
    if not rows:
        return None, None
    return int(rows[0]["snapshot_id"]), rows[0]["committed_at"]


def _emit_column_stats(*, spark, df, run_id, output_key, table_identifier):
    """Compute per-column stats off `df` and append one row per column to
    the workspace system column_stats table. Caller catches exceptions —
    best-effort, see _run_transform's wrapper.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415
    from pyspark.sql.types import StringType  # noqa: PLC0415

    columns = list(df.schema.fields)
    if not columns:
        return

    # Single aggregation pass: row_count + per-column null_count,
    # approx_count_distinct, min, max, and percentiles for numerics.
    aggs = [F.count(F.lit(1)).alias("__row_count")]
    for f in columns:
        aggs.append(F.sum(F.col(f.name).isNull().cast("long")).alias(f"{f.name}__nulls"))
        aggs.append(F.approx_count_distinct(F.col(f.name)).alias(f"{f.name}__distinct"))
        # min/max over the underlying type so numeric ordering is
        # preserved (cast-then-min would lex-sort: "99" > "499"),
        # then string-cast the *result* for uniform wire shape.
        aggs.append(F.min(F.col(f.name)).cast(StringType()).alias(f"{f.name}__min"))
        aggs.append(F.max(F.col(f.name)).cast(StringType()).alias(f"{f.name}__max"))
        if _is_numeric_type(f.dataType):
            aggs.append(
                F.percentile_approx(F.col(f.name).cast("double"), [0.5, 0.95]).alias(
                    f"{f.name}__pcts"
                )
            )

    agg_row = df.agg(*aggs).collect()[0]
    row_count = int(agg_row["__row_count"]) if agg_row["__row_count"] is not None else 0

    # Per-column top-K, capped to keep the cost bounded on wide tables.
    # Only fire for columns under the cardinality cutoff — above it the
    # tail dominates and the result reads as noise.
    top_k_by_col: dict[str, list[dict]] = {}
    top_k_columns_used = 0
    for f in columns:
        if top_k_columns_used >= _COLUMN_STATS_TOPK_MAX_COLUMNS:
            break
        distinct = agg_row[f"{f.name}__distinct"]
        if distinct is None or distinct == 0 or distinct > _COLUMN_STATS_TOPK_CUTOFF:
            continue
        try:
            rows = (
                df.groupBy(F.col(f.name).cast(StringType()).alias("value"))
                .count()
                .orderBy(F.col("count").desc())
                .limit(_COLUMN_STATS_TOPK_LIMIT)
                .collect()
            )
        except Exception:  # noqa: BLE001
            # Best-effort per column — keep going so the rest of the
            # stats land even if one column's top-K errors out.
            continue
        top_k_by_col[f.name] = [
            {"value": r["value"], "count": int(r["count"])} for r in rows
        ]
        top_k_columns_used += 1

    snapshot_id, snapshot_ts = _read_latest_snapshot(spark, table_identifier)
    pipeline = os.environ.get("CLAVESA_PIPELINE", "default")
    node = os.environ.get("CLAVESA_NODE", "node")
    computed_at = _dt.datetime.now(tz=_dt.timezone.utc)

    rows_out = []
    for f in columns:
        null_count = agg_row[f"{f.name}__nulls"]
        null_count_i = int(null_count) if null_count is not None else None
        null_pct = None
        if null_count_i is not None and row_count > 0:
            null_pct = float(null_count_i) / float(row_count)
        elif row_count == 0:
            null_pct = 0.0
        distinct = agg_row[f"{f.name}__distinct"]
        p50 = p95 = None
        if _is_numeric_type(f.dataType):
            pcts = agg_row[f"{f.name}__pcts"]
            if pcts is not None and len(pcts) >= 2:
                p50 = float(pcts[0]) if pcts[0] is not None else None
                p95 = float(pcts[1]) if pcts[1] is not None else None
        rows_out.append({
            "run_id": run_id,
            "pipeline": pipeline,
            "node": node,
            "output_key": output_key,
            "table_identifier": table_identifier,
            "snapshot_id": snapshot_id,
            "snapshot_ts": snapshot_ts,
            "column_name": f.name,
            "column_type": f.dataType.simpleString(),
            "row_count": row_count,
            "null_count": null_count_i,
            "null_pct": null_pct,
            "approx_count_distinct": int(distinct) if distinct is not None else None,
            "min_value": agg_row[f"{f.name}__min"],
            "max_value": agg_row[f"{f.name}__max"],
            "approx_p50": p50,
            "approx_p95": p95,
            "top_10": top_k_by_col.get(f.name),
            "computed_at": computed_at,
        })

    _record_column_stats(rows_out)


def _record_column_stats(rows):
    """Append per-column stats rows to the system column_stats Iceberg
    table. Same create-or-append branching pattern as _record_node_run
    and _record_table_state, including the `_system_table_location`
    override that pins the table to the workspace-shared `_system/`
    prefix in cloud warehouses.
    """
    if not rows:
        return
    spark = _spark()
    table_id = _column_stats_table_id()
    db_part = table_id.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {db_part}")

    df = spark.createDataFrame(rows, schema=_column_stats_schema())
    writer = df.writeTo(table_id).option("mergeSchema", "true")
    if spark.catalog.tableExists(table_id):
        writer.append()
    else:
        location = _system_table_location("column_stats")
        if location:
            writer = writer.tableProperty("location", location)
        writer.create()


def run_record_run() -> None:
    """CLAVESA_RECORD_RUN=1 mode — read one row's fields from stdin JSON,
    append to <pipeline>.runs.

    Stdin shape (timestamps as Unix millis so we don't fight ISO parsing
    across Go/Python nanosecond precision):
        {
          "run_id": "...", "pipeline": "...", "sf_execution_arn": "",
          "status": "SUCCEEDED" | "FAILED",
          "trigger": "manual",
          "started_at_ms": 173..., "ended_at_ms": 173...,
          "duration_ms": 1234,
          "failed_step": "", "error_class": "", "error_msg": ""
        }
    """
    payload = json.loads(sys.stdin.read())

    started_ms = int(payload["started_at_ms"])
    ended_ms_raw = payload.get("ended_at_ms")
    ended_dt = None
    if ended_ms_raw is not None:
        ended_dt = _dt.datetime.fromtimestamp(int(ended_ms_raw) / 1000, tz=_dt.timezone.utc)

    error_msg = payload.get("error_msg") or None
    if error_msg is not None and len(error_msg) > 4096:
        error_msg = error_msg[:4093] + "..."

    row = {
        "run_id": str(payload["run_id"]),
        "pipeline": str(payload["pipeline"]),
        "sf_execution_arn": payload.get("sf_execution_arn") or "",
        "status": str(payload["status"]),
        "trigger": payload.get("trigger") or "",
        "target_table": payload.get("target_table") or None,
        "started_at": _dt.datetime.fromtimestamp(started_ms / 1000, tz=_dt.timezone.utc),
        "ended_at": ended_dt,
        "duration_ms": int(payload["duration_ms"]) if payload.get("duration_ms") is not None else None,
        "failed_step": payload.get("failed_step") or "",
        "error_class": payload.get("error_class") or "",
        "error_msg": error_msg,
    }
    _record_run(row)

    # Backfill node_runs rows for cascade-skipped nodes — service.RunPipeline
    # bypasses the runner for these so they wouldn't otherwise appear in the
    # Runs grid. Sharing this invocation's Spark session keeps the cost ~0s
    # vs invoking the runner once per skipped node (Spark startup × N).
    cascade = payload.get("cascade_skipped_nodes") or []
    if cascade:
        run_id_str = str(payload["run_id"])
        pipeline_name = str(payload["pipeline"])
        sf_arn = payload.get("sf_execution_arn") or ""
        runner_image_digest = os.environ.get("CLAVESA_RUNNER_IMAGE_DIGEST", "")
        module_version = os.environ.get("CLAVESA_MODULE_VERSION", "")
        for entry in cascade:
            entry_started_ms = int(entry.get("started_at_ms") or started_ms)
            entry_ended_ms = int(entry.get("ended_at_ms") or entry_started_ms)
            _record_node_run({
                "run_id": run_id_str,
                "pipeline": pipeline_name,
                "node": str(entry["node"]),
                "started_at": _dt.datetime.fromtimestamp(entry_started_ms / 1000, tz=_dt.timezone.utc),
                "ended_at": _dt.datetime.fromtimestamp(entry_ended_ms / 1000, tz=_dt.timezone.utc),
                "duration_ms": max(entry_ended_ms - entry_started_ms, 0),
                "status": "skipped",
                "compute_target": "local",
                "memory_mb": None,
                "cold_start": None,
                "lambda_request_id": None,
                "sf_execution_arn": sf_arn,
                "error_class": "",
                "error_msg": entry.get("reason") or "all upstreams skipped",
                "runner_image_digest": runner_image_digest,
                "module_version": module_version,
                "output_rows": None,
            })

    print(json.dumps({"status": "ok"}), flush=True)


def handler(event, context):
    """Lambda / local production entry point.

    Wraps ``_run_transform`` with timing and node_runs metadata capture.
    The metadata write is best-effort — a failed write logs to stderr but
    does not affect the transform outcome (or its exception, if any).

    Operation kinds (Gate 1): when ``event["_operation"]`` is set, the
    Lambda performs a backfill control-plane action (promote, discard)
    against Iceberg via SparkSQL instead of running the user transform.
    The runner image already carries Spark + Iceberg + the IAM scope to
    read/write any table in the workspace catalog, so we route these
    through the same Lambda rather than introducing a parallel ops
    Lambda or doing the write from Athena (Athena's MERGE syntax
    requires column enumeration and lacks `SET *`; SparkSQL accepts both).
    """
    if isinstance(event, dict) and event.get("_operation"):
        return _run_operation(event)

    started_ms = int(time.time() * 1000)
    is_cold_start = _SPARK is None
    run_id = uuid.uuid4().hex
    # Threaded by the orchestration emitter (v0.13+) — empty string for
    # runs outside Step Functions (local CLI, ad-hoc invocations).
    sf_execution_arn = ""
    if isinstance(event, dict):
        raw_arn = event.get("_sf_execution_arn", "")
        if raw_arn:
            sf_execution_arn = str(raw_arn)
    status = "ok"
    error_class: str | None = None
    error_msg: str | None = None
    # Sum of added-records across this run's Iceberg outputs. Stays None
    # for path-mode-only runs and skipped runs (the column should be
    # NULL, not 0, so analytics queries can distinguish "0 rows produced"
    # from "no Iceberg outputs").
    output_rows_total: int | None = None

    try:
        response = _run_transform(event, context, run_id=run_id)
        if isinstance(response, dict) and response.get("status") == "skipped":
            status = "skipped"
        # Record one row per Iceberg-mode output into <pipeline>.tables so
        # the catalog page can show row count + last refresh per table
        # without each viewer re-querying every transform's snapshots view.
        # Path-mode outputs (plain Parquet at a destination override) are
        # skipped — they don't have an Iceberg snapshot to read from.
        if isinstance(response, dict) and isinstance(response.get("outputs"), dict):
            for output_key, target in response["outputs"].items():
                if not isinstance(target, str):
                    continue
                # Iceberg table identifiers are dotted (catalog.db.table);
                # path-mode targets are filesystem/S3 paths and skipped here.
                if "/" in target or "\\" in target:
                    continue
                if target.count(".") < 2:
                    continue
                try:
                    added = _record_table_state(run_id, output_key, target)
                    if added is not None:
                        output_rows_total = (output_rows_total or 0) + added
                except Exception as table_exc:  # noqa: BLE001
                    print(
                        f"[clavesa] tables row write failed for {target!r}: {table_exc!r}",
                        file=sys.stderr,
                    )
        # Surface output_rows on the response so the Go orchestrator can
        # stamp it onto state.json (and the dashboard's node-detail
        # drawer can read it without spinning Spark up to query the
        # Iceberg node_runs table). None when the transform wrote no
        # Iceberg outputs — path-mode-only writes and skipped runs both
        # land here.
        if isinstance(response, dict) and output_rows_total is not None:
            response["output_rows"] = output_rows_total
        return response
    except Exception as exc:
        status = "failed"
        error_class = type(exc).__name__
        # str(exc) carries the human-readable message — for PySpark
        # exceptions (AnalysisException &c.) repr(exc) is just
        # "AnalysisException()" with the message dropped. Fall back to
        # repr only when str is empty.
        error_msg = str(exc) or repr(exc)
        raise
    finally:
        ended_ms = int(time.time() * 1000)
        try:
            row = _build_node_run_row(
                run_id=run_id,
                started_ms=started_ms,
                ended_ms=ended_ms,
                status=status,
                error_class=error_class,
                error_msg=error_msg,
                cold_start=is_cold_start,
                context=context,
                sf_execution_arn=sf_execution_arn,
                output_rows=output_rows_total,
            )
            _record_node_run(row)
        except Exception as record_exc:  # noqa: BLE001
            print(
                f"[clavesa] node_runs write failed: {record_exc!r}",
                file=sys.stderr,
            )


def run_local() -> None:
    """CLAVESA_RUN=1 mode — read event JSON from stdin, invoke handler, print result.

    Used by `clavesa pipeline run` to drive transforms via the same handler
    that backs Lambda. The event shape is identical to the Lambda contract.
    """
    event = json.loads(sys.stdin.read())
    result = handler(event, None)
    print(json.dumps(result), flush=True)


# ---------------------------------------------------------------------------
# Query mode (CLAVESA_QUERY=1) — local-cloud parity per ADR-014
# ---------------------------------------------------------------------------


def run_query() -> None:
    """CLAVESA_QUERY=1 mode — run one Spark SQL statement, emit JSON to stdout.

    Used by the Go-side observability LocalProvider to read Iceberg-backed
    surfaces (node_runs, runs, snapshots, transform output tables) for
    compute = "local" pipelines. Cloud's CloudProvider goes through Athena
    for the same reads; this is the local equivalent at the same SQL surface.

    SQL is read from CLAVESA_SQL (env) or stdin when the env is empty —
    long queries with line breaks fit poorly in env vars.

    Output: {"columns": [name...], "column_types": [type...],
    "rows": [[v, v, ...], ...]} on success, or {"error": "..."} on failure
    (exit 1). column_types carries Spark's simpleString of each column's
    DataType so the cloud side's Athena type strings have a parity-matched
    counterpart in the SampleTable / Query response (ADR-014). Values are
    JSON-encoded primitives or stringified for non-JSON-native types
    (timestamps, decimals).
    """
    sql = os.environ.get("CLAVESA_SQL", "").strip()
    if not sql:
        sql = sys.stdin.read().strip()
    if not sql:
        print(json.dumps({"error": "no SQL provided (CLAVESA_SQL or stdin)"}), flush=True)
        sys.exit(1)

    spark = _spark()
    df = spark.sql(sql)

    columns = list(df.columns)
    column_types = [f.dataType.simpleString() for f in df.schema.fields]
    # Use Pandas as the JSON serialization shim so timestamps / decimals /
    # nested types format consistently across Spark versions. Pandas matches
    # how preview-mode emits rows, keeping the on-the-wire format aligned.
    pdf = df.toPandas()
    rows_records = json.loads(pdf.to_json(orient="records", date_format="iso"))
    rows = [[r.get(c) for c in columns] for r in rows_records]

    print(json.dumps({"columns": columns, "column_types": column_types, "rows": rows}), flush=True)


# ---------------------------------------------------------------------------
# Query-server mode (CLAVESA_QUERY_SERVER=1) — long-lived warm Spark
# ---------------------------------------------------------------------------


def _query_to_payload(sql: str) -> dict:
    """Run one SparkSQL statement, return the same shape CLAVESA_QUERY=1 emits."""
    df = _spark().sql(sql)
    columns = list(df.columns)
    column_types = [f.dataType.simpleString() for f in df.schema.fields]
    pdf = df.toPandas()
    records = json.loads(pdf.to_json(orient="records", date_format="iso"))
    rows = [[r.get(c) for c in columns] for r in records]
    return {"columns": columns, "column_types": column_types, "rows": rows}


def run_query_server() -> None:
    """CLAVESA_QUERY_SERVER=1 mode — long-lived warm Spark served over HTTP.

    Wired by `clavesa ui` so the Catalog/dashboard/TableDetail surfaces share
    one warm JVM instead of paying the ~18-30s cold start on every read.
    Single-threaded handler — Spark SQL is the bottleneck, threading buys
    nothing.

    Routes:
      GET  /healthz  → 200 once Spark is warm AND a probe SQL still
                       succeeds. 503 if Spark is gone (JVM crash, gateway
                       disconnect, OOM); Go-side persistent runner evicts
                       and respawns on 503.
      POST /query    → body: {"sql": "..."}. Returns the same JSON shape
                       CLAVESA_QUERY=1 emits, or {"error": "..."} on
                       failure (still 200 — the client checks the envelope,
                       matching CLAVESA_QUERY=1's behavior).
    """
    from http.server import BaseHTTPRequestHandler, HTTPServer  # noqa: PLC0415

    port = int(os.environ.get("CLAVESA_QUERY_SERVER_PORT", "8765"))

    # Eager warm — /healthz returning 200 implies the next /query call
    # won't pay JVM startup. The Go-side persistent runner polls /healthz
    # for up to 60s; once it sees 200 the warehouse is queryable at warm
    # speed.
    _spark()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                # Touch Spark for real — a SELECT 1 is cheap (~ms on a
                # warm JVM) but catches "container up, JVM dead" cases
                # where the HTTP server is alive but py4j has lost the
                # gateway. Without this probe Go never evicts because
                # the socket stays open.
                try:
                    _spark().sql("SELECT 1").collect()
                except Exception as exc:  # noqa: BLE001
                    self._json(503, {"error": f"spark dead: {exc}"})
                    return
                self._respond(200, b"ok", content_type="text/plain")
                return
            self._respond(404, b"", content_type="text/plain")

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/query":
                self._respond(404, b"", content_type="text/plain")
                return
            length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(length).decode("utf-8") if length else ""
            try:
                req = json.loads(body) if body else {}
                sql = (req.get("sql") or "").strip()
            except Exception as exc:  # noqa: BLE001
                self._json(400, {"error": f"bad request body: {exc}"})
                return
            if not sql:
                self._json(400, {"error": "missing sql"})
                return
            try:
                payload = _query_to_payload(sql)
            except Exception as exc:  # noqa: BLE001
                # Match CLAVESA_QUERY=1: error envelope, 200 status —
                # the client distinguishes success vs failure by inspecting
                # the JSON, not the HTTP code.
                self._json(200, {"error": str(exc)})
                return
            self._json(200, payload)

        def _json(self, code: int, payload: dict) -> None:
            data = json.dumps(payload).encode("utf-8")
            self._respond(code, data, content_type="application/json")

        def _respond(self, code: int, data: bytes, *, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        def log_message(self, fmt: str, *args) -> None:  # noqa: ARG002
            # Quiet — match the rest of the runner's stdout discipline.
            return

    HTTPServer(("0.0.0.0", port), Handler).serve_forever()


if os.environ.get("CLAVESA_PREVIEW") == "1":
    try:
        run_preview()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_RUN") == "1":
    try:
        run_local()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_QUERY") == "1":
    try:
        run_query()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_QUERY_SERVER") == "1":
    try:
        run_query_server()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
elif os.environ.get("CLAVESA_RECORD_RUN") == "1":
    try:
        run_record_run()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}), file=sys.stderr, flush=True)
        sys.exit(1)
