"""Databricks bundle tasks on the clavesa runner (ADR-031).

A bundle job runs as one clavesa pipeline whose nodes are *task* nodes: the
task's own code writes its tables, and clavesa applies no output mode. This
module runs one task in the runner's Spark session:

- spark_python_task: the file runs as __main__ with sys.argv from its
  parameters; SparkSession.builder.getOrCreate() returns the runner's
  session.
- sql_task with a file: the statements run one by one through spark.sql,
  with the parameters bound as named parameter markers (:name); the legacy
  {{name}} text substitution is applied first.
- notebook_task: the notebook was translated into a script at deploy
  (bundle_notebook.translate); it runs like a Python file, with spark,
  dbutils, display and displayHTML in its globals.

Around the task's code it provides `dbutils` (widgets, secrets, task values,
notebook.exit, fs) and resolves every table name the code reads or writes
(section 6): `catalog.schema.table` names go to Glue database
`<catalog>__<schema>` in `catalog: glue`.

Stdlib-only at import time: pyspark is imported inside the functions that
need it, so tests/runner/test_bundle_task.py runs with plain python3.
"""

from __future__ import annotations

import builtins
import contextlib
import datetime as _dt
import json
import os
import re
import runpy
import sys
import time
import types
from typing import Any

# The token the Go side puts where ${workspace.file_path} is (the folder
# that holds the bundle's files); the runner replaces it with the real
# folder of this run.
FILES_PLACEHOLDER = "/__clavesa_bundle_files__"


class TaskError(RuntimeError):
    """A task failed for a reason clavesa reports (not the user's code)."""


class NotebookExit(Exception):
    """dbutils.notebook.exit(value): ends the task with success."""

    def __init__(self, value):
        super().__init__(value)
        self.value = value


# ---------------------------------------------------------------------------
# SQL text: comments and string literals blanked, statements split.
# ---------------------------------------------------------------------------


def blank_sql(s: str) -> str:
    """Replace comments and the contents of string literals with spaces
    (newlines kept, quotes kept), so offsets into the result are offsets
    into the original."""
    out = list(s)
    i, n = 0, len(s)

    def blank(k):
        if out[k] != "\n":
            out[k] = " "

    while i < n:
        c = s[i]
        if c == "-" and s.startswith("--", i):
            while i < n and s[i] != "\n":
                blank(i)
                i += 1
            continue
        if c == "/" and s.startswith("/*", i):
            while i < n and not s.startswith("*/", i):
                blank(i)
                i += 1
            if i < n:
                blank(i)
                blank(i + 1)
                i += 2
            continue
        if c in "'\"":
            q = c
            i += 1
            while i < n:
                if s[i] == "\\" and i + 1 < n:
                    blank(i)
                    blank(i + 1)
                    i += 2
                    continue
                if s[i] == q:
                    if i + 1 < n and s[i + 1] == q:
                        blank(i)
                        blank(i + 1)
                        i += 2
                        continue
                    break
                blank(i)
                i += 1
            i += 1
            continue
        if c == "`":
            i += 1
            while i < n and s[i] != "`":
                i += 1
            i += 1
            continue
        i += 1
    return "".join(out)


def split_statements(text: str) -> list[str]:
    """Split SQL into statements at semicolons outside comments and string
    literals; statements that are only comments are dropped."""
    clean = blank_sql(text)
    out, start = [], 0
    for i in range(len(clean) + 1):
        if i == len(clean) or clean[i] == ";":
            if clean[start:i].strip():
                out.append(text[start:i].strip())
            start = i + 1
    return out


_MARKER_RE = re.compile(r"(?<![:\w]):([A-Za-z_]\w*)")


def used_markers(stmt: str) -> list[str]:
    """Named parameter markers (:name) used in a statement."""
    return _MARKER_RE.findall(blank_sql(stmt))


_MUSTACHE_RE = re.compile(r"\{\{\s*([A-Za-z_][\w.]*)\s*\}\}")


def sql_string(value: Any) -> str:
    """A SQL string literal for value: quotes and backslashes escaped."""
    v = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + v + "'"


def substitute_mustache(text: str, params: dict[str, str]) -> str:
    """The legacy {{name}} parameter syntax of a Databricks SQL file: the
    value goes in as a quoted string literal (Databricks escapes quotes and
    backslashes and adds the quotation marks; docs: sql-editor
    mustache-parameters). Use IDENTIFIER({{name}}) for a name. Unknown
    names stay."""
    return _MUSTACHE_RE.sub(lambda m: sql_string(params[m.group(1)]) if m.group(1) in params else m.group(0), text)


_READ_FILES_RE = re.compile(r"(?i)(?<![\w.])read_files\s*\(")
_RF_ARG_RE = re.compile(r"\s*(?:([A-Za-z_]\w*)\s*=>\s*)?('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|[A-Za-z0-9_.+-]+)\s*(,|\))")


def _sql_literal(tok: str) -> str:
    if tok[:1] in "'\"":
        return re.sub(r"\\(.)", r"\1", tok[1:-1])
    return tok


def expand_read_files(spark, stmt: str, counter=[0]) -> str:  # noqa: B006 (a per-process counter)
    """read_files(path, format => 'json', opt => value, ...) in batch SQL
    (Databricks): the files are read with spark.read into a temporary view,
    and the call becomes that view's name. STREAM read_files is refused by
    the plan; here a STREAM before the call is an error too."""
    clean = blank_sql(stmt)
    out, last = [], 0
    for m in _READ_FILES_RE.finditer(clean):
        if re.search(r"(?i)\bSTREAM\s*$", clean[:m.start()]):
            raise TaskError("STREAM read_files runs only in Lakeflow pipelines: read the files as a batch")
        pos, args, named = m.end(), [], {}
        while True:
            am = _RF_ARG_RE.match(stmt, pos)
            if not am:
                raise TaskError(f"read_files(...): clavesa reads literal arguments only: {stmt[m.start():m.start() + 120]}")
            key, val = am.group(1), _sql_literal(am.group(2))
            if key:
                named[key.lower()] = val
            else:
                args.append(val)
            pos = am.end()
            if am.group(3) == ")":
                break
        if not args:
            raise TaskError("read_files(...) without a path")
        fmt = named.pop("format", "")
        if not fmt:
            raise TaskError("read_files(...): give format => 'json' (or csv, parquet, text, avro, orc); clavesa does not guess it")
        reader = spark.read.format(fmt)
        for k, v in named.items():
            reader = reader.option(k, v)
        paths = [("s3a://" + p[len("s3://"):]) if p.startswith("s3://") else p for p in args]
        counter[0] += 1
        view = f"__clavesa_read_files_{counter[0]}"
        reader.load(paths if len(paths) > 1 else paths[0]).createOrReplaceTempView(view)
        out.append(stmt[last:m.start()])
        out.append(view)
        last = pos
    out.append(stmt[last:])
    return "".join(out)


_IDENT_LITERAL_RE = re.compile(r"(?i)\bIDENTIFIER\s*\(\s*'((?:[^'\\]|\\.)*)'\s*\)")


def unwrap_identifier(stmt: str) -> str:
    """IDENTIFIER('a.b.c') with a literal string is the name a.b.c, so the
    table-name rewrite sees it."""
    return _IDENT_LITERAL_RE.sub(lambda m: ".".join(f"`{p}`" for p in m.group(1).split(".")), stmt)


# ---------------------------------------------------------------------------
# Table names (ADR-031 section 6).
# ---------------------------------------------------------------------------

_IDENT = r"(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"
_CHAIN_RE = re.compile(r"(?<![\w.`])(" + _IDENT + r"(?:\s*\.\s*" + _IDENT + r"){0,2})(?![\w`])(?!\s*\.)")
_CALL_AFTER_RE = re.compile(r"\s*\(")
_TABLE_KW_RE = re.compile(
    r"(?is)(?:\bFROM|\bJOIN|\bINTO|\bTABLE|\bUPDATE|\bUSING|\bEXISTS|\bOPTIMIZE|\bDESCRIBE|\bDESC|\bVACUUM)\s*$"
)
_USE_RE = re.compile(
    r"(?is)^\s*USE\s+(CATALOG|SCHEMA|DATABASE)?\s*(?:IDENTIFIER\s*\(\s*(.+?)\s*\)|(" + _IDENT + r"(?:\s*\.\s*" + _IDENT + r")?))\s*$"
)


def _unquote(p: str) -> str:
    p = p.strip()
    if p.startswith("`") and p.endswith("`"):
        return p[1:-1]
    return p


def split_name(name: str) -> list[str]:
    parts, cur, inq = [], "", False
    for ch in name:
        if ch == "`":
            inq = not inq
            cur += ch
        elif ch == "." and not inq:
            parts.append(_unquote(cur))
            cur = ""
        else:
            cur += ch
    parts.append(_unquote(cur))
    return parts


class Names:
    """Resolves the table names a task uses. One per run: the current
    catalog and schema (USE CATALOG / USE SCHEMA) carry across the tasks'
    statements the way a Databricks session does within one task."""

    def __init__(self, mode: str = "glue", catalogs: dict | None = None,
                 known: set | None = None, writes: set | None = None, shadow: set | None = None,
                 job_writes: set | None = None, uc_reader=None):
        self.mode = mode
        # catalog: unity (ADR-031 section 6): the tables the job writes live
        # in Glue as in catalog: glue (and are published to Unity Catalog
        # after the task); every other table is read from Unity Catalog
        # through uc_reader(parts) -> the name of a temp view over it.
        self.job_writes = {k.lower() for k in (job_writes or writes or set())}
        self.uc_reader = uc_reader
        self.catalogs = dict(catalogs or {})
        # Every table the plan knows this task reads or writes, lower-case
        # three-level names.
        self.known = {k.lower() for k in (known or set())}
        self.writes = {k.lower() for k in (writes or set())}
        self.known_catalogs = {k.split(".")[0] for k in self.known | self.writes}
        # Tables written under the shadow name (<table>_clavesa) in shadow
        # mode (ADR-031 section 9): the job's own outputs.
        self.shadow = {k.lower() for k in (shadow or set())}
        self.catalog = ""
        self.schema = ""

    def reset(self):
        self.catalog, self.schema = "", ""

    def full(self, name: str) -> list[str] | None:
        parts = split_name(name)
        if len(parts) == 3:
            return parts
        if len(parts) == 2 and self.catalog:
            return [self.catalog] + parts
        if len(parts) == 1 and self.catalog and self.schema:
            return [self.catalog, self.schema] + parts
        return None

    def physical(self, parts: list[str]) -> str:
        """The identifier Spark uses for a three-level name."""
        cat, sch, tbl = parts
        if f"{cat}.{sch}.{tbl}".lower() in self.shadow:
            tbl = tbl + "_clavesa"
        if self.mode == "unity" and f"{cat}.{sch}.{parts[2]}".lower() not in self.job_writes:
            if self.uc_reader is not None:
                return self.uc_reader(parts)
            return f"`{cat}`.`{sch}`.`{tbl}`"
        cat = self.catalogs.get(cat, cat)
        return f"`{cat}__{sch}`.`{tbl}`"

    def database_of(self, three: str) -> str:
        cat, sch, _ = split_name(three)
        return f"{self.catalogs.get(cat, cat)}__{sch}"

    def uc_name(self, three: str) -> str:
        """The Unity Catalog name a written table is published as: the
        shadow name in shadow mode."""
        cat, sch, tbl = split_name(three)
        if three.lower() in self.shadow:
            tbl += "_clavesa"
        return f"{cat}.{sch}.{tbl}"

    def table(self, name: str, write: bool = False) -> str:
        """Map a name from Python code (spark.table, saveAsTable, ...)."""
        parts = self.full(name)
        if parts is None:
            if write:
                raise TaskError(
                    f"table {name!r} has no catalog: use catalog.schema.table (or USE CATALOG / USE SCHEMA first)"
                )
            return name  # a temp view, or a name Spark resolves itself
        key = ".".join(parts).lower()
        if write and self.writes and key not in self.writes:
            raise TaskError(
                f"the task writes {'.'.join(parts)}, which is not in its tables: add it to clavesa.tables "
                "in the bundle (the plan needs every table before the first run)"
            )
        return self.physical(parts)

    def use(self, stmt: str, args: dict | None = None) -> bool:
        """Apply a USE statement to the state; True when stmt was one."""
        m = _USE_RE.match(blank_sql(stmt))
        if not m:
            return False
        kind = (m.group(1) or "").upper()
        if m.group(2):  # IDENTIFIER(<expr>)
            expr = stmt[m.start(2):m.end(2)].strip()
            if expr.startswith(":"):
                val = (args or {}).get(expr[1:])
                if val is None:
                    raise TaskError(f"USE ... IDENTIFIER({expr}): no parameter {expr[1:]!r}")
            else:
                val = expr.strip("'\"")
            name = str(val)
        else:
            name = stmt[m.start(3):m.end(3)]
        parts = split_name(name)
        if kind == "CATALOG":
            self.catalog = parts[0]
        elif len(parts) == 2:
            self.catalog, self.schema = parts
        else:
            self.schema = parts[0]
        return True

    def rewrite_sql(self, stmt: str) -> str:
        """Replace the table names of known tables in one statement with
        their physical names. A three-level name is a table wherever it
        is; a shorter name only after a keyword that names a table."""
        if not _USE_RE.match(blank_sql(stmt)):
            stmt = unwrap_identifier(stmt)
        clean = blank_sql(stmt)
        out, last = [], 0
        for m in _CHAIN_RE.finditer(clean):
            raw = stmt[m.start(1):m.end(1)]
            parts = split_name(re.sub(r"\s+", "", raw))
            after_kw = bool(_TABLE_KW_RE.search(clean[:m.start(1)]))
            # name( is a function call, except right after TABLE / INTO
            # (CREATE TABLE t (cols), INSERT INTO t (cols)).
            if _CALL_AFTER_RE.match(clean, m.end(1)) and not after_kw:
                continue
            if len(parts) < 3 and not after_kw:
                continue
            full = self.full(".".join(f"`{p}`" for p in parts))
            if full is None:
                continue
            key = ".".join(full).lower()
            if key not in self.known and key not in self.writes and not (
                len(parts) == 3 and full[0].lower() in self.known_catalogs
            ):
                continue
            out.append(stmt[last:m.start(1)])
            out.append(self.physical(full))
            last = m.end(1)
        out.append(stmt[last:])
        return "".join(out)


# ---------------------------------------------------------------------------
# {{...}} references (ADR-031 section 7).
# ---------------------------------------------------------------------------

_REF_RE = re.compile(r"\{\{\s*([^}]*?)\s*\}\}")


class RunState:
    """What one run of a bundle job knows: job parameters, task values and
    results, ids and times. Shared by every task of the run."""

    def __init__(self, job: dict, params: dict, run_id: str, start: float | None = None):
        self.job = job or {}
        self.params = dict(params or {})
        self.run_id = run_id
        self.start = time.time() if start is None else start
        self.values: dict[str, dict[str, Any]] = {}
        self.results: dict[str, dict[str, Any]] = {}
        self.current_task = ""
        self.inputs: list[Any] = []

    def resolve(self, text: Any, task_key: str = "", item: Any = None) -> Any:
        if not isinstance(text, str):
            return text

        def one(m):
            v = self.value(m.group(1), task_key, item)
            if isinstance(v, (dict, list)):
                return json.dumps(v)
            if isinstance(v, bool):
                return "true" if v else "false"
            return str(v)

        return _REF_RE.sub(one, text)

    def value(self, ref: str, task_key: str = "", item: Any = None) -> Any:
        t = _dt.datetime.fromtimestamp(self.start, tz=_dt.timezone.utc)
        if ref.startswith("job.parameters."):
            name = ref[len("job.parameters."):]
            if name not in self.params:
                raise TaskError(f"{{{{{ref}}}}}: the job has no parameter {name!r}")
            return self.params[name]
        simple = {
            "job.id": self.job.get("key", ""),
            "job.name": self.job.get("name", ""),
            "job.run_id": self.run_id,
            "job.repair_count": 0,
            "job.trigger.type": self.job.get("trigger_type", "one_time"),
            "task.name": task_key,
            "task.run_id": f"{self.run_id}-{task_key}",
            "task.execution_count": 1,
        }
        if ref in simple:
            return simple[ref]
        for prefix in ("job.start_time.", "job.trigger.time."):
            if ref.startswith(prefix):
                return _time_field(t, ref[len(prefix):])
        if ref == "input":
            return item
        if ref.startswith("input."):
            cur = item
            for p in ref[len("input."):].split("."):
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    raise TaskError(f"{{{{{ref}}}}}: the item has no field {p!r}")
            return cur
        if ref.startswith("tasks."):
            parts = ref.split(".", 3)
            if len(parts) >= 3:
                k, what = parts[1], parts[2]
                if what == "values" and len(parts) == 4:
                    vals = self.values.get(k, {})
                    if parts[3] not in vals:
                        raise TaskError(f"{{{{{ref}}}}}: task {k!r} set no value {parts[3]!r}")
                    return vals[parts[3]]
                res = self.results.get(k, {})
                if what == "result_state":
                    return res.get("result_state", "")
                if what == "run_id":
                    return f"{self.run_id}-{k}"
                if what == "error_code":
                    return res.get("error_code", "")
                if what == "execution_count":
                    return res.get("execution_count", 0)
        if ref.startswith("secrets/"):
            _, scope, key = ref.split("/", 2)
            return Secrets().get(scope, key)
        raise TaskError(f"{{{{{ref}}}}} is not a reference clavesa resolves")


def _time_field(t: _dt.datetime, f: str) -> Any:
    return {
        "iso_date": t.date().isoformat(),
        "iso_datetime": t.strftime("%Y-%m-%dT%H:%M:%S"),
        "timestamp_ms": int(t.timestamp() * 1000),
        "year": t.year,
        "month": t.month,
        "day": t.day,
        "hour": t.hour,
        "minute": t.minute,
        "second": t.second,
        "is_weekday": t.weekday() < 5,
        "is_weekend": t.weekday() >= 5,
    }[f]


# ---------------------------------------------------------------------------
# dbutils (ADR-031 section 5).
# ---------------------------------------------------------------------------


class InputWidgetNotDefined(Exception):
    pass


class Widgets:
    def __init__(self, values: dict[str, str]):
        self._values = dict(values)
        self._defaults: dict[str, str] = {}

    def _define(self, name, default):
        self._defaults[name] = "" if default is None else str(default)

    def text(self, name, defaultValue="", label=None):  # noqa: N803 (Databricks API names)
        self._define(name, defaultValue)

    def dropdown(self, name, defaultValue, choices, label=None):  # noqa: N803
        self._define(name, defaultValue)

    def combobox(self, name, defaultValue, choices, label=None):  # noqa: N803
        self._define(name, defaultValue)

    def multiselect(self, name, defaultValue, choices, label=None):  # noqa: N803
        self._define(name, defaultValue)

    def get(self, name):
        if name in self._values:
            return self._values[name]
        if name in self._defaults:
            return self._defaults[name]
        raise InputWidgetNotDefined(f"No input widget named {name} is defined")

    def getArgument(self, name, defaultValue=None):  # noqa: N802,N803
        try:
            return self.get(name)
        except InputWidgetNotDefined:
            return defaultValue

    def getAll(self):  # noqa: N802
        out = dict(self._defaults)
        out.update(self._values)
        return out

    def remove(self, name):
        self._defaults.pop(name, None)

    def removeAll(self):  # noqa: N802
        self._defaults.clear()


class Secrets:
    """dbutils.secrets: read from the environment (local runs:
    CLAVESA_DBX_SECRET__<scope>__<key>), else from AWS Secrets Manager under
    CLAVESA_DBX_SECRET_PREFIX + <scope>/<key> (clavesa/<workspace>/dbx/)."""

    def get(self, scope, key):
        env = f"CLAVESA_DBX_SECRET__{scope}__{key}"
        if env in os.environ:
            return os.environ[env]
        prefix = os.environ.get("CLAVESA_DBX_SECRET_PREFIX", "")
        if not prefix:
            raise TaskError(
                f"secret {scope}/{key}: not set. Locally, export {env}; in AWS, put it in Secrets Manager "
                "as clavesa/<workspace>/dbx/" + f"{scope}/{key}"
            )
        import boto3  # noqa: PLC0415

        name = f"{prefix}{scope}/{key}"
        try:
            return boto3.client("secretsmanager").get_secret_value(SecretId=name)["SecretString"]
        except Exception as exc:  # noqa: BLE001
            raise TaskError(f"secret {scope}/{key}: cannot read Secrets Manager secret {name}: {exc}") from exc

    def getBytes(self, scope, key):  # noqa: N802
        return self.get(scope, key).encode()

    def list(self, scope):
        raise TaskError("dbutils.secrets.list is not supported on clavesa")

    def listScopes(self):  # noqa: N802
        raise TaskError("dbutils.secrets.listScopes is not supported on clavesa")


class TaskValues:
    def __init__(self, state: RunState):
        self._state = state

    def set(self, key, value):
        json.dumps(value)  # Databricks requires a JSON-serializable value
        self._state.values.setdefault(self._state.current_task, {})[key] = value

    def get(self, taskKey, key, default=None, debugValue=None):  # noqa: N803
        vals = self._state.values.get(taskKey, {})
        if key in vals:
            return vals[key]
        if default is not None:
            return default
        raise ValueError(f"no task value {key!r} from task {taskKey!r}")


class Notebook:
    def exit(self, value):
        raise NotebookExit(value)

    def run(self, *a, **k):
        raise TaskError("dbutils.notebook.run is not supported on clavesa: use %run or a run_job_task")

    def getContext(self):  # noqa: N802
        raise TaskError("dbutils.notebook.getContext is Databricks only")


class FileInfo(tuple):
    __slots__ = ()
    _fields = ("path", "name", "size", "modificationTime")

    def __new__(cls, path, name, size, mtime):
        return tuple.__new__(cls, (path, name, size, mtime))

    path = property(lambda s: s[0])
    name = property(lambda s: s[1])
    size = property(lambda s: s[2])
    modificationTime = property(lambda s: s[3])  # noqa: N815


class Fs:
    """dbutils.fs over Hadoop FileSystem, so s3:// and local paths work."""

    def _fs(self, path):
        from pyspark.sql import SparkSession  # noqa: PLC0415

        spark = SparkSession.builder.getOrCreate()
        jvm = spark.sparkContext._jvm
        if path.startswith(("dbfs:", "/dbfs/")):
            raise TaskError(f"{path}: DBFS exists only in Databricks")
        p = jvm.org.apache.hadoop.fs.Path(path)
        return p.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), p, jvm

    def ls(self, path):
        fs, p, _ = self._fs(path)
        out = []
        for st in fs.listStatus(p):
            name = st.getPath().getName() + ("/" if st.isDirectory() else "")
            out.append(FileInfo(str(st.getPath()) + ("/" if st.isDirectory() else ""), name, st.getLen(),
                                st.getModificationTime()))
        return out

    def mkdirs(self, path):
        fs, p, _ = self._fs(path)
        return bool(fs.mkdirs(p))

    def rm(self, path, recurse=False):
        fs, p, _ = self._fs(path)
        return bool(fs.delete(p, bool(recurse)))

    def put(self, path, contents, overwrite=False):
        fs, p, _ = self._fs(path)
        if fs.exists(p) and not overwrite:
            raise TaskError(f"{path} exists; pass overwrite=True")
        out = fs.create(p, True)
        try:
            out.write(contents.encode())
        finally:
            out.close()
        return True

    def head(self, path, maxBytes=65536):  # noqa: N803
        fs, p, _ = self._fs(path)
        stream = fs.open(p)
        try:
            buf = bytearray()
            while len(buf) < maxBytes:
                b = stream.read()
                if b < 0:
                    break
                buf.append(b)
            return buf.decode(errors="replace")
        finally:
            stream.close()

    def cp(self, src, dst, recurse=False):
        sfs, sp, jvm = self._fs(src)
        dfs, dp, _ = self._fs(dst)
        from pyspark.sql import SparkSession  # noqa: PLC0415

        conf = SparkSession.builder.getOrCreate().sparkContext._jsc.hadoopConfiguration()
        return bool(jvm.org.apache.hadoop.fs.FileUtil.copy(sfs, sp, dfs, dp, False, True, conf))

    def mv(self, src, dst, recurse=False):
        ok = self.cp(src, dst, recurse)
        self.rm(src, True)
        return ok


class Library:
    def __getattr__(self, name):
        raise TaskError("dbutils.library is not supported on clavesa: move the package to the task's libraries")


class Jobs:
    def __init__(self, state):
        self.taskValues = TaskValues(state)  # noqa: N815


class DBUtils:
    def __init__(self, state: RunState, widgets: dict[str, str]):
        self.widgets = Widgets(widgets)
        self.secrets = Secrets()
        self.jobs = Jobs(state)
        self.notebook = Notebook()
        self.fs = Fs()
        self.library = Library()


def display(obj, *a, **k):
    """display(df) logs the first 20 rows."""
    show = getattr(obj, "show", None)
    if callable(show):
        show(20, truncate=False)
    else:
        print(obj)


def displayHTML(*a, **k):  # noqa: N802
    return None


# ---------------------------------------------------------------------------
# Running a task.
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _patched_spark(spark, names: Names):
    """Route every table name the code uses through `names` while the task
    runs, and restore the classes after."""
    from pyspark.sql import DataFrameReader, DataFrameWriter, SparkSession  # noqa: PLC0415

    saved = []

    def patch(cls, attr, make):
        orig = getattr(cls, attr)
        saved.append((cls, attr, orig))
        setattr(cls, attr, make(orig))

    patch(SparkSession, "table", lambda o: lambda self, n: o(self, names.table(n)))
    patch(DataFrameReader, "table", lambda o: lambda self, n: o(self, names.table(n)))
    patch(DataFrameWriter, "saveAsTable",
          lambda o: lambda self, n, *a, **k: o(self, names.table(n, write=True), *a, **k))
    patch(DataFrameWriter, "insertInto",
          lambda o: lambda self, n, *a, **k: o(self, names.table(n, write=True), *a, **k))

    def make_sql(orig):
        def sql(self, sqlQuery, args=None, **kwargs):  # noqa: N803
            if names.use(sqlQuery, args):
                return orig(self, "SELECT 1 WHERE false")
            return orig(self, names.rewrite_sql(expand_read_files(self, sqlQuery)), args, **kwargs)

        return sql

    patch(SparkSession, "sql", make_sql)
    try:
        from delta.tables import DeltaTable  # noqa: PLC0415

        orig_for_name = DeltaTable.forName
        saved.append((DeltaTable, "forName", orig_for_name))
        DeltaTable.forName = classmethod(  # type: ignore[assignment]
            lambda cls, sp, n: orig_for_name.__func__(cls, sp, names.table(n, write=True))
        )
    except ImportError:
        pass
    try:
        yield
    finally:
        for cls, attr, orig in reversed(saved):
            setattr(cls, attr, orig)


@contextlib.contextmanager
def _runtime_modules(spark, dbutils):
    """`from databricks.sdk.runtime import spark, dbutils` gets the runner's
    session and this task's dbutils."""
    mod = types.ModuleType("databricks.sdk.runtime")
    mod.spark = spark
    mod.dbutils = dbutils
    mod.display = display
    mod.displayHTML = displayHTML
    saved = {k: sys.modules.get(k) for k in ("databricks", "databricks.sdk", "databricks.sdk.runtime")}
    if saved["databricks"] is None:
        sys.modules["databricks"] = types.ModuleType("databricks")
        sys.modules["databricks"].__path__ = []  # type: ignore[attr-defined]
    if saved["databricks.sdk"] is None:
        sys.modules["databricks.sdk"] = types.ModuleType("databricks.sdk")
        sys.modules["databricks.sdk"].__path__ = []  # type: ignore[attr-defined]
    sys.modules["databricks.sdk.runtime"] = mod
    old_builtins = {k: getattr(builtins, k, None) for k in ("dbutils", "display", "displayHTML")}
    builtins.dbutils = dbutils  # type: ignore[attr-defined]
    builtins.display = display  # type: ignore[attr-defined]
    builtins.displayHTML = displayHTML  # type: ignore[attr-defined]
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
        for k, v in old_builtins.items():
            if v is None:
                with contextlib.suppress(AttributeError):
                    delattr(builtins, k)
            else:
                setattr(builtins, k, v)


def database_location(db: str) -> str:
    """Where a bundle job's Glue database keeps its tables in the cloud:
    s3://<bucket>/_bundle_warehouse/<db>.db/ in the workspace bucket. Empty
    on a local warehouse (the metastore's own folder)."""
    wh = os.environ.get("CLAVESA_WAREHOUSE", "")
    if not wh.startswith("s3://"):
        return ""
    bucket = wh[len("s3://"):].split("/", 1)[0]
    return f"s3://{bucket}/_bundle_warehouse/{db}.db/"


def prepare_cloud_event(event: dict, files: bool = True) -> dict:
    """A deployed bundle pipeline's run event names bundle-job.json in S3
    as each task's logic_path. Load the job, download the bundle's files
    next to it into /tmp and build the same _bundle_job a local run gets.
    Job parameters come from the run input's "params". No-op for any other
    event."""
    transforms = event.get("transforms") or []
    if event.get("_bundle_job") or not transforms or transforms[0].get("language") != "task":
        return event
    logic = transforms[0].get("logic_path", "")
    if not logic.startswith("s3://"):
        raise TaskError(f"bundle pipeline run without its job file in S3 (logic_path {logic!r})")
    import boto3  # noqa: PLC0415

    s3 = boto3.client("s3")
    bucket, key = logic[len("s3://"):].split("/", 1)
    job = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    prefix = key.rsplit("/", 1)[0] + "/files/"
    exec_input = event.get("_execution_input") if isinstance(event.get("_execution_input"), dict) else {}
    run_id = str(event.get("_sf_execution_arn") or event.get("run_id") or "run")
    root = os.path.join("/tmp", "clavesa-bundle", re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)[-80:])
    os.makedirs(root, exist_ok=True)
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix) if files else []
    for page in pages:
        for obj in page.get("Contents", []):
            rel = obj["Key"][len(prefix):]
            if not rel or rel.endswith("/"):
                continue
            dst = os.path.join(root, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            s3.download_file(bucket, obj["Key"], dst)
    params = {p["name"]: str(p.get("default", "")) for p in job.get("parameters") or []}
    for k, v in (exec_input.get("params") or event.get("params") or {}).items():
        if k not in params:
            raise TaskError(f"run parameter {k!r}: job {job.get('job')} has no such parameter")
        params[k] = str(v)
    tasks = job.pop("tasks", {}) or {}
    job.update({"params": params, "bundle_root": root, "run_id": run_id})
    event["_bundle_job"] = job
    for t in transforms:
        if t.get("node") in tasks:
            t["task"] = tasks[t["node"]]
    return event


def _files(value: Any, root: str) -> Any:
    if isinstance(value, str):
        return value.replace(FILES_PLACEHOLDER, root)
    return value


def _run_script(path: str, argv: list[str], init_globals: dict | None = None):
    """Run a Python file as __main__ with sys.argv set, from its folder."""
    old_argv, old_path, old_cwd = sys.argv, list(sys.path), os.getcwd()
    sys.argv = [path] + [str(a) for a in argv]
    sys.path.insert(0, os.path.dirname(path))
    os.chdir(os.path.dirname(path))
    try:
        runpy.run_path(path, init_globals=init_globals, run_name="__main__")
    except SystemExit as exc:
        if exc.code not in (None, 0):
            raise TaskError(f"{os.path.basename(path)} exited with status {exc.code}") from exc
    finally:
        sys.argv, sys.path[:] = old_argv, old_path
        os.chdir(old_cwd)


def run_sql_file(spark, names: Names, text: str, params: dict[str, str]):
    """Run a sql_task file: {{name}} substitution, then each statement with
    the named parameters it uses."""
    text = substitute_mustache(text, params)
    for stmt in split_statements(text):
        args = {k: params[k] for k in used_markers(stmt) if k in params}
        missing = [k for k in used_markers(stmt) if k not in params]
        if missing:
            raise TaskError(f"the SQL uses :{missing[0]}, which is not a parameter of the task")
        if names.use(stmt, args):
            continue
        spark.sql(names.rewrite_sql(expand_read_files(spark, stmt)), args or None)


def run_task(spark, task: dict, ctx: dict, state: RunState, names: Names) -> dict:
    """Run one bundle task. Returns {"result": <notebook exit value or None>}.

    task is the resolved task (its JSON in the bundle); ctx carries
    bundle_root (the folder with the bundle's files), the script a notebook
    task was translated to, and the run's job parameters."""
    root = ctx["bundle_root"]
    key = task.get("task_key", "")
    state.current_task = key
    item = ctx.get("item")
    res = lambda v: _files(state.resolve(v, key, item), root)  # noqa: E731

    # Task tables live in Glue databases the pipeline does not own: make
    # sure the written ones exist (Hive metastore locally).
    if names.mode in ("glue", "unity"):
        for db in sorted({names.database_of(w) for w in names.writes}):
            loc = database_location(db)
            spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`" + (f" LOCATION '{loc}'" if loc else ""))
    spark.conf.set("spark.sql.sources.default", "delta")
    names.reset()

    result = None
    if ctx.get("wheels") and "python_wheel_task" not in task:
        install_wheels([os.path.join(root, w) for w in ctx["wheels"]], os.path.join(root, "_wheels", key or "task"))
    if "spark_python_task" in task:
        t = task["spark_python_task"]
        argv = [res(p) for p in t.get("parameters") or []]
        dbutils = DBUtils(state, {k: str(v) for k, v in state.params.items()})
        with _patched_spark(spark, names), _runtime_modules(spark, dbutils):
            _run_script(os.path.join(root, t["python_file"]), argv)
    elif "python_wheel_task" in task:
        t = task["python_wheel_task"]
        target = os.path.join(root, "_wheels", key or "task")
        install_wheels([os.path.join(root, w) for w in ctx.get("wheels") or []], target)
        fn = wheel_entry_point(t["package_name"], t.get("entry_point", "main"), target)
        dbutils = DBUtils(state, {k: str(v) for k, v in state.params.items()})
        old_argv = sys.argv
        sys.argv = [t["package_name"]] + wheel_argv(t, res)
        try:
            with _patched_spark(spark, names), _runtime_modules(spark, dbutils):
                fn()
        except SystemExit as exc:
            if exc.code not in (None, 0):
                raise TaskError(f"{t['package_name']}.{t.get('entry_point')} exited with status {exc.code}") from exc
        finally:
            sys.argv = old_argv
    elif "sql_task" in task:
        t = task["sql_task"]
        f = (t.get("file") or {}).get("path")
        if not f:
            raise TaskError("sql_task without file")
        params = {k: res(v) for k, v in (t.get("parameters") or {}).items()}
        with open(os.path.join(root, f), encoding="utf-8") as fh:
            text = fh.read()
        with _patched_spark(spark, names):
            run_sql_file(spark, names, text, params)
    elif "notebook_task" in task:
        t = task["notebook_task"]
        script = ctx.get("script")
        if not script:
            raise TaskError("the notebook was not translated at deploy (no script)")
        widgets = {k: str(v) for k, v in state.params.items()}
        widgets.update({k: res(v) for k, v in (t.get("base_parameters") or {}).items()})
        # Job parameters win over base_parameters of the same name.
        widgets.update({k: str(v) for k, v in state.params.items()})
        dbutils = DBUtils(state, widgets)
        g = {"spark": spark, "sc": spark.sparkContext, "dbutils": dbutils, "display": display,
             "displayHTML": displayHTML, "__clavesa_names__": names}
        with _patched_spark(spark, names), _runtime_modules(spark, dbutils):
            try:
                _run_script(os.path.join(root, script), [], init_globals=g)
            except NotebookExit as e:
                result = e.value
    else:
        kind = next((k for k in task if k.endswith("_task")), "?")
        raise TaskError(f"{kind}: not run by the clavesa runner")
    return {"result": result}


# ---------------------------------------------------------------------------
# Helpers the translated notebook scripts call (ADR-031 section 5).
# ---------------------------------------------------------------------------

_DOLLAR_RE = re.compile(r"\$\{\s*([A-Za-z_]\w*)\s*\}")


def notebook_sql(spark, names: Names, dbutils, text: str, where: str = ""):
    """Run a %sql cell: ${name} and {{name}} replaced by widget values as
    text, then each statement with the widget values it uses as :name
    parameters. Returns the last statement's DataFrame."""
    widgets = dbutils.widgets.getAll()
    text = _DOLLAR_RE.sub(lambda m: str(widgets[m.group(1)]) if m.group(1) in widgets else m.group(0), text)
    text = substitute_mustache(text, widgets)
    last = None
    for stmt in split_statements(text):
        markers = used_markers(stmt)
        missing = [k for k in markers if k not in widgets]
        if missing:
            raise TaskError(f"{where}: the SQL uses :{missing[0]}, which is not a widget or job parameter")
        args = {k: widgets[k] for k in markers}
        if names.use(stmt, args):
            continue
        last = spark.sql(names.rewrite_sql(expand_read_files(spark, stmt)), args or None)
    return last


def notebook_fs(dbutils, args: str):
    """A %fs cell: `%fs ls /path` is dbutils.fs.ls("/path")."""
    import shlex  # noqa: PLC0415

    parts = shlex.split(args)
    if not parts:
        raise TaskError("%fs without a command")
    cmd, rest = parts[0], parts[1:]
    recurse = False
    if "-r" in rest:
        rest.remove("-r")
        recurse = True
    fn = getattr(dbutils.fs, cmd, None)
    if fn is None:
        raise TaskError(f"%fs {cmd}: not supported (ls, cp, mv, rm, head, mkdirs, put)")
    if cmd in ("cp", "mv", "rm"):
        out = fn(*rest, recurse=recurse)
    else:
        out = fn(*rest)
    display(out)
    return out


# ---------------------------------------------------------------------------
# python_wheel_task (ADR-031 section 5).
# ---------------------------------------------------------------------------


def install_wheels(wheels: list[str], target: str) -> None:
    """Install the task's wheels (no dependencies: those are runner
    requirements, baked into the image) into a per-run folder and put it
    first on sys.path."""
    import subprocess  # noqa: PLC0415

    if not wheels:
        return
    os.makedirs(target, exist_ok=True)
    cmd = [sys.executable, "-m", "pip", "install", "--quiet", "--no-deps", "--no-index",
           "--disable-pip-version-check", "--target", target, *wheels]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise TaskError(f"installing {', '.join(os.path.basename(w) for w in wheels)} failed: {proc.stderr.strip()[-800:]}")
    if target not in sys.path:
        sys.path.insert(0, target)
    import importlib  # noqa: PLC0415

    importlib.invalidate_caches()


def wheel_entry_point(package: str, entry_point: str, search_path: str):
    """The function a python_wheel_task calls: the entry point of that name
    in the package's distribution (any group, as Databricks looks it up),
    else <package>.<entry_point>."""
    from importlib import metadata  # noqa: PLC0415

    want = {package.lower(), package.lower().replace("_", "-"), package.lower().replace("-", "_")}
    for dist in metadata.distributions(path=[search_path]):
        name = (dist.metadata["Name"] or "").lower()
        if name not in want and name.replace("-", "_") not in want:
            continue
        for ep in dist.entry_points:
            if ep.name == entry_point:
                return ep.load()
    import importlib  # noqa: PLC0415

    mod = importlib.import_module(package)
    fn = getattr(mod, entry_point, None)
    if fn is None:
        raise TaskError(f"package {package} has no entry point {entry_point!r}")
    return fn


def wheel_argv(task: dict, res) -> list[str]:
    """parameters as they are, or named_parameters as --key=value, the way
    Databricks passes them."""
    if task.get("named_parameters"):
        return [f"--{k}={res(v)}" for k, v in task["named_parameters"].items()]
    return [str(res(p)) for p in task.get("parameters") or []]


# ---------------------------------------------------------------------------
# The runner's entry point for a task node.
# ---------------------------------------------------------------------------

# One RunState per run (task values and results are shared by the tasks
# of one run, which all go through one runner process).
_STATES: dict[str, RunState] = {}


def run_node(spark, event: dict) -> dict:
    """Run the task node of a bundle pipeline. event carries _bundle_job
    (the job, its parameter values, the files folder) and _bundle_task (the
    task from bundle-job.json). Returns the handler response: the tables
    the task wrote are its outputs, so the catalog records them."""
    job = event.get("_bundle_job") or {}
    entry = event.get("_bundle_task") or {}
    run_id = str(job.get("run_id") or event.get("_sf_execution_arn") or "")
    state = _STATES.get(run_id)
    if state is None:
        _STATES.clear()
        state = RunState({"key": job.get("job", ""), "name": job.get("name", "")}, job.get("params") or {}, run_id)
        _STATES[run_id] = state
    key = entry.get("task_key", "")
    reads = set(entry.get("reads") or [])
    writes = set(entry.get("writes") or [])
    job_writes = set(job.get("writes") or []) | writes
    names = Names(
        mode=job.get("catalog") or "glue",
        catalogs=job.get("catalogs") or {},
        known=reads | writes,
        writes=writes,
        shadow=job_writes if job.get("shadow") else set(),
        job_writes=job_writes,
    )
    uc = None
    if names.mode == "unity" and not any(k in (entry.get("task") or {}) for k in ("condition_task", "run_job_task")):
        uc = UnityTask(spark, names, job, key)
    ctx = {"bundle_root": job.get("bundle_root", ""), "script": entry.get("script"),
           "wheels": entry.get("wheels") or []}
    task = entry.get("task") or {}
    state.current_task = key
    if "condition_task" in task:
        c = task["condition_task"]
        took = evaluate_condition(c.get("op", ""), state.resolve(c.get("left", ""), key), state.resolve(c.get("right", ""), key))
        state.results[key] = {"result_state": "success", "outcome": "true" if took else "false"}
        print(f"[clavesa] condition {key}: {c.get('left')} {c.get('op')} {c.get('right')} -> {str(took).lower()}",
              file=sys.stderr, flush=True)
        return {"status": "ok", "outputs": {}}
    try:
        if uc is not None:
            uc.before()
        with _deadline(spark, key, event.get("_bundle_timeout") or 0):
            if "run_job_task" in task:
                out = _run_child_job(spark, task, entry, job, state)
            elif "for_each_task" in task:
                out = _run_for_each(spark, task, ctx, state, names)
            else:
                out = run_task(spark, task, ctx, state, names)
    except Exception as exc:
        state.results[key] = {"result_state": "timedout" if isinstance(exc, TaskTimeout) else "failed",
                              "error_code": type(exc).__name__}
        raise
    state.results[key] = {"result_state": "success", "result": out.get("result")}
    outputs = {w: names.physical(split_name(w)).replace("`", "") for w in sorted(writes)}
    resp = {"status": "ok", "outputs": outputs, "result": out.get("result")}
    if uc is not None:
        resp["unity_catalog"] = uc.after(sorted(reads))
    return resp


# ---------------------------------------------------------------------------
# The job loop (ADR-031 section 4): DAB semantics in the runner, so a local
# run and a cloud run behave the same.
# ---------------------------------------------------------------------------

RUN_IF = ("ALL_SUCCESS", "ALL_DONE", "NONE_FAILED", "AT_LEAST_ONE_SUCCESS", "ALL_FAILED", "AT_LEAST_ONE_FAILED")
# Result states a parent can end in; "excluded" is a task that did not run
# because its run_if or an outcome said no (Databricks: EXCLUDED), and
# "disabled" is disabled: true.
_DONE_OK = ("success",)
_DONE_FAILED = ("failed", "timedout", "upstream_failed")


def should_run(run_if: str, parent_states: list[str]) -> bool:
    """Whether a task runs, from its run_if and its parents' end states
    (the Databricks rules; an excluded or disabled parent counts as
    neither success nor failure)."""
    run_if = run_if or "ALL_SUCCESS"
    if not parent_states:
        return True
    ok = [s for s in parent_states if s in _DONE_OK]
    bad = [s for s in parent_states if s in _DONE_FAILED]
    if run_if == "ALL_SUCCESS":
        return len(ok) == len(parent_states)
    if run_if == "ALL_DONE":
        return True
    if run_if == "NONE_FAILED":
        return not bad
    if run_if == "AT_LEAST_ONE_SUCCESS":
        return bool(ok)
    if run_if == "ALL_FAILED":
        return len(bad) == len(parent_states)
    if run_if == "AT_LEAST_ONE_FAILED":
        return bool(bad)
    raise TaskError(f"run_if {run_if!r} is not one of {', '.join(RUN_IF)}")


def evaluate_condition(op: str, left: Any, right: Any) -> bool:
    """condition_task, with the rules of the Databricks docs (jobs/if-else):
    EQUAL_TO and NOT_EQUAL compare the operands as strings ("12.0 == 12" is
    false); the four ordering operators compare them as numbers and fail
    when one is not a number. A boolean task value is "true" / "false"."""
    def text(v):
        if isinstance(v, bool):
            return "true" if v else "false"
        return str(v)

    a, b = text(left), text(right)
    if op == "EQUAL_TO":
        return a == b
    if op == "NOT_EQUAL":
        return a != b
    if op in ("GREATER_THAN", "GREATER_THAN_OR_EQUAL", "LESS_THAN", "LESS_THAN_OR_EQUAL"):
        try:
            x, y = float(a), float(b)
        except ValueError as exc:
            raise TaskError(f"condition {op}: {a!r} and {b!r} must both be numbers") from exc
        return {"GREATER_THAN": x > y, "GREATER_THAN_OR_EQUAL": x >= y,
                "LESS_THAN": x < y, "LESS_THAN_OR_EQUAL": x <= y}[op]
    raise TaskError(f"condition op {op!r} is not one of EQUAL_TO, NOT_EQUAL, GREATER_THAN, "
                    "GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL")


def dependency_state(dep: dict, results: dict) -> str:
    """One depends_on entry seen from the child: the parent's end state,
    with an `outcome` that the parent's condition did not take turned into
    "excluded"."""
    parent = results.get(dep.get("task_key", ""), {})
    state = parent.get("result_state", "excluded")
    want = dep.get("outcome")
    if want is not None and state == "success":
        if str(parent.get("outcome", "")).lower() != str(want).lower():
            return "excluded"
    return state


def run_job(event: dict, context, handler, progress=None) -> dict:
    """Run every task of a bundle job in dependency order, with run_if,
    outcomes, retries and timeouts (ADR-031 section 4). `handler` runs one
    task attempt (runner.handler: it writes the node_runs row and the
    progress marker); `progress(node, payload)` writes a terminal marker for
    a task that did not run. Returns the pipeline_handler response."""
    job = event.get("_bundle_job") or {}
    order = job.get("order") or [t.get("node") for t in event.get("transforms", [])]
    entries = {t.get("node"): t for t in event.get("transforms", []) if isinstance(t, dict)}
    results = []
    # A mixed job's earlier segments ran elsewhere (ADR-031 section 8):
    # their end states come in with the event.
    states: dict[str, dict] = {k: dict(v) for k, v in (event.get("_bundle_states") or {}).items()}
    overall, failed_node = "ok", None
    run_id = str(job.get("run_id") or event.get("_sf_execution_arn") or "")
    disabled = set(job.get("disabled") or [])
    for k, st in states.items():
        _state_of(run_id).results.setdefault(k, st)

    for key in order:
        if key in states:
            continue  # ran in an earlier segment
        entry = entries.get(key)
        if entry is None:
            if key in disabled or not job.get("segments"):
                states[key] = {"result_state": "disabled"}
            continue  # a task of another segment
        task_entry = entry.get("task") or {}
        if task_entry.get("place") == "databricks":
            # A local run of a mixed job: the Databricks segment runs here,
            # in the job deployed to Databricks.
            seg = [k for k in order if (entries.get(k) or {}).get("task", {}).get("place") == "databricks"
                   and (entries.get(k) or {}).get("task", {}).get("segment") == task_entry.get("segment")]
            seg_states = run_databricks_segment(job, seg, states, entries)
            for k in seg:
                st = seg_states.get(k, {"result_state": "excluded"})
                states[k] = st
                _state_of(run_id).results[k] = st
                status = {"success": "ok", "excluded": "skipped", "disabled": "skipped"}.get(st["result_state"], "failed")
                results.append({"node": k, "status": status, "note": f"on Databricks: {st['result_state']}"})
                if status == "failed":
                    overall, failed_node = "failed", failed_node or k
            continue
        task_entry = entry.get("task") or {}
        task = task_entry.get("task") or {}
        deps = task.get("depends_on") or []
        dep_states = [dependency_state(d, states) for d in deps]
        # A disabled parent does not hold back its children (Databricks
        # treats it as skipped successfully).
        dep_states = ["success" if s == "disabled" else s for s in dep_states]
        if not should_run(task.get("run_if", "ALL_SUCCESS"), dep_states):
            st = "upstream_failed" if any(s in _DONE_FAILED for s in dep_states) else "excluded"
            states[key] = {"result_state": st}
            _record_state(run_id, key, st)
            if progress:
                progress(key, st)
            results.append({"node": key, "status": "skipped", "note": f"{st} (run_if {task.get('run_if', 'ALL_SUCCESS')})", "timing": None})
            continue

        retries = task.get("max_retries", 0) or 0
        if retries < 0 or retries > 10:
            retries = 10  # -1 (forever) is capped; the plan warns
        interval = (task.get("min_retry_interval_millis") or 0) / 1000.0
        timeout = task.get("timeout_seconds") or 0
        attempt, last_exc = 0, None
        while True:
            attempt += 1
            sub = dict(entry.get("_sub_event") or {})
            sub["_bundle_task"] = task_entry
            sub["_bundle_job"] = job
            sub["_bundle_attempt"] = attempt
            sub["_bundle_timeout"] = timeout
            try:
                resp = handler(sub, context)
                last_exc = None
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                # The whole traceback goes to the log (CloudWatch on AWS):
                # the run record keeps only the class and the message.
                import traceback  # noqa: PLC0415

                print(f"[clavesa] task {key} attempt {attempt} failed:", file=sys.stderr, flush=True)
                traceback.print_exception(exc, file=sys.stderr)
                timed_out = isinstance(exc, TaskTimeout)
                if attempt > retries or (timed_out and not task.get("retry_on_timeout")):
                    break
                print(f"[clavesa] task {key} attempt {attempt} failed ({type(exc).__name__}); retrying in {interval:g}s",
                      file=sys.stderr, flush=True)
                time.sleep(interval)
        if last_exc is not None:
            st = "timedout" if isinstance(last_exc, TaskTimeout) else "failed"
            states[key] = {"result_state": st, "execution_count": attempt}
            results.append({"node": key, "status": "failed", "error_class": type(last_exc).__name__,
                            "error_msg": str(last_exc) or repr(last_exc), "attempts": attempt})
            overall, failed_node = "failed", failed_node or key
            continue  # later tasks may still run (run_if ALL_DONE and the like)
        states[key] = dict(_state_of(run_id).results.get(key, {}))
        states[key]["result_state"] = "success"  # this attempt succeeded
        states[key]["execution_count"] = attempt
        note = ""
        if "outcome" in states[key]:
            note = f"condition {states[key]['outcome']}"
        results.append({"node": key, "status": "ok", "output_rows": (resp or {}).get("output_rows"),
                        "note": note, "attempts": attempt})
    failed = any(st.get("result_state") in _DONE_FAILED for st in states.values())
    if failed and overall == "ok":
        overall = "failed"
    return {"status": overall, "transforms": results, "failed_node": failed_node,
            "states": states, "failed": failed}


class TaskTimeout(TaskError):
    """A task ran past its timeout_seconds."""


def _state_of(run_id: str) -> RunState:
    st = _STATES.get(run_id)
    if st is None:
        st = RunState({}, {}, run_id)
        _STATES[run_id] = st
    return st


def _record_state(run_id: str, key: str, state: str):
    _state_of(run_id).results[key] = {"result_state": state}


@contextlib.contextmanager
def _deadline(spark, key: str, seconds: float):
    """The task's timeout_seconds: on expiry, cancel the task's Spark jobs
    and interrupt its Python code; the task fails with TaskTimeout."""
    if not seconds:
        yield
        return
    import _thread  # noqa: PLC0415
    import threading  # noqa: PLC0415

    group = f"clavesa-task-{key}"
    fired = threading.Event()
    sc = spark.sparkContext
    sc.setJobGroup(group, f"bundle task {key}", interruptOnCancel=True)

    def expire():
        fired.set()
        with contextlib.suppress(Exception):
            sc.cancelJobGroup(group)
        _thread.interrupt_main()

    timer = threading.Timer(seconds, expire)
    timer.daemon = True
    timer.start()
    try:
        yield
    except BaseException as exc:  # noqa: BLE001
        if fired.is_set():
            raise TaskTimeout(f"task {key} ran past its timeout_seconds ({seconds:g}s)") from exc
        raise
    finally:
        timer.cancel()
        with contextlib.suppress(Exception):
            sc.setLocalProperty("spark.jobGroup.id", None)
    if fired.is_set():
        raise TaskTimeout(f"task {key} ran past its timeout_seconds ({seconds:g}s)")


def _run_for_each(spark, task: dict, ctx: dict, state: RunState, names: Names) -> dict:
    """for_each_task: the inner task once per item of inputs, one after the
    other (v1; concurrency is a plan warning). An item failure fails the
    task after the inner task's own retries."""
    fe = task["for_each_task"]
    raw = state.resolve(fe.get("inputs", "[]"), state.current_task)
    try:
        items = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError as exc:
        raise TaskError(f"for_each_task inputs is not a JSON list: {raw!r}") from exc
    if not isinstance(items, list):
        raise TaskError(f"for_each_task inputs is not a JSON list: {raw!r}")
    inner = fe.get("task") or {}
    retries = inner.get("max_retries", 0) or 0
    retries = 10 if retries < 0 or retries > 10 else retries
    for i, item in enumerate(items):
        attempt = 0
        while True:
            attempt += 1
            try:
                run_task(spark, inner, {**ctx, "item": item}, state, names)
                break
            except Exception as exc:  # noqa: BLE001
                if attempt > retries:
                    raise TaskError(f"for_each item {i} ({json.dumps(item)}) failed: {exc}") from exc
                time.sleep((inner.get("min_retry_interval_millis") or 0) / 1000.0)
    print(f"[clavesa] for_each {state.current_task}: {len(items)} items", file=sys.stderr, flush=True)
    return {"result": None}


def _run_child_job(spark, task: dict, entry: dict, job: dict, state: RunState) -> dict:
    """run_job_task on a job of the same bundle, in this runner (a local
    run; ADR-031 section 5): the child job's tasks run with its own
    parameters and the same semantics, and a failed child fails the task."""
    child = entry.get("job")
    if not child:
        raise TaskError("run_job_task: the started job is not in this bundle; a Databricks job is started by a "
                        "mixed target (not on a local run)")
    rj = task["run_job_task"]
    params = {p["name"]: str(p.get("default", "")) for p in child.get("parameters") or []}
    for k, v in (rj.get("job_parameters") or {}).items():
        params[k] = str(state.resolve(v, state.current_task))
    child_run = f"{state.run_id}/{state.current_task}"
    child_job = {k: v for k, v in child.items() if k != "tasks"}
    child_job.update({"params": params, "bundle_root": job.get("bundle_root", ""), "run_id": child_run})
    tasks = child.get("tasks") or {}
    event = {
        "_bundle_job": child_job,
        "transforms": [{"node": k, "task": t} for k, t in tasks.items() if not t.get("disabled")],
    }
    parent_task = state.current_task

    def handler(sub, _ctx):
        return run_node(spark, sub)

    print(f"[clavesa] {parent_task}: starting job {child.get('job')} in this runner", file=sys.stderr, flush=True)
    try:
        resp = run_job(event, None, handler)
    finally:
        _STATES.pop(child_run, None)
        _STATES[state.run_id] = state
        state.current_task = parent_task
    if resp["status"] != "ok":
        raise TaskError(f"job {child.get('job')} failed at task {resp['failed_node']}")
    return {"result": None}


class UnityTask:
    """catalog: unity around one task (ADR-031 sections 6 and 9, with the
    ADR-027 machinery in unity_catalog.py): before the task, each table it
    writes is prepared (a new table, or an existing external table taken
    over in place; the shadow name in shadow mode); tables it reads that
    the job does not write are read from Unity Catalog; after the task,
    each written table is published (registered, or checked) and its
    lineage pushed with the task as its own box."""

    def __init__(self, spark, names: Names, job: dict, key: str, uc_module=None, client=None, cfg=None):
        if uc_module is None:
            import unity_catalog as uc_module  # noqa: PLC0415
        self.uc = uc_module
        self.cfg = cfg if cfg is not None else self.uc.config()
        if self.cfg is None:
            raise TaskError(
                "catalog: unity needs a run on the cloud warehouse of a workspace connected to Databricks "
                "(clavesa workspace connect databricks, then clavesa bundle deploy); a local warehouse has no "
                "Unity Catalog. Use a target with catalog: glue for local runs."
            )
        self.client = client if client is not None else self.uc.Client(self.cfg)
        self.spark, self.names, self.job, self.key = spark, names, job, key
        self.ctx: dict[str, dict] = {}
        self.vended: list[str] = []
        names.uc_reader = self._read

    def _read(self, parts: list[str]) -> str:
        name = ".".join(parts)
        view = "__uc_" + re.sub(r"[^A-Za-z0-9_]", "_", name)
        if view not in self.ctx.get("__views__", {}):
            df, info = self.uc.open_table(self.spark, self.client, name)
            df.createOrReplaceTempView(view)
            self.ctx.setdefault("__views__", {})[view] = name
            if info.get("vended"):
                self.vended.append(info["location"])
        return f"`{view}`"

    def before(self):
        spec = {}
        if self.job.get("replaces_job"):
            spec["replaces_job"] = int(self.job["replaces_job"])
        for w in sorted(self.names.writes):
            table_id = self.names.physical(split_name(w)).replace("`", "")
            db = table_id.split(".")[0]
            loc = database_location(db)
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`" + (f" LOCATION '{loc}'" if loc else ""))
            self.ctx[w] = self.uc.prepare(self.client, self.cfg, self.spark, table_id,
                                          {**spec, "uc_table": self.names.uc_name(w)})

    def after(self, reads: list[str]) -> list[dict]:
        recs = []
        for loc in self.vended:
            with contextlib.suppress(Exception):
                self.uc.set_bucket_credentials(self.spark, self.uc.bucket_of(loc), None)
        inputs = {r: {"kind": "uc_table", "table": self.names.uc_name(r) if r.lower() in self.names.job_writes else r}
                  for r in reads if r.lower() not in self.names.writes}
        for w in sorted(self.names.writes):
            table_id = self.names.physical(split_name(w)).replace("`", "")
            rec = self.uc.publish(self.client, self.cfg, self.spark, table_id, self.ctx[w])
            if self.cfg.get("lineage"):
                try:
                    self.uc.push_lineage(self.client, self.cfg, inputs, rec["full_name"],
                                         self.spark.table(table_id).schema.fieldNames())
                    rec["lineage"] = "pushed"
                except Exception as exc:  # noqa: BLE001
                    rec["lineage"] = f"failed: {exc}"
                    print(f"[clavesa] unity catalog: lineage push failed: {exc}", file=sys.stderr, flush=True)
            recs.append({"output": w, **rec})
        return recs


# ---------------------------------------------------------------------------
# Databricks segments of a mixed job (ADR-031 section 8).
# ---------------------------------------------------------------------------

# Databricks task result states, as the job loop names them.
_DBX_STATES = {
    "SUCCESS": "success", "SUCCESS_WITH_FAILURES": "success", "FAILED": "failed", "TIMEDOUT": "timedout",
    "CANCELED": "failed", "MAXIMUM_CONCURRENT_RUNS_REACHED": "failed", "EXCLUDED": "excluded",
    "UPSTREAM_FAILED": "upstream_failed", "UPSTREAM_CANCELED": "upstream_failed", "SKIPPED": "excluded",
    "DISABLED": "disabled",
}
_DBX_DONE = ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")


class DatabricksJobs:
    """The two Jobs API calls a Databricks segment needs. Signs in as the
    workspace's service principal (the ADR-027 connection, on a deployed
    pipeline) or with DATABRICKS_HOST / DATABRICKS_TOKEN (a local run)."""

    def __init__(self):
        import unity_catalog as uc  # noqa: PLC0415

        host = os.environ.get("CLAVESA_DATABRICKS_HOST", "").rstrip("/")
        if host and os.environ.get("CLAVESA_DATABRICKS_CLIENT_ID"):
            self._client = uc.Client({"host": host, "client_id": os.environ["CLAVESA_DATABRICKS_CLIENT_ID"]})
            self.call = self._client.call
            return
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        if not (host and token):
            raise TaskError("a task of this job runs on Databricks: connect the workspace to Databricks "
                            "(clavesa workspace connect databricks) or, for a local run, export DATABRICKS_HOST "
                            "and DATABRICKS_TOKEN")
        if not host.startswith("http"):
            host = "https://" + host
        self._host, self._token = host, token

    def call(self, method, path, body=None):
        import urllib.request  # noqa: PLC0415

        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self._host + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self._token)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
        except Exception as exc:  # noqa: BLE001
            detail = getattr(exc, "read", lambda: b"")()[:400]
            raise TaskError(f"{method} {path}: {exc} {detail!r}") from exc
        return json.loads(raw) if raw else {}


def segment_runnable(job_tasks: dict, seg: list[str], states: dict) -> list[str]:
    """The tasks of a Databricks segment to start: each task whose
    dependencies outside the segment let it run (run_if, outcome)."""
    seg_set = set(seg)
    out = []
    for k in seg:
        task = (job_tasks.get(k) or {}).get("task") or {}
        outside = [d for d in task.get("depends_on") or [] if d.get("task_key") not in seg_set]
        dep_states = ["success" if s == "disabled" else s for s in (dependency_state(d, states) for d in outside)]
        if should_run(task.get("run_if", "ALL_SUCCESS"), dep_states):
            out.append(k)
    return out


def dbx_start(api, job: dict, seg: list[str], states: dict, job_tasks: dict, params: dict | None):
    """run-now on the deployed job with only the segment's tasks. Returns
    the run id, or 0 with end states when nothing of the segment runs."""
    only = segment_runnable(job_tasks, seg, states)
    if not only:
        return 0
    body = {"job_id": int(job["databricks_job_id"]), "only": only}
    if params:
        body["job_parameters"] = params
    out = api.call("POST", "/api/2.2/jobs/run-now", body)
    print(f"[clavesa] Databricks segment {seg}: run-now job {job['databricks_job_id']} only {only} -> run {out.get('run_id')}",
          file=sys.stderr, flush=True)
    return int(out["run_id"])


def dbx_status(api, run_id: int, seg: list[str]):
    """(done, {task: state}) of a Databricks run."""
    run = api.call("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
    life = (run.get("state") or {}).get("life_cycle_state") or (run.get("status") or {}).get("state", "")
    done = life in _DBX_DONE or life == "TERMINATED"
    states = {}
    for t in run.get("tasks") or []:
        k = t.get("task_key")
        if k not in seg:
            continue
        rs = (t.get("state") or {}).get("result_state") or ""
        if rs:
            st = {"result_state": _DBX_STATES.get(rs, "failed")}
            outcome = (t.get("condition_task") or {}).get("outcome")
            if outcome is not None:
                st["outcome"] = str(outcome).lower()
            states[k] = st
    if done:
        for k in seg:
            states.setdefault(k, {"result_state": "excluded"})
    return done, states


def run_databricks_segment(job, seg, states, entries, api=None, poll_seconds=20.0):
    """A local run's Databricks segment, start to end, in this process."""
    if not job.get("databricks_job_id"):
        raise TaskError(f"tasks {seg} run on Databricks, but the job file has no deployed Databricks job: "
                        "run databricks bundle deploy for the target, then clavesa bundle run again")
    api = api or DatabricksJobs()
    run_id = dbx_start(api, job, seg, states, {k: entries[k].get("task") for k in seg}, None)
    if not run_id:
        return {k: {"result_state": "excluded"} for k in seg}
    while True:
        done, seg_states = dbx_status(api, run_id, seg)
        if done:
            return seg_states
        time.sleep(poll_seconds)


def dbx_operation(event: dict, api=None) -> dict:
    """The Step Functions side of a Databricks segment (the runner Lambda,
    no Spark): dbx_run_now starts it, dbx_run_get polls it. Both return the
    job's task states with the segment's merged in, failed, run_id, done."""
    states = {k: dict(v) for k, v in (event.get("states") or {}).items()}
    seg = list(event.get("only") or [])
    op = event.get("_operation")
    api = api or DatabricksJobs()
    run_id = int(event.get("run_id") or 0)
    if op == "dbx_run_now":
        prepared = prepare_cloud_event({"transforms": [{"node": k, "language": "task", "logic_path": event.get("logic_path", "")}
                                                       for k in seg], "_sf_execution_arn": event.get("_sf_execution_arn"),
                                        "_execution_input": event.get("_execution_input")}, files=False)
        job = prepared["_bundle_job"]
        job_tasks = {t["node"]: t.get("task") for t in prepared["transforms"]}
        params = (event.get("_execution_input") or {}).get("params") or None
        run_id = dbx_start(api, {**job, "databricks_job_id": event.get("job_id")}, seg, states, job_tasks, params)
        if not run_id:
            for k in seg:
                states[k] = {"result_state": "excluded"}
            return {"states": states, "failed": _any_failed(states), "run_id": 0, "done": True}
        return {"states": states, "failed": _any_failed(states), "run_id": run_id, "done": False}
    if op == "dbx_run_get":
        done, seg_states = dbx_status(api, run_id, seg)
        if done:
            states.update(seg_states)
            print(f"[clavesa] Databricks run {run_id} ended: {seg_states}", file=sys.stderr, flush=True)
        return {"states": states, "failed": _any_failed(states), "run_id": run_id, "done": done}
    raise TaskError(f"unknown operation {op!r}")


def _any_failed(states: dict) -> bool:
    return any(st.get("result_state") in _DONE_FAILED for st in states.values())
