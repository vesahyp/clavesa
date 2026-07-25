"""Local validation for the csv/tsv schema-inference safety helpers.

Run with: python3 -m pytest tests/runner/test_read_schema.py -v
   or:    python3 tests/runner/test_read_schema.py

Exercises the pure-Python helpers in runner/runner.py without any Spark:
  - _string_safe_schema: inferred date/time-family columns demote to string
    (Spark's lenient inference rewrites e.g. a time-only "07:36:42" into a
    timestamp anchored to the read date); numeric/string typing is kept
  - _read_delimited_two_pass: re-reads only when demotion is needed, applies
    the `columns` rename after the final read

pyspark/boto3/botocore are stubbed so runner.py imports without native deps;
pyspark.sql.types carries minimal StructType/StructField/StringType fakes
compatible with the helpers.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


class _DataType:
    _type_name = ""

    def typeName(self):  # noqa: N802 — mirrors pyspark's API
        return self._type_name


def _dtype(name: str) -> _DataType:
    t = _DataType()
    t._type_name = name
    return t


class _StringType(_DataType):
    _type_name = "string"


class _StructField:
    def __init__(self, name, dataType, nullable=True):  # noqa: N803
        self.name = name
        self.dataType = dataType
        self.nullable = nullable


class _StructType:
    def __init__(self, fields=None):
        self.fields = list(fields or [])


def _load_runner():
    """Import runner.py with boto3/pyspark/botocore stubbed so the pure
    helpers are importable without the runner image."""
    boto3_mod = types.ModuleType("boto3")
    boto3_mod.client = lambda *a, **k: None  # type: ignore[attr-defined]

    botocore_mod = types.ModuleType("botocore")
    botocore_exceptions = types.ModuleType("botocore.exceptions")

    class _ClientError(Exception):
        def __init__(self, response):
            self.response = response
            super().__init__(response)

    botocore_exceptions.ClientError = _ClientError
    botocore_mod.exceptions = botocore_exceptions

    pyspark_mod = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")

    class _DataFrame:
        pass

    pyspark_sql.DataFrame = _DataFrame
    pyspark_sql.SparkSession = object

    pyspark_types = types.ModuleType("pyspark.sql.types")
    pyspark_types.StringType = _StringType  # type: ignore[attr-defined]
    pyspark_types.StructField = _StructField  # type: ignore[attr-defined]
    pyspark_types.StructType = _StructType  # type: ignore[attr-defined]
    pyspark_sql.types = pyspark_types
    pyspark_mod.sql = pyspark_sql

    sys.modules["boto3"] = boto3_mod
    sys.modules["botocore"] = botocore_mod
    sys.modules["botocore.exceptions"] = botocore_exceptions
    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.types"] = pyspark_types

    spec = importlib.util.spec_from_file_location("runner", str(RUNNER))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


runner = _load_runner()


# ---------------------------------------------------------------------------
# _string_safe_schema
# ---------------------------------------------------------------------------


def test_string_safe_schema_demotes_date_time_family():
    schema = _StructType([
        _StructField("d", _dtype("date")),
        _StructField("ts", _dtype("timestamp")),
        _StructField("ts_ntz", _dtype("timestamp_ntz")),
        _StructField("t", _dtype("time")),
    ])
    out = runner._string_safe_schema(schema)
    assert out is not None
    assert [f.name for f in out.fields] == ["d", "ts", "ts_ntz", "t"]
    for f in out.fields:
        assert f.dataType.typeName() == "string"
        assert f.nullable is True


def test_string_safe_schema_keeps_other_types():
    n = _StructField("n", _dtype("integer"))
    x = _StructField("x", _dtype("double"))
    s = _StructField("s", _dtype("string"))
    ts = _StructField("ts", _dtype("timestamp"))
    out = runner._string_safe_schema(_StructType([n, x, s, ts]))
    assert out is not None
    # untouched fields pass through as the same objects, types intact
    assert out.fields[0] is n
    assert out.fields[1] is x
    assert out.fields[2] is s
    assert out.fields[3].dataType.typeName() == "string"


def test_string_safe_schema_none_when_nothing_to_demote():
    schema = _StructType([
        _StructField("n", _dtype("integer")),
        _StructField("s", _dtype("string")),
        _StructField("b", _dtype("boolean")),
    ])
    assert runner._string_safe_schema(schema) is None


# ---------------------------------------------------------------------------
# _read_delimited_two_pass
# ---------------------------------------------------------------------------


class _FakeDF:
    def __init__(self, schema, label=""):
        self.schema = schema
        self.label = label
        self.renamed_to = None

    def toDF(self, *names):  # noqa: N802 — mirrors pyspark's API
        self.renamed_to = list(names)
        return self


def _factory(schemas_seen, clean_schema):
    """reader_factory recording each call's schema arg. Pass 1 (schema=None)
    returns a df with `clean_schema`; a re-read returns a df carrying the
    explicit schema it was given."""

    def factory(schema):
        schemas_seen.append(schema)
        return _FakeDF(clean_schema if schema is None else schema, label=f"pass{len(schemas_seen)}")

    return factory


def test_two_pass_single_read_when_clean():
    schemas_seen: list = []
    clean = _StructType([_StructField("n", _dtype("integer")), _StructField("s", _dtype("string"))])
    df = runner._read_delimited_two_pass(_factory(schemas_seen, clean), {})
    assert schemas_seen == [None]  # no re-read
    assert df.schema is clean


def test_two_pass_rereads_with_demoted_schema():
    schemas_seen: list = []
    inferred = _StructType([_StructField("tm", _dtype("timestamp")), _StructField("n", _dtype("integer"))])
    df = runner._read_delimited_two_pass(_factory(schemas_seen, inferred), {})
    assert len(schemas_seen) == 2
    assert schemas_seen[0] is None
    assert schemas_seen[1] is not None
    assert df.schema.fields[0].dataType.typeName() == "string"
    assert df.schema.fields[1].dataType.typeName() == "integer"


def test_two_pass_columns_rename_after_final_read():
    schemas_seen: list = []
    inferred = _StructType([_StructField("_c0", _dtype("date")), _StructField("_c1", _dtype("integer"))])
    df = runner._read_delimited_two_pass(
        _factory(schemas_seen, inferred), {"columns": "day, bytes"}
    )
    assert len(schemas_seen) == 2
    assert df.renamed_to == ["day", "bytes"]  # rename applied to the re-read df
    assert df.label == "pass2"


# ---------------------------------------------------------------------------
# Test runner: prints PASS/FAIL summary when invoked directly.
# ---------------------------------------------------------------------------


def _all_tests():
    g = globals()
    return [(name, fn) for name, fn in sorted(g.items()) if name.startswith("test_") and callable(fn)]


def main():
    passed = 0
    failed: list[tuple[str, str]] = []
    for name, fn in _all_tests():
        try:
            fn()
        except Exception as e:
            failed.append((name, f"{type(e).__name__}: {e}"))
            print(f"FAIL  {name}  →  {type(e).__name__}: {e}")
        else:
            passed += 1
            print(f"PASS  {name}")
    total = passed + len(failed)
    print(f"\n{passed}/{total} passed", "❌" if failed else "✅")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
