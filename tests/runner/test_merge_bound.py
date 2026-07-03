"""Local validation for the MERGE scan-bound helpers (Tier 1 + Tier 2).

Run with: python3 -m pytest tests/runner/test_merge_bound.py -v
   or:    python3 tests/runner/test_merge_bound.py

Exercises the pure-Python helpers in runner/runner.py without any Spark:
  - _sql_lit: Python value -> Spark-SQL literal (or None for unsafe types)
  - _bound_predicate_sql: distinct source values -> IN / BETWEEN predicate
  - _merge_bound_cols: merge_keys ∩ skipping columns (Tier 1) + bound_by (Tier 2)
  - _resolve_output: bound_by round-trips off the dict descriptor

pyspark/boto3/botocore are stubbed so runner.py imports without native deps.
"""

from __future__ import annotations

import datetime
import decimal
import importlib.util
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUNNER = REPO / "runner" / "runner.py"


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
    pyspark_mod.sql = pyspark_sql

    sys.modules["boto3"] = boto3_mod
    sys.modules["botocore"] = botocore_mod
    sys.modules["botocore.exceptions"] = botocore_exceptions
    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = pyspark_sql

    spec = importlib.util.spec_from_file_location("runner", str(RUNNER))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


runner = _load_runner()


# ---------------------------------------------------------------------------
# _sql_lit
# ---------------------------------------------------------------------------


def test_sql_lit_int():
    assert runner._sql_lit(42) == "42"
    assert runner._sql_lit(-7) == "-7"


def test_sql_lit_bool_not_int():
    # bool is an int subclass; must render as true/false, NOT 1/0.
    assert runner._sql_lit(True) == "true"
    assert runner._sql_lit(False) == "false"


def test_sql_lit_date():
    assert runner._sql_lit(datetime.date(2026, 6, 29)) == "DATE '2026-06-29'"


def test_sql_lit_datetime():
    dt = datetime.datetime(2026, 6, 29, 13, 5, 9)
    assert runner._sql_lit(dt) == "TIMESTAMP '2026-06-29 13:05:09'"
    # datetime must be matched before date (datetime is a date subclass).
    assert not runner._sql_lit(dt).startswith("DATE")


def test_sql_lit_datetime_microseconds():
    dt = datetime.datetime(2026, 6, 29, 13, 5, 9, 123456)
    assert runner._sql_lit(dt) == "TIMESTAMP '2026-06-29 13:05:09.123456'"


def test_sql_lit_str_quote_doubling():
    assert runner._sql_lit("o'brien") == "'o''brien'"
    assert runner._sql_lit("plain") == "'plain'"


def test_sql_lit_str_backslash_escaped():
    # Spark's default parser processes C-style escapes inside string literals,
    # so a raw backslash must be doubled or the literal != the value (GH #70).
    assert runner._sql_lit("C:\\temp") == "'C:\\\\temp'"
    assert runner._sql_lit("a\\b") == "'a\\\\b'"


def test_sql_lit_str_backslash_then_quote():
    # value: a\'b — backslash doubled first, THEN quote doubled; wrong order
    # would let the doubled backslash swallow the quote escape.
    assert runner._sql_lit("a\\'b") == "'a\\\\''b'"


def test_sql_lit_str_trailing_backslash():
    # A trailing backslash must not escape the closing quote.
    assert runner._sql_lit("dir\\") == "'dir\\\\'"


def test_sql_lit_float_finite_vs_inf():
    assert runner._sql_lit(1.5) == repr(1.5)
    assert runner._sql_lit(float("inf")) is None
    assert runner._sql_lit(float("-inf")) is None
    assert runner._sql_lit(float("nan")) is None


def test_sql_lit_decimal():
    assert runner._sql_lit(decimal.Decimal("3.14")) == "3.14"


def test_sql_lit_decimal_nonfinite_is_none():
    # str(Decimal("NaN")) is the bare token NaN, which Spark parses as a
    # column reference — must be skipped like non-finite floats.
    assert runner._sql_lit(decimal.Decimal("NaN")) is None
    assert runner._sql_lit(decimal.Decimal("Infinity")) is None
    assert runner._sql_lit(decimal.Decimal("-Infinity")) is None


def test_sql_lit_none_and_unknown():
    assert runner._sql_lit(None) is None
    assert runner._sql_lit(object()) is None
    assert runner._sql_lit([1, 2]) is None


# ---------------------------------------------------------------------------
# _bound_predicate_sql
# ---------------------------------------------------------------------------


def test_bound_predicate_small_in_list():
    p = runner._bound_predicate_sql("domain", ["a.com", "b.com"])
    assert p == "target.`domain` IN ('a.com', 'b.com')"


def test_bound_predicate_any_unrenderable_skips_column():
    # A partially rendered IN list is unsound (it can exclude a target row the
    # un-bounded MERGE would match) — any unrenderable value skips the whole
    # column, never just that value (GH #70).
    assert runner._bound_predicate_sql("k", [1, object(), 3]) is None


def test_bound_predicate_nan_float_skips_column():
    # Spark treats NaN = NaN as true, so a NaN key CAN match in the un-bounded
    # MERGE; dropping it from the IN list would silently duplicate that row.
    assert runner._bound_predicate_sql("k", [1.5, float("nan"), 2.5]) is None
    assert runner._bound_predicate_sql("k", [1.5, float("inf")]) is None


def test_bound_predicate_nonfinite_decimal_skips_column():
    assert runner._bound_predicate_sql("k", [decimal.Decimal("1"), decimal.Decimal("NaN")]) is None


def test_bound_predicate_backslash_values_render_escaped():
    p = runner._bound_predicate_sql("path", ["C:\\a", "C:\\b"])
    assert p == "target.`path` IN ('C:\\\\a', 'C:\\\\b')"


def test_bound_predicate_empty_is_none():
    assert runner._bound_predicate_sql("k", []) is None


def test_bound_predicate_all_untypeable_is_none():
    assert runner._bound_predicate_sql("k", [object(), [1]]) is None


def test_bound_predicate_large_set_between():
    # threshold lowered so we don't need 201 values to hit the BETWEEN branch.
    p = runner._bound_predicate_sql("k", [3, 1, 2], threshold=2)
    assert p == "target.`k` BETWEEN 1 AND 3"


def test_bound_predicate_force_between_true_bounds():
    p = runner._bound_predicate_sql("event_date", [datetime.date(2026, 6, 1), datetime.date(2026, 6, 30)], force_between=True)
    assert p == "target.`event_date` BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'"


def test_bound_predicate_between_mixed_types_none():
    # min/max over mixed types is a TypeError -> None.
    assert runner._bound_predicate_sql("k", [1, "x", 2], threshold=1) is None


# ---------------------------------------------------------------------------
# _merge_bound_cols
# ---------------------------------------------------------------------------


def test_merge_bound_cols_random_pk_no_overlap():
    # request_id is the merge key but not a skipping column -> Tier 1 no-op.
    assert runner._merge_bound_cols(["request_id"], ["event_date", "domain"]) == []


def test_merge_bound_cols_preserves_merge_key_order():
    got = runner._merge_bound_cols(["domain", "event_date", "dim"], ["event_date", "domain"])
    assert got == ["domain", "event_date"]


def test_merge_bound_cols_defaults_to_merge_keys():
    # No explicit cluster_by: a merge table clusters by its merge_keys.
    assert runner._merge_bound_cols(["a", "b"], []) == ["a", "b"]


def test_merge_bound_cols_dedup():
    assert runner._merge_bound_cols(["a", "a", "b"], []) == ["a", "b"]


def test_merge_bound_cols_caps_at_max_cluster_cols():
    # _create_delta_table clusters at most 4 columns; keys 5+ can never prune,
    # so they must not be bounded (each would cost a collect job for nothing).
    assert runner._MAX_CLUSTER_COLS == 4
    got = runner._merge_bound_cols(["a", "b", "c", "d", "e"], [])
    assert got == ["a", "b", "c", "d"]


def test_merge_bound_cols_cap_applies_to_explicit_cluster_by():
    # Only the first 4 of an explicit cluster_by are actually clustered; a
    # merge key appearing at position 5 is not a skipping column.
    got = runner._merge_bound_cols(["e"], ["a", "b", "c", "d", "e"])
    assert got == []


def test_merge_bound_cols_bound_by_not_capped():
    # Tier 2 is the author's explicit opt-in — appended even beyond the cap
    # (a non-skipping bound column is safe; it just may not prune).
    got = runner._merge_bound_cols(["a", "b", "c", "d", "e"], [], ["e"])
    assert got == ["a", "b", "c", "d", "e"]


# ---------------------------------------------------------------------------
# _merge_bound_cols — Tier 2 bound_by
# ---------------------------------------------------------------------------


def test_merge_bound_cols_bound_by_random_pk_facts():
    # Random PK is the only merge key (not clustered -> Tier 1 empty); the
    # author asserts event_date is determined by it via bound_by.
    got = runner._merge_bound_cols(
        ["x_edge_request_id"], ["event_date", "website_domain"], ["event_date"]
    )
    assert got == ["event_date"]


def test_merge_bound_cols_bound_by_not_duplicated_when_already_tier1():
    # event_date is both a clustered merge key (Tier 1) AND named in bound_by;
    # it must appear once, in Tier-1 order.
    got = runner._merge_bound_cols(
        ["event_date", "request_id"], ["event_date"], ["event_date"]
    )
    assert got == ["event_date"]


def test_merge_bound_cols_bound_by_not_in_cluster_by_still_included():
    # bound_by is unconditional — included even if absent from cluster_by.
    got = runner._merge_bound_cols(["request_id"], ["something_else"], ["event_date"])
    assert got == ["event_date"]


def test_merge_bound_cols_bound_by_appends_after_tier1():
    # Tier-1 columns come first (merge_keys order), then bound_by extras.
    got = runner._merge_bound_cols(
        ["domain", "request_id"], ["domain", "event_date"], ["event_date"]
    )
    assert got == ["domain", "event_date"]


def test_merge_bound_cols_bound_by_dedups_within_bound_by():
    # cluster_by names a non-key column so Tier 1 is empty; the repeated
    # bound_by entry must collapse to one.
    got = runner._merge_bound_cols(["request_id"], ["other"], ["event_date", "event_date"])
    assert got == ["event_date"]


def test_merge_bound_cols_bound_by_none_is_tier1():
    # bound_by=None (the default) behaves exactly like Tier 1.
    assert runner._merge_bound_cols(["request_id"], ["event_date"], None) == []
    assert runner._merge_bound_cols(["a", "b"], [], None) == ["a", "b"]


# ---------------------------------------------------------------------------
# _resolve_output — bound_by round-trip (table_id given so no env touched)
# ---------------------------------------------------------------------------


def test_resolve_output_bound_by_round_trips():
    s = runner._resolve_output(
        "default",
        {
            "kind": "delta_table",
            "table_id": "db.t",
            "mode": "merge",
            "merge_keys": ["x_edge_request_id"],
            "cluster_by": ["event_date", "website_domain"],
            "bound_by": ["event_date"],
        },
    )
    assert s["bound_by"] == ["event_date"]


def test_resolve_output_bound_by_absent_is_empty():
    s = runner._resolve_output("default", {"kind": "delta_table", "table_id": "db.t"})
    assert s["bound_by"] == []


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
