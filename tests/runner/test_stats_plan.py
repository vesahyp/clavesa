"""The column stats plan of runner/runner.py: which outputs get the stats
pass, and that a node which read a full snapshot (a first or forced run)
skips it. Also checks that _release_stats_cache unpersists every cached
frame once. Pure Python: runner.py is imported with its native deps stubbed
(test_merge_bound._load_runner). Run with plain python3 (make test-runner-py).
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_merge_bound import runner  # noqa: E402


class StatsPlanTest(unittest.TestCase):
    def test_opt_in_delta_only(self):
        self.assertEqual(runner._stats_plan({"stats": True, "kind": "delta_table"}, False, []), (True, ""))
        self.assertEqual(runner._stats_plan({"stats": False, "kind": "delta_table"}, False, []), (False, ""))
        self.assertEqual(runner._stats_plan({"stats": True, "kind": "path"}, False, []), (False, ""))

    def test_backfill_staging_never(self):
        self.assertEqual(runner._stats_plan({"stats": True, "kind": "delta_table"}, True, []), (False, ""))

    def test_full_snapshot_skips_with_a_line(self):
        want, why = runner._stats_plan({"stats": True, "kind": "delta_table"}, False, ["enriched"])
        self.assertFalse(want)
        self.assertIn("column stats skipped", why)
        self.assertIn("enriched", why)

    def test_full_snapshot_without_stats_says_nothing(self):
        self.assertEqual(runner._stats_plan({"stats": False, "kind": "delta_table"}, False, ["enriched"]), (False, ""))


class ReleaseStatsCacheTest(unittest.TestCase):
    def test_unpersists_each_once(self):
        calls = []

        class Frame:
            def __init__(self, name):
                self.name = name

            def unpersist(self):
                calls.append(self.name)

        class Broken:
            def unpersist(self):
                raise RuntimeError("session gone")

        runner._STATS_CACHED.extend([Frame("a"), Broken(), Frame("b")])
        runner._release_stats_cache()
        runner._release_stats_cache()
        self.assertEqual(sorted(calls), ["a", "b"])
        self.assertEqual(runner._STATS_CACHED, [])


if __name__ == "__main__":
    unittest.main()
