"""Unit tests for the garbage collector choice in runner/spark-class
(docs/ideas/runner-memory.md).

Run with: python3 tests/runner/test_spark_class_gc.py

spark-class picks Serial GC for a heap of 2 GB or less and leaves larger
heaps on the JVM default (G1). The Lambda memory size does not change the
choice. CLAVESA_JVM_GC overrides it. The script is run with a stand-in java,
first on PATH, that prints the arguments it gets. Stdlib only.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SPARK_CLASS = REPO / "runner" / "spark-class"


def run(env_extra: dict[str, str]) -> list[str]:
    """Run spark-class with a fake java; return the args of the exec'd JVM."""
    with tempfile.TemporaryDirectory() as d:
        fake = Path(d) / "java"
        fake.write_text('#!/bin/sh\n[ "$1" = "-version" ] && exit 0\nfor a in "$@"; do echo "ARG:$a"; done\n')
        fake.chmod(0o755)
        env = {"PATH": f"{d}:/usr/bin:/bin"}
        env.update(env_extra)
        out = subprocess.run(["bash", str(SPARK_CLASS), "org.example.Main"], env=env,
                             capture_output=True, text=True, check=True).stdout
        return [l[4:] for l in out.splitlines() if l.startswith("ARG:")]


def gc_flags(args: list[str]) -> list[str]:
    return [a for a in args if a.startswith("-XX:+Use") and a.endswith("GC")]


class SparkClassGC(unittest.TestCase):
    def test_lambda_3008_with_capped_heap_is_serial(self):
        args = run({"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "3008", "CLAVESA_JVM_HEAP_MB": "1024"})
        self.assertEqual(gc_flags(args), ["-XX:+UseSerialGC"])
        self.assertIn("-Xmx1024m", args)
        self.assertEqual(args[-1], "org.example.Main")

    def test_small_lambda_with_heap_above_2_gb_keeps_g1(self):
        args = run({"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "3008", "CLAVESA_JVM_HEAP_MB": "2600"})
        self.assertEqual(gc_flags(args), [])

    def test_uses_the_java_on_path(self):
        args = run({"CLAVESA_JVM_HEAP_MB": "1024"})
        self.assertIn("-Xmx1024m", args)

    def test_large_lambda_with_large_heap_keeps_g1(self):
        args = run({"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "10240", "CLAVESA_JVM_HEAP_MB": "7680"})
        self.assertEqual(gc_flags(args), [])

    def test_large_lambda_with_small_heap_is_serial(self):
        args = run({"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "10240", "CLAVESA_JVM_HEAP_MB": "1024"})
        self.assertEqual(gc_flags(args), ["-XX:+UseSerialGC"])

    def test_fargate_style_heap_above_2_gb_keeps_g1(self):
        args = run({"CLAVESA_JVM_HEAP_MB": "3072"})
        self.assertEqual(gc_flags(args), [])

    def test_heap_of_exactly_2_gb_is_serial(self):
        self.assertEqual(gc_flags(run({"CLAVESA_JVM_HEAP_MB": "2048"})), ["-XX:+UseSerialGC"])

    def test_override_g1_on_small_runner(self):
        args = run({"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "3008", "CLAVESA_JVM_HEAP_MB": "1024", "CLAVESA_JVM_GC": "g1"})
        self.assertEqual(gc_flags(args), ["-XX:+UseG1GC"])

    def test_override_serial_on_large_runner(self):
        args = run({"CLAVESA_JVM_HEAP_MB": "7680", "CLAVESA_JVM_GC": "serial"})
        self.assertEqual(gc_flags(args), ["-XX:+UseSerialGC"])


if __name__ == "__main__":
    unittest.main()
