"""Fails on the first attempt of a run, passes on the retry."""
import os
import sys

marker = f"/tmp/clavesa-flaky-{sys.argv[1]}"
if not os.path.exists(marker):
    open(marker, "w").close()
    raise RuntimeError("first attempt fails on purpose")
