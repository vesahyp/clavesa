#!/usr/bin/env python3
"""Build the end-to-end bundle of ADR-031 in a folder (stdlib only).

Fetches Databricks' default_sql example (github.com/databricks/bundle-examples,
default_sql, at a pinned commit) with its LICENSE, applies the edits listed in
README.md, and puts our databricks.yml next to it with the workspace host and
the input bucket filled in. Nothing of the example is kept in clavesa's repo.

  python3 prepare.py OUT_DIR --host https://<workspace>.cloud.databricks.com --source-bucket <bucket>
"""

import argparse
import os
import re
import shutil
import sys
import urllib.request

REPO = "databricks/bundle-examples"
COMMIT = "1b358c3d3d82c1560aaed60a68647e1b2d5f0fd2"  # main on 2026-11-14
SOURCE_URL = f"https://github.com/{REPO}/tree/{COMMIT}/default_sql"
FILES = ["resources/default_sql_sql.job.yml", "src/orders_raw.sql", "src/orders_daily.sql"]
HERE = os.path.dirname(os.path.abspath(__file__))

CHANGED = (
    "-- Changed by clavesa's end-to-end test (testdata/e2e/default_sql/README.md) from\n"
    f"-- {SOURCE_URL}/{{path}}\n"
    "-- under the Databricks License (LICENSE in this folder).\n"
)


def fetch(path: str) -> str:
    url = f"https://raw.githubusercontent.com/{REPO}/{COMMIT}/{path}"
    with urllib.request.urlopen(url, timeout=60) as resp:
        return resp.read().decode()


def must_sub(pattern: str, repl: str, text: str, what: str) -> str:
    new, n = re.subn(pattern, repl, text, flags=re.S)
    if n != 1:
        sys.exit(f"prepare.py: the example changed ({what} matched {n} times); update prepare.py")
    return new


def edit(path: str, text: str) -> str:
    if path.endswith(".job.yml"):
        # 1. No email to the example's placeholder address user@company.com.
        text = must_sub(r"\n      email_notifications:\n        on_failure:\n          - user@company\.com\n", "\n", text,
                        "email_notifications")
        return CHANGED.format(path=path).replace("-- ", "# ") + text
    # 2. Databricks fills {{name}} in as a quoted string: a name goes in IDENTIFIER().
    text = must_sub(r"USE CATALOG `\{\{catalog\}\}`;", "USE CATALOG IDENTIFIER({{catalog}});", text, "USE CATALOG")
    if path.endswith("orders_raw.sql"):
        # 3. A streaming table runs only in Lakeflow pipelines: a table over
        #    read_files (a batch read), from the source parameter.
        text = must_sub(r"CREATE OR REFRESH STREAMING TABLE", "CREATE OR REPLACE TABLE", text, "streaming table")
        text = must_sub(r"FROM STREAM READ_FILES\(\s*\n\s*\"/databricks-datasets/retail-org/sales_orders/\",",
                        "FROM READ_FILES(\n  {{source}},", text, "read_files path")
    if path.endswith("orders_daily.sql"):
        # 4. A materialized view runs only in Databricks: a table.
        text = must_sub(r"CREATE OR REPLACE MATERIALIZED VIEW", "CREATE OR REPLACE TABLE", text, "materialized view")
    return CHANGED.format(path=path) + text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--host", required=True, help="the Databricks workspace URL")
    ap.add_argument("--source-bucket", required=True, help="the S3 bucket with e2e-bundle/retail-org/sales_orders/")
    ap.add_argument("--as-published", action="store_true",
                    help="keep the example's SQL as published (to see what clavesa bundle plan says about it)")
    a = ap.parse_args()
    if os.path.exists(a.out):
        shutil.rmtree(a.out)
    for path in FILES + ["LICENSE"]:
        src = fetch(path if path == "LICENSE" else "default_sql/" + path)
        dst = os.path.join(a.out, path)
        os.makedirs(os.path.dirname(dst) or a.out, exist_ok=True)
        if path != "LICENSE" and not (a.as_published and path.endswith(".sql")):
            src = edit(path, src)
        with open(dst, "w") as f:
            f.write(src)
    with open(os.path.join(HERE, "databricks.yml")) as f:
        yml = f.read()
    yml = yml.replace("https://dbc-00000000-0000.cloud.databricks.com", a.host.rstrip("/"))
    yml = yml.replace("E2E_SOURCE_BUCKET", a.source_bucket)
    with open(os.path.join(a.out, "databricks.yml"), "w") as f:
        f.write(yml)
    print(f"bundle ready in {a.out} ({SOURCE_URL})")


if __name__ == "__main__":
    main()
