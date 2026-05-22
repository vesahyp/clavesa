#!/usr/bin/env python3
"""Synthetic event generator for clavesa cloud verification.

Writes Parquet partitioned `year=YYYY/month=MM/day=DD/hour=HH/part-NNNN.parquet`
either to a local directory or to an S3 bucket. Idempotent per partition:
re-running with the same `--seed` and window overwrites with identical data.

Usage:
    pip install -r scripts/requirements.txt

    # Local smoke test
    python scripts/synth_events.py --out /tmp/synth-out \\
        --start 2026-05-12T00:00:00Z --end 2026-05-12T03:00:00Z

    # Personal-account S3
    AWS_PROFILE=personal AWS_REGION=eu-north-1 \\
    python scripts/synth_events.py --bucket clavesa-synth-vk \\
        --start 2026-05-12T00:00:00Z --end 2026-05-13T00:00:00Z
"""

from __future__ import annotations

import argparse
import hashlib
import io
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PATHS = [
    "/", "/blog", "/blog/post-1", "/blog/post-2", "/about", "/contact",
    "/pricing", "/docs", "/docs/getting-started", "/api/v1/health",
]
COUNTRIES = ["FI", "SE", "NO", "DK", "DE", "GB", "US", "FR", "ES", "IT"]
STATUSES = [200, 200, 200, 200, 200, 304, 404, 301, 500]

SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("ts", pa.timestamp("us", tz="UTC")),
    ("user_id", pa.string()),
    ("path", pa.string()),
    ("status", pa.int32()),
    ("bytes", pa.int64()),
    ("country", pa.string()),
])

# Dim-customer shape — used by the merge-dim-table cookbook walk where
# the same natural key needs to reappear across runs with mutable
# attributes. Schema mirrors what an SCD-Type-1 dimension typically
# looks like: stable id + slowly-changing tier.
TIERS = ["bronze", "silver", "gold", "platinum"]
FIRST_NAMES = ["Aino", "Pekka", "Liam", "Sara", "Noor", "Jonas", "Maya", "Ravi", "Elin", "Theo"]
LAST_NAMES = ["Korhonen", "Andersen", "Smith", "Müller", "Garcia", "Tanaka", "Patel", "Dubois", "Costa", "Olsen"]

DIM_SCHEMA = pa.schema([
    ("customer_id", pa.int64()),
    ("name", pa.string()),
    ("email", pa.string()),
    ("signup_date", pa.date32()),
    ("tier", pa.string()),
])


def gen_hour_rows(hour_start: datetime, n: int, rng: random.Random) -> pa.Table:
    rows = []
    for _ in range(n):
        offset_s = rng.randrange(0, 3600)
        rows.append({
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "ts": hour_start + timedelta(seconds=offset_s),
            "user_id": f"u{rng.randrange(1, 5000):05d}",
            "path": rng.choice(PATHS),
            "status": rng.choice(STATUSES),
            "bytes": rng.randrange(200, 50000),
            "country": rng.choice(COUNTRIES),
        })
    rows.sort(key=lambda r: r["ts"])
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def gen_dim_customers(n: int, revision: int, seed: int) -> pa.Table:
    """Generate N customer rows. Revision >=1 mutates customer #revision's
    tier upward by one band, so the recipe's "change a single row and
    re-run, see the dim table update in place" claim is reproducible
    end-to-end."""
    base_rng = random.Random(seed)
    base_tiers: list[str] = []
    base_signups: list[date] = []
    base_names: list[str] = []
    for cid in range(1, n + 1):
        per_row = random.Random(base_rng.randrange(0, 1 << 32) ^ cid)
        base_tiers.append(per_row.choice(TIERS))
        days_ago = per_row.randrange(30, 365 * 5)
        base_signups.append(date(2026, 5, 14) - timedelta(days=days_ago))
        first = per_row.choice(FIRST_NAMES)
        last = per_row.choice(LAST_NAMES)
        base_names.append(f"{first} {last}")
    # Promote customer #revision one tier (capped at platinum). Revision 0
    # is the baseline (no mutation); revision K (1..N) promotes customer K.
    if 1 <= revision <= n:
        idx = revision - 1
        cur = base_tiers[idx]
        try:
            base_tiers[idx] = TIERS[min(TIERS.index(cur) + 1, len(TIERS) - 1)]
        except ValueError:
            pass
    rows = []
    for cid in range(1, n + 1):
        idx = cid - 1
        name = base_names[idx]
        local = name.split(" ")[0].lower()
        rows.append({
            "customer_id": cid,
            "name": name,
            "email": f"{local}{cid:04d}@example.com",
            "signup_date": base_signups[idx],
            "tier": base_tiers[idx],
        })
    return pa.Table.from_pylist(rows, schema=DIM_SCHEMA)


def _partition_subpath(prefix: str, hour: datetime, part_idx: int) -> str:
    return (
        f"{prefix.strip('/')}/year={hour.year:04d}/month={hour.month:02d}"
        f"/day={hour.day:02d}/hour={hour.hour:02d}/part-{part_idx:04d}.parquet"
    )


def write_local(table: pa.Table, root: Path, prefix: str, hour: datetime, part_idx: int) -> Path:
    rel = _partition_subpath(prefix, hour, part_idx)
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")
    return path


def write_s3(table: pa.Table, bucket: str, prefix: str, hour: datetime, part_idx: int, region: str) -> str:
    # Use boto3 (honors AWS_PROFILE / SSO) rather than pyarrow.fs.S3FileSystem
    # which only reads raw access-key env vars.
    import boto3  # noqa: PLC0415

    key = _partition_subpath(prefix, hour, part_idx)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket, Key=key, Body=buf.getvalue()
    )
    return f"s3://{bucket}/{key}"


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def write_single(table: pa.Table, root: Path, prefix: str, filename: str) -> Path:
    rel = f"{prefix.strip('/')}/{filename}"
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")
    return path


def write_single_s3(table: pa.Table, bucket: str, prefix: str, filename: str, region: str) -> str:
    import boto3  # noqa: PLC0415

    key = f"{prefix.strip('/')}/{filename}"
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    boto3.client("s3", region_name=region).put_object(
        Bucket=bucket, Key=key, Body=buf.getvalue()
    )
    return f"s3://{bucket}/{key}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--shape", choices=("events", "dim"), default="events",
                    help="What to generate. 'events' (default) writes Hive-partitioned event "
                         "parquet over a time range; 'dim' writes one customers.parquet for "
                         "the merge-dim-table cookbook walk.")
    ap.add_argument("--bucket", help="S3 bucket name (omit for local FS output via --out)")
    ap.add_argument("--out", type=Path, default=Path("synth-out"),
                    help="Local output root when --bucket is unset (default: ./synth-out)")
    ap.add_argument("--prefix", default="synth-events",
                    help="Key prefix (S3) or sub-directory (local). Default: synth-events")
    # Events-shape args
    ap.add_argument("--start",
                    help="[events] ISO 8601 start (inclusive, hour-floored). Z or +00:00 both fine.")
    ap.add_argument("--end",
                    help="[events] ISO 8601 end (exclusive).")
    ap.add_argument("--rows-per-hour", type=int, default=200,
                    help="[events] Rows per hour partition. Default: 200.")
    # Dim-shape args
    ap.add_argument("--customers", type=int, default=100,
                    help="[dim] Total customer rows in the dimension. Default: 100.")
    ap.add_argument("--revision", type=int, default=0,
                    help="[dim] 0 = baseline; K>=1 promotes customer #K one tier "
                         "(silver→gold etc.) so the recipe's 'change a single row' "
                         "claim is reproducible. Same key, mutated attribute.")
    # Shared args
    ap.add_argument("--seed", type=int, default=42,
                    help="Base seed; per-hour (events) or per-row (dim) RNG mixes this with the "
                         "row identity so re-runs are deterministic.")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-north-1"),
                    help="AWS region for S3 writes (default: $AWS_REGION or eu-north-1)")
    args = ap.parse_args()

    if args.shape == "dim":
        if args.customers <= 0:
            ap.error("--customers must be > 0")
        if args.revision < 0:
            ap.error("--revision must be >= 0")
        table = gen_dim_customers(args.customers, args.revision, args.seed)
        if args.bucket:
            uri = write_single_s3(table, args.bucket, args.prefix, "customers.parquet", args.region)
        else:
            uri = str(write_single(table, args.out, args.prefix, "customers.parquet").resolve())
        print(uri)
        print(f"# wrote {table.num_rows} customers, ~{table.nbytes / 1024:.1f} KiB (in-memory), "
              f"revision={args.revision}")
        return

    # shape == "events"
    if not args.start or not args.end:
        ap.error("--start and --end are required with --shape events")
    start = parse_iso(args.start).replace(minute=0, second=0, microsecond=0)
    end = parse_iso(args.end)
    if end <= start:
        ap.error("--end must be after --start")

    hours = 0
    rows = 0
    bytes_written = 0

    hour = start
    while hour < end:
        digest = hashlib.sha256(f"{args.seed}|{hour.isoformat()}".encode()).digest()
        per_hour_seed = int.from_bytes(digest[:8], "big")
        local_rng = random.Random(per_hour_seed)
        table = gen_hour_rows(hour, args.rows_per_hour, local_rng)
        if args.bucket:
            uri = write_s3(table, args.bucket, args.prefix, hour, 0, args.region)
        else:
            uri = str(write_local(table, args.out, args.prefix, hour, 0).resolve())
        print(uri)
        hours += 1
        rows += table.num_rows
        bytes_written += table.nbytes
        hour += timedelta(hours=1)

    print(f"# wrote {hours} partition(s), {rows} rows, ~{bytes_written / 1024:.1f} KiB (in-memory)")


if __name__ == "__main__":
    main()
