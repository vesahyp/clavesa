# Web analytics from CloudFront logs

> **When you have one:** a website behind CloudFront and a want for Google-Analytics-style traffic insight — sessions, pageviews, top pages, referrers — **without** a third-party tag, cookies, or shipping visitor data out of your own account. Everything here stays in your AWS region: a tracking pixel writes to CloudFront's access logs, and clavesa turns those logs into queryable Delta tables.

This is the ingestion core of the analytics that runs on clavesa.dev itself (see [Going further](#going-further) for the full build — bot filtering, geo, conversion funnels). It's cookieless and EU-friendly by construction: no data leaves the bucket your logs already land in.

## What you'll end up with

- An `events` Delta table — one typed row per beacon hit (event, session, visitor, path, referrer, timestamp).
- A `daily` rollup table — sessions, visitors, events, and referred sessions per day.
- Both readable from Athena, the Catalog, or `clavesa query`, refreshable on a schedule (see [scheduled-rollup](scheduled-rollup.md)).

## Collect the data

Two one-time bits of setup, both outside clavesa (infrastructure, not part of the pipeline):

**1. Add the tracker.** Drop the ready-made [`tracker.js`](../../web-tracker/tracker.js) on your pages — a small, dependency-free file (the same one that runs clavesa.dev, tested end to end). Host it yourself and add one script tag:

```html
<script src="/tracker.js" defer></script>
```

Then tag the links and buttons you want to measure with `data-track`:

```html
<a href="/signup" data-track="signup">Start free</a>
<a href="/docs"   data-track="docs">Read the docs</a>
```

The tracker keeps only a random id in `localStorage` (no cookies) and fires 1×1 pixel beacons to `/t.gif` — `session_start`, scroll depth, web vitals, and, for every `data-track` element, `view` / `displayed` / `click`. Those last three are what give you click-through rate and conversion funnels; each event rides in the query string, so CloudFront logs it and there's no backend to run. Serve any 1×1 object at `/t.gif` (a transparent GIF is traditional — the response body is irrelevant, the request just needs to be logged). See [`web-tracker/`](../../web-tracker/README.md) for the full event list and config.

**2. Standard logging on the distribution.** Turn on CloudFront **standard (legacy) access logs** to an S3 bucket — Console: *Distribution → Settings → Standard logging → S3 bucket*, or Terraform:

```hcl
resource "aws_cloudfront_distribution" "site" {
  # …
  logging_config {
    bucket = aws_s3_bucket.cf_logs.bucket_domain_name
    prefix = "cloudfront/"
  }
}
```

Logs arrive as **gzipped TSV**, each file beginning with two `#`-prefixed header lines (`#Version` and `#Fields`). That format is exactly what the source below reads.

## The recipe

```bash
# 1. Register the log bucket as a tsv source. CloudFront logs are
#    tab-separated with no column header and #-prefixed metadata lines,
#    so turn the header off and treat # as a comment (skips #Version/#Fields).
bin/clavesa source register cflogs \
  --from s3://your-cf-logs/cloudfront/ \
  --format tsv \
  --read-option header=false \
  --read-option comment=#

# 2. Create the pipeline and add the beacon-parsing transform.
bin/clavesa pipeline create analytics
bin/clavesa node add analytics --type transform --name events
```

The columns arrive unnamed (`_c0`, `_c1`, …) because CloudFront logs have no header row — a standard log has 30+ fields and we need only a handful by position. Parsing the double-encoded beacon query string is a job for Python, so save this as `<workspace>/analytics/transforms/parse_beacon.py`:

```python
"""
parse_beacon — turn CloudFront /t.gif beacon hits into typed event rows.

The runner hands this transform the raw CloudFront access logs as headerless,
tab-separated columns (_c0.._cN — a standard v1.0 log has 30+ fields; we read
only the few we need). We keep the /t.gif requests and pull the analytics
fields out of the query string.

That query string is DOUBLE URL-encoded: the tracker URL-encodes each value,
then CloudFront URL-encodes the whole query field again when it writes the log.
parse_qs peels CloudFront's layer; unquote peels the tracker's.

Positional columns we use (0-indexed) in a CloudFront standard log:
  _c7  cs-uri-stem    (keep == /t.gif)
  _c11 cs-uri-query   (the beacon params — includes the client timestamp t)
"""

from urllib.parse import parse_qs, unquote

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import MapType, StringType


def _parse_qs(query):
    """cs-uri-query -> {beacon field: fully-decoded value}."""
    out = {}
    if not query or query == "-":
        return out
    for key, vals in parse_qs(query, keep_blank_values=True).items():
        if vals:
            out[key] = unquote(vals[0])   # unquote peels the tracker's layer
    return out


def transform(spark, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
    parse_udf = F.udf(_parse_qs, MapType(StringType(), StringType()))

    beacons = (
        inputs["logs"]
        .where(F.col("_c7") == "/t.gif")
        .where(F.col("_c11").isNotNull() & (F.col("_c11") != "") & (F.col("_c11") != "-"))
        .withColumn("q", parse_udf(F.col("_c11")))
    )

    events = beacons.select(
        F.col("q")["e"].alias("event"),
        F.col("q")["sid"].alias("session_id"),
        F.col("q")["uid"].alias("visitor_id"),
        F.col("q")["p"].alias("path"),
        F.col("q")["ref"].alias("referrer"),
        # The beacon carries its own millisecond timestamp (t); TRY_CAST so a
        # malformed value becomes NULL instead of failing the read.
        F.expr("timestamp_millis(TRY_CAST(q['t'] AS BIGINT))").alias("event_ts"),
        F.expr("to_date(timestamp_millis(TRY_CAST(q['t'] AS BIGINT)))").alias("day"),
    ).where(F.col("event").isNotNull() & (F.col("event") != ""))

    return {"default": events}
```

Wire that file in, then add a SQL rollup that reads the parsed events:

```bash
# 3. Point the transform at the Python file and feed it the logs source.
bin/clavesa node edit analytics events \
  --set "language=python" \
  --set "python=file(transforms/parse_beacon.py)"
bin/clavesa source attach analytics cflogs --to events --as logs

# 4. A daily rollup over the parsed events.
bin/clavesa node add analytics --type transform --name daily
bin/clavesa node edit analytics daily --set "sql=
  SELECT
    day,
    COUNT(DISTINCT session_id) AS sessions,
    COUNT(DISTINCT visitor_id) AS visitors,
    COUNT(*)                   AS events,
    COUNT(DISTINCT CASE WHEN referrer IS NOT NULL AND referrer <> ''
                        THEN session_id END) AS referred_sessions
  FROM events
  GROUP BY day
  ORDER BY day"
bin/clavesa node connect analytics --from events --to daily --input events

# 5. Run it.
bin/clavesa pipeline run analytics
```

```
NODE    TYPE       STATUS  OUTPUT
events  transform  ok      clavesa_<workspace>__analytics.events
daily   transform  ok      clavesa_<workspace>__analytics.daily
```

## What you should see

- `pipeline run` reports both `events` and `daily` as `ok`.
- `/` (Catalog) shows the two tables under `clavesa_<workspace>__analytics`.
- Click `events`: one row per beacon hit, with `event` / `session_id` / `path` / `referrer` decoded out of the query string (no `%2F` noise — the double-decode worked).
- Ad-hoc queries answer the GA-style questions directly:

```bash
# Top pages
bin/clavesa query "SELECT path, COUNT(*) AS views
  FROM clavesa_<workspace>__analytics.events
  GROUP BY path ORDER BY views DESC LIMIT 10"

# Top referrers (external only)
bin/clavesa query "SELECT referrer, COUNT(*) AS hits
  FROM clavesa_<workspace>__analytics.events
  WHERE referrer IS NOT NULL AND referrer <> ''
  GROUP BY referrer ORDER BY hits DESC LIMIT 10"
```

Counts are your own traffic, so there are no fixed numbers to match here — the [verify gate](../../scripts/verify-cookbook.sh) asserts exact counts against a synthetic log fixture instead.

## Going further

This recipe stops at parsed events + a daily rollup. The clavesa.dev analytics build layers more on the same shape, each an add-on transform or reference-data lookup:

- **Bot filtering** — [crawlerdetect](runner-deps.md) on the user-agent plus a datacenter-IP range check (both loaded from an S3 reference file), to split humans from crawlers.
- **Geography** — an IP→country lookup (DB-IP ranges) for a per-country breakdown / world map, keeping only the 2-letter code so no raw IP is stored.
- **Conversion funnels + click-through rate** — the tracker already emits `view` / `displayed` / `click` for every `data-track` element, so this is pure aggregation: group by the `data-track` name for a per-element click-through rate, or roll up `sessions → saw a CTA → clicked` for a funnel. No extra instrumentation.

Bot filtering and geo need external reference data (DB-IP, a bot list); the funnel and CTR are just SQL over the events the tracker already sends. The [clavesa source](https://github.com/vesahyp/clavesa) has the full implementation if you want to grow into it.

## Troubleshooting

**Every row is `NULL` / the run reads zero events.** Check the `--read-option`s. CloudFront logs have **no header row** (`header=false`) and lead with `#Version` / `#Fields` lines that must be skipped (`comment=#`). Without `comment=#`, Spark treats the `#Fields` line as data and the column positions shift.

**`path` / `referrer` come out as `%2F` or `https%3A%2F%2F…`.** That's a single decode where two are needed. The values are double-encoded (tracker + CloudFront); `parse_qs` handles one layer and the `unquote` in `_parse_qs` handles the other — make sure both are present.

**`events` is empty but the logs clearly have `/t.gif` hits.** The stem filter is exact: `_c7 == "/t.gif"`. If you served the pixel at a different path, match it. Confirm the beacon requests actually reached CloudFront (a `/t.gif` served from browser cache never hits the edge, so add `Cache-Control: no-store` on the pixel).

**Column positions look wrong.** Field order is fixed by CloudFront's standard-log spec; this recipe reads `_c0` date, `_c1` time, `_c7` cs-uri-stem, `_c11` cs-uri-query. If your fields differ you're likely on **real-time** logs (a different, configurable schema) rather than **standard** logs.

## See also

- [python-transform](python-transform.md) — the `language = "python"` contract the parser uses.
- [s3-bulk-ingest](s3-bulk-ingest.md) — the `s3://` source mechanics under `source register`.
- [scheduled-rollup](scheduled-rollup.md) — run this `daily` rollup on a cron so the tables stay fresh.
- [main README quick-start](../../README.md#quick-start) — the on-ramp if you're new.
