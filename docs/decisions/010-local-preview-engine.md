# ADR 010: Local Preview Engine

**Status**: Superseded by [ADR 012](012-pyspark-universal-engine.md)

> **Why superseded:** The dual-engine choice (DuckDB locally, Athena in production) introduced exactly the dialect-drift class of bugs it was meant to side-step — DuckDB SQL agreed with Athena on the basics but diverged on date/regex/struct semantics, and Python UDFs available in DuckDB had no Athena equivalent. The architecture pretended preview and production used "the same SQL" when they didn't. ADR 012 settles on a single engine (PySpark) for both, accepting the cold-start cost in exchange for behavioural identity. DuckDB has been removed from the codebase; the local preview now boots an in-process Spark session via the runner container.

---

## Context (original, retained for history)

The transform preview feature lets users see the output of their SQL transforms before deploying. This requires executing SQL against sample data fetched from S3.

Two approaches were considered:

**Athena** — the production SQL engine. Results are exact. But Athena has per-query cost (~$5/TB scanned), 1–3 second latency even for small queries, and requires AWS credentials with Athena permissions. For a local development preview that runs on every edit, cost and latency make it impractical.

**Local embedded SQL engine** — runs SQL in-process with zero network calls. DuckDB is the leading embedded analytical database: column-oriented, supports standard SQL, handles Parquet/JSON natively, and has a mature Go driver (`github.com/marcboeker/go-duckdb`).

The preview workflow: fetch source data from S3 as JSON items, load them into an in-memory DuckDB instance, execute the user's SQL, return results to the UI.

## Decision (original, retained for history)

**DuckDB as the local SQL engine for transform preview.** SQL is executed in-memory via the Go DuckDB driver. No data leaves the user's machine during preview. Source data is fetched from S3 once and cached; the SQL runs locally with sub-millisecond latency.

Implementation lived in `internal/preview/duckdb.go` (now removed).

## Consequences (original, retained for history)

**Positive:**
- **Instant feedback** — SQL executes locally in milliseconds, enabling live preview as users edit transforms
- **Zero cost** — no Athena queries, no AWS compute charges during development
- **Works offline** — once source data is cached, preview works without network access
- **Simple architecture** — single function (`ExecuteSQL`) takes tables + query, returns results

**Negative:**
- **CGO dependency** — the Go DuckDB driver wraps the C library, requiring a C compiler at build time (`xcode-select --install` on macOS, `build-essential` on Linux)
- **SQL dialect differences** — DuckDB uses its own SQL dialect; some Athena/Trino-specific syntax may produce different results or errors locally. Users must validate with real Athena runs before deploying.
- **Type inference is approximate** — columns are inferred as DOUBLE or VARCHAR based on sample data, not the Iceberg schema. Preview results may show different types than production.

The "negative" items above turned out to be the load-bearing argument against this choice; see ADR 012.
