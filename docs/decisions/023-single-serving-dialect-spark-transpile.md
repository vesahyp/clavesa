# ADR 023: Author serving SQL in Spark, transpile to Trino

**Status**: Accepted (2026-06-09). Supersedes ADR-022 §Decision-3 (the Trino-portable serving contract) and the §Consequences bullet that deferred a transpiler.

## Context

ADR-022 settled which engine serves interactive reads (Athena in cloud, Spark locally) and declared the serving-SQL contract **Trino-portable**: dashboard, widget, and `/query` SQL had to be authored in the Athena/Trino-compatible subset, with Spark accepted only because it is a superset of that subset.

In practice that contract was not one rule, it was two, and they disagreed. `validateDashboardSQL` parsed a statement against the warm Spark worker on a local workspace and against an Athena `EXPLAIN` dry-run on a cloud workspace. The same query got a different judge depending on workspace mode, and the two judges reject in opposite directions: Spark requires `VARCHAR(n)` and rejects a bare `VARCHAR`; Trino has no `datediff` and rejects it. An author writing against a local workspace was effectively forced to satisfy the intersection of both dialects, and the answer to "what SQL is accepted" was an unexplainable subset. ADR-022 also left a known gap: the Athena gate only ran in cloud, so Spark-only serving SQL authored locally passed validation and broke only after a deploy.

ADR-022 itself anticipated the exit ("a future transpiler e.g. SQLGlot could widen the writable surface") and deferred it on the worry that it "leaks on complex types and UDFs." A go/no-go fidelity test against the real web-traffic dashboards retired that worry (see Consequences).

## Decision

**Serving SQL is authored in one dialect — Spark — and transpiled to Trino/Athena for cloud serving. Local serving runs the authored Spark unchanged.**

"What SQL is accepted" now has a one-sentence answer: **Spark SQL** — the same dialect users already write for transforms. Clavesa absorbs the portability difference instead of taxing the author with it.

1. **One authoring dialect.** Dashboard datasets, control selects, widget SQL, and ad-hoc `/query` are authored as Spark SQL. There is no separate serving dialect to learn.

2. **Transpile for cloud serving.** On a cloud workspace, serving SQL is transpiled to the Athena/Trino dialect via `sqlglot` before it reaches Athena. On a local workspace it runs as authored on the Spark worker. Response shapes are identical across both (ADR-014).

3. **Transpile is the portability gate, and it runs everywhere.** Dashboard save runs two checks: a Spark `/parse` (author dialect, best error positions) and a transpile through `sqlglot` with `unsupported_level=RAISE`. The transpile both confirms portability and populates a cache. Because portability is a property of the SQL and not of the deployment, the gate runs on local workspaces too — closing the ADR-022 gap where local-authored serving SQL only broke after a cloud deploy. The Athena-`EXPLAIN` gate is removed.

4. **Transpile at save, cache the serving form.** Serving SQL changes only on save, so the Spark→Trino transpilation is computed once and cached on disk (`<workspace>/.clavesa/cache/transpile/`, keyed by a content hash that folds in a transpiler-version tag). Render and `/query` on cloud read the cached Trino form; they never transpile on the hot path. Templates are transpiled with `{{name}}` placeholders sentinelized to string literals so the cache key is parameter-independent.

5. **No standing service.** The transpiler is `sqlglot`, pure Python, no JVM. It runs in a lightweight, non-Spark sidecar container (`CLAVESA_TRANSPILE_SERVER`) that boots in milliseconds and is reused for the `clavesa ui` session. It reuses the runner image clavesa already builds; it adds no host dependency and no cloud service.

## Mechanism

- `runner/runner.py` `CLAVESA_TRANSPILE_SERVER` mode: `POST /transpile` returns `{ok, trino, error, line, col}` via `sqlglot.transpile(read="spark", write="athena", unsupported_level=RAISE)`.
- `internal/observability/transpileSidecar`: lazily spawns and drives one container; an `ok:false` envelope becomes a `*DialectError`, a dead server becomes a transport error.
- `internal/service` `Transpiler` / `WithTranspiler` / `TranspileServing`, wired with the cache at the CLI, dashboards-CLI, and `clavesa ui` service-construction sites.
- `internal/servingsql`: placeholder sentinel round-trip + content-addressed cache.
- Dashboard save transpiles and caches each template; render (`servingTemplate`) and the api `/query` hook use the cached Trino form on cloud, Spark on local.

## Consequences

- **Existing dashboards need no migration.** All 33 dataset statements in the production web-traffic dashboards — authored in the ADR-022 Trino-workaround style (`CAST(x AS VARCHAR(10))`, `INTERVAL '1' DAY`, `url_decode`, `row_number() OVER`) — parse as Spark unchanged, so they keep validating and serving under the new author gate with no rewrite. New authoring can use natural Spark (`datediff`, `to_date(x)`, `CAST AS STRING`, `approx_count_distinct`, `percentile_approx`); the smoke test confirmed each transpiles to valid Athena.
- **The gate rarely rejects, by design.** `sqlglot`'s Spark→Trino mapping is broad: nearly all Spark serving SQL transpiles, which is the feature working — authors get to write Spark. Genuine `UnsupportedError` (a construct with no Athena form) is blocked at save, local-and-cloud, with the message carried against the Spark input. Invalid Spark is caught first by the Spark `/parse` gate.
- **Two fidelity caveats, documented not blocking.** `sqlglot` maps Spark `CAST` to Trino `TRY_CAST` (Spark throws on a bad cast, Trino returns NULL) and silently drops `CLUSTER BY` / `DISTRIBUTE BY` rather than raising. Both are irrelevant to serving reads (dashboards are `SELECT`/aggregation; distribution hints are meaningless in a display query), but they are real semantic differences to keep in mind if serving SQL ever grows beyond that shape. The transpiler version tag in the cache key lets a `sqlglot` upgrade re-transpile everything cleanly.
- **`clavesa query` is unaffected.** It is local-only and runs SparkSQL against the local catalog, which is already the single-dialect contract (Spark on Spark, no transpile). If it ever gains cloud dispatch it routes through the same transpile seam (ADR-015).
- **The Provider seam and the option-C hedge (ADR-022 §4) are untouched.** A future warm `SparkConnectProvider` in cloud would simply skip the transpile step; nothing in this decision forecloses it.

## Relationships

- Supersedes the relevant parts of **ADR-022**: the serving contract is now "Spark, transpiled" rather than "Trino-portable subset." ADR-022's serving-vs-authoring taxonomy and its option-B engine choice (Athena in cloud) still stand.
- Honors **ADR-012** (Spark universal compute): serving SQL is now authored in the same Spark dialect as transforms.
- Honors **ADR-014** (local/cloud parity): one definition, Provider-dispatched engine, identical response shapes; the transpile changes only which SQL runs, never the response.
- Honors **ADR-015** (CLI/UI parity): the transpile seam lives in the service layer, so both surfaces inherit it.
