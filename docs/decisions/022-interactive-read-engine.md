# ADR 022: Interactive read engine and the serving-vs-authoring SQL contract

**Status**: Accepted (2026-05-31)

## Context

ADR-012 makes Spark the universal *compute* engine: every transform runs as SparkSQL or PySpark across local, Lambda, Fargate, and EMR Serverless. ADR-014 makes local and cloud return identical response shapes from a Provider seam, while allowing the backend to differ (warm Spark worker locally, Athena in cloud).

Cloud-deploy verification of the web-traffic workload exposed a gap that neither ADR settled: **which engine serves interactive reads, and in which SQL dialect users author the SQL those reads run.** Concretely, dashboard widget SQL authored against the local Spark worker (`to_date(...)` and other Spark-dialect functions) fails on Athena with `PARSE_SYNTAX_ERROR`, so a dashboard built locally is broken the moment the workspace flips to cloud. The same tension applies to `/query` and any saved query.

A first instinct was to unify reads on Spark in cloud too (route the warm worker against the cloud Delta tables over S3A), making one dialect everywhere. That was considered and rejected for the current product shape: Athena is serverless, scales to zero, and carries no standing cost, which is exactly right for read-only serving. A warm Spark endpoint in cloud is a standing service we do not want to require for a self-managed deploy.

The clarifying move is to stop treating this as "local vs. cloud" and sort read surfaces by **what they actually do**:

- **Serving reads** — `SELECT`/aggregation against *finished* Delta tables: Catalog, table preview, dashboard widgets, ad-hoc `/query`. Dialect-portable in principle. Athena serves them cheaply with zero standing infra.
- **Authoring reads** — running *user-authored transform code* to see its output: node preview, notebook cells, "promote query to transform." A transform may be PySpark, and previewing it *is* running it. **These can never run on Athena.** They require Spark.

PySpark is the fixed point: it cannot run on Athena, ever. So authoring surfaces need Spark compute regardless of environment. Today preview runs on the **local docker runner** even in cloud mode, so a self-managed CLI deploy needs no cloud Spark at all.

## Decision

**Cloud interactive reads stay on Athena (option B). Spark is the engine for authoring surfaces only.**

1. **Serving surfaces** (Catalog, table preview, dashboard widgets, `/query`, saved queries) run through the active Provider: local Spark worker locally, Athena in cloud (ADR-014, unchanged).

2. **Authoring surfaces** (node preview, notebooks, promote-to-transform) run on Spark always. In the self-managed deploy that is the local docker runner. They are never routed through Athena.

3. **The serving-SQL contract is Trino-portable.** All serving SQL — dashboard datasets, widget SQL, `/query`, saved queries — is authored in the Athena/Trino-compatible subset. Spark is a superset of that subset, so the same SQL runs unchanged on both engines. This is the binding rule: it is what lets a dashboard authored locally run against Athena in cloud, and it is baked into authored artifacts the moment a user writes them. Authoring SQL/PySpark (transform logic) remains full-power Spark, because it never executes on Athena.

4. **The Provider seam is preserved as the future hedge.** The ADR-014 read interface (`LocalProvider` / `AthenaProvider`, matching response shapes) can gain a `SparkConnectProvider` later without reshaping any caller. The B-vs-C choice then becomes a per-deployment flag, not a code fork.

## Consequences

- The web-traffic dashboard `PARSE_SYNTAX_ERROR` is fixed by rewriting its dataset SQL to the portable subset, not by swapping engines. This becomes the rule for all dashboard/`/query` SQL going forward.
- Authoring surfaces carry a hard dependency on Spark compute being reachable. Self-managed: local docker. This is acceptable and already how preview works.
- We accept that serving authors lose Spark-only SQL functions in dashboards and ad-hoc queries. A future transpiler (e.g. SQLGlot) could widen the writable surface, but it leaks on complex types and UDFs and is not part of this decision.
- Athena's column-read of Spark-written Delta tables requires the Glue table to carry `table_type = DELTA`; the runner must stamp it (separate slice). Without it, no serving read works in cloud regardless of dialect.

### When option C reopens

Only if a **hosted UI** ships. Hosting removes the local docker runner, so preview and notebooks must run on *on-demand* cloud Spark (a cold-start Lambda/Fargate invocation is acceptable for an authoring click). That builds the hard part of C — Spark reading Delta over S3A in cloud. What remains is a cost/latency knob: whether to keep that Spark endpoint *warm* and serve interactive reads through it (dropping Athena), or keep Athena as the cheap scan path and Spark warm only for authoring. Because the serving-SQL contract is already Trino-portable, that decision invalidates no existing dashboard. Revisit then; do not build it now.

### Relationships

- Extends **ADR-012** (Spark universal compute) to the read path: authoring reads are Spark; serving reads are not required to be.
- Honors **ADR-014** (local/cloud parity): one definition, Provider-dispatched engine, matching response shapes. Adds the dialect contract ADR-014 left implicit.
- Honors **ADR-015** (CLI/UI parity): the serving-SQL contract applies equally to `clavesa query` and the UI `/query`.
