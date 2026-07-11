# Changelog

User-visible changes only. Implementation rationale lives in commit messages
(`git log <hash>` for any line below). Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions track
`ModuleVersion` in `internal/service/version.go`, which is the `?ref=vX.Y.Z`
git tag workspace `.tf` pins against.

**Release rule:** every `ModuleVersion` bump needs a CHANGELOG entry, an
annotated tag pushed to origin, and green tests + `terraform validate`. See
`CLAUDE.md` "Releasing a new module version".

## [Unreleased]

### Added
- `web-tracker/` — a ready-made, dependency-free, cookieless web-analytics tracker (the same one that runs clavesa.dev, tested end to end), with a README. Drop it on a site, tag elements with `data-track`, and pair it with the cloudfront-web-analytics recipe for sessions, funnels, and click-through rate.

### Changed
- Cookbook recipe **cloudfront-web-analytics** now uses the ready-made `web-tracker/tracker.js` instead of a hand-rolled beacon snippet.

## [v2.17.1] — 2026-07-11

### Added
- Cookbook recipe **cloudfront-web-analytics** — cookieless web analytics from CloudFront access logs (tracking pixel → gzipped-TSV logs → a clavesa pipeline → daily rollups), with a deterministic `verify-cookbook` gate.

## [v2.17.0] — 2026-07-08

### Added

- File sources can read delimited text via `format = "tsv"` plus a `read_options` map (`delimiter`, `comment`, `header`, `columns`); gzip is transparent. Lets a source read CloudFront legacy standard access logs (gzipped TSV with `#`-prefixed `#Version`/`#Fields` header lines) directly — `clavesa source register logs --kind s3 --bucket b --prefix cloudfront/ --format tsv --read-option comment=# --read-option header=false` (GH #88). Preview of tsv sources isn't wired yet (GH #89); read them with `pipeline run`.
- A transform can write its output as a **single JSON file** to an S3 (or local) path instead of a Delta table, via `output_definitions = { default = { format = "json", path = "s3://…/x.json", content_type = "application/json", cache_control = "…" } }`. The runner collects the (small, rollup-sized) frame and writes one JSON array of row objects with the given content-type/cache-control — a web-servable artifact, not a Parquet part-file directory (GH #57). Works in local `pipeline run` and cloud.

### Fixed

- Local `pipeline run` now forwards host AWS credentials to the runner when a node **writes** to S3 (e.g. a `format = "json"` output), not only when it reads an S3 source — previously such a write failed with `NoCredentialsError`.

## [v2.16.0] — 2026-07-06

### Fixed

- Catalog row counts for MERGE/append tables with long histories are now exact: derived from Delta snapshot state (checkpoint stats + commit replay) instead of summing per-commit deltas over a 200-commit window; when stats are unavailable the fallback estimate is marked with `~`, and the commit count reports the table's lifetime total (GH #66).
- The Logs drawer for local runs shows the run's captured runner output again (the per-run bundle log, with per-line timestamps) instead of always coming up empty (GH #64).
- Cloud-local runs (`--compute local` on a cloud warehouse) now appear in the pipeline dashboard's Recent executions list, and opening their run detail reads the run's failure context instead of erroring (GH #65).
- Local execution references use one encoding across `/pipeline/status` and the execution detail/states/logs endpoints, so a ref from the status listing is accepted everywhere (the old `<dir>:<runID>` form still decodes) (GH #78).
- Local-warehouse backfill `list` and `diff` work again: the staging-table scan, on-disk path resolution, and schema reads now follow the current Delta (`_delta_log`) and ADR-019 nested warehouse layout instead of the retired Iceberg/flat layout, so `pipeline backfill list`/`diff` no longer come up empty or fail with "malformed staging table id" (GH #68).
- Partitioned S3 sources work against a custom S3 endpoint (moto/MinIO/LocalStack) in local runs: the runner's boto3 clients now honor `CLAVESA_S3_ENDPOINT` (previously only Spark's S3A did, so partition discovery hit real AWS and failed). No effect in cloud, where the endpoint override is unset (GH #87).
- `clavesa query` (and the UI's ad-hoc query panel) against a nonexistent table now errors instead of silently returning zero rows with a success exit — so it's safe as a script check. `dashboards render` likewise exits non-zero when a widget's query fails. Dashboard-live views and catalog surfaces over a not-yet-materialized table still render empty rather than erroring.

## [v2.15.0] — 2026-07-04

### Fixed

- CLI `pipeline optimize` (and every other command) now runs against the shared local Derby metastore instead of the embedded fallback, matching `pipeline run` and the UI's optimize route — no more "Another instance of Derby may have already booted" when a warm worker is live (GH #76).

### Changed

- The dashboard widget editor's query route (`POST /api/dashboards/query`) now runs through the same query seam as `/data/query` and `clavesa query`: the ADR-023 Trino-portability gate applies on a local warehouse (non-portable SparkSQL is rejected at edit time with the dialect error, instead of succeeding locally and failing at dashboard save), and querying a cloud warehouse that isn't deployed returns the actionable "run `clavesa workspace deploy`" error instead of an empty result.
- Dialog overlays no longer blur the background (perf parity with sheets); native selects share one styled primitive, so a few pickers gained the standard focus ring.

## [v2.14.0] — 2026-07-03

### Fixed

- Deleting a node upstream of a multi-input (join) transform no longer wipes the transform's whole `inputs` map — surviving edges from other producers, registry references, and external table refs are preserved; edge edits also keep authored non-default output references (`outputs["<key>"]`) instead of rewriting them to `default` (GH #69).
- `make dev` starts the backend again (single-dash long flags rejected by the CLI).
- MERGE scan-bound literals now escape backslashes, and a bound column whose values can't all be rendered is skipped entirely (including NaN/Inf floats and decimals), so a backslash- or NaN-bearing merge key can no longer make the GH #62 bound exclude a matching target row and silently duplicate it on merge (GH #70).
- Local runner failures now end with a `full runner log: <path>` line pointing at the complete Spark output on disk, alongside the inline stderr tail. The bundle log survives even when the run dir can't be created (falls back to the workspace `.clavesa` dir, then the system temp dir), and single-node/backfill failures write their buffered output to the run-log dir instead of discarding it.
- Replace-mode outputs now follow the transform's schema: adding a column no longer fails the run with a Delta schema mismatch, and changing a column's type no longer requires a manual `DROP TABLE` (GH #39). Liquid clustering is re-asserted after an evolving overwrite.
- Merge-mode outputs now actually evolve additively: a newly-added column reaches the target (populated on rows the batch touches, NULL elsewhere) instead of being silently dropped — with plain merges and with `merge_update` (GH #61). Delta's `MERGE WITH SCHEMA EVOLUTION`, shipped as the fix in v2.11.0, never applied to the runner's SQL shape; the runner now adds missing columns to the target itself before the MERGE. Evolution is additive only — column removals and type changes on merge/append outputs remain unsupported.

### Changed

- Merge outputs persist the staging frame across bound computation, the `bound_by` tripwire, and the MERGE itself, eliminating up to ~6 full recomputations of the transform per merge output and closing a non-determinism window that could also cause silent duplicates. Tier-1 bound columns are capped at Delta's 4-column clustering limit; columns beyond it never pruned and only cost collect jobs.

## [v2.13.0] — 2026-07-01

### Added

- Per-table file count and average file size, surfaced local-and-cloud on both UI and CLI (GH #26): a "Files" column on the Catalog list, a "Storage layout" card on the table detail page, and `FILES` / `AVG SIZE` columns (plus `file_count` / `total_bytes` in `--json`) on `clavesa workspace tables`. A conservative "small files" badge flags tables whose file fan-out would inflate S3 read-request cost before it shows up on the bill.

## [v2.12.0] — 2026-06-30

### Added

- `bound_by` output-definition attribute (`node ... --output-bound-by`): for a merge output whose merge key is uncorrelated with the table's clustering (e.g. a random request-id key on a date-clustered fact), name the clustering column(s) that are functionally determined by the merge keys and the runner bounds the MERGE scan on them too (GH #62). On the web-traffic facts, `bound_by = ["event_date"]` cuts the nightly scan ~68× and holds flat as the fact grows. A per-batch tripwire fails the run if a merge key maps to more than one value of a `bound_by` column.

### Changed

- Merge-mode outputs now bound the MERGE target scan to the batch's key range when a merge key is also a clustering column, so an incremental merge reads only the touched files instead of full-scanning the whole table every run (GH #62). Automatic, no config, and provably semantics-preserving; the win grows with table size. Delta does no dynamic file pruning on the merge join condition itself, so `merge_keys` alone never bounded the scan before.

## [v2.11.1] — 2026-06-15

### Fixed

- Local runs no longer break once ~30 workspaces have been created (GH #42). The shared local metastore used a per-workspace docker network that was never reaped; each tempdir/throwaway workspace leaked one, and after they fully subnetted docker's address pool, `network create` failed, every local run silently fell back to embedded Derby, and the next run deadlocked on the Derby lock with an inscrutable `XSDB6` stack. All workspaces now share one `clavesa-net` network (per-workspace container names keep clients unambiguous), so nothing leaks. `clavesa ui` also reaps orphaned per-workspace networks from older versions on startup, so an already-affected machine self-heals.
- Local docker runs (`pipeline run`, `pipeline run --compute local`, backfill stage/promote/discard, and the `_maintenance` OPTIMIZE/VACUUM) no longer throttle Spark to a 1 GB JVM heap on multi-GB hosts (GH #58). The runner's heap auto-sizer only fired for memory-capped or Lambda containers, never for the uncapped local-docker path, so every local run silently used the 1 GB fallback regardless of host RAM. Clavesa now sizes the heap from the Docker VM's available memory, which is exactly the big-backfill / cost-per-billion path where a 1 GB heap hurts most. The `--compute local` dispatcher (no co-resident metastore) gets a generous heap; local-warehouse runs reserve more, since under `clavesa ui` they share the VM with the metastore and a warm query-worker JVM. `CLAVESA_JVM_HEAP_MB` still overrides.

## [v2.11.0] — 2026-06-14

### Added

- `workspace init` now scaffolds an opt-in `_maintenance` pipeline (GH #53): a single PySpark transform that OPTIMIZEs and VACUUMs the workspace system bookkeeping tables (`node_runs`, `runs`, `tables`, `column_stats`) on a daily schedule so their Delta `_delta_log` and small-file count stay bounded — the active half of the #53 fix, as a scheduled, observable pipeline rather than work hidden on the per-node write path. It is written to disk but not deployed; enable it with `clavesa pipeline deploy _maintenance`, change the schedule via `trigger_schedule`, or delete the directory to opt out. Touches only the workspace-owned system catalog, so it needs no IAM beyond the standard per-pipeline runner grant. Also backfills the #53 retention properties onto system tables created before they shipped.
- `pipeline run --compute local` (ADR-024): on a cloud warehouse, run the whole pipeline in a local docker container against the cloud data instead of dispatching Step Functions — laptop compute against real cloud tables, the cost-per-billion win. The local run drains the same SQS source cursors and advances the same watermarks as a deployed run, and lands output + `node_runs` (with `compute_target=local`) in the same cloud warehouse; `--wait` and the dashboard read live progress identically. The dashboard surfaces it as a dedicated **Run locally** button next to **Run on cloud** (placement is visible before you click, not hidden in a menu), dispatched asynchronously so the browser navigates straight to live run progress. Needs the local principal to have source-S3 read, warehouse-S3 read/write, Glue read/write, and SQS consume.
- `pipeline backfill promote --compute local` and `pipeline backfill discard --compute local` (ADR-024): the MERGE / staging-table cleanup runs in a local docker container against the cloud warehouse, like `stage --compute local`. Exposed as one shared cloud-warehouse-only toggle above the promote/discard buttons in the backfill detail view.
- `pipeline backfill stage --compute local` (ADR-024): on a cloud warehouse, run the heavy Spark staging job in a local docker container against the cloud data instead of the deployed Lambda — the workaround for Lambda's 15-minute cap on large historical windows (GH #43). Output still lands in the cloud warehouse; `node_runs` records `compute_target=local` and the run is tagged so the backfill list and the "computed locally" UI chip show where it ran. The UI exposes it as a cloud-warehouse-only checkbox in the stage dialog. Needs the local principal to have source-S3 read, warehouse-S3 read/write, and Glue read/write; `CLAVESA_JVM_HEAP_MB` overrides the container heap for big windows.

- Engine badges (ADR-024): every SQL-running surface — /query, the dashboard widget editor, notebook cells, editor preview, and the table sample-rows card — shows which engine and warehouse will run the SQL (e.g. "Spark · local docker · cloud warehouse", "Athena (transpiled) · cloud warehouse"). The badge is shown up front — predicted from the warehouse + surface kind before you run, dimmed, then confirmed (and gaining "(transpiled)") from each response's `served` metadata, which the executing code stamps. `clavesa query` prints the same line and carries `served` in `--json`. Raw S3 source previews carry no badge (no engine runs).

### Changed

- Ad-hoc `/query` and `clavesa query` now validate SparkSQL for Trino/Athena portability on every warehouse, not just cloud. Non-portable SparkSQL is rejected up front on a local warehouse too (it still executes on local Spark once the check passes), so a query that runs in `/query` is guaranteed to run as a cloud dashboard widget instead of only failing after deploy. The `/query` page copy now states this.
- Deployed pipeline runs acquire the same warehouse run lock (ADR-024): the pipeline Lambda takes `s3://<bucket>/<pipeline>/_locks/run.json` before the first node and releases on terminal, so a scheduled run and any other compute serialize per pipeline; a held lock fails the SFN execution with the holder's identity, and `pipeline run` on a cloud warehouse pre-flights the lock for a fast CLI failure. Already-deployed pipelines enforce after their next `workspace upgrade` + deploy.
- Local pipeline runs are serialized by a per-pipeline lease lock in the warehouse (ADR-024): a second `pipeline run` — another process, the CLI racing the UI, or a second machine on a shared warehouse — is rejected with the holder's identity ("run lock held by run <id> (compute=local, host=…, acquired 8s ago)") instead of risking concurrent Spark drivers corrupting a Delta log. The lock releases the moment a run is observably terminal, fixing the spurious 409 when re-running right after completion (GH #48).
- /query dialect rejections (SparkSQL that can't transpile to Athena) now return 400 with the transpiler's message, matching the dashboards editor, instead of a generic 500.
- Notebooks, editor preview, and the warm query worker now follow the workspace warehouse (ADR-024): on a cloud-warehouse workspace they run local Spark against the deployed Glue catalog + S3 data instead of silently targeting the local on-disk warehouse. A cloud warehouse on an undeployed workspace is an actionable error (HTTP 409 / CLI exit 1), and SQL parse-checks degrade to a loud skip.
- `clavesa query` now dispatches by the workspace warehouse like the UI's /query page (was: always local, returning nothing on cloud workspaces): cloud routes to Athena with your SparkSQL transpiled (ADR-023), `--warehouse local|cloud` overrides per invocation. `--json` now emits the same wire shape as the UI (`columns`/`rows`/`row_count`/`truncated`, string-rendered values) on both warehouses.
- The workspace environment mode is now the **warehouse** (ADR-024): the header toggle, `workspace use --warehouse`, and `pipeline run --warehouse` set/override where all workspace state lives (local Hadoop catalog vs Glue + S3). `--env` remains as a deprecated alias; `.clavesa/environment.json` and `GET/PUT /workspace/environment` carry both the new `warehouse` key and the legacy `mode` key. Where heavy work executes (local docker vs Lambda) becomes a separate per-action compute choice in upcoming releases.
- `make release-gates` runs the three release gates (`test`, `smoke-cloud`, `verify-readme`) concurrently with per-gate logs; `make release-check` now also enforces tree-exact green stamps for the local gates.

### Fixed

- S3-event-driven sources now drain their SQS queue to empty per run instead of one batch per trigger (GH #52): the runner treated any `ReceiveMessage` returning fewer than 10 messages as "queue drained" and stopped, but SQS routinely returns short batches even with thousands of objects still queued, so ingestion was capped at roughly one batch per poller tick regardless of arrival rate (a 19-message backlog could not recover in a single run). The drain loop now continues until a genuinely empty receive (with a 20s long poll so that read is authoritative) or the existing per-run cap, and a backlog beyond the cap finishes on the next poller tick. The per-pipeline run lock keeps those executions serialized.
- The workspace bookkeeping tables (`node_runs`, `runs`, `tables`, `column_stats` under the system catalog) now bound their Delta `_delta_log` growth (GH #53): on first creation they get a 24-hour `logRetentionDuration` / `deletedFileRetentionDuration` and an explicit checkpoint interval, so checkpoint truncation reclaims old commit files instead of accumulating thousands of them (which had grown to ~77% of per-cycle S3 LIST cost). These are append-mostly operational logs and don't need Delta's 30-day default retention; active compaction/VACUUM is handled separately by the scaffolded `_maintenance` pipeline. Existing system tables get the properties set the next time the maintenance pipeline runs.
- The workspace bucket now expires noncurrent object versions and reaps delete markers (GH #54): with versioning on and Delta churning every MERGE/OPTIMIZE/VACUUM/`_delta_log` truncation, superseded objects previously accumulated forever. A new always-on lifecycle rule expires noncurrent versions after `noncurrent_version_retention_days` (default 7) and removes expired delete markers, decoupled from the Athena-results retention window. Already-deployed workspaces get it on the next `workspace upgrade` + deploy.
- Cloud-local runs (`pipeline run --compute local` on a cloud warehouse) now report correct live per-node progress and final status in the dashboard run-detail and the CLI; the run-detail DAG previously stuck on the first node while the run finished underneath. Run progress now follows the warehouse rather than the runtime — every compute (Lambda, local docker, Fargate, EMR) writes per-node progress to the warehouse `_progress` channel, read back uniformly.
- The Catalog's Rows and Commits columns now populate for the workspace system tables (`runs`, `node_runs`, `column_stats`, `tables`) on a cloud warehouse; they previously showed "—" because dir-less catalog reads routed to an Athena-only provider that couldn't read the Delta `_delta_log`.

## [v2.10.0] — 2026-06-11

### Changed

- `pipeline deploy` and `clavesa deploy`/`plan` now regenerate `orchestration.tf` from the installed binary's emitter before terraform runs. Emitter fixes reach deployed pipelines on the next deploy instead of waiting for a version-bump `upgrade`. `orchestration.tf` is a generated file — manual edits to it no longer survive a deploy.
- `make test` now includes the docker-gated runner suite (`test-runner`); docker is required, no silent skip.
- New cloud smoke gate: `make smoke-cloud` verifies a release end-to-end against a deployed AWS workspace (SFN run, Athena/Glue readback, UI API, backfill cycle); `make release-check` requires a green stamp for the version being released.

### Fixed

- Lambda runner `/tmp` exhaustion during large Spark shuffles (GH #43): ephemeral storage raised from 512 MB to 10 GB in generated `orchestration.tf`; the Spark session is recycled after any failure (transform, backfill stage, or operation — Spark's shutdown hooks clean blockmgr/spill dirs) and before any invocation when `/tmp` is already >50% full on a warm Lambda container. The pressure check is Lambda-only: local Docker containers see the host disk there, and recycling on a half-full laptop would defeat warm-session bundling.
- Fresh cloud deploys with a partitioned s3 source never ingested pre-existing files: the trigger queue (empty, since the files predate it) was consulted before the first-run `start_from` listing, so every run skipped. The queue now takes over only after the first listed run commits its watermark. The flat-source variant is GH #44.
- `pipeline run --force` now actually forces past an empty trigger queue (full-range listing re-read; queued messages are left for the next unforced drain). On cloud the flag never reached the runner at all — the orchestrator read it from the wrong event level.
- Cloud runs stamp `clavesa.trigger` on Delta commits again (the manual/scheduled/upstream badges on the table volume timeline) — same wrong-event-level read.

## [v2.9.0] — 2026-06-10

### Added

- `clavesa deploy` applies the workspace infra and every pipeline in one command (workspace first, since pipelines read its remote state). `clavesa plan` is the no-apply dry run. Re-running is a cheap no-op; terraform decides what actually changes.
- `clavesa pipeline reset` drops a pipeline's canonical output tables (Delta data + Glue catalog entries) and, by default, its CDF watermarks, so the next run rebuilds everything from source — without touching the deployed Lambda/SFN/IAM stack. `--node` scopes to one transform; `--include-watermarks=false` keeps cursors. Also available from the pipeline dashboard ("Reset data"). (#10)

### Fixed

- Saving SQL from the UI editor failed with a parse error (`Syntax error at or near 'file'`) whenever the warm Spark worker was up — the authoring parse-check validated the editor's `file("<node>.sql")` reference instead of the SQL it points at, leaving the node half-saved. The check now validates the referenced file's content.

## [v2.8.1] — 2026-06-09

### Changed

- Serving SQL (dashboard datasets, widgets, ad-hoc `/query`) is now authored in **Spark SQL** and transpiled to Athena/Trino for cloud serving; local runs the authored Spark unchanged. Previously serving SQL had to be hand-written in the Trino-portable subset. Existing dashboards keep working with no migration, and Spark-only serving SQL is now caught at author time on local workspaces instead of breaking only after a cloud deploy. See ADR-023.

### Fixed

- `clavesa workspace deploy` now always pushes the runner image to ECR. Previously the push was gated on the runner version, so rebuilding the image with new content under the same version (e.g. adding a pip dependency) left ECR — and the deployed Lambda — on the stale image. (#35)
- `clavesa pipeline backfill stage`/`promote`/`discard` on a deployed cloud pipeline silently did nothing — they returned `status: ok` in ~40 ms without running Spark, materializing a staging table, or executing the promote/discard. (The per-pipeline runner Lambda only dispatched full-pipeline runs and ignored single-node backfill/operation payloads.) Backfill stage also now exits non-zero with an accurate message when the staging run is skipped/fails or the staging table can't be registered in Glue, instead of printing "Backfill staged".
- Cloud backfill against a source with event-driven triggers read 0 rows — the runner's notification-queue drain short-circuited before the backfill window was read. The historical window now takes precedence over the live queue.
- Cloud `backfill promote` on the first backfill for a node (no canonical table yet) failed with `TABLE_OR_VIEW_NOT_FOUND`. It now creates the canonical target from the staging table, as the diff output already promised.

## [v2.8.0] — 2026-06-08

### Added

- Runner Python dependencies for UDFs. Add third-party pip packages (e.g. `pyasn`, `crawlerdetect`) to the transform runner image via `clavesa runner requirements add/remove/list/import/show`, the `/runner` UI page, or by editing `.clavesa/runner-requirements.txt` (standard pip format) directly. They install into the image on the next build and are importable from SQL/PySpark transforms. Replaces the dead-end of editing `runner/requirements.txt` (a regenerated mirror that gets clobbered on every build).

### Removed

- `docs/cookbook/migrate-to-v2.md`. The v1.x→v2.x recreate-from-source recipe had no install base to serve; the Iceberg→Delta tradeoff it summarised lives in ADR-018.

### Fixed

- `clavesa pipeline backfill` (`stage`/`diff`/`promote`/`discard`) on a deployed cloud pipeline failed with `node "<name>" not found in SFN definition` for every node, making the whole backfill flow unusable. Node resolution still looked for one Step Functions state per transform, but since v2.2.0 the state machine is a single `RunPipeline` task carrying all transforms in its payload; resolution now reads the node out of that payload (and still handles the pre-v2.2.0 per-state shape).

## [v2.7.5] — 2026-06-08

### Changed

- `world_map` widgets now colour countries with the theme's accent at a value-ramped opacity (dim → solid) and shade no-data countries with the muted "land" token, instead of a light-blue scale that bottomed out near white. Reads correctly on the dark dashboard (and stays right in light mode), and matches the bar/pie palette.

### Fixed

- `workspace upgrade` and `workspace deploy` no longer fail with a missing `:vX.Y.Z` runner image when a version bump ships no runner-code changes. The local runner image is now always rebuilt (docker's layer cache makes a no-change rebuild a fast no-op) and tagged with both `:latest` and the version, replacing a hand-rolled SHA-label staleness check that could leave the version tag uncreated.
- `world_map` dashboard widgets rendered as an empty (all-uncoloured) map: the API and CLI bridge structs dropped `region_field` (and `tooltip_field`) when serving a dashboard, so the choropleth had no region column to join on. Both fields now round-trip.
- `clavesa query` text output no longer prints large integers in scientific notation (`2319046` showed as `2.319046e+06`); counts and IDs render as plain integers, matching `--json`.
- A failed local query (e.g. a typo'd table name) now shows just the Spark error message instead of ~80 lines of Java/Scala stack trace; the command still exits non-zero. Applies to `clavesa query`, notebooks, and dashboards, which share the warm-Spark error path.

## [v2.7.4] — 2026-06-07

### Changed

- The Column profile's top-value breakdown now reads like Kibana's field popover: each value sits in its own aligned column (value · bar · percentage), the percentage is of all rows (not just the shown top-K), and a muted "other" row makes the long tail visible when the top values don't cover everything. Exact counts show on hover.

### Fixed

- The Catalog page no longer stalls for tens of seconds on workspaces with long-lived append tables. Reading a Delta table's schema now uses the table's checkpoint instead of replaying every commit back to version 0, so an append-only table like `node_runs` with thousands of commits reads its schema in a handful of S3 requests rather than one per commit. On a real cloud workspace this dropped the catalog load from ~47s to ~4s.

## [v2.7.3] — 2026-06-06

### Changed

- S3 sources now ingest by draining their notification queue (the SQS queue fed by S3 `Object Created` events) instead of listing the bucket prefix every run, the same file-notification approach as Databricks Auto Loader. Each run reads only the new objects and deletes the messages after the write commits, so source `ListBucket` cost stops growing with accumulated history. Applies to partitioned and flat s3 sources alike; flat sources, which previously re-read the whole prefix every run, benefit most. Default and automatic, with no configuration. Local runs fall back to listing (no queue) and produce identical results. Ingest is at-least-once on retry, so pair `append` outputs with a downstream dedup or use `merge`.

### Fixed

- The source trigger queue's visibility timeout is now sized to the run (default 900s) and backed by a dead-letter queue, so an in-flight run's messages can't be re-read mid-run and a repeatedly-failing object drops to the DLQ instead of cycling forever.

## [v2.7.2] — 2026-06-06

### Fixed

- `.tf` files emitted by `node add` / `node edit` / `node disable`/`enable` are now byte-stable: block attributes write in a fixed canonical order, so re-emitting an unchanged block is a no-op diff instead of reshuffling attribute alignment run-to-run.
- `clavesa ui` now tears down its shared local Derby metastore container on exit, so a subsequent `clavesa pipeline run --env local` no longer fails with "Another instance of Derby may have already booted the database". If a held lock is still hit (e.g. after a SIGKILL, or a `ui` left running), the local run now leads with an actionable remedy: stop `clavesa ui`, or `docker stop` the named container, instead of a raw stack trace.

## [v2.7.1] — 2026-06-05

### Added

- `clavesa pipeline cost [dir]` and a "Cost" card on the pipeline dashboard surfacing clavesa's north-star metric: **cost per billion records processed**. Prices each node's recent runner invocations from a static per-target table (Lambda, Fargate, EMR Serverless; local = $0) and reports the blended cost-per-billion alongside sustained throughput (records/sec). Local pipelines show $0 compute and still report throughput, so the efficiency half holds before deploy. `--json`, `--last N`, local and cloud (ADR-014/015).

### Fixed

- Runner now self-heals a dead Spark session instead of wedging the pipeline. A crashed/heartbeat-killed session was cached and reused on warm Lambda invocations with no liveness check, so every later run (and the scheduled cron) failed with `ConnectionRefusedError` until the container recycled. `_spark()` now probes the cached session and rebuilds it when dead, so the next invocation recovers cleanly. Failed-node telemetry survives a dead session too.
- Runner JVM heap is now sized to the container instead of a hard-coded 1 GB. The launcher capped the driver/executor JVM at `-Xmx1g` regardless of the Lambda's memory, so a shuffle-heavy transform exhausted the heap and GC-thrashed until the session died — even with gigabytes of *container* memory free (it was the JVM heap ceiling, not the container, that ran out). The heap is now ~75% of `AWS_LAMBDA_FUNCTION_MEMORY_SIZE` (or the cgroup limit locally), overridable via `CLAVESA_JVM_HEAP_MB`. Heartbeat/network timeouts were also widened as a secondary cushion, and the resolved Spark master + heap size are logged for diagnosis.
- Local pipeline runs surface the runner container's real stderr. Bundle-run stderr is teed to `_bundle.log` and appended to the failure error even when the container exits 0, so the actual Spark stack trace is visible and failures stop being attributed to the wrong node.

## [v2.7.0] — 2026-06-03

### Added

- `cluster_by` on `output_definitions`: liquid-cluster a non-merge (replace/append) output Delta table on declared columns for prune-friendly reads (e.g. `cluster_by = ["event_date"]` on a fact queried by date). Set via the CLI (`node edit --output-cluster-by`) or the node config panel's "Cluster by" field; it does not change the write mode. Merge outputs already cluster by their merge keys. Capped at Delta's 4-column limit.
- `clavesa pipeline optimize [dir]` and an "Optimize tables" button on the pipeline dashboard: compact a pipeline's Delta output tables. `--recluster` migrates pre-clustering tables to liquid clustering (`ALTER TABLE CLUSTER BY` + `OPTIMIZE`); `--vacuum` prunes tombstoned files. Per-node or whole-pipeline, `--json`, local and cloud.
- `merge_update` on `output_definitions`: per-column merge expressions for `mode = "merge"` outputs, so incrementally-read aggregates accumulate instead of being overwritten. Each column maps to a keyword (`additive`, `min`, `max`, `sketch`) or a raw SparkSQL expression over `target.`/`source.`; unlisted columns keep replace semantics. Fixes lifetime counters, daily `SUM` rollups, and `first_seen`/`last_seen` that merge-replace silently clobbered. Set via `node edit --output-merge-update` or the node config panel's "Merge update" field.

## [v2.6.1] — 2026-06-03

### Fixed

- Cloud per-node run history and column profiles populate again. The runner's `node_runs` / `tables` / `column_stats` observability tables were registered in Glue at an empty placeholder location with no Delta provider, so Athena and the catalog couldn't read them — the data was intact at the warehouse path, just orphaned from the Glue pointer. The runner now repairs each table's Glue location + Delta provider on write; deployed workspaces self-heal on the next run after `workspace upgrade` + deploy. Local was unaffected (no Glue).

### Changed

- `mode = "merge"` output tables are now created with Delta **liquid clustering** on their merge keys (the first 4 if the key is wider — Delta's clustering-column limit), so MERGE upserts and keyed reads prune to the relevant files instead of scanning the whole table. Automatic, no config. Existing tables pick it up when next created (or after a reset); a full recluster of already-written data awaits a compaction command.
- Optimized writes (`delta.optimizeWrite`) are on by default for the runner, coalescing small output files at write time so file counts stay bounded without a manual `OPTIMIZE`.

## [v2.6.0] — 2026-06-02

### Fixed

- Restored table-to-table **incremental (CDF) reads** — the reason for the Iceberg→Delta move (ADR-018) — which had silently broken. The transform module dropped the `incremental_inputs` variable after v0.19.0, so declaring it failed `terraform validate` ("Unsupported argument"), and `pipeline upgrade` *stripped* it — even though the orchestration emitter still read it to build the `delta_table_cdf` descriptor. Net effect: an incremental input fell back to a full scan every run, and a keyed `merge` output behind it that should have deduped instead piled up duplicates. The module re-declares `incremental_inputs` (a passthrough the emitter reads), and `pipeline upgrade` no longer strips it. Authoring shape: `incremental_inputs = ["<alias>"]` + a `merge` output keyed on the upstream's grain. The node config panel's "Incremental upstream reads" toggle now also lists cross-pipeline inputs (it was scoped to same-pipeline transform edges only), so the full-table-vs-CDF choice is settable from the UI for a separate-pipeline medallion, not just the CLI/`.tf`.
- `CLAVESA_MODULE_VERSION` stamped into the pipeline Lambda (provenance on `node_runs`) now tracks the real version — it had drifted, hardcoded at `v2.2.2` since v2.2.2 while the binary moved to v2.5.0. Sourced from the leaf `internal/version` package (no import cycle), so it can't drift again.

### Changed

- The bundled pipeline runner caches an input that multiple nodes read (the medallion shape — one silver table feeding many gold dims) for the duration of a run, so the shared upstream is scanned from S3 once instead of once per node. On the web-traffic gold pipeline this cut per-node overhead enough that `stats = true` now completes in ~526s where it previously hit the 900s Lambda ceiling (GH #14).
- The runner right-sizes `spark.sql.shuffle.partitions` to the runtime instead of using Spark's cluster-oriented default of 200 — ~4 partitions per vCPU (Lambda: 3008 MB → 8, scaling with memory; off-Lambda falls back to CPU count), with AQE coalescing left on. Removes needless task-scheduling overhead on the small-data Lambda tier. (Note: this is a sound default but was *not* the cause of the gold-pipeline 900s timeout — that's per-node file-scan of the small-file upstream plus column-stats passes; see #14.)

### Added

- Nodes can be paused with `enabled = false`: a disabled transform is skipped in runs but keeps its module and last output table, so downstream still reads that table — a pause, not a delete. Settable from the CLI (`clavesa node disable` / `enable <pipeline> <node>`) or the node config panel; paused nodes render dimmed with a "Disabled" badge on the canvas and run DAG.
- Dashboard saves are gated on serving-engine portability (ADR-022). On a cloud workspace each dataset/control query is dry-run via Athena `EXPLAIN` at save, so Spark-only serving SQL (single-arg `to_date`, `APPROX_COUNT_DISTINCT`, a bare timestamp-vs-string comparison, …) is rejected at save with the engine's own message instead of returning 500 at render. Local workspaces (no Athena) are unaffected.

### Fixed

- The "Recent executions" list on a pipeline's Runs tab is ordered newest-first again. Step Functions' `ListExecutions` ordering isn't reliable when several executions start in the same minute (a scheduled run plus cross-pipeline triggers), so the list rendered out of order; the handler now sorts by start time explicitly.
- Catalog now shows real row counts for cloud Delta tables (the "rows" column was blank). The count is derived by walking the Delta commit history — the same computation local already did — instead of reading only the latest commit's record total, which MERGE-mode commits don't carry.
- Catalog's "commits" column shows the true commit count instead of a capped "20+".
- The pipeline runs grid's sticky node column is now opaque — run cells no longer scroll visibly through the "Node · output table" panel.
- The run-detail sheet no longer blurs the page behind it; the backdrop-blur over the live dashboard was the cause of the high CPU and sluggishness while the sheet was open.
- Cloud observability queries (catalog, run history, dashboards) reuse Athena results within a short window, cutting the per-query cold-start on repeated page loads.
- The run-detail DAG now colours live during a cloud (Step Functions) run. The pipeline-status handler's cloud provider was built without an S3 client or the workspace bucket, so it could not read the per-node `_progress/<execARN>/<node>.json` objects the runner publishes each poll tick — in-flight node states came back empty and the DAG stayed grey until the run finished. It is now wired with both, matching the other UI handlers; the runner-side publishing was already working.
- Cloud run status is now live and in sync across the run-detail DAG and the dashboard runs grid: each node turns green the moment it finishes, instead of completed nodes lagging behind a slow Athena read or showing as frozen/pending mid-run. The runner now writes a terminal `succeeded`/`failed` marker into each node's `_progress` file, and the status channel reads node status straight from those files for both running and finished executions — one fast authoritative source per node.
- The run-detail DAG no longer flickers its edge labels or janks the canvas during a live run — per-node status updates no longer trigger a full graph relayout.

- Cloud run history now appears on the dashboard and `/pipelines` (previously "Never run" even though the scheduled runs succeeded). Two causes: (1) the runs-writer Lambda's role was missing `glue:GetDatabase` on the `default` database, so its Spark session failed to initialise and the per-execution `runs` table was never created; (2) on Lake-Formation-enabled accounts the workspace owner — the principal the local `clavesa ui` queries Athena as — had no read grant on clavesa's catalogs, so Athena denied every dashboard query. The orchestration now grants the deploying principal `SELECT`/`DESCRIBE` on each pipeline's database and the system observability catalog.

- Cloud Delta tables now show their real schema in the Glue Data Catalog instead of a single `col array<string>` stub — so the Athena table browser, autocomplete, `information_schema.columns`, external/BI consumers, and Lake Formation (which requires the catalog schema to match the transaction log) all see the genuine columns. Spark's `saveAsTable` registers the stub because the real schema lives in the Delta log; the runner now syncs the actual columns into each Glue table's `StorageDescriptor` after every write, covering transform outputs and the system observability tables (`runs`, `node_runs`, `tables`, `column_stats`). `SELECT` already worked via Spark's `spark.sql.sources.provider=delta` marker; this fixes everything that reads Glue's column metadata.
- Saving a dashboard that uses a control (e.g. a time-range filter) no longer fails with a spurious `PARSE_SYNTAX_ERROR`. The save-time SQL parse-check now expands `{{...}}` control placeholders from the controls' defaults before parsing — the same expansion the render path does — instead of feeding the raw template to the parser, which choked on `{{`.

## [v2.5.0] — 2026-05-31

### Added

- Notebooks have a "Run all" action that runs every code cell top to bottom, sequentially (cells share one Spark session), stopping on the first error. To avoid clobbering the notebook file, the client no longer autosaves while a cell run is in flight: each run is persisted server-side, so a concurrent client autosave during rapid "Run all" execution could otherwise race the server's per-cell write and drop cells. Autosave resumes once runs finish.
- In-flight Spark query feedback in the local UI. While a Spark-backed query runs, the header shows "Spark · running query…" (it previously only reflected worker boot state), the table-detail Columns card shows "querying…" with a spinner, and each column's null % / examples render a loading skeleton instead of a bare "—" — so a pending query reads as loading, not as missing data. Driven entirely client-side from the in-flight query state; "—" now unambiguously means "no data".
- Local querying and pipeline runs now run side-by-side. A shared per-workspace Derby metastore service (the local analog of cloud's Glue) lets the UI's warm Spark query worker keep serving the Catalog, table previews, and `/query` while an on-demand `pipeline run` executes against the same warehouse, and lets a CLI `clavesa pipeline run` run while `clavesa ui` is open. Previously these collided on the single-writer embedded Derby lock and one side failed with `Unable to instantiate SessionHiveMetaStoreClient`. The metastore container is brought up automatically; no configuration needed.

### Changed

- Dashboards are now workspace-level IaC: each definition is a JSON file under `.clavesa/dashboards/<slug>.json`, read directly and version-controlled alongside pipelines and sources (ADR-021). Create/edit now works identically in local and cloud mode (the prior system Delta table could not be written through Athena), survives a warehouse rebuild, and loads hand-authored `*.json` dashboards. Existing dashboards in `.clavesa/dashboards.imported/` or a workspace-root `dashboards/` directory migrate into the registry automatically on first read. Widget SQL still runs through the active engine (local Spark / cloud Athena), unchanged.

### Fixed

- The local warm Spark worker no longer breaks after a long idle. Spark Connect garbage-collects an idle session (default 60m) and the worker cached the session handle forever, so the first query after a few hours failed with `[INVALID_HANDLE.SESSION_CLOSED]` and stayed broken until the worker was restarted. The idle-session timeout is now pushed out to 7 days, and if a session is closed anyway the worker rotates to a fresh session id and retries once (a reaped session id is tombstoned, so reusing it fails again). Queries self-heal transparently.
- The `/query` editor's default query no longer errors on load. It seeded `SHOW NAMESPACES IN clavesa`, but there is no catalog named `clavesa` (tables are addressed `<catalog>__<schema>.<table>` under the default catalog), so it failed with `SCHEMA_NOT_FOUND`. The default is now `SHOW DATABASES`, valid on both the local Spark and cloud Athena engines.
- Catalog no longer double-lists tables that are both deployed and run locally. It now reads only the active environment's world (the Local/Cloud toggle): local mode lists the on-disk warehouse, cloud mode lists Glue. As a bonus, local mode no longer pays the Glue + Delta-log enrichment for cloud tables you weren't viewing.
- Catalog and table-detail no longer spew console errors for a table whose name is not a clean identifier (e.g. a manual `foo.backup_20260530` with a dot). The snapshot, column-stats, and sample queries are skipped for such names instead of firing requests the server rejects with a 400.
- `clavesa ui` now self-heals a missing workspace runner image. If the local `<workspace>/transform-runner:latest` tag is gone (pruned, or only a versioned tag survived a fresh checkout), the warm Spark worker used to fail with a cryptic "pull access denied" and every Spark-backed surface (table preview, `/query`, column profiles) silently errored. Startup now retags the image from the dev/embedded source the same way `workspace init` does, in the background, and prints a clear actionable message if it cannot.
- Pipeline DAG now frames the whole graph on load. A wide fan-out (e.g. a star schema with many dimensions reading one source) no longer spills off-canvas clipped at the top and bottom; the view re-fits once node sizes are measured and can zoom out far enough to show every node.
- Terminal star-schema dimensions no longer raise false "node has no edges" warnings. A transform fed only by a workspace source or a cross-pipeline table is connected to the data flow even though that reference is not an edge between two nodes in its own pipeline.
- Workspace-wide system tables (`runs`, `node_runs`, `column_stats`, `tables`) no longer 400 on their snapshot timeline in a local workspace. They have no owning pipeline, so they carry no `dir`; the data endpoints now dispatch dir-less requests to the workspace-level local provider instead of rejecting them. Removes a batch of console errors on the Catalog page in local mode.
- Catalog and table-detail pages load fast again. The per-table Delta-log schema reads are now cached (5-minute TTL), so repeat catalog loads drop from ~14s to under a second. The table-detail page no longer blocks its Columns card on the whole-catalog query — it renders the schema from the table's own sample as soon as the fast pipelines list resolves, which also fixes the Columns card spinning forever on tables without a column profile.
- Cloud Catalog now shows each Delta table's real schema instead of a single `col array<string>` stub. Glue records Delta tables with a placeholder schema (the real one lives in the Delta log), so the catalog list, the table-detail Columns card, and the editor node cards all mis-reported every cloud table as one column. The catalog now reads the schema from each table's `_delta_log/` on S3, matching the local path (ADR-014). Tables are also correctly badged `DELTA`, which lights up the format and commit-count columns that were blank.

## [v2.4.0] — 2026-05-30

### Added

- Per-run Spark job stats on every node run: peak process memory (`peak_rss_mb`), peak execution memory, memory/disk spill, shuffle read/write, input rows/bytes, stage/task counts (incl. failed tasks), GC time, and executor CPU/run time. Captured by the runner via the Spark event log + `/proc` and stored on the `node_runs` table, so they work for local and cloud pipelines alike. The run-detail node drawer now shows a "Peak memory" reading (with utilization vs allocated) and a "Spark metrics" panel.
- Live in-flight task progress for local and cloud runs: while a transform runs, its stage/task counts stream into the run-detail node (a "124/300 tasks · stage 3/9" progress bar) and the `clavesa pipeline status [dir] [--json]` command. Captured by a Spark statusTracker poller in the runner. Cloud runs surface the same per-node progress, read from the `_progress` objects the runner publishes to S3.
- Rightsizing recommendations (recommend-only): `clavesa pipeline rightsize [dir] [--json] [--last N]` and a "Rightsizing" card in the run-detail node drawer recommend a per-node Lambda memory allocation from the p95 of recent peak RSS, factoring spill. Reads the same `node_runs` metrics for local and cloud pipelines; never re-deploys.

### Fixed

- `clavesa ui` no longer intermittently fails to start its warm Spark worker with `warm Spark spawn failed … docker port … exit status 1 (last output: "")`. Docker Desktop sometimes accepts a port-publish request but never wires the host-side forwarding; the warm-worker spawn now retries with a fresh container, binds the worker to loopback only, and surfaces the real container state + logs when it does fail.

## [v2.3.1] — 2026-05-30

### Added

- The pipeline dashboard's Run button now works for cloud pipelines (was previously local-only) and exposes a Force checkbox + force-nodes input so the UI matches `clavesa pipeline run --force` / `--force-node`.
- `clavesa workspace destroy --yes` and `clavesa pipeline destroy --yes` skip the interactive `yes` prompt. `workspace destroy` also pre-empties the versioned workspace bucket and drains the Athena workgroup so `terraform destroy` doesn't 409 on bucket / workgroup state.

### Changed

- `clavesa workspace upgrade` now upgrades the workspace shell AND every pipeline in one shot; pass `--shell-only` for the previous shell-only behaviour.

### Fixed

- CSV sources now infer numeric column types instead of defaulting every column to STRING (which broke any `WHERE col > N` predicate with a `CAST_INVALID_INPUT` error).
- Local-mode runs that short-circuit early (every node skips, or no work dispatched) now write a terminal `SUCCEEDED` row to the `runs` system table — fixes the phantom `Running` row + `—` duration on the pipeline dashboard.
- `clavesa workspace upgrade` bumps `runner_version` in variables.tf so post-upgrade deploys push the new runner image.
- `clavesa pipeline upgrade --version` help text now states the default is this CLI's module version.
- A transform that reads another transform in the same pipeline no longer fails with `TABLE_OR_VIEW_NOT_FOUND` — a default-only output is now addressed by its bare table name everywhere (module output and runner agreed on the bare name; the module previously emitted a phantom `__default` suffix). (#5)
- String-form intra-pipeline input refs (`inputs = { x = "<schema>.<sibling>" }`) are now ordered correctly; previously they lost their dependency edge and a downstream node could run before its upstream table existed, deadlocking the first run. (#6)
- Cross-pipeline reads now work on Lake-Formation-gated accounts: the runner role is granted read access on each upstream schema it reads, fixing `Insufficient Lake Formation permission(s): Required Describe` that broke the EventBridge medallion cascade. (#4)
- The pipeline dashboard no longer 502s when loading backfills against a cloud workspace (the backfill paths used a non-existent per-node Lambda name instead of the single `clavesa-<pipeline>-runner`).
- `clavesa pipeline backfill list/diff/promote/discard` no longer report `run_id not found` for a default-only transform — the backfill bookkeeping recorded the table as `<node>__default` while the runner writes the bare `<node>`; both canonical-name paths now use the bare name. (#9)

## [v2.3.0] — 2026-05-29

### Added

- `clavesa node edit --set sql=…`, the UI SQL editor, `clavesa dashboards apply`, and `clavesa node preview` now parse SQL before persisting or dispatching — bad SQL is rejected with the parser's pointer-into-SQL message instead of failing after a Spark cold start.
- `clavesa sql lint <file>` — parse-only check for use in pre-commit hooks and CI.
- `clavesa pipeline run --force` (and `--force-node <id>`) bypasses the runner's incremental-skip checks for one run (partitioned source cursor + Delta upstream version cursor). Watermarks still advance on success. Append-mode outputs without `merge_keys` get a duplicate-warning before dispatch.

### Fixed

- Lake Formation-gated AWS accounts (default on accounts created after Aug 2023) now provision cleanly: orchestration emits `aws_lakeformation_permissions` alongside every Glue database it creates, so the runner Lambda no longer hits `Required Describe on clavesa_<workspace>__<schema>` on first read. Deploying principal must be a `DataLakeAdmin`; see the README "Lake Formation-enabled accounts" callout.
- Step Functions executions now fail when any transform in the bundle fails — previously the runner returned a `{status: failed}` payload that SFN treated as success, leaving downstream pipelines silently triggered on hidden failures and the `runs` system table polluted with false-positive successes.
- `clavesa pipeline upgrade` now generates `orchestration.tf` when missing instead of silently skipping the re-sync. Pipelines that ended up without an `orchestration.tf` (older `pipeline create` flows, hand-authored directories) no longer deploy as data-only stacks with no Lambda or Step Function.
- `clavesa pipeline deploy` fails fast with a clear message when `orchestration.tf` is missing, pointing at `pipeline upgrade` or `pipeline orchestration sync` instead of letting terraform emit a cryptic error.
- Append + merge transform outputs now tolerate additive schema drift (default `mergeSchema=true` on append; `MERGE WITH SCHEMA EVOLUTION` on merge). The runner emits one stderr line per run when a new column actually landed.
- Cloud observability and dashboard endpoints return an empty result instead of 500 when the system Delta tables don't exist yet (fresh deploy before first run, or after a system-catalog Glue DB rebuild).

## [v2.2.2] — 2026-05-28

### Fixed

- `clavesa workspace plan`, `clavesa pipeline plan`, and `clavesa
  pipeline destroy` now run `terraform init -input=false` before
  shelling out, so a fresh checkout or a tree after `pipeline
  upgrade` (which rewrites module versions and invalidates
  `.terraform/`) no longer fails with `Initialization required`.
  Idempotent when `.terraform/` is current. Matches what
  `workspace deploy` / `pipeline deploy` have always done.
- Pipeline Lambda's `CLAVESA_MODULE_VERSION` env var now reports
  the actual module version (v2.2.2). v2.2.1 missed bumping the
  emitter's hardcoded literal, so newly-deployed pipelines stamped
  the stale `"v2.2.0"` string into Lambda env and into provenance
  observability output. Architectural follow-up to thread the value
  through `Pipeline` so this can't be missed again is filed in
  TODO.md.

### Changed

- Pipeline-runner Lambda's default `memory_size` is now 3008MB,
  down from 10240MB. New AWS accounts cap per-function memory at
  3008MB until the Service Quotas limit is raised, so the previous
  default made every new-account deploy fail at `terraform apply`
  with a quota error. Users with a raised quota can edit the
  emitted `orchestration.tf` to bump it back up for headroom on
  Spark broadcast tables. Existing pipelines pick up the new
  default on the next `pipeline upgrade` (which re-runs
  `SyncOrchestration`).

## [v2.2.1] — 2026-05-28

### Fixed

- Transform module's `var.inputs` is now `any`, so the documented
  cross-pipeline (`inputs = { x = "<schema>.<table>" }`) and
  registry-source (`inputs = { x = "sources.<name>" }`) authoring
  patterns finally pass `terraform validate`. Previously the typed
  `map(object({...}))` rejected bare strings even though the parser +
  orchestration emitter handled them correctly.
- Pipeline Lambda IAM now grants `s3:GetObject` on external (non-
  workspace) input buckets. v2.2.0 collapsed the per-transform Lambda
  IAM into one pipeline-Lambda role scoped to the workspace bucket
  only, so any pipeline reading from a cross-account S3 source
  (CloudFront logs, public datasets) started 403'ing on upgrade.
  Orchestration emitter now walks every transform's inline + registered
  s3 source buckets, dedupes, and emits an `S3ReadExternal` IAM
  statement listing them. Empty list → no statement.
- `clavesa pipeline backfill stage --node <downstream>` no longer
  errors with `"upstream node X has not produced output yet"` on
  multi-stage pipelines. The single-node backfill path now seeds
  `outputPath` / `outputFormat` for transitive intra-pipeline transform
  upstreams via the same `autoDeltaTableID(...)` the normal-run path
  uses, so `buildInputs` resolves them just like a full pipeline run.
- `resolveWorkspace` now prefers a cwd that IS a workspace over the
  stale global state file at `~/.config/clavesa/current-workspace`.
  Previously `clavesa workspace destroy` could target a different,
  last-used workspace when run from inside a workspace dir — and tear
  down the wrong one. Destructive commands (`workspace deploy`,
  `workspace destroy`, `pipeline deploy`, `pipeline destroy`,
  `pipeline run`) now also echo the resolved workspace name + path
  before acting.
- Dashboard editor: opening a dashboard whose datasets reference a
  control placeholder (e.g. `{{period.start}}`) no longer 400s the
  widget grid. `EditorGrid` now forwards the resolved control params
  to each `<Widget>`, matching the viewer.
- Dashboard editor: the widget drawer's body has padding again; the
  header still spans edge-to-edge so its bottom border looks intact.
- Dashboard editor: the widget drawer now shows a live results
  preview (up to 10 rows) under the SQL editor, sourced from the
  same query that populates the field pickers — no extra request,
  same React Query cache entry.

### Changed

- Transform module's `var.output_definitions.mode` validator now
  accepts `"merge"` (previously `replace, append` only) and the object
  schema includes `merge_keys = optional(list(string), [])`. Merge-mode
  outputs authored via `node edit --output-mode merge --output-merge-keys`
  now pass `terraform plan`; previously failed only on `compute = "cloud"`
  because local runs skip plan.

### Removed

- Transform module's `var.runner_image` (no caller after v2.2.0's
  per-transform-Lambda collapse). `clavesa pipeline upgrade` now
  strips `runner_image = ...` from existing v2.1.x pipeline `module
  "X"` blocks during the v2.1.x → v2.2.1 hop; new transforms authored
  via `node add` don't emit the attribute.
- `"local"` from the transform module's `var.compute` validation list.
  The Go side already rejects `compute = "local"` on input and strips
  it on upgrade; the module's validator now matches.
- Dead `input_buckets` local in `modules/transform/aws/main.tf` (its
  only consumer was the per-transform IAM policy dropped in v2.2.0).
- `internal/orchestration/aslgen` package — multi-state ASL builder
  with no callers after v2.2.0's single-Task SFN collapse.

## [v2.2.0] — 2026-05-28

### Changed

- Pipeline runs (local AND cloud Lambda) now share one container/Lambda
  and one Spark session across every transform in a pipeline. JVM cold
  start is paid once per pipeline run instead of once per transform —
  pipelines with many small transforms see large wallclock wins (a
  14-transform pipeline saves 50–70 s of pure JVM boot on first run
  after idle). Per-node observability (progress, status, output rows,
  error class, node_runs rows) is preserved.
- Cloud: the orchestration emitter now creates one
  `aws_lambda_function "pipeline_runner"` per pipeline (image-based,
  invokes `runner.pipeline_handler`) and a single-Task Step Functions
  state machine that hands the topo-ordered transform list to it as
  the Lambda payload. Per-transform Lambda functions are no longer
  created — the next `terraform apply` after upgrade destroys them and
  creates the pipeline-level Lambda. The 15-minute Lambda cap now
  applies to the whole pipeline.
- Local: `clavesa pipeline run` boots one runner container per
  invocation; transforms execute sequentially via the runner's
  shared `_SPARK` singleton. Per-transform progress streams to stdout
  as JSON-line `_event` markers the Go side dispatches to the run
  channel in real time, so the UI's state.json updates per node as
  before.

### Removed

- The Step Functions PipelineFailed state and per-state Retry/Catch.
  Terminal failures bubble through SFN itself; runs_writer's
  EventBridge rule still captures status changes.
- Multi-task ASL emission paths in `internal/orchestration/aslgen` /
  `tfgen` (`emitStates`, `emitTask`, `emitParallel`, `emitRetryCatch`).
  Side benefit: the multi-root DAG limitation filed after v2.1.2 is
  gone — the runner sees the topo order in the event payload and the
  ASL is always a single Task.

## [v2.1.2] — 2026-05-28

### Added

- Cross-pipeline auto-trigger: pipelines that read another pipeline's
  output via `<schema>.<table>` references now emit an EventBridge rule
  that auto-starts them when the producer pipeline's Step Functions
  execution succeeds. No knob — the cross-pipeline reference is the
  opt-in. Runs from this path stamp `runs.trigger = "upstream"` and
  carry the producer name in `_upstream_pipeline` on the SFN input.

## [v2.1.1] — 2026-05-28

### Fixed

- Pipeline editor now draws edges from cross-pipeline source nodes
  (`<schema>.<table>` references) to their consuming transforms.
- Clicking a cross-pipeline source node in the editor no longer
  crashes the inspector drawer.

## [v2.1.0] — 2026-05-28

### Added

- `clavesa --version` (and `-v`) print the binary version, alongside the
  existing `clavesa version` subcommand.
- SQL editor catches the common `FROM "db"."table"` double-quote mistake
  before sending to the runner, with a friendly hint that Spark reads
  double-quoted text as a string literal not an identifier.

### Changed

- `clavesa workspace init <name>` now scaffolds into `./<name>/` when
  `--workspace` is omitted, instead of writing in-place into the current
  directory. Re-initializing a workspace (existing `clavesa.json`) is
  refused with a clear error. Explicit `--workspace <path>` is unchanged.
- TableDetail's sample query emits bare identifiers (`db.table`) instead
  of double-quoted ones, so the default `SELECT * FROM ... LIMIT 100`
  succeeds on Spark.
- Single-output transforms write their output as `<node>` instead of
  `<node>__default`. Multi-output nodes still use `<node>__<key>`. Read
  paths transparently resolve both the new bare form and the legacy
  suffixed form, so existing tables and bookmarks keep working.
- Local pipelines write Delta tables under a per-catalog warehouse
  layout (`<warehouse>/<catalog>/<schema>/<table>/`), replacing the flat
  `<warehouse>/<catalog>__<schema>.db/<table>/` form. The Hive
  metastore database name stays at `<catalog>__<schema>` for now (Delta
  4.0 doesn't yet support DeltaCatalog as a non-session V2 catalog);
  the nested on-disk shape comes from a `LOCATION` clause on
  `CREATE DATABASE`. The catalog page and table reads transparently
  fall back to the legacy layout for not-yet-rewritten workspaces.
- Catalog API exposes `catalog`, `schema`, and `table` as separate
  fields on each entry. The legacy `database` field stays as a back-
  compat alias for one release.
- UI displays the three-level `<catalog>.<schema>.<table>` shape on
  every table-naming surface — Catalog page tree, TableDetail header
  chip, lineage panel rows, transform input picker, and the SQL
  editor's table browser. The `__default` suffix on single-output
  tables is hidden from display throughout. SQL editors and the
  TableDetail SQL pane still emit the wire form
  (`<catalog>__<schema>.<table>`) because that's what Spark and
  Athena accept (ADR-020).

### Fixed

- Catalog page's "Rows" column shows real counts for local Delta tables.
  Previously rendered `—` because the runner emits per-commit
  `numOutputRows` but no running total; the snapshots endpoint walks
  the commit log respecting each commit's semantics: overwrite (CTAS,
  CREATE OR REPLACE, WRITE Overwrite) resets the running total to that
  commit's `numOutputRows`; MERGE adds `numTargetRowsInserted -
  numTargetRowsDeleted` (updates don't change row count); append adds
  `numOutputRows`; delete subtracts. Merge-keyed dim tables in
  particular now report the correct stable row count across runs.

## [v2.0.0] — 2026-05-27

### Fixed

- Cloud-deployed Delta transforms now register their output tables in
  Glue Data Catalog. Pre-fix, the runner's cloud Spark session ran
  with an in-memory session catalog, so `saveAsTable` wrote Parquet
  + `_delta_log/` to S3 correctly but the table only lived inside
  the Lambda container and vanished on exit — UI Catalog, Athena,
  and cross-pipeline reads all saw nothing. The runner now bundles
  the AWS Glue Data Catalog client for Hive Metastore JARs (Spark 4
  build) and wires `AWSGlueDataCatalogHiveClientFactory` so Spark's
  session catalog federates to Glue. New `CREATE DATABASE` calls
  pin a `LOCATION` derived from `CLAVESA_WAREHOUSE`, fixing the
  follow-on `IllegalArgumentException: Can not create a Path from
  an empty string` from `saveAsTable` on a freshly-created Glue DB.
- Cloud runs against Spark 4 + hadoop-aws no longer fail with
  `NumberFormatException: For input string: "60s"` on the first
  `spark.read.parquet("s3://...")`. The bundled hadoop-aws is now
  3.4.1 (was 3.3.4) to match Spark 4.0.2's hadoop-client-3.4.1
  jars, which use duration strings (`"60s"`) for s3a timeouts.
- `clavesa workspace deploy` no longer rejects a freshly-retagged
  workspace runner image with "does not match the embedded runner
  SHA". The retag path now stamps the `clavesa.runner_sha` label
  via a tiny `docker build` (FROM + LABEL) so the deploy preflight
  recognises it as current.

### Changed (BREAKING — v2.0.0 cutover)

- **Delta Lake replaces Apache Iceberg as the default table format.**
  [ADR-018](docs/decisions/018-delta-table-format.md) supersedes ADR-013.
  Existing Iceberg workspaces (v1.x) cannot be auto-upgraded; recreate
  the workspace under v2.0.0 and re-run pipelines from sources. Athena
  loses INSERT/MERGE on clavesa-managed tables (Athena's Delta support
  is read-only). Gained: Change Data Feed (`readChangeFeed = true`)
  delivers the incremental-MERGE-downstream-of-MERGE-upstream pattern
  Iceberg v2 couldn't serve.
- **Runner upgraded to Spark 4.0.2 + Delta 4.0.0 + Java 17 + Scala 2.13.**
  Iceberg JARs removed; `delta-spark_2.13-4.0.0` + `delta-storage-4.0.0`
  added. Spark session config loads `io.delta.sql.DeltaSparkSessionExtension`
  and wraps `spark_catalog` with `DeltaCatalog`. Single-writer log store
  (`S3SingleDriverLogStore`) is the default for S3-backed warehouses.
  `pyspark[connect]==4.0.2`, pandas 3.0, numpy 2.4, pyarrow 24.0 ride in
  on the pyspark bump.
- **ANSI SQL is now on by default.** Lax casts that silently returned
  NULL in Spark 3 now throw `CAST_INVALID_INPUT`; division by zero
  throws `DIVIDE_BY_ZERO`; integer overflow throws `ARITHMETIC_OVERFLOW`.
  Use `try_cast(x AS <type>)` when you want the old silent-NULL
  behaviour and guard divisions with `NULLIF(divisor, 0)`. Cookbook
  examples already follow these patterns; pre-existing transforms that
  relied on Spark-3 lax casts need updating.
- **VARIANT type + Delta 4 identity columns + type widening GA.**
  New surface inherited from the Spark 4 / Delta 4 bump. VARIANT
  stores semi-structured columns without a declared schema (relevant
  for evolving event payloads); identity columns give monotonically-
  increasing surrogate keys without runner-side wiring; type widening
  lets columns broaden (`int` → `bigint`, `float` → `double`) without
  rewrites.
- **Runner write paths use Delta `DataFrameWriter`.**
  Transform output + system tables (`node_runs`, `runs`, `tables`,
  `column_stats`) and backfill-promote writes now go through
  `df.write.format("delta").mode("…").saveAsTable(…)` instead of
  Iceberg's `df.writeTo(…).createOrReplace()` / `.append()` / `.create()`.
  Append-first-run no longer branches on `tableExists` (Delta auto-
  creates); MERGE first-run still does.
- **Provenance via Delta `commitInfo.userMetadata`.** `clavesa.trigger`
  and `clavesa.run-id` are stamped on every Delta commit (DataFrame
  writes + `MERGE INTO`) by setting
  `spark.databricks.delta.commitInfo.userMetadata` once per
  transform / backfill-promote scope. Surfaces via
  `DESCRIBE HISTORY <table>` — replaces Iceberg's
  `snapshot-property.<key>` summary entries.
- **Incremental reads use Delta Change Data Feed.** The
  snapshot-id-bounded scan (`start-snapshot-id` / `end-snapshot-id`,
  blocked on MERGE upstreams by [apache/iceberg#1949](https://github.com/apache/iceberg/issues/1949))
  is replaced by `spark.read.format("delta").option("readChangeFeed", "true")`
  over `(last_version, current_version]`. CDF is enabled session-wide
  (`delta.properties.defaults.enableChangeDataFeed = true`) so every
  table the runner creates carries it by default — no per-table opt-in.
  When the descriptor carries `merge_keys`, the framework dedupes to
  the latest row per key by `_commit_version DESC`, replacing the
  obsolete v1.x `recency_column` design. The four Iceberg
  metadata-table reads (`.history`, `.snapshots`) now go through
  `DESCRIBE HISTORY`.
- **Docs sweep + v1.x migration recipe.** README, CLAUDE.md, TODO.md,
  and all cookbook recipes updated to reflect Delta as the default
  format. `merge-dim-table.md` gains a "Consuming this dim
  incrementally" section covering the CDF downstream pattern.
  New `docs/cookbook/migrate-to-v2.md` documents the recreate-from-source
  upgrade path for existing Iceberg workspaces.

### Added

- **Dashboard autosave + restore.** The editor now debounces every
  state change to `localStorage["dashboard-draft:<slug>"]`. A reload
  mid-edit surfaces a "Restore unsaved changes" banner; accepting
  re-seeds the editor with the autosaved spec, dismissing discards
  it. Server `updated_at` decides whether a draft is fresher than
  the saved copy (an out-of-date draft is silently discarded so the
  user isn't pestered).
- **Publish-time validation gate on Save.** A pure validator
  (`validateDraft`) runs before the POST and blocks publishes that
  would otherwise break: widget bound to a missing dataset, widget
  with required field-mapping unset for its type, dataset
  referencing `{{name}}` no control declares, field-mapping pointing
  at a column the dataset's last successful result didn't return.
  Failures surface as an inline banner listing each issue; clicking
  one opens the offending widget's drawer. Save pill on the header
  reflects state: saving · saved · blocked.
- **"Create dashboard from this table" on `/tables/:db/:table`.** New
  button next to the table-type badge that opens a fresh dashboard
  preloaded with a `SELECT * FROM <table> LIMIT 100` and one
  full-width table widget bound to it. Disabled when the owning
  pipeline isn't known (a dataset needs a `dir` to dispatch). Spec
  flows via React Router location state so the editor opens directly
  on it, no extra round trip.
- **Dashboard template gallery on `?new=1`.** Picker shows before the
  editor opens: Blank / Scoreboard (four big-numbers) / Line + bar /
  Top-N table. Selection materialises a starting spec and stamps
  `?template=<id>` on the URL so a mid-edit reload rebuilds the same
  shape rather than re-prompting.
- **`world_map` widget — country choropleth.** New dashboard widget that
  colors world countries by a numeric metric from a `(region, value)`
  result. Region codes accepted in ISO 3166-1 alpha-2 (`US`, `DE`) or
  alpha-3 (`USA`, `DEU`); auto-detected from the first non-empty row.
  Optional `tooltip_field` shows on hover. Topology ships in the binary
  via the `world-atlas` package's `countries-50m` TopoJSON — no Mapbox
  token, no tile server. Bundle cost: dashboard chunk grew ~280KB gz.
  Unknown region codes are skipped silently (`console.debug` for the
  author), missing regions render grey. v2 follow-ons deferred (US-state
  choropleth, lat/lng pin maps).
- **Unknown placeholder linter on dataset SQL.** The widget drawer's SQL
  editor now flags `{{name}}` references that no declared control
  provides, with a warning-level gutter mark and an inline hint listing
  the placeholders the dashboard actually accepts. Same regex as the
  server-side parser (`internal/dashboardsql/expand.go`) so what the
  linter accepts is what the runtime accepts. The drawer also surfaces
  an "Available:" chip strip under the SQL editor for click-to-insert.
- **Time-range picker on AWS Console + Grafana standards.** Dashboard
  `time_range` controls now offer 11 quick-pick presets matching AWS
  CloudWatch's range dropdown (5m / 15m / 30m / 1h / 3h / 12h / 24h
  / 3d / 7d / 30d / 90d). "Custom range" reveals a relative
  expression field that accepts Grafana's `now-<n><unit>` syntax
  (`now-1h`, `now-2w`, units `m|h|d|w`) plus the existing absolute
  start/end inputs. URL gains an optional `?<name>.rel=now-1h`
  that re-evaluates on every render — picking "Last 1 hour" no
  longer freezes after the first paint. Absolute pairs (`.start`
  + `.end`) still freeze the window for shareable point-in-time
  links. Legacy preset keys (`last_24h` / `last_7d` / `last_30d`
  / `last_90d`) keep working: dashboards saved before this change
  read back through the back-compat alias in
  `internal/service/timerange.go` and `ui/src/lib/timeRange.ts`.
- **Auto-refresh dropdown on the dashboard controls strip.** Off
  (default) / 30s / 1m / 5m / 15m, URL-synced as `?refresh=30s`.
  When set, the page re-fires every widget query on the interval —
  the knob the prior "per-dashboard refresh interval" TODO asked
  for.

### Changed

- **Dashboard editor is chart-first.** The three-tab editor (Datasets /
  Controls / Widgets) is gone; the new shell renders the live grid with
  a per-widget side drawer that holds everything for that widget — title,
  type, SQL, field mapping, layout — in one place. Selection is URL-synced
  (`?widget=<id>`) so deep links and the back button work. Field pickers
  populate automatically as you type SQL (idle-debounced, last good
  columns kept sticky through transient errors), with per-role type
  hints (numeric columns first for `value_field` / `y_field`, etc.).
  Datasets stay shareable — promote any widget's inline query to a
  named dataset from the drawer, then bind other widgets to it.

### Fixed

- **SQL linter now installs on the first paint.** `CodeEditor`'s lint
  compartment was reconfigured in a post-mount `useEffect` that ran
  before CodeMirror created its view, so the initial reconfigure
  dispatch was a no-op and the linter never attached. `onCreateEditor`
  now also reconfigures the lint compartment, so the linter is live
  immediately. Affects the dataset-SQL placeholder linter (above) and
  the transform-editor SQL parse-error linter.

## [v1.1.7] — 2026-05-25

### Added

- **`clavesa workspace upgrade`.** Refreshes the workspace's embedded
  modules tree and the local runner image to the running binary's
  `ModuleVersion`, and rewrites `module "workspace" { source = … }`
  in `main.tf` to the new version with the `./` prefix Terraform 1.x
  requires. Per-pipeline counterpart is `pipeline upgrade <dir>`;
  before v1.1.7 there was no discoverable workspace equivalent and
  the workaround was re-running `workspace init` (not advertised by
  its help text).

### Fixed

- **Local-mode pipelines now surface run history.** `GET /pipeline/status`
  and `GET /pipeline/execution` were cloud-only — local pipelines
  showed empty "Recent executions" forever. Local mode now reads from
  the on-disk run-state files.
- **Local-mode `/data/*` endpoints return 400 instead of 500 when `?dir`
  is missing.** Previously fell back to the cloud provider, whose nil
  Athena client crashed the request. Cloud mode unchanged.
- **Pre-ADR-016 Glue DBs (`clavesa_<pipeline>`) now appear in the
  catalog.** Un-migrated workspaces showed "AWS available, 0 tables"
  with no signal; the catalog filter now accepts the legacy
  single-underscore form and logs a hint to run
  `clavesa pipeline upgrade`.
- **`pipeline list` no longer false-positives on dirs whose .tf files
  literally contain the substring "clavesa"** (e.g.
  `bucket = "clavesa-prod"` in a non-pipeline sibling). The marker
  now matches the embedded-modules path (`.clavesa/modules/`) or the
  legacy GitHub `?ref=` form.
- **Hyphenated pipeline names (`marketing-funnel`) now render runs in
  PipelinesList.** UI was sanitising `-`→`_` before joining against
  `runs.pipeline`, but the runs writer stores the literal name.

### Changed

- **HTTP handlers route through one canonical service.Service instead of
  building a fresh one per request.** Two endpoints (`node rename`,
  `node connect`) plus the workspace-level pipeline-CRUD handlers
  previously bypassed cache eviction; the wired service is now used
  uniformly.

### Fixed

- **Orchestration: ASL Catch.Next no longer references PipelineFailed
  from inside a Parallel branch.** v1.1.5's tfgen emitted
  `Catch = [{ Next = "PipelineFailed" }]` on every Task — including
  those nested inside Parallel branches. AWS rejected the state
  machine with `MISSING_TRANSITION_TARGET` because a branch's States
  map is its own scope; PipelineFailed only exists at the top level.
  Inner Tasks and inner Parallels now omit the Catch — errors
  propagate up to the enclosing Parallel state, whose own Catch (in
  top-level scope) sends control to PipelineFailed. Same semantic
  behaviour, valid ASL.
- **`workspace init` writes `source = "./.clavesa/modules/…"` with the
  required `./` prefix.** Terraform 1.x rejects the bare form as
  "ambiguous registry / local". Existing workspaces parse the bare
  form via hclparser's embedded-form heuristic and get rewritten on
  next `clavesa workspace upgrade`.
- **`pipeline upgrade` strips deprecated `incremental_inputs = [...]`
  from `main.tf`.** The transform module dropped the variable in
  v0.19.0 but `pipeline upgrade` left old declarations in place;
  affected pipelines failed `terraform validate` with "Unsupported
  argument". The upgrade migration now removes the line alongside
  the existing `compute = "local"` cleanup.
- **`tests/runner/test_runs_writer.py` references the new sidecar
  path.** v1.1.5 moved `runs_writer/index.py` to
  `internal/orchestration/sidecar/runs_writer/` but the Python test
  file kept the deleted path, leaving the v1.1.5 release commit
  test-red.

## [v1.1.5] — 2026-05-25

### Added

- **Notebooks.** Multi-cell SQL + PySpark notebooks (Jupyter `.ipynb`)
  at `<workspace>/notebooks/<name>.ipynb`. Cells share one SparkSession
  via per-notebook Spark Connect session_id; Python globals persist
  across cells. `%%sql` / `%%python` magic for per-cell language.
  Local-only in v1. CLI parity: `clavesa notebook
  create|list|show|run|delete|clear-outputs`.
- **Query.** Ad-hoc SparkSQL surface — top-level `/query` page and
  a collapsible "Query this table" pane on `/tables/:db/:table`
  pre-filled with `SELECT * FROM <table> LIMIT 100`. CLI peer:
  `clavesa query "<SQL>"` (or stdin) with `--json`.
- **Graduate cell → transform.** Per-cell `Graduate` button (and
  `clavesa notebook graduate <nb> --cell <id> --to <pipeline> --as
  <name>`) writes the cell source to
  `<pipeline>/transforms/<name>.{sql,py}` and registers a new
  transform node. Closes the explore → productionize loop.
- **Catalog browser sidebar on `/query` and in the notebook editor.**
  Click any workspace table to insert its fully-qualified name at the
  cursor; expand a table to insert column names.
- **Local commands auto-refresh the workspace runner image after a CLI
  upgrade.** CLI now checks the workspace image's `clavesa.runner_sha`
  label against the SHA of the runner files embedded in the binary at
  every local docker entry point; on mismatch it auto-retags from a
  candidate image (cheap) or rebuilds from embedded files (one-time
  per CLI upgrade that touches runner code). Lifts the
  `workspace.EnsureLocalRunnerImage` helper out of `workspace init`
  for reuse by `pipeline run`, `pipeline backfill`, and preview.

### Changed

- **Orchestration is now plain Terraform, not a clavesa module.**
  Generated `orchestration.tf` contains the Step Functions state
  machine, IAM roles, log group, Glue DB, runs_writer Lambda, and
  optional schedule / poller wiring as direct resources. Detaching from
  clavesa now means deleting one header comment and editing standard
  Terraform — no module dependency to vendor. Existing pipelines pinned
  at older versions keep working until re-deployed.
- `/tables/:db/:table` collapses the old Schema-on-left / Sample-on-
  right grid into a single column-oriented "Columns" card (per-column
  name + type + null % + example values, or the rich profile when
  `stats=true`). "Query this table" pane is open and auto-running
  `SELECT * … LIMIT 100` on page load; the separate Sample card is gone.
- Dashboards `CatalogBrowser` lifted to a shared
  `components/CatalogBrowser` with a new `scope="workspace"` mode;
  existing `scope="pipeline"` behaviour unchanged.
- Warm Spark worker now runs Spark Connect (Spark 3.5). Internal
  prep for Spark 4 and per-session isolation; no user-visible change.
- `pipeline run` no longer tears down the warm Spark container —
  Catalog and dashboards stay responsive throughout a transform run.

### Fixed

- **Orchestration: nested fanouts + multi-hop branch states no longer
  unreachable.** v1.1.4 emitted top-level orphan states for any branch
  whose work continued past one Lambda invocation — AWS rejected the
  state machine with `MISSING_TRANSITION_TARGET`. The ASL builder moved
  from HCL (which can't recurse) into Go.
- **`node_runs.module_version` now tracks the CLI version, not the
  image's baked-in build-arg.** Cache retags share an image digest
  across multiple version tags, so the runner's `CLAVESA_MODULE_VERSION`
  ENV (set at image-build time from the Dockerfile ARG) could lag the
  CLI version and the run-detail triage strip would show the stale
  value. Every local `docker run` now passes
  `-e CLAVESA_MODULE_VERSION=<current>`.

## [v1.1.4] — 2026-05-24

### Fixed

- **`backfill promote` evolves the target schema instead of silently
  dropping new columns.** Adding a column to a transform between the
  canonical run and the backfill used to silently drop the new column
  on MERGE (`UPDATE SET *` / `INSERT *` resolved against the target
  schema, then ignored unresolved staging columns), or error with
  `INSERT_COLUMN_ARITY_MISMATCH` on append+allow_duplicates. Promote
  now `ALTER TABLE … ADD COLUMN`s any staging-only columns before the
  merge (Iceberg schema evolution); existing canonical rows read back
  NULL for the added columns. CLI prints the evolved columns; UI
  surfaces them in the promote-success toast; the promote API now
  returns `{"columns_added": [...]}` instead of 204.
- **Backfills card now shows for local pipelines.** The pipeline
  dashboard previously hid the card and stage dialog whenever the
  workspace env was local, even though `backfill_local.go` was fully
  wired. Local pipelines now stage/diff/promote/discard through the
  same UI as cloud (ADR-014).

## [v1.1.3] — 2026-05-23

### Fixed

- **GitHub Release pages actually carry the CHANGELOG section now.**
  v1.1.2 tried to wire this via `goreleaser release --release-notes
  <file>`, but the body came out empty above the footer — most likely
  a silent conflict with `changelog.disable: true`. The release
  workflow now exports the extracted CHANGELOG section as a multiline
  `$GITHUB_ENV` variable, and `.goreleaser.yaml`'s `release.header`
  templates it in via `{{ .Env.CLAVESA_RELEASE_NOTES }}`. Bulletproof,
  doesn't depend on flag-vs-config precedence in GoReleaser.

## [v1.1.2] — 2026-05-23

### Fixed

- **`release` GitHub Action no longer trips GoReleaser's dirty-tree check.**
  The workflow's `Build UI` step ran `vite build` directly — and Vite's
  `emptyOutDir: true` wipes the tracked `internal/ui/dist/.gitkeep`
  placeholder. GoReleaser then refused with `git is in a dirty state`
  and no binaries shipped. The workflow now re-touches the placeholder
  after the Vite build, matching what `make build-ui` does locally. The
  v1.1.1 release tag exists but had no binaries attached because of
  this; v1.1.2 is the first tag to actually publish on the new
  `release` workflow path.
- **GitHub Release pages now show the per-version CHANGELOG section.**
  GoReleaser was configured with `changelog.disable: true` and only a
  static footer, so every release page said the same thing — two
  boilerplate lines. The workflow now extracts the `## [vX.Y.Z]`
  section out of `CHANGELOG.md` and passes it to GoReleaser as
  `--release-notes`, so what users see on the Release page matches
  what shipped.

## [v1.1.1] — 2026-05-23

### Fixed

- **Release snapshot no longer strips `internal/ui/dist/.gitkeep`.** The
  rsync exclude that drops the root-level GoReleaser `dist/` directory
  was unanchored, so it also stripped the nested
  `internal/ui/dist/.gitkeep` placeholder — leaving `go test ./...` to
  blow up on `//go:embed all:dist` until a UI build regenerated the
  directory. v1.0.3 and v1.1.0 both shipped with a red `test` CI
  workflow because of this; the actual binary releases were unaffected
  (their workflow runs a UI build before the embed).

## [v1.1.0] — 2026-05-23

### Added

- **`pipeline backfill` works on `compute = "local"` pipelines.** `stage`,
  `list`, `diff`, `promote`, `discard`, and `dedup-check` all now route
  through the runner image directly against the workspace warehouse when
  the workspace env mode is local, mirroring the cloud (Lambda + Athena +
  Glue) path. Sidecar JSON next to each staging Iceberg dir replaces the
  Glue table tagging the cloud uses, so `list` finds the same metadata
  without a catalog roundtrip.
- **Dashboard controls.** Dashboards can declare top-of-page filter
  controls — a time-range picker (presets `last_24h` / `last_7d` /
  `last_30d` / `last_90d` / custom) and a select dropdown populated by
  a SQL query or static options. Values bind to dataset SQL via
  `{{name}}` placeholders (`{{name.start}}` / `{{name.end}}` for time
  ranges) and round-trip through URL search params so a filtered view
  is shareable. CLI: `clavesa dashboards render <slug> --param key=value`.
- **Pie and donut widgets.** Two new widget types for share-of-total
  categoricals; long-tail categories beyond the top 7 collapse into
  one "Other" slice so the chart stays legible.

### Changed

- **Eager Spark warmup on `clavesa ui`.** The warm Spark worker now starts
  in the background the moment the UI server boots, so the header
  indicator flips to "Starting Spark…" on the first poll and is
  "Spark ready" by the time you click into a table — instead of staying
  stuck on "Spark idle" until a Spark-backed page triggers the lazy
  cold-boot. Skipped in uninitialized workspaces.
- **Airflow-style runtime bars on the pipeline Runs grid.** Each run's
  column header now carries a vertical bar whose height encodes its
  duration and whose color encodes its status, replacing the small
  duration text. Slow runs jump out at a glance.
- **Sticky Node / output-table column reads as a distinct band.** The
  edge-fade gradients on the Runs grid are gone; the sticky left column
  now carries a different background to the scrollable run matrix, so
  it's obvious which side is fixed and which scrolls.

### Fixed

- **Warm Spark worker detects "container up, JVM dead".** `/healthz` now
  runs a probe SQL against the Spark gateway, and the UI server evicts +
  respawns the worker on the next query when py4j surfaces a
  gateway-shutdown error — instead of every dashboard query hanging on a
  dead JVM until you manually restart `clavesa ui`.

## [v1.0.3] — 2026-05-23

### Fixed

- **`brew install --cask` no longer leaves the binary Gatekeeper-quarantined.**
  v1.0.2's cask was missing a post-install hook to strip
  `com.apple.quarantine`, so running `clavesa` after a fresh
  `brew install --cask` got SIGKILL'd. The cask now strips the attr
  via a `postflight` xattr call.

## [v1.0.2] — 2026-05-23

### Changed

- **Homebrew install is now a cask, not a formula.** New invocation:
  `brew install --cask vesahyp/clavesa/clavesa`. Existing formula
  installs are migrated to the cask transparently on `brew upgrade`
  via the tap's `tap_migrations.json`. Driven by GoReleaser's
  deprecation of `brews:` in favor of `homebrew_casks:` (removal
  scheduled for GoReleaser v2.16).

## [v1.0.1] — 2026-05-22

### Added

- **Prebuilt binaries on every release.** GitHub Actions cross-compiles
  `darwin/amd64`, `darwin/arm64`, `linux/amd64`, `linux/arm64` on tag
  push and attaches tar.gz archives + checksums to the GitHub Release.
- **Homebrew tap** at `vesahyp/homebrew-clavesa`. Install with
  `brew install vesahyp/clavesa/clavesa`; `brew upgrade` picks up new
  versions automatically.
- **`clavesa version`** subcommand prints the module version. Used by the
  Homebrew formula's smoke test and as a quick discoverability path for
  anyone wondering which build they're on.
- **Pipeline dashboard "Runs" tab** (renamed from "Nodes"). Airflow-style
  status squares (ok / failed / running / skipped / missing) with hover
  tooltips; "← N older / N newer →" pills when older / newer runs are off
  screen.
- **Run-detail Sheet** — clicking a run column header opens a right-side
  panel with header, DAG, and per-node breakdown. `?run=<id>` URL state;
  `/pipelines/run?…` redirects in for deep-link parity.
- **Cascade skip:** when every upstream of a transform skipped this run,
  the transform skips too — no runner invocation, no Spark cold start.
  Backfilled node_runs rows record the skip.
- **Persistent Spark indicator** in the top header — idle / starting /
  ready, with tooltip on what needs Spark.
- **Live in-flight column** in the grid, sourced from state.json, with
  per-node cells painting live as the run progresses.

### Changed

- **Dashboard reads from the filesystem,** not Spark. Node-runs project
  from `state.json` files, tables-state from each Iceberg
  `metadata.json`. ~40× faster on node-runs, ~20× on tables-state. The
  drill-down Sheet still hits Spark for the richer columns
  (`runner_image_digest`, `cold_start`, etc.).
- **"Rows written" reports net change** (`added - deleted`) instead of
  raw `added-records`. A pure-update merge that rewrites rows in place
  now honestly reports 0.
- **Cloud-only panels** ("Recent executions", "Backfills") hide based on
  workspace env mode, not the per-pipeline `compute` attribute — a
  pipeline that doesn't declare `compute = "local"` no longer nags about
  `terraform apply` while running locally.
- **Run pipeline** stays on the dashboard. The new column appears in the
  grid live; opening the Sheet is explicit.
- **Brand:** sidebar / tab title now read **Clavesλ**, with a database
  SVG favicon.

### Fixed

- **Orphan RUNNING runs** (state.json from a killed orchestrator) are
  auto-downgraded to FAILED on read when the file hasn't been touched in
  60s — the dashboard stops painting them as in-flight indefinitely.
- **`UNKNOWN_MODULE_SOURCE` validator gap** that flagged every node in a
  v0.30.0+ pipeline (the embedded `../.clavesa/modules/vX.Y.Z/...` source
  shape was missing from the allowlist).
- **Graph-tab node overlap** when nodes carry column profiles — dagre
  now gets the rendered card height per node so adjacent rows no longer
  collide.

## [v1.0.0] — 2026-05-21

### Changed

- **Renamed astrophage → clavesa.** Go module path, binary name (`bin/clavesa`),
  CLI command, `CLAVESA_*` env vars, `clavesa_<pipeline>` Iceberg namespace
  prefix, `<workspace>/.clavesa/` metadata dir, `clavesa:*` AWS resource tags,
  local Docker tag (`clavesa/transform-runner`). No upgrade path from v0.x;
  existing v0.x deployments continue to resolve their old `?ref=` via the
  renamed GitHub repo. New deploys land on v1.0.0 fresh.

### Added

- **MIT LICENSE** at the repo root. Copyright 2026 Vesa Hyppönen.

## [v0.30.0] — 2026-05-21

### Changed

- **Terraform modules now ship embedded in the binary.** `workspace init`
  extracts them into `<workspace>/.clavesa/modules/v0.30.0/`; generated
  pipeline and workspace `.tf` files reference modules via relative
  local paths (`../.clavesa/modules/v0.30.0/<type>/aws`). `terraform
  init` no longer fetches from GitHub. `pipeline upgrade` rewrites legacy
  `github.com/vesahyp/clavesa//modules/...?ref=` source URLs into the
  embedded form at the current `ModuleVersion`.
- **Node editor splits into Code and Settings tabs and gains an
  expand-to-fullscreen toggle.** Clicking a transform in the pipeline
  dashboard still opens the right drawer, but the contents are now
  split — *Code* (inputs + SQL/Python editor + output sample) and
  *Settings* (output mode, merge keys, stats, compute target,
  incremental upstream toggles). The expand button (top-right of the
  drawer) swaps the drawer for an overlay that covers the page area
  (left nav stays visible) with a single-column Code layout — inputs,
  editor, then output sample. `Esc` collapses the overlay before
  closing the panel; the SQL/Python buffer survives tab switches.
- **Editor folded into the pipeline dashboard.** `/pipelines/dashboard?dir=…`
  is now the single pipeline page — authoring (DAG, ConfigPanel, node
  palette, preview, pipeline settings) lives in the Graph tab alongside
  the Nodes grid, history, and backfills. The `/editor` route redirects.
- **Click a source or external-table node in the DAG** to open a
  read-only inspector showing the source kind, location, and inferred
  columns (or the catalog row + columns for an external table).
- **Click any node on the run page DAG** to open a drawer with its
  inputs, output, run status, and step logs — no more bouncing back to
  the editor to triage a failed step.
- **Remove a transform input with the X button on each input row** —
  source attachments, external-table references, and upstream edges
  all share one affordance.
- **Edge-delete keybinding works again.** Clicking an edge then pressing
  Backspace or Delete removes it. The previous attempt was swallowed by
  upstream keydown handlers.
- **Pipeline-specific actions moved from the global header to the local
  pipeline header.** Run, Settings, and the validation badge now live in
  PipelineHealthHeader's right side; the app's top bar holds workspace-
  level affordances only (AWS profile, env toggle).
- **DAG nodes show columns and write mode without a click.** Catalog-
  resolved column lists populate every node whose Iceberg table exists,
  and the footer shows the output write mode (`replace`/`append`/`merge`)
  next to the deploy target.
- **Editor drawer extends to the full app height.** ConfigPanel and the
  source / external-table inspector now overlay at the app-shell level
  rather than inside the Graph tab card, so they're not cropped.
- **Auto-preview when an output table exists.** Opening a transform whose
  Iceberg table has rows now samples 10 rows inline and lists the output
  columns in the right-side inputs browser — no Preview click needed.
  The Preview button still re-executes when you want to validate unsaved
  SQL edits.

### Added

- **`clavesa source detach`** — remove an attached input from a
  transform regardless of kind (registry source, external table, or
  upstream edge). The HTTP twin is `POST /api/pipeline/inputs/detach`.

### Fixed

- **Pipeline dashboard's per-node run cells populate again for pipelines
  whose name contains a hyphen** (e.g. `cloudfront-model`). The
  `/api/data/{node-runs,runs,tables-state}` validator rejected hyphens
  on the `pipeline=` query param, and the UI silently underscored the
  name before querying — a contract that broke once the runner started
  storing the literal `pipeline_name` (dashes preserved) in
  `node_runs.pipeline` / `runs.pipeline`. Validator now accepts the
  shape `pipeline create` accepts; UI passes the dir-derived name as-is.

- **Catalog page now labels `column_stats` and `dashboards` as clavesa
  system tables.** Both live in the workspace system catalog alongside
  `node_runs` / `runs` / `tables` but the allowlist was incomplete, so
  they rendered as user tables.

### Changed

- **README quick-start now lands raw NYC TLC trip data as its own
  `trips` transform with `Compute column stats` turned on**, then
  aggregates into `revenue_by_payment`. The Column profile card on
  `/tables/.../trips__default` is part of the documented walkthrough.

- **Preview reuses each upstream transform's existing Iceberg snapshot
  when the pipeline hasn't been edited since the last run.** Previously
  every Preview re-ran every upstream (and re-fetched every source);
  now an upstream's already-materialized output is sampled directly,
  skipping the SQL/PySpark re-execution and the source fetch behind
  it. Any edit to a `.tf`/`.sql`/`.py` file in the pipeline dir
  invalidates the snapshot and falls back to a full re-execute, so
  unsaved upstream edits are never silently ignored.

## [v0.29.0] - 2026-05-20

### Changed

- **The SQL and Python editors are now CodeMirror 6 (was Monaco).** Same
  keywords, schema-aware completion, advisory SQL parse warning, and
  catalog-insert in the dashboard editor. The swap fixes the space-key
  bug where typing space inside the suggest widget sometimes inserted
  a dot.

### Fixed

- **Pipeline dashboard's sticky health header no longer gets painted over
  by the nodes grid as you scroll.** Both were on `z-10`; bumped the
  header to `z-30` so page chrome outranks embedded scroll content.

- **`compute=local` pipelines with a partitioned S3 source now get AWS
  credentials forwarded into the runner container.** `hasS3Input` only
  matched `kind=s3`, not the `kind=partitioned_path` shape registered
  s3 sources resolve to at run time, so the bronze transform failed
  with "Unable to locate credentials".

### Added

- **Opt-in column stats on the Catalog table page.** Toggle "Compute
  column stats" on a transform (editor checkbox or `node edit
  --output-stats`) to render a Column profile card with null %, approx
  distinct, top-10 values, min/max, and p50/p95 per column. Default off.

- **The SQL transform editor has autocomplete and a syntax check.**
  Completion covers SparkSQL keywords and the transform's input aliases
  and columns; `<alias>.` suggests that input's columns. A best-effort
  parse flags syntax errors as advisory warnings (Preview stays the
  authoritative check). The space key no longer accepts a suggestion
  mid-type.

- **Nodes can be renamed.** Click a node's name in the editor config
  panel to rename it, or run `clavesa node rename <pipeline> <old>
  <new>`. The rename moves the module block, every downstream edge that
  reads it, and the transform's SQL/PySpark script files. Note: a node's
  id is also its Iceberg output table name, so a rename renames that
  table too.

- **Compute target is editable in the pipeline editor.** A transform's
  config panel has a **Compute** select — `lambda` / `fargate` /
  `emr-serverless` — matching what `node edit --set compute=` already did
  on the CLI.

- **In-UI dashboard builder.** Create and edit dashboards from
  `/dashboards` — name datasets, bind widgets, save. New `clavesa
  dashboards` CLI: `list` / `show` / `render` / `apply` / `delete`.
- **Stacked bar and bar+line chart widgets.** Stacked bar picks an x
  column and several value columns, stacking one segment per value
  column; bar+line draws a bar metric and a line metric on a shared x
  with dual axes.
- **Filter box on the Dashboards and Credentials lists**, matching the
  Catalog, Pipelines, and Sources pages.
- **Table detail shows the fully-qualified table name** —
  `clavesa.<catalog>__<schema>.<table>` — with a copy button, so it
  drops straight into a dashboard dataset or ad-hoc SQL.
- **Pipeline commands infer the directory from the current directory.**
  Every command that takes a `<pipeline-dir>` argument now accepts it as
  optional — omit it and the command uses the current directory once you
  have cd'd into the pipeline. Run outside any pipeline with no argument
  and the command reports a clear error instead of a bare usage line.

### Changed

- **Pipeline editor wiring is a guided menu, not drag-to-connect.** Each
  node's output handle has a **+** button: create a downstream node
  already wired in, or connect to an existing one. It works on registered
  source nodes too — the menu attaches the source into a new or existing
  transform. Connecting two nodes no longer drops earlier inputs. Select
  an edge and press Backspace to remove it.

- **Dashboard editing is direct-manipulation.** Drag widgets to move
  them and drag a corner to resize, instead of typing grid coordinates.
  Chart fields are picked from dropdowns of the dataset's real columns,
  dataset SQL has a **Run** button that previews the result inline, and
  **Add widget** opens a type picker that seeds sensible defaults. The
  editor is split into **Datasets** and **Widgets** tabs, and each
  dataset's SQL editor has a table browser listing the pipeline's tables
  and columns — click one to drop its name into the query. Saving keeps
  the editor open instead of kicking back to the rendered view.
- **Left-nav order** groups the surfaces you work in — Catalog,
  Pipelines, Dashboards — ahead of the Sources and Credentials
  registries, instead of interleaving them.
- **Dashboards are a shared system table.** Dashboards moved from
  per-workspace JSON files into the `dashboards` system Iceberg table, so
  everyone with workspace access sees the same ones. They follow a
  datasets model: a named SQL query feeds one or more widgets, and each
  dataset names its own pipeline — one dashboard can blend several.
  Existing dashboard files migrate automatically on first load.

- **`pipeline backfill diff/promote/discard` take the pipeline directory
  as a positional argument**, e.g. `backfill diff <pipeline-dir> <run_id>`.
  The `--dir` flag is removed, matching every other pipeline command.
- **Commands prefer the workspace you are standing in.** When the current
  directory is inside a workspace, that workspace wins over one pinned by
  `workspace use`. The pinned workspace still applies from anywhere else.

### Fixed

- **The run-detail DAG shows registered sources.** The per-run graph at
  `/pipelines/run` omitted `source:` nodes; it now renders them like the
  editor and pipeline dashboard do.

- **Dashboard bar and line charts render correctly.** They were drawn
  with `hsl(var(--primary))`, which a browser does not resolve in an SVG
  attribute — bars came out black and lines were invisible; the color is
  now a resolved literal and line widgets show point dots. Y-axis ticks
  compact large numbers (`60M` instead of a clipped `00000`), and the
  hover tooltip is dark-themed, names both the X and Y columns, and
  shows values with thousands separators (`65,533,599.31`). Big-number
  widgets likewise show a compact value (`79.46M`) with the exact figure
  on hover.
- **Drawing an edge from a source node in the editor no longer corrupts
  the pipeline.** Source nodes are read-only; an edge from one used to be
  written as invalid HCL, leaving the pipeline's `.tf` unparseable. The
  editor now refuses the connection and `add-edge` rejects an unknown
  from-node.

## [v0.28.0] — 2026-05-19

### Added

- **Snapshot provenance on the table timeline.** Every entry on a
  table's Volume timeline now carries a badge for what produced it —
  `backfill`, `triggered`, `scheduled`, or `manual` — alongside the
  runner invocation id. Snapshots written outside clavesa show as
  `external`. Works the same for local and deployed pipelines.

### Fixed

- **Backfill review screen no longer errors on leaving.** Promoting or
  discarding a backfill could fire failed requests against the
  just-dropped staging table; the page now exits cleanly.

## [v0.27.0] — 2026-05-18

### Added

- **AWS profile switcher in the app header.** The AWS identity chip is
  now a dropdown: it shows `profile · account-id` and lets you switch to
  any profile in `~/.aws`. The choice is persisted per-workspace
  (`.clavesa/aws-profile.json`, gitignored) and the server restarts
  itself to apply it. CLI twin: `clavesa workspace use --profile`.

- **Compute deploy-target badge on transform nodes.** Each transform
  node in the pipeline graph shows its `compute` target (`lambda` /
  `fargate` / `emr-serverless`) as a small chip in the node footer.

- **AWS identity chip in the app header.** Shows which AWS account /
  profile the UI server is operating as — the quick answer to "why did
  this preview/run 403?". Hidden in local-only mode. Backed by
  `GET /api/runtime/identity`.

- **Warm-Spark status in the app header.** The first Catalog or table
  query of a session waited ~30s while the local Spark worker booted,
  with no hint. The header now shows a "Starting Spark…" indicator
  while it spawns and a brief "Spark ready" when it finishes; it stays
  hidden otherwise. Backed by `GET /api/runtime/workers`.

- **Workspace environment mode (local / cloud).** A per-workspace mode
  drives local-vs-cloud dispatch for pipeline runs and the observability
  surfaces; it defaults to `local`. Set it from the CLI —
  `clavesa workspace use --env local|cloud`, or `pipeline run --env`
  to override a single run — or from the UI: a Local/Cloud toggle in the
  app header switches the whole workspace and refetches every page.

- **Schema-ownership validation (ADR-016).** `pipeline create`, the
  orchestration emitter, and `pipeline deploy` now refuse a configuration
  where a pipeline's schema is already owned by another pipeline in the
  workspace, with an error naming the conflicting pipeline. Each schema has
  exactly one producing pipeline.

- **Schema-scoped Catalog view.** The Catalog page accepts `?catalog=`
  and `?schema=` query params and filters to that catalog or schema,
  with an active-filter banner and a clear affordance. Catalog and
  schema names — in the Catalog headers and the TableDetail breadcrumb
  — are now links that set them. `clavesa workspace tables` gains
  matching `--catalog` / `--schema` filter flags.
- **Preview a registered source.** New `clavesa source preview
  <name>` CLI command and a per-row Preview button on `/sources` sample
  a source's data — http and s3 — without attaching it to a pipeline.
- **Cookbook recipe: changing HTTP source.** New
  `docs/cookbook/http-changing-source.md` — re-fetch a moving HTTP API
  (Hacker News) into a merge-keyed dimension plus an append snapshot
  fact that captures the change history the API never exposes.
  Backed by `GET /api/sources/{name}/preview`. Credential-backed sources
  aren't previewable yet (works in `pipeline run`).
- **Edit a registered source.** New `clavesa source edit <name>` CLI
  command and a per-row edit form on `/sources` update a source's URL,
  format, credentials, partitions, and start-from in place — previously
  the only way to change a source was delete + re-register. The name is
  fixed (pipelines reference it); renaming is still delete + register.
  Backed by `PUT /api/sources/{name}`.
- **Pipeline delete from the UI.** Each pipeline card on `/pipelines`
  carries a trash affordance that fires a confirm dialog and the new
  `DELETE /api/pipelines?dir=` endpoint, mirroring `clavesa pipeline
  delete --force`. The confirm dialog is the UI equivalent of the
  mandatory `--force` flag; cloud teardown still requires `clavesa
  pipeline destroy` first.
- **Pipeline module-version chip on the per-pipeline dashboard.** Shows
  `Module: vX.Y.Z` with `→ vY.Z.A` and an Upgrade button when the
  pipeline's `?ref=` lags behind the latest tag on
  github.com/vesahyp/clavesa. Backed by new `GET
  /api/pipeline/module-version` and `POST /api/pipeline/upgrade`
  endpoints that delegate to `service.UpgradePipeline` — same code path
  the CLI's `pipeline upgrade` uses, so both surfaces rewrite .tf and
  re-sync orchestration identically.
- **Keyboard input wiring in the editor.** The transform Inputs panel
  gains a "Pipeline node" tab alongside Source and Workspace table:
  pick an upstream transform in the same pipeline and type the SQL
  alias, the typed mirror of dragging an edge in the DAG (and of the
  CLI's `node connect --from … --input …`). Intra-pipeline node inputs
  now also appear in the panel's input list, not just the DAG.
- **Source-registration inference hint.** The `/sources` register form
  shows the kind / bucket / prefix / format it will infer from the
  pasted URL, live as you type, before submitting.
- **Workspace creation from the UI.** `clavesa ui` started in a
  directory with no `clavesa.json` now shows a "Create a workspace"
  screen instead of a broken app. The create action POSTs to the new
  `POST /api/workspace/init` — same code path as
  `clavesa workspace init` (manifest, workspace Terraform, runner
  source, local preview image). No server restart needed: the root
  directory is unchanged, it just gains a manifest.
- **`clavesa pipeline lineage <dir>` CLI command.** Prints the
  data-lineage graph (source/transform/destination edges, the catalog
  table each edge flows through, cross-pipeline reads) the UI's
  TableDetail panel already showed. `--json` for scripting. Closes a
  CLI/UI parity gap found in the `--json` audit.
- **`clavesa workspace tables` CLI command.** Lists every Iceberg
  table the workspace catalog owns — the CLI counterpart of the
  Catalog page. `--json` for scripting. Both surfaces call the same
  `CatalogHandler.Tables` core, so the list is identical. Local-pipeline
  tables show without AWS; cloud (Glue) tables need credentials.
- **Type-to-filter search on the list pages.** Catalog, Pipelines, and
  Sources each gain a search box that filters the list client-side as
  you type — Esc or the inline clear button resets it. Catalog matches
  table name, node, output key, catalog, and schema; the count line
  shows `N of M` while filtering. Matching substrings are highlighted in
  the rows that survive the filter.

### Changed

- **CLI commands honor the workspace's AWS profile.** Every CLI command
  now applies `.clavesa/aws-profile.json` (the profile the UI
  switcher / `workspace use --profile` sets) — previously only
  `clavesa ui` did, so a terminal `pipeline run` against a
  cross-account source still needed `AWS_PROFILE` exported by hand.
  `pipeline run` and `pipeline deploy` / `workspace deploy` also print a
  one-line target summary (environment mode + AWS profile) to stderr.

- **The local/cloud toggle is now authoritative.** The workspace
  environment mode is the sole switch for local-vs-cloud dispatch — the
  per-node `compute` attr no longer overrides it. Previously any
  pipeline still carrying `compute = "local"` stayed pinned to the
  local warehouse regardless of the toggle, so switching to Cloud
  appeared to do nothing.

- **"Run pipeline" gives immediate feedback.** Clicking Run now jumps
  straight to the run page, which shows the run live (DAG colored per
  node) instead of the UI freezing until the whole run finished. Local
  runs dispatch in the background; a second run of the same pipeline
  while one is in flight is rejected with a clear message.

- **Pipeline ≡ schema is visible in the UI.** Each Catalog schema
  section now names its producing pipeline and links to that pipeline's
  dashboard; Pipelines list cards show the schema each pipeline writes
  into. The New pipeline dialog hides the schema field behind an
  "Advanced" disclosure — schema defaults to the pipeline name.

- **`compute` is now strictly a cloud deploy target.** `node add` no
  longer writes `compute = "local"` and `node edit --set compute=local`
  is rejected — `compute` selects `lambda` / `fargate` /
  `emr-serverless`. Pipelines run locally by default regardless.
  `pipeline upgrade` strips the legacy `compute = "local"` attribute
  from existing pipelines' `.tf`.

- **Editor laid out around the canvas.** The DAG canvas now fills the
  editor; the node palette, config panel and data preview overlay it
  instead of permanently flanking it. "Add node" is a floating button
  (top-left) that opens a popover. Selecting a node opens a right-edge
  config drawer — closed with its ✕ or Escape — and pans the canvas so
  the node stays clear of the drawer. Preview opens as a centered modal
  (closes on Escape or an outside click), and renders rows as a real
  column-header grid with a sticky header and horizontal scroll —
  previously each record was transposed into one row per field, which
  was unnavigable for wide tables like CloudFront logs (30+ columns).

- **Cross-pipeline reads now work on `compute = "local"`.** A local
  transform can read another pipeline's Iceberg table via `node connect
  --from-table <schema>.<table>` — previously this only resolved under
  `compute = "lambda"`. The local Iceberg warehouse moved from per-pipeline
  (`<pipeline>/.clavesa/warehouse/`) to one workspace-shared warehouse
  (`<workspace>/.clavesa/warehouse/`), so every local pipeline's tables
  share one Hadoop catalog. Existing workspaces auto-migrate their
  per-pipeline warehouses on first load; pre-migration local run history
  for all but one pipeline is dropped and rebuilds on the next run.

- **Pipeline dashboard redesigned around health and a nodes grid.** A
  pinned header answers "is this pipeline healthy?" at a glance — status,
  last run, success rate. Below it, two tabs: Nodes and Graph. The Nodes
  tab is one row per node — its output table (row count + freshness) on
  the left, an Airflow-style run matrix on the right: one column per run
  grouped by day (newest right, each header showing the run's time and
  total duration), each cell a bar whose height encodes that node's
  runtime for that run, so a row reads as the node's duration trend.
  Clicking a node or a cell opens a right-side detail drawer in place —
  the node's inputs, output table + write mode, the chosen run's facts
  (rows written, cold start, compute, runner build, error) and its step
  logs — with a run switcher; a run-column header still opens the full
  run page. Replaces the old stack of disconnected cards (run history,
  per-node activity, output tables) — node, its table, and its runs now
  read as one row instead of three separate vocabularies.
- **Navigation redesign — collapsible sidebar + breadcrumb header.** The
  global nav (Catalog, Sources, Credentials, Pipelines, Dashboards) moved
  from a per-page top bar into a left sidebar that collapses to an icon
  rail; the collapse state is remembered. The header now shows a
  breadcrumb of the current location — every segment clickable, so going
  up a level is one click — and the page's own actions. One consistent
  shell across every page, including the editor, which previously had a
  bespoke bar with no global nav. The ad-hoc per-page "Back to …" links
  are gone; the breadcrumb replaces them.
- **The Pipelines list uses full-width rows, with compute target and run
  health.** It was a grid of cards while every other list page (Catalog,
  Sources, Credentials) used full-width rows; it now matches them — one
  row per pipeline. Each row also shows the compute target (`local` /
  `lambda` / …) and a strip of the last few runs colored by status, with
  the latest run's recency. The run strip loads lazily per row, so the
  list itself still renders instantly.
- **Pipeline DAG shows where each transform writes and how edges read.**
  On both the editor and the pipeline dashboard, every transform node
  footer names its Iceberg output table (`<schema>.<table>`), linked to
  the table's Catalog page. Edges into a transform marked
  `incremental_input` are drawn dashed and animated with an `incremental`
  label; full-read edges stay plain.
- **Pipelines show the registered sources they consume.** The
  `/pipelines` cards list each pipeline's `sources.<name>` references as
  chips, and the per-pipeline dashboard DAG renders them as source nodes
  feeding the transforms. `pipeline list --json` (and `GET /pipelines`)
  carry a new `sources` array. Covers both the http and typed-s3
  reference forms.
- **Catalog groups tables by catalog, with schemas nested inside.**
  Each catalog is one box; its schemas (one per pipeline) are labelled,
  collapsible sub-sections within it, instead of every `(catalog,
  schema)` pair being a separate card that repeated the catalog name. A
  catalog reads as one data model.
- **The `__default` output-key suffix is hidden in the UI.** Catalog
  rows, the TableDetail heading/breadcrumb, and the pipeline dashboard's
  Output tables card show `revenue_by_payment` instead of
  `revenue_by_payment__default` for single-output transforms.
  Multi-output nodes still show their key. The underlying Glue/Iceberg
  table identifier is unchanged.
- **`POST /api/pipeline/edges` and `GET /api/pipelines` now delegate to
  the service layer.** Both HTTP handlers carried their own copy of
  logic the CLI reached through `service.AddEdge` / `service.ListPipelines`.
  The edge handler's inline version *replaced* a transform's whole
  `inputs` map on every connect, silently dropping other edges into the
  same node; routing through `service.AddEdge` (which merges) fixes that.
  `service.PipelineInfo` gained the `cloud` / `compute` fields the list
  handler used to compute on its own, so the CLI's `pipeline list --json`
  now reports them too.

### Fixed

- **Merge-mode transform outputs now deploy to cloud.** A transform with
  `mode = "merge"` + `merge_keys` previously failed `terraform plan` —
  the transform module's output validation only accepted `replace` and
  `append`. Merge is the idempotent shape for dimension tables and
  backfill promotes.

- **Cloud mode no longer 500s on an undeployed workspace.** Switching a
  workspace with no deployed AWS infrastructure to Cloud now shows empty
  observability surfaces (Catalog, dashboards, table snapshots, run
  history) instead of 500 errors.

- **DAG node-output labels show the pipeline's own schema.** A node's
  output-table footer derived the schema from a lineage `via_table`,
  which for a pipeline that only reads cross-pipeline pointed at the
  *upstream* pipeline's schema — the silver `events` node showed
  `bronze.events` instead of `silver.events`. The lineage response now
  carries the queried pipeline's own catalog + schema; the label uses
  that directly.

- **Preview works for transforms that read cross-pipeline.** Previewing a
  transform whose input is an ADR-016 `external_inputs` (`--from-table`)
  reference failed — preview resolved graph edges and registered sources
  but not cross-pipeline tables, so the runner got an empty input map
  (`KeyError` / `TABLE_OR_VIEW_NOT_FOUND`). Preview now samples the
  referenced table from the workspace-shared local warehouse via a
  query-mode runner. Also: the UI preview endpoint now resolves a
  `sql = file("…")` attribute to the file's contents (it already did so
  for `python = file("…")`).

- **Graphs show where a transform's data comes from.** The DAG derived
  synthetic upstream nodes only for registered sources
  (`source_inputs`), and only on the per-pipeline dashboard. A pipeline
  whose only input is a cross-pipeline read (`external_inputs`) — or any
  pipeline opened in the editor — rendered a lone disconnected node. The
  derivation now also covers cross-pipeline reads, and the editor enables
  it too (synthetic source / external nodes are read-only — clicks on
  them are ignored).

- **Editor no longer blanks on a transform with an `s3` source input.**
  Selecting a node whose `source_inputs` held a kind=s3 attachment crashed
  the ConfigPanel (React error #31 — the resolved `{spec_name, bucket,
  prefix, format}` descriptor was rendered directly as a child). The
  inputs list now shows `sources.<name>` for both the s3 descriptor form
  and the http string sentinel.

- **Hive-partitioned `s3` sources keep their partition columns.** The
  runner read prefix-style sources with `recursiveFileLookup`, which finds
  nested files but disables Spark's partition discovery — a source laid
  out `…/day=26/hour=NN/` lost the `hour` column. The runner now detects a
  Hive-partition layout (`name=value/` children) and reads it with default
  partition discovery instead, so the keys surface as columns; non-Hive
  nested layouts still use `recursiveFileLookup`. Runner-image change.

- **Failed-transform errors show the real message.** The runner recorded
  a failed node's `error_msg` with `repr(exc)`, which for PySpark
  exceptions is just `AnalysisException()` — the message dropped. Run
  detail now shows the actual Spark error (e.g. the unresolved-column name
  and suggestions). Runner-image change; rebuild with `make build-runner`.

- **Editor DAG zoom controls are dark-themed.** The React Flow `<Controls>`
  buttons rendered with the library's default white background, showing as
  a stray white box bottom-left of the canvas. They now match the dark
  editor theme, like the minimap.
- **TableDetail sample rows no longer render large numbers in scientific
  notation.** A revenue figure now reads as `79456384.28`, not
  `7.945638428e+07`. Affects local-catalog sample rows and dashboard
  query results.
- **Editor: connecting a second input to a transform no longer drops the
  first.** `POST /api/pipeline/edges` replaced the node's entire `inputs`
  map instead of merging — a transform wired to two upstreams via the UI
  kept only the last one. (See Changed above.)
- **Warm-Spark runner picked up a workspace created mid-session.** The
  persistent query runner resolved its Docker image name once at
  `clavesa ui` startup; if the workspace didn't exist yet it cached
  the invalid `clavesa//transform-runner` reference and every
  catalog / dashboard query failed with "docker: invalid reference
  format". The image name is now resolved lazily per container spawn.
- **Preview now resolves registry-source inputs.** Transforms whose inputs
  reference a workspace-registered source (`inputs = { x = "sources.<name>" }`
  or a typed `source_inputs[x] = { spec_name = "..." }` block) previewed
  empty and failed with `TABLE_OR_VIEW_NOT_FOUND` since v0.21.0. Both the
  CLI (`clavesa node preview`) and the editor's Preview button now
  fetch http and s3 sources from the workspace registry, matching what
  `pipeline run` already does.
- **Editor Preview no longer 400s on relative pipeline dirs.** The UI
  sends `dir=demo`; the preview HTTP handler now resolves that against
  the active workspace (CLI sends absolute paths and was unaffected).

## [v0.26.0] — 2026-05-14

### Added

- Uniform AWS resource tagging across every workspace-managed
  resource. Workspace-emitted `main.tf` now declares
  `provider "aws" { default_tags { tags = {...} } }` with
  `clavesa:workspace` + `clavesa:managed-by = "clavesa"`,
  so every resource any module creates carries the workspace identity
  even if a module forgets to thread tags explicitly. `transform`,
  `source`, and `destination` modules gain a `tags` input variable
  (default `{}`) that merges on top of each module's own `clavesa:*`
  tags. Tag schema documented in `docs/architecture.md` under
  "Resource tagging". One-time billing-console activation of the
  `clavesa:*` keys is still required for Cost Explorer to roll up
  spend by workspace / pipeline.

### Changed

- `clavesa workspace destroy` now sweeps the workspace's system-catalog
  Glue DB (`<system_catalog>__pipelines`, holding the multi-writer
  `runs` / `node_runs` / `tables` tables across every pipeline in the
  workspace) before invoking `terraform destroy`. Same shape as the
  shipped `pipeline destroy` sweep — explicit `yes` confirmation,
  per-table delete via the Glue SDK, `--skip-sweep` to bypass. Pipelines
  should still be destroyed individually first via `clavesa pipeline
  destroy`; this command does not chain into per-pipeline destroys.
- `clavesa pipeline destroy` now sweeps runtime-created Glue tables
  before invoking `terraform destroy`. The runner creates Iceberg
  tables (`<node>__<output>`, plus per-pipeline storage) at execution
  time; none of them are in terraform state, so vanilla `terraform
  destroy` refused with "database is not empty" on
  `aws_glue_catalog_database.pipeline` and the user had to drop into
  the AWS console. The sweep lists every table in the pipeline's Glue
  DB (default `<catalog>__sanitize(<pipeline>)` per ADR-016), prints
  them, asks for explicit `yes` confirmation, then deletes each via
  the Glue SDK before running `terraform destroy`. `--skip-sweep`
  bypasses the step; `--glue-db <name>` targets a different DB (use
  when `var.schema` was overridden from its default). System-DB row
  cleanup (workspace-shared runs / node_runs / tables) is NOT done —
  those rows live inside multi-writer Iceberg tables and would need an
  Athena DELETE; filed as a follow-up.
- `clavesa workspace deploy` and `clavesa pipeline deploy` now run
  the substantive lifecycle `terraform init -upgrade → plan -out=tfplan
  → apply tfplan` with preflight checks, instead of one-shot
  `terraform apply`. Preflight refuses to invoke terraform when
  `clavesa.json` is missing, when AWS credentials don't resolve
  (10s `sts:GetCallerIdentity` against the default credential chain),
  or — for `workspace deploy` only — when the local runner image's
  `clavesa.runner_sha` label doesn't match the embedded runner files
  (catches the stale-image-pushed-silently case). The flow saves the
  plan to `tfplan` and pauses for a `yes` confirmation before applying;
  the plan is removed on success or cancel. New `--yes` / `-y` skips
  the prompt (CI / scripted use); new `--plan-only` stops after `plan`
  without applying. Preflight prints the resolved AWS account + ARN
  before invoking terraform so it's clear which account is about to
  receive the apply.

### Added

- Workspace bucket gets an S3 lifecycle rule expiring objects under
  `athena-results/` (the Athena workgroup's query-result location)
  after `athena_results_retention_days` days (default 14). Without
  this, a workspace running daily accumulates one tiny result object
  per Catalog page hit, dashboard widget reload, and Athena query —
  tens of thousands per year. Set the variable to 0 to disable. Per-
  pipeline `<pipeline>/_athena-results/` written by `runs_writer` is
  not covered (lifecycle prefix filters can't match a wildcard
  segment); lower-volume, deferred.

## [v0.25.0] — 2026-05-14

### Changed

- New pipelines now default `trigger_batch_window = "rate(1 minute)"`
  in their `variables.tf` (was: `null`). S3-event-driven pipelines
  (`source register --kind s3 … --manage-notifications` + transform
  attach) now deploy with a working SQS poller Lambda end-to-end with
  no extra config — matching what `s3-trigger.md`
  promises. Before this, the EventBridge → SQS path was wired but no
  poller drained the queue, so dropped S3 files never triggered runs.
  Existing pipelines on disk keep their old `null` default until the
  user manually edits `variables.tf` or sets the value in
  `terraform.tfvars`; future fresh `pipeline create` invocations get
  the new default.

### Fixed

- `node_runs` and `tables` Iceberg data now lands at the workspace-shared
  `s3://<bucket>/_system/pipelines/<table>/` prefix, matching where
  `runs` already lives. Previously these two were auto-located by Spark
  under the invoking pipeline's per-pipeline `_warehouse/` prefix, so a
  second pipeline in the same workspace would have written to a
  different S3 path even though both register against the workspace-
  wide system Glue DB. Tables created before this change keep their
  existing location (Iceberg pins LOCATION at create time); fresh
  deployments get the unified path.
- S3-trigger pipelines no longer occasionally miss a freshly-arrived
  event. The poller Lambda now uses `receive_message(VisibilityTimeout=0)`
  instead of `ApproximateNumberOfMessages`, which is eventually
  consistent. IAM policy gains `sqs:ReceiveMessage`, drops
  `sqs:GetQueueAttributes`.
- Workspace `main.tf` no longer emits the `data.aws_region.current.name`
  deprecation warning on every `terraform plan` / `apply`. Renamed to
  `.region` (AWS provider v6+ form).
- `clavesa node edit … --output-merge-keys <col>` on its own now
  flips the output's `mode` to `"merge"` in the emitted HCL — the flag's
  help text and `merge-dim-table.md` both promise this, but the CLI was
  only setting `merge_keys` while leaving `mode` empty. Side effect:
  the local-pipeline-run path fell through to `mode = "replace"` and
  ran a full-table `createOrReplace` instead of `MERGE INTO`, so
  snapshot history showed repeated `append +N` ops instead of the COW
  `overwrite +N/-N` the recipe documents. Now matches the cloud
  orchestration path and the runner's own merge-keys-implies-merge
  default.

### Added

- TableDetail's **Lineage** panel now surfaces registered-source upstreams
  (`inputs = { x = "sources.<name>" }`) as a synthetic upstream chip
  labelled `sources.<name> · source-registry`. Previously the panel said
  "No upstream" for transforms whose only input was a registered source,
  contradicting the `multi-stage-pipeline` cookbook's claim that bronze's
  lineage shows the source as upstream.

### Fixed

- `clavesa workspace init --workspace <dir>` now creates `<dir>` if it
  doesn't exist. Previously the command errored with a confusing message
  pointing at `clavesa.json`, forcing every cookbook to start with
  `mkdir -p <dir>`.

## [v0.24.0] — 2026-05-14

### Added

- **Snapshot-bounded incremental reads on transform upstreams.**
  `multi-stage-pipeline.md`'s "what's coming" placeholder is now
  shipped. Mark an upstream alias on a downstream transform with
  `clavesa node edit <node> --incremental-input <alias>` (or the
  matching checkbox in the editor's right-panel "Incremental upstream
  reads" section). The runner reads only Iceberg snapshots committed
  since the consumer's last successful run, tracking watermark per
  `(consumer, alias)`. First run reads full, stamps watermark to the
  current snapshot; subsequent runs read the
  `(last_seen, current_snapshot]` range via Spark's
  `start-snapshot-id` / `end-snapshot-id` options. ADR-014 parity:
  local pipelines store watermarks under `.clavesa/watermarks/`,
  cloud uses the existing pipeline-bucket convention. At-least-once
  on retry, same caveat as partitioned-source incremental; pair with
  `mode = "merge"` + `merge_keys` for idempotent shapes.
- **Multi-output transforms work end-to-end** (rolled up from
  Unreleased). `python-transform.md`'s "Multiple outputs" recipe was
  honest-but-broken; the orchestration emitter hard-coded `outputs = {
  default = "" }` so cloud deployments silently dropped non-default
  keys back to runner-side auto-tables with no per-key mode or
  merge_keys plumbing. Emitter now writes one entry per declared
  `output_definitions` key, and local `pipeline run` carries the same
  per-key payload. New `clavesa node edit --add-output <key>` /
  `--remove-output <key>` flags declare extra output keys without
  hand-editing HCL; matching "Extra outputs" list in the editor's
  right-panel **Output** section. Single-default replace-mode
  transforms emit unchanged (bare-string back-compat).

## [v0.23.0] — 2026-05-14

### Added

- **Registered s3 sources auto-wire S3 event triggers.** Attaching a
  kind=s3 source now materialises one `module "src_<name>"` per source
  into `orchestration.tf`, so the SQS queue + EventBridge rule + poller
  Lambda actually exist on `terraform apply`. Without this, registered
  sources only fired on the cron schedule. Existing pipelines pick up
  the wiring on the next `clavesa source attach` or `pipeline upgrade`.
- **`source register --manage-notifications`.** Have terraform own the
  source bucket's `aws_s3_bucket_notification` (EventBridge enabled).
  Skips the out-of-band `aws s3api put-bucket-notification-configuration`
  step when clavesa owns the bucket. Off by default; the resource is
  authoritative and replaces other notification config. Matching
  checkbox on `/sources` Register → Advanced.

## [v0.22.0] — 2026-05-14

### Added

- **Registered s3 sources work on cloud-compute.** New
  `var.source_inputs` on `modules/transform/aws` accepts the resolved
  source descriptor (bucket / prefix / format / partitions /
  start_from) as a typed object; `clavesa source attach` writes
  there. Closes the TODO #29 plan-time rejection: cloud-deployed
  transforms with registered s3 sources now `terraform plan` cleanly,
  and the Lambda's IAM read scope includes the source bucket
  automatically. Legacy `inputs = { x = "sources.X" }` still parses
  and runs locally; cloud users with that shape re-`source attach` to
  migrate.

- **`source register --partitions` / `--start-from`.** Declare
  Hive-style partition keys on a registered s3 source for incremental
  reads. Runner walks the partition tree, advances a watermark each
  run, reads only new partitions. Matching fields on the `/sources`
  Register form (Advanced). Parquet only.

- **`node edit --output-mode` / `--output-merge-keys`.** Set the
  default-output write mode (`replace` / `append` / `merge`) and the
  merge-key list from the CLI without hand-editing
  `output_definitions` in `.tf`. The editor's right-panel grows a
  matching "Output" section. ADR-015 parity for the merge workflow.

- **Backfill UI (Gate 1, UI half).** PipelineDashboard grows a Backfills
  card on cloud pipelines: list open staging tables, **Stage backfill**
  opens a dialog (node + from + to + `--direct`), each row links to a
  new `/backfills?dir=&run=` review page that mirrors `pipeline backfill
  diff` (staging vs canonical row counts, schema match, merge-key
  counts) plus inline **Promote** / **Discard** actions. ADR-015 parity
  with the v0.21.0 CLI; backed by new `/api/backfills` REST endpoints.
  Append-mode Promote uses a column dropdown sourced from the staging
  schema (auto-picks `event_id`-shaped columns) with a live "would
  update X / would insert Y" preview so the user sees consequences
  before pressing the button; "just append anyway" is tucked under
  Advanced. Local pipelines hide the card (cloud-only feature today).

### Changed

- **Backfill matching/new-key counts now count staging rows, not
  joins.** `pipeline backfill diff` (and the new dedup-check) used
  `staging JOIN canonical` which double-counted when canonical had
  duplicate keys — "200 staging rows would update 400" was confusing
  nonsense. Now uses `WHERE EXISTS` so matching + new always sums to
  staging row count.

### Fixed

- **Backfill Stage dialog: clicking Stage was a silent dead end.** The
  dialog mounts when the dashboard mounts (it's always present, just
  hidden via the `open` prop), and `useState` captured an empty
  `transformNodes` array because the pipeline graph fetch hadn't
  returned yet. The dropdown visually showed `passthrough` (browsers
  render the first option when a controlled `value=""` doesn't match),
  but React state stayed empty — `handleStage` hit the "no node"
  early-return, the toast fired and auto-faded in milliseconds, and
  the user just saw a closed dialog and nothing else. Now a useEffect
  syncs `node` when `transformNodes` lands. Caught the hard way by a
  real button-click rather than a curl test.
- **Console 502s after Promote/Discard.** React Query was refetching
  the diff and dedup-check endpoints for a staging table that had
  just been dropped. Switched to `removeQueries` for those per-run
  keys before navigating away.

- **Local `runs` table now matches the cloud schema (ADR-014 parity).**
  v0.21.0 added `target_table` to the cloud runs_writer's Iceberg schema
  but I missed the matching column in the runner-side `_runs_schema()`,
  so local pipelines wrote 11-column runs tables while cloud wrote 12.
  Surfaced by a README walkthrough against `/tmp/clavesa-readme-walk`:
  catalog page showed `runs · 11 columns`. Existing local tables widen
  on next write via `.option("mergeSchema", "true")`; no migration
  needed. Caught the lesson too: runner-touching commits need the
  README walkthrough before push, not after.

### Removed

## [v0.21.0] — 2026-05-13

### Added

- **Backfill (Gate 1) — CLI surface.** Replay a transform over a historical
  partition window into a parallel Iceberg staging table; review before
  promote.

      clavesa pipeline backfill stage <dir> --node <n> --from <c> --to <c>
      clavesa pipeline backfill list <dir>
      clavesa pipeline backfill diff <run_id> --dir <pipeline>
      clavesa pipeline backfill promote <run_id> --dir <pipeline>
      clavesa pipeline backfill discard <run_id> --dir <pipeline>

  Staging table named `<canonical>__backfill__<run_id>`, tagged in Glue
  with `clavesa:backfill = true` plus the window range so the Catalog
  page and `backfill list` can find it. Promote/discard route through
  the runner Lambda (Spark MERGE INTO, same engine that wrote the
  staging); diff reads via Athena (read-only COUNT + schema). Mode-aware
  promote: `merge` outputs MERGE on declared keys; `append` outputs
  refuse unless `--force-dedup <col>` (drops dupes via MERGE) or
  `--allow-duplicates` (plain INSERT); `replace` outputs not supported
  in this slice — use `--direct` at stage time.
- **`--direct` escape hatch on stage** writes straight to the canonical
  target, skipping staging. Trigger stamped as `backfill-direct` so it's
  distinguishable from a normal manual run.
- **Runner `_operation` event kind.** When the Lambda event carries
  `_operation = "backfill_promote"` or `_operation = "backfill_discard"`,
  the runner skips the transform path and runs the matching SparkSQL
  MERGE / DROP TABLE. Same Spark/Iceberg path that powers transform
  writes, so MERGE semantics stay consistent with `mode = "merge"`
  outputs.

### Changed

- **`runs.target_table` column** added to the workspace system catalog's
  `runs` Iceberg table. NULL on regular runs; on SFN-driven backfill
  runs (future slice) the runs_writer Lambda extracts the staging table
  from `_backfill.target_outputs` and records it so the UI can join
  target → staging. `ALTER TABLE ADD COLUMNS` ships in the
  runs_writer's bootstrap, so pre-v0.21 tables widen on next write.
- **TRIGGER_VALUES** in runs_writer accepts `backfill` and
  `backfill-direct` alongside `manual`/`scheduled`/`event`.

### Fixed

### Removed

## [v0.20.2] — 2026-05-13

### Added

- **`mode = "merge"` output (Gate 4).** Transforms declare
  `output_definitions = { default = { merge_keys = ["id"] } }`; the
  runner generates `MERGE INTO target USING source ON keys WHEN MATCHED
  UPDATE * WHEN NOT MATCHED INSERT *`. First run creates the table (no
  rows to match against); subsequent runs upsert on the declared keys.
  When `merge_keys` is set and `mode` is unset, defaults to `"merge"`.
- **Node palette accepts an optional name** before adding a transform —
  the node and its output table id (`<name>__default`) both carry the
  name. Empty falls back to `transform1`.
- **Editor breadcrumb links to the pipeline dashboard.** Click the
  pipeline name in the editor header to reach Run pipeline.
- **Editor input picker — cross-pipeline tables (ADR-016 Slice 2d).**
  The transform "Add input" affordance gains a *Workspace table* tab
  beside the existing *Source* tab. Picks any Iceberg table produced
  by another pipeline in this workspace; writes the
  `inputs = { alias = "<schema>.<table>" }` shorthand. New endpoint
  `POST /api/pipeline/external-table/attach` is the HTTP twin of the
  CLI `node connect --from-table` command.

### Changed

- **PipelinesList rows open the pipeline dashboard** (not the editor
  directly). Dashboard's Open editor button keeps the round-trip one
  click.
- **Orchestration module `var.nodes` typed `any`** (was
  `map(object({inputs = map(any), outputs = map(any), ...}))`).
  Terraform's map-of-object unification refused multi-node pipelines
  whose nodes carried mixed shapes — partitioned-source inputs (object)
  next to Iceberg-table inputs (string), or merge outputs next to
  replace outputs. The new emitter always writes dict-form outputs for
  non-default modes, including `merge_keys = []` for non-merge outputs,
  so the module accepts both old and new pipelines.

### Fixed

- **Partitioned-source reads now preserve Hive partition columns.**
  Multi-path reads (`spark.read.parquet(*paths)`) drop year/month/day/
  hour columns unless `basePath` is set, so a transform appending to a
  table created in full-prefix mode failed with
  `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA`. Runner now passes
  `option("basePath", "s3://<bucket>/<prefix>/")` on partitioned reads.
- README quick-start now matches reality 1:1 — UI surfaces, navigation
  flow, table names, bonus-dashboard SQL.
- Pipeline dashboard no longer 500s on a fresh pipeline (empty
  ExecutionStates / NodeRuns).
- Transform Save no longer 500s when the round-tripped config carries
  `output_definitions = { default = {} }` or parser synthetic keys.
- Pipeline-run OOM on default Docker Desktop memory: the warm-Spark
  worker is now evicted for the duration of the run.
- Inputs section refreshes immediately after Attach instead of needing
  a node re-select.

## [v0.20.1] — 2026-05-12

### Added

- **Cross-pipeline reads (ADR-016 Slice 2).** Transforms can address
  Iceberg tables produced by other pipelines in the same workspace
  via `<schema>.<table>` strings in their `inputs` map. CLI:
  `clavesa node connect <pipeline> --from-table <schema>.<table>
  --to <transform>`. Orchestration emitter resolves the reference to
  the runner's `spark.table()` call at sync time.
- **Cross-pipeline lineage edges.** The Lineage panel on TableDetail
  surfaces upstream/downstream rows that cross pipeline boundaries
  with a distinct chip + the producing/consuming pipeline name.
  Unresolved references render as `(external)`.

### Changed

- **Catalog page and TableDetail surface the three-level namespace.**
  Tables group under `<catalog> / <schema>` headers; the workspace
  system catalog renders with a `system` badge and sorts last. Table
  URLs become `/tables/<catalog>/<schema>/<table>`; pre-v0.20
  `/tables/<db>/<table>` bookmarks auto-redirect.
- **Transform IAM scoped to the workspace catalog instead of `*`**
  (ADR-016 Slice 2). Transforms read any Iceberg table in their
  workspace's `<catalog>__*` Glue DBs and `<bucket>/*/_warehouse/*`
  S3 prefixes. Writes still pin to this pipeline's schema plus the
  workspace system DB. Resolves Session F P2.

## [v0.20.0] — 2026-05-12

### Changed

- **`runs` / `node_runs` / `tables` moved out of the pipeline schema**
  into a workspace-owned `<workspace>_system.pipelines.*` catalog
  (ADR-016). User pipeline schemas now show only the user's outputs.
  Pre-v0.20 per-pipeline observability tables stay in place but are
  no longer written or read — query them directly in Athena for history.

- **Warm Spark worker for `clavesa ui`.** The Catalog / dashboards /
  TableDetail pages used to spawn a fresh runner container per query,
  paying the ~18-30s Spark JVM cold start every time (1–3 min on
  the literal README landing page). One warm container per pipeline
  warehouse now stays alive for the lifetime of the UI process: first
  call ~15s, subsequent <500ms. Stopped cleanly on Ctrl-C; orphans
  from a SIGKILL'd prior session are swept on next startup. CLI
  one-shots (`pipeline run`, `node preview`) keep per-call containers.

### Added

### Removed

- **Inline source modules — slice 4 (ADR-017) flag day.** Sources are
  workspace-registry entries only. `clavesa node add --type source`
  errors with a clear `source register` redirect; `node add --from`
  is gone (was deprecated in slice 1, dead now). Service
  `AddSourceFromURL`, HTTP `POST /pipeline/sources-from-url`, the UI
  palette's "S3 Source" entry, and the `sourceFetchBridge` plumbing all
  removed. Existing inline `module "src_X" { source = "...source/aws" }`
  blocks in already-authored pipelines continue to parse and run for
  backward compatibility — only the *creation* path is gone. The
  README quick-start collapses from `node add --from … node connect …`
  to `source register --from … source attach …`.

### Added

- **`s3` source kind — slice 3 (ADR-017).** Sources can now point at
  same-account S3 prefixes, not just public http(s) URLs.
  - **CLI:** `source register --from s3://bucket/prefix/` (auto-promotes
    to `kind=s3`, derives bucket+prefix+format from the URL) or explicit
    `--kind s3 --bucket … --prefix … --format …`.
  - **UI:** the same single URL field handles both `https://` and
    `s3://` — server sniffs the prefix.
  - **Runner:** `kind=s3` reads via Spark's S3A. Recursive prefix
    listing on by default for prefix-style paths so `s3://b/events/`
    picks up `events/2024/jan.json` without users encoding the
    partition layout.
  - **Local pipeline-run** forwards `AWS_*` env vars + mounts `~/.aws`
    read-only so `compute=local` pipelines pick up dev creds without
    extra config. Honors `CLAVESA_S3_ENDPOINT` for moto/MinIO
    test infrastructure.
  - **Output cleanups:** `source list --json` and `credential list
    --json` now include the `name` field (was previously dropped via
    storage `json:"-"` tag, breaking `jq` pipes). `source delete
    <unknown>` and `credential delete <unknown>` print "X not
    registered" instead of leaking the filesystem path.
  - **Bug fix:** `file:`-backed credentials now work for local pipeline
    runs — workspace credentials dir is bind-mounted into the runner
    container so the secret payload at the host-absolute path the
    descriptor inlines actually resolves.
- **Credentials registry — slice 2 (ADR-017).** Named secrets at
  `<workspace>/.clavesa/credentials/<name>.json`; sources reference
  them via `--credentials <name>`. The credential file records the
  *reference*, not the secret material itself. Slice 2 supports
  `kind=header` only.
  - **CLI:** `clavesa credential register|list|show|delete`. Three
    secret backends: `arn:aws:secretsmanager:...`, `env:VAR`,
    `file:<workspace-rel>`. Local-only backends (`env:`/`file:`) get
    rejected at orchestration emit for cloud-deployed transforms with a
    clear message.
  - **CLI source register** gains `--credentials <name>` (validated
    against the registry at register time).
  - **UI:** new `/credentials` route + nav link. `/sources` register
    form gains a credential dropdown; rows show a `cred:` chip.
  - **HTTP:** `GET/POST /api/credentials`, `GET/DELETE /api/credentials/{name}`.
    409 + structured `{usages: [{source_name}]}` body when in use.
  - **Runner:** kind=http path resolves the credential at runtime —
    env/file backends in stdlib, `arn:aws:secretsmanager:` via boto3.
  - **`workspace init`** writes `.gitignore` excluding
    `.clavesa/credentials/*.secret` for `file:`-backed payloads.
- **Workspace source registry — slice 1 (ADR-017).** Sources are now
  workspace-level resources at `<workspace>/.clavesa/sources/<name>.json`.
  Pipelines reference sources by name (`inputs = { x = "sources.<name>" }`)
  alongside the existing inline `module "src_X"` shape (inline support is
  removed in slice 4).
  - **CLI:** `clavesa source register|list|show|delete|attach`. Slice 1
    supports `kind=http` only (no auth — credentials registry lands in
    slice 2). `delete` refuses when pipelines reference the source;
    `--force` overrides.
  - **UI:** new `/sources` route — list view, register form, delete with
    inline dependency surfacing.
  - **HTTP:** `GET/POST /api/sources`, `GET/DELETE /api/sources/{name}`,
    `POST /api/sources/{name}/attach`.
  - **Runner:** new `kind=http` input descriptor — runner downloads the
    URL at execution time (cached per Lambda container) and reads the
    file with the registered format.
  - **Orchestration emitter / pipeline run:** both resolve `sources.<name>`
    references against the workspace registry and emit kind-specific
    runner descriptors.
- URL-source authoring on both surfaces (ADR-015 parity):
  - **CLI:** `clavesa node add --from <url>` fetches a public dataset
    (http/https) into the pipeline's `inputs/` dir, infers `format`
    from the file extension, and configures `bucket`/`prefix` in one
    step. Implies `--type=source`; `--name` derives from the URL
    filename when omitted.
  - **UI:** new "Source from URL" affordance in the editor's node
    palette — paste a URL, hit "Fetch & add", same backend service
    method as the CLI.
  - **HTTP:** `POST /api/pipeline/sources-from-url`.
  Collapses the README quick-start's mkdir/curl/node-add/node-edit
  dance into one command.
- README: worked second-dashboard example over the demo pipeline's
  `revenue_by_payment__default` gold table — a copy-pasteable JSON file
  showing all four widget types against real data.

### Fixed

- Seeded `pipeline-runs-<name>` dashboard now queries the ADR-016
  `<catalog>__<schema>` Glue namespace (e.g.
  `clavesa_demo_ws__demo`) instead of the pre-ADR-016
  `clavesa_<pipeline>` form. Previously the seed SQL silently
  returned 0 rows on every workspace where workspace-name ≠
  pipeline-name, which is most of them.
- `node disconnect` and `DELETE /api/pipeline/edges/{id}` actually remove the
  edge for transforms — previously they cleared a non-existent singular
  `input` attr while the canonical `inputs = { … }` map went untouched.
- `clavesa pipeline plan|deploy|destroy <pipeline>` now resolve the
  pipeline arg against the active workspace, matching `pipeline run` and
  every other `pipeline X <dir>` command.

## [v0.19.0] — 2026-05-10

### Added

- `clavesa workspace use <path>` records the current workspace so later
  commands resolve without `--workspace`.
- `clavesa pipeline run` dispatches cloud pipelines to SFN
  `StartExecution` (ADR-014). `--wait` blocks until terminal.
- Workspace bucket: versioning, SSE-S3 default (KMS via `var.kms_key_id`),
  public-access-block. New `var.force_destroy` (default `false`).
- `make validate-examples` runs `terraform validate` on every module
  example; gates `make release-check`.

### Changed (breaking module contract)

- `transform.runner_image` is required and must be a private ECR URI.
  The old public-ECR default never worked.
- `workspace.force_destroy` defaults to `false`. Tear-down of test
  workspaces requires `force_destroy = true` or a manual `s3 rm` first.
- Removed `modules/output-table/aws/` (dead module, no consumers).

### Changed

- `clavesa ui` auto-derives `ATHENA_OUTPUT_BUCKET` from
  `terraform.tfstate` when unset.
- `workspace init` / `pipeline create` print editor/CLI-oriented next
  steps.
- `runs.trigger` rejects unknown values (coerced to `"manual"`); allowed
  set documented as `runs_writer/index.py:TRIGGER_VALUES`.
- All module READMEs rewritten — old content described the retired
  Athena/Glue Jobs/CTAS architecture.

### Fixed

- Pipeline dashboard run-history / node-runs / tables-state panels load
  for cloud pipelines (post-ADR-016 DB encoding).
- Empty pipelines render an empty state instead of a 500.

## [v0.18.1] — 2026-05-09

### Fixed

- Orchestration module now creates the Glue DB at the same encoded
  `<catalog>__<schema>` name the transform module writes to, so
  run-history and table records land in the workspace's catalog
  instead of an orphan `clavesa_<pipeline>` DB. Without this,
  v0.18.0 cloud deploys failed on the transform Lambda's first
  write (Glue `CreateDatabase` AccessDenied).

### Changed (breaking module contract)

- `modules/orchestration/aws` now requires `catalog` and `schema`
  variables (parity with the v0.18.0 transform module). Pipelines
  re-emit `orchestration.tf` automatically on the next mutation
  via the v0.18.1 binary; manual callers must thread both through.

## [v0.18.0] — 2026-05-09

### Removed (breaking)

- Pre-ADR-016 fallback paths dropped. `catalog` and `schema` are
  required everywhere — runner, transform module, encoder, catalog
  handler. Pipelines pinning to v0.18+ must thread both through
  every transform module call or `terraform validate` fails.

### Changed

- Workspaces without a `catalog` field auto-migrate on first
  `Manifest.Load` (gets `clavesa_<sanitize(name)>`). The
  deferred slice 5 migrate-catalog tooling is dropped — this
  one-shot covers the residual case.

### Fixed

- Catalog page (`GET /api/workspace/tables`) no longer surfaces
  tables from other workspaces' Glue DBs in the same AWS account.
  Pre-1g it filtered by the global `clavesa_` prefix; now it
  scopes to the current workspace's catalog. Side benefit: the
  500-error noise from cross-workspace S3 snapshot fetches is gone.

## [v0.17.0] — 2026-05-09

### Added — Three-level namespace (ADR-016 slices 1a–1f)

- `<catalog>.<schema>.<table>` addressing end-to-end. New flags:
  `workspace init --catalog`, `pipeline create --schema`, plus a
  Schema field on the UI's New-pipeline form. Defaults:
  `clavesa_<sanitize(workspace_name)>` and
  `sanitize(pipeline_name)`. Display name and identifier are now
  distinct (e.g. workspace `demo-ws` → Glue DB `clavesa_demo_ws`).
- New-workspace Glue DBs encode as `<catalog>__<schema>` (e.g.
  `clavesa_demo_ws__cloudfront` instead of `clavesa_cloudfront`)
  — different workspaces no longer share a Glue namespace.
- Legacy workspaces (no `catalog` field) keep today's
  `clavesa_<pipeline>` until they migrate.
- `pipeline create` over HTTP now matches the CLI: trigger vars,
  `.gitignore`, orchestration sync, and seed dashboard. Previously
  the HTTP shortcut had drifted.

### Added — Dashboards

- New `/dashboards` route. Saved SQL widgets (`big_number`, `line`,
  `bar`, `table`) over the workspace catalog, authored as JSON in
  `<workspace>/.clavesa/dashboards/<slug>.json`. First
  `pipeline create` in an empty workspace seeds a "Pipeline runs"
  dashboard. Recharts is lazy-loaded so the front-door bundle stays
  ~229 KB gzip. In-UI authoring (drag-resize, query editor, save)
  is follow-up.

### Fixed — ADR-014 local↔cloud parity

- Local pipeline run history populates the dashboard panel (was
  always empty — querying an Iceberg table the local orchestrator
  never wrote). Local provider now reads the filesystem progress
  channel directly.
- Local ExecutionLogs carry per-event `timestamp` (was always `""`).
- Local SampleTable / Query responses carry per-column types.
- ExecutionLogs response carries `source: "cloudwatch"|"local"` so
  the UI can show backend-appropriate hints (`tail <file>` vs
  "query CloudWatch directly").
- `POST /api/dashboards/query` requires `dir` on both backends
  (cloud previously accepted missing `dir` silently).
- Run-duration line widget's X-axis now reads oldest → newest (was
  reversed). Existing dashboards keep their old SQL until edited.
- "Recent executions" placeholder card no longer renders on local
  pipelines (duplicated the Run-history card downstack).
- Catalog correctly labels the `tables` system table.

### Fixed — Backend HTTP error contract

- `GET /api/pipeline*` returns 404 (was 500) when `?dir=` doesn't
  exist.
- `POST /api/pipeline/edges` surfaces parse errors instead of
  silently writing malformed Terraform.
- `GET /api/workspace/tables` uses the standard `{"error":"…"}`
  envelope and degrades to `aws_available: false` (was 500) when
  Glue is unreachable.

### Fixed

- `CLAVESA_QUERY=1` runner mode no longer crashes on
  `TimestampType` columns — Dockerfile installs `tzdata` and sets
  `TZ=UTC`.

## [v0.16.0] — 2026-05-08

- Lineage panel on TableDetail: upstream + downstream neighbors, click-through.
- Catalog freshness chip now lights up for cloud (`compute = "lambda"`)
  pipelines, not just local.
- Sample panel works for local-only Iceberg tables (was 500ing — went
  through Athena unconditionally).
- New `node_runs.output_rows` column (sum of added-records across this run's
  Iceberg outputs); RunDetail surfaces it.
- ~32 KB gzip off the data-first bundle by lazy-loading the legacy editor
  (`/editor` only).

## [v0.15.0] — 2026-05-08

- Per-transform `freshness_sla = "4h"` → Fresh / Aging / Stale chip on
  Catalog tiles. Hidden when no SLA declared.
- Run-detail page (`/pipelines/run?dir=…&run=…`): pipeline DAG colored
  by per-node status, triage strip (module version + image digest),
  inline failure detail, per-node breakdown.
- Per-node sparkline on the dashboard — recent run durations, surfaces
  slow-creep regressions the pass/fail strip misses.
- `clavesa_<pipeline>.tables` writer — runner appends one row per
  Iceberg-mode output. Catalog dashboard panel shows row count + age
  per table.
- Local `pipeline run` writes Iceberg outputs by default (was plain
  Parquet). Closes catalog parity gap.
- Catalog snapshot fetches dispatch by pipeline dir — local tables
  no longer 500.
- Local `runs ↔ node_runs` join works (both sides now stamp
  `sf_execution_arn` consistently).

## [v0.14.0] — 2026-05-07

- New `runner_image_digest` + `module_version` columns on `node_runs`.
  "Which build produced this row?" is a SQL query now.
- `runs.trigger` populated (manual / scheduled / event) via SFN-input
  smuggling. Trigger badge on dashboard Run history.
- Local `runs` table writer — `clavesa pipeline run` appends to
  `clavesa_<pipeline>.runs` so the dashboard's run history works for
  `compute = "local"` too.
- Local–cloud observability parity (ADR-014). The `/api/data/*` and
  `/api/pipeline/execution/*` endpoints accept `dir=`; new
  `internal/observability/` package picks Cloud or Local provider per
  request based on the pipeline's `compute` attr.
- Filesystem progress channel for local runs at
  `<pipelineDir>/.clavesa/runs/<runID>/`.
- Per-pipeline Hadoop warehouse at `<pipelineDir>/.clavesa/warehouse/`
  (was a shared `/tmp` dir).
- Catalog page surfaces local Iceberg tables alongside Glue ones.
- Fixed: `pipeline run` honored only Parquet sources, now CSV/JSON too.
- Fixed: `workspace init` cache stopped serving stale runner images on
  embedded-source edits.

**Upgrade note:** existing cloud `node_runs` schema evolves on the first
write after the upgrade (Iceberg mergeSchema). Trigger one transform run
before querying the new columns from Athena.

## [v0.13.0] — 2026-05-07

- Per-pipeline run history queryable from Athena.
  `clavesa_<pipeline>.runs` (per-execution rollup, EventBridge writer
  Lambda) and `clavesa_<pipeline>.node_runs` (per-invocation, runner
  self-reports). Same SQL surface as user data.
- New endpoints: `GET /api/data/tables/{db}/{t}/snapshots`,
  `/api/data/node-runs`, `/api/data/runs`,
  `/api/pipeline/execution/{states,logs}`.
- Data-first UI: Catalog at `/`, TableDetail at `/tables/:db/:table`,
  per-pipeline dashboard at `/pipelines/dashboard?dir=…`. Legacy editor
  remains at `/editor?dir=…`.
- Live SFN run state on the editor DAG (PipelineNode borders pulse during
  in-flight executions).
- Inline CloudWatch log lines on failed nodes.
- UI rewritten on Tailwind v4 + shadcn + TanStack Query. All backend
  routes mount under `/api/*`.

## [v0.12.0]

Partitioned-source incremental reads + append output mode. Watermarks
land per `(node, input_alias)`. (Pre-CHANGELOG era; see `git log
v0.11.0..v0.12.0`.)

## [v0.11.3]

Fix: orchestration emits Iceberg table id for transform→transform inputs.
See `8d244ed`.

## [v0.11.2]

`pipeline upgrade` re-syncs orchestration; Lambda image digest auto-
updates via `aws_ecr_image` data source. See `b9b1428`.

## [v0.11.1]

Fix: `s3://` scheme mapping for Hadoop-AWS; widened Iceberg warehouse IAM.
See `debd85a`.

## [v0.11.0]

Iceberg auto-tables — every transform output lands as an Iceberg table in
Glue Data Catalog by default (per ADR-013). See `f67aa34`.

[Unreleased]: https://github.com/vesahyp/clavesa/compare/v0.16.0...HEAD
[v0.16.0]: https://github.com/vesahyp/clavesa/compare/v0.15.0...v0.16.0
[v0.15.0]: https://github.com/vesahyp/clavesa/compare/v0.14.0...v0.15.0
[v0.14.0]: https://github.com/vesahyp/clavesa/compare/v0.13.0...v0.14.0
[v0.13.0]: https://github.com/vesahyp/clavesa/compare/v0.11.3...v0.13.0
[v0.12.0]: https://github.com/vesahyp/clavesa/compare/v0.11.3...v0.12.0
[v0.11.3]: https://github.com/vesahyp/clavesa/compare/v0.11.2...v0.11.3
[v0.11.2]: https://github.com/vesahyp/clavesa/compare/v0.11.1...v0.11.2
[v0.11.1]: https://github.com/vesahyp/clavesa/compare/v0.11.0...v0.11.1
[v0.11.0]: https://github.com/vesahyp/clavesa/releases/tag/v0.11.0
