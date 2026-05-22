# ADR 014: Local–Cloud Parity Across the User Surface

**Status**: Accepted (extends ADR 012)

## Context

ADR 012 committed clavesa to a single transform engine (PySpark) running on every compute target. A query that runs locally runs identically in Lambda. **Engine parity** was the promise, and it's holding for transforms.

What we never wrote down: parity stops at the engine. The user-facing surface — Catalog, TableDetail, Pipeline Dashboard, editor DAG overlays, log tails — has been quietly drifting toward cloud-only. The v0.13.0 observability slice made this concrete:

- **Catalog** enumerates Glue databases prefixed `clavesa_*`. A `compute = "local"` pipeline writes to a Hadoop catalog under `/tmp/clavesa-warehouse`; those tables don't show up.
- **Snapshots / Volume timeline / Run history / Per-node activity** all hit Athena. Local has no Athena.
- **Live SFN run state on the editor DAG** polls `GetExecutionHistory`. Local has no Step Functions.
- **Inline CloudWatch logs** in the StatusPanel failure row read `/aws/lambda/<func>` log groups. Local has stdout.
- **`runs` table** is populated by an EventBridge → Lambda writer triggered by SFN status changes. Local emits no events.

The result: a `compute = "local"` pipeline can run end-to-end (engine parity, ✓), but the entire data-first UI surface is dark or in graceful-degradation error states. Local becomes a CI fixture, not a real environment. Users who want to develop pipelines on a laptop must reach for AWS to actually see what their pipeline did.

This is the wrong default. Clavesa's positioning is "the visual ETL tool you can run on your laptop and deploy to AWS unchanged." Drifting cloud-only on observability turns that into "you can run it on a laptop, but you can only see what it did from the cloud."

An earlier open question — "should `compute = "local"` pipelines also write run history" — was answered "probably yes" in planning. This ADR makes "yes" the binding answer and extends the principle to every UI surface, not just history.

### Why now

The `tables` metadata table is the next slice. The `runs` table EventBridge writer is already cloud-only and represents debt. Without a parity decision, every new metadata artifact will replicate the same trap: cloud writer + Athena reader, with `compute = "local"` quietly broken.

Better to commit to the principle now and design `tables` for both environments from the start than to ship cloud-first and retrofit local later.

## Decision

**Local–cloud parity is a binding constraint on every user-facing clavesa surface.** Concretely:

1. **Every UI feature that works against a deployed pipeline must work against a `compute = "local"` pipeline too.** The two paths may use different backends; the user-facing behavior must be the same.

2. **Backends are allowed to differ.** Cloud reads from Athena over Glue-cataloged Iceberg; local reads from a Hadoop-cataloged Iceberg warehouse via Spark or by parsing Iceberg metadata directly. Cloud sees live runs through SFN history; local sees them through a runner-emitted progress channel. The UI's `useNodeRuns` / `useRuns` / `useExecutionStates` hooks return the same shapes from both.

3. **The "compute target" of the *pipeline being inspected*, not the host running clavesa, decides the backend path.** A `compute = "local"` pipeline is always observed via the local backend, even when clavesa is running on a workstation that has AWS credentials. A `compute = "lambda"` pipeline is always observed via the cloud backend, even when clavesa is running offline (in which case those panels surface "AWS unavailable" — same graceful-degradation we already have).

4. **New observability features ship local *and* cloud. "Cloud-only first, local later" is not allowed.** The local path is part of the feature, not a follow-on.

### Backend boundary

A new internal abstraction: **per-pipeline observability provider.** The HTTP layer (`internal/dataquery`, `internal/pipelinestatus`) doesn't talk to Athena/SFN/CloudWatch directly. It asks a provider for `Snapshots(table)`, `NodeRuns(pipeline)`, `Runs(pipeline)`, `ExecutionStates(arn)`, `Logs(arn, step)`. Two implementations:

- **`cloudProvider`** — Athena queries, SFN API, CloudWatch FilterLogEvents. The current code paths, refactored behind the interface.
- **`localProvider`** — reads Iceberg metadata files directly from the local warehouse path; reads `node_runs` / `runs` Hadoop-catalog tables via Spark or by parsing Parquet; reads runner stdout from a captured rotating file; reads progress events from a runner-emitted JSON state file in the workspace.

The HTTP handler picks the provider per-pipeline by inspecting the `compute` attribute of the pipeline's transform modules (already in the `.tf`). One pipeline can use the cloud provider; a sibling pipeline in the same workspace can use local. They coexist.

### Local progress channel

Live execution state is the hardest gap. Cloud has SFN's `GetExecutionHistory`. Local has the runner-as-subprocess.

Mechanism: `clavesa pipeline run` writes a JSON state file at `<pipeline-dir>/.clavesa/run-<run-id>.json` (gitignored). The runner appends one line per state transition (node entered, succeeded, failed). The local provider tails this file; the same UI hook (`useExecutionStates`) consumes it. State files older than 24 hours are pruned on next run.

Logs: the runner's stdout/stderr is captured to `<pipeline-dir>/.clavesa/logs/<run-id>/<node>.log` per invocation. The `Logs` provider serves these.

These are filesystem-only — no IPC, no daemon, survives crashes, easy to inspect by hand.

### Local Glue substitute

The Catalog page enumerates `clavesa_*` Glue databases today. The local provider reads the Hadoop catalog metadata directly: each `clavesa_<pipeline>.<table>` is a directory under `<warehouse>/clavesa_<pipeline>.db/<table>/metadata/`. The provider lists these and surfaces the same `CatalogTable` shape.

Snapshots: parse `<warehouse>/.../metadata/<n>.metadata.json` directly. No SQL engine needed for snapshot listing.

## Consequences

**Positive:**

- **Local is a first-class environment.** A pipeline developed on a laptop has the same dashboard, the same Catalog, the same drill-in flows as one deployed to AWS. The "data-first front door" works without AWS credentials.
- **CI works fully.** Test suites can exercise the entire UI surface against in-process pipelines, no LocalStack, no moto, no AWS account.
- **Cost floor for new users approaches zero.** Try clavesa with `compute = "local"`, see results in the same UI, decide later whether to deploy. Cloud becomes a deployment choice, not a usability gate.
- **Forces good API shapes.** A backend interface that has to be implemented twice tends to grow cleaner contracts than one wired directly to a single SDK.
- **Two-environment validation.** Bugs that only show up under one backend get caught faster because both run in CI.

**Negative:**

- **Two implementations per feature.** Roughly 1.5–2× the backend code per observability feature, indefinitely. Real cost; we accept it in exchange for the principle.
- **Local-specific failure modes.** File locking on the runner state file, multiple `pipeline run` invocations sharing the same Hadoop catalog, stale `.clavesa/` artifacts. Each will need attention.
- **Drift risk.** The two providers can subtly diverge in behavior (an extra column on cloud, a different ordering on local). Mitigation: a shared contract test suite that runs the same `Snapshots() / NodeRuns() / Runs()` calls against both providers and asserts identical response shapes for the same input data.
- **Local progress channel is a new concept.** The filesystem-as-IPC approach is simple but not battle-tested at scale. Acceptable because local pipelines are single-user single-process by definition.
- **Some features may not have a meaningful local equivalent.** "Failed Lambda request id" doesn't exist locally. The schema includes the column; local providers leave it null. Document, don't pretend.

**Tradeoffs accepted:**

- Maintenance cost of two backend paths in exchange for local being a real environment.
- Filesystem-as-IPC for local progress in exchange for not running a daemon.
- Slight schema awkwardness (cloud-only fields nulled on local) in exchange for one shared response shape.

## What this changes elsewhere

- **The v0.13.0 observability work is now in arrears for local.** Each Athena-backed endpoint (`/api/data/snapshots`, `/api/data/node-runs`, `/api/data/runs`, `/api/pipeline/execution/states`, `/api/pipeline/execution/logs`) needs a local provider before the parity claim is real.
- **The `tables` metadata table** ships local and cloud together. Runner writes to whichever catalog is active (Glue for cloud, Hadoop for local). Same code path, different warehouse — pattern already established for `node_runs` in v0.13.0.
- **The `runs` writer Lambda** is the cloud backend for one specific feature. Local needs an equivalent: the runner emits a `run.json` summary file at end of execution, which the local provider reads as the per-execution rollup. CLI's `clavesa pipeline run` becomes responsible for writing that file.
- **Workspace init.** `clavesa workspace init` creates a `.clavesa/` directory under the workspace root with subdirs for `runs/`, `logs/`, `progress/`. Documented as runner-emitted state; safe to delete.
- **CLAUDE.md.** New hard rule: "New observability features ship local-and-cloud, not cloud-first." References this ADR.

## Open questions

- **What does `compute = "lambda"` mean when clavesa is running offline?** Probably: cloud provider attempts the Athena/SFN call, fails, the UI shows the existing "AWS unavailable" empty state. No local fallback for cloud pipelines — they're cloud pipelines.
- **Single-pipeline workspaces vs multi-pipeline.** Each pipeline picks its own provider per its `compute` attr. Mixed workspaces (one local, one cloud) work; the dashboards just route differently per page. No global "are we in cloud mode?" toggle.
- **Schema migration if a pipeline switches `compute` from "local" to "lambda".** The local Hadoop-catalog tables and the cloud Glue-catalog tables are separate. Migration is out of scope; switching `compute` means starting over on the table data. Document, don't paper over.

## References

- ADR 012 — PySpark as universal execution engine. This ADR extends 012's parity principle from the engine to the user surface.
