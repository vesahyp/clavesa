# ADR 024: Split environment mode into warehouse and compute

**Status**: Accepted (2026-06-12).

## Context

Since TODO bucket 16 the workspace has carried a single toggle — the "environment mode," `local` or `cloud` — that answered every local-vs-cloud question at once: where tables live, which observability provider serves reads, where `pipeline run` executes, where watermarks advance. One switch, one answer.

Real operation wants two answers. The most common ask: a workspace whose state lives in the cloud (Glue + S3, the deployed pipeline), but where a particular run or backfill should execute on the laptop — to iterate on transform logic against real data without paying a deploy cycle, or to drive a backfill from a machine with better visibility. The single toggle cannot express "cloud state, local execution"; flipping it to `local` moves *everything*, including which catalog reads come from, which is exactly not what the user meant.

The two questions are genuinely different in kind. Where state lives is a workspace-wide fact: half the surfaces reading one catalog while the other half reads another is incoherence, not flexibility. Where heavy work executes is a per-action choice: it changes nothing about what the work produces or where it lands.

## Decision

**The single environment mode splits into two concepts:**

1. **Warehouse** (`local` | `cloud`) — workspace-wide, the rename of today's mode. It decides where **all** state lives:
   - catalog + data: local Hadoop-catalog directory vs Glue + S3;
   - the observability provider (runs, node_runs, logs — ADR-014's Provider seam);
   - the serving read engine (ADR-022/023: Spark locally, Athena + transpiled SQL in cloud);
   - watermarks and SQS source cursors;
   - which catalog interactive Spark surfaces (preview, notebooks) target.

2. **Compute** (`local` | `cloud`) — per-action (`pipeline run`, `backfill`), defaulting to the warehouse. It decides only **which machine executes heavy work**: the local runner container vs the deployed Lambda/SFN. Nothing else.

Valid combinations: `(local, local)`, `(cloud, cloud)`, and cloud warehouse with local compute. Local warehouse with cloud compute is impossible — cloud compute cannot reach a laptop disk — and is rejected.

**Within one warehouse, all compute reads and writes the same state.** A locally-computed run against a cloud warehouse drains the same SQS cursor, advances the same `_watermarks/*.json`, and lands rows in the same Glue/S3 tables as a Lambda run. This is by design: compute is *only* an execution-placement choice, never a state fork.

**Concurrency: runs are serialized per pipeline by a lease lock stored in the warehouse.** `s3://<bucket>/<pipeline>/_locks/run.json`, acquired via S3 conditional PUT (compare-and-swap); a local file plays the same role on a local warehouse. Every compute — Lambda, laptop, a second laptop — acquires the same lock before running. This was chosen over an SFN-overlap check because it is symmetric: it guards cloud-vs-local in both directions and two laptops against each other, and it keeps the coordination state in the warehouse next to the cursor state it protects. The rationale is hard: S3SingleDriverLogStore tolerates only one Spark driver writing a Delta table at a time, so two concurrent runs against the same warehouse can corrupt a Delta log.

### Wire and flag aliasing

The rename is loud in Go (`workspace.Warehouse`, `LoadWarehouse`, `WriteWarehouse`, `ParseWarehouse`) and aliased on the wire:

- `.clavesa/environment.json` keeps its filename; reads prefer the `warehouse` key and fall back to the legacy `mode` key; writes carry both keys with the same value, so older binaries keep reading the file.
- `GET/PUT /workspace/environment` keeps its route and returns both `warehouse` and `mode` (deprecated alias, same value); PUT accepts either, `warehouse` winning.
- `--env` on `workspace use` and `pipeline run` survives as a hidden, deprecated alias for `--warehouse`.

Engine identity (which engine actually served a response) becomes per-response `served` metadata in a later slice; it is no longer derivable from a single toggle once compute can diverge from the warehouse.

## Consequences

- This ADR ships the model split and rename only. The `--compute` flag on `pipeline run` / `backfill`, the shared dispatcher, and the lease-lock implementation land in follow-up slices; until then compute always equals the warehouse and behavior is unchanged.
- Every dispatch site that read the environment mode is, by definition, reading the warehouse now — the resolver, the catalog handler, reset/optimize/backfill. No site needed a semantic re-decision; the rename is the decision.
- At-least-once input re-reads (the watermark gotcha) are unchanged by the split, but the lease lock removes the *concurrent* variant of the hazard: two runs can no longer interleave a re-read window.

## Relationships

- Honors **ADR-014** (local/cloud parity): the warehouse is the provider switch ADR-014 already mandated; compute divergence never changes response shapes.
- Refines **ADR-016**: the warehouse decides which physical catalog the three-level namespace resolves against; naming is untouched.
- Honors **ADR-022/ADR-023**: the serving read engine and the transpile seam follow the warehouse, not the compute.
