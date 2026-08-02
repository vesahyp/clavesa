# ADR 026: Mirror s3 sources locally for local-warehouse runs

**Status**: Accepted (2026-08-02). Issue: clavesa#91.

## Context

On a local-warehouse `pipeline run`, a registered s3 source resolves to a direct-S3
listing read: the runner container scans the full `bucket/prefix` over the internet,
every run. Spark amplifies this — JSON/CSV schema inference is a full pass on its own,
and each downstream action can re-scan the source — so one nightly run reads the same
bytes several times.

This is a direct hit on the north star (cost per billion records) at the local rung of
the ladder, and it was measured, not hypothesized: the ecarbrowser insights pipeline
(dogfooding) read a 0.52 GB JSONL history ~8× per nightly run, ~4.2 GB/night of billed
egress — enough to exhaust an account's 100 GB/month free data-transfer tier
mid-month, with cost compounding as history grows. The clavesa.dev traffic pipeline
has the same shape on its CloudFront logs. Both grew per-pipeline workarounds
(hand-authored local-path source + `aws s3 sync` in a cron script) — the exact class
of external-script workaround that is supposed to become product work.

The pieces to fix it already exist: the runner reads `{"kind": "path", "path": …,
"format": …, "read_options": …}` descriptors, and the local run's input-mount
collection (`inputLocalPath`) already mounts `kind=path` inputs into the container.
What is missing is the sync and the descriptor swap.

## Decision

**On a local-warehouse run, non-partitioned s3 registry sources are mirrored into the
workspace cache and the runner reads the mirror, not S3.**

1. **Mirror location**: `<workspace>/.clavesa/cache/sources/<name>/`. The cache dir is
   already the disposable, gitignored, delete-anytime area; a deleted mirror simply
   re-syncs on the next run.
2. **Sync semantics**: one `ListObjectsV2` walk of `bucket/prefix` per run, then
   - download keys that are new or changed (size differs, or S3 `LastModified` differs
     from the local file's mtime; after download the local mtime is set to the S3
     `LastModified`, making the comparison cheap and idempotent);
   - delete local files whose key no longer exists remotely (a mirror, not an
     accumulator — S3 lifecycle expiry must propagate);
   - keys nest into subdirectories on `/`.
   Steady-state cost is one listing plus the day's new files.
3. **Descriptor swap**: the resolved input becomes
   `{"kind": "path", "path": <mirror dir>, "format": <spec.format>,
   "read_options": <spec.read_options>}` — the shape the runner and the mount
   collector already handle. The runner is unchanged.
4. **Failure is loud**: if the sync fails (no credentials, network down, bucket gone),
   the run fails with a clear error. No silent fallback to a stale mirror — stale
   input data indistinguishable from fresh is worse than a failed run. (Direct-S3
   would fail in the same situations anyway.)
5. **Scope — what stays direct-S3**:
   - **Partitioned / `start_from` sources**: their incremental cursor semantics live
     in the runner's partition walk; mirroring underneath them buys little (they
     already read only new partitions) and risks cursor drift. Unchanged.
   - **`--compute local` against a cloud warehouse** (ADR-024): that path's contract
     is exact parity with a deployed run, including SQS notification drain. Unchanged.
   - **Preview**: reads a bounded sample host-side; not worth coupling to the mirror
     in this slice.
6. **Opt-out**: `CLAVESA_SOURCE_MIRROR=off` restores the direct-S3 descriptor, for
   sources too large to spend local disk on. The sync logs what it did (files
   downloaded / deleted / total mirror size) so the disk trade is visible.

## Consequences

- Local-run egress for listing-read sources drops from
  `full-history × scans-per-run × runs` to `new-files-per-day`, and repeated
  in-run scans hit local disk. For the two dogfooding pipelines this retires
  ~130 GB/month of billed egress and both hand-rolled workarounds.
- Local disk now holds a copy of each mirrored source. Acceptable: the cache dir is
  disposable, the sizes are logged, and the opt-out exists.
- Semantics are unchanged — the mirror is byte-identical to S3 at sync time, so
  local/cloud parity (ADR-014) holds: same rows either way. The one visible
  difference is at-run-start staleness (an object landing in S3 *during* a run is
  not seen — which is already true of a direct read's listing pass).
- First run after this ships re-downloads each source once to prime the mirror; cost
  equals one of today's runs.
