# Migrating from clavesa v1.x to v2.0.0

v2.0.0 switches the default table format from Apache Iceberg to Delta Lake. The v2.0.0 binary does not read or write v1.x Iceberg workspaces — you recreate them from source.

## Who needs to migrate

Everyone running v1.x with a deployed or local workspace. If you haven't run `clavesa workspace init` yet, you're starting fresh on v2.0.0 and this recipe doesn't apply to you.

## The migration path

No automated tool ships in v2.0.0. Recreate from source: your pipeline code is unchanged, your source data stays where it is, and the Delta tables materialise on the first run.

**Step 1 — Create a new v2.0.0 workspace.**

```bash
WS=/path/to/new-workspace
mkdir -p $WS
bin/clavesa workspace init my-workspace --workspace $WS
```

**Step 2 — Re-author the pipelines.**

The `.tf` syntax is unchanged at the user level. If you have the old workspace checked into git, copy the pipeline directories across:

```bash
cp -r old-workspace/my-pipeline/ $WS/
```

No `?ref=` changes needed — v2.0.0 embeds modules in the binary. If `pipeline upgrade` prompts to rewrite `?ref=` values, accept it; the embedded modules take over.

Sources in the registry (`.clavesa/sources/`) copy across unchanged:

```bash
cp -r old-workspace/.clavesa/sources/ $WS/.clavesa/
```

Credentials in `.clavesa/credentials/` copy across unchanged too.

**Step 3 — Run the pipelines.**

```bash
bin/clavesa pipeline run my-pipeline --workspace $WS
```

Delta tables materialise on first write. System tables (`runs`, `node_runs`, `tables`, `column_stats`, `dashboards`) recreate automatically on the first run that writes to them. Old run history does not carry over.

**Step 4 — Deploy to cloud (if applicable).**

```bash
bin/clavesa workspace deploy --workspace $WS
bin/clavesa pipeline deploy my-pipeline --workspace $WS
```

The workspace module provisions a new S3 bucket and ECR repo; it does not modify the old workspace's bucket. Run history, Athena results, and Glue tables from the v1.x workspace stay in place until you explicitly tear down the old workspace (`clavesa workspace destroy` against the old path).

## What you lose

- **Run history.** Rows in the old `runs` / `node_runs` tables don't migrate.
- **Athena INSERT / MERGE on clavesa-managed tables.** Delta on Athena is read-only. Analyst workflows that used Athena to write into clavesa outputs no longer work. See [ADR-018](../decisions/018-delta-table-format.md) for the full tradeoff.
- **Old Glue table registrations.** v1.x Iceberg tables stay registered in Glue under the old workspace's catalog. They don't interfere with v2.0.0 but aren't cleaned up automatically.

## What stays the same

- Your source data (S3 buckets, HTTP endpoints) is untouched.
- Pipeline `.tf` syntax is unchanged.
- MERGE SQL syntax is identical between Iceberg and Delta — no transform SQL rewrites.
- Athena SELECT against clavesa-managed tables continues to work.

## Gotchas

**Source re-reading costs.** If your source is a large S3 bucket or a high-latency HTTP API, the first v2.0.0 run re-reads everything. For very large CloudFront log archives this is fast (S3 is cheap to scan); for a single large-file source it's one full read.

**Incremental watermarks don't transfer.** v1.x Iceberg snapshot-id watermarks have no equivalent in v2.0.0 Delta version watermarks. The first incremental run after migration reads the full source range from `start_from` (or `"all"` if no `start_from` was set). Subsequent runs advance the Delta version watermark normally.

**Backfill staging tables from v1.x.** Any open `__backfill__<run_id>` staging tables in the old workspace are Iceberg tables the new binary can't read. Promote or discard them before migrating.

## Future tooling

`clavesa workspace migrate-format` is explicitly deferred from v2.0.0. If demand surfaces, it will land in a v2.x minor. File a GitHub issue with your workload shape if recreate-from-source isn't feasible for you.

## See also

- [ADR-018](../decisions/018-delta-table-format.md) — the full decision record, including what clavesa gains and loses.
- [backfill](backfill.md) — for re-loading historical windows after the migration if the source isn't fully re-readable.
