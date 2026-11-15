# The numbers of a `clavesa workspace summary --json` that must be the same
# on the real workspace tree and on its hosted tree copy (ADR-032). Leaves
# out generated_at (the clock) and account (only the copy knows it).
{
  name, kind, status,
  last_run: .last_run.run_id,
  runs_30d: ([.pipelines[].runs | length] | add),
  records_30d, cost_usd_30d, stale_tables,
  pipelines: [.pipelines[] | {name, status, runs: (.runs | length), last_run: .last_run.run_id, error}]
}
