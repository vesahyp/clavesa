# clavesa workspace summary

Show the workspace health summary

Show the health of the workspace: its status, the last run, runs per
day, tables past their freshness_sla and the compute cost of the last 30
days.

The status is the worst of: the last run of a pipeline failed (fail), a
table is past its freshness_sla (warn), a pipeline with a trigger_schedule
did not run inside its expected interval (warn).

Days are counted, and times written, in one time zone: $CLAVESA_TIME_ZONE
when set, else "time_zone" in clavesa.json (an IANA name such as
"Europe/Helsinki"), else the machine's local zone. The UI shows days in
the same zone, so the 14 and 30 day counts agree.

With --publish, the summary is also written to the hosted UI's data
bucket (summaries/<workspace>.json, ADR-032) through the hosted UI's
publisher role, which the workspace's own AWS identity assumes. The
bucket and the role come from <hosted url>/.well-known/clavesa.json, so
the workspace needs a hosted block (clavesa workspace hosted enable). The
role writes only the names registered as local-warehouse workspaces in
the hosted UI's hosted/infra/workspaces.tf.

  clavesa workspace summary
  clavesa workspace summary --json
  clavesa workspace summary --publish

## Usage

```
clavesa workspace summary [flags]
```

## Flags

```
      --json      output as JSON
      --publish   also write the summary to the hosted UI's data bucket (ADR-032)
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
