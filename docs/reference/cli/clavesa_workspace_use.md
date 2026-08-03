# clavesa workspace use

Switch the current workspace, or set its warehouse / AWS profile

With <path>, record it as the current workspace — subsequent commands
without --workspace and without $CLAVESA_WORKSPACE resolve to it. The
selection is stored in $XDG_CONFIG_HOME/clavesa/current-workspace
(default: ~/.config/clavesa/current-workspace).

With --warehouse, set where all workspace state lives:

  - local: author and run against the local runner + Hadoop catalog.
  - cloud: operate the deployed pipeline (Step Functions, Glue, Athena).

The warehouse is stored per-workspace in .clavesa/environment.json
(gitignored) and defaults to "local". It drives local-vs-cloud dispatch
for pipeline runs and the observability surfaces. (--env is the
deprecated alias.)

With --profile, set the AWS profile the workspace operates as — the
profile `clavesa ui` resolves AWS credentials from, and forwards
into the runner for S3-source reads. Stored in
.clavesa/aws-profile.json (gitignored). Pass an empty value
(--profile "") to clear the override and fall back to the ambient
AWS_PROFILE / default credential chain. The profile must exist in
~/.aws; a running `clavesa ui` server picks the change up on its
next start.

Run with no arguments to print the current workspace, warehouse, and AWS profile.

## Usage

```
clavesa workspace use [path] [flags]
```

## Flags

```
      --profile string     set the AWS profile (must exist in ~/.aws); "" clears the override
      --warehouse string   set the workspace warehouse: "local" or "cloud"
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace](clavesa_workspace.md) — Manage workspaces
- [Command index](README.md)
