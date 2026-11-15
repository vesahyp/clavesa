# clavesa ui

Start the visual editor in your browser

Start the UI server and open it in your browser.

The UI reads and writes .tf files in the workspace directory. It renders
pipelines as interactive DAGs and lets you edit nodes, connect edges,
and preview data visually.

With --workspaces <dir>, one server shows every workspace directly under
<dir> (each directory with a clavesa.json). / lists them, /w/<name>/ is
that workspace's UI and /w/<name>/api/ its API. Each workspace's AWS
clients use its own saved AWS profile; reads that go through the default
credential chain use AWS_PROFILE, so set it when the workspaces share
an account.

--read-only answers 405 to every request except GET and HEAD and hides
the controls that change state. It also refuses the two read POSTs
(the SQL query editor and dashboard widget queries).

Examples:
  clavesa ui
  clavesa ui --workspace /path/to/project
  clavesa ui --no-browser
  clavesa ui --workspaces ~/clavesa-workspaces --read-only

## Usage

```
clavesa ui [flags]
```

## Flags

```
      --no-browser          do not open the browser automatically
      --read-only           answer 405 to every method except GET and HEAD, and hide the controls that change state
      --workspaces string   serve every workspace directly under this directory (multi-workspace mode)
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
