# clavesa runner

Manage the PySpark runner image (Python deps, etc.)

Manage the workspace-level PySpark runner image.

The runner is the container that executes transforms (local, Lambda,
Fargate, EMR Serverless). "requirements" declares extra Python packages
(e.g. for PySpark UDFs) that get pip-installed into the image at build
time.

Examples:
  clavesa runner requirements list
  clavesa runner requirements add "pyasn>=1.6"
  clavesa runner requirements remove pyasn
  clavesa runner requirements import requirements.txt
  clavesa runner requirements show

## Usage

```
clavesa runner
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa runner requirements](clavesa_runner_requirements.md) — Manage extra Python packages baked into the runner image

## See also

- [clavesa](clavesa.md) — Visual ETL for Terraform pipelines
- [Command index](README.md)
