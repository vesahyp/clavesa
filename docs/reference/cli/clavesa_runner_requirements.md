# clavesa runner requirements

Manage extra Python packages baked into the runner image

Manage the extra Python packages installed into the runner image.

These are pip-installed at runner build time, on top of the baseline
PySpark + Delta + AWS stack. Use them for transform UDF dependencies.

## Usage

```
clavesa runner requirements
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## Subcommands

- [clavesa runner requirements add](clavesa_runner_requirements_add.md) — Add a runner requirement (pip spec)
- [clavesa runner requirements import](clavesa_runner_requirements_import.md) — Replace all runner requirements with the contents of a file
- [clavesa runner requirements list](clavesa_runner_requirements_list.md) — List the extra runner requirements
- [clavesa runner requirements remove](clavesa_runner_requirements_remove.md) — Remove a runner requirement
- [clavesa runner requirements show](clavesa_runner_requirements_show.md) — Show the raw requirements file (exactly what gets installed)

## See also

- [clavesa runner](clavesa_runner.md) — Manage the PySpark runner image (Python deps, etc.)
- [Command index](README.md)
