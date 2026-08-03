# clavesa dashboards render

Execute every widget's dataset and print the results

Execute a dashboard — runs each widget's bound dataset SQL and
prints the results. Datasets shared by multiple widgets execute once.

Useful for cron / CI smoke tests: a non-zero exit means at least one
widget's query failed.

Pass dashboard control values with --param key=value (repeatable). Keys
not provided fall back to each control's declared default — a
time_range with default "last_30d" expands to {start, end} at "now".
For a time_range control named "tr", the two keys are "tr.start" and
"tr.end"; for a select control, the key is the control name.

## Usage

```
clavesa dashboards render <slug> [flags]
```

## Flags

```
      --json                output as JSON
      --param stringArray   control value as key=value (repeatable; e.g. --param tr.start=2026-01-01)
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa dashboards](clavesa_dashboards.md) — List, inspect, render, and author workspace dashboards
- [Command index](README.md)
