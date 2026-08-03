# clavesa sql lint

Parse-check a SparkSQL file; exits non-zero on parse failure

Parse-check a SparkSQL file. Exits 0 with no output on success,
non-zero with the parser's pointer-into-SQL hint on stderr on
failure. Useful in pre-commit hooks and CI to catch SQL typos
before they land in a transform's .tf and cost a Spark cold start
to surface.

Examples:
  clavesa sql lint transforms/enrich.sql
  find . -name '*.sql' -exec clavesa sql lint {} \;

## Usage

```
clavesa sql lint <file>
```

## Global flags

```
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa sql](clavesa_sql.md) — SparkSQL tooling (parse-check, lint)
- [Command index](README.md)
