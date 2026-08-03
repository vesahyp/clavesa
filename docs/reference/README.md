# Reference

Look-up material: exact, information-oriented, no narrative. This is the
[Diátaxis](https://diataxis.fr) *reference* quadrant — the place to find *what a
thing is*, not how to learn it (see the [quick-start](../../README.md#quick-start)
for that) or how to accomplish a task (see the [cookbook](../cookbook/README.md)).

- **[CLI reference](cli/README.md)** — every `clavesa` command, its flags, and
  its usage. **Generated** from the command tree by `make docs-cli`; a Go test
  (`internal/clidocs`) fails the build if this directory drifts from the CLI, so
  it can't silently rot. Don't hand-edit files under `cli/` — change the command
  in `internal/cli/` and regenerate.

Module input/output reference lives with each module
([transform](../../modules/transform/aws/README.md),
[source](../../modules/source/aws/README.md),
[destination](../../modules/destination/aws/README.md)).
