# ADR 017: Workspace source registry (External Locations)

**Status**: Accepted

## Context

Clavesa's input sources are pipeline-scoped today. Every `module "src_X"` lives inside one pipeline's `main.tf` with its own `bucket` / `prefix` / `format`. Two pipelines reading the same data each declare their own source, copy the same strings, and end up with two separate watermarks, two separate IAM policies, and the obvious drift risk when one of them gets edited and the other doesn't.

The shape we want:

1. **A source declared once at the workspace level**, named, reusable across pipelines. Mirrors Databricks' External Locations, Unity Catalog volumes, and the way clavesa already treats catalogs (ADR-016 — workspace owns the catalog).
2. **Cross-pipeline reuse without copy-paste.** `cloudfront-logs` is *one thing*; pipelines either consume it or don't.
3. **A "where is this used" view.** Today: grep across `main.tf` files.
4. **Workspace-scoped permissions for sources.** One IAM chokepoint, not per-pipeline rederivation.
5. **A natural home for event-driven ingest** (S3 ObjectCreated → fan out to dependent pipelines). Belongs to the source, not duplicated per pipeline.

The CLI `node add --from <url>` work sharpened the gap: HTTP-fetched sources land at a host-local path (`<pipeline>/inputs/<name>/`) that a deployed Lambda can't read. The deeper cause: sources are pinned to whatever path/bucket the user typed, with no abstraction over "where this lives" and no place to express auth or cross-pipeline reuse.

## Decision

**Sources are a workspace-level resource. There is no inline source declaration.** Every input source clavesa reads — every URL, every S3 prefix — is a named entry in a workspace registry. The `module "src_X"` shape is removed from supported authoring.

One mental model, one resolution path in the orchestration emitter, no two-shapes drift.

### Source kinds

The registry abstracts *where raw data lives* into a kind discriminator. Two kinds:

1. **`http`** — A URL. The source IS the URL; nothing is staged. The runner fetches at execution time over HTTPS, optionally with header auth resolved through the credentials registry.
   ```json
   { "kind": "http", "url": "https://d37ci6vzurychx.cloudfront.net/.../yellow_tripdata_2024-01.parquet", "format": "parquet" }
   ```
   ```json
   { "kind": "http", "url": "https://api.stripe.com/v1/events", "format": "json", "credentials": "stripe-api" }
   ```

2. **`s3`** — A bucket + prefix. Runner reads via S3A; same-account workloads use the workspace's IAM. Cross-account reads go through an `assume-role` credential reference.
   ```json
   { "kind": "s3", "bucket": "logs-prod", "prefix": "cf/", "format": "json" }
   ```

**Tables are not a source kind.** A table — produced by another clavesa pipeline, or by an external system (dbt, a hand-rolled Glue crawler, etc.) — lives in the catalog and is addressed `<catalog>.<schema>.<table>`. ADR-016's cross-pipeline-reads already cover this: a transform's `inputs` map accepts table strings directly. The catalog *is* the registry for tables; mirrors Databricks Unity Catalog.

The discriminator extends to new kinds (JDBC, Kafka, REST-with-pagination, webhook receivers, cross-account-or-cross-region Glue tables) per their own ADRs without breaking the registry shape.

### Credentials registry

A separate workspace-level registry at `<workspace>/.clavesa/credentials/<name>.json` holds named credentials that source specs reference by name. Mirrors Databricks' Storage Credentials being separate from External Locations: one credential typically backs many sources (one Stripe API key for several Stripe endpoints, one assume-role for all S3 paths in the partner account), so it lives on its own.

The credential records the *backend* and *reference*, never the secret material itself:

```json
// <workspace>/.clavesa/credentials/stripe-api.json
{
  "kind": "header",
  "header_name": "Authorization",
  "value_prefix": "Bearer ",
  "secret": "env:STRIPE_KEY"
}
```

Three secret backends, identified by URL-style prefix:

- **`arn:aws:secretsmanager:...`** — AWS Secrets Manager. The runner fetches at request time. Native rotation, no redeploy required.
- **`env:VAR_NAME`** — runner reads from its environment. Local-development convenience; cloud-deployed pipelines using `env:` get a clean error at emit time directing the user to `arn:`.
- **`file:<workspace-relative-path>`** — plaintext file gitignored by default. Local-only; refused at emit time for cloud-deployed pipelines.

Credential kinds:

- **`header`** — single HTTP header injection. Covers Bearer tokens and most simple API auth. Multi-header / OAuth / signed-request flows extend the discriminator per their own designs.
- **`assume-role`** — AWS STS AssumeRole, recorded as `role_arn` + optional `external_id`. Sources reference this for cross-account access.

### Workspace IAM

Two roles, one purpose each:

- **`<workspace>__source_reader`** — read access to every registered `s3` source's bucket+prefix. Pipeline transform Lambdas assume it for source reads.
- **`<workspace>__credentials_reader`** — `secretsmanager:GetSecretValue` on every credential whose secret is an `arn:`. Granular per-credential ARN; revoking a credential drops the policy entry.

Catalog table reads continue to use the existing transform IAM, widened per ADR-016.

**No `local-fs` kind.** A registered source must be readable by every compute target the workspace might use. Laptop filesystem paths fail that bar.

### Storage shape

Sources live at `<workspace>/.clavesa/sources/<name>.json` — same authoring location as dashboards. JSON for ergonomics: readable, diffable, machine-writable from the UI without an HCL writer. The orchestration emitter compiles the registry into a workspace-level Terraform tree at `terraform apply` time.

### Reference syntax in pipelines

A transform's `inputs` map accepts two reference shapes, neither of which is an inline module reference:

```hcl
module "agg_revenue" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.20.0"

  inputs = {
    primary   = "sources.cloudfront-logs"   # source-registry reference
    customers = "marketing.dim_customers"   # catalog table reference (ADR-016)
    # raw       = module.src_inline.outputs["default"]   # not a supported shape — inline sources removed
  }
  # ...
}
```

The emitter routes per shape:

- `sources.<name>` → workspace registry lookup; resolves to URL (http) or bucket+prefix (s3); runner switches on kind to read.
- `<schema>.<table>` → catalog table reference (ADR-016); runner reads via the Iceberg catalog. Same mechanism whether the table was produced by another clavesa pipeline or by an external system tagging into the same catalog.

### Authoring surfaces (ADR-015 parity)

CLI and UI are equal-class — both hit the same service layer.

**CLI** — two new top-level nouns, `source` and `credential`, parallel to `pipeline` and `workspace`:

```
clavesa credential register <name> \
  --kind header \
  --header-name Authorization \
  --value-prefix "Bearer " \
  --secret env:STRIPE_KEY                # or arn:aws:secretsmanager:... or file:./stripe.secret

clavesa source register <name> --from <url> [--credentials <cred-name>]
clavesa source register <name> --kind s3 --bucket <b> --prefix <p> --format <f>
```

Symmetric noun verbs:

- `clavesa source list|show|delete|attach`
- `clavesa credential list|show|delete`

Both `delete` actions hit the deletion guard (sources scan pipelines; credentials scan sources).

Catalog table references (`<schema>.<table>`) don't need a separate CLI noun — they're addressed in `node edit` / `node connect` like any other input.

**UI** — two new workspace-level routes (parallel to `/pipelines`, `/dashboards`):

- **`/sources`** — list of registered sources with kind, freshness, "used by N pipelines" count. Detail view shows full spec, dependent pipelines (clickable), "Delete" action with the dependency guard. Source-create form has an optional credential dropdown populated from the credentials registry.
- **`/credentials`** — list of registered credentials with kind, secret backend (`arn:` / `env:` / `file:`), "used by N sources" count. Detail view never shows the secret material itself, only the reference — fetching the value is a runner concern.
- Editor's input picker speaks `sources.<name>` references; the legacy NodePalette's "S3 Source" + "Source from URL" entries collapse into a "Pick / register a source" flow.

### Deletion guards

`clavesa source delete <name>` and the UI delete action both invoke a service-layer scan that walks every pipeline in the workspace looking for `sources.<name>` references. Refuses with the consumer list if any exist. CLI has `--force` for scripted teardown. UI surfaces "used by N pipelines" inline before delete.

The credentials registry has the same guard: `clavesa credential delete <name>` scans every source spec for credential references; refuses if any exist.

### Versioning

A registered source's spec change takes effect on the next `terraform apply` for every dependent pipeline. Same model as workspace-level Terraform shared resources work today. Per-pipeline pinning is not part of the model.

### Triggers

Event-driven ingest (`s3-object-created` → SQS → EventBridge → SFN StartExecution per dependent pipeline) is a property of the source. A source declares `triggers`; the emitter wires fan-out to each dependent pipeline. The pipeline's own schedule becomes one trigger among many.

## Consequences

**Positive:**

- **One canonical home per source.** Drift impossible by construction. "Where is `cloudfront-logs` defined?" has one answer.
- **One source-resolution path** in the orchestration emitter and the runner. No "is this inline or registered" branching. Less code, fewer bugs.
- **Cross-pipeline reuse is mechanical.** No copy-paste. `clavesa source attach pipeline-B cloudfront-logs` and pipeline B reads what pipeline A reads.
- **Cell-completeness for input sources** is settled: `http` works everywhere; `s3` works everywhere AWS creds reach (cred-forwarding is the only outstanding item). Catalog table reads work wherever the catalog is reachable, governed by ADR-016. No source kind has a partial-cell story.
- **Auth'd HTTP sources are first-class** without storing secrets in version control. Stripe / internal APIs / any token-protected endpoint registers cleanly via the credentials registry.
- **Workspace IAM consolidates.** Two purpose-specific roles (`source_reader`, `credentials_reader`), audit-friendly, closes Session F's transform-IAM-overscoped P2 the same way ADR-016 does.
- **`--from` cell-completeness solved without any S3 staging.** The previous draft's "fetch into a workspace bucket" idea was overcomplication; the URL itself is the source.
- **Maps onto Databricks / Unity-Catalog mental model.** Users coming from those systems see "External Locations" + "Storage Credentials" as two registries, exactly as Databricks does.

**Negative:**

- **A flag-day migration for every pipeline that exists.** Bounded today (one production pipeline plus the demo fixture), but real — and growing every week we wait.
- **No-AWS workspaces are `http`-only.** `s3` sources require AWS creds. The previous inline-source model implied "local FS works the same as S3" via `bucket = /path`, which was a half-truth that obscured the cred requirement. Honesty is positive but the README needs to signal it.
- **Two new nouns to learn.** Sources and credentials sit alongside pipelines, transforms, dashboards, and the catalog. Mitigated by the model matching Databricks / Unity Catalog mental shape.

**Tradeoffs accepted:**

- Inline sources gone. The registry is the only path for raw-data sources.
- Tables are catalog references (ADR-016), not sources. The catalog is the registry for tables.
- No per-source versioning. Spec changes apply to all dependents on the next `terraform apply`.
- No `local-fs` kind. Data on a laptop is reachable only by a laptop runner; registry sources must be reachable by every compute target the workspace declares.

## What this changes elsewhere

- **Service layer.** `AddNode` for source type and `AddSourceFromURL` collapse into the new source-registry service methods; both currently emit inline `module "src_X"` blocks, which the registry replaces.
- **Orchestration emitter.** Walks `sources/*.json` and `credentials/*.json`; resolves `sources.<name>` references in pipeline `inputs` maps; emits kind-specific runner config plus the `source_reader` and `credentials_reader` IAM roles. Catalog `<schema>.<table>` references continue to resolve per ADR-016.
- **Runner.** Reads a kind discriminator alongside the inputs/outputs map. New code path for `kind=http` with optional credential resolution (look up secret backend → fetch value → set request header). The existing local-FS path goes away.
- **CLI.** Two new top-level subcommand trees: `source` and `credential`.
- **UI.** Two new workspace-level routes: `/sources` and `/credentials`. NodePalette's source-add affordances become a pick-or-register-a-source flow.
- **Lineage panel.** A `sources.<name>` reference renders an upstream lane distinct from intra-pipeline and cross-pipeline-table edges.
- **CLAUDE.md.** Where-things-live gains `<workspace>/.clavesa/sources/` and `<workspace>/.clavesa/credentials/`. Hard rules: registered sources are the only source-declaration path; secret material never appears in the credentials registry, only references.
- **`.gitignore` template** at `workspace init` excludes `.clavesa/credentials/*.secret`.
- **README quick-start** collapses the source-add ceremony to one line:
  ```
  clavesa source register trips --from https://d37ci6vzurychx.cloudfront.net/.../yellow_tripdata_2024-01.parquet --attach demo
  ```
  An auth'd source picks up one preceding `credential register`.

## Open questions

- **`http` source caching.** A large file fetched on every Lambda cold-start costs real time. Cache shapes (`/tmp` per-invocation, workspace S3 cache, CloudFront in front of the runner) are deferred until a workload demands one.
- **Schema metadata.** A registered source is a natural place to cache inferred column schema next to the spec — clavesa today re-reads the file every preview. Out of scope here; mentioned because the registry shape should leave room for it.
- **Multi-workspace shared registry.** A team with `dev` and `prod` workspaces wants one source defined once. Each workspace owns its own registry today; cross-workspace sharing is its own design problem.

## References

- ADR 014 — Local–cloud parity. The kind discriminator is the chokepoint where local↔cloud resolution happens.
- ADR 015 — CLI / UI parity. `source` / `credential` (CLI) and `/sources` / `/credentials` (UI) speak the same registry through the same service layer.
- ADR 016 — Catalog / schema namespace. Workspace owns the catalog; ADR-017 extends "workspace owns" to raw-data sources. Tables (clavesa-produced or external) are addressed via ADR-016's catalog references — there is no separate "table source" in this ADR.
- Databricks External Locations and Storage Credentials — the inspiration. ADR-017 mirrors the shape: two registries, sources and credentials are separate workspace-level resources.
