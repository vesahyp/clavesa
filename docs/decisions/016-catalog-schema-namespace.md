# ADR 016: Catalog / Schema / Table Namespace

**Status**: Accepted (operative). ADR-019 attempted to supersede but was itself superseded-before-shipping when investigation found the path to a native Glue Catalog blocked at the AWS API level and the V2 Spark catalog blocked by Delta. See ADR-019 for that history. ADR-020 documents the display-normalization-only follow-on. The `<catalog>__<schema>` flat-encoding described below remains the wire format clavesa uses.

## Context

Clavesa's table namespace today is two levels, flat:

- **Glue Data Catalog** (the AWS metastore — one per account) acts as our implicit catalog. Locally, the Hadoop catalog under `<workspace>/.clavesa/warehouse/` is the equivalent.
- **Glue database** per pipeline: `clavesa_<pipeline>`.
- **Table** per output: `<pipeline>.<node>__<key>`.

The industry baseline is three levels: `catalog.schema.table`. Unity Catalog, Snowflake, BigQuery, dbt, and current versions of Spark all use this shape. Clavesa's flat model is adequate for a single-pipeline workspace running in one environment, but several real questions have no clean answer in it:

1. **Multi-environment.** Dev / staging / prod against the same workspace — today the only path is separate AWS accounts or `_dev`/`_prod` suffixes on every database.
2. **Domain organization.** Two pipelines that conceptually belong together (`marketing.web_sessions`, `marketing.email_events`) have no shared home; each lives in its own `clavesa_<pipeline>` database.
3. **Bronze/silver/gold layering.** Every output lands at one level regardless of refinement stage. Sub-namespaces (`<pipeline>_bronze` / `<pipeline>_silver`) work as a convention but are unenforced.
4. **Cross-pipeline reads.** A `dim_customers` produced by pipeline A and consumed by pipeline B has no first-class home — pipeline B reaches across to pipeline A's database. Works mechanically, feels wrong, lineage doesn't follow.

An open question previously surfaced as *"what is a 'dataset'? Letting users group across pipelines (`marketing`, `infra`) is more work but matches how teams think."* That open question is the catalog/schema question.

### Why now

The active focus is CLI/UI authoring parity (ADR-015) plus first real cloud workloads via cloudfront-analytics. New authoring affordances and any cost-allocation work (the run-cost rollup TODO) get cleaner if the namespace is decided first. Adding catalog/schema later as a retrofit means re-emitting every fixture, rewriting every observability path that hard-codes `clavesa_<pipeline>`, and rebuilding the Catalog page's iteration. Decide once now while there's still one production user.

### Three options considered

- **A. Pipeline = schema, workspace = catalog.** Promote the implicit `clavesa` prefix to a real workspace-level catalog name. Multi-environment = multiple workspaces, one catalog each. Domain grouping unsolved.
- **B. User-defined catalogs and schemas per pipeline.** Each pipeline declares both `catalog` and `schema`. Two pipelines can share either. More knobs, possible naming collisions, no clean unit-of-deployment story.
- **C. Decouple table location from pipeline ownership.** Tables are first-class; pipelines are jobs that produce them. Closest to dbt. Inverts the current mental model; rewrites lineage, the editor, the orchestration emitter.

## Decision

**Adopt A for the workspace ↔ catalog edge, B for the pipeline ↔ schema edge.** Skip C.

Concretely:

1. **One catalog per workspace.** The workspace declares a catalog name (default `clavesa`). Multi-environment is achieved by multiple workspaces, each with its own catalog (`clavesa_dev`, `clavesa_prod`). A pipeline cannot pick a catalog the workspace doesn't own — workspaces already represent a unit of deployment (one S3 bucket, one ECR repo, one set of AWS creds), and the catalog stays attached to that unit.

2. **Pipeline owns its schema, with rename override.** A pipeline's default schema identifier is its sanitized name; setting `schema = "marketing"` lets a pipeline produce into a domain-named schema rather than a pipeline-named one. The override is for *naming*, not for sharing — a schema is owned by exactly one producing pipeline (see §5). Schemas materialize as Glue databases (cloud) or nested Iceberg namespaces (local); the provider seam handles the encoding.

3. **Tables are addressed three-level: `<catalog>.<schema>.<table>`.** Transform outputs land at `<workspace_catalog>.<pipeline_schema>.<node>__<key>`. The translation to backend-specific names happens at the provider seam (see "Backend boundary").

4. **Cross-pipeline reads are first-class.** A transform's `inputs` map can reference `<schema>.<table>` strings, not only `module.<other>.outputs[…]`. Lineage, the input picker, and IAM all follow the same rule.

5. **Cross-pipeline writes are not supported — full schema ownership.** A schema is owned by exactly one producing pipeline; readers are unconstrained. Pipeline B cannot write *any* table into a schema pipeline A owns, even if the tables don't collide. Two pipelines sharing a schema with disjoint tables would still create ambiguous lineage and "who broke it" diagnostics that aren't worth designing for yet — Iceberg supports concurrent writers mechanically, but the *user model* for them isn't a today problem. Clavesa's orchestration emitter rejects any pipeline configuration that writes into a schema another pipeline in the same workspace already owns; the user resolves it by choosing a different `schema` for one of the two.

### Backend boundary

The provider seam (per ADR-014) handles the namespace translation:

- **Cloud (Glue Data Catalog).** Glue databases have flat names. Encode `<catalog>.<schema>` as a single Glue database `<catalog>__<schema>` and translate at the provider. The UI sees three-level names; the metastore sees flat ones.
- **Local (Hadoop catalog).** Iceberg's Hadoop catalog supports nested namespaces natively (`/warehouse/<catalog>/<schema>/<table>/`). No translation needed.

The translation cost is paid once at the provider boundary; consumers (UI, lineage, dashboards) see uniform three-level names regardless of backend.

### Defaults preserve current behavior

A pipeline that declares neither `catalog` nor `schema` produces tables at exactly the same backend names as today (`clavesa_<pipeline>.<table>` in Glue), because the workspace catalog defaults to `clavesa` and the pipeline schema defaults to `<pipeline>`. The new framing is "what was already happening, named." Migration is opt-in per workspace and per pipeline; no flag day.

### What works, what doesn't

**Free in this model (the SQL works the moment a transform points at the table):**

- Cross-pipeline reads via SparkSQL (`SELECT * FROM marketing.dim_customers`).
- Multi-environment via parameterized catalog (`catalog = var.environment`).
- Bronze/silver/gold via per-output schema overrides within one pipeline.
- Reading external (non-clavesa-produced) tables in the same catalog.

**Bounded additional work, listed by required slice:**

- **Lineage across pipelines.** The lineage resolver iterates over all pipelines in the workspace and links a transform's input table to whichever transform (in any pipeline) writes it. Builds on the workspace-pipeline-discovery walk Session A flagged for consolidation.
- **Input picker in the editor.** Adds a second mode beyond "nodes in this pipeline's graph": "tables in this workspace produced by other pipelines" (Glue `ListTables` + workspace pipeline scan).
- **IAM widening.** Transform Lambda needs read access to the warehouse bucket, not just its own pipeline prefix. Session F's P2 already flagged the over-scoped IAM; this lands the fix at the same time.
- **Schema metadata for external inputs.** Editor reads Iceberg metadata directly to show columns; we already do this for the Catalog page.

**Optional follow-ups, not load-bearing:**

- **Cross-pipeline orchestration triggers.** Pipeline A finishes → EventBridge rule starts pipeline B. Today's per-pipeline Step Function model supports this as a new event source. Simpler answer for now: pipeline B is scheduled and reads whatever's there.

### Workspace system catalog (resolves the §"System database" open question)

Observability tables — today per-pipeline `<catalog>.<pipeline>.runs|node_runs|tables` — move to a **separate workspace-owned catalog** named `<workspace_catalog>_system` (default `<DefaultCatalog>_system`, e.g., `clavesa_demo_ws_system`). Schemas inside it group by domain mirroring Databricks Unity Catalog's account-level `system` catalog:

| Schema | Tables | Status |
|---|---|---|
| `pipelines` | `runs`, `node_runs`, `tables` | This ADR's relocation slice |
| `query` | `history` | Future — `query_history` TODO |
| `billing` | `run_costs` | Future — `run_costs` TODO |
| `access` | `audit` | Future — once clavesa goes multi-user |

The system catalog is **owned by the workspace, not by any pipeline**. Every pipeline appends to `<workspace>_system.pipelines.*`; the per-pipeline filter is the `pipeline` column on each row (already present in today's schemas). This is a deliberate exemption from §5's "one pipeline per schema" rule — the Slice 4 schema-ownership validator must skip catalogs whose identifier equals the workspace's system catalog. Rationale: forcing the multi-writer rollup into a per-pipeline schema would either fragment the data (one `runs` table per pipeline, requiring UNION queries for any workspace-wide view) or require a managed Glue View over them. A single workspace-owned write target is operationally simpler and matches the Databricks pattern users coming from Unity Catalog will recognize.

This resolves three concerns at once:
- User pipeline schemas (`<catalog>.<pipeline>`) now contain only the user's actual outputs — Catalog page shows business tables, not metadata clutter.
- The `run_costs` and `query_history` TODOs get a natural home (`<workspace>_system.billing.run_costs`, `<workspace>_system.query.history`) instead of being retrofitted into per-pipeline schemas.
- IAM separation is clean: a future `<workspace>__system_reader` role gets perms on the system catalog only.

## Consequences

**Positive:**

- **Multi-environment is real.** Same `.tf`, parameterized `catalog`, deploy to two workspaces — `prod` and `dev` coexist in the same Glue Data Catalog without per-resource suffixing.
- **Domain organization is real.** Pipelines that belong together share a schema; teams thinking in `marketing` / `infra` / `growth` get those terms as first-class.
- **Cross-pipeline sharing is mechanical, not awkward.** A `dim_customers` consumed by three downstream pipelines has one canonical home. Lineage tracks it.
- **Cost-allocation tags get richer.** The tagging TODO grows two new keys (`clavesa:catalog`, `clavesa:schema`) for free; Cost Explorer can pivot by environment and domain.
- **Industry-standard mental model.** Users coming from Snowflake / BigQuery / Databricks see the namespace they expect. Docs simplify.
- **Backwards compat is automatic.** Pipelines that don't set the new fields produce tables at exactly the same backend names as today.

**Negative:**

- **Translation layer at the Glue boundary.** Cloud's Glue databases have flat names; we encode `<catalog>__<schema>` and translate. Real but bounded; one place, well-tested.
- **Schema ownership invariant has to be enforced.** "A schema is owned by exactly one producing pipeline" is a constraint the orchestration emitter and `pipeline create` / `pipeline upgrade` must check at config-validation time. Rejecting a shared-schema configuration is a small new validator that walks the workspace's pipelines and looks for collisions on the resolved `<catalog>.<schema>` pair.
- **Lineage / picker / IAM follow-on slices.** Cross-pipeline reads work mechanically before these land, but the affordances are not first-class until they ship.
- **Two new optional fields per pipeline.** More surface for the user to learn. Mitigated by sensible defaults — most users won't set either.

**Tradeoffs accepted:**

- Workspace as the catalog boundary, even for users who'd want per-pipeline catalogs. The unit-of-deployment argument wins; if it doesn't, we revisit.
- No cross-pipeline writes. Iceberg supports them; the user model isn't worth building today. Revisit if a real workload demands it.
- Three-level addressing in Glue via flat-name encoding. Slight ugliness in raw Glue console; users see clean three-level names everywhere clavesa owns the surface.

## What this changes elsewhere

- **Workspace authoring.** `workspace init` learns a `--catalog` flag (default `clavesa`). Existing workspaces migrate by setting the catalog explicitly in their `clavesa.json` — backwards compat via the default.
- **Pipeline authoring.** Pipeline `.tf` learns an optional `schema` variable; default = pipeline name. CLI `pipeline create --schema <name>`; UI per ADR-015 mirrors this with a schema field.
- **Transform output paths.** `<catalog>.<schema>.<node>__<key>`, replacing today's `clavesa_<pipeline>.<node>__<key>`. Default values reproduce current backend names.
- **Catalog page.** Three-level breadcrumbs (`<catalog> / <schema> / <table>`); group tables by schema. Empty schemas not rendered.
- **Lineage panel.** Walks across pipelines in the workspace; cross-pipeline edges shown distinctly from intra-pipeline ones (different stroke color, hover shows producing pipeline).
- **Input picker in the editor.** Two modes: "nodes in this pipeline" and "tables in this workspace from other pipelines" (with the latter showing producing pipeline as metadata).
- **IAM (transform module).** Read access to the workspace warehouse bucket, scoped to the workspace catalog; writes still pinned to the producing pipeline's prefix. Resolves Session F's P2 about over-scoped IAM.
- **Cost-allocation tagging (ADR-014 follow + tagging TODO).** Add `clavesa:catalog` and `clavesa:schema` tag keys.
- **Observability tables location.** Move from per-pipeline `<catalog>.<pipeline>.runs|node_runs|tables` to a workspace-owned `<workspace_catalog>_system` catalog (`pipelines` schema). See the "Workspace system catalog" section above for the full shape.
- **CLAUDE.md.** Update "where things live" + the hard-rule list to reference this ADR. Note that transforms address tables three-level.

## Open questions

- **Schema rename.** A pipeline switches `schema = "marketing"` → `schema = "growth"`. Existing tables in `marketing` don't move. We probably refuse the rename when the pipeline has produced tables, and document a manual migration. Same answer as ADR-014 gave for `compute` switches.
- **External tables in the same catalog.** Clavesa doesn't create them, but a workspace's catalog might contain tables produced by other tools (a hand-rolled Glue crawler, an upstream dbt model). Treating these as first-class read inputs is consistent with the principle. The picker should show them; the lineage panel marks them "external (no producer)."
- **Catalog name collisions across workspaces.** Two workspaces with `catalog = "clavesa"` writing to the same Glue Data Catalog will conflict on schema names. The default is unique by accident today (one workspace per machine); for multi-workspace setups, the workspace name probably wants to be the catalog default. Decision: catalog defaults to `clavesa_<workspace_name>`, can be overridden, must be unique within the AWS account.
- **Spark catalog configuration.** PySpark needs `spark.sql.catalog.<name>` configured per request. Today we hard-code `clavesa`; the runner needs to read the workspace catalog name from env (`CLAVESA_CATALOG`) and configure SparkSession accordingly.

## References

- ADR 012 — PySpark as universal execution engine.
- ADR 013 — Iceberg as the table format. Iceberg's hierarchical namespace is what makes this ADR cheap.
- ADR 014 — Local–cloud parity. The provider seam handles the catalog/schema translation between Glue (flat) and Hadoop (hierarchical).
- ADR 015 — CLI / UI parity. Both surfaces expose `catalog` and `schema` as authoring fields.
