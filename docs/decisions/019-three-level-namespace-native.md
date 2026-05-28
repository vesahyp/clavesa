# ADR 019: Native three-level namespace (supersedes ADR 016)

**Status (2026-05-28, revised)**: **Superseded before shipping by reality.** Drafted as the path to retire the `<catalog>__<schema>` flat-encoding kludge from ADR-016. Investigation during implementation found every mechanism for "native three-level Glue Catalog hierarchy on cloud" closed at the AWS API or upstream-engine level:

1. **Spark V2 multi-catalog** (`spark.sql.catalog.<workspace> = io.delta.sql.DeltaCatalog`) NPEs against Delta 4.0/4.1. Delta's `DeltaCatalog` extends `DelegatingCatalogExtension`; Spark only invokes `setDelegateCatalog` for the session catalog, so non-session V2 registrations trip a `NullPointerException` in `DelegatingCatalogExtension.name()`. Open against Delta: [delta-io/delta#2434](https://github.com/delta-io/delta/issues/2434), [delta-io/delta#3312](https://github.com/delta-io/delta/issues/3312).
2. **Hive metastore federation** (`hive.metastore.glue.catalogid`) accepts a 12-digit AWS account ID only — it's the [cross-account](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-glue.html) selector, not a within-account sub-catalog selector. No documented Hive-client path to "write into a non-default named Glue Catalog."
3. **Explicit `glue:CreateTable` against `aws_glue_catalog`** (the alternative we then planned) requires the workspace catalog to exist first; verified during this slice that the [Glue `CreateCatalog` API](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-catalogs.html) rejects every input shape that isn't a Redshift-federated catalog or a resource-link catalog. Direct boto3 returned `InvalidInputException: Create glue native catalog is not supported.` in both `eu-north-1` and `us-east-1`. The Catalog data model exists, the create path for native catalogs doesn't.
4. **Iceberg V2 catalog plugin** is AWS's documented multi-catalog path on Spark, but ADR-018 keeps clavesa on Delta for CDC merge — switching back is not in play.

The original "Decision" section below is preserved unmodified for historical context; the conceptual model (one catalog per workspace, schema owned by one pipeline, three-level addressing) is what we wanted, and might become achievable later when the upstream constraints lift. **For what shipped, see ADR-020 (display normalization).** The `<catalog>__<schema>` flat-encoding from ADR-016 remains the operative wire-format.

Constraint memories: [[project_delta_v2_catalog_blocker]], [[project_glue_multi_catalog_constraints]], [[project_glue_createcatalog_federation_only]].

## Original context (for history)

ADR-016 introduced three-level addressing (`<catalog>.<schema>.<table>`) and noted Glue Data Catalog's namespace was permanently flat, so cloud-side encoded the catalog/schema pair as a single Glue database `<catalog>__<schema>`. That constraint is no longer true.

In late 2024 AWS Glue added a real `Catalog` resource that sits above Database. The `CreateCatalog`, `UpdateCatalog`, `DeleteCatalog` APIs are GA. Lake Formation manages catalog-level permissions. Athena addresses tables as `<catalog>.<database>.<table>` natively in SQL. References:

- [AWS Glue API: Catalog actions](https://docs.aws.amazon.com/glue/latest/webapi/API_Catalog.html)
- [Lake Formation: Creating a catalog in the Data Catalog](https://docs.aws.amazon.com/lake-formation/latest/dg/creating-catalog.html)

The `__` flat encoding is therefore solving a problem that no longer exists. It also leaks. Raw Glue console shows `clavesa_demoworkspace__cloudfront` instead of a clean `clavesa_demoworkspace.cloudfront`, and Athena queries authored outside clavesa have to know the encoding rule. Keeping the workaround past the point of necessity costs more than it saves.

## Decision

**Native three-level addressing across both backends. Drop the `__` flat encoding.**

Concretely:

1. **One real Glue Catalog per workspace** (cloud), created via `aws_glue_catalog`. Default identifier `clavesa_<sanitize(workspace_name)>` as before; the entity is now a real Catalog rather than a name prefix on a flat database.
2. **Glue Database = schema.** Per-pipeline default identifier = `sanitize(pipeline_name)`; pipelines may override via the `schema` attribute, same as ADR-016.
3. **Glue Table = table.** Transform outputs land at the table level. The `__default` suffix for single-output transforms is dropped. A transform writing one output produces `<node>` (not `<node>__default`); multi-output transforms keep `<node>__<key>` to disambiguate keys.
4. **Full identifier on the wire is `<catalog>.<schema>.<table>` in cloud; on local Spark stays on the two-segment `<catalog>__<schema>.<table>` form because Delta 4.0 blocks user V2 catalogs (see Backend boundary below).** The UI and observability surfaces always present the three-level shape; local-mode SQL editors emit the two-segment form so it runs against the engine.

The conceptual rules from ADR-016 carry forward unchanged: one catalog per workspace, one producing pipeline per schema (cross-pipeline reads are first-class, cross-pipeline writes refused), the workspace system catalog `<catalog>_system` for observability.

### Backend boundary

- **Local (per-workspace Derby metastore with bare-schema DB names; on-disk path is V2-shaped from Slice 4).** Delta 4.0's `DeltaCatalog` extends `DelegatingCatalogExtension`; Spark only invokes `setDelegateCatalog` on the session catalog, so a custom V2 registration (`spark.sql.catalog.<workspace_catalog> = io.delta.sql.DeltaCatalog`) trips `NullPointerException: Cannot invoke "CatalogPlugin.name()" because "this.delegate" is null` on every operation. The session catalog stays the writer. From Slice 6 onward Hive databases are renamed from `<catalog>__<schema>` to just `<schema>` (no collision risk inside a workspace's own Derby store); the on-disk warehouse layout is the V2 shape Slice 4 already shipped (`<warehouse>/<catalog>/<schema>/<table>/`). Clavesa SQL inside the workspace is two-part `<schema>.<table>`; the catalog is the implicit workspace context. Re-evaluate when Delta ships a delegate-free V2 implementation.
- **Cloud (`aws_glue_catalog` per workspace; runner registers tables via explicit Glue API calls from boto3 — Spark writes Delta files to S3 via filesystem paths and does not federate to a workspace-scoped Glue catalog via the Hive client).** Two documented mechanisms exist for routing Spark writes into a non-default Glue catalog and neither works: the V2-catalog path (`spark.sql.catalog.<workspace_catalog> = io.delta.sql.DeltaCatalog`) hits the Delta 4.0 NPE noted above, and the Hive-metastore federation path (`hive.metastore.glue.catalogid`) is cross-account-only per the AWS EMR documentation. So the runner decouples Spark write paths from Glue catalog registration: Spark's `saveAsTable` writes Delta data files via filesystem paths (`s3a://<bucket>/...`), then the runner explicitly calls `glue.create_table(CatalogId=<workspace_catalog>, DatabaseName=<schema>, ...)` from boto3 to register the table in the workspace's Glue Catalog. Inputs are pre-registered in an ephemeral in-session Derby metastore via `glue:GetTable` lookups at session start. Athena, dbt, and external boto3 clients see and address the workspace's tables as `<workspace_catalog>.<schema>.<table>` natively — the three-level shape is genuine on the metadata wire, even though Spark's write path stays catalog-implicit.

The provider seam (ADR-014) still owns identifier sanitization. Glue's `[A-Za-z_][A-Za-z0-9_]*` constraint on Catalog, Database, and Table names is identical at every level, so the existing `Sanitize` rule applies uniformly. Display name and identifier remain distinct.

## Migration

**Fresh-write only. No automatic data migration.**

Existing v1.x and v2.0 workspaces stay on the flat-encoded `<catalog>__<schema>` shape; they continue to read and write correctly because Glue databases with `__` in their names are still valid Glue database identifiers. New workspaces created on the post-cutover release ship with real Glue Catalog resources and three-level addressing from the first write.

Users who want the new namespace on an existing workspace recreate it: `clavesa workspace init` against a fresh directory, re-author pipelines (no syntax change at the user level since `catalog` and `schema` attributes already exist), re-run from source. This mirrors the v2.0.0 (ADR-018) migration posture. Clavesa's sources are designed to be re-readable, so a recreate is the cheap path.

The `identutil.EncodeGlueDatabase` helper stays in place for back-compat reads of pre-cutover tables; new writes route through `TableID.Wire()` once Slices 4 through 7 swap call sites. Slice 8 cuts the legacy helper.

## Consequences

**Positive:**
- Native Athena three-part SQL on clavesa-managed tables. No encoding rule to teach external SQL authors.
- Raw Glue console shows clean catalog/database/table hierarchy.
- Lake Formation catalog-level permissions become available without any clavesa-side translation.
- Removes the only piece of clavesa state that diverges visibly between local (nested) and cloud (flat). The two backends now match the user-facing identifier shape byte for byte.

**Negative:**
- Cloud workspaces gain a new top-level resource (`aws_glue_catalog`) with its own IAM and Lake Formation surface. Teardown order matters.
- Workspaces created before the cutover and workspaces created after look different in raw Glue. Documentation has to call this out.
- The Hive metastore federation `catalogid` path is less travelled than the default-catalog path; expect at least one operational gotcha during Slice 6.

**Tradeoffs accepted:**
- We pay the cost of one more cloud resource per workspace in exchange for retiring an encoding rule that affects every observability path, every UI surface, and every Athena query users write outside clavesa.

## References

- ADR 014: Local-cloud parity. The provider seam handles identifier sanitization at every level uniformly; no per-backend translation layer remains.
- ADR 016: Original three-level namespace decision. Superseded on the backend-encoding axis; the conceptual rules carry forward.
- ADR 018: Delta as the table format. Delta's DeltaCatalog provides the local V2 multi-catalog implementation; Delta tables register into Glue via Hive metastore federation in cloud.
- [AWS Glue API: Catalog actions](https://docs.aws.amazon.com/glue/latest/webapi/API_Catalog.html)
- [Lake Formation: Creating a catalog](https://docs.aws.amazon.com/lake-formation/latest/dg/creating-catalog.html)
