# ADR 020: Three-level display normalization (cloud engine stays on ADR-016 encoding)

**Status**: Accepted (2026-05-28). Operative. Co-exists with ADR-016 (whose flat-encoding wire format remains in force).

## Context

ADR-019 attempted to deliver native three-level `<workspace>.<schema>.<table>` addressing across both backends, eliminating the `<catalog>__<schema>` flat encoding from ADR-016. Investigation during implementation found every mechanism closed at the upstream level (Delta V2 catalog NPE, Hive metastore cross-account-only, Glue `CreateCatalog` rejects native catalogs, Iceberg path conflicts with ADR-018). See ADR-019 for the full breadcrumb.

The user-facing pain that motivated ADR-019 (ugly `clavesa_web_traffic__bronze.cloudfront_raw__default` identifiers) is still real. The decision below ships what's achievable today.

## Decision

**Inside clavesa: present three-level. On the wire: stay on ADR-016 flat encoding.**

Concretely:

1. **Wire format unchanged.** Glue Database = `<catalog>__<schema>`. Spark identifier = `<catalog>__<schema>.<table>` (two-segment). Athena SQL outside clavesa = same two-segment.
2. **Clavesa UI presents the three-level shape on every readable surface.** Catalog page (already), TableDetail header chip, lineage labels, dashboard table chips, pipeline editor input picker. The wire form is hidden from display.
3. **Clavesa UI SQL editor emits the wire form** (`<catalog>__<schema>.<table>`) because that's what the engine accepts. Server-side rewriter would close the gap but is out of scope for this ADR; revisit if user pain warrants.
4. **The on-disk warehouse layout is the V2 three-level shape Slice 4 shipped** (`<warehouse>/<catalog>/<schema>/<table>/`). The Hive metastore database name stays the flat-encoded form to match the cloud wire format. Local and cloud read the same shape.
5. **`__default` suffix is dropped for single-output transforms** (Slice 3 shipped this). Multi-output keeps `<node>__<key>`.
6. **API exposes `catalog`, `schema`, `table` as separate fields.** The legacy `database` field (= `<catalog>__<schema>`) stays for one release as a back-compat alias; UI consumes the three-piece form natively, not via `splitDatabase('__')` client-side parsing.

## What this delivers

- User-facing identifier in the clavesa UI: clean `clavesa_web_traffic.bronze.cloudfront_raw` everywhere a name is rendered.
- URLs: three-level (`/tables/<catalog>/<schema>/<table>`) — Slice 3 already.
- Cross-pipeline references: `<schema>.<table>` two-part — ADR-016 unchanged.
- Local warehouse on disk: three-level layout (Slice 4).

## What it does not deliver

- Athena console catalog dropdown does not gain a workspace entry (the data structure for native catalogs doesn't exist at AWS).
- Athena SQL outside clavesa stays on `<catalog>__<schema>.<table>` two-part form.
- dbt / external Spark / raw boto3 see the encoded Glue Database name.
- Spark inside clavesa transforms uses the encoded form in SQL (`SELECT * FROM clavesa_web_traffic__bronze.cloudfront_raw`).

## Revisit conditions

Promote back to ADR-019's full native three-level when any of:

1. AWS Glue `CreateCatalog` adds support for native (non-federated) catalogs. Track [aws/aws-cdk#35019](https://github.com/aws/aws-cdk/issues/35019) and [Glue release notes](https://docs.aws.amazon.com/glue/latest/dg/whatsnew.html).
2. Delta Lake ships a V2 catalog implementation that doesn't extend `DelegatingCatalogExtension`. Track [delta-io/delta#2434](https://github.com/delta-io/delta/issues/2434) and [delta-io/delta#3312](https://github.com/delta-io/delta/issues/3312).
3. AWS publishes a supported Spark+Hive+multi-catalog within-account path.

Any of these unblocks the cloud engine side; ADR-019's path becomes implementable and supersedes this decision.

## References

- ADR 014: Local-cloud parity. Display normalization respects it — same identifier shape rendered both backends.
- ADR 016: Operative wire format. This ADR adds the display layer without changing the encoding.
- ADR 018: Delta as table format. Keeps us off the Iceberg path that AWS's docs treat as the supported multi-catalog mechanism.
- ADR 019: The original three-level-native attempt, superseded-before-shipping.
