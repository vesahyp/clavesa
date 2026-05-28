// Package identutil holds the single rule for turning a user-typed display
// name (workspace, pipeline, node) into a SQL identifier safe for the Glue
// Data Catalog and Iceberg's Hadoop catalog.
//
// Glue database names accept [A-Za-z_][A-Za-z0-9_]* only — dashes are not
// allowed. Display names (user-typed, kept in clavesa.json / .tf) can
// contain dashes; the identifier used by the backend is derived. The same
// rule applies at every level of the three-level namespace introduced by
// ADR-016: catalog (default clavesa_<sanitize(workspace_name)>), schema
// (default sanitize(pipeline_name)), and table-suffix node id (already
// sanitized at the runner via runner.py:_table_id_for).
//
// ADR-019 makes that three-level shape native end-to-end (`<catalog>.<schema>.<table>`)
// and retires the flat `<catalog>__<schema>` Glue-database encoding. The
// canonical type is TableID below; EncodeGlueDatabase and
// EncodeExternalTableRef stay for back-compat reads of pre-ADR-019 tables
// until Slice 8 cuts them.
//
// One helper, one rule. Keep this package free of imports beyond stdlib.
package identutil

import (
	"fmt"
	"strings"
)

// Sanitize maps a display name to an identifier safe for Glue / Hadoop
// catalog use. Today the only transformation is dashes → underscores —
// the same operation runner.py:_table_id_for performs for table names.
// Extend conservatively: any new transform applies to all three layers
// (catalog, schema, table) at once or we drift back into the duplication
// this package exists to prevent.
func Sanitize(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}

// EncodeGlueDatabase translates an ADR-016 (catalog, schema) pair into a
// flat Glue Data Catalog database name. Glue databases are a flat
// namespace; the level boundary between catalog and schema is encoded
// with the `__` separator (same convention `<node>__<key>` already
// uses for the node↔key boundary in table names). Single-underscore
// stays reserved for sanitized in-name dashes — `__` always means
// "level boundary the backend forced flat."
//
// Both inputs are required as of v0.18.0. The pre-ADR-016 legacy
// fallback (catalog == "" → `clavesa_<schema>` literal) was
// removed once the only production user (cloudfront-analytics)
// migrated. Pipelines pinning to pre-v0.18 module refs continue to
// work via that older module's local fallback; new module versions
// emit `<catalog>__<schema>` uniformly.
//
// Inputs are sanitized defensively even though Init persists already-
// sanitized identifiers — pipelines may have hand-edited .tf with a
// dashed schema and the runner shouldn't blow up at write time.
func EncodeGlueDatabase(catalog, schema string) string {
	return Sanitize(catalog) + "__" + Sanitize(schema)
}

// TableID is the three-level (catalog, schema, table) identifier that
// ADR-019 makes the canonical addressing shape. Slices 4-7 swap call
// sites from EncodeGlueDatabase to TableID.Wire(); EncodeGlueDatabase
// stays for back-compat reads of pre-cutover tables.
type TableID struct {
	Catalog string
	Schema  string
	Table   string
}

// Wire returns the native three-level identifier <catalog>.<schema>.<table>.
// Spark V2 multi-catalog and Athena's three-part SQL form both consume
// this shape directly. Inputs are sanitized defensively (same posture as
// EncodeGlueDatabase).
func (t TableID) Wire() string {
	return Sanitize(t.Catalog) + "." + Sanitize(t.Schema) + "." + Sanitize(t.Table)
}

// LegacyGlueDB returns the flat <catalog>__<schema> Glue database name
// used pre-ADR-019. Kept for back-compat read paths until Slice 8 cutover.
// New code should not write this form.
func (t TableID) LegacyGlueDB() string {
	return EncodeGlueDatabase(t.Catalog, t.Schema)
}

// EncodeExternalTableRef translates an ADR-016 cross-pipeline reference
// `<schema>.<table>` into the runner Delta table identifier
// `<catalog>__<schema>.<table>`. The Delta catalog (ADR-018) lives under
// Spark's default `spark_catalog`, so the identifier is the bare
// two-segment `<db>.<table>` form — no leading catalog prefix. The
// database segment is the flat-encoded (catalog, schema) pair. Both the
// cloud orchestration emitter and the local pipeline-run path resolve
// cross-pipeline inputs through this so the two surfaces can't drift.
// Errors when `ref` lacks the `.` separator.
func EncodeExternalTableRef(catalog, ref string) (string, error) {
	dot := strings.Index(ref, ".")
	if dot < 0 {
		return "", fmt.Errorf("malformed cross-pipeline reference %q (want <schema>.<table>)", ref)
	}
	schema, table := ref[:dot], ref[dot+1:]
	return EncodeGlueDatabase(catalog, schema) + "." + table, nil
}
