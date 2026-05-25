// Package lineagetype carries the canonical Edge + Response shapes for
// /pipeline/lineage so internal/api and internal/service can both
// reference one type. Before C12 (2026-05-24 review), each side declared
// its own struct and a bridge in cli/ui.go translated field-for-field —
// pure duplication maintained by hand.
package lineagetype

// Edge is one upstream→downstream relationship in a pipeline DAG.
type Edge struct {
	// FromNode / ToNode are the unsanitized HCL module names. Pipelines
	// can mix - and _ in node ids; preserve what the user wrote.
	FromNode string `json:"from_node"`
	FromType string `json:"from_type"` // "source" | "transform"
	ToNode   string `json:"to_node"`
	ToType   string `json:"to_type"` // "transform" | "destination"

	// ViaTable is the catalog identifier the downstream node reads —
	// "<database>.<table>" — for transform→transform and transform→
	// destination edges where the upstream writes an Iceberg auto-table.
	// Empty for source→transform edges.
	ViaTable string `json:"via_table,omitempty"`

	// FromPipeline / ToPipeline name the producing / consuming pipelines
	// when the edge crosses a pipeline boundary (ADR-016 slice 2).
	// Empty means same pipeline as the one being queried.
	FromPipeline string `json:"from_pipeline,omitempty"`
	ToPipeline   string `json:"to_pipeline,omitempty"`

	// ToTable is the consumer's own output table id —
	// `<consumer_db>.<consumer_node>__default`. Only set when the
	// edge crosses a pipeline boundary downstream.
	ToTable string `json:"to_table,omitempty"`
}

// Response wraps the edges array plus the queried pipeline's own
// ADR-016 namespace (catalog + schema). The UI uses catalog/schema to
// label a node's output table.
type Response struct {
	Edges   []Edge `json:"edges"`
	Catalog string `json:"catalog,omitempty"`
	Schema  string `json:"schema,omitempty"`
}
