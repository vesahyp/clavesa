// Package preview implements the DATA-PREVIEW API: HTTP endpoints for
// browsing source data items and executing SQL/PySpark transforms through
// the PySpark runner container (ADR-012 — one engine; preview.RunPreview
// shells out to the same image production runs use).
package preview

import (
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/observability"
)

// PreviewResult holds a pageable set of data items returned by the preview endpoints.
type PreviewResult struct {
	Items     []map[string]interface{} `json:"items"`
	Schema    []graph.Column           `json:"schema"`
	Total     int                      `json:"total"` // -1 if unknown
	Truncated bool                     `json:"truncated"`
	// Served identifies the engine + warehouse this preview executed
	// against (ADR-024). Stamped by the HTTP handler at execution time.
	Served *observability.Served `json:"served,omitempty"`
}

// TransformPair holds one input row and the output rows it produced.
type TransformPair struct {
	Input  map[string]interface{}   `json:"input"`
	Output []map[string]interface{} `json:"output"`
}

// TransformPreviewResult is returned by GET /preview/transform.
type TransformPreviewResult struct {
	Pairs []TransformPair `json:"pairs"`
	SQL   string          `json:"sql"`
	// Served — see PreviewResult.Served.
	Served *observability.Served `json:"served,omitempty"`
}
