// Package preview implements the DATA-PREVIEW API: HTTP endpoints for browsing
// source data items and executing SQL transforms locally via DuckDB.
package preview

import "github.com/vesahyp/clavesa/internal/graph"

// PreviewResult holds a pageable set of data items returned by the preview endpoints.
type PreviewResult struct {
	Items     []map[string]interface{} `json:"items"`
	Schema    []graph.Column           `json:"schema"`
	Total     int                      `json:"total"` // -1 if unknown
	Truncated bool                     `json:"truncated"`
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
}
