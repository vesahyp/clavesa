// Package dataquery implements the DATA-QUERY-API: HTTP endpoints for reading
// sample data from S3 sources and querying Iceberg output tables via Athena.
package dataquery

import (
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/observability"
)

// QueryResult is the response envelope returned by both /data/source and
// /data/table endpoints.
type QueryResult struct {
	Columns   []graph.Column `json:"columns"`
	Rows      [][]string     `json:"rows"`
	RowCount  int            `json:"row_count"`
	Truncated bool           `json:"truncated"`
	// Served identifies the engine + warehouse that executed the request
	// (ADR-024). Copied verbatim from the provider/service result; absent
	// on responses no engine served (e.g. /data/source, raw S3 reads).
	Served *observability.Served `json:"served,omitempty"`
}
