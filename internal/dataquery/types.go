// Package dataquery implements the DATA-QUERY-API: HTTP endpoints for reading
// sample data from S3 sources and querying Iceberg output tables via Athena.
package dataquery

import "github.com/vesahyp/clavesa/internal/graph"

// QueryResult is the response envelope returned by both /data/source and
// /data/table endpoints.
type QueryResult struct {
	Columns   []graph.Column `json:"columns"`
	Rows      [][]string     `json:"rows"`
	RowCount  int            `json:"row_count"`
	Truncated bool           `json:"truncated"`
}
