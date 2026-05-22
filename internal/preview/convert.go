package preview

import "github.com/vesahyp/clavesa/internal/dataquery"

// Convert zips the columns and rows from a QueryResult into a PreviewResult.
// Each item is a map[string]interface{} keyed by column name.
// Null/missing column values become nil in the map.
func Convert(qr *dataquery.QueryResult) *PreviewResult {
	schema := qr.Columns
	items := make([]map[string]interface{}, 0, len(qr.Rows))

	for _, row := range qr.Rows {
		item := make(map[string]interface{}, len(schema))
		for i, col := range schema {
			if i < len(row) && row[i] != "" {
				item[col.Name] = row[i]
			} else {
				item[col.Name] = nil
			}
		}
		items = append(items, item)
	}

	return &PreviewResult{
		Items:     items,
		Schema:    schema,
		Total:     -1,
		Truncated: qr.Truncated,
	}
}
