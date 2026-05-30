package preview

import (
	"strconv"

	"github.com/vesahyp/clavesa/internal/dataquery"
)

// Convert zips the columns and rows from a QueryResult into a PreviewResult.
// Each item is a map[string]interface{} keyed by column name.
// Null/missing column values become nil in the map.
//
// When a column carries a numeric Type ("long" / "double") set by the
// source parser, the string value is parsed back into a typed scalar so
// downstream JSON marshalling ships it as a number, not a string. The
// runner-side `spark.createDataFrame(rows)` then infers the column as
// numeric; SQL predicates like `WHERE amount > 0` work without an
// explicit cast. CSV is the path that depends on this (Go-side parser
// reads everything as string); JSON/Parquet sources leave Type empty
// or non-numeric and fall through to the string emit unchanged.
func Convert(qr *dataquery.QueryResult) *PreviewResult {
	schema := qr.Columns
	items := make([]map[string]interface{}, 0, len(qr.Rows))

	for _, row := range qr.Rows {
		item := make(map[string]interface{}, len(schema))
		for i, col := range schema {
			if i >= len(row) || row[i] == "" {
				item[col.Name] = nil
				continue
			}
			item[col.Name] = coerceValue(row[i], col.Type)
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

// coerceValue parses `s` according to `colType`, falling back to the
// original string when the parse fails (defensive — the parser that set
// `colType` should already have verified every value, but a mid-stream
// schema drift shouldn't crash the preview).
//
// "double" columns are wrapped in a marshaller that always emits a JSON
// number with a decimal point. Plain `float64(150.0)` would marshal to
// `150` via `json.Marshal` — that's a JSON integer, which the Python
// side then loads as `int`. The next row with `0.5` loads as `float`,
// and Spark's `createDataFrame(rows)` fails with
// `[CANNOT_MERGE_TYPE] Can not merge type LongType and DoubleType` when
// it tries to reconcile the per-row inferred schemas. Forcing the
// "always-decimal" emit keeps the whole column on the Python `float`
// path so Spark sees DoubleType uniformly.
func coerceValue(s, colType string) any {
	switch colType {
	case "long":
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v
		}
	case "double":
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return jsonFloat(v)
		}
	}
	return s
}

// jsonFloat is a float64 that marshals as a JSON number with at least
// one digit after the decimal point. See coerceValue for the rationale.
type jsonFloat float64

func (f jsonFloat) MarshalJSON() ([]byte, error) {
	// `strconv.FormatFloat(_, 'g', -1, 64)` produces the shortest
	// round-trippable representation. Compose: if the result lacks a
	// '.' or 'e', append ".0" so the emitted JSON token is unambiguously
	// a float (e.g. `150` → `150.0`, `7.5e10` → `7.5e10`).
	s := strconv.FormatFloat(float64(f), 'g', -1, 64)
	hasDot := false
	hasExp := false
	for _, r := range s {
		if r == '.' {
			hasDot = true
		}
		if r == 'e' || r == 'E' {
			hasExp = true
		}
	}
	if !hasDot && !hasExp {
		s += ".0"
	}
	return []byte(s), nil
}
