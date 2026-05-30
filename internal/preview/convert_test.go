package preview

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/graph"
)

// TestJSONFloatAlwaysHasDecimal pins the property the runner depends on:
// every double-typed value emits a JSON number with a '.' or 'e' so
// Python's json.loads decodes it as float, not int. Without this Spark's
// createDataFrame fails CANNOT_MERGE_TYPE across rows that round to ints
// (e.g. amount=150.00) vs rows that don't (e.g. amount=75.50).
func TestJSONFloatAlwaysHasDecimal(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{150.0, "150.0"},
		{75.50, "75.5"},
		{0.0, "0.0"},
		{-10.0, "-10.0"},
		{1e21, "1e+21"}, // already exponent-shaped — leave alone
	}
	for _, c := range cases {
		got, err := json.Marshal(jsonFloat(c.in))
		if err != nil {
			t.Fatalf("marshal jsonFloat(%v): %v", c.in, err)
		}
		if string(got) != c.want {
			t.Errorf("jsonFloat(%v) marshalled as %q, want %q", c.in, got, c.want)
		}
	}
}

func TestConvert(t *testing.T) {
	tests := []struct {
		name  string
		input *dataquery.QueryResult
		want  *PreviewResult
	}{
		{
			name: "empty result",
			input: &dataquery.QueryResult{
				Columns:   []graph.Column{{Name: "id", Type: "string", Nullable: true}},
				Rows:      [][]string{},
				RowCount:  0,
				Truncated: false,
			},
			want: &PreviewResult{
				Items:     []map[string]interface{}{},
				Schema:    []graph.Column{{Name: "id", Type: "string", Nullable: true}},
				Total:     -1,
				Truncated: false,
			},
		},
		{
			name: "single row",
			input: &dataquery.QueryResult{
				Columns: []graph.Column{
					{Name: "order_id", Type: "string", Nullable: false},
					{Name: "amount", Type: "string", Nullable: false},
				},
				Rows:      [][]string{{"ORD-001", "99.99"}},
				RowCount:  1,
				Truncated: false,
			},
			want: &PreviewResult{
				Items: []map[string]interface{}{
					{"order_id": "ORD-001", "amount": "99.99"},
				},
				Schema: []graph.Column{
					{Name: "order_id", Type: "string", Nullable: false},
					{Name: "amount", Type: "string", Nullable: false},
				},
				Total:     -1,
				Truncated: false,
			},
		},
		{
			name: "multiple rows",
			input: &dataquery.QueryResult{
				Columns: []graph.Column{
					{Name: "name", Type: "string", Nullable: true},
					{Name: "val", Type: "string", Nullable: true},
				},
				Rows: [][]string{
					{"alpha", "1"},
					{"beta", "2"},
					{"gamma", "3"},
				},
				RowCount:  3,
				Truncated: true,
			},
			want: &PreviewResult{
				Items: []map[string]interface{}{
					{"name": "alpha", "val": "1"},
					{"name": "beta", "val": "2"},
					{"name": "gamma", "val": "3"},
				},
				Schema: []graph.Column{
					{Name: "name", Type: "string", Nullable: true},
					{Name: "val", Type: "string", Nullable: true},
				},
				Total:     -1,
				Truncated: true,
			},
		},
		{
			name: "numeric column types coerce string cells to typed scalars",
			input: &dataquery.QueryResult{
				Columns: []graph.Column{
					{Name: "id", Type: "string", Nullable: false},
					{Name: "qty", Type: "long", Nullable: false},
					{Name: "amount", Type: "double", Nullable: false},
				},
				Rows:      [][]string{{"ORD-1", "3", "75.50"}},
				RowCount:  1,
				Truncated: false,
			},
			want: &PreviewResult{
				Items: []map[string]interface{}{
					{"id": "ORD-1", "qty": int64(3), "amount": jsonFloat(75.50)},
				},
				Schema: []graph.Column{
					{Name: "id", Type: "string", Nullable: false},
					{Name: "qty", Type: "long", Nullable: false},
					{Name: "amount", Type: "double", Nullable: false},
				},
				Total:     -1,
				Truncated: false,
			},
		},
		{
			name: "null handling — empty string becomes nil",
			input: &dataquery.QueryResult{
				Columns: []graph.Column{
					{Name: "id", Type: "string", Nullable: true},
					{Name: "note", Type: "string", Nullable: true},
				},
				Rows:      [][]string{{"123", ""}},
				RowCount:  1,
				Truncated: false,
			},
			want: &PreviewResult{
				Items: []map[string]interface{}{
					{"id": "123", "note": nil},
				},
				Schema: []graph.Column{
					{Name: "id", Type: "string", Nullable: true},
					{Name: "note", Type: "string", Nullable: true},
				},
				Total:     -1,
				Truncated: false,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Convert(tc.input)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Convert() =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}
}
