package preview

import (
	"reflect"
	"testing"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/graph"
)

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
