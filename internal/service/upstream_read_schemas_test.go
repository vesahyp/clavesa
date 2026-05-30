package service

import (
	"reflect"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
)

// TestUpstreamReadSchemas verifies the distinct-upstream-schema derivation
// feeding tfgen's GH #4 cross-pipeline Lake Formation grants: two distinct
// upstream schemas (one referenced twice across two transforms) yield exactly
// one entry per distinct schema, sorted, with the pipeline's own schema
// excluded.
func TestUpstreamReadSchemas(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{
				ID:   "silver",
				Type: "transform",
				Config: map[string]interface{}{
					"external_inputs": map[string]interface{}{
						// distinct upstream schema #1
						"raw": "bronze.cloudfront_raw",
						// own schema — must be excluded
						"sib": "silver.helper_table",
					},
				},
			},
			{
				ID:   "gold",
				Type: "transform",
				Config: map[string]interface{}{
					"external_inputs": map[string]interface{}{
						// distinct upstream schema #2
						"evt": "raw_events.clicks",
						// duplicate ref to upstream schema #1 — must dedupe
						"again": "bronze.cloudfront_raw",
					},
				},
			},
			// non-transform node must be ignored
			{ID: "dest", Type: "destination"},
		},
	}

	got := upstreamReadSchemas(g, "silver")
	want := []string{"bronze", "raw_events"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("upstreamReadSchemas = %v, want %v (distinct, sorted, own-schema excluded)", got, want)
	}
}

// TestUpstreamReadSchemasSanitizes confirms dashed schema identifiers are
// hyphen→underscore sanitized and own-schema matching survives sanitization.
func TestUpstreamReadSchemasSanitizes(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{
				ID:   "t",
				Type: "transform",
				Config: map[string]interface{}{
					"external_inputs": map[string]interface{}{
						"a": "raw-events.clicks", // dashed upstream
						"b": "my-schema.local",   // matches own (dashed)
						"c": "bronze.x",          // plain upstream
					},
				},
			},
		},
	}
	got := upstreamReadSchemas(g, "my-schema")
	want := []string{"bronze", "raw_events"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("upstreamReadSchemas = %v, want %v", got, want)
	}
}

// TestUpstreamReadSchemasEmpty confirms a pipeline with no cross-pipeline
// reads yields an empty slice (tfgen then emits no input_* grants).
func TestUpstreamReadSchemasEmpty(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "only", Type: "transform", Config: map[string]interface{}{}},
		},
	}
	if got := upstreamReadSchemas(g, "silver"); len(got) != 0 {
		t.Fatalf("upstreamReadSchemas = %v, want empty", got)
	}
}
