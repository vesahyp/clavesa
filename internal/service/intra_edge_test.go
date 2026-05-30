package service

import (
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
)

// GH #6: a string-form intra-pipeline ref "<own-schema>.<sibling-table>" lands
// in external_inputs with no edge. reclassifyIntraPipelineEdges must synthesise
// the producer→consumer edge, strip the alias from external_inputs, and (via
// the existing topoSort) order the consumer after the producer — even when the
// consumer's node ID sorts alphabetically BEFORE the producer's (which would
// otherwise put it first under the Kahn smallest-ID tie-break).
func TestReclassifyIntraPipelineEdges_StringRef(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			// Producer "z_src" writes its default (bare) table "z_src".
			{ID: "z_src", Type: "transform"},
			// Consumer "a_consumer" references it as a plain "demo.z_src"
			// string instead of module.z_src.outputs["default"]...
			{ID: "a_consumer", Type: "transform", Config: map[string]interface{}{
				"external_inputs": map[string]interface{}{
					"in": "demo.z_src",
				},
			}},
		},
	}

	reclassifyIntraPipelineEdges(&g, "demo")

	// Exactly one synthetic edge z_src -> a_consumer with alias "in".
	if len(g.Edges) != 1 {
		t.Fatalf("edges = %+v, want exactly one synthetic edge", g.Edges)
	}
	e := g.Edges[0]
	if e.FromNode != "z_src" || e.ToNode != "a_consumer" || e.ToInput != "in" {
		t.Fatalf("synthetic edge = %+v, want z_src->a_consumer in", e)
	}

	// The alias must be gone from external_inputs (and the empty map dropped).
	consumer := g.Nodes[1]
	if _, present := consumer.Config["external_inputs"]; present {
		t.Fatalf("external_inputs still present after reclassification: %+v", consumer.Config["external_inputs"])
	}

	// topoSort now orders the producer before the consumer despite a_consumer
	// sorting first alphabetically.
	order, err := topoSort(&g)
	if err != nil {
		t.Fatalf("topoSort: %v", err)
	}
	zi, ai := indexOfStr(order, "z_src"), indexOfStr(order, "a_consumer")
	if zi < 0 || ai < 0 || zi > ai {
		t.Fatalf("order = %v, want z_src before a_consumer", order)
	}
}

// A string ref to the own schema that matches NO sibling-produced table stays
// in external_inputs (e.g. an externally-created table in the same schema).
func TestReclassifyIntraPipelineEdges_NoSiblingStaysExternal(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "only", Type: "transform", Config: map[string]interface{}{
				"external_inputs": map[string]interface{}{
					"in": "demo.some_external_table",
				},
			}},
		},
	}

	reclassifyIntraPipelineEdges(&g, "demo")

	if len(g.Edges) != 0 {
		t.Fatalf("edges = %+v, want none (no sibling produces the table)", g.Edges)
	}
	ext, _ := g.Nodes[0].Config["external_inputs"].(map[string]interface{})
	if ext["in"] != "demo.some_external_table" {
		t.Fatalf("external_inputs[in] = %v, want the original ref preserved", ext["in"])
	}
}

// intraPipelineProducer guards: a cross-schema ref (genuine cross-pipeline
// read), a self-reference, and a three-part name must not be reclassified.
func TestIntraPipelineProducer_Guards(t *testing.T) {
	g := graph.PipelineGraph{
		Nodes: []graph.Node{
			{ID: "a", Type: "transform"},
			{ID: "b", Type: "transform"},
		},
	}
	produced := producedTableIndex(&g)

	if p, ok := intraPipelineProducer("other.a", "demo", "b", produced); ok {
		t.Fatalf("cross-schema ref reclassified (producer=%q)", p)
	}
	if p, ok := intraPipelineProducer("demo.a", "demo", "a", produced); ok {
		t.Fatalf("self-reference reclassified (producer=%q)", p)
	}
	if p, ok := intraPipelineProducer("cat.demo.a", "demo", "b", produced); ok {
		t.Fatalf("three-part ref reclassified (producer=%q)", p)
	}
	if p, ok := intraPipelineProducer("demo.a", "demo", "b", produced); !ok || p != "a" {
		t.Fatalf("intraPipelineProducer = (%q,%v), want (a,true)", p, ok)
	}
}

func indexOfStr(ss []string, v string) int {
	for i, s := range ss {
		if s == v {
			return i
		}
	}
	return -1
}
