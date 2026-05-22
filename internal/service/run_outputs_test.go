package service

import (
	"reflect"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
)

// TestBuildLocalOutputsMergeKeysImpliesMergeMode covers the contract
// merge-dim-table.md relies on: an output declaring merge_keys but no
// explicit mode is a MERGE write, not a replace. Without this default,
// the runner saw mode="replace" and ran createOrReplace, leaving the
// table's snapshot history as a series of `append +N` ops instead of
// the COW-merge overwrites the recipe documents.
func TestBuildLocalOutputsMergeKeysImpliesMergeMode(t *testing.T) {
	node := &graph.Node{
		ID:   "dim_customers",
		Type: "transform",
		Config: map[string]interface{}{
			"output_definitions": map[string]interface{}{
				"default": map[string]interface{}{
					"merge_keys": []interface{}{"customer_id"},
				},
			},
		},
	}

	out := buildLocalOutputs(node, "clavesa.demo.dim_customers__default")
	def, ok := out["default"].(map[string]any)
	if !ok {
		t.Fatalf("default output not a dict: %T", out["default"])
	}
	if def["mode"] != "merge" {
		t.Errorf("mode = %v, want \"merge\"", def["mode"])
	}
	if !reflect.DeepEqual(def["merge_keys"], []string{"customer_id"}) {
		t.Errorf("merge_keys = %v, want [customer_id]", def["merge_keys"])
	}
	if def["table_id"] != "clavesa.demo.dim_customers__default" {
		t.Errorf("table_id = %v, want demo table id", def["table_id"])
	}
}

// TestBuildLocalOutputsExplicitModeWins guards against the new default
// stomping a user-declared mode. `mode = "append" + merge_keys = [...]`
// is a legal shape — at-least-once dedup on appends.
func TestBuildLocalOutputsExplicitModeWins(t *testing.T) {
	node := &graph.Node{
		ID:   "events",
		Type: "transform",
		Config: map[string]interface{}{
			"output_definitions": map[string]interface{}{
				"default": map[string]interface{}{
					"mode":       "append",
					"merge_keys": []interface{}{"event_id"},
				},
			},
		},
	}
	out := buildLocalOutputs(node, "clavesa.demo.events__default")
	def := out["default"].(map[string]any)
	if def["mode"] != "append" {
		t.Errorf("explicit mode=append lost: got %v", def["mode"])
	}
}

// TestBuildLocalOutputsBareReplace keeps the legacy fast path: bare
// "default" entry with no merge_keys + no explicit mode stays as the
// raw target string the runner reads as auto-Iceberg replace.
func TestBuildLocalOutputsBareReplace(t *testing.T) {
	node := &graph.Node{
		ID:     "passthrough",
		Type:   "transform",
		Config: map[string]interface{}{},
	}
	out := buildLocalOutputs(node, "clavesa.demo.passthrough__default")
	if out["default"] != "clavesa.demo.passthrough__default" {
		t.Errorf("bare-replace default = %v, want bare target string", out["default"])
	}
}
