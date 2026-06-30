package cli

import (
	"reflect"
	"testing"
)

// TestUpdateOutputDefaultBoundBySet covers the --output-bound-by flag
// path: a bound_by list lands on the default output_definitions entry as
// an []interface{}, parallel to cluster_by. bound_by does not flip mode.
func TestUpdateOutputDefaultBoundBySet(t *testing.T) {
	out := updateOutputDefault(
		nil,
		false, "", // mode unchanged
		false, nil, // merge_keys unchanged
		false, nil, // cluster_by unchanged
		true, []string{"event_date"}, // bound_by changed
		false, nil, // merge_update unchanged
	)
	def, ok := out["default"].(map[string]interface{})
	if !ok {
		t.Fatalf("default not a dict: %T", out["default"])
	}
	want := []interface{}{"event_date"}
	if !reflect.DeepEqual(def["bound_by"], want) {
		t.Errorf("bound_by = %v, want %v", def["bound_by"], want)
	}
	if _, hasMode := def["mode"]; hasMode {
		t.Errorf("bound_by must not set mode, got %v", def["mode"])
	}
}

// TestUpdateOutputDefaultBoundByClear covers passing --output-bound-by
// with no value: an existing bound_by is deleted from the default entry.
func TestUpdateOutputDefaultBoundByClear(t *testing.T) {
	existing := map[string]interface{}{
		"default": map[string]interface{}{
			"mode":     "merge",
			"bound_by": []interface{}{"event_date"},
		},
	}
	out := updateOutputDefault(
		existing,
		false, "",
		false, nil,
		false, nil,
		true, nil, // bound_by changed, empty → clear
		false, nil,
	)
	def := out["default"].(map[string]interface{})
	if _, ok := def["bound_by"]; ok {
		t.Errorf("bound_by should be cleared, still present: %v", def["bound_by"])
	}
	if def["mode"] != "merge" {
		t.Errorf("clearing bound_by must not touch mode, got %v", def["mode"])
	}
}
