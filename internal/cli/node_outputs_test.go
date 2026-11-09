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

// TestApplyUCOutputEdit covers the partition_by and Unity Catalog flags:
// set, clear, and the partition_by + cluster_by conflict.
func TestApplyUCOutputEdit(t *testing.T) {
	defs := map[string]interface{}{"default": map[string]interface{}{"mode": "replace_partitions"}}
	err := applyUCOutputEdit(defs, ucOutputEdit{
		partitionChanged: true, partitionBy: []string{"event_date"},
		tableChanged: true, table: "main.a.events",
		locationChanged: true, location: "s3://b/events",
		jobChanged: true, job: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	def := defs["default"].(map[string]interface{})
	if !reflect.DeepEqual(def["partition_by"], []interface{}{"event_date"}) ||
		def["unity_catalog_table"] != "main.a.events" ||
		def["unity_catalog_location"] != "s3://b/events" ||
		def["replaces_databricks_job"] != int64(42) {
		t.Errorf("unexpected def: %v", def)
	}
	// Clearing the job (0) and the table ("").
	if err := applyUCOutputEdit(defs, ucOutputEdit{tableChanged: true, jobChanged: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok := def["replaces_databricks_job"]; ok {
		t.Errorf("job not cleared: %v", def)
	}
	if _, ok := def["unity_catalog_table"]; ok {
		t.Errorf("table not cleared: %v", def)
	}
	// partition_by with cluster_by is refused.
	def["cluster_by"] = []interface{}{"id"}
	if err := applyUCOutputEdit(defs, ucOutputEdit{}); err == nil {
		t.Errorf("expected partition_by + cluster_by conflict error")
	}
	// replace_partitions without partition_by is refused.
	bad := map[string]interface{}{"default": map[string]interface{}{"mode": "replace_partitions"}}
	if err := applyUCOutputEdit(bad, ucOutputEdit{}); err == nil {
		t.Errorf("expected replace_partitions needs partition_by error")
	}
}
