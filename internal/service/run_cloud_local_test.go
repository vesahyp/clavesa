package service

import (
	"encoding/json"
	"testing"
)

func TestBuildPipelineRunEvent(t *testing.T) {
	transforms := []map[string]any{
		{"node": "trips", "language": "sql", "inputs": map[string]any{}, "outputs": map[string]any{}, "parents": []any{}},
		{"node": "revenue_by_payment", "language": "sql", "parents": []any{"trips"}},
	}

	t.Run("base shape", func(t *testing.T) {
		ev := buildPipelineRunEvent("demo", "local-abc", transforms, false, nil)
		if ev["_pipeline_run"] != true {
			t.Errorf("_pipeline_run = %v, want true", ev["_pipeline_run"])
		}
		if ev["run_id"] != "local-abc" {
			t.Errorf("run_id = %v, want local-abc", ev["run_id"])
		}
		// _sf_execution_arn must equal the run_id — that's the join key onto
		// node_runs the runner threads through.
		if ev["_sf_execution_arn"] != "local-abc" {
			t.Errorf("_sf_execution_arn = %v, want local-abc", ev["_sf_execution_arn"])
		}
		if ev["pipeline"] != "demo" {
			t.Errorf("pipeline = %v, want demo", ev["pipeline"])
		}
		if ev["_trigger"] != "manual" {
			t.Errorf("_trigger = %v, want manual", ev["_trigger"])
		}
		got, _ := ev["transforms"].([]map[string]any)
		if len(got) != 2 {
			t.Fatalf("transforms len = %d, want 2", len(got))
		}
		if _, ok := ev["_force"]; ok {
			t.Error("_force should be absent when force=false")
		}
	})

	t.Run("force without nodes", func(t *testing.T) {
		ev := buildPipelineRunEvent("demo", "local-abc", transforms, true, nil)
		if ev["_force"] != true {
			t.Errorf("_force = %v, want true", ev["_force"])
		}
		if _, ok := ev["_force_nodes"]; ok {
			t.Error("_force_nodes should be absent when no nodes scoped")
		}
	})

	t.Run("force with scoped nodes", func(t *testing.T) {
		ev := buildPipelineRunEvent("demo", "local-abc", transforms, true, []string{"trips"})
		if ev["_force"] != true {
			t.Errorf("_force = %v, want true", ev["_force"])
		}
		fn, _ := ev["_force_nodes"].([]string)
		if len(fn) != 1 || fn[0] != "trips" {
			t.Errorf("_force_nodes = %v, want [trips]", ev["_force_nodes"])
		}
	})

	t.Run("marshals to valid JSON", func(t *testing.T) {
		ev := buildPipelineRunEvent("demo", "local-abc", transforms, true, []string{"trips"})
		if _, err := json.Marshal(ev); err != nil {
			t.Fatalf("marshal event: %v", err)
		}
	})
}

func TestAllTransformsFromDefinition(t *testing.T) {
	t.Run("single-Task v2.2.0 shape", func(t *testing.T) {
		def := `{
			"StartAt": "RunPipeline",
			"States": {
				"RunPipeline": {
					"Type": "Task",
					"Parameters": {
						"Payload": {
							"_pipeline_run": true,
							"transforms": [
								{"node": "trips", "language": "sql", "inputs": {"a": "s3://x"}, "outputs": {"default": ""}, "parents": []},
								{"node": "revenue", "language": "sql", "outputs": {"default": ""}, "parents": ["trips"]}
							]
						}
					}
				}
			}
		}`
		got, err := allTransformsFromDefinition(def)
		if err != nil {
			t.Fatalf("allTransformsFromDefinition: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len = %d, want 2", len(got))
		}
		if got[0]["node"] != "trips" {
			t.Errorf("transforms[0].node = %v, want trips", got[0]["node"])
		}
		if got[1]["node"] != "revenue" {
			t.Errorf("transforms[1].node = %v, want revenue", got[1]["node"])
		}
	})

	t.Run("legacy per-node shape errors clearly", func(t *testing.T) {
		// No transforms[] array — the pre-v2.2.0 multi-state ASL.
		def := `{"StartAt": "trips", "States": {"trips": {"Type": "Task", "Parameters": {"Payload": {"inputs": {}, "outputs": {}}}}}}`
		_, err := allTransformsFromDefinition(def)
		if err == nil {
			t.Fatal("expected an error for a definition with no transforms[] array")
		}
	})

	t.Run("malformed JSON errors", func(t *testing.T) {
		if _, err := allTransformsFromDefinition("{not json"); err == nil {
			t.Fatal("expected a parse error")
		}
	})
}
