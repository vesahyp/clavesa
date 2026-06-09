package service

import (
	"reflect"
	"strings"
	"testing"
)

// TestNodeIOFromDefinition pins backfill node resolution against the deployed
// SFN definition shape. The bug: loadNodeIO matched --node against named SFN
// states, but v2.2.0 collapsed the machine into a single RunPipeline Task that
// carries every transform in Parameters.Payload.transforms[] (see
// orchestration/tfgen.emitStateMachine). No state is named after a node, so
// resolution failed for *every* node on every deployed pipeline.
//
// The new-shape JSON below mirrors emitStateMachine's jsonencode output so the
// test pins the real contract, not an invented shape.
func TestNodeIOFromDefinition(t *testing.T) {
	// Three transforms, topo-ordered, exactly as the bundle Task emits them.
	newShape := `{
	  "Comment": "Clavesa pipeline: web-traffic",
	  "StartAt": "RunPipeline",
	  "States": {
	    "RunPipeline": {
	      "Type": "Task",
	      "Resource": "arn:aws:states:::lambda:invoke",
	      "TimeoutSeconds": 900,
	      "Parameters": {
	        "FunctionName": "arn:aws:lambda:eu-west-1:1:function:clavesa-web-traffic-runner",
	        "Payload": {
	          "_pipeline_run": true,
	          "pipeline": "web-traffic",
	          "transforms": [
	            {"node":"cloudfront_raw","language":"sql","logic_path":"s3://wt-clavesa/web-traffic/cloudfront_raw/logic.sql","inputs":{"src":"sources.cf"},"outputs":{"default":"clavesa_wt__bronze.cloudfront_raw"},"parents":[]},
	            {"node":"sessions","language":"python","logic_path":"s3://wt-clavesa/web-traffic/sessions/logic.py","inputs":{"cloudfront_raw":"clavesa_wt__bronze.cloudfront_raw"},"outputs":{"default":"clavesa_wt__silver.sessions"},"parents":["cloudfront_raw"]},
	            {"node":"daily_rollup","language":"sql","logic_path":"s3://wt-clavesa/web-traffic/daily_rollup/logic.sql","inputs":{"sessions":"clavesa_wt__silver.sessions"},"outputs":{"default":"clavesa_wt__gold.daily_rollup"},"parents":["sessions"]}
	          ],
	          "_sf_execution_arn.$": "$$.Execution.Id"
	        }
	      },
	      "End": true
	    }
	  }
	}`

	// Pre-v2.2.0: one named Task per node, I/O on its own Payload.
	legacyShape := `{
	  "StartAt": "cloudfront_raw",
	  "States": {
	    "cloudfront_raw": {
	      "Type": "Task",
	      "Parameters": {"Payload": {"inputs":{"src":"sources.cf"},"outputs":{"default":"clavesa_wt__bronze.cloudfront_raw"}}},
	      "Next": "sessions"
	    },
	    "sessions": {
	      "Type": "Task",
	      "Parameters": {"Payload": {"language":"python","logic_path":"s3://wt-clavesa/web-traffic/sessions/logic.py","inputs":{"cloudfront_raw":"clavesa_wt__bronze.cloudfront_raw"},"outputs":{"default":"clavesa_wt__silver.sessions"}}},
	      "End": true
	    }
	  }
	}`

	// Pre-v2.2.0 fanout: a node nested in a Parallel state's Branches.
	legacyParallel := `{
	  "StartAt": "fanout",
	  "States": {
	    "fanout": {
	      "Type": "Parallel",
	      "Branches": [
	        {"StartAt":"branch_a","States":{"branch_a":{"Type":"Task","Parameters":{"Payload":{"inputs":{"x":"db.x"},"outputs":{"default":"db.branch_a"}}},"End":true}}}
	      ],
	      "End": true
	    }
	  }
	}`

	cases := []struct {
		name          string
		def           string
		node          string
		wantInputs    map[string]any
		wantOutputs   map[string]any
		wantLanguage  string
		wantLogicPath string
		wantErr       string // substring; "" means no error
	}{
		{
			name:          "new shape, first transform",
			def:           newShape,
			node:          "cloudfront_raw",
			wantInputs:    map[string]any{"src": "sources.cf"},
			wantOutputs:   map[string]any{"default": "clavesa_wt__bronze.cloudfront_raw"},
			wantLanguage:  "sql",
			wantLogicPath: "s3://wt-clavesa/web-traffic/cloudfront_raw/logic.sql",
		},
		{
			// The symptom node from the bug report — proves the fix end to end.
			name:          "new shape, middle transform (not index 0)",
			def:           newShape,
			node:          "sessions",
			wantInputs:    map[string]any{"cloudfront_raw": "clavesa_wt__bronze.cloudfront_raw"},
			wantOutputs:   map[string]any{"default": "clavesa_wt__silver.sessions"},
			wantLanguage:  "python",
			wantLogicPath: "s3://wt-clavesa/web-traffic/sessions/logic.py",
		},
		{
			name:          "new shape, last transform",
			def:           newShape,
			node:          "daily_rollup",
			wantInputs:    map[string]any{"sessions": "clavesa_wt__silver.sessions"},
			wantOutputs:   map[string]any{"default": "clavesa_wt__gold.daily_rollup"},
			wantLanguage:  "sql",
			wantLogicPath: "s3://wt-clavesa/web-traffic/daily_rollup/logic.sql",
		},
		{
			name:    "new shape, node absent",
			def:     newShape,
			node:    "no_such_node",
			wantErr: `node "no_such_node" not found`,
		},
		{
			name:          "legacy shape, top-level node",
			def:           legacyShape,
			node:          "sessions",
			wantInputs:    map[string]any{"cloudfront_raw": "clavesa_wt__bronze.cloudfront_raw"},
			wantOutputs:   map[string]any{"default": "clavesa_wt__silver.sessions"},
			wantLanguage:  "python",
			wantLogicPath: "s3://wt-clavesa/web-traffic/sessions/logic.py",
		},
		{
			// Legacy parallel branch carries no language/logic_path — the
			// extractor tolerates the absence and returns empty strings.
			name:          "legacy shape, parallel-branch node",
			def:           legacyParallel,
			node:          "branch_a",
			wantInputs:    map[string]any{"x": "db.x"},
			wantOutputs:   map[string]any{"default": "db.branch_a"},
			wantLanguage:  "",
			wantLogicPath: "",
		},
		{
			name:    "malformed definition",
			def:     `{not json`,
			node:    "x",
			wantErr: "parse SFN definition",
		},
		{
			name:    "empty definition",
			def:     `{}`,
			node:    "x",
			wantErr: `node "x" not found`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in, out, language, logicPath, err := nodeIOFromDefinition(c.def, c.node)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(in, any(c.wantInputs)) {
				t.Errorf("inputs = %#v, want %#v", in, c.wantInputs)
			}
			if !reflect.DeepEqual(out, any(c.wantOutputs)) {
				t.Errorf("outputs = %#v, want %#v", out, c.wantOutputs)
			}
			if language != c.wantLanguage {
				t.Errorf("language = %q, want %q", language, c.wantLanguage)
			}
			if logicPath != c.wantLogicPath {
				t.Errorf("logic_path = %q, want %q", logicPath, c.wantLogicPath)
			}
		})
	}
}
