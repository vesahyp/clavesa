package workspace

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// PipelineBucket reads the workspace's terraform.tfstate and returns the
// `pipeline_bucket` output, or "" if the workspace hasn't been deployed yet
// (no tfstate, no outputs, or apply not yet run). Callers use this to
// auto-derive cloud-side defaults like ATHENA_OUTPUT_BUCKET — never to
// drive deployment behavior, since the tfstate may be stale.
func PipelineBucket(workspaceRoot string) string {
	return tfstateOutput(workspaceRoot, "pipeline_bucket")
}

// tfstateOutput pulls a named string output from <root>/terraform.tfstate.
// Returns "" on any read/parse/missing error — caller decides whether
// that's an empty case or a hard error. Format:
//
//	{ "outputs": { "<name>": { "value": <…>, "type": "string" } } }
func tfstateOutput(root, name string) string {
	data, err := os.ReadFile(filepath.Join(root, "terraform.tfstate"))
	if err != nil {
		return ""
	}
	var s struct {
		Outputs map[string]struct {
			Value any `json:"value"`
		} `json:"outputs"`
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return ""
	}
	out, ok := s.Outputs[name]
	if !ok {
		return ""
	}
	if str, ok := out.Value.(string); ok {
		return str
	}
	return ""
}

