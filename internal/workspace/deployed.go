package workspace

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// DeployedFile is the file at the root of a hosted tree copy
// (s3://<pipeline_bucket>/_clavesa/tree/deployed.json, ADR-032) that holds
// the Terraform outputs the read paths need. A workspace on the developer's
// disk has no such file: `clavesa deploy` writes it only into the upload.
// When it is present, every state read (ReadStackState) answers from it,
// so a tree synced from S3 into a temp dir serves the read paths with no
// Terraform state and no access to the state bucket.
const DeployedFile = "deployed.json"

// DeployedVersion is the deployed.json format version.
const DeployedVersion = 1

// Deployed is the deployed.json shape.
type Deployed struct {
	Version     int    `json:"version"`
	GeneratedAt string `json:"generated_at"`
	Account     string `json:"account,omitempty"`
	Region      string `json:"region,omitempty"`
	// Workspace holds the workspace stack's string outputs the read paths
	// use: pipeline_bucket, system_catalog, hosted_workgroup and the like.
	Workspace DeployedStack `json:"workspace"`
	// Pipelines is keyed by the pipeline directory's base name (the same
	// key the ADR-025 state layout uses).
	Pipelines map[string]DeployedPipeline `json:"pipelines"`
}

// DeployedStack is one stack's outputs.
type DeployedStack struct {
	Outputs map[string]string `json:"outputs"`
}

// DeployedPipeline is what the read paths need of one pipeline stack.
type DeployedPipeline struct {
	StateMachineARN string `json:"state_machine_arn,omitempty"`
}

// DeployedWorkspaceOutputs are the workspace stack outputs deployed.json
// carries. Nothing else from the state is copied.
var DeployedWorkspaceOutputs = []string{
	"pipeline_bucket", "system_catalog", "system_pipelines_db", "athena_workgroup",
	"hosted_workgroup", "hosted_reader_role_arn",
}

// LoadDeployed reads <root>/deployed.json. (nil, nil) when the file does
// not exist: the normal case for a workspace on disk.
func LoadDeployed(root string) (*Deployed, error) {
	data, err := os.ReadFile(filepath.Join(root, DeployedFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", DeployedFile, err)
	}
	var d Deployed
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("parse %s: %w", DeployedFile, err)
	}
	return &d, nil
}

// HostedAthenaWorkGroup returns the reader's Athena workgroup when the
// workspace at root is a hosted tree copy, else "". Queries in a hosted
// tree must go through it: the reader role may start queries there only.
func HostedAthenaWorkGroup(root string) string {
	d, err := LoadDeployed(root)
	if err != nil || d == nil {
		return ""
	}
	return d.Workspace.Outputs["hosted_workgroup"]
}

// stackState renders the part of a Terraform state the read paths parse:
// string outputs for the workspace stack, the aws_sfn_state_machine.pipeline
// resource for a pipeline stack. nil when the stack is not in the file
// ("not deployed", as ReadStackState reports it).
func (d *Deployed) stackState(workspaceRoot, stackDir string) ([]byte, error) {
	type output struct {
		Value string `json:"value"`
		Type  string `json:"type"`
	}
	wsAbs, wsErr := filepath.Abs(workspaceRoot)
	stackAbs, stackErr := filepath.Abs(stackDir)
	if wsErr == nil && stackErr == nil && wsAbs == stackAbs {
		outs := map[string]output{}
		for k, v := range d.Workspace.Outputs {
			outs[k] = output{Value: v, Type: "string"}
		}
		return json.Marshal(map[string]any{"version": 4, "outputs": outs, "resources": []any{}})
	}
	p, ok := d.Pipelines[filepath.Base(stackDir)]
	if !ok || p.StateMachineARN == "" {
		return nil, nil
	}
	return json.Marshal(map[string]any{
		"version": 4,
		"outputs": map[string]any{},
		"resources": []any{map[string]any{
			"mode": "managed", "type": "aws_sfn_state_machine", "name": "pipeline",
			"instances": []any{map[string]any{"attributes": map[string]any{"arn": p.StateMachineARN}}},
		}},
	})
}

// StateMachineARNFromState returns the ARN of aws_sfn_state_machine.pipeline
// in a pipeline stack's state JSON, or "" when it is not there.
func StateMachineARNFromState(data []byte) string {
	var state struct {
		Resources []struct {
			Type      string `json:"type"`
			Name      string `json:"name"`
			Instances []struct {
				Attributes struct {
					ARN string `json:"arn"`
				} `json:"attributes"`
			} `json:"instances"`
		} `json:"resources"`
	}
	if json.Unmarshal(data, &state) != nil {
		return ""
	}
	for _, r := range state.Resources {
		if r.Type == "aws_sfn_state_machine" && r.Name == "pipeline" && len(r.Instances) > 0 {
			return r.Instances[0].Attributes.ARN
		}
	}
	return ""
}

// GlueDatabaseFromState returns the name of aws_glue_catalog_database.pipeline
// (the pipeline's <catalog>__<schema> database) in a pipeline stack's
// state JSON, or "" when it is not there: the pipeline is not deployed.
func GlueDatabaseFromState(data []byte) string {
	var state struct {
		Resources []struct {
			Mode      string `json:"mode"`
			Type      string `json:"type"`
			Name      string `json:"name"`
			Instances []struct {
				Attributes struct {
					Name string `json:"name"`
				} `json:"attributes"`
			} `json:"instances"`
		} `json:"resources"`
	}
	if json.Unmarshal(data, &state) != nil {
		return ""
	}
	for _, r := range state.Resources {
		if r.Mode != "data" && r.Type == "aws_glue_catalog_database" && r.Name == "pipeline" && len(r.Instances) > 0 {
			return r.Instances[0].Attributes.Name
		}
	}
	return ""
}

// StackOutput returns one string output of a stack's state JSON, "" when
// absent. Exported for the deploy-time deployed.json builder.
func StackOutput(data []byte, name string) string { return tfstateOutput(data, name) }
