package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

// RunPipelineCloud starts a Step Functions execution for a deployed
// pipeline. The state machine name follows the orchestration emitter's
// convention (`clavesa-<pipeline_name>`); it's looked up by name rather
// than by reading the pipeline's tfstate so the caller doesn't have to
// be sitting on the apply machine.
//
// opts.Force / opts.ForceNodes are threaded into the execution input
// payload (`{"_trigger":"manual","_force":bool,"_force_nodes":[...]}`) —
// the SFN ASL forwards that payload to every per-transform Lambda
// invocation, where the runner's `_is_forced()` check reads the same
// keys and bypasses incremental-skip on matching nodes.
//
// Returns the execution ARN of the started run. Polling for terminal
// status stays in the CLI layer (it's a shell-loop concern); HTTP
// callers tail `/pipeline/execution/states` instead.
//
// AWS client is built on demand from ambient config — the CLI's
// applyWorkspaceAWSProfile (and the HTTP handler's `ensureAWS` lazy
// boundary) populate `AWS_PROFILE` / `AWS_REGION` before this method
// fires. Don't introduce a package-level singleton.
func (s *Service) RunPipelineCloud(ctx context.Context, dir string, opts RunOpts) (string, error) {
	abs := s.resolveDir(dir)
	pipelineName := filepath.Base(abs)
	stateMachineName := "clavesa-" + pipelineName

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("load AWS config: %w", err)
	}
	client := sfn.NewFromConfig(cfg)

	arn, err := findStateMachineByName(ctx, client, stateMachineName)
	if err != nil {
		return "", err
	}

	// SFN ASL passes the execution input forward to the per-transform
	// Lambda invocation payload. The runner reads `_force` / `_force_nodes`
	// from the event and threads them through to its incremental-skip
	// bypass check (runner.py:_is_forced).
	//
	// ForceNodes implies Force (mirrors RunPipelineWithOpts' defend-in-depth).
	effectiveForce := opts.Force || len(opts.ForceNodes) > 0
	inputPayload := map[string]any{"_trigger": "manual"}
	if effectiveForce {
		inputPayload["_force"] = true
		if len(opts.ForceNodes) > 0 {
			inputPayload["_force_nodes"] = opts.ForceNodes
		}
	}
	inputJSON, err := json.Marshal(inputPayload)
	if err != nil {
		return "", fmt.Errorf("marshal execution input: %w", err)
	}

	startOut, err := client.StartExecution(ctx, &sfn.StartExecutionInput{
		StateMachineArn: aws.String(arn),
		Input:           aws.String(string(inputJSON)),
	})
	if err != nil {
		return "", fmt.Errorf("StartExecution: %w", err)
	}
	return aws.ToString(startOut.ExecutionArn), nil
}

// findStateMachineByName paginates ListStateMachines until it finds an
// exact name match. Errors with a clear message if not found — usually
// means the pipeline hasn't been deployed yet.
func findStateMachineByName(ctx context.Context, client *sfn.Client, name string) (string, error) {
	var nextToken *string
	for {
		out, err := client.ListStateMachines(ctx, &sfn.ListStateMachinesInput{
			MaxResults: 1000,
			NextToken:  nextToken,
		})
		if err != nil {
			return "", fmt.Errorf("ListStateMachines: %w", err)
		}
		for _, sm := range out.StateMachines {
			if aws.ToString(sm.Name) == name {
				return aws.ToString(sm.StateMachineArn), nil
			}
		}
		if out.NextToken == nil {
			return "", fmt.Errorf("state machine %q not found in account/region — has the pipeline been deployed (terraform apply)?", name)
		}
		nextToken = out.NextToken
	}
}
