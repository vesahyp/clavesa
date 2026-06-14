package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/vesahyp/clavesa/internal/runlock"
	"github.com/vesahyp/clavesa/internal/workspace"
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

	// ADR-024 slice 5: best-effort pre-flight on the warehouse run lock
	// (s3://<bucket>/<pipeline>/_locks/run.json — the lease the deployed
	// Lambda's pipeline_handler acquires). If it's held and unexpired,
	// fail fast with the holder's identity instead of dispatching an
	// execution the Lambda will FAIL seconds later. The Lambda owns
	// acquisition — this never writes; any S3/read problem proceeds to
	// dispatch.
	if err := preflightRunLock(ctx, cfg, filepath.Dir(abs), pipelineName); err != nil {
		return "", err
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

// preflightRunLock is a best-effort fast-fail check on the warehouse run
// lease (internal/runlock's S3 backend object,
// `s3://<bucket>/<pipeline>/_locks/run.json`). It GETs the lease and returns
// a HeldError-derived error — wrapped under ErrRunInFlight so the HTTP 409
// mapping matches the local path in prepareRun — when the lease is held and
// not expired past the takeover grace. It deliberately does NOT acquire:
// the deployed Lambda owns acquisition (runner/run_lock.py); this exists
// purely so `pipeline run` rejects in milliseconds instead of dispatching
// an execution that fails seconds later.
//
// Best-effort means: no deployed bucket recorded, GET failure (no creds,
// no such key, permissions), unparseable lease — all proceed to dispatch.
//
// Version-skew note: a Lambda deployed before this slice never writes a
// lease, so the pre-flight finds nothing and older deployments behave as
// before until `workspace upgrade` + deploy rebuilds their image.
func preflightRunLock(ctx context.Context, cfg aws.Config, workspaceRoot, pipelineName string) error {
	bucket := workspace.PipelineBucket(workspaceRoot)
	if bucket == "" {
		return nil
	}
	// Same key derivation as runlock.New's S3 backend (bucket-root prefix):
	// path.Join("", pipeline, "_locks", "run.json").
	key := pipelineName + "/_locks/run.json"
	out, err := s3.NewFromConfig(cfg).GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil // best-effort: absent lease, no perms, etc. — dispatch
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil
	}
	// The slice of the lease document the held-check needs (the full shape
	// lives in internal/runlock/runlock.go leaseDoc).
	var doc struct {
		Holder     runlock.Holder `json:"holder"`
		AcquiredAt time.Time      `json:"acquired_at"`
		ExpiresAt  time.Time      `json:"expires_at"`
		State      string         `json:"state"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil
	}
	if doc.State != "held" {
		return nil // tombstoned — the Lambda takes over immediately
	}
	if time.Now().After(doc.ExpiresAt.Add(runlock.TakeoverGrace)) {
		return nil // expired past grace — the Lambda takes over
	}
	held := &runlock.HeldError{Holder: doc.Holder, AcquiredAt: doc.AcquiredAt, ExpiresAt: doc.ExpiresAt}
	return fmt.Errorf("%w: %s", ErrRunInFlight, held.Error())
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
