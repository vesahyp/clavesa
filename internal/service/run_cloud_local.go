package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// StartRunCloudLocal executes a whole deployed pipeline in the workspace-local
// docker runner against the CLOUD warehouse, instead of StartExecution on Step
// Functions (ADR-024 — cloud warehouse, local compute). The locally-computed
// run drains the same SQS cursors, advances the same _watermarks, and lands
// output + node_runs in the same Glue/S3 warehouse a Lambda run would; compute
// is only an execution-placement choice, never a state fork.
//
// Flow:
//   - Resolve the deployed runner Lambda's env (lambdaRunnerEnv) — it carries
//     the s3:// warehouse + Glue catalog/schema the container must target.
//   - Read the full ordered transform list from the deployed SFN definition
//     (loadPipelineTransforms) — the post-apply resolved inputs/outputs the
//     runner needs, which can't be rebuilt from the .tf.
//   - run_id = "local-<uuid>" so the UI/CLI route this run's states/logs to
//     the warehouse `_progress` tree (it is NOT an SFN execution).
//   - Dispatch the `_pipeline_run` bundle event through the shared cloud-local
//     docker dispatcher. The runner writes its per-node `_progress/<run>/
//     <node>.json` markers to the cloud warehouse bucket itself (ADR-024); the
//     Go side writes only the run-level `_run.json` marker (RUNNING at
//     dispatch, terminal at completion) to the SAME bucket, so --wait + the
//     dashboard read live progress identically against either warehouse.
//   - After the bundle finishes, write the terminal runs row to the CLOUD
//     system warehouse. The SFN-triggered runs_writer sidecar does NOT fire for
//     a local dispatch (there is no execution), so the Go side must write it;
//     node_runs rows are written by the runner itself (compute_target=local).
//
// Returns the run_id ("local-<uuid>") so the CLI/UI can open the run detail.
//
// The runner acquires the real warehouse run lock inside the s3 `_pipeline_run`
// bundle (slice 5); preflightRunLock here only fails fast at the CLI when the
// lease is already held, mirroring RunPipelineCloud.
func (s *Service) StartRunCloudLocal(ctx context.Context, dir string, opts RunOpts) (string, error) {
	prep, err := s.prepareCloudLocalRun(ctx, dir, opts)
	if err != nil {
		return "", err
	}
	dispErr := s.executeCloudLocalRun(ctx, prep)
	return prep.runID, dispErr
}

// StartRunCloudLocalAsync is the UI dispatch path: it prepares the run
// synchronously — resolving the deployed env, building the bundle event, and
// writing the RUNNING `_run.json` marker to the cloud warehouse, so a held run
// lock or a resolution failure surfaces to the caller immediately (the HTTP layer maps ErrRunInFlight
// to 409) — then runs the bundle in a background goroutine and returns the
// `local-<uuid>` run id at once. The browser navigates to the run detail and
// polls the progress channel; it never blocks on the whole run the way the
// CLI's synchronous StartRunCloudLocal (--wait is implicit there) does. Mirrors
// StartRunWithOpts. The background context is detached (context.Background) so
// the run outlives the HTTP request that dispatched it.
func (s *Service) StartRunCloudLocalAsync(dir string, opts RunOpts) (string, error) {
	prep, err := s.prepareCloudLocalRun(context.Background(), dir, opts)
	if err != nil {
		return "", err
	}
	go s.executeCloudLocalRun(context.Background(), prep)
	return prep.runID, nil
}

// cloudLocalRunPrep is the resolved, ready-to-run state shared between the
// synchronous (CLI) and asynchronous (UI) cloud-local dispatch paths: the
// `local-` run id, the deployed Lambda env mirrored into the container, the
// `_pipeline_run` bundle event, the run-level outcome, and the S3 progress
// store the `_run.json` marker is written through.
type cloudLocalRunPrep struct {
	runID       string
	pipelineDir string
	lambdaEnv   map[string]string
	event       map[string]any
	outcome     *runOutcome
	// store is the S3 progress store rooted at the bucket the runner PUTs its
	// per-node markers to (derived from CLAVESA_SYSTEM_WAREHOUSE) — built with
	// a PutObject-capable *s3.Client so WriteRunMarker actually persists. nil
	// when the bucket couldn't be resolved (the marker write then no-ops).
	store observability.ProgressStore
}

// prepareCloudLocalRun does everything up to (and including) starting the
// progress channel: the run-lock pre-flight, deployed-env resolution, the
// transform-list read, and the bundle-event build. Synchronous on both paths
// so failures here (lock held, undeployed, no transforms) reach the caller
// rather than a background goroutine.
func (s *Service) prepareCloudLocalRun(ctx context.Context, dir string, opts RunOpts) (*cloudLocalRunPrep, error) {
	abs := s.resolveDir(dir)
	pipelineName := filepath.Base(abs)
	workspaceRoot := filepath.Dir(abs)

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	// Fast-fail on a held warehouse run lease (best-effort) — the same
	// pre-flight RunPipelineCloud runs. The runner owns acquisition.
	if err := preflightRunLock(ctx, cfg, workspaceRoot, pipelineName); err != nil {
		return nil, err
	}

	// Deployed runner Lambda env: the s3:// warehouse, Glue catalog/schema,
	// and system-catalog the container targets. One round-trip, reused for
	// both the bundle dispatch and the runs-row write.
	functionName := pipelineRunnerLambdaName(pipelineName)
	lc := lambda.NewFromConfig(cfg)
	lambdaEnv, err := lambdaRunnerEnv(ctx, lc, functionName)
	if err != nil {
		return nil, fmt.Errorf("resolve deployed runner env: %w", err)
	}

	// Full ordered transform list (post-apply resolved I/O) from the deployed
	// SFN definition.
	transforms, err := loadPipelineTransforms(ctx, sfn.NewFromConfig(cfg), pipelineName)
	if err != nil {
		return nil, fmt.Errorf("resolve pipeline transforms from SFN definition: %w", err)
	}
	if len(transforms) == 0 {
		return nil, fmt.Errorf("deployed pipeline %q has no transforms to run", pipelineName)
	}

	// run_id carries the `local-` prefix purely as a human/debug marker that
	// this run was locally computed. Routing no longer keys on it — the
	// states/logs endpoints route by WAREHOUSE (cloud → the S3 `_progress`
	// tree this run writes to), the same as any deployed run.
	runID := "local-" + newRunID()

	// ForceNodes implies Force (mirrors RunPipelineCloud / RunPipelineWithOpts).
	force := opts.Force || len(opts.ForceNodes) > 0

	event := buildPipelineRunEvent(pipelineName, runID, transforms, force, opts.ForceNodes)

	// Run-level outcome + S3 progress store. The runner PUTs its per-node
	// `_progress/<run>/<node>.json` markers to the bucket derived from
	// CLAVESA_SYSTEM_WAREHOUSE (runner._progress_target); write the run-level
	// `_run.json` into the SAME bucket so the Go reader's per-pipeline run
	// listing finds it alongside them. The store is built with the concrete
	// *s3.Client (PutObject-capable) — the read-only s3fs.S3API a CloudProvider
	// holds would no-op WriteKey.
	outcome := newRunOutcome(runID, pipelineName, "manual")
	var store observability.ProgressStore
	if bucket := progressBucketFromEnv(lambdaEnv); bucket != "" {
		store = observability.NewS3ProgressStore(s3.NewFromConfig(cfg), bucket)
		// RUNNING marker at dispatch so the dashboard renders an in-flight
		// column the instant the async path returns the run id.
		writeRunMarker(ctx, store, outcome)
	}

	return &cloudLocalRunPrep{
		runID:       runID,
		pipelineDir: abs,
		lambdaEnv:   lambdaEnv,
		event:       event,
		outcome:     outcome,
		store:       store,
	}, nil
}

// progressBucketFromEnv derives the S3 bucket the runner writes its progress
// markers to from the deployed Lambda's CLAVESA_SYSTEM_WAREHOUSE env. Mirrors
// runner._progress_target: the bucket is the first path segment of the
// `s3://<bucket>/<key>` system-warehouse URI. Returns "" when the env is unset
// or not an s3:// URI (a non-s3 warehouse, which a deployed pipeline never
// has) so the caller skips the marker write rather than guessing a bucket.
func progressBucketFromEnv(env map[string]string) string {
	sw := env["CLAVESA_SYSTEM_WAREHOUSE"]
	if !strings.HasPrefix(sw, "s3://") {
		return ""
	}
	rest := strings.TrimPrefix(sw, "s3://")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i]
	}
	return rest
}

// executeCloudLocalRun runs the prepared bundle to completion: docker-dispatch
// (the runner writes its own per-node progress markers), then the terminal
// `_run.json` marker + the runs-row write to the cloud system warehouse.
// Blocking — the sync path returns its error, the async path runs it in a
// goroutine (the `_run.json` marker carries the failure to the UI either way).
// Returns the dispatch error.
func (s *Service) executeCloudLocalRun(ctx context.Context, prep *cloudLocalRunPrep) error {
	// The runner writes the per-node `<node>.json` progress markers itself
	// (to the same S3 bucket the run marker lands in) and returns the
	// aggregate result; the Go side no longer feeds a per-node `_event`
	// stream. Pass onEvent=nil and capture the final response for the runs row.
	res, dispErr := s.cloudLocalDispatch(ctx, prep.lambdaEnv, nil, prep.event, nil)
	prep.outcome.finish(dispErr)

	// Terminal run-level marker → the SAME S3 bucket the runner PUT its
	// per-node markers to, so the reader's run listing sees a terminal run.
	writeRunMarker(ctx, prep.store, prep.outcome)

	// Terminal runs row → CLOUD system warehouse (the sidecar won't fire).
	// Best-effort, like recordLocalRun: a failure logs but doesn't fail the run.
	var resp map[string]any
	if res != nil {
		resp = res.Response
	}
	s.recordCloudLocalRun(ctx, prep.lambdaEnv, prep.pipelineDir, prep.outcome, resp)

	return dispErr
}

// buildPipelineRunEvent assembles the `_pipeline_run` bundle event the deployed
// runner's pipeline_handler consumes. Matches the shape tfgen emits into the
// SFN Task Payload (and runPipelineBundle builds for local runs): the ordered
// transforms[] carrying each node's resolved inputs/outputs/parents, the
// run_id, and `_sf_execution_arn` set to the run_id (the join key onto
// node_runs). Pure so the shape is unit-testable.
func buildPipelineRunEvent(pipelineName, runID string, transforms []map[string]any, force bool, forceNodes []string) map[string]any {
	event := map[string]any{
		"_pipeline_run":     true,
		"pipeline":          pipelineName,
		"run_id":            runID,
		"transforms":        transforms,
		"_sf_execution_arn": runID,
		"_trigger":          "manual",
	}
	if force {
		event["_force"] = true
		if len(forceNodes) > 0 {
			event["_force_nodes"] = forceNodes
		}
	}
	return event
}

// recordCloudLocalRun writes the terminal runs row to the CLOUD system
// warehouse via the runner's CLAVESA_RECORD_RUN=1 mode. Unlike recordLocalRun
// (local Hadoop catalog + Derby metastore + local warehouse mount), this
// mirrors the deployed Lambda's warehouse env (s3:// system warehouse, Glue
// catalog/schema) and forwards the host's AWS credentials — no metastore arg,
// no local warehouse, no -v mount. Best-effort: a failure logs to stderr.
//
// node_runs rows were already written by the runner during the bundle
// (compute_target=local), so this writes ONLY the runs rollup — no double
// write.
func (s *Service) recordCloudLocalRun(ctx context.Context, lambdaEnv map[string]string, pipelineDir string, outcome *runOutcome, resp map[string]any) {
	// Fold the runner's aggregate failure context into the outcome so the runs
	// row's failed_step matches the node the runner actually failed at (the
	// dispatch error carries the same node, but the runner's `failed_node` is
	// the authoritative source). First-failure-wins: a Go-side pre-bundle
	// failure already recorded via markFailed is not overwritten.
	if resp != nil {
		if status, _ := resp["status"].(string); status == "failed" {
			failed, _ := resp["failed_node"].(string)
			outcome.markFailed(failed, "PipelineFailed", "")
		}
	}
	payload := recordRunPayload(outcome, nil)
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}

	// Resolve the workspace-local runner image, refreshed if a CLI upgrade
	// shipped new runner code — same resolution the dispatcher uses.
	image := workspace.LocalRunnerImageTag(s.workspace)
	if _, err := workspace.Load(s.workspace); err == nil {
		if ensured, eerr := workspace.EnsureLocalRunnerImage(s.workspace); eerr == nil {
			image = ensured
		}
	}

	args := []string{"run", "--rm", "-i"}
	args = append(args, "-e", "CLAVESA_RECORD_RUN=1")
	// Warehouse-shaping env mirrored from the deployed Lambda — the runs row
	// lands in the same s3:// system warehouse + Glue DB the node_runs rows
	// this run just produced did.
	for _, key := range []string{
		"CLAVESA_WAREHOUSE",
		"CLAVESA_SYSTEM_WAREHOUSE",
		"CLAVESA_CATALOG",
		"CLAVESA_SCHEMA",
		"CLAVESA_SYSTEM_CATALOG",
		"CLAVESA_PIPELINE",
	} {
		if v, ok := lambdaEnv[key]; ok {
			args = append(args, "-e", key+"="+v)
		}
	}
	args = append(args, "-e", "CLAVESA_MODULE_VERSION="+ModuleVersion)
	if digest := dockerImageDigest(image); digest != "" {
		args = append(args, "-e", "CLAVESA_RUNNER_IMAGE_DIGEST="+digest)
	}
	// Host AWS credentials + ~/.aws mount — the container reaches the cloud
	// catalog + S3 through these (same as the dispatcher).
	args = append(args, runner.AWSEnvDockerArgs(ctx)...)
	args = append(args, image)

	execRecordRunContainer(ctx, args, body)
}
