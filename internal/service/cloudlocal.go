package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// cloudLocalDispatcher is the shared "cloud warehouse, local compute"
// seam (ADR-024): heavy Spark work runs in the workspace-local runner
// container against a cloud (s3://) warehouse, mirroring the env the
// deployed Lambda would have carried. Backfill stage uses it now;
// pipeline run --compute local and the promote/discard operations follow
// in later slices.
//
// It deliberately does NOT reuse runTransform / runOperation: those wire
// in local-only machinery (a local warehouse mkdir+mount, the Derby
// metastore via appendMetastoreArgs, watermark and input mounts) that is
// wrong against a cloud warehouse. The container here reaches the cloud
// catalog + S3 directly via the host's AWS credentials.

// lambdaEnvGetter is the narrow slice of *lambda.Client the env-extraction
// helper needs. Pulling it behind an interface keeps lambdaRunnerEnv
// unit-testable with a fake (no AWS).
type lambdaEnvGetter interface {
	GetFunctionConfiguration(ctx context.Context, in *lambda.GetFunctionConfigurationInput, optFns ...func(*lambda.Options)) (*lambda.GetFunctionConfigurationOutput, error)
}

// lambdaRunnerEnv returns the deployed runner Lambda's full environment
// variable map in one GetFunctionConfiguration call. Both
// canonicalFromLambdaEnv (which reads CLAVESA_CATALOG / CLAVESA_SCHEMA to
// compute the canonical table id) and the cloud-local docker dispatcher
// (which mirrors the whole env into the container) build on this so they
// share a single API round-trip when called together.
func lambdaRunnerEnv(ctx context.Context, lc lambdaEnvGetter, functionName string) (map[string]string, error) {
	cfg, err := lc.GetFunctionConfiguration(ctx, &lambda.GetFunctionConfigurationInput{
		FunctionName: aws.String(functionName),
	})
	if err != nil {
		return nil, fmt.Errorf("GetFunctionConfiguration %q: %w", functionName, err)
	}
	if cfg.Environment == nil || cfg.Environment.Variables == nil {
		return nil, fmt.Errorf("Lambda %q has no environment variables", functionName)
	}
	return cfg.Environment.Variables, nil
}

// cloudLocalDockerArgs assembles the `docker run` argument vector for the
// cloud-local dispatcher. Pure (no docker / AWS calls beyond the
// pre-resolved awsArgs) so the env wiring is unit-testable.
//
// Env mirrored from the deployed Lambda map (only the keys the runner
// reads against a cloud warehouse — never the local-only ones); per-node
// env layered on top (stage passes CLAVESA_NODE / CLAVESA_LANGUAGE /
// CLAVESA_LOGIC_S3_PATH); triage columns (module version + image digest);
// the host-resolved AWS args appended last. There is intentionally NO -v
// mount, NO metastore arg, NO local warehouse — the container reads the
// cloud catalog + S3 directly.
func cloudLocalDockerArgs(image string, env, perNodeEnv map[string]string, heapArgs, awsArgs []string) []string {
	args := []string{"run", "--rm", "-i"}
	args = append(args, "-e", "CLAVESA_RUN=1")

	// Mirror the warehouse-shaping env the deployed Lambda carries. These
	// are exactly the keys the runner consults to target the cloud Glue +
	// S3 catalog and the cloud system table; the local-only keys
	// (CLAVESA_METASTORE_ADDR and friends) are deliberately absent.
	for _, key := range []string{
		"CLAVESA_WAREHOUSE",
		"CLAVESA_SYSTEM_WAREHOUSE",
		"CLAVESA_CATALOG",
		"CLAVESA_SCHEMA",
		"CLAVESA_SYSTEM_CATALOG",
		"CLAVESA_PIPELINE",
		"CLAVESA_WATERMARKS",
	} {
		if v, ok := env[key]; ok {
			args = append(args, "-e", key+"="+v)
		}
	}

	// Per-node env (stage). Sorted-key iteration would be nicer but the
	// docker run order doesn't matter; keep it deterministic on the small
	// known set the callers pass.
	for _, key := range []string{"CLAVESA_NODE", "CLAVESA_LANGUAGE", "CLAVESA_LOGIC_S3_PATH"} {
		if v, ok := perNodeEnv[key]; ok {
			args = append(args, "-e", key+"="+v)
		}
	}

	// Triage columns: which CLI version + image produced these node_runs
	// rows. Override the baked-in module version with the orchestrating
	// CLI's, same as runTransform/runOperation.
	args = append(args, "-e", "CLAVESA_MODULE_VERSION="+ModuleVersion)
	if digest := dockerImageDigest(image); digest != "" {
		args = append(args, "-e", "CLAVESA_RUNNER_IMAGE_DIGEST="+digest)
	}

	// Heap sizing: the uncapped local container otherwise falls back to a
	// 1 GB Spark driver heap, and big historical windows are the whole point
	// of running locally (GH #58). The caller resolves an explicit
	// CLAVESA_JVM_HEAP_MB or a Docker-VM-derived size into heapArgs.
	args = append(args, heapArgs...)

	// Host-resolved AWS credentials + ~/.aws mount — the container reads
	// the cloud catalog and S3 through these.
	args = append(args, awsArgs...)

	args = append(args, image)
	return args
}

// cloudLocalResult carries the docker-run outcome the callers care about:
// the parsed terminal response envelope and its raw bytes (so the same
// runnerResponseStatus / runnerResponseMessage checks the Lambda path runs
// apply verbatim).
type cloudLocalResult struct {
	Response    map[string]any
	RawResponse []byte
}

// runCloudLocalEvent docker-runs the workspace-local runner image with the
// deployed Lambda's env mirrored, feeding the event JSON on stdin. It
// parses stdout exactly like runPipelineBundle: `_event` lines stream to
// onEvent (nil-safe) and the final line with no `_event` key is the
// terminal response. A {"error":…} envelope or a non-ok runner status maps
// to a Go error the same way the Lambda Invoke path does, so a skipped or
// empty window still fails loudly (the c8f55f2 regression class).
func (s *Service) runCloudLocalEvent(ctx context.Context, env, perNodeEnv map[string]string, event any, onEvent func(map[string]any)) (*cloudLocalResult, error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("docker is required for --compute local but was not found on PATH: %w", err)
	}

	// Workspace-local runner image, refreshed if a CLI upgrade shipped new
	// runner code. Same resolution runOperation uses.
	image := runner.LocalImageName("") + ":latest"
	if _, err := workspace.Load(s.workspace); err == nil {
		ensured, err := workspace.EnsureLocalRunnerImage(s.workspace)
		if err != nil {
			return nil, fmt.Errorf("ensure runner image: %w", err)
		}
		image = ensured
	}

	// Module-version skew is observable, not fatal: the local image's
	// runner source may differ from the deployed Lambda's. node_runs
	// records the local digest + version, so warn and proceed.
	if deployed := env["CLAVESA_MODULE_VERSION"]; deployed != "" && deployed != ModuleVersion {
		fmt.Fprintf(os.Stderr,
			"warning: deployed pipeline is module version %s but this CLI's local runner is %s — running the backfill against the local runner image\n",
			deployed, ModuleVersion)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	// No co-resident metastore on this path — the container reads the cloud
	// catalog directly — so reserve only OS/daemon slack.
	args := cloudLocalDockerArgs(image, env, perNodeEnv, localHeapArgs(reserveStandaloneMB), runner.AWSEnvDockerArgs(ctx))
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(body)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("docker start: %w", err)
	}

	var finalLine []byte
	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var msg map[string]any
		if json.Unmarshal(line, &msg) != nil {
			// Spark log noise — not JSON. Ignore (it's teed to stderr by
			// the runner already when it matters).
			continue
		}
		if _, isEvent := msg["_event"]; isEvent {
			if onEvent != nil {
				onEvent(msg)
			}
			continue
		}
		// Last non-event JSON object = the terminal response. Keep a copy
		// of the raw bytes so the shared runnerResponse* checks apply.
		finalLine = append(finalLine[:0], line...)
	}

	runErr := cmd.Wait()

	out := bytes.TrimSpace(finalLine)
	if len(out) > 0 {
		var resp map[string]any
		if err := json.Unmarshal(out, &resp); err == nil {
			if msg, _ := resp["error"].(string); msg != "" {
				return nil, fmt.Errorf("runner: %s", msg)
			}
			return &cloudLocalResult{Response: resp, RawResponse: append([]byte(nil), out...)}, nil
		}
	}
	if runErr != nil {
		return nil, fmt.Errorf("docker run: %w\nstderr: %s", runErr, strings.TrimSpace(stderrBuf.String()))
	}
	return nil, fmt.Errorf("runner produced no parseable response\nstderr: %s", strings.TrimSpace(stderrBuf.String()))
}

// stageLocalDispatch runs one backfill-stage event through the cloud-local
// docker dispatcher and returns the runner's raw terminal response bytes.
// On dispatch failure it stamps run.Status/ErrorMsg + timing and returns
// the error so the caller can surface the partial run. Split out of
// BackfillStage so the local routing + failure handling is unit-testable
// (the dispatcher itself is the injected s.cloudLocalDispatch).
func (s *Service) stageLocalDispatch(ctx context.Context, run *BackfillRun, lambdaEnv map[string]string, node, language, logicPath string, payload any) ([]byte, error) {
	run.StartedAt = time.Now()
	perNodeEnv := map[string]string{
		"CLAVESA_NODE":          node,
		"CLAVESA_LANGUAGE":      language,
		"CLAVESA_LOGIC_S3_PATH": logicPath,
	}
	res, derr := s.cloudLocalDispatch(ctx, lambdaEnv, perNodeEnv, payload, nil)
	run.StoppedAt = time.Now()
	if derr != nil {
		run.Status = "failed"
		run.ErrorMsg = derr.Error()
		return nil, fmt.Errorf("local backfill staging: %w", derr)
	}
	return res.RawResponse, nil
}

// operationLocalDispatch resolves the deployed runner Lambda's env and
// dispatches a backfill `_operation` payload (promote / discard) through the
// cloud-local docker runner instead of the Lambda. Operations are NOT
// per-node — the runner routes `_operation` events to _run_operation before
// any node logic, so no CLAVESA_NODE / LANGUAGE / LOGIC_S3_PATH is set
// (perNodeEnv=nil). Returns the runner's raw terminal response bytes so the
// caller runs the same runnerResponse* status checks the Lambda path runs.
// No run lock — operations on a private staging table don't acquire the
// warehouse run lease (consistent with stage, slice 6).
func (s *Service) operationLocalDispatch(ctx context.Context, lc lambdaEnvGetter, functionName string, payload any) ([]byte, error) {
	lambdaEnv, err := lambdaRunnerEnv(ctx, lc, functionName)
	if err != nil {
		return nil, fmt.Errorf("resolve runner env: %w", err)
	}
	res, derr := s.cloudLocalDispatch(ctx, lambdaEnv, nil, payload, nil)
	if derr != nil {
		return nil, derr
	}
	return res.RawResponse, nil
}

// ValidateCompute is the exported wrapper over validateCompute for CLI
// callers (`pipeline run --compute`) that resolve the warehouse + compute
// pairing before dispatch. The service-internal backfill path calls the
// unexported form directly.
func ValidateCompute(warehouse workspace.Warehouse, compute string) error {
	return validateCompute(warehouse, compute)
}

// validateCompute rejects an impossible warehouse/compute combination
// before any work begins (ADR-024). The matrix:
//
//   - ""      → nil (defaults to the warehouse; no override).
//   - "local" → nil ALWAYS. On a cloud warehouse it routes heavy work to
//     the local docker runner; on a local warehouse it's a harmless no-op
//     (compute already equals the warehouse), NOT an error. This is softer
//     than the original plan — flagging the no-op as an error punished
//     scripts that pass --compute local unconditionally.
//   - "cloud" + local warehouse → error: cloud compute cannot reach a
//     laptop disk.
//   - anything else → error.
func validateCompute(warehouse workspace.Warehouse, compute string) error {
	switch compute {
	case "":
		return nil
	case "local":
		return nil
	case "cloud":
		if warehouse == workspace.WarehouseLocal {
			return fmt.Errorf("cloud compute cannot reach a local warehouse — deploy and switch with 'workspace use --warehouse cloud'")
		}
		return nil
	default:
		return fmt.Errorf("unknown compute %q (want \"local\" or \"cloud\")", compute)
	}
}
