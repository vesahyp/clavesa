// Package pipelinestatus provides the GET /pipeline/status endpoint which
// reads terraform.tfstate from the pipeline directory and queries AWS Step
// Functions for recent execution history.
package pipelinestatus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/vesahyp/clavesa/internal/errs"
	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pathutil"
)

// RunOpts mirrors service.RunOpts at this package boundary. Force /
// ForceNodes thread through to the runner's _is_forced() check (Slice
// 4) — exposed to the HTTP body so the UI's Run button can pass the
// force-checkbox + force-nodes-input the same way `clavesa pipeline run
// --force / --force-node` does (ADR-015).
type RunOpts struct {
	Force      bool
	ForceNodes []string
}

// LocalPipelineRunner is the local-execution path used when a pipeline has
// any `compute = "local"` transform. StartRunWithOpts begins the run
// asynchronously and returns a run id immediately so the UI can navigate
// to the run page without blocking; it returns ErrRunInFlight when the
// pipeline already has a run executing. Implemented by service.Service;
// the interface lives here so internal/pipelinestatus stays free of an
// internal/service import — ui.go wires a bridge.
type LocalPipelineRunner interface {
	StartRunWithOpts(dir string, opts RunOpts) (string, error)
}

// CloudPipelineRunner is the cloud-execution path used when the
// inspected pipeline's compute attr is not "local". RunPipelineCloud
// looks up the deployed SFN state machine by name, starts an execution
// with the optional force payload, and returns the execution ARN.
// Implemented by service.Service; the interface lives here so
// internal/pipelinestatus stays free of an internal/service import.
type CloudPipelineRunner interface {
	RunPipelineCloud(ctx context.Context, dir string, opts RunOpts) (string, error)
}

// CloudLocalPipelineRunner is the "cloud warehouse, local compute" path
// (ADR-024): the whole pipeline bundle runs in the workspace-local docker
// runner against the cloud warehouse instead of an SFN execution. Returns a
// `local-<uuid>` run id the UI routes to the filesystem progress channel.
// Implemented by service.Service; the interface lives here so
// internal/pipelinestatus stays free of an internal/service import.
type CloudLocalPipelineRunner interface {
	StartRunCloudLocal(ctx context.Context, dir string, opts RunOpts) (string, error)
}

// ErrRunInFlight is re-exported from internal/errs so callers comparing
// with errors.Is continue to work; the underlying sentinel is shared with
// service.ErrRunInFlight, eliminating the cli/ui.go bridge (C10,
// 2026-05-24).
var ErrRunInFlight = errs.ErrRunInFlight

// Handler serves GET /pipeline/status, /pipeline/execution, and the two
// execution-detail endpoints (states + logs). The execution-detail endpoints
// delegate to observability.CloudProvider so the local provider can implement
// the same shapes; ADR-014.
type Handler struct {
	root string // workspace root — used to resolve relative dir params

	awsOnce   sync.Once
	sfnClient *sfn.Client
	cwlClient *cloudwatchlogs.Client
	cloud     *observability.CloudProvider
	awsRegion string
	awsErr    error

	// athenaOutputBucket is the workspace bucket that also holds the
	// `_progress/<execARN>/<node>.json` objects the runner publishes per
	// poll tick. liveProgressStates LISTs it to colour the run DAG live;
	// without it (and the S3 client wired in ensureAWS) the cloud provider
	// short-circuits and in-flight node states never surface.
	athenaOutputBucket string

	// resolver, when set, lets states/logs dispatch per-pipeline based on
	// `compute` attr (ADR-014). When nil, the handler falls through to the
	// cloud-only ARN path — preserves the pre-resolver call shape for tests.
	resolver *observability.Resolver

	// localRunner, when set, lets POST /pipeline/run dispatch
	// compute = "local" pipelines through service.RunPipeline (the same
	// code path `clavesa pipeline run` uses). Without it, all run
	// requests fall through to the SFN StartExecution path.
	localRunner LocalPipelineRunner

	// cloudRunner, when set, lets POST /pipeline/run dispatch cloud
	// pipelines through service.RunPipelineCloud — the same path
	// `clavesa pipeline run` follows for compute != local. Threads the
	// optional force flags through to SFN's execution input so the UI's
	// Force checkbox / force-nodes input land at the runner's
	// _is_forced() check (ADR-015). Without it, the handler builds an
	// SFN client lazily and dispatches inline without execution input
	// (legacy behaviour; preserves pre-Slice-C tests).
	cloudRunner CloudPipelineRunner

	// cloudLocalRunner, when set, lets POST /pipeline/run dispatch a cloud
	// warehouse run through the local docker runner when the request body
	// carries compute="local" (ADR-024). Without it, compute="local" on a
	// cloud warehouse falls back to the SFN path.
	cloudLocalRunner CloudLocalPipelineRunner
}

// NewHandler returns a new Handler rooted at the given workspace directory.
func NewHandler(root string) *Handler {
	return &Handler{root: root}
}

// WithResolver wires a per-pipeline observability resolver. When set, the
// states + logs endpoints accept a `dir` query param and dispatch through
// the resolver (cloud or local); without `dir` the legacy ARN-based cloud
// path is used. Returns h for chained construction.
func (h *Handler) WithResolver(r *observability.Resolver) *Handler {
	h.resolver = r
	return h
}

// WithAthenaOutputBucket wires the workspace bucket so the cloud provider
// can read the runner's `_progress/` objects and colour the run DAG live.
// Returns h for chained construction.
func (h *Handler) WithAthenaOutputBucket(b string) *Handler {
	h.athenaOutputBucket = b
	return h
}

// WithLocalRunner enables POST /pipeline/run to dispatch local pipelines
// through service.RunPipeline. Tests can leave this unset to keep the
// handler cloud-only.
func (h *Handler) WithLocalRunner(r LocalPipelineRunner) *Handler {
	h.localRunner = r
	return h
}

// WithCloudRunner enables POST /pipeline/run to dispatch cloud pipelines
// through service.RunPipelineCloud — the same path the CLI uses. Tests
// can leave this unset; the handler then falls back to the inline SFN
// StartExecution dispatch (legacy behaviour, no force payload).
func (h *Handler) WithCloudRunner(r CloudPipelineRunner) *Handler {
	h.cloudRunner = r
	return h
}

// WithCloudLocalRunner enables POST /pipeline/run to dispatch a cloud-warehouse
// run through the local docker runner when the request body carries
// compute="local" (ADR-024). Tests can leave this unset; compute="local" then
// falls back to the SFN path.
func (h *Handler) WithCloudLocalRunner(r CloudLocalPipelineRunner) *Handler {
	h.cloudLocalRunner = r
	return h
}

// RegisterRoutes wires the handler into mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /pipeline/status", h.GetStatus)
	mux.HandleFunc("GET /pipeline/execution", h.GetExecutionDetail)
	mux.HandleFunc("GET /pipeline/execution/states", h.GetExecutionStates)
	mux.HandleFunc("GET /pipeline/execution/logs", h.GetExecutionLogs)
	mux.HandleFunc("POST /pipeline/run", h.RunPipeline)
}

// ---------------------------------------------------------------------------
// AWS client lazy-init
// ---------------------------------------------------------------------------

// ensureAWS lazily initializes SFN + CWL clients and the cloud provider that
// wraps them. All execution endpoints share the same lazy boundary so a
// missing AWS credentials chain produces one error, not three.
func (h *Handler) ensureAWS(ctx context.Context) {
	h.awsOnce.Do(func() {
		cfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			h.awsErr = err
			return
		}
		h.sfnClient = sfn.NewFromConfig(cfg)
		h.cwlClient = cloudwatchlogs.NewFromConfig(cfg)
		h.awsRegion = cfg.Region
		// Pass the workspace bucket + an S3 client so liveProgressStates can
		// LIST the runner's `_progress/<execARN>/<node>.json` objects and
		// colour the run DAG live. Without both, the provider short-circuits
		// and in-flight node states stay empty (the DAG never colours).
		h.cloud = observability.NewCloudProvider(nil, h.athenaOutputBucket, h.sfnClient, h.cwlClient).
			WithS3(s3.NewFromConfig(cfg))
	})
}

// ---------------------------------------------------------------------------
// GET /pipeline/status?dir=<dir>
// ---------------------------------------------------------------------------

type executionInfo struct {
	Name         string `json:"name"`
	Status       string `json:"status"`
	StartedAt    string `json:"started_at"`
	StoppedAt    string `json:"stopped_at,omitempty"`
	ConsoleURL   string `json:"console_url"`
	ExecutionARN string `json:"execution_arn"`
}

type executionDetail struct {
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
	Cause      string `json:"cause,omitempty"`
	FailedStep string `json:"failed_step,omitempty"`
	StepError  string `json:"step_error,omitempty"`
	StepCause  string `json:"step_cause,omitempty"`
}

type statusResponse struct {
	Deployed        bool            `json:"deployed"`
	Cloud           string          `json:"cloud,omitempty"`
	StateMachineARN string          `json:"state_machine_arn,omitempty"`
	Executions      []executionInfo `json:"executions"`
}

func (h *Handler) GetStatus(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: dir")
		return
	}
	abs := pathutil.ResolveDir(h.root, dir)

	// Local-mode dispatch (ADR-014, B P1-2 from 2026-05-24): instead of
	// hunting for terraform.tfstate we serve the run history out of
	// LocalProvider.Runs. A local pipeline has no SFN ARN, no tfstate —
	// it's still "deployed" in the sense that the runner image exists
	// and the pipeline is runnable today.
	if h.resolver != nil && h.resolver.IsLocal() {
		p, err := h.resolver.For(abs)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		res, err := p.Runs(r.Context(), observability.RunsQuery{
			PipelineName: filepath.Base(abs),
			PipelineDir:  abs,
			Limit:        20,
		})
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "list local runs: "+err.Error())
			return
		}
		execs := make([]executionInfo, 0, len(res.Rows))
		for _, run := range res.Rows {
			execs = append(execs, executionInfo{
				Name:         run.RunID,
				Status:       run.Status,
				StartedAt:    run.StartedAt,
				StoppedAt:    run.EndedAt,
				ConsoleURL:   "",
				ExecutionARN: formatLocalExecRef(abs, run.RunID),
			})
		}
		httputil.WriteJSON(w, http.StatusOK, statusResponse{
			Deployed:   true,
			Cloud:      "local",
			Executions: execs,
		})
		return
	}

	stateARN, err := readStateMachineARN(abs)
	if err != nil || stateARN == "" {
		httputil.WriteJSON(w, http.StatusOK, statusResponse{Deployed: false, Executions: []executionInfo{}})
		return
	}

	h.ensureAWS(r.Context())
	if h.awsErr != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "AWS client: "+h.awsErr.Error())
		return
	}

	execs, err := h.listExecutions(r.Context(), stateARN)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "list executions: "+err.Error())
		return
	}

	httputil.WriteJSON(w, http.StatusOK, statusResponse{
		Deployed:        true,
		Cloud:           "aws",
		StateMachineARN: stateARN,
		Executions:      execs,
	})
}

// formatLocalExecRef synthesises a recognisable execution reference for
// a local run so the same /pipeline/execution?arn=… endpoint serves
// both cloud and local. Prefix `local:` makes splitLocalExecRef easy;
// `#` (vs `:`) separates dir from runID so dirs containing colons
// round-trip (B P2-5).
func formatLocalExecRef(dir, runID string) string {
	return "local:" + dir + "#" + runID
}

// splitLocalExecRef is the inverse of formatLocalExecRef. Returns
// (dir, runID, ok) — ok=false means the input is a cloud ARN.
func splitLocalExecRef(ref string) (string, string, bool) {
	rest, ok := strings.CutPrefix(ref, "local:")
	if !ok {
		return "", "", false
	}
	dir, runID, ok := strings.Cut(rest, "#")
	if !ok {
		return "", "", false
	}
	return dir, runID, true
}

// readStateMachineARN reads terraform.tfstate from dir and extracts the ARN
// of the aws_sfn_state_machine.pipeline resource. Returns "" if absent or
// the resource is not found.
func readStateMachineARN(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, "terraform.tfstate"))
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

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
	if err := json.Unmarshal(data, &state); err != nil {
		return "", fmt.Errorf("parse tfstate: %w", err)
	}

	for _, res := range state.Resources {
		if res.Type == "aws_sfn_state_machine" && res.Name == "pipeline" {
			if len(res.Instances) > 0 {
				return res.Instances[0].Attributes.ARN, nil
			}
		}
	}
	return "", nil
}

// ---------------------------------------------------------------------------
// SFN ListExecutions
// ---------------------------------------------------------------------------

func (h *Handler) listExecutions(ctx context.Context, arnStr string) ([]executionInfo, error) {
	out, err := h.sfnClient.ListExecutions(ctx, &sfn.ListExecutionsInput{
		StateMachineArn: &arnStr,
		MaxResults:      20,
	})
	if err != nil {
		return nil, err
	}

	result := make([]executionInfo, 0, len(out.Executions))
	for _, e := range out.Executions {
		arn := aws.ToString(e.ExecutionArn)
		ei := executionInfo{
			Name:         nameFromARN(aws.ToString(e.Name)),
			Status:       string(e.Status),
			StartedAt:    observability.FormatTime(e.StartDate),
			ConsoleURL:   consoleURL(h.awsRegion, arn),
			ExecutionARN: arn,
		}
		if e.StopDate != nil {
			ei.StoppedAt = observability.FormatTime(e.StopDate)
		}
		result = append(result, ei)
	}
	// SFN ListExecutions is documented as most-recent-first, but in practice
	// the ordering isn't reliable across near-simultaneous starts (e.g. a
	// scheduled run plus two cross-pipeline triggers landing in the same
	// minute), so the "Recent executions" list rendered out of order. Sort
	// explicitly by start time, newest first — StartedAt is ISO-8601 UTC, so
	// a reverse string compare is chronological.
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].StartedAt > result[j].StartedAt
	})
	return result, nil
}

// ---------------------------------------------------------------------------
// GET /pipeline/execution?arn=<execution-arn>
// ---------------------------------------------------------------------------

// GetExecutionDetail returns error details for a single execution.
// For failed/timed-out executions it also scans the event history to identify
// which step failed and what error it produced.
func (h *Handler) GetExecutionDetail(w http.ResponseWriter, r *http.Request) {
	arn := r.URL.Query().Get("arn")
	if arn == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: arn")
		return
	}

	// Local-mode dispatch: arn is `local:<dir>#<runID>` synthesised by
	// GetStatus above. The failure context comes from the warehouse
	// `_run.json` run marker (ADR-024), read via the provider's RunDetail.
	if dir, runID, ok := splitLocalExecRef(arn); ok {
		if h.resolver == nil {
			httputil.WriteError(w, http.StatusInternalServerError, "no observability resolver configured")
			return
		}
		prov, err := h.providerForRun(dir, runID)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		rd, ok := prov.(runDetailer)
		if !ok {
			httputil.WriteError(w, http.StatusInternalServerError, "provider does not support run detail")
			return
		}
		detail, err := rd.RunDetail(r.Context(), runID)
		if err != nil {
			httputil.WriteError(w, http.StatusNotFound, "read run detail: "+err.Error())
			return
		}
		httputil.WriteJSON(w, http.StatusOK, executionDetail{
			Status:     detail.Status,
			Error:      detail.ErrorClass,
			Cause:      detail.ErrorMsg,
			FailedStep: detail.FailedStep,
			StepError:  detail.ErrorClass,
			StepCause:  detail.ErrorMsg,
		})
		return
	}

	h.ensureAWS(r.Context())
	if h.awsErr != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "AWS client: "+h.awsErr.Error())
		return
	}

	desc, err := h.sfnClient.DescribeExecution(r.Context(), &sfn.DescribeExecutionInput{
		ExecutionArn: &arn,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "describe execution: "+err.Error())
		return
	}

	detail := executionDetail{
		Status: string(desc.Status),
		Error:  aws.ToString(desc.Error),
		Cause:  aws.ToString(desc.Cause),
	}

	hist, err := h.sfnClient.GetExecutionHistory(r.Context(), &sfn.GetExecutionHistoryInput{
		ExecutionArn:         &arn,
		IncludeExecutionData: boolPtr(false),
	})
	if err == nil {
		detail.FailedStep, detail.StepError, detail.StepCause = findFailedStep(hist.Events)
	}

	httputil.WriteJSON(w, http.StatusOK, detail)
}

// ---------------------------------------------------------------------------
// GET /pipeline/execution/states?arn=<execution-arn>
// ---------------------------------------------------------------------------

// GetExecutionStates returns per-state status for one execution, designed to
// be polled (~2s) by the editor to overlay live DAG colors during a running
// execution.
//
// Two dispatch modes:
//   - dir=<dir>[&run=<id>]: route through the resolver (cloud or local based
//     on the workspace warehouse, ADR-024). Local pipelines must use this
//     form — ARNs don't exist locally.
//   - arn=<arn>: legacy cloud-only path; preserved while UI clients migrate.
func (h *Handler) GetExecutionStates(w http.ResponseWriter, r *http.Request) {
	if dir := r.URL.Query().Get("dir"); dir != "" && h.resolver != nil {
		run := r.URL.Query().Get("run")
		p, err := h.providerForRun(dir, run)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		ref := observability.FormatExecRef(dir, run)
		res, err := p.ExecutionStates(r.Context(), observability.ExecutionStatesQuery{
			ExecutionRef: ref,
		})
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		httputil.WriteJSON(w, http.StatusOK, res)
		return
	}

	arn := r.URL.Query().Get("arn")
	if arn == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: arn (or dir for local pipelines)")
		return
	}

	h.ensureAWS(r.Context())
	if h.awsErr != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "AWS client: "+h.awsErr.Error())
		return
	}

	res, err := h.cloud.ExecutionStates(r.Context(), observability.ExecutionStatesQuery{
		ExecutionRef: arn,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// GET /pipeline/execution/logs?arn=<execution-arn>&step=<state-name>
// ---------------------------------------------------------------------------

// GetExecutionLogs returns log lines for one step within one execution.
//
// Cloud serves CloudWatch FilterLogEvents output; local serves the captured
// runner stdout/stderr at <pipelineDir>/.clavesa/runs/<runID>/logs/.
// Dispatch follows the same dir-vs-arn convention as GetExecutionStates.
func (h *Handler) GetExecutionLogs(w http.ResponseWriter, r *http.Request) {
	step := r.URL.Query().Get("step")
	if step == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: step")
		return
	}

	if dir := r.URL.Query().Get("dir"); dir != "" && h.resolver != nil {
		run := r.URL.Query().Get("run")
		p, err := h.providerForRun(dir, run)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		ref := observability.FormatExecRef(dir, run)
		res, err := p.ExecutionLogs(r.Context(), observability.ExecutionLogsQuery{
			ExecutionRef: ref,
			Step:         step,
		})
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		httputil.WriteJSON(w, http.StatusOK, res)
		return
	}

	arn := r.URL.Query().Get("arn")
	if arn == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: arn (or dir for local pipelines)")
		return
	}

	h.ensureAWS(r.Context())
	if h.awsErr != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "AWS client: "+h.awsErr.Error())
		return
	}

	res, err := h.cloud.ExecutionLogs(r.Context(), observability.ExecutionLogsQuery{
		ExecutionRef: arn,
		Step:         step,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// POST /pipeline/run
// ---------------------------------------------------------------------------

type runRequest struct {
	Dir string `json:"dir"`
	// Force + ForceNodes mirror `clavesa pipeline run --force` /
	// `--force-node` — bypass the runner's incremental-skip check for
	// this run. Both optional; absent = false / empty. Threaded into
	// either StartRunWithOpts (local) or RunPipelineCloud (cloud)
	// without payload duplication.
	Force      bool     `json:"force,omitempty"`
	ForceNodes []string `json:"force_nodes,omitempty"`
	// Compute is the per-action execution placement (ADR-024). "" defaults to
	// the warehouse; "local" on a cloud warehouse runs the bundle in the local
	// docker runner against the cloud warehouse (response carries run_id with
	// the `local-` prefix). Ignored on a local warehouse (already local).
	Compute string `json:"compute,omitempty"`
}

type runResponse struct {
	// One of execution_arn (cloud) or run_id (local) is populated.
	ExecutionARN string `json:"execution_arn,omitempty"`
	RunID        string `json:"run_id,omitempty"`
	// For local runs: the per-node result table the CLI prints.
	Nodes any `json:"nodes,omitempty"`
}

// RunPipeline triggers a pipeline run. Local pipelines (any transform with
// `compute = "local"`) dispatch through service.RunPipeline; cloud pipelines
// start a Step Functions execution. ADR-014 / ADR-015 binds parity: the UI
// button works in both modes, response shape signals which path ran.
func (h *Handler) RunPipeline(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Dir == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir is required")
		return
	}
	abs := pathutil.ResolveDir(h.root, req.Dir)

	opts := RunOpts{Force: req.Force, ForceNodes: req.ForceNodes}

	// Local-first dispatch: if the resolver says this pipeline is local-
	// compute and a runner is wired, fire the in-process path. Same code
	// `clavesa pipeline run` uses. Falls through to cloud (SFN start)
	// when the resolver returns cloud or isn't wired.
	if h.localRunner != nil && h.isLocalCompute(abs) {
		// StartRunWithOpts dispatches asynchronously: it prepares the run
		// (so the run id + RUNNING progress channel exist) and returns
		// the id immediately, then walks the DAG in the background. The
		// UI navigates to /pipelines/run with this id and polls the
		// progress channel — no more blocking for the whole run.
		runID, err := h.localRunner.StartRunWithOpts(abs, opts)
		if err != nil {
			if errors.Is(err, ErrRunInFlight) {
				httputil.WriteError(w, http.StatusConflict, err.Error())
				return
			}
			httputil.WriteError(w, http.StatusInternalServerError, "local run: "+err.Error())
			return
		}
		httputil.WriteJSON(w, http.StatusOK, runResponse{RunID: runID})
		return
	}

	// Cloud warehouse, local compute (ADR-024): run the whole pipeline bundle
	// in the local docker runner against the cloud warehouse. Returns a
	// `local-<uuid>` run id the UI routes to the filesystem progress channel.
	if req.Compute == "local" && h.cloudLocalRunner != nil {
		runID, err := h.cloudLocalRunner.StartRunCloudLocal(r.Context(), abs, opts)
		if err != nil {
			if errors.Is(err, ErrRunInFlight) {
				httputil.WriteError(w, http.StatusConflict, err.Error())
				return
			}
			// A non-nil run id with an error means the bundle started and
			// failed mid-run — surface the id so the UI can open the run
			// detail; still report the failure status.
			if runID != "" {
				httputil.WriteJSON(w, http.StatusOK, runResponse{RunID: runID})
				return
			}
			httputil.WriteError(w, http.StatusInternalServerError, "cloud-local run: "+err.Error())
			return
		}
		httputil.WriteJSON(w, http.StatusOK, runResponse{RunID: runID})
		return
	}

	// Cloud dispatch. Prefer the wired CloudPipelineRunner (production
	// path) — it lifts the SFN client construction + execution-input
	// payload into the service so CLI and UI share one code path and
	// the force flags flow through. Falls back to the inline SFN call
	// only when no runner is wired (preserves pre-Slice-C handler tests
	// that don't construct a full service).
	if h.cloudRunner != nil {
		execARN, err := h.cloudRunner.RunPipelineCloud(r.Context(), abs, opts)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "start execution: "+err.Error())
			return
		}
		httputil.WriteJSON(w, http.StatusOK, runResponse{ExecutionARN: execARN})
		return
	}

	stateARN, err := readStateMachineARN(abs)
	if err != nil || stateARN == "" {
		httputil.WriteError(w, http.StatusBadRequest, "pipeline not deployed (no terraform.tfstate or state machine ARN not found)")
		return
	}

	h.ensureAWS(r.Context())
	if h.awsErr != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "AWS client: "+h.awsErr.Error())
		return
	}

	out, err := h.sfnClient.StartExecution(r.Context(), &sfn.StartExecutionInput{
		StateMachineArn: &stateARN,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "start execution: "+err.Error())
		return
	}

	httputil.WriteJSON(w, http.StatusOK, runResponse{ExecutionARN: aws.ToString(out.ExecutionArn)})
}

// isLocalCompute consults the observability resolver to decide whether the
// workspace operates locally. The resolver encapsulates the local/cloud
// routing rule (the workspace warehouse); reusing it here keeps
// that rule in one place. Without a resolver wired (test mode), the
// handler defaults to cloud — preserves the legacy behavior.
func (h *Handler) isLocalCompute(_ string) bool {
	if h.resolver == nil {
		return false
	}
	return h.resolver.IsLocal()
}

// providerForRun picks the observability provider for a states/logs request.
// Routing is purely by warehouse via the resolver (ADR-024): a cloud-local
// run (cloud warehouse, local compute) is read by the cloud provider off the
// S3 `_progress` tree the runner wrote there, exactly like a fully-cloud run
// — the provider seam reads the warehouse, not SFN, for per-node state, so
// the old `local-`-prefix special-case (which forced the local provider) is
// no longer needed. The run id no longer steers dispatch.
func (h *Handler) providerForRun(dir, run string) (observability.Provider, error) {
	return h.resolver.For(dir)
}

// runDetailer is the narrow capability the local execution-detail path needs:
// reading the warehouse `_run.json` run marker for failure context. Both the
// cloud and local providers satisfy it; asserted (not part of the Provider
// interface) so the seam stays minimal.
type runDetailer interface {
	RunDetail(ctx context.Context, run string) (observability.RunDetail, error)
}

func boolPtr(b bool) *bool { return &b }

// findFailedStep scans SFN history events (forward order) to find the last
// state that was entered before a task failure and returns its name along
// with the error/cause from the TaskFailed event. Used by the
// /pipeline/execution detail endpoint to populate failed_step on the
// response. The execution-states endpoint instead reads the warehouse
// `_progress` markers via the observability providers (3be08e3) and never
// walks SFN history for per-node status.
func findFailedStep(events []sfntypes.HistoryEvent) (step, errCode, cause string) {
	lastState := ""
	for _, ev := range events {
		switch ev.Type {
		case sfntypes.HistoryEventTypeTaskStateEntered:
			if ev.StateEnteredEventDetails != nil {
				lastState = aws.ToString(ev.StateEnteredEventDetails.Name)
			}
		case sfntypes.HistoryEventTypeTaskFailed:
			if ev.TaskFailedEventDetails != nil {
				return lastState,
					aws.ToString(ev.TaskFailedEventDetails.Error),
					aws.ToString(ev.TaskFailedEventDetails.Cause)
			}
		case sfntypes.HistoryEventTypeExecutionFailed:
			if ev.ExecutionFailedEventDetails != nil {
				return "",
					aws.ToString(ev.ExecutionFailedEventDetails.Error),
					aws.ToString(ev.ExecutionFailedEventDetails.Cause)
			}
		}
	}
	return "", "", ""
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// nameFromARN returns the last segment of an ARN (execution name).
func nameFromARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) == 0 {
		return arn
	}
	return parts[len(parts)-1]
}

// consoleURL builds the AWS console URL for a Step Functions execution.
func consoleURL(region, execARN string) string {
	return fmt.Sprintf(
		"https://%s.console.aws.amazon.com/states/home?region=%s#/v2/executions/details/%s",
		region, region, execARN,
	)
}
