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
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pathutil"
)

// LocalPipelineRunner is the local-execution path used when a pipeline has
// any `compute = "local"` transform. StartRun begins the run
// asynchronously and returns a run id immediately so the UI can navigate
// to the run page without blocking; it returns ErrRunInFlight when the
// pipeline already has a run executing. Implemented by service.Service;
// the interface lives here so internal/pipelinestatus stays free of an
// internal/service import — ui.go wires a bridge.
type LocalPipelineRunner interface {
	StartRun(dir string) (string, error)
}

// ErrRunInFlight is the sentinel a LocalPipelineRunner returns when a run
// for the pipeline is already executing. The bridge in ui.go maps
// service.ErrRunInFlight onto this so the handler can answer 409 without
// importing internal/service.
var ErrRunInFlight = errors.New("a run is already in progress for this pipeline")

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

	// resolver, when set, lets states/logs dispatch per-pipeline based on
	// `compute` attr (ADR-014). When nil, the handler falls through to the
	// cloud-only ARN path — preserves the pre-resolver call shape for tests.
	resolver *observability.Resolver

	// localRunner, when set, lets POST /pipeline/run dispatch
	// compute = "local" pipelines through service.RunPipeline (the same
	// code path `clavesa pipeline run` uses). Without it, all run
	// requests fall through to the SFN StartExecution path.
	localRunner LocalPipelineRunner
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

// WithLocalRunner enables POST /pipeline/run to dispatch local pipelines
// through service.RunPipeline. Tests can leave this unset to keep the
// handler cloud-only.
func (h *Handler) WithLocalRunner(r LocalPipelineRunner) *Handler {
	h.localRunner = r
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
		h.cloud = observability.NewCloudProvider(nil, "", h.sfnClient, h.cwlClient)
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
		arn := derefStr(e.ExecutionArn)
		ei := executionInfo{
			Name:         nameFromARN(derefStr(e.Name)),
			Status:       string(e.Status),
			StartedAt:    formatTime(e.StartDate),
			ConsoleURL:   consoleURL(h.awsRegion, arn),
			ExecutionARN: arn,
		}
		if e.StopDate != nil {
			ei.StoppedAt = formatTime(e.StopDate)
		}
		result = append(result, ei)
	}
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
		Error:  derefStr(desc.Error),
		Cause:  derefStr(desc.Cause),
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
//     on the inspected pipeline's compute attr). Local pipelines must use
//     this form — ARNs don't exist locally.
//   - arn=<arn>: legacy cloud-only path; preserved while UI clients migrate.
func (h *Handler) GetExecutionStates(w http.ResponseWriter, r *http.Request) {
	if dir := r.URL.Query().Get("dir"); dir != "" && h.resolver != nil {
		p, err := h.resolver.For(dir)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		ref := observability.FormatExecRef(dir, r.URL.Query().Get("run"))
		res, err := p.ExecutionStates(r.Context(), observability.ExecutionStatesQuery{
			ExecutionRef: ref,
		})
		if err != nil {
			writeProviderError(w, err)
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
		p, err := h.resolver.For(dir)
		if err != nil {
			httputil.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		ref := observability.FormatExecRef(dir, r.URL.Query().Get("run"))
		res, err := p.ExecutionLogs(r.Context(), observability.ExecutionLogsQuery{
			ExecutionRef: ref,
			Step:         step,
		})
		if err != nil {
			writeProviderError(w, err)
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

// writeProviderError translates a provider error into the right HTTP status.
// LocalProvider's not-yet-implemented sentinel becomes 501; everything else
// is 500. Keeps the UI's error rendering predictable across backends.
func writeProviderError(w http.ResponseWriter, err error) {
	if err == observability.ErrLocalNotImplemented {
		httputil.WriteError(w, http.StatusNotImplemented, err.Error())
		return
	}
	httputil.WriteError(w, http.StatusInternalServerError, err.Error())
}

// ---------------------------------------------------------------------------
// POST /pipeline/run
// ---------------------------------------------------------------------------

type runRequest struct {
	Dir string `json:"dir"`
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

	// Local-first dispatch: if the resolver says this pipeline is local-
	// compute and a runner is wired, fire the in-process path. Same code
	// `clavesa pipeline run` uses. Falls through to cloud (SFN start)
	// when the resolver returns cloud or isn't wired.
	if h.localRunner != nil && h.isLocalCompute(abs) {
		// StartRun dispatches asynchronously: it prepares the run (so the
		// run id + RUNNING progress channel exist) and returns the id
		// immediately, then walks the DAG in the background. The UI
		// navigates to /pipelines/run with this id and polls the
		// progress channel — no more blocking for the whole run.
		runID, err := h.localRunner.StartRun(abs)
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

	httputil.WriteJSON(w, http.StatusOK, runResponse{ExecutionARN: derefStr(out.ExecutionArn)})
}

// isLocalCompute consults the observability resolver to decide whether the
// workspace operates locally. The resolver encapsulates the local/cloud
// routing rule (the workspace environment mode); reusing it here keeps
// that rule in one place. Without a resolver wired (test mode), the
// handler defaults to cloud — preserves the legacy behavior.
func (h *Handler) isLocalCompute(_ string) bool {
	if h.resolver == nil {
		return false
	}
	return h.resolver.IsLocal()
}

func boolPtr(b bool) *bool { return &b }

// findFailedStep scans SFN history events (forward order) to find the last
// state that was entered before a task failure and returns its name along
// with the error/cause from the TaskFailed event. Used by the
// /pipeline/execution detail endpoint to populate failed_step on the
// response. The execution-states endpoint instead consumes
// observability.StateStatusesFromHistory which produces the full status map.
func findFailedStep(events []sfntypes.HistoryEvent) (step, errCode, cause string) {
	lastState := ""
	for _, ev := range events {
		switch ev.Type {
		case sfntypes.HistoryEventTypeTaskStateEntered:
			if ev.StateEnteredEventDetails != nil {
				lastState = derefStr(ev.StateEnteredEventDetails.Name)
			}
		case sfntypes.HistoryEventTypeTaskFailed:
			if ev.TaskFailedEventDetails != nil {
				return lastState,
					derefStr(ev.TaskFailedEventDetails.Error),
					derefStr(ev.TaskFailedEventDetails.Cause)
			}
		case sfntypes.HistoryEventTypeExecutionFailed:
			if ev.ExecutionFailedEventDetails != nil {
				return "",
					derefStr(ev.ExecutionFailedEventDetails.Error),
					derefStr(ev.ExecutionFailedEventDetails.Cause)
			}
		}
	}
	return "", "", ""
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func formatTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

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
