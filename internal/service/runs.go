package service

import (
	"context"
	"fmt"

	"github.com/vesahyp/clavesa/internal/observability"
)

// Runs returns recent Step Functions / local-run executions for the
// pipeline at `dir` — the service-layer seam the CLI reads through so it
// returns rows identical to the /data/runs HTTP handler (ADR-015). `limit`
// <=0 leaves the query's Limit at zero so the provider applies its own
// default (50), matching the handler's behavior when no `limit` query
// param is set.
//
// Provider dispatch follows the workspace warehouse via the resolver
// (ADR-024), same as ExecutionStates above.
func (s *Service) Runs(ctx context.Context, dir string, limit int) (*observability.RunsResult, error) {
	if s.dashResolver == nil {
		return nil, fmt.Errorf("runs: observability resolver not configured")
	}
	prov, err := s.dashResolver.For(dir)
	if err != nil {
		return nil, err
	}
	q := observability.RunsQuery{
		// PipelineName must match what handleRuns receives from the UI's
		// `pipeline` query param — the dir-derived, dash-preserving name
		// (PipelineDashboard.tsx: "Pipeline names land in node_runs/runs
		// as the literal pipeline_name var.tf value"). Resolver.PipelineName
		// is the same filepath.Base(dir) computation ListPipelines uses to
		// populate that param, so CLI and UI resolve to byte-identical rows.
		PipelineName: s.dashResolver.PipelineName(dir),
		Database:     s.systemGlueDB(),
		PipelineDir:  dir,
	}
	if limit > 0 {
		q.Limit = limit
	}
	return prov.Runs(ctx, q)
}

// NodeRuns returns per-node runner invocations for the pipeline at `dir`,
// optionally narrowed to one execution (`run`). Mirrors /data/node-runs;
// CLI and UI share this exact query shape (ADR-015).
//
// Local dispatch deliberately avoids setting SfExecutionARN on the query:
// a non-empty SfExecutionARN forces LocalProvider.NodeRuns off its
// `_progress`-marker fast path and onto the Spark-container query path
// (local.go's nodeRunsFromProgress guard), which is far slower for what
// is otherwise a cheap filesystem read. Instead we fetch every row the
// fast path already returns and filter by run id in the wrapper — NodeRun
// carries the local run id in RunID (nodeRunsFromProgress stamps it there;
// SfExecutionARN is set to the same value locally, but RunID is the
// documented join key). Cloud dispatch passes SfExecutionARN straight
// through: Athena filters server-side at no extra cost, and the cloud
// provider has no cheaper fast path to preserve.
func (s *Service) NodeRuns(ctx context.Context, dir, run string, limit int) (*observability.NodeRunsResult, error) {
	if s.dashResolver == nil {
		return nil, fmt.Errorf("node runs: observability resolver not configured")
	}
	prov, err := s.dashResolver.For(dir)
	if err != nil {
		return nil, err
	}
	q := observability.NodeRunsQuery{
		PipelineName: s.dashResolver.PipelineName(dir),
		Database:     s.systemGlueDB(),
		PipelineDir:  dir,
	}
	if limit > 0 {
		q.Limit = limit
	}

	if s.dashResolver.IsLocal() {
		res, err := prov.NodeRuns(ctx, q)
		if err != nil {
			return nil, err
		}
		if run == "" {
			return res, nil
		}
		filtered := &observability.NodeRunsResult{
			Rows:      make([]observability.NodeRun, 0, len(res.Rows)),
			Truncated: res.Truncated,
		}
		for _, r := range res.Rows {
			if r.RunID == run {
				filtered.Rows = append(filtered.Rows, r)
			}
		}
		return filtered, nil
	}

	q.SfExecutionARN = run
	return prov.NodeRuns(ctx, q)
}

// ExecutionLogs returns the log events for one node within one execution,
// mirroring GET /pipeline/execution/logs (ADR-015). `run` is optional
// (empty means "the most recent run"); `step` names the node/Lambda whose
// logs to fetch and `maxLines` caps the response (<=0 keeps each
// provider's existing default cap).
//
// Both providers require a non-empty Step. Locally the value is cosmetic
// (LocalProvider.ExecutionLogs ignores its content — the bundle runner
// shares one log across every node in a run), so an omitted step defaults
// to "run". On the cloud provider a step selects which per-Lambda
// CloudWatch log group to read, so there is no safe default: an omitted
// step there is a user error, and we say so rather than let the
// provider's generic "execution ref and step are required" surface.
func (s *Service) ExecutionLogs(ctx context.Context, dir, run, step string, maxLines int) (*observability.ExecutionLogsResult, error) {
	if s.dashResolver == nil {
		return nil, fmt.Errorf("execution logs: observability resolver not configured")
	}
	prov, err := s.dashResolver.For(dir)
	if err != nil {
		return nil, err
	}
	if step == "" {
		if s.dashResolver.IsLocal() {
			step = "run"
		} else {
			return nil, fmt.Errorf("execution logs: cloud runs are addressed per-Lambda-step; pass --node <step>")
		}
	}
	return prov.ExecutionLogs(ctx, observability.ExecutionLogsQuery{
		ExecutionRef: observability.FormatExecRef(dir, run),
		Step:         step,
		MaxLines:     maxLines,
	})
}
