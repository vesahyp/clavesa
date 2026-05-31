package service

import (
	"context"

	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// systemGlueDB returns the workspace system catalog DB
// (`<system_catalog>__pipelines`) — where runs / node_runs / column_stats
// live. Used by the rightsize aggregation to scope its node_runs scan.
func (s *Service) systemGlueDB() string {
	catalog := ""
	if m, _ := workspace.Load(s.workspace); m != nil {
		catalog = m.SystemCatalogIdentifier()
	}
	if catalog == "" {
		// No manifest (bare directory) — defensive fallback. Fresh
		// workspaces always have a manifest, so this only bites tests
		// that skip workspace init.
		return "clavesa_system__pipelines"
	}
	return identutil.EncodeGlueDatabase(catalog, "pipelines")
}

// Rightsize returns a per-node memory recommendation for the named pipeline,
// computed from its last `lastN` runner invocations. Recommend-only: it
// reads node_runs and returns advice; it never mutates the pipeline.
//
// The CLI (`clavesa pipeline rightsize`) and the run-detail node drawer both
// reach the same aggregation through this method (CLI) and the
// `/data/rightsize` handler (UI) — ADR-015 parity, ADR-014 local/cloud
// parity (the provider is picked by workspace mode, same shapes either way).
//
// IncludeMetrics forces the metrics-bearing SQL scan; the local provider's
// state.json fast path omits the Spark-metric columns this needs. The
// workspace-shared system DB means any pipeline dir resolves the same local
// warehouse, so we pass the workspace root as the dir for dispatch.
func (s *Service) Rightsize(ctx context.Context, pipeline string, lastN int) ([]observability.NodeRightsize, error) {
	prov, err := s.dashboardProvider()
	if err != nil {
		return nil, err
	}
	res, err := prov.NodeRuns(ctx, observability.NodeRunsQuery{
		PipelineName:   pipeline,
		Database:       s.systemGlueDB(),
		PipelineDir:    s.workspace,
		Limit:          lastN,
		IncludeMetrics: true,
	})
	if err != nil {
		return nil, err
	}
	return observability.Rightsize(res.Rows), nil
}
