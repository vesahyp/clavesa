package service

import (
	"context"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pricing"
)

// PipelineCostForDir returns the cost-per-billion-records rollup for the named
// pipeline, computed from its last `lastN` runner invocations.
//
// It reuses the SAME local/cloud provider dispatch as the dashboard's
// node-runs (via dashboardProvider), so the figure is identical-shaped across
// compute targets — ADR-014 local/cloud parity. The CLI wrapper and the
// `/data/pipeline-cost` HTTP handler both reach this one aggregation, ADR-015.
//
// IncludeMetrics forces the metrics-bearing SQL scan so input_records /
// memory_mb are populated; the local provider's state.json fast path omits
// them. The workspace-shared system DB means any pipeline dir resolves the
// same local warehouse, so the workspace root is passed as the dispatch dir.
func (s *Service) PipelineCostForDir(ctx context.Context, pipeline string, lastN int) (pricing.PipelineCost, error) {
	prov, err := s.dashboardProvider()
	if err != nil {
		return pricing.PipelineCost{}, err
	}
	res, err := prov.NodeRuns(ctx, observability.NodeRunsQuery{
		PipelineName:   pipeline,
		Database:       s.systemGlueDB(),
		PipelineDir:    s.workspace,
		Limit:          lastN,
		IncludeMetrics: true,
	})
	if err != nil {
		return pricing.PipelineCost{}, err
	}
	return pricing.AggregateCost(pipeline, res.Rows), nil
}
