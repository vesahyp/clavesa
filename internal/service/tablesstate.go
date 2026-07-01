package service

import (
	"context"

	"github.com/vesahyp/clavesa/internal/observability"
)

// TablesStateForDir returns the current-state-per-table summary (row / file
// count and byte size of each output table's latest snapshot) for the
// pipeline at `dir`.
//
// It reuses the SAME local/cloud provider dispatch as the dashboard's
// tables-state (via dashboardProvider), so the figures are identical-shaped
// across compute targets — ADR-014 local/cloud parity. The `workspace tables`
// CLI enrichment and the `/data/tables-state` HTTP handler both reach this
// one provider call (ADR-015).
//
// `pipeline` is the row-filter name (the sanitized pipeline / schema form the
// runner writes into the `tables` table's `pipeline` column). `dir` is the
// on-disk pipeline directory (workspace-relative or absolute); the local
// provider needs it to locate the Delta warehouse for the fast path, so —
// unlike the workspace-shared node_runs scans — it must be the real pipeline
// dir, not the workspace root.
func (s *Service) TablesStateForDir(ctx context.Context, pipeline, dir string, limit int) (*observability.TablesResult, error) {
	prov, err := s.dashboardProvider()
	if err != nil {
		return nil, err
	}
	return prov.Tables(ctx, observability.TablesQuery{
		PipelineName: pipeline,
		Database:     s.systemGlueDB(),
		PipelineDir:  dir,
		Limit:        limit,
	})
}
