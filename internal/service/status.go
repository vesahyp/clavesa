package service

import (
	"context"
	"fmt"

	"github.com/vesahyp/clavesa/internal/observability"
)

// ExecutionStates returns the per-node execution state for the latest run of
// the pipeline at `dir` (or a specific run when `run` is non-empty). It's the
// service-layer seam the CLI's `pipeline status` reads through, mirroring the
// /pipeline/execution/states HTTP handler so both surfaces consume identical
// shapes (ADR-015). Provider is picked per the workspace warehouse via the
// resolver (ADR-024) — both providers read the warehouse `_progress` marker
// tree (local: filesystem, cloud: S3; cloud additionally consults SFN for
// the overall status of ARN-addressed runs).
//
// `dir` is the pipeline directory (absolute or workspace-relative). `run` is
// optional: empty means "the most recent run".
func (s *Service) ExecutionStates(ctx context.Context, dir, run string) (*observability.ExecutionStatesResult, error) {
	if s.dashResolver == nil {
		return nil, fmt.Errorf("execution states: observability resolver not configured")
	}
	prov, err := s.dashResolver.For(dir)
	if err != nil {
		return nil, err
	}
	return prov.ExecutionStates(ctx, observability.ExecutionStatesQuery{
		ExecutionRef: observability.FormatExecRef(dir, run),
	})
}
