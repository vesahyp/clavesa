package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// QueryOptions configures one ad-hoc Query call. The zero value is the
// common case: follow the workspace warehouse, scope to the workspace
// root, no row cap.
type QueryOptions struct {
	// Dir is the pipeline dir scoping the provider's reference. The
	// warehouse is workspace-shared, so any dir in the workspace resolves
	// the same catalog (see observability.ExecQuery); empty falls back to
	// the workspace root, which satisfies the local provider's non-empty
	// reference guard.
	Dir string
	// MaxRows caps the rows returned even when the SQL has no LIMIT.
	// Zero means no cap — the SQL is trusted to bound itself (the CLI's
	// contract; HTTP callers pass their route cap).
	MaxRows int
	// Warehouse overrides the workspace warehouse for this query only
	// (the `--warehouse local|cloud` flag, ADR-024). Empty follows the
	// workspace's persisted warehouse.
	Warehouse workspace.Warehouse
}

// Query runs one free-form ad-hoc SQL statement against the workspace
// catalog — the shared seam behind both `clavesa query` and the UI's
// POST /data/query (ADR-015: same service call, same dispatch, same
// dialect handling on both surfaces).
//
// Dispatch follows the workspace warehouse (ADR-024) unless
// opts.Warehouse overrides it. Either way the authored SparkSQL is
// validated for Trino/Athena portability first (ADR-023, via
// TranspileServing — the same cached transpiler + gate the dashboard
// save path applies on both warehouses), so a query that runs here is
// guaranteed to run as a cloud dashboard widget — no local/cloud dialect
// surprise. What executes differs: cloud runs the transpiled Trino
// through the cloud provider (Athena); local has no Trino engine, so it
// runs the authored SparkSQL through the local provider (warm runner
// Spark against the Hadoop catalog) once the portability check passes.
// A transpile rejection surfaces as *DialectError on either warehouse.
//
// Requires WithResolver; WithTranspiler is nil-safe (pass-through, the
// standard docker-free-test contract) but production cloud wiring should
// always provide it — untranspiled Spark-only constructs would otherwise
// reach Athena verbatim.
func (s *Service) Query(ctx context.Context, sql string, opts QueryOptions) (*observability.QueryResult, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("query: sql is required")
	}
	if s.dashResolver == nil {
		return nil, fmt.Errorf("query: observability resolver not configured")
	}

	wh := opts.Warehouse
	if wh == "" {
		wh = workspace.LoadWarehouse(s.workspace)
	}
	prov, err := s.dashResolver.ForWarehouse(wh)
	if err != nil {
		return nil, err
	}

	// Both warehouses gate on Trino/Athena portability (same as the
	// dashboard save path) so local can't accept Spark-only syntax that
	// would break once the query is a cloud dashboard widget. Cloud also
	// dispatches the transpiled form (Athena speaks Trino); local discards
	// the Trino output and runs the authored Spark on its own engine.
	//
	// Undeployed is an error here, not an empty result: the cloud
	// provider's soft undeployed() path exists for dashboard widgets
	// (render empty, don't 500 the dashboard), but an interactive ad-hoc
	// query answering "0 rows, success" on a workspace with no Athena
	// catalog would be silently wrong. Checked against the *effective*
	// warehouse, so `--warehouse cloud` on a local-warehouse workspace
	// gets the same actionable error.
	sqlToRun := sql
	transpiled := false
	if wh == workspace.WarehouseCloud {
		if workspace.PipelineBucket(s.workspace) == "" {
			return nil, fmt.Errorf("query targets the cloud warehouse, %w — run `clavesa workspace deploy` to create it, or query the local warehouse with `--warehouse local` / `clavesa workspace use --warehouse local`", workspace.ErrWarehouseUndeployed)
		}
		sqlToRun, err = s.TranspileServing(ctx, sql)
		if err != nil {
			return nil, err
		}
		// TranspileServing is a pass-through when no transpiler is wired
		// (docker-free tests) — only claim a transpile that actually ran.
		transpiled = s.transpiler != nil
	} else {
		// Local portability gate: transpile only to surface a *DialectError
		// for non-portable SparkSQL, then discard the Trino output and run
		// the authored Spark (sqlToRun stays sql; transpiled stays false).
		// Pass-through when no transpiler is wired, so docker-free tests and
		// transpiler-less wiring keep running raw Spark with no gate.
		if _, err = s.TranspileServing(ctx, sql); err != nil {
			return nil, err
		}
	}

	dir := opts.Dir
	if dir == "" {
		dir = s.workspace
	}
	res, err := prov.Query(ctx, observability.QueryQuery{
		SQL:         sqlToRun,
		PipelineDir: dir,
		MaxRows:     opts.MaxRows,
		// This is the interactive ad-hoc seam (`clavesa query`, the UI's
		// /data/query panel). A query against a table that doesn't exist
		// must error, not answer "0 rows, success" — the same reasoning
		// as the undeployed-warehouse guard above. The soft missing-table
		// path is for non-ad-hoc surfaces (dashboard-live widgets,
		// catalog) that call prov.Query directly.
		StrictMissing: true,
	})
	if err != nil {
		return nil, err
	}
	// The provider stamps engine + warehouse on Served (it executed the
	// SQL); only this seam knows a SparkSQL→Trino transpile happened, so
	// the transpile flag is set here (ADR-024).
	if res.Served != nil && transpiled {
		res.Served.Transpiled = true
	}
	return res, nil
}
