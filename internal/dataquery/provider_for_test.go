package dataquery

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// TestProviderForEmptyDirUsesResolverWorkspace is the regression guard for the
// catalog Rows/Commits columns reading blank on every workspace system table
// (runs, node_runs, column_stats, tables). Those tables have no owning pipeline
// dir, so the snapshots/column-stats requests arrive with an empty `dir`. The
// empty-dir cloud path used to fall back to the handler's bare `h.cloud` — a
// CloudProvider built by NewHandler WITHOUT Glue/S3, so Snapshots (which reads
// the Delta `_delta_log`) returned empty. It must instead route to the
// resolver's workspace provider, which IS wired with Glue + S3 (the same one
// For(dir) returns for pipeline-owned tables).
func TestProviderForEmptyDirUsesResolverWorkspace(t *testing.T) {
	ws := t.TempDir()
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
		t.Fatalf("set cloud warehouse: %v", err)
	}

	// The resolver's cloud provider is the fully-wired one (in production it
	// carries WithGlue/WithS3); the handler's own h.cloud is the bare fallback.
	// Distinct instances so identity reveals which path providerFor took.
	resolverCloud := observability.NewCloudProvider(nil, "resolver-bucket", nil, nil)
	resolverLocal := observability.NewLocalProvider(ws)
	res := observability.NewResolver(ws, resolverCloud, resolverLocal)

	h := &Handler{
		cloud:    observability.NewCloudProvider(nil, "bare-handler-cloud", nil, nil),
		resolver: res,
	}

	// Empty dir — a workspace system-table query.
	req := httptest.NewRequest(http.MethodGet, "/data/tables/db/runs/snapshots", nil)
	w := httptest.NewRecorder()
	p, ok := h.providerFor(w, req)
	if !ok {
		t.Fatalf("providerFor returned not-ok: %d %s", w.Code, w.Body.String())
	}
	if p != observability.Provider(resolverCloud) {
		t.Errorf("empty-dir cloud query routed to the bare h.cloud fallback; want the resolver's wired workspace provider (system-table Rows/Commits would read blank)")
	}
}
