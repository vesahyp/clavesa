package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// queryRecordingProvider wraps fakeProvider to record the full QueryQuery
// (fakeProvider only records SQL), so Query tests can assert the
// PipelineDir / MaxRows threading.
type queryRecordingProvider struct {
	fakeProvider
	lastQuery observability.QueryQuery
	hits      int
}

func (p *queryRecordingProvider) Query(ctx context.Context, q observability.QueryQuery) (*observability.QueryResult, error) {
	p.lastQuery = q
	p.hits++
	return p.fakeProvider.Query(ctx, q)
}

// queryService builds a Service over a fresh temp workspace wired with
// distinct local and cloud recording providers plus a marker transpiler,
// so each test can assert which provider was dispatched and whether the
// transpile hook ran. Returns the workspace root for warehouse writes.
func queryService(t *testing.T) (*Service, string, *queryRecordingProvider, *queryRecordingProvider, *fakeTranspiler) {
	t.Helper()
	ws := t.TempDir()
	local := &queryRecordingProvider{}
	cloud := &queryRecordingProvider{}
	tr := &fakeTranspiler{
		toServing: func(_ context.Context, sparkSQL string) (string, error) {
			return "TRINO:" + sparkSQL, nil
		},
	}
	resolver := observability.NewResolver(ws, cloud, local)
	return New(ws).WithResolver(resolver).WithTranspiler(tr), ws, local, cloud, tr
}

// markDeployed writes the minimal tfstate that makes the workspace look
// deployed (PipelineBucket non-empty) — the cloud Query path refuses to
// dispatch against an undeployed workspace rather than returning the
// provider's soft empty result.
func markDeployed(t *testing.T, ws string) {
	t.Helper()
	tfstate := `{
  "version": 4,
  "outputs": {
    "pipeline_bucket": { "value": "clavesa-test-bucket", "type": "string" }
  }
}`
	if err := os.WriteFile(filepath.Join(ws, "terraform.tfstate"), []byte(tfstate), 0o644); err != nil {
		t.Fatalf("write tfstate: %v", err)
	}
}

func TestQueryCloudUndeployedErrors(t *testing.T) {
	ctx := context.Background()
	s, ws, local, cloud, tr := queryService(t)
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
		t.Fatal(err)
	}
	// No tfstate written — the workspace is cloud-warehouse but undeployed.
	_, err := s.Query(ctx, "SELECT 1", QueryOptions{})
	if !errors.Is(err, workspace.ErrWarehouseUndeployed) {
		t.Fatalf("err = %v, want ErrWarehouseUndeployed", err)
	}
	if local.hits != 0 || cloud.hits != 0 {
		t.Errorf("undeployed must not dispatch: local=%d cloud=%d", local.hits, cloud.hits)
	}
	if len(tr.seen) != 0 {
		t.Errorf("undeployed must not transpile: %v", tr.seen)
	}
}

func TestQueryLocalWarehouse(t *testing.T) {
	ctx := context.Background()
	s, ws, local, cloud, tr := queryService(t)

	res, err := s.Query(ctx, "SELECT 1", QueryOptions{MaxRows: 42})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res == nil {
		t.Fatal("Query returned nil result")
	}
	if local.hits != 1 || cloud.hits != 0 {
		t.Fatalf("dispatch: local=%d cloud=%d, want local=1 cloud=0", local.hits, cloud.hits)
	}
	// Local gates on Trino/Athena portability (the transpiler runs as a
	// check) but executes the authored Spark unchanged — the transpiled
	// form is never dispatched on the local path.
	if len(tr.seen) != 1 || tr.seen[0] != "SELECT 1" {
		t.Errorf("local portability gate: transpiler saw %v, want the authored Spark once", tr.seen)
	}
	if got := local.lastQuery.SQL; got != "SELECT 1" {
		t.Errorf("SQL = %q, want authored Spark unchanged (not the transpiled form)", got)
	}
	// Empty Dir falls back to the workspace root (the local provider's
	// non-empty reference guard; the warehouse is workspace-shared).
	if got := local.lastQuery.PipelineDir; got != ws {
		t.Errorf("PipelineDir = %q, want workspace root %q", got, ws)
	}
	if got := local.lastQuery.MaxRows; got != 42 {
		t.Errorf("MaxRows = %d, want 42", got)
	}
}

func TestQueryCloudWarehouseTranspiles(t *testing.T) {
	ctx := context.Background()
	s, ws, local, cloud, tr := queryService(t)
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
		t.Fatal(err)
	}
	markDeployed(t, ws)

	if _, err := s.Query(ctx, "SELECT datediff(d2, d1) FROM t", QueryOptions{Dir: "demo"}); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if cloud.hits != 1 || local.hits != 0 {
		t.Fatalf("dispatch: local=%d cloud=%d, want cloud=1 local=0", local.hits, cloud.hits)
	}
	if len(tr.seen) != 1 || tr.seen[0] != "SELECT datediff(d2, d1) FROM t" {
		t.Fatalf("transpiler saw %v, want the authored Spark once", tr.seen)
	}
	if got := cloud.lastQuery.SQL; got != "TRINO:SELECT datediff(d2, d1) FROM t" {
		t.Errorf("SQL = %q, want the transpiled form dispatched", got)
	}
	if got := cloud.lastQuery.PipelineDir; got != "demo" {
		t.Errorf("PipelineDir = %q, want explicit dir passed through", got)
	}
}

func TestQueryWarehouseOverride(t *testing.T) {
	ctx := context.Background()

	t.Run("cloud override on a local workspace", func(t *testing.T) {
		s, ws, local, cloud, tr := queryService(t)
		markDeployed(t, ws)
		if _, err := s.Query(ctx, "SELECT 1", QueryOptions{Warehouse: workspace.WarehouseCloud}); err != nil {
			t.Fatalf("Query: %v", err)
		}
		if cloud.hits != 1 || local.hits != 0 {
			t.Fatalf("dispatch: local=%d cloud=%d, want cloud=1 local=0", local.hits, cloud.hits)
		}
		if len(tr.seen) != 1 {
			t.Errorf("transpiler calls = %d, want 1 (cloud path transpiles)", len(tr.seen))
		}
	})

	t.Run("local override on a cloud workspace", func(t *testing.T) {
		s, ws, local, cloud, tr := queryService(t)
		if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Query(ctx, "SELECT 1", QueryOptions{Warehouse: workspace.WarehouseLocal}); err != nil {
			t.Fatalf("Query: %v", err)
		}
		if local.hits != 1 || cloud.hits != 0 {
			t.Fatalf("dispatch: local=%d cloud=%d, want local=1 cloud=0", local.hits, cloud.hits)
		}
		// The local portability gate fires on the override path too.
		if len(tr.seen) != 1 {
			t.Errorf("local portability gate: transpiler calls = %d, want 1", len(tr.seen))
		}
	})
}

// TestQueryServedStamps pins the ADR-024 engine-metadata contract at the
// seam: the provider stamps engine + warehouse on Served (it executed the
// SQL); only Service.Query knows a SparkSQL→Trino transpile ran, so it —
// and nothing else — flips Transpiled on, and only on the cloud path with
// a real transpiler wired.
func TestQueryServedStamps(t *testing.T) {
	ctx := context.Background()

	t.Run("cloud with transpiler sets Transpiled", func(t *testing.T) {
		s, ws, _, cloud, _ := queryService(t)
		if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
			t.Fatal(err)
		}
		markDeployed(t, ws)
		cloud.queryRes = &observability.QueryResult{
			Served: &observability.Served{Engine: "athena", Warehouse: "cloud"},
		}

		res, err := s.Query(ctx, "SELECT 1", QueryOptions{})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if res.Served == nil {
			t.Fatal("Served missing on cloud result")
		}
		if !res.Served.Transpiled {
			t.Error("Transpiled = false, want true after a cloud transpile")
		}
		if res.Served.Engine != "athena" || res.Served.Warehouse != "cloud" {
			t.Errorf("Served = %+v, want the provider's {athena cloud} stamp kept", *res.Served)
		}
	})

	t.Run("local keeps the provider stamp untranspiled", func(t *testing.T) {
		s, _, local, _, _ := queryService(t)
		local.queryRes = &observability.QueryResult{
			Served: &observability.Served{Engine: "spark", Warehouse: "local"},
		}

		res, err := s.Query(ctx, "SELECT 1", QueryOptions{})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if res.Served == nil {
			t.Fatal("Served missing on local result")
		}
		if res.Served.Transpiled {
			t.Error("Transpiled = true on the local path — local never transpiles")
		}
		if res.Served.Engine != "spark" || res.Served.Warehouse != "local" {
			t.Errorf("Served = %+v, want {spark local}", *res.Served)
		}
	})

	t.Run("cloud without transpiler does not claim a transpile", func(t *testing.T) {
		// No WithTranspiler: TranspileServing is a pass-through, so claiming
		// transpiled=true would be a lie.
		ws := t.TempDir()
		cloud := &queryRecordingProvider{}
		cloud.queryRes = &observability.QueryResult{
			Served: &observability.Served{Engine: "athena", Warehouse: "cloud"},
		}
		resolver := observability.NewResolver(ws, cloud, &queryRecordingProvider{})
		s := New(ws).WithResolver(resolver)
		if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
			t.Fatal(err)
		}
		markDeployed(t, ws)

		res, err := s.Query(ctx, "SELECT 1", QueryOptions{})
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if res.Served == nil || res.Served.Transpiled {
			t.Errorf("Served = %+v, want {athena cloud} without a transpile claim", res.Served)
		}
	})
}

func TestQueryDialectErrorBlocksDispatch(t *testing.T) {
	ctx := context.Background()
	s, ws, local, cloud, tr := queryService(t)
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
		t.Fatal(err)
	}
	markDeployed(t, ws)
	tr.toServing = func(context.Context, string) (string, error) {
		return "", &observability.DialectError{Message: "cannot transpile FOO()", Line: 1, Col: 8}
	}

	_, err := s.Query(ctx, "SELECT FOO()", QueryOptions{})
	var de *DialectError
	if !errors.As(err, &de) {
		t.Fatalf("err = %T (%v), want *service.DialectError", err, err)
	}
	if cloud.hits != 0 || local.hits != 0 {
		t.Errorf("a dialect rejection must not dispatch: local=%d cloud=%d", local.hits, cloud.hits)
	}
}

// TestQueryLocalDialectErrorBlocksDispatch pins the local portability gate:
// a SparkSQL construct that can't transpile to Trino is rejected on the
// LOCAL warehouse too, before any local-Spark dispatch — so a query that
// runs in /query is guaranteed to run as a cloud dashboard widget.
func TestQueryLocalDialectErrorBlocksDispatch(t *testing.T) {
	ctx := context.Background()
	s, _, local, cloud, tr := queryService(t) // default (local) warehouse
	tr.toServing = func(context.Context, string) (string, error) {
		return "", &observability.DialectError{Message: "cannot transpile FOO()", Line: 1, Col: 8}
	}

	_, err := s.Query(ctx, "SELECT FOO()", QueryOptions{})
	var de *DialectError
	if !errors.As(err, &de) {
		t.Fatalf("err = %T (%v), want *service.DialectError on the local path", err, err)
	}
	if cloud.hits != 0 || local.hits != 0 {
		t.Errorf("a local dialect rejection must not dispatch: local=%d cloud=%d", local.hits, cloud.hits)
	}
}

func TestQueryValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("empty sql", func(t *testing.T) {
		s, _, _, _, _ := queryService(t)
		if _, err := s.Query(ctx, "   \n", QueryOptions{}); err == nil {
			t.Fatal("want error for empty sql")
		}
	})

	t.Run("no resolver wired", func(t *testing.T) {
		s := New(t.TempDir())
		if _, err := s.Query(ctx, "SELECT 1", QueryOptions{}); err == nil {
			t.Fatal("want error when no resolver is configured")
		}
	})
}
