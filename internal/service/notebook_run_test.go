package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/notebooks"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// stubNotebookRunner satisfies NotebookRunner without Docker. Records the
// warehouse RunCell dispatched to so tests can assert it matches the
// Served stamp on the response.
type stubNotebookRunner struct {
	gotWarehouse string
}

func (s *stubNotebookRunner) RunCell(_ context.Context, _, warehouse, _, _, _ string) (*observability.CellResult, error) {
	s.gotWarehouse = warehouse
	return &observability.CellResult{Status: "ok", DurationMS: 5, Stdout: "ok\n"}, nil
}
func (s *stubNotebookRunner) CancelCell(context.Context, string, string, string) error { return nil }
func (s *stubNotebookRunner) StopSession(context.Context, string) error                { return nil }
func (s *stubNotebookRunner) Sessions() []observability.NotebookSessionStatus          { return nil }

// runCellService builds a Service over a fresh workspace with one
// notebook ("scratch") containing one %%sql code cell, returning the
// cell's ID and the recording runner stub.
func runCellService(t *testing.T) (*Service, string, string, *stubNotebookRunner) {
	t.Helper()
	ws := t.TempDir()
	runner := &stubNotebookRunner{}
	s := New(ws).WithNotebookRunner(runner)
	nb, err := s.CreateNotebook("scratch")
	if err != nil {
		t.Fatalf("CreateNotebook: %v", err)
	}
	nb.Cells = append(nb.Cells, notebooks.Cell{
		CellType: notebooks.CellTypeCode,
		Source:   []string{"%%sql\n", "SELECT 1"},
	})
	nb, err = s.SaveNotebook(nb)
	if err != nil {
		t.Fatalf("SaveNotebook: %v", err)
	}
	return s, ws, nb.Cells[len(nb.Cells)-1].ID, runner
}

// TestRunCellServedStamp pins the ADR-024 engine metadata on the run-cell
// response envelope: always Spark (the REPL), warehouse per the URI the
// dispatch actually targeted, never persisted into the .ipynb on disk.
func TestRunCellServedStamp(t *testing.T) {
	ctx := context.Background()

	t.Run("local warehouse", func(t *testing.T) {
		s, ws, cellID, runner := runCellService(t)
		res, err := s.RunCell(ctx, "scratch", cellID)
		if err != nil {
			t.Fatalf("RunCell: %v", err)
		}
		if res.Served == nil || res.Served.Engine != "spark" || res.Served.Warehouse != "local" || res.Served.Transpiled {
			t.Errorf("Served = %+v, want {spark local false}", res.Served)
		}
		if strings.HasPrefix(runner.gotWarehouse, "s3://") {
			t.Errorf("dispatched warehouse %q, want the local dir", runner.gotWarehouse)
		}

		// Response-envelope only: the persisted .ipynb must not pick up
		// the served stamp (a saved notebook records outputs, not where
		// they ran — a stale stamp would lie after a warehouse flip).
		raw, err := os.ReadFile(notebooks.New(ws).Path("scratch"))
		if err != nil {
			t.Fatalf("read persisted notebook: %v", err)
		}
		if strings.Contains(string(raw), `"served"`) {
			t.Error("persisted .ipynb contains a served stamp; it must stay response-only")
		}
	})

	t.Run("cloud warehouse", func(t *testing.T) {
		s, ws, cellID, runner := runCellService(t)
		if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
			t.Fatal(err)
		}
		markDeployed(t, ws)

		res, err := s.RunCell(ctx, "scratch", cellID)
		if err != nil {
			t.Fatalf("RunCell: %v", err)
		}
		if res.Served == nil || res.Served.Engine != "spark" || res.Served.Warehouse != "cloud" || res.Served.Transpiled {
			t.Errorf("Served = %+v, want {spark cloud false}", res.Served)
		}
		if !strings.HasPrefix(runner.gotWarehouse, "s3://") {
			t.Errorf("dispatched warehouse %q, want an s3:// URI", runner.gotWarehouse)
		}
	})
}
