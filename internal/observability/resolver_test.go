package observability_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// stubProvider is a no-op Provider used to verify dispatch routing.
type stubProvider struct{ tag string }

func (s *stubProvider) NodeRuns(context.Context, observability.NodeRunsQuery) (*observability.NodeRunsResult, error) {
	return nil, nil
}
func (s *stubProvider) Runs(context.Context, observability.RunsQuery) (*observability.RunsResult, error) {
	return nil, nil
}
func (s *stubProvider) Tables(context.Context, observability.TablesQuery) (*observability.TablesResult, error) {
	return nil, nil
}
func (s *stubProvider) Snapshots(context.Context, observability.SnapshotsQuery) (*observability.SnapshotsResult, error) {
	return nil, nil
}
func (s *stubProvider) ColumnStats(context.Context, observability.ColumnStatsQuery) (*observability.ColumnStatsResult, error) {
	return nil, nil
}
func (s *stubProvider) SampleTable(context.Context, observability.SampleTableQuery) (*observability.SampleTableResult, error) {
	return nil, nil
}
func (s *stubProvider) Query(context.Context, observability.QueryQuery) (*observability.QueryResult, error) {
	return nil, nil
}
func (s *stubProvider) Exec(context.Context, observability.ExecQuery) error {
	return nil
}
func (s *stubProvider) ExecutionStates(context.Context, observability.ExecutionStatesQuery) (*observability.ExecutionStatesResult, error) {
	return nil, nil
}
func (s *stubProvider) ExecutionLogs(context.Context, observability.ExecutionLogsQuery) (*observability.ExecutionLogsResult, error) {
	return nil, nil
}

// writePipeline writes a minimal .tf file with one transform whose compute
// attribute matches the supplied value. parser.Parse only requires the
// clavesa/<type>/<cloud> source convention to recognize the module.
func writePipeline(t *testing.T, compute string) string {
	t.Helper()
	dir := t.TempDir()
	tf := `variable "pipeline_name" { type = string default = "demo" }

module "src" {
  source        = "clavesa/source/aws"
  pipeline_name = var.pipeline_name
  bucket        = "in"
  prefix        = "raw/"
  format        = "csv"
}

module "xform" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  inputs        = { primary = module.src.outputs["default"] }
  compute       = "` + compute + `"
  language      = "sparksql"
  sql           = "SELECT 1"
}
`
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(tf), 0o644); err != nil {
		t.Fatalf("write tf: %v", err)
	}
	return dir
}

func TestResolverDispatchByMode(t *testing.T) {
	cloud := &stubProvider{tag: "cloud"}
	local := &stubProvider{tag: "local"}

	tests := []struct {
		name    string
		mode    workspace.Mode // "" = no environment.json → defaults to local
		compute string
		want    string
	}{
		{"default mode (absent file) → local", "", "lambda", "local"},
		{"mode local → local", workspace.ModeLocal, "lambda", "local"},
		{"mode cloud → cloud", workspace.ModeCloud, "lambda", "cloud"},
		{"mode cloud, compute attr ignored → cloud", workspace.ModeCloud, "local", "cloud"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := writePipeline(t, tt.compute)
			if tt.mode != "" {
				if err := workspace.WriteEnvironmentMode(dir, tt.mode); err != nil {
					t.Fatalf("write mode: %v", err)
				}
			}
			r := observability.NewResolver(dir, cloud, local)

			p, err := r.For(".")
			if err != nil {
				t.Fatalf("resolver.For: %v", err)
			}
			got := p.(*stubProvider).tag
			if got != tt.want {
				t.Errorf("dispatched %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolverEmptyDir(t *testing.T) {
	r := observability.NewResolver("/tmp", nil, nil)
	if _, err := r.For(""); err == nil {
		t.Error("expected error for empty dir")
	}
}

func TestResolverMissingCloud(t *testing.T) {
	dir := writePipeline(t, "lambda")
	if err := workspace.WriteEnvironmentMode(dir, workspace.ModeCloud); err != nil {
		t.Fatalf("write mode: %v", err)
	}
	r := observability.NewResolver(dir, nil, &stubProvider{})
	if _, err := r.For("."); err == nil {
		t.Error("expected error when cloud provider unavailable for cloud pipeline")
	}
}

func TestResolverMissingLocal(t *testing.T) {
	dir := writePipeline(t, "local")
	r := observability.NewResolver(dir, &stubProvider{}, nil)
	if _, err := r.For("."); err == nil {
		t.Error("expected error when local provider unconfigured for local pipeline")
	}
}

func TestResolverIsLocal(t *testing.T) {
	tests := []struct {
		name string
		mode workspace.Mode
		want bool
	}{
		{"default mode → local", "", true},
		{"mode local → local", workspace.ModeLocal, true},
		{"mode cloud → cloud", workspace.ModeCloud, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.mode != "" {
				if err := workspace.WriteEnvironmentMode(dir, tt.mode); err != nil {
					t.Fatalf("write mode: %v", err)
				}
			}
			r := observability.NewResolver(dir, nil, nil)
			if got := r.IsLocal(); got != tt.want {
				t.Errorf("IsLocal = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolverPipelineName(t *testing.T) {
	dir := writePipeline(t, "lambda")
	r := observability.NewResolver(filepath.Dir(dir), nil, nil)
	if got, want := r.PipelineName(filepath.Base(dir)), filepath.Base(dir); got != want {
		t.Errorf("PipelineName = %q, want %q", got, want)
	}
}
