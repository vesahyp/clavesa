package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

// TestAthenaOutput_Backfill: the backfill queries (diff, dedup-check)
// write their results where $CLAVESA_ATHENA_OUTPUT says, else to the
// pipeline bucket's athena-results/.
func TestAthenaOutput_Backfill(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "clavesa.json"), []byte(`{"name":"ws","cloud":"aws","version":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s := New(root)
	ctx := context.Background()
	t.Setenv(observability.AthenaOutputEnv, "")
	if got, err := s.athenaOutput(ctx); err != nil || got != "s3://ws-clavesa/athena-results/" {
		t.Errorf("default: %q, %v", got, err)
	}
	t.Setenv(observability.AthenaOutputEnv, "s3://ws-clavesa/athena-results/viewer/")
	if got, err := s.athenaOutput(ctx); err != nil || got != "s3://ws-clavesa/athena-results/viewer/" {
		t.Errorf("setting: %q, %v", got, err)
	}
	t.Setenv(observability.AthenaOutputEnv, "nope")
	if _, err := s.athenaOutput(ctx); err == nil {
		t.Error("a bad setting must fail")
	}
}
