package cli

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// TestWorkspaceSummaryJSONGolden builds a workspace the way a user would
// (init with an S3 backend, a pipeline with a scheduled transform that
// declares a freshness_sla) and pins `workspace summary --json` against
// testdata/workspace_summary.golden. The clock-dependent values
// (generated_at and the day series dates) are normalized first.
// UPDATE_GOLDEN=1 rewrites the golden file.
//
// Not t.Parallel(): captureStdout swaps the process-global os.Stdout.
func TestWorkspaceSummaryJSONGolden(t *testing.T) {
	ws := t.TempDir()
	steps := [][]string{
		{"workspace", "init", "summary-ws", "--workspace", ws,
			"--backend", "s3", "--state-bucket", "summary-ws-tfstate", "--state-region", "eu-north-1"},
		{"pipeline", "create", "orders", "--workspace", ws},
		{"node", "add", "--type", "transform", "--workspace", ws, "orders"},
		{"node", "edit", "--set", "freshness_sla=4h", "--workspace", ws, "orders", "transform1"},
	}
	for _, args := range steps {
		if err := Run(args); err != nil {
			t.Fatalf("Run(%v): %v", args, err)
		}
	}
	if err := os.WriteFile(filepath.Join(ws, "orders", "terraform.tfvars"),
		[]byte(`trigger_schedule = "rate(1 day)"`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := Run([]string{"workspace", "summary", "--json", "--workspace", ws}); err != nil {
			t.Fatalf("workspace summary --json: %v", err)
		}
	})
	got := regexp.MustCompile(`"generated_at": "[^"]+"`).ReplaceAllString(out, `"generated_at": "<now>"`)
	got = regexp.MustCompile(`"date": "\d{4}-\d{2}-\d{2}"`).ReplaceAllString(got, `"date": "<day>"`)

	path := filepath.Join("testdata", "workspace_summary.golden")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		_ = os.MkdirAll("testdata", 0o755)
		_ = os.WriteFile(path, []byte(got), 0o644)
	}
	want, _ := os.ReadFile(path)
	if got != string(want) {
		t.Errorf("workspace summary --json differs from %s (UPDATE_GOLDEN=1 rewrites):\n%s", path, got)
	}
}
