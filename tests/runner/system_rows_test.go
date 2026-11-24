//go:build integration

package runner_test

import (
	"strings"
	"testing"
)

// TestSystemRows_ArrowMatchesClassic runs system_rows_check.py in the runner
// image: runner._system_df (Arrow, no Python worker) must write the same
// node_runs, runs, tables and column_stats rows as the classic
// spark.createDataFrame(<list>, schema), and reject None in the same
// non-nullable fields (docs/ideas/runner-memory.md).
func TestSystemRows_ArrowMatchesClassic(t *testing.T) {
	out, stderr := dockerRunCapture(t, dockerOpts{
		env:        map[string]string{"CLAVESA_WAREHOUSE": "/tmp/clavesa-warehouse"},
		mountSrc:   repoPath("tests/runner/system_rows_check.py"),
		mountDst:   "/tmp/system_rows_check.py",
		entrypoint: "python",
		cmd:        []string{"/tmp/system_rows_check.py"},
	})
	if !strings.Contains(out, "SYSTEM_ROWS_OK") {
		t.Fatalf("system rows differ between Arrow and classic createDataFrame:\nstdout: %s\nstderr tail: %s", out, tail(stderr, 2000))
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
