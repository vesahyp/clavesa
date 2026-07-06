package service

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/observability"
)

// Runner-failure log durability. When a local runner container fails, the
// inline error carries only a bounded stderr tail (boundedStderrTail) — the
// full Spark output must land on disk and the error must say where, or a
// real debugging session ends up with a truncated stack trace and nothing
// else. These helpers guarantee both halves: createRunnerLogFile never
// silently drops the disk copy (it falls back from the preferred per-run
// location to the workspace .clavesa dir to the system temp dir), and
// runnerLogRef is the single format for the trailing "full runner log:
// <path>" line every runner-failure error appends.

// createRunnerLogFile opens the full-runner-log file at preferred, falling
// back to a temp file under <workspaceRoot>/.clavesa and then the system
// temp dir when the preferred location can't be created. Returns the open
// file plus the path actually used; err is non-nil only when every location
// failed (the file is nil then). pattern is the os.CreateTemp name pattern
// used by the fallbacks.
func createRunnerLogFile(preferred, workspaceRoot, pattern string) (*os.File, string, error) {
	var firstErr error
	if preferred == "" {
		firstErr = errors.New("no per-run log location")
	} else if err := os.MkdirAll(filepath.Dir(preferred), 0o755); err != nil {
		firstErr = err
	} else if f, err := os.Create(preferred); err != nil {
		firstErr = err
	} else {
		return f, preferred, nil
	}
	if workspaceRoot != "" {
		dir := filepath.Join(workspaceRoot, ".clavesa")
		if err := os.MkdirAll(dir, 0o755); err == nil {
			if f, err := os.CreateTemp(dir, pattern); err == nil {
				return f, f.Name(), nil
			}
		}
	}
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, "", fmt.Errorf("%v (temp fallback: %v)", firstErr, err)
	}
	return f, f.Name(), nil
}

// runnerLogRef formats the trailing error line that points the user at the
// durable full runner log — or, when no file could be created anywhere,
// says so with the reason instead of staying silent.
func runnerLogRef(path string, createErr error) string {
	if path != "" {
		return "full runner log: " + path
	}
	if createErr == nil {
		createErr = errors.New("log file not created")
	}
	return fmt.Sprintf("full runner log unavailable: %v", createErr)
}

// writeRunnerFailureLog persists the buffered stdout+stderr of a failed
// runner invocation via createRunnerLogFile (same fallback ladder) and
// returns the path written. Used by paths that buffer output in memory
// instead of teeing to disk during the run (runTransform's single-node
// docker path); the stdout section is prefixed so the two streams stay
// distinguishable in one file.
func writeRunnerFailureLog(preferred, workspaceRoot, pattern string, stdout, stderr []byte) (string, error) {
	f, path, err := createRunnerLogFile(preferred, workspaceRoot, pattern)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if len(stdout) > 0 {
		fmt.Fprintln(f, "--- stdout ---")
		_, _ = f.Write(stdout)
		if !bytes.HasSuffix(stdout, []byte("\n")) {
			fmt.Fprintln(f)
		}
		fmt.Fprintln(f, "--- stderr ---")
	}
	_, _ = f.Write(stderr)
	return path, nil
}

// transformFailureLogRef makes the full output of a failed single-node
// runner invocation (the backfill-replay path) durable and returns the
// "full runner log: …" line for the error message (GH #82). Writes the
// buffered stdout+stderr to the standard run-log location
// (observability.RunLogPath), with the same fallback ladder as the bundle
// path.
func transformFailureLogRef(pipelineDir, runID, nodeID, workspaceRoot string, stdout, stderr []byte) string {
	preferred := ""
	if runID != "" {
		preferred = observability.RunLogPath(pipelineDir, runID, nodeID)
	}
	pattern := "clavesa-runner-" + sanitizeLogToken(nodeID) + "-*.log"
	path, err := writeRunnerFailureLog(preferred, workspaceRoot, pattern, stdout, stderr)
	return runnerLogRef(path, err)
}

// sanitizeLogToken strips path separators from an identifier embedded in an
// os.CreateTemp pattern (patterns must not contain separators).
func sanitizeLogToken(s string) string {
	s = strings.ReplaceAll(s, "/", "_")
	return strings.ReplaceAll(s, string(os.PathSeparator), "_")
}
