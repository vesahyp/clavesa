//go:build integration

package integration

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

// TestPipelineRunsAndLogs drives `pipeline runs`, `pipeline logs`, and
// `pipeline status` through the built binary (GH #94, ADR-015 CLI parity
// for run history and log inspection — the CLI half of the dashboard's
// Runs tab and run-detail log panel).
//
// One workspace carries the whole test: a source and transform are set up
// once, the pipeline is run twice while healthy (SUCCEEDED rows) and once
// more after the transform is broken (a FAILED row with a concrete
// runner error), and every assertion below is packed onto those three
// runs rather than spinning up new ones — Spark runs cost real wall time
// (~40s) so the run count is kept to the minimum that still covers:
//
//   - the pre-run empty states for both `pipeline runs` and `pipeline logs`
//   - `pipeline run` text mode printing a `Run: <id>` line
//   - `pipeline runs --json` row shape: newest-first ordering, FAILED row
//     with failed_step + error_msg, SUCCEEDED row with trigger/timing
//   - `pipeline runs` text mode flattening a multi-line error onto one
//     table row (run id and error substring on the same line)
//   - `--limit` / `--status` filtering, including the invalid-status error
//     and the "no rows for this status" empty state
//   - `pipeline logs --json` shape (source/log_group/events), `--run`
//     targeting an older run, `--tail` truncation, and the run-not-found
//     empty state
//   - `pipeline status --json` reporting the failed run's per-node error
func TestPipelineRunsAndLogs(t *testing.T) {
	ws := t.TempDir()

	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	run(t, "pipeline", "create", "orders", "--workspace", ws)

	srv, _ := addRegistrySourceFromTestdata(t, ws, "orders", "orders.csv", "csv")
	defer srv.Close()

	transformID := addNode(t, ws, "orders", "transform")
	run(t, "node", "edit", "orders", transformID,
		"--set", "sql=SELECT status, SUM(amount) AS total FROM orders GROUP BY status",
		"--workspace", ws,
	)
	run(t, "source", "attach", "orders", "orders", "--to", transformID, "--as", "orders", "--workspace", ws)

	// --- Step 2: empty states before any run. ---

	runsOut := run(t, "pipeline", "runs", "orders", "--workspace", ws)
	if !strings.Contains(runsOut, "No runs yet") {
		t.Errorf("pipeline runs (pre-run) = %q, want it to contain %q", runsOut, "No runs yet")
	}
	logsOut := run(t, "pipeline", "logs", "orders", "--workspace", ws)
	if !strings.Contains(logsOut, "No captured log") {
		t.Errorf("pipeline logs (pre-run) = %q, want it to contain %q", logsOut, "No captured log")
	}

	// --- Step 3: first successful run, text mode; extract the run id. ---

	firstRunOut := run(t, "pipeline", "run", "orders", "--workspace", ws)
	firstRunID := extractRunID(t, firstRunOut)

	// A second successful run so `pipeline runs` has >1 SUCCEEDED row to
	// distinguish "newest first" ordering from "only one row exists".
	run(t, "pipeline", "run", "orders", "--workspace", ws)

	// --- Step 4: break the transform, run again expecting failure. ---

	run(t, "node", "edit", "orders", transformID,
		"--set", "sql=SELECT nonexistent_column FROM orders",
		"--workspace", ws,
	)
	failCmd := exec.Command(binPath, "pipeline", "run", "orders", "--workspace", ws)
	failOut, failErr := failCmd.CombinedOutput()
	if failErr == nil {
		t.Fatalf("expected non-zero exit running the broken transform; output: %s", failOut)
	}

	// --- Step 5: `pipeline runs --json` row shape. ---

	type runRow struct {
		RunID      string `json:"run_id"`
		Pipeline   string `json:"pipeline"`
		Status     string `json:"status"`
		Trigger    string `json:"trigger"`
		StartedAt  string `json:"started_at"`
		EndedAt    string `json:"ended_at"`
		DurationMs *int64 `json:"duration_ms"`
		FailedStep string `json:"failed_step"`
		ErrorClass string `json:"error_class"`
		ErrorMsg   string `json:"error_msg"`
	}
	type runsResult struct {
		Rows      []runRow `json:"rows"`
		Truncated bool     `json:"truncated"`
	}

	jsonOut := run(t, "pipeline", "runs", "orders", "--workspace", ws, "--json")
	var res runsResult
	if err := json.Unmarshal([]byte(jsonOut), &res); err != nil {
		t.Fatalf("parse pipeline runs --json: %v\noutput: %s", err, jsonOut)
	}
	if len(res.Rows) < 2 {
		t.Fatalf("want >=2 runs (2 succeeded + 1 failed), got %d\noutput: %s", len(res.Rows), jsonOut)
	}

	newest := res.Rows[0]
	if newest.Status != "FAILED" {
		t.Errorf("newest run status = %q, want FAILED (newest-first ordering)\nrow: %+v", newest.Status, newest)
	}
	if newest.FailedStep != transformID {
		t.Errorf("newest run failed_step = %q, want %q", newest.FailedStep, transformID)
	}
	if !strings.Contains(newest.ErrorMsg, "nonexistent_column") {
		t.Errorf("newest run error_msg = %q, want it to contain %q", newest.ErrorMsg, "nonexistent_column")
	}
	failedRunID := newest.RunID

	var older *runRow
	for i := 1; i < len(res.Rows); i++ {
		if res.Rows[i].Status == "SUCCEEDED" {
			older = &res.Rows[i]
			break
		}
	}
	if older == nil {
		t.Fatalf("expected a SUCCEEDED row among the older runs\nrows: %+v", res.Rows)
	}
	if older.Trigger != "manual" {
		t.Errorf("older succeeded run trigger = %q, want %q", older.Trigger, "manual")
	}
	if older.RunID == "" {
		t.Error("older succeeded run has empty run_id")
	}
	if older.StartedAt == "" {
		t.Error("older succeeded run has empty started_at")
	}
	if older.DurationMs == nil || *older.DurationMs <= 0 {
		t.Errorf("older succeeded run duration_ms = %v, want a positive value", older.DurationMs)
	}

	// firstRunID should still show up somewhere in the row set (either as
	// the "older" row above or the third row, depending on clock
	// resolution) — sanity check the id extracted from text-mode step 3
	// round-trips through the JSON listing.
	var sawFirstRunID bool
	for _, r := range res.Rows {
		if r.RunID == firstRunID {
			sawFirstRunID = true
			break
		}
	}
	if !sawFirstRunID {
		t.Errorf("run id %q extracted from text-mode `pipeline run` output not found in `pipeline runs --json`; rows: %+v", firstRunID, res.Rows)
	}

	// --- Step 6: text mode flattens the multi-line error onto one row. ---

	runsTextOut := run(t, "pipeline", "runs", "orders", "--workspace", ws)
	var failedLine string
	for _, line := range strings.Split(runsTextOut, "\n") {
		if strings.Contains(line, failedRunID) {
			failedLine = line
			break
		}
	}
	if failedLine == "" {
		t.Fatalf("no line in `pipeline runs` text output contains the failed run id %q\noutput: %s", failedRunID, runsTextOut)
	}
	if !strings.Contains(failedLine, "nonexistent_column") {
		t.Errorf("failed run's text-table line = %q, want it to also contain %q (flattened error on the same line)", failedLine, "nonexistent_column")
	}
	if strings.Contains(failedLine, "\n") {
		t.Errorf("failed run's text-table line unexpectedly contains an embedded newline: %q", failedLine)
	}

	// --- Step 7: --limit / --status filtering. ---

	limitOut := run(t, "pipeline", "runs", "orders", "--workspace", ws, "--limit", "1", "--json")
	var limitRes runsResult
	if err := json.Unmarshal([]byte(limitOut), &limitRes); err != nil {
		t.Fatalf("parse pipeline runs --limit 1 --json: %v\noutput: %s", err, limitOut)
	}
	if len(limitRes.Rows) != 1 {
		t.Errorf("--limit 1 returned %d rows, want 1", len(limitRes.Rows))
	}
	if !limitRes.Truncated {
		t.Error("--limit 1 with >1 run available should report truncated=true")
	}

	statusFailedOut := run(t, "pipeline", "runs", "orders", "--workspace", ws, "--status", "failed", "--json")
	var statusFailedRes runsResult
	if err := json.Unmarshal([]byte(statusFailedOut), &statusFailedRes); err != nil {
		t.Fatalf("parse pipeline runs --status failed --json: %v\noutput: %s", err, statusFailedOut)
	}
	if len(statusFailedRes.Rows) == 0 {
		t.Error("--status failed returned no rows")
	}
	for _, r := range statusFailedRes.Rows {
		if r.Status != "FAILED" {
			t.Errorf("--status failed returned a row with status %q", r.Status)
		}
	}

	statusCmd := exec.Command(binPath, "pipeline", "runs", "orders", "--workspace", ws, "--status", "bogus")
	statusOutBytes, statusErr := statusCmd.CombinedOutput()
	if statusErr == nil {
		t.Fatalf("expected non-zero exit for --status bogus; output: %s", statusOutBytes)
	}
	if !strings.Contains(string(statusOutBytes), "RUNNING") || !strings.Contains(string(statusOutBytes), "SUCCEEDED") || !strings.Contains(string(statusOutBytes), "FAILED") {
		t.Errorf("--status bogus error should mention the valid values, got: %s", statusOutBytes)
	}

	statusRunningOut := run(t, "pipeline", "runs", "orders", "--workspace", ws, "--status", "running")
	if !strings.Contains(statusRunningOut, "No RUNNING runs") {
		t.Errorf("--status running (no running runs) = %q, want it to contain %q", statusRunningOut, "No RUNNING runs")
	}

	// --- Step 8: `pipeline logs`. ---

	type logEvent struct {
		Timestamp string `json:"timestamp"`
		Message   string `json:"message"`
	}
	type logsResult struct {
		Source    string     `json:"source"`
		LogGroup  string     `json:"log_group"`
		Events    []logEvent `json:"events"`
		Truncated bool       `json:"truncated"`
	}

	latestLogsOut := run(t, "pipeline", "logs", "orders", "--workspace", ws, "--json")
	var latestLogs logsResult
	if err := json.Unmarshal([]byte(latestLogsOut), &latestLogs); err != nil {
		t.Fatalf("parse pipeline logs --json (latest): %v\noutput: %s", err, latestLogsOut)
	}
	if latestLogs.Source != "local" {
		t.Errorf("latest run logs source = %q, want %q", latestLogs.Source, "local")
	}
	if !strings.HasSuffix(latestLogs.LogGroup, "_bundle.log") {
		t.Errorf("latest run logs log_group = %q, want it to end with %q", latestLogs.LogGroup, "_bundle.log")
	}
	if len(latestLogs.Events) == 0 {
		t.Error("latest (failed) run logs returned no events")
	}

	olderLogsOut := run(t, "pipeline", "logs", "orders", "--workspace", ws, "--run", older.RunID, "--json")
	var olderLogs logsResult
	if err := json.Unmarshal([]byte(olderLogsOut), &olderLogs); err != nil {
		t.Fatalf("parse pipeline logs --run %s --json: %v\noutput: %s", older.RunID, err, olderLogsOut)
	}
	if len(olderLogs.Events) == 0 {
		t.Errorf("logs for older succeeded run %q returned no events", older.RunID)
	}

	untailedOut := run(t, "pipeline", "logs", "orders", "--workspace", ws, "--json")
	var untailed logsResult
	if err := json.Unmarshal([]byte(untailedOut), &untailed); err != nil {
		t.Fatalf("parse pipeline logs --json (untailed): %v\noutput: %s", err, untailedOut)
	}
	tailedOut := run(t, "pipeline", "logs", "orders", "--workspace", ws, "--tail", "3", "--json")
	var tailed logsResult
	if err := json.Unmarshal([]byte(tailedOut), &tailed); err != nil {
		t.Fatalf("parse pipeline logs --tail 3 --json: %v\noutput: %s", err, tailedOut)
	}
	if len(tailed.Events) > 3 {
		t.Errorf("--tail 3 returned %d events, want <=3", len(tailed.Events))
	}
	if !tailed.Truncated {
		t.Error("--tail 3 against a longer log should report truncated=true")
	}
	if len(untailed.Events) > len(tailed.Events) && len(tailed.Events) > 0 {
		wantTail := untailed.Events[len(untailed.Events)-len(tailed.Events):]
		for i, ev := range tailed.Events {
			if ev.Message != wantTail[i].Message {
				t.Errorf("--tail 3 event %d = %q, want the last-N slice of the untailed log %q (tail should return the END of the log, not the start)", i, ev.Message, wantTail[i].Message)
				break
			}
		}
	}

	missingRunOut := run(t, "pipeline", "logs", "orders", "--workspace", ws, "--run", "does-not-exist")
	if !strings.Contains(missingRunOut, "No captured log") {
		t.Errorf("pipeline logs --run does-not-exist = %q, want it to contain %q", missingRunOut, "No captured log")
	}

	// --- Step 9: `pipeline status --json` after the failed run. ---

	type nodeState struct {
		Status   string `json:"status"`
		ErrorMsg string `json:"error_msg"`
	}
	type statusResult struct {
		RunID  string               `json:"run_id"`
		Status string               `json:"status"`
		States map[string]nodeState `json:"states"`
	}
	statusJSONOut := run(t, "pipeline", "status", "orders", "--workspace", ws, "--json")
	var statusRes statusResult
	if err := json.Unmarshal([]byte(statusJSONOut), &statusRes); err != nil {
		t.Fatalf("parse pipeline status --json: %v\noutput: %s", err, statusJSONOut)
	}
	if statusRes.Status != "FAILED" {
		t.Errorf("pipeline status --json overall status = %q, want FAILED", statusRes.Status)
	}
	if statusRes.RunID == "" {
		t.Error("pipeline status --json has empty run_id")
	}
	st, ok := statusRes.States[transformID]
	if !ok {
		t.Fatalf("pipeline status --json states missing node %q; states: %+v", transformID, statusRes.States)
	}
	if st.Status != "FAILED" {
		t.Errorf("node %q status = %q, want FAILED", transformID, st.Status)
	}
	if st.ErrorMsg == "" {
		t.Errorf("node %q error_msg is empty, want the runner's error text", transformID)
	}
}

// extractRunID pulls the run id out of a text-mode `pipeline run`'s
// "Run: <id>" line.
func extractRunID(t *testing.T, out string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "Run: ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "Run: "))
		}
	}
	t.Fatalf("no \"Run: <id>\" line found in pipeline run output:\n%s", out)
	return ""
}
