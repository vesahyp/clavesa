//go:build integration

// Warm-session half of the runner integration suite.
//
// The data-semantics tests (promote, schema evolution, incremental CDF,
// OPTIMIZE, merge bounds) don't care how Spark started — paying a full
// container + JVM cold boot (~25s) per test only re-proved the launcher,
// which the cold tests in runner_test.go still cover. These tests instead
// share ONE long-lived container running tests/runner/warm_driver.py: a
// stdin/stdout JSON-line exec loop over a single SparkSession built by the
// production runner._spark(), mirroring how a warm Lambda container reuses
// its process and calls handler() repeatedly on one recycled session (GH #43).
//
// Isolation contract: each test uses its own database (wt_*) for every
// table it touches, its own watermark dir and logic path (set per snippet —
// the runner reads those env vars per handler() call), and must not rely on
// temp views, Spark conf mutations, or any other session state from a prior
// snippet. A test that needs a genuinely fresh session stays cold in
// runner_test.go.
package runner_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// warmSentinel prefixes every driver response line so the Go side can pick
// protocol frames out of Spark's stdout noise.
const warmSentinel = "CLAVESA_WARM_JSON:"

// warm is the suite-wide shared container, started by TestMain right after
// the image build (so the Spark boot overlaps the cold tests) and stopped
// at suite exit.
var warm *warmRunner

type warmEnvelope struct {
	ID     string          `json:"id"`
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error"`
}

// tailBuffer keeps the last max bytes written — the container's stderr tail
// for failure messages without holding a whole Spark log stream in memory.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		b.buf = append(b.buf[:0:0], b.buf[len(b.buf)-b.max:]...)
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

type warmRunner struct {
	name     string
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	respCh   chan warmEnvelope
	deadCh   chan struct{} // closed when the container's stdout hits EOF
	stderr   *tailBuffer
	startErr error

	mu    sync.Mutex // serializes exec calls (tests run serially anyway)
	seq   int
	ready bool
}

// startWarmRunner launches the shared warm container without blocking on
// Spark readiness — the first warm test waits for the driver's __ready__
// frame. Any start failure is recorded and reported by that first test.
func startWarmRunner() *warmRunner {
	w := &warmRunner{
		name:   fmt.Sprintf("clavesa-warm-itest-%d", os.Getpid()),
		respCh: make(chan warmEnvelope, 16),
		deadCh: make(chan struct{}),
		stderr: &tailBuffer{max: 64 * 1024},
	}
	driver := repoPath("tests/runner/warm_driver.py")
	args := []string{
		"run", "--rm", "-i",
		"--name", w.name,
		"-v", driver + ":/warm_driver.py:ro",
		"-e", "CLAVESA_WAREHOUSE=/warm/wh",
		"--entrypoint", "python",
		image, "/warm_driver.py",
	}
	w.cmd = exec.Command("docker", args...)
	stdin, err := w.cmd.StdinPipe()
	if err != nil {
		w.startErr = err
		return w
	}
	stdout, err := w.cmd.StdoutPipe()
	if err != nil {
		w.startErr = err
		return w
	}
	w.stdin = stdin
	w.cmd.Stderr = w.stderr
	if err := w.cmd.Start(); err != nil {
		w.startErr = err
		return w
	}
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		for sc.Scan() {
			line := sc.Text()
			i := strings.Index(line, warmSentinel)
			if i < 0 {
				continue
			}
			var env warmEnvelope
			if json.Unmarshal([]byte(line[i+len(warmSentinel):]), &env) != nil {
				continue
			}
			w.respCh <- env
		}
		// EOF: the container exited (clean stop or mid-suite death). Closing
		// deadCh makes every in-flight and future exec fail fast instead of
		// hanging.
		close(w.deadCh)
		_ = w.cmd.Wait()
	}()
	return w
}

// stop shuts the warm container down: closing stdin ends the driver's stdin
// loop, python exits, and --rm reaps the container. docker kill is the
// fallback if the driver doesn't exit promptly.
func (w *warmRunner) stop() {
	if w == nil || w.cmd == nil || w.cmd.Process == nil {
		return
	}
	if w.stdin != nil {
		_ = w.stdin.Close()
	}
	select {
	case <-w.deadCh:
	case <-time.After(30 * time.Second):
		_ = exec.Command("docker", "kill", w.name).Run()
	}
}

// await pulls frames off respCh until it sees id, failing fast when the
// container died or the timeout lapses — the remaining warm tests then fail
// with "warm runner died" instead of hanging.
func (w *warmRunner) await(t *testing.T, id string, timeout time.Duration) warmEnvelope {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case env := <-w.respCh:
			if env.ID == id {
				return env
			}
			t.Fatalf("warm runner: unexpected response id %q (want %q): ok=%v error=%s",
				env.ID, id, env.OK, env.Error)
		case <-w.deadCh:
			t.Fatalf("warm runner died mid-suite (container exited)\nstderr tail:\n%s", w.stderr.String())
		case <-deadline:
			t.Fatalf("warm runner: timed out after %s waiting for %q\nstderr tail:\n%s",
				timeout, id, w.stderr.String())
		}
	}
}

// warmExecRaw runs a Python snippet in the warm container's shared session
// and returns the snippet's RESULT dict as raw JSON.
func warmExecRaw(t *testing.T, source string) json.RawMessage {
	t.Helper()
	w := warm
	if w == nil {
		t.Fatal("warm runner not started (TestMain did not run?)")
	}
	if w.startErr != nil {
		t.Fatalf("warm runner failed to start: %v", w.startErr)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.ready {
		// First use: wait for the driver's Spark session build. The boot
		// normally overlapped the cold tests; 5 minutes is a cold-cache
		// worst case, not the expected wait.
		env := w.await(t, "__ready__", 5*time.Minute)
		if !env.OK {
			t.Fatalf("warm runner failed to become ready: %s", env.Error)
		}
		w.ready = true
	}
	w.seq++
	id := fmt.Sprintf("req-%d", w.seq)
	frame, err := json.Marshal(map[string]string{"id": id, "source": source})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.stdin.Write(append(frame, '\n')); err != nil {
		t.Fatalf("warm runner died (stdin write failed: %v)\nstderr tail:\n%s", err, w.stderr.String())
	}
	env := w.await(t, id, 5*time.Minute)
	if !env.OK {
		t.Fatalf("warm snippet failed:\n%s", env.Error)
	}
	return env.Result
}

// warmRun is the warm twin of the old runRawDriver: run the snippet, decode
// RESULT into a generic map.
func warmRun(t *testing.T, script string) map[string]any {
	t.Helper()
	raw := warmExecRaw(t, script)
	var r map[string]any
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("parse warm RESULT: %v\nraw: %s", err, raw)
	}
	if r == nil {
		t.Fatalf("warm snippet did not set RESULT")
	}
	return r
}

// warmPrelude is the shared snippet header: per-test env (the runner reads
// these per handler() call), imports, and the test's own database. db is
// the test's isolation unit — every table the snippet touches lives under
// it, and the watermark dir + logic path are keyed by it.
func warmPrelude(db string) string {
	return fmt.Sprintf(`
import json, os
os.environ["CLAVESA_LANGUAGE"] = "sql"
os.environ["CLAVESA_LOGIC_S3_PATH"] = "/warm/logic_%[1]s.txt"
os.environ["CLAVESA_WATERMARKS"] = "/warm/wm/%[1]s"
os.environ["CLAVESA_CATALOG"] = "itest_cat"
os.environ["CLAVESA_SCHEMA"] = %[1]q
os.environ["CLAVESA_PIPELINE"] = "itest"
os.environ["CLAVESA_NODE"] = "itest"
os.makedirs("/warm/wm/%[1]s", exist_ok=True)
LOGIC = os.environ["CLAVESA_LOGIC_S3_PATH"]
from runner import handler, _run_operation
spark.sql("CREATE DATABASE IF NOT EXISTS %[1]s")
`, db)
}

func pyBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

// asStrMap converts a RESULT JSON object value to map[string]string,
// tolerating missing keys (empty string).
func asStrMap(v any) map[string]string {
	out := map[string]string{}
	m, _ := v.(map[string]any)
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Operation-mode tests (the path UI's promote / discard buttons exercise)
// ---------------------------------------------------------------------------

// warmPromoteScript creates a Delta canonical + staging table with the given
// column schemas under db, invokes runner._run_operation with
// _operation=backfill_promote, and sets RESULT so the test asserts both the
// operation response and the resulting table state.
//
// Namespace shape mirrors production (canonicalTargetFor in
// internal/service/backfill.go): a single-part database namespace holding
// `<table>` and its `<table>__backfill__<run_id>` staging sibling, addressed
// two-segment as `<db>.<table>`.
func warmPromoteScript(db, canonicalCols, stagingCols, canonicalRows, stagingRows, mode, mergeKeys, forceDedup string, allowDupes bool) string {
	return warmPrelude(db) + fmt.Sprintf(`
spark.createDataFrame(%[2]s, %[3]q).write.format("delta").mode("overwrite").saveAsTable("%[1]s.canon")
spark.createDataFrame(%[4]s, %[5]q).write.format("delta").mode("overwrite").saveAsTable("%[1]s.canon__backfill__rid")

event = {
    "_operation": "backfill_promote",
    "staging": "%[1]s.canon__backfill__rid",
    "target": "%[1]s.canon",
    "mode": %[6]q,
    "merge_keys": %[7]s,
    "force_dedup": %[8]q,
    "allow_duplicates": %[9]s,
}
result = _run_operation(event)

target_schema = [(f.name, f.dataType.simpleString()) for f in spark.table("%[1]s.canon").schema]
target_rows = sorted(
    (r.asDict() for r in spark.table("%[1]s.canon").collect()),
    key=lambda r: tuple((k, v) for k, v in sorted(r.items()) if v is not None),
)
staging_exists = spark.catalog.tableExists("%[1]s.canon__backfill__rid")
RESULT = {
    "result": result,
    "target_schema": target_schema,
    "target_rows": target_rows,
    "staging_exists": staging_exists,
}
`, db, canonicalRows, canonicalCols, stagingRows, stagingCols,
		mode, mergeKeys, forceDedup, pyBool(allowDupes))
}

type promoteResult struct {
	Result struct {
		Status         string   `json:"status"`
		Operation      string   `json:"operation"`
		Target         string   `json:"target"`
		StagingDropped string   `json:"staging_dropped"`
		ColumnsAdded   []string `json:"columns_added"`
	} `json:"result"`
	TargetSchema  [][]string       `json:"target_schema"`
	TargetRows    []map[string]any `json:"target_rows"`
	StagingExists bool             `json:"staging_exists"`
}

func runWarmPromote(t *testing.T, script string) promoteResult {
	t.Helper()
	raw := warmExecRaw(t, script)
	var r promoteResult
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("parse warm promote RESULT: %v\nraw: %s", err, raw)
	}
	return r
}

// Baseline: schemas match by name → MERGE updates existing keys, inserts
// new ones, columns_added is empty.
func TestRunner_PromoteMerge_SchemaMatch(t *testing.T) {
	r := runWarmPromote(t, warmPromoteScript(
		"wt_pm_match",
		"id long, amount long",
		"id long, amount long",
		`[{"id":1,"amount":10},{"id":2,"amount":20}]`,
		`[{"id":2,"amount":99},{"id":3,"amount":30}]`,
		"merge", `["id"]`, "", false,
	))
	if r.Result.Status != "ok" {
		t.Fatalf("status: want ok, got %q", r.Result.Status)
	}
	if len(r.Result.ColumnsAdded) != 0 {
		t.Errorf("columns_added: want empty, got %v", r.Result.ColumnsAdded)
	}
	if r.StagingExists {
		t.Errorf("staging table should be dropped after promote")
	}
	wantRows := map[float64]float64{1: 10, 2: 99, 3: 30}
	if len(r.TargetRows) != len(wantRows) {
		t.Fatalf("rows: want %d, got %d (%v)", len(wantRows), len(r.TargetRows), r.TargetRows)
	}
	for _, row := range r.TargetRows {
		id := row["id"].(float64)
		if got := row["amount"].(float64); got != wantRows[id] {
			t.Errorf("id=%v: want amount=%v, got %v", id, wantRows[id], got)
		}
	}
}

// Schema-evolving MERGE: staging has an extra `processed_at` column the
// canonical doesn't. Before the fix this silently dropped the column.
// After the fix the runner ALTERs canonical to add the column, then the
// MERGE populates it for matched / inserted rows; the original canonical
// row reads back NULL on that column.
func TestRunner_PromoteMerge_SchemaEvolution(t *testing.T) {
	r := runWarmPromote(t, warmPromoteScript(
		"wt_pm_evo",
		"id long, amount long",
		"id long, amount long, processed_at string",
		`[{"id":1,"amount":10},{"id":2,"amount":20}]`,
		`[{"id":2,"amount":99,"processed_at":"2026-05-24T00:00:00Z"},{"id":3,"amount":30,"processed_at":"2026-05-24T00:00:01Z"}]`,
		"merge", `["id"]`, "", false,
	))
	if r.Result.Status != "ok" {
		t.Fatalf("status: want ok, got %q", r.Result.Status)
	}
	if len(r.Result.ColumnsAdded) != 1 || r.Result.ColumnsAdded[0] != "processed_at" {
		t.Errorf("columns_added: want [processed_at], got %v", r.Result.ColumnsAdded)
	}
	// Canonical schema must include processed_at after promote.
	cols := map[string]string{}
	for _, fld := range r.TargetSchema {
		cols[fld[0]] = fld[1]
	}
	if cols["processed_at"] == "" {
		t.Fatalf("processed_at column missing from canonical after promote: %v", r.TargetSchema)
	}
	// Row 1: unchanged canonical row, processed_at must read back NULL.
	// Rows 2 and 3: from staging, processed_at must be populated.
	for _, row := range r.TargetRows {
		id := row["id"].(float64)
		pAt, ok := row["processed_at"]
		switch id {
		case 1:
			if ok && pAt != nil {
				t.Errorf("id=1: processed_at should be NULL (canonical row pre-evolution), got %v", pAt)
			}
		case 2, 3:
			if !ok || pAt == nil || pAt == "" {
				t.Errorf("id=%v: processed_at should be populated from staging, got %v", id, pAt)
			}
		}
	}
}

// Append-mode + allow_duplicates path used to be `INSERT INTO target
// SELECT * FROM staging` — positional, errors on arity mismatch. The fix
// switches to DataFrameWriter.append() with mergeSchema=true so a new
// column lands cleanly. Asserts both that no error is raised and that
// the new column appears in canonical with populated values for staging
// rows + NULL for the pre-evolution canonical row.
func TestRunner_PromoteAppendAllowDupes_SchemaEvolution(t *testing.T) {
	r := runWarmPromote(t, warmPromoteScript(
		"wt_pm_app",
		"id long, amount long",
		"id long, amount long, processed_at string",
		`[{"id":1,"amount":10}]`,
		`[{"id":2,"amount":20,"processed_at":"2026-05-24T00:00:00Z"}]`,
		"append", `[]`, "", true,
	))
	if r.Result.Status != "ok" {
		t.Fatalf("status: want ok, got %q", r.Result.Status)
	}
	if len(r.Result.ColumnsAdded) != 1 || r.Result.ColumnsAdded[0] != "processed_at" {
		t.Errorf("columns_added: want [processed_at], got %v", r.Result.ColumnsAdded)
	}
	if len(r.TargetRows) != 2 {
		t.Fatalf("rows: want 2 (original + appended), got %d (%v)", len(r.TargetRows), r.TargetRows)
	}
}

// ---------------------------------------------------------------------------
// CDF-path tests: aggregate-aware merge (#19) and OPTIMIZE non-re-emission
// (#15). Both exercise the real readChangeFeed incremental path against a
// managed Delta source, not a full re-read.
// ---------------------------------------------------------------------------

// TestRunner_IncrementalMergeAdditive proves the #19 fix end-to-end on the
// real CDF path: an `additive` merge_update output reading an upstream via
// `delta_table_cdf` accumulates a lifetime counter across batches instead of
// the prior `UPDATE SET *` clobber. Key A is seen twice in batch 1 and once
// in batch 2; its merged count must be 3 (2+1), not 1 (replaced) or 5
// (full re-read summed in).
func TestRunner_IncrementalMergeAdditive(t *testing.T) {
	script := warmPrelude("wt_incr") + `
with open(LOGIC, "w") as f:
    f.write("SELECT k, COUNT(*) AS cnt FROM src GROUP BY k")

event = {
    "inputs": {"src": {"kind": "delta_table_cdf", "table": "wt_incr.src"}},
    "outputs": {"default": {"kind": "delta_table", "table_id": "wt_incr.dim",
        "mode": "merge", "merge_keys": ["k"], "merge_update": {"cnt": "additive"}}},
}

# Batch 1: A x2, B x1 -> first incremental run reads the full snapshot, creates dim.
spark.createDataFrame([{"k": "A"}, {"k": "A"}, {"k": "B"}]).write.format("delta").mode("append").saveAsTable("wt_incr.src")
r1 = handler(event, None)

# Batch 2: A x1, C x1 (one commit) -> CDF read of only the new rows, additive merge.
spark.createDataFrame([{"k": "A"}, {"k": "C"}]).write.format("delta").mode("append").saveAsTable("wt_incr.src")
r2 = handler(event, None)

rows = {row["k"]: int(row["cnt"]) for row in spark.table("wt_incr.dim").collect()}
RESULT = {"r1": r1.get("status"), "r2": r2.get("status"), "rows": rows}
`
	r := warmRun(t, script)
	if r["r1"] != "ok" || r["r2"] != "ok" {
		t.Fatalf("handler status: r1=%v r2=%v (want ok/ok)", r["r1"], r["r2"])
	}
	rows, _ := r["rows"].(map[string]any)
	want := map[string]float64{"A": 3, "B": 1, "C": 1}
	for k, w := range want {
		if got, _ := rows[k].(float64); got != w {
			t.Errorf("cnt[%s]: want %v, got %v (full rows: %v)", k, w, rows[k], rows)
		}
	}
	if len(rows) != len(want) {
		t.Errorf("row count: want %d keys, got %v", len(want), rows)
	}
}

// TestRunner_OptimizeCDFNoReemit proves the #15 CDF-safety claim: an OPTIMIZE
// commit is dataChange=false, so a downstream readChangeFeed range spanning it
// returns no change rows — the consumer neither errors nor re-emits already-
// consumed data. The consumer's lifetime counts must be unchanged after the
// source is OPTIMIZEd and the consumer re-runs.
func TestRunner_OptimizeCDFNoReemit(t *testing.T) {
	script := warmPrelude("wt_opt") + `
with open(LOGIC, "w") as f:
    f.write("SELECT k, COUNT(*) AS cnt FROM src2 GROUP BY k")

event = {
    "inputs": {"src2": {"kind": "delta_table_cdf", "table": "wt_opt.src2"}},
    "outputs": {"default": {"kind": "delta_table", "table_id": "wt_opt.dim2",
        "mode": "merge", "merge_keys": ["k"], "merge_update": {"cnt": "additive"}}},
}

# Two append commits, then a first consumer run reads the full snapshot.
spark.createDataFrame([{"k": "A"}]).write.format("delta").mode("append").saveAsTable("wt_opt.src2")
spark.createDataFrame([{"k": "B"}]).write.format("delta").mode("append").saveAsTable("wt_opt.src2")
r1 = handler(event, None)
before = {row["k"]: int(row["cnt"]) for row in spark.table("wt_opt.dim2").collect()}

# OPTIMIZE the source: a dataChange=false commit that bumps the version.
opt = _run_operation({"_operation": "optimize", "table": "wt_opt.src2"})

# Re-run the consumer: its CDF range now spans only the OPTIMIZE commit.
r2 = handler(event, None)
after = {row["k"]: int(row["cnt"]) for row in spark.table("wt_opt.dim2").collect()}

RESULT = {"r1": r1.get("status"), "opt": opt.get("status"),
    "r2": r2.get("status"), "before": before, "after": after}
`
	r := warmRun(t, script)
	if r["r1"] != "ok" {
		t.Fatalf("first consumer run: want ok, got %v", r["r1"])
	}
	if r["opt"] != "ok" {
		t.Fatalf("optimize op: want ok, got %v", r["opt"])
	}
	if r["r2"] != "ok" && r["r2"] != "skipped" {
		t.Fatalf("consumer run after OPTIMIZE: want ok/skipped (no error), got %v", r["r2"])
	}
	before, _ := r["before"].(map[string]any)
	after, _ := r["after"].(map[string]any)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Errorf("counts changed across OPTIMIZE (re-emit!): before=%v after=%v", before, after)
	}
	// Sanity: the counts are the lifetime values, not zeroed.
	if got, _ := after["A"].(float64); got != 1 {
		t.Errorf("after[A]: want 1, got %v (full: %v)", got, after)
	}
	if got, _ := after["B"].(float64); got != 1 {
		t.Errorf("after[B]: want 1, got %v (full: %v)", got, after)
	}
}

// TestRunner_MergeBoundBy proves the GH #62 `bound_by` MERGE scan-bound feature
// on the real Delta MERGE path, covering both sides of the determinism tripwire:
//
//   - Happy path: an `bound_by = ["d"]` merge output where the merge_keys
//     functionally determine `d` within each staging batch. The bound predicate
//     prunes the target scan, the MERGE applies, and the result is correct with
//     no duplicates.
//   - Raise path: a full-snapshot (non-CDF) input where the SAME merge key carries
//     two distinct `d` values in one staging batch. bound_by is then NOT
//     functionally determined by the keys, so the static bound could drop a real
//     match and silently duplicate — the runner's tripwire raises and handler()
//     re-raises, failing the run with a clear message instead of corrupting data.
//
// The raise path deliberately uses a plain bare-string input (`spark.table` full
// re-read) rather than `delta_table_cdf`: the CDF read dedups to one change row
// per key, which would make the per-batch tripwire vacuous. `_resolve_input` has
// no `delta_table` descriptor kind, so the string form is the un-deduped
// full-snapshot read available to the test.
func TestRunner_MergeBoundBy(t *testing.T) {
	script := warmPrelude("wt_bound") + `
import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType

d1 = datetime.date(2026, 1, 1)
d2 = datetime.date(2026, 1, 2)

# Explicit schema so 'd' is a real DateType column (DATE literals in the bound
# predicate / tripwire) and 'v' a LongType.
schema = StructType([
    StructField("k", StringType(), True),
    StructField("d", DateType(), True),
    StructField("v", LongType(), True),
])

# ---------- Happy path: CDF input, bound_by functionally determined ----------
with open(LOGIC, "w") as f:
    f.write("SELECT k, d, v FROM src")

event = {
    "inputs": {"src": {"kind": "delta_table_cdf", "table": "wt_bound.src"}},
    "outputs": {"default": {"kind": "delta_table", "table_id": "wt_bound.fct",
        "mode": "merge", "merge_keys": ["k"], "cluster_by": ["d"], "bound_by": ["d"]}},
}

# Batch 1: first incremental run reads the full snapshot and CREATES wt_bound.fct
# (MERGE has nothing to match against on the create run, so the tripwire is not
# yet exercised here).
spark.createDataFrame([("A", d1, 1), ("B", d1, 1)], schema).write.format("delta").mode("append").saveAsTable("wt_bound.src")
r1 = handler(event, None)

# Batch 2: one new commit (key C, d2). The CDF read returns only the new row;
# each key maps to exactly one d, so bound_by passes and the MERGE inserts C.
spark.createDataFrame([("C", d2, 1)], schema).write.format("delta").mode("append").saveAsTable("wt_bound.src")
r2 = handler(event, None)

cnt = spark.table("wt_bound.fct").count()
dups = spark.sql("SELECT k FROM wt_bound.fct GROUP BY k HAVING count(*) > 1").count()

# ---------- Raise path: full-snapshot input, bound_by NOT determined ----------
with open(LOGIC, "w") as f:
    f.write("SELECT k, d, v FROM src2")

# Bare-string input -> spark.table() full re-read each run (no watermark, no
# CDF dedup), so the staging batch keeps every row for a key.
ev2 = {
    "inputs": {"src2": "wt_bound.src2"},
    "outputs": {"default": {"kind": "delta_table", "table_id": "wt_bound.fct2",
        "mode": "merge", "merge_keys": ["k"], "cluster_by": ["d"], "bound_by": ["d"]}},
}

# First run: one clean row per key -> creates wt_bound.fct2, skips MERGE.
spark.createDataFrame([("A", d1, 1)], schema).write.format("delta").mode("append").saveAsTable("wt_bound.src2")
handler(ev2, None)

# Now the SAME key A carries two distinct d values in the full snapshot. The
# second run goes through MERGE; the determinism tripwire fires and handler()
# re-raises after recording the failure.
spark.createDataFrame([("A", d2, 2)], schema).write.format("delta").mode("append").saveAsTable("wt_bound.src2")
try:
    handler(ev2, None)
    raised = False
    msg = ""
except Exception as e:
    raised = True
    msg = str(e)

RESULT = {
    "r1": r1.get("status"), "r2": r2.get("status"),
    "cnt": cnt, "dups": dups,
    "raised": raised, "msg_ok": ("not functionally determined" in msg),
}
`
	r := warmRun(t, script)

	// Happy path: both runs ok, 3 rows total, no duplicate keys.
	if r["r1"] != "ok" || r["r2"] != "ok" {
		t.Fatalf("happy-path handler status: r1=%v r2=%v (want ok/ok)", r["r1"], r["r2"])
	}
	if got, _ := r["cnt"].(float64); got != 3 {
		t.Errorf("wt_bound.fct row count: want 3, got %v", r["cnt"])
	}
	if got, _ := r["dups"].(float64); got != 0 {
		t.Errorf("wt_bound.fct duplicate keys: want 0, got %v", r["dups"])
	}

	// Raise path: tripwire must fire with the expected message.
	if raised, _ := r["raised"].(bool); !raised {
		t.Errorf("raise-path: expected handler to raise on non-determined bound_by, but it did not (raised=%v)", r["raised"])
	}
	if msgOK, _ := r["msg_ok"].(bool); !msgOK {
		t.Errorf("raise-path: tripwire message missing %q substring (msg_ok=%v)", "not functionally determined", r["msg_ok"])
	}
}

// ---------------------------------------------------------------------------
// Additive schema evolution across output modes (GH #39, GH #61 and the
// escalated deal_scores replace-mode failure). Grouped under
// TestRunner_SchemaEvolution_* so `go test -run TestRunner_SchemaEvolution`
// runs the set.
// ---------------------------------------------------------------------------

// TestRunner_SchemaEvolution_ReplaceAddColumn proves the replace-mode fix:
// run 2 adds a column to the transform's SQL and must succeed with the new
// column materialized (previously Delta schema enforcement failed the
// saveAsTable); run 3 changes an existing column's type and must also
// succeed (GH #39 — previously required a manual DROP TABLE).
func TestRunner_SchemaEvolution_ReplaceAddColumn(t *testing.T) {
	script := warmPrelude("wt_rep") + `
spark.createDataFrame(
    [{"id": 1, "amount": 10}, {"id": 2, "amount": 20}]
).write.format("delta").mode("append").saveAsTable("wt_rep.src_rep")

event = {
    "inputs": {"src": "wt_rep.src_rep"},
    "outputs": {"default": {"kind": "delta_table", "table_id": "wt_rep.rep", "mode": "replace"}},
}

# Run 1: baseline two-column table.
with open(LOGIC, "w") as f:
    f.write("SELECT id, amount FROM src")
r1 = handler(event, None)

# Run 2: the escalated bug — same output, one added column.
with open(LOGIC, "w") as f:
    f.write("SELECT id, amount, amount * 2 AS amount_doubled FROM src")
r2 = handler(event, None)
schema2 = {f.name: f.dataType.simpleString() for f in spark.table("wt_rep.rep").schema}
rows2 = {str(r["id"]): r.asDict() for r in spark.table("wt_rep.rep").collect()}

# Run 3: GH #39 — an existing column's type changes.
with open(LOGIC, "w") as f:
    f.write("SELECT id, CAST(amount AS STRING) AS amount FROM src")
r3 = handler(event, None)
schema3 = {f.name: f.dataType.simpleString() for f in spark.table("wt_rep.rep").schema}

RESULT = {
    "r1": r1.get("status"), "r2": r2.get("status"), "r3": r3.get("status"),
    "schema2": schema2, "rows2": rows2, "schema3": schema3,
}
`
	r := warmRun(t, script)
	if r["r1"] != "ok" || r["r2"] != "ok" {
		t.Fatalf("replace add-column: r1=%v r2=%v (want ok/ok)", r["r1"], r["r2"])
	}
	schema2 := asStrMap(r["schema2"])
	if schema2["amount_doubled"] == "" {
		t.Errorf("run 2: amount_doubled missing from table schema: %v", r["schema2"])
	}
	rows2, _ := r["rows2"].(map[string]any)
	if row, _ := rows2["1"].(map[string]any); row != nil {
		if got, _ := row["amount_doubled"].(float64); got != 20 {
			t.Errorf("run 2: id=1 amount_doubled: want 20, got %v", row["amount_doubled"])
		}
	} else {
		t.Errorf("run 2: id=1 row missing: %v", rows2)
	}
	if r["r3"] != "ok" {
		t.Fatalf("replace type-change (GH #39): r3=%v (want ok)", r["r3"])
	}
	schema3 := asStrMap(r["schema3"])
	if schema3["amount"] != "string" {
		t.Errorf("run 3: amount type: want string, got %q (schema: %v)", schema3["amount"], r["schema3"])
	}
}

// TestRunner_SchemaEvolution_MergeAddColumn proves GH #61 on the UPDATE SET * /
// INSERT * merge path with a multi-column merge key (real rollup shape, not a
// single id): run 2 adds a `scrolls` column. The matched row must be updated
// WITH the new column's value, the new row inserted with it, and the row NOT
// in run 2's source must survive untouched with NULL in the new column —
// nothing dropped, no destructive reset needed.
func TestRunner_SchemaEvolution_MergeAddColumn(t *testing.T) {
	script := warmPrelude("wt_m1") + `
import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType

d1 = datetime.date(2026, 6, 1)
d2 = datetime.date(2026, 6, 2)
schema = StructType([
    StructField("site", StringType(), True),
    StructField("d", DateType(), True),
    StructField("v", LongType(), True),
])

event = {
    "inputs": {"src": "wt_m1.src_m1"},
    "outputs": {"default": {"kind": "delta_table", "table_id": "wt_m1.rollup1",
        "mode": "merge", "merge_keys": ["site", "d"]}},
}

# Run 1: creates the target with the two-key three-column shape.
with open(LOGIC, "w") as f:
    f.write("SELECT site, d, v FROM src")
spark.createDataFrame([("a.com", d1, 1), ("b.com", d1, 2)], schema).write.format("delta").mode("overwrite").saveAsTable("wt_m1.src_m1")
r1 = handler(event, None)

# Run 2: transform gains a column; the batch touches a.com (matched) and
# c.com (new) but NOT b.com — b.com must keep its data and read NULL scrolls.
with open(LOGIC, "w") as f:
    f.write("SELECT site, d, v, v * 10 AS scrolls FROM src")
spark.createDataFrame([("a.com", d1, 5), ("c.com", d2, 3)], schema).write.format("delta").mode("overwrite").saveAsTable("wt_m1.src_m1")
r2 = handler(event, None)

rows = {}
for r in spark.table("wt_m1.rollup1").collect():
    rd = r.asDict()
    rows[rd["site"]] = {"v": rd["v"], "scrolls": rd.get("scrolls")}
schema_out = {f.name: f.dataType.simpleString() for f in spark.table("wt_m1.rollup1").schema}
RESULT = {
    "r1": r1.get("status"), "r2": r2.get("status"),
    "rows": rows, "schema": schema_out,
}
`
	r := warmRun(t, script)
	if r["r1"] != "ok" || r["r2"] != "ok" {
		t.Fatalf("merge add-column: r1=%v r2=%v (want ok/ok)", r["r1"], r["r2"])
	}
	if schema := asStrMap(r["schema"]); schema["scrolls"] == "" {
		t.Fatalf("scrolls column missing from target schema after evolving MERGE: %v", r["schema"])
	}
	rows, _ := r["rows"].(map[string]any)
	if len(rows) != 3 {
		t.Fatalf("row count: want 3 (a+b+c, nothing dropped), got %d: %v", len(rows), rows)
	}
	get := func(site string) map[string]any {
		m, _ := rows[site].(map[string]any)
		if m == nil {
			t.Fatalf("row for %s missing: %v", site, rows)
		}
		return m
	}
	a := get("a.com")
	if v, _ := a["v"].(float64); v != 5 {
		t.Errorf("a.com v: want 5 (matched row updated), got %v", a["v"])
	}
	if s, _ := a["scrolls"].(float64); s != 50 {
		t.Errorf("a.com scrolls: want 50 (matched row gets new column value), got %v", a["scrolls"])
	}
	c := get("c.com")
	if s, _ := c["scrolls"].(float64); s != 30 {
		t.Errorf("c.com scrolls: want 30 (inserted row), got %v", c["scrolls"])
	}
	b := get("b.com")
	if b["scrolls"] != nil {
		t.Errorf("b.com scrolls: want NULL (pre-evolution row untouched), got %v", b["scrolls"])
	}
	if v, _ := b["v"].(float64); v != 2 {
		t.Errorf("b.com v: want 2 (untouched row keeps data), got %v", b["v"])
	}
}

// TestRunner_SchemaEvolution_MergeUpdateAddColumn is the merge_update variant:
// with an explicit WHEN MATCHED THEN UPDATE SET clause (_merge_set_clause,
// here `v = additive`), a newly-added source column must still evolve the
// target. Delta resolves the clause's `target.scrolls = source.scrolls`
// assignment against the CURRENT target schema — `WITH SCHEMA EVOLUTION`
// never rescued it (DELTA_MERGE_UNRESOLVED_EXPRESSION) — so the runner must
// ALTER the target to add the column before the MERGE. Multi-column merge
// key, same untouched-row-NULL assertion.
func TestRunner_SchemaEvolution_MergeUpdateAddColumn(t *testing.T) {
	script := warmPrelude("wt_m2") + `
import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType

d1 = datetime.date(2026, 6, 1)
d2 = datetime.date(2026, 6, 2)
schema = StructType([
    StructField("site", StringType(), True),
    StructField("d", DateType(), True),
    StructField("v", LongType(), True),
])

event = {
    "inputs": {"src": "wt_m2.src_m2"},
    "outputs": {"default": {"kind": "delta_table", "table_id": "wt_m2.rollup2",
        "mode": "merge", "merge_keys": ["site", "d"],
        "merge_update": {"v": "additive"}}},
}

with open(LOGIC, "w") as f:
    f.write("SELECT site, d, v FROM src")
spark.createDataFrame([("a.com", d1, 1), ("b.com", d1, 2)], schema).write.format("delta").mode("overwrite").saveAsTable("wt_m2.src_m2")
r1 = handler(event, None)

with open(LOGIC, "w") as f:
    f.write("SELECT site, d, v, v * 10 AS scrolls FROM src")
spark.createDataFrame([("a.com", d1, 4), ("c.com", d2, 3)], schema).write.format("delta").mode("overwrite").saveAsTable("wt_m2.src_m2")
r2 = handler(event, None)

rows = {}
for r in spark.table("wt_m2.rollup2").collect():
    rd = r.asDict()
    rows[rd["site"]] = {"v": rd["v"], "scrolls": rd.get("scrolls")}
RESULT = {
    "r1": r1.get("status"), "r2": r2.get("status"), "rows": rows,
}
`
	r := warmRun(t, script)
	if r["r1"] != "ok" || r["r2"] != "ok" {
		t.Fatalf("merge_update add-column: r1=%v r2=%v (want ok/ok)", r["r1"], r["r2"])
	}
	rows, _ := r["rows"].(map[string]any)
	if len(rows) != 3 {
		t.Fatalf("row count: want 3, got %d: %v", len(rows), rows)
	}
	get := func(site string) map[string]any {
		m, _ := rows[site].(map[string]any)
		if m == nil {
			t.Fatalf("row for %s missing: %v", site, rows)
		}
		return m
	}
	a := get("a.com")
	if v, _ := a["v"].(float64); v != 5 {
		t.Errorf("a.com v: want 5 (additive 1+4), got %v", a["v"])
	}
	if s, _ := a["scrolls"].(float64); s != 40 {
		t.Errorf("a.com scrolls: want 40 (new column replaces from source on matched row), got %v", a["scrolls"])
	}
	c := get("c.com")
	if s, _ := c["scrolls"].(float64); s != 30 {
		t.Errorf("c.com scrolls: want 30 (inserted row), got %v", c["scrolls"])
	}
	b := get("b.com")
	if b["scrolls"] != nil {
		t.Errorf("b.com scrolls: want NULL (untouched pre-evolution row), got %v", b["scrolls"])
	}
}
