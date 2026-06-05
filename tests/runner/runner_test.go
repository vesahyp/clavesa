//go:build integration

// Package runner_test exercises the Clavesa transform runner end-to-end via
// the Docker image. Covers preview mode (the path the UI hits) and the
// production handler (the path Lambda hits) over local-FS input/output.
//
// S3-via-motoserver and Lambda-RIE tests are deferred.
package runner_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const image = "clavesa-runner-spark:itest"

func TestMain(m *testing.M) {
	cmd := exec.Command("docker", "build", "-q", "-t", image, repoPath("runner"))
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build runner image: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func repoPath(rel string) string {
	_, file, _, _ := runtime.Caller(0)
	// tests/runner/runner_test.go → tests/runner → tests → repo root
	root := filepath.Dir(filepath.Dir(filepath.Dir(file)))
	return filepath.Join(root, rel)
}

type dockerOpts struct {
	env        map[string]string
	mountSrc   string // host path
	mountDst   string // container path
	entrypoint string
	cmd        []string
	stdin      string
}

func dockerRun(t *testing.T, opts dockerOpts) string {
	t.Helper()
	out, _ := dockerRunCapture(t, opts)
	return out
}

// dockerRunCapture is dockerRun but also returns the container's stderr, which
// some tests assert on (e.g. counting the "[clavesa] spark master = ..." line
// the runner prints once per Spark session build).
func dockerRunCapture(t *testing.T, opts dockerOpts) (stdout, stderr string) {
	t.Helper()
	args := []string{"run", "--rm"}
	if opts.stdin != "" {
		args = append(args, "-i")
	}
	for k, v := range opts.env {
		args = append(args, "-e", k+"="+v)
	}
	if opts.mountSrc != "" {
		args = append(args, "-v", opts.mountSrc+":"+opts.mountDst)
	}
	if opts.entrypoint != "" {
		args = append(args, "--entrypoint", opts.entrypoint)
	}
	args = append(args, image)
	args = append(args, opts.cmd...)

	cmd := exec.Command("docker", args...)
	if opts.stdin != "" {
		cmd.Stdin = strings.NewReader(opts.stdin)
	}
	var errBuf strings.Builder
	cmd.Stderr = &errBuf
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("docker run failed: %v\nstdout: %s\nstderr: %s", err, out, errBuf.String())
	}
	return string(out), errBuf.String()
}

// ---------------------------------------------------------------------------
// Preview-mode tests (the path the UI exercises via the Go preview package)
// ---------------------------------------------------------------------------

func TestRunner_PreviewSQL(t *testing.T) {
	out := dockerRun(t, dockerOpts{
		env: map[string]string{
			"CLAVESA_PREVIEW":              "1",
			"CLAVESA_PREVIEW_INPUT_ORDERS": `[{"id":1,"amount":10},{"id":2,"amount":20},{"id":3,"amount":30}]`,
			"CLAVESA_SQL":                  "SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders WHERE amount > 10",
		},
		entrypoint: "python",
		cmd:        []string{"/var/task/runner.py"},
	})

	var result map[string][]map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &result); err != nil {
		t.Fatalf("parse output: %v\nout: %s", err, out)
	}
	rows := result["default"]
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d: %v", len(rows), rows)
	}
	if got := rows[0]["n"]; got != float64(2) {
		t.Errorf("n: want 2, got %v", got)
	}
	if got := rows[0]["total"]; got != float64(50) {
		t.Errorf("total: want 50, got %v", got)
	}
}

func TestRunner_PreviewPython(t *testing.T) {
	script := `
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

def is_bot(ua):
    return any(s in ua.lower() for s in ["bot", "crawler", "curl"])

def transform(spark, inputs):
    is_bot_udf = udf(is_bot, BooleanType())
    df = inputs["logs"].withColumn("is_bot", is_bot_udf("ua"))
    return {"default": df}
`
	out := dockerRun(t, dockerOpts{
		env: map[string]string{
			"CLAVESA_PREVIEW":            "1",
			"CLAVESA_PREVIEW_INPUT_LOGS": `[{"ua":"Mozilla/5.0 Chrome/120"},{"ua":"Googlebot/2.1"},{"ua":"curl/8.0"}]`,
			"CLAVESA_PYTHON_SCRIPT":      script,
		},
		entrypoint: "python",
		cmd:        []string{"/var/task/runner.py"},
	})

	var result map[string][]map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &result); err != nil {
		t.Fatalf("parse output: %v\nout: %s", err, out)
	}
	rows := result["default"]
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(rows), rows)
	}
	want := []bool{false, true, true}
	for i, r := range rows {
		if got := r["is_bot"]; got != want[i] {
			t.Errorf("row %d is_bot: want %v, got %v (ua=%v)", i, want[i], got, r["ua"])
		}
	}
}

// ---------------------------------------------------------------------------
// Production-mode tests (the path Lambda exercises via runner.handler)
// ---------------------------------------------------------------------------

// driverScript returns a Python script that:
//  1. Creates an `orders` Parquet input via Spark
//  2. Writes the supplied SQL or Python transform to a logic file
//  3. Imports runner.handler and invokes it with a local-FS event
//  4. Reads the output Parquet back via Spark
//  5. Prints {"result": ..., "rows": [...]} as the final stdout line
func driverScript(language, logic string) string {
	return fmt.Sprintf(`
import json, os, sys, shutil
shutil.rmtree("/work/inputs", ignore_errors=True)
shutil.rmtree("/work/outputs", ignore_errors=True)
os.makedirs("/work/inputs/orders", exist_ok=True)

sys.path.insert(0, "/var/task")
os.environ["CLAVESA_LOGIC_S3_PATH"] = "/work/logic.txt"
os.environ["CLAVESA_LANGUAGE"] = %q

from runner import _spark, handler

spark = _spark()
spark.createDataFrame(
    [{"id": 1, "amount": 10}, {"id": 2, "amount": 20}, {"id": 3, "amount": 30}]
).write.mode("overwrite").parquet("/work/inputs/orders")

with open("/work/logic.txt", "w") as f:
    f.write(%q)

event = {
    "inputs": {"orders": "/work/inputs/orders"},
    "outputs": {"default": "/work/outputs/default"},
}
result = handler(event, None)

rows = sorted(
    (r.asDict() for r in spark.read.parquet("/work/outputs/default").collect()),
    key=lambda r: tuple(sorted(r.items())),
)
print("RESULT_LINE:" + json.dumps({"result": result, "rows": rows}))
`, language, logic)
}

type handlerResult struct {
	Result struct {
		Status  string            `json:"status"`
		Outputs map[string]string `json:"outputs"`
	} `json:"result"`
	Rows []map[string]any `json:"rows"`
}

// runDriver mounts a temp dir, writes the driver script, runs the container,
// and returns the parsed RESULT_LINE.
func runDriver(t *testing.T, script string) handlerResult {
	t.Helper()
	work := t.TempDir()
	scriptPath := filepath.Join(work, "driver.py")
	if err := os.WriteFile(scriptPath, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out := dockerRun(t, dockerOpts{
		mountSrc:   work,
		mountDst:   "/work",
		entrypoint: "python",
		cmd:        []string{"/work/driver.py"},
	})

	// Find the RESULT_LINE prefix — Spark writes a lot of warnings before it.
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "RESULT_LINE:") {
			var r handlerResult
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RESULT_LINE:")), &r); err != nil {
				t.Fatalf("parse RESULT_LINE: %v\nline: %s", err, line)
			}
			return r
		}
	}
	t.Fatalf("RESULT_LINE not found in output:\n%s", out)
	return handlerResult{}
}

func TestRunner_HandlerLocalSQL(t *testing.T) {
	r := runDriver(t, driverScript("sql",
		"SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders WHERE amount > 10",
	))

	if r.Result.Status != "ok" {
		t.Fatalf("handler status: want ok, got %q", r.Result.Status)
	}
	if r.Result.Outputs["default"] != "/work/outputs/default" {
		t.Errorf("output path: %v", r.Result.Outputs)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("want 1 row, got %d: %v", len(r.Rows), r.Rows)
	}
	if got := r.Rows[0]["n"]; got != float64(2) {
		t.Errorf("n: want 2, got %v", got)
	}
	if got := r.Rows[0]["total"]; got != float64(50) {
		t.Errorf("total: want 50, got %v", got)
	}
}

func TestRunner_HandlerLocalPython(t *testing.T) {
	pyTransform := `
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

def transform(spark, inputs):
    big = udf(lambda x: x > 15, BooleanType())
    df = inputs["orders"].withColumn("big", big("amount"))
    return {"default": df}
`
	r := runDriver(t, driverScript("python", pyTransform))

	if r.Result.Status != "ok" {
		t.Fatalf("handler status: want ok, got %q", r.Result.Status)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(r.Rows), r.Rows)
	}
	// Rows are sorted by (amount, big, id) tuple — but the driver sorts by all
	// items so the order is deterministic. amount=10,big=false; 20,big=true; 30,big=true.
	for _, r := range r.Rows {
		amount := r["amount"].(float64)
		expectedBig := amount > 15
		if got := r["big"].(bool); got != expectedBig {
			t.Errorf("amount=%v big=%v, expected %v", amount, got, expectedBig)
		}
	}
}

// ---------------------------------------------------------------------------
// Operation-mode tests (the path UI's promote / discard buttons exercise)
// ---------------------------------------------------------------------------

// promoteDriverScript creates a Hadoop-catalog Iceberg canonical + staging
// table with the given column schemas, invokes runner._run_operation with
// _operation=backfill_promote, and prints {"result": ..., "target_schema":
// [...], "target_rows": [...]} so the test asserts both the operation
// response and the resulting table state.
func promoteDriverScript(canonicalCols, stagingCols, canonicalRows, stagingRows, mode, mergeKeys, forceDedup string, allowDupes bool) string {
	return fmt.Sprintf(`
import json, os, sys, shutil

shutil.rmtree("/work/warehouse", ignore_errors=True)
os.makedirs("/work/warehouse", exist_ok=True)
os.environ["CLAVESA_WAREHOUSE"] = "/work/warehouse"
os.environ["CLAVESA_PIPELINE"] = "p"
os.environ["CLAVESA_NODE"] = "n"

sys.path.insert(0, "/var/task")
from runner import _spark, _run_operation

spark = _spark()
spark.sql("CREATE NAMESPACE IF NOT EXISTS clavesa.itest")

# Canonical table — created via writeTo so it's a real Iceberg table.
spark.createDataFrame(%s, %q).writeTo("clavesa.itest.canon").createOrReplace()
# Staging table — column set may differ from canonical.
spark.createDataFrame(%s, %q).writeTo("clavesa.itest.canon__backfill__rid").createOrReplace()

event = {
    "_operation": "backfill_promote",
    "staging": "clavesa.itest.canon__backfill__rid",
    "target": "clavesa.itest.canon",
    "mode": %q,
    "merge_keys": %s,
    "force_dedup": %q,
    "allow_duplicates": %s,
}
result = _run_operation(event)

target_schema = [(f.name, f.dataType.simpleString()) for f in spark.table("clavesa.itest.canon").schema]
target_rows = sorted(
    (r.asDict() for r in spark.table("clavesa.itest.canon").collect()),
    key=lambda r: tuple((k, v) for k, v in sorted(r.items()) if v is not None),
)
staging_exists = spark.catalog.tableExists("clavesa.itest.canon__backfill__rid")
print("RESULT_LINE:" + json.dumps({
    "result": result,
    "target_schema": target_schema,
    "target_rows": target_rows,
    "staging_exists": staging_exists,
}))
`, canonicalRows, canonicalCols, stagingRows, stagingCols,
		mode, mergeKeys, forceDedup, pyBool(allowDupes))
}

func pyBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

type promoteResult struct {
	Result struct {
		Status        string   `json:"status"`
		Operation     string   `json:"operation"`
		Target        string   `json:"target"`
		StagingDropped string  `json:"staging_dropped"`
		ColumnsAdded  []string `json:"columns_added"`
	} `json:"result"`
	TargetSchema  [][]string       `json:"target_schema"`
	TargetRows    []map[string]any `json:"target_rows"`
	StagingExists bool             `json:"staging_exists"`
}

func runPromoteDriver(t *testing.T, script string) promoteResult {
	t.Helper()
	work := t.TempDir()
	scriptPath := filepath.Join(work, "driver.py")
	if err := os.WriteFile(scriptPath, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out := dockerRun(t, dockerOpts{
		mountSrc:   work,
		mountDst:   "/work",
		entrypoint: "python",
		cmd:        []string{"/work/driver.py"},
	})
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "RESULT_LINE:") {
			var r promoteResult
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RESULT_LINE:")), &r); err != nil {
				t.Fatalf("parse RESULT_LINE: %v\nline: %s", err, line)
			}
			return r
		}
	}
	t.Fatalf("RESULT_LINE not found in output:\n%s", out)
	return promoteResult{}
}

// Baseline: schemas match by name → MERGE updates existing keys, inserts
// new ones, columns_added is empty.
func TestRunner_PromoteMerge_SchemaMatch(t *testing.T) {
	r := runPromoteDriver(t, promoteDriverScript(
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
	r := runPromoteDriver(t, promoteDriverScript(
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
	r := runPromoteDriver(t, promoteDriverScript(
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
// Bundle session self-heal (#23): pipeline_handler reuses the module-level
// _SPARK singleton across nodes. If that cached session dies mid-bundle (GC
// pause trips the heartbeat, driver JVM gone, py4j gateway closed), the next
// node's _spark() must transparently rebuild it instead of erroring. The
// CLAVESA_TEST_KILL_SESSION_AFTER_NODE hook kills the session after a named
// node succeeds WITHOUT clearing the global, leaving a dead handle cached so
// the following node exercises the self-heal path.
// ---------------------------------------------------------------------------

// pipelineBundleEvent builds the on-disk logic files + the CLAVESA_RUN=1
// `_pipeline_run` stdin payload for a 2-node hermetic bundle. Each node is a
// self-contained SQL transform (no external inputs, no parent edges) writing a
// Delta table under the /work warehouse, so the test needs no fixtures and the
// two nodes are independent — a clean stage for the kill/rebuild assertion.
func pipelineBundleEvent(t *testing.T, work string) string {
	t.Helper()
	// logic_path points at host paths mounted into the container at /work.
	if err := os.WriteFile(filepath.Join(work, "logic_a.txt"), []byte("SELECT 1 AS x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "logic_b.txt"), []byte("SELECT 2 AS y"), 0o644); err != nil {
		t.Fatal(err)
	}
	event := map[string]any{
		"_pipeline_run":     true,
		"run_id":            "itest-heal",
		"_sf_execution_arn": "itest-heal",
		"_trigger":          "manual",
		"transforms": []map[string]any{
			{
				"node":       "a",
				"language":   "sql",
				"logic_path": "/work/logic_a.txt",
				"inputs":     map[string]any{},
				"outputs": map[string]any{
					"default": map[string]any{"kind": "delta_table", "table_id": "itest.a"},
				},
				"parents": []string{},
			},
			{
				"node":       "b",
				"language":   "sql",
				"logic_path": "/work/logic_b.txt",
				"inputs":     map[string]any{},
				"outputs": map[string]any{
					"default": map[string]any{"kind": "delta_table", "table_id": "itest.b"},
				},
				"parents": []string{},
			},
		},
	}
	b, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// bundleEnv mirrors the env the CLAVESA_RUN=1 local path sets for a bundle run,
// using the /work mount as the Delta warehouse so the run is hermetic.
func bundleEnv(extra map[string]string) map[string]string {
	env := map[string]string{
		"CLAVESA_RUN":        "1",
		"CLAVESA_WAREHOUSE":  "/work/wh",
		"CLAVESA_WATERMARKS": "/work/wm",
		"CLAVESA_CATALOG":    "itest_cat",
		"CLAVESA_SCHEMA":     "itest",
		"CLAVESA_PIPELINE":   "itest",
	}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

// bundleResult is the aggregated pipeline_handler result — the LAST stdout line
// (the only line with no top-level "_event" key).
type bundleResult struct {
	Status     string `json:"status"`
	FailedNode string `json:"failed_node"`
	Transforms []struct {
		Node   string `json:"node"`
		Status string `json:"status"`
	} `json:"transforms"`
}

// parseBundle scans the runner stdout for the per-node _event lines and the
// final aggregate result line. Returns the succeeded node set and the result.
func parseBundle(t *testing.T, stdout string) (succeeded map[string]bool, res bundleResult) {
	t.Helper()
	succeeded = map[string]bool{}
	var resultLine string
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var probe map[string]any
		if json.Unmarshal([]byte(line), &probe) != nil {
			continue
		}
		if ev, ok := probe["_event"]; ok {
			if ev == "succeeded" {
				if node, _ := probe["node"].(string); node != "" {
					succeeded[node] = true
				}
			}
			continue
		}
		// No _event key + has a "status" key => the aggregate result line.
		if _, ok := probe["status"]; ok {
			resultLine = line
		}
	}
	if resultLine == "" {
		t.Fatalf("aggregate result line not found in bundle stdout:\n%s", stdout)
	}
	if err := json.Unmarshal([]byte(resultLine), &res); err != nil {
		t.Fatalf("parse aggregate result: %v\nline: %s", err, resultLine)
	}
	return succeeded, res
}

// TestPipelineBundle_HealsDeadSession kills the shared Spark session after node
// `a` succeeds (without clearing the global), then asserts node `b` still
// succeeds — proving _spark() detected the dead cached handle and rebuilt it
// transparently mid-bundle. The "[clavesa] spark master = ..." stderr line is
// printed once per session build, so seeing it TWICE is direct proof of the
// rebuild.
func TestPipelineBundle_HealsDeadSession(t *testing.T) {
	work := t.TempDir()
	stdin := pipelineBundleEvent(t, work)

	stdout, stderr := dockerRunCapture(t, dockerOpts{
		env: bundleEnv(map[string]string{
			"CLAVESA_TEST_KILL_SESSION_AFTER_NODE": "a",
		}),
		mountSrc:   work,
		mountDst:   "/work",
		entrypoint: "python",
		cmd:        []string{"/var/task/runner.py"},
		stdin:      stdin,
	})

	succeeded, res := parseBundle(t, stdout)
	if !succeeded["a"] {
		t.Errorf("node a should have emitted succeeded; stdout:\n%s", stdout)
	}
	if !succeeded["b"] {
		t.Errorf("node b should have emitted succeeded after the session was killed (self-heal failed); stdout:\n%s", stdout)
	}
	if res.Status != "ok" {
		t.Errorf("bundle status: want ok, got %q (failed_node=%q)", res.Status, res.FailedNode)
	}

	// Two session builds = two master log lines = the rebuild happened.
	builds := strings.Count(stderr, "[clavesa] spark master = ")
	if builds != 2 {
		t.Errorf("expected 2 Spark session builds (initial + post-kill rebuild), got %d; stderr:\n%s", builds, stderr)
	}
}

// TestPipelineBundle_SingleSessionNoKill is the control: without the kill hook
// both nodes run on one session, so exactly one "[clavesa] spark master = ..."
// line appears. Proves the double-build in the heal test is caused by the kill,
// not by the bundle building a session per node.
func TestPipelineBundle_SingleSessionNoKill(t *testing.T) {
	work := t.TempDir()
	stdin := pipelineBundleEvent(t, work)

	stdout, stderr := dockerRunCapture(t, dockerOpts{
		env:        bundleEnv(nil),
		mountSrc:   work,
		mountDst:   "/work",
		entrypoint: "python",
		cmd:        []string{"/var/task/runner.py"},
		stdin:      stdin,
	})

	succeeded, res := parseBundle(t, stdout)
	if !succeeded["a"] || !succeeded["b"] {
		t.Errorf("both nodes should succeed in the control run; stdout:\n%s", stdout)
	}
	if res.Status != "ok" {
		t.Errorf("bundle status: want ok, got %q", res.Status)
	}
	if builds := strings.Count(stderr, "[clavesa] spark master = "); builds != 1 {
		t.Errorf("control: expected exactly 1 Spark session build, got %d; stderr:\n%s", builds, stderr)
	}
}

// ---------------------------------------------------------------------------
// CDF-path tests: aggregate-aware merge (#19) and OPTIMIZE non-re-emission
// (#15). Both exercise the real readChangeFeed incremental path against a
// managed Delta source, not a full re-read.
// ---------------------------------------------------------------------------

// runRawDriver runs a self-contained driver script (which sets up its own
// Delta state under the /work warehouse) and returns the parsed RESULT_LINE
// as a generic map — the result shapes here differ per test.
func runRawDriver(t *testing.T, script string) map[string]any {
	t.Helper()
	work := t.TempDir()
	scriptPath := filepath.Join(work, "driver.py")
	if err := os.WriteFile(scriptPath, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out := dockerRun(t, dockerOpts{
		mountSrc:   work,
		mountDst:   "/work",
		entrypoint: "python",
		cmd:        []string{"/work/driver.py"},
	})
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "RESULT_LINE:") {
			var r map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RESULT_LINE:")), &r); err != nil {
				t.Fatalf("parse RESULT_LINE: %v\nline: %s", err, line)
			}
			return r
		}
	}
	t.Fatalf("RESULT_LINE not found in output:\n%s", out)
	return nil
}

// TestRunner_IncrementalMergeAdditive proves the #19 fix end-to-end on the
// real CDF path: an `additive` merge_update output reading an upstream via
// `delta_table_cdf` accumulates a lifetime counter across batches instead of
// the prior `UPDATE SET *` clobber. Key A is seen twice in batch 1 and once
// in batch 2; its merged count must be 3 (2+1), not 1 (replaced) or 5
// (full re-read summed in).
func TestRunner_IncrementalMergeAdditive(t *testing.T) {
	script := `
import json, os, sys
sys.path.insert(0, "/var/task")
os.environ["CLAVESA_WAREHOUSE"] = "/work/wh"
os.environ["CLAVESA_WATERMARKS"] = "/work/wm"
os.environ["CLAVESA_LANGUAGE"] = "sql"
os.environ["CLAVESA_LOGIC_S3_PATH"] = "/work/logic.txt"
os.environ["CLAVESA_CATALOG"] = "itest_cat"
os.environ["CLAVESA_SCHEMA"] = "itest"
os.environ["CLAVESA_PIPELINE"] = "itest"
os.environ["CLAVESA_NODE"] = "itest"
os.makedirs("/work/wm", exist_ok=True)

from runner import _spark, handler

spark = _spark()
spark.sql("CREATE DATABASE IF NOT EXISTS itest")

with open("/work/logic.txt", "w") as f:
    f.write("SELECT k, COUNT(*) AS cnt FROM src GROUP BY k")

event = {
    "inputs": {"src": {"kind": "delta_table_cdf", "table": "itest.src"}},
    "outputs": {"default": {"kind": "delta_table", "table_id": "itest.dim",
        "mode": "merge", "merge_keys": ["k"], "merge_update": {"cnt": "additive"}}},
}

# Batch 1: A x2, B x1 -> first incremental run reads the full snapshot, creates dim.
spark.createDataFrame([{"k": "A"}, {"k": "A"}, {"k": "B"}]).write.format("delta").mode("append").saveAsTable("itest.src")
r1 = handler(event, None)

# Batch 2: A x1, C x1 (one commit) -> CDF read of only the new rows, additive merge.
spark.createDataFrame([{"k": "A"}, {"k": "C"}]).write.format("delta").mode("append").saveAsTable("itest.src")
r2 = handler(event, None)

rows = {row["k"]: int(row["cnt"]) for row in spark.table("itest.dim").collect()}
print("RESULT_LINE:" + json.dumps({"r1": r1.get("status"), "r2": r2.get("status"), "rows": rows}))
`
	r := runRawDriver(t, script)
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
	script := `
import json, os, sys
sys.path.insert(0, "/var/task")
os.environ["CLAVESA_WAREHOUSE"] = "/work/wh"
os.environ["CLAVESA_WATERMARKS"] = "/work/wm"
os.environ["CLAVESA_LANGUAGE"] = "sql"
os.environ["CLAVESA_LOGIC_S3_PATH"] = "/work/logic.txt"
os.environ["CLAVESA_CATALOG"] = "itest_cat"
os.environ["CLAVESA_SCHEMA"] = "itest"
os.environ["CLAVESA_PIPELINE"] = "itest"
os.environ["CLAVESA_NODE"] = "itest"
os.makedirs("/work/wm", exist_ok=True)

from runner import _spark, handler, _run_operation

spark = _spark()
spark.sql("CREATE DATABASE IF NOT EXISTS itest")

with open("/work/logic.txt", "w") as f:
    f.write("SELECT k, COUNT(*) AS cnt FROM src2 GROUP BY k")

event = {
    "inputs": {"src2": {"kind": "delta_table_cdf", "table": "itest.src2"}},
    "outputs": {"default": {"kind": "delta_table", "table_id": "itest.dim2",
        "mode": "merge", "merge_keys": ["k"], "merge_update": {"cnt": "additive"}}},
}

# Two append commits, then a first consumer run reads the full snapshot.
spark.createDataFrame([{"k": "A"}]).write.format("delta").mode("append").saveAsTable("itest.src2")
spark.createDataFrame([{"k": "B"}]).write.format("delta").mode("append").saveAsTable("itest.src2")
r1 = handler(event, None)
before = {row["k"]: int(row["cnt"]) for row in spark.table("itest.dim2").collect()}

# OPTIMIZE the source: a dataChange=false commit that bumps the version.
opt = _run_operation({"_operation": "optimize", "table": "itest.src2"})

# Re-run the consumer: its CDF range now spans only the OPTIMIZE commit.
r2 = handler(event, None)
after = {row["k"]: int(row["cnt"]) for row in spark.table("itest.dim2").collect()}

print("RESULT_LINE:" + json.dumps({"r1": r1.get("status"), "opt": opt.get("status"),
    "r2": r2.get("status"), "before": before, "after": after}))
`
	r := runRawDriver(t, script)
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
