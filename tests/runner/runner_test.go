//go:build integration

// Package runner_test exercises the Clavesa transform runner end-to-end via
// the Docker image.
//
// This file holds the COLD-BOOT tests: each one pays a full container +
// Spark JVM start, which is real coverage for the stripped spark-class
// launcher, entrypoint.sh, the Lambda handler env contract, JVM heap
// sizing, and fresh-metastore Delta init — preview mode (the path the UI
// hits), the production handler (the path Lambda hits) over local-FS
// input/output, and the pipeline-bundle session-lifecycle tests (which
// count session builds in a fresh process, so they can't share a session).
//
// The data-semantics tests (promote, schema evolution, incremental CDF,
// OPTIMIZE, merge bounds) live in warm_test.go on a single long-lived
// container — see the warmRunner harness there.
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
	// Boot the shared warm-session container (warm_test.go) in the
	// background: the cold tests in this file run first, so the ~25s Spark
	// boot overlaps them; the first warm test blocks on readiness.
	warm = startWarmRunner()
	code := m.Run()
	warm.stop()
	os.Exit(code)
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

// bundleResult is the aggregated pipeline_handler result — the JSON object on
// stdout carrying a top-level "status" key (the only structured stdout line
// now that per-node progress goes to the warehouse `_progress` tree).
type bundleResult struct {
	Status     string `json:"status"`
	FailedNode string `json:"failed_node"`
	Transforms []struct {
		Node   string `json:"node"`
		Status string `json:"status"`
	} `json:"transforms"`
}

// parseBundle scans the runner stdout for the final aggregate result line.
// The runner no longer emits per-node `_event` progress lines (progress now
// goes to the warehouse `_progress/<run>/<node>.json` tree, not stdout), so
// the per-node succeeded set is derived from the aggregate's transforms[]
// (status == "ok"). Returns the succeeded node set and the result.
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
		// The aggregate result line is the JSON object carrying a "status" key.
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
	for _, tr := range res.Transforms {
		if tr.Status == "ok" {
			succeeded[tr.Node] = true
		}
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
		t.Errorf("node a should have reported ok; stdout:\n%s", stdout)
	}
	if !succeeded["b"] {
		t.Errorf("node b should have reported ok after the session was killed (self-heal failed); stdout:\n%s", stdout)
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

