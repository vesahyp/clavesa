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
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("docker run failed: %v\nstdout: %s\nstderr: %s", err, out, stderr.String())
	}
	return string(out)
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
