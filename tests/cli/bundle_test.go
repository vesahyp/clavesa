//go:build integration

package integration

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// queryRows runs `clavesa query --json` and returns the rows as text.
func queryRows(t *testing.T, ws, sql string) [][]string {
	t.Helper()
	out := run(t, "query", "--workspace", ws, "--json", sql)
	var res struct {
		Rows [][]string `json:"rows"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("query --json: %v\n%s", err, out)
	}
	return res.Rows
}

// runFails runs the binary, expects a non-zero exit and returns stderr.
func runFails(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command(binPath, args...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("clavesa %s: want a failure, got success:\n%s", strings.Join(args, " "), out)
	}
	return string(out)
}

// TestBundleRunScriptMode is the ADR-031 slice 3 test: a bundle job with a
// spark_python_task and a sql_task file runs locally in catalog: glue, the
// tables land in Glue database main__sales, a --param changes the run, and
// the generated pipeline refuses edits.
func TestBundleRunScriptMode(t *testing.T) {
	ws := newWorkspace(t)
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	bundleDir := filepath.Join(repoRoot(), "testdata", "bundles", "orders_sql")

	out := run(t, "bundle", "run", "--bundle", bundleDir, "--workspace", ws, "--compute", "local")
	for _, want := range []string{"Pipeline: orders_sql_orders", "load", "clean"} {
		if !strings.Contains(out, want) {
			t.Fatalf("bundle run output lacks %q:\n%s", want, out)
		}
	}
	if strings.Count(out, " ok") < 2 {
		t.Fatalf("both tasks should be ok:\n%s", out)
	}

	rows := queryRows(t, ws, "SELECT order_id, country, CAST(amount AS STRING) AS amount FROM main__sales.orders_clean ORDER BY order_id")
	got := []string{}
	for _, r := range rows {
		got = append(got, strings.Join(r, " "))
	}
	want := "1004 FI 40.00|1005 EE 19.90|1007 FI 5.25|1008 DE 99.99"
	if strings.Join(got, "|") != want {
		t.Fatalf("orders_clean after the 2026-11-13 run:\n got %s\nwant %s", strings.Join(got, "|"), want)
	}

	// The other day, through --param: MERGE adds its three orders.
	run(t, "bundle", "run", "--bundle", bundleDir, "--workspace", ws, "--param", "run_date=2026-11-12")
	if n := len(queryRows(t, ws, "SELECT * FROM main__sales.orders_clean")); n != 7 {
		t.Fatalf("after run_date=2026-11-12: want 7 rows, got %d", n)
	}

	// The generated pipeline is read-only.
	msg := runFails(t, "node", "edit", filepath.Join(ws, "orders_sql_orders"), "clean", "--set", "enabled=false", "--workspace", ws)
	if !strings.Contains(msg, "generated from job orders of the Databricks bundle orders_sql") {
		t.Fatalf("node edit on a bundle pipeline: %s", msg)
	}
	// An unknown parameter is refused before anything runs.
	msg = runFails(t, "bundle", "run", "--bundle", bundleDir, "--workspace", ws, "--param", "nope=1")
	if !strings.Contains(msg, "job orders has no parameter nope") {
		t.Fatalf("unknown --param: %s", msg)
	}
}

// copyDir copies a testdata folder into a temp folder.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

// TestBundleRunNotebooks is the ADR-031 slice 4 test: notebook tasks in the
// three formats (.py source with %run and %sql, .sql, .ipynb) run locally,
// and the example bundle's ingest notebook (widgets, a secret, %sql with a
// :run_date parameter, a task value) feeds its clean task.
func TestBundleRunNotebooks(t *testing.T) {
	ws := newWorkspace(t)
	run(t, "workspace", "init", "test-ws", "--workspace", ws)

	nb := filepath.Join(repoRoot(), "testdata", "bundles", "notebooks")
	run(t, "bundle", "run", "--bundle", nb, "--workspace", ws)
	rows := queryRows(t, ws, "SELECT n, total FROM main__nb.summary")
	if len(rows) != 1 || strings.Join(rows[0], " ") != "3 50" {
		t.Fatalf("summary: want [3 50], got %v", rows)
	}

	// The example bundle, ingest and clean only: a copy with a target that
	// disables the tasks later slices bring.
	ds := copyDir(t, filepath.Join(repoRoot(), "testdata", "bundles", "daily_sales"))
	yml, _ := os.ReadFile(filepath.Join(ds, "databricks.yml"))
	extra := `
  notebooks-only:
    variables:
      source_path: ${workspace.file_path}/data/orders
      clavesa:
        catalog: glue
    resources:
      jobs:
        daily_sales:
          tasks:
            - { task_key: enrich, disabled: true }
            - { task_key: check, disabled: true }
            - { task_key: publish, disabled: true }
            - { task_key: refresh_gold, disabled: true }
            - { task_key: weekly, disabled: true }
`
	if err := os.WriteFile(filepath.Join(ds, "databricks.yml"), append(yml, []byte(extra)...), 0o644); err != nil {
		t.Fatal(err)
	}
	// Without the secret the ingest notebook fails, and says how to set it.
	msg := runFails(t, "bundle", "run", "--bundle", ds, "-t", "notebooks-only", "--job", "daily_sales", "--workspace", ws, "--json")
	if !strings.Contains(msg, "CLAVESA_DBX_SECRET__sales__api_key") {
		t.Fatalf("missing secret: %s", msg)
	}
	cmd := exec.Command(binPath, "bundle", "run", "--bundle", ds, "-t", "notebooks-only", "--job", "daily_sales", "--workspace", ws)
	cmd.Env = append(os.Environ(), "CLAVESA_DBX_SECRET__sales__api_key=test-key")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bundle run daily_sales: %v\n%s", err, out)
	}
	rows = queryRows(t, ws, "SELECT order_id, country, CAST(amount AS STRING) FROM main__sales.orders_clean ORDER BY order_id")
	got := []string{}
	for _, r := range rows {
		got = append(got, strings.Join(r, " "))
	}
	if want := "1004 FI 40.00|1005 EE 19.90|1007 FI 5.25|1008 DE 99.99"; strings.Join(got, "|") != want {
		t.Fatalf("orders_clean from ingest + clean:\n got %s\nwant %s", strings.Join(got, "|"), want)
	}
}

// TestBundleRunControlFlow is the ADR-031 slice 5 test: a condition with
// both outcomes, for_each over a job parameter, a retry, a timeout and a
// run_if ALL_DONE task, run locally. The job fails (the timed-out task),
// and every other task ends as Databricks would end it.
func TestBundleRunControlFlow(t *testing.T) {
	ws := newWorkspace(t)
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	b := filepath.Join(repoRoot(), "testdata", "bundles", "control_flow")
	cmd := exec.Command(binPath, "bundle", "run", "--bundle", b, "--workspace", ws, "--json")
	out, err := cmd.Output()
	if err == nil {
		t.Fatalf("the job has a timed-out task and must fail:\n%s", out)
	}
	var res struct {
		Nodes []struct {
			NodeID string `json:"node_id"`
			Status string `json:"status"`
			Note   string `json:"note"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("bundle run --json: %v\n%s", err, out)
	}
	got := map[string]string{}
	for _, n := range res.Nodes {
		got[n.NodeID] = n.Status + " " + n.Note
	}
	for task, want := range map[string]string{
		"count":   "ok",
		"check":   "ok condition true",
		"each":    "ok",
		"never":   "skipped excluded",
		"flaky":   "ok",
		"slow":    "failed task slow ran past its timeout_seconds (5s)",
		"cleanup": "ok",
	} {
		if !strings.HasPrefix(got[task], want) {
			t.Errorf("task %s: got %q, want %q...", task, got[task], want)
		}
	}
	rows := queryRows(t, ws, "SELECT concat_ws(',', sort_array(collect_list(item))) FROM main__cf.items")
	if len(rows) != 1 || rows[0][0] != "a,b,c" {
		t.Errorf("for_each items: %v", rows)
	}
	tables := queryRows(t, ws, "SHOW TABLES IN main__cf")
	var names []string
	for _, r := range tables {
		names = append(names, r[1])
	}
	if strings.Join(names, ",") != "cleanup,items" {
		t.Errorf("tables: want cleanup,items (never is excluded), got %v", names)
	}
}

// exampleTables are the tables of the example bundle's acceptance test and
// the query that dumps each one in a fixed order.
var exampleTables = []struct{ name, sql string }{
	{"main.sales.orders_clean", "SELECT order_id, CAST(order_date AS STRING), country, CAST(amount AS STRING), customer FROM main__sales.orders_clean ORDER BY order_id"},
	{"main.sales.orders_enriched", "SELECT order_id, country, country_name, CAST(amount AS STRING), CAST(home_market AS STRING) FROM main__sales.orders_enriched ORDER BY order_id"},
	{"main.sales.daily_revenue", "SELECT country, country_name, orders, CAST(revenue AS STRING), CAST(run_date AS STRING) FROM main__sales.daily_revenue ORDER BY country"},
	{"main.sales.weekly_revenue", "SELECT country, CAST(revenue AS STRING), orders FROM main__sales.weekly_revenue ORDER BY country"},
}

// TestBundleRunWholeExample is ADR-031 slice 6 and the acceptance test of
// section 12, step 3: the example bundle with the step 2 fix (%pip moved to
// libraries) runs on aws-local with --compute local, wheel and all: ingest
// (notebook) -> clean (SQL) -> enrich (wheel built from the bundle's
// artifact, pycountry from the runner requirements) -> check (condition on
// a task value) -> publish (notebook with %run and tabulate) -> weekly
// (run_job_task: job weekly_report, in the same runner). refresh_gold is
// disabled in that target. The tables equal
// testdata/bundles/daily_sales/expected/tables-aws-local.txt.
func TestBundleRunWholeExample(t *testing.T) {
	ws := newWorkspace(t)
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	ds := copyDir(t, filepath.Join(repoRoot(), "testdata", "bundles", "daily_sales"))
	// Step 2 of the acceptance test: the one edit.
	nb := filepath.Join(ds, "notebooks", "publish.py")
	src, _ := os.ReadFile(nb)
	_ = os.WriteFile(nb, []byte(strings.Replace(string(src), "# MAGIC %pip install tabulate==0.9.0\n\n# COMMAND ----------\n\n", "", 1)), 0o644)
	job := filepath.Join(ds, "resources", "daily_sales.job.yml")
	src, _ = os.ReadFile(job)
	_ = os.WriteFile(job, []byte(strings.Replace(string(src),
		"            notebook_path: ../notebooks/publish.py\n",
		"            notebook_path: ../notebooks/publish.py\n          libraries:\n            - pypi:\n                package: tabulate==0.9.0\n", 1)), 0o644)
	if out := run(t, "bundle", "plan", "--bundle", ds, "-t", "aws-local"); !strings.Contains(out, "Ready: yes.") {
		t.Fatalf("plan after the fix:\n%s", out)
	}

	cmd := exec.Command(binPath, "bundle", "run", "--bundle", ds, "-t", "aws-local", "--job", "daily_sales", "--compute", "local", "--workspace", ws)
	cmd.Env = append(os.Environ(), "CLAVESA_DBX_SECRET__sales__api_key=test-key")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bundle run: %v\n%s", err, out)
	}
	for _, task := range []string{"ingest", "check", "clean", "enrich", "publish", "weekly"} {
		if !strings.Contains(string(out), task) {
			t.Errorf("run output lacks task %s:\n%s", task, out)
		}
	}
	if reqs := run(t, "runner", "requirements", "list", "--workspace", ws); !strings.Contains(reqs, "pycountry==24.6.1") || !strings.Contains(reqs, "tabulate==0.9.0") {
		t.Errorf("runner requirements should hold the bundle's libraries:\n%s", reqs)
	}

	var sb strings.Builder
	for _, tb := range exampleTables {
		sb.WriteString("## " + tb.name + "\n")
		for _, r := range queryRows(t, ws, tb.sql) {
			sb.WriteString(strings.Join(r, "\t") + "\n")
		}
	}
	golden := filepath.Join(repoRoot(), "testdata", "bundles", "daily_sales", "expected", "tables-aws-local.txt")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		_ = os.WriteFile(golden, []byte(sb.String()), 0o644)
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if sb.String() != string(want) {
		t.Errorf("tables differ from %s:\ngot:\n%s\nwant:\n%s", golden, sb.String(), want)
	}
}

// fixedExample copies the example bundle with the acceptance step 2 edit.
func fixedExample(t *testing.T) string {
	t.Helper()
	ds := copyDir(t, filepath.Join(repoRoot(), "testdata", "bundles", "daily_sales"))
	nb := filepath.Join(ds, "notebooks", "publish.py")
	src, _ := os.ReadFile(nb)
	_ = os.WriteFile(nb, []byte(strings.Replace(string(src), "# MAGIC %pip install tabulate==0.9.0\n\n# COMMAND ----------\n\n", "", 1)), 0o644)
	job := filepath.Join(ds, "resources", "daily_sales.job.yml")
	src, _ = os.ReadFile(job)
	_ = os.WriteFile(job, []byte(strings.Replace(string(src),
		"            notebook_path: ../notebooks/publish.py\n",
		"            notebook_path: ../notebooks/publish.py\n          libraries:\n            - pypi:\n                package: tabulate==0.9.0\n", 1)), 0o644)
	return ds
}

// TestBundleDeployTerraform is the ADR-031 slice 7 test: bundle deploy
// --emit-only writes one pipeline per job of the example's aws-local
// target, with the job's schedule (EventBridge Scheduler, Europe/Helsinki),
// file_arrival trigger, failure notifications, health alarm, Fargate size
// from the clusters and Glue access, and terraform validate passes on
// both. No apply: the live deploy is the last slice.
func TestBundleDeployTerraform(t *testing.T) {
	if _, err := exec.LookPath("terraform"); err != nil {
		t.Skip("terraform not on PATH")
	}
	ws := newWorkspace(t)
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	ds := fixedExample(t)
	out := run(t, "bundle", "deploy", "--bundle", ds, "-t", "aws-local", "--workspace", ws, "--emit-only")
	if !strings.Contains(out, "Job daily_sales -> pipeline daily_sales_daily_sales") || !strings.Contains(out, "Job weekly_report -> pipeline daily_sales_weekly_report") {
		t.Fatalf("deploy --emit-only:\n%s", out)
	}
	checks := map[string][]string{
		"daily_sales_daily_sales": {
			`schedule_expression = "cron(30 4 * * ? *)"`,
			`schedule_expression_timezone = "Europe/Helsinki"`,
			`state               = "ENABLED"`,
			`endpoint  = "sales-data@example.com"`,
			`resource "aws_cloudwatch_metric_alarm" "bundle_duration"`,
			`"arn:aws:glue:*:*:database/main__sales"`,
			`language   = "task"`,
			`resource "aws_ecs_task_definition"`,
		},
		"daily_sales_weekly_report": {
			`bucket = { name = ["example-sales-landing"] }, object = { key = [{ prefix = "weekly/" }] }`,
		},
	}
	for dir, wants := range checks {
		p := filepath.Join(ws, dir)
		tf, err := os.ReadFile(filepath.Join(p, "orchestration.tf"))
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range wants {
			if !strings.Contains(string(tf), w) {
				t.Errorf("%s/orchestration.tf lacks %q", dir, w)
			}
		}
		for _, args := range [][]string{{"init", "-backend=false", "-input=false", "-no-color"}, {"validate", "-no-color"}} {
			c := exec.Command("terraform", args...)
			c.Dir = p
			if out, err := c.CombinedOutput(); err != nil {
				t.Fatalf("terraform %s in %s: %v\n%s", args[0], dir, err, out)
			}
		}
	}
	vars, _ := os.ReadFile(filepath.Join(ws, "daily_sales_daily_sales", "bundle.auto.tfvars"))
	if !strings.Contains(string(vars), "fargate_cpu       = 8") || !strings.Contains(string(vars), "fargate_memory_gb = 60") {
		t.Errorf("Fargate size from the clusters (photon: 1 driver + 2 workers r6id.xlarge = 12 vCPU / 96 GB, rounded down to 8 / 60):\n%s", vars)
	}
}

// TestBundleDeployMixed is the ADR-031 slice 9 test: the example's aws
// target is mixed (refresh_gold is a Lakeflow pipeline on Databricks).
// bundle deploy finds the deployed Databricks job with databricks bundle
// summary (a fake CLI here), and the state machine runs the clavesa
// segment on Lambda, then run-now with only [refresh_gold] and a
// runs/get poll loop; terraform validate passes.
func TestBundleDeployMixed(t *testing.T) {
	if _, err := exec.LookPath("terraform"); err != nil {
		t.Skip("terraform not on PATH")
	}
	ws := newWorkspace(t)
	run(t, "workspace", "init", "test-ws", "--workspace", ws)
	ds := fixedExample(t)
	bin := t.TempDir()
	fake := "#!/bin/sh\ncase \"$*\" in\n  *\"bundle summary\"*) echo '{\"resources\":{\"jobs\":{\"daily_sales\":{\"id\":\"923072819710352\"}}}}' ;;\n  *) exit 1 ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "databricks"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binPath, "bundle", "deploy", "--bundle", ds, "-t", "aws", "--job", "daily_sales", "--workspace", ws, "--emit-only")
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bundle deploy --emit-only -t aws: %v\n%s", err, out)
	}
	dir := filepath.Join(ws, "daily_sales_daily_sales")
	tf, _ := os.ReadFile(filepath.Join(dir, "orchestration.tf"))
	for _, want := range []string{`_operation = "dbx_run_now"`, `_operation = "dbx_run_get"`, `job_id     = 923072819710352`,
		`only       = ["refresh_gold"]`, `Next = "Seg1Start"`, `Type    = "Wait"`, `"_bundle_states.$" = "$.states"`} {
		if !strings.Contains(string(tf), want) {
			t.Errorf("orchestration.tf lacks %q", want)
		}
	}
	for _, args := range [][]string{{"init", "-backend=false", "-input=false", "-no-color"}, {"validate", "-no-color"}} {
		c := exec.Command("terraform", args...)
		c.Dir = dir
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("terraform %s: %v\n%s", args[0], err, out)
		}
	}
	// Without the deployed Databricks job, deploy stops and says what to do.
	bad := "#!/bin/sh\necho 'Error: not deployed' >&2\nexit 1\n"
	_ = os.WriteFile(filepath.Join(bin, "databricks"), []byte(bad), 0o755)
	cmd = exec.Command(binPath, "bundle", "deploy", "--bundle", ds, "-t", "aws", "--job", "daily_sales", "--workspace", ws, "--emit-only")
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "databricks bundle deploy -t aws") {
		t.Errorf("deploy without the Databricks job: %v\n%s", err, out)
	}
}
