package bundle

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const dailySales = "../../testdata/bundles/daily_sales"

func noEnv(string) (string, bool) { return "", false }

// golden compares got with testdata/bundles/<bundle>/expected/<name>;
// UPDATE_GOLDEN=1 rewrites the file.
func golden(t *testing.T, bundleDir, name, got string) {
	t.Helper()
	p := filepath.Join(bundleDir, "expected", name)
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.WriteFile(p, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("%v (run with UPDATE_GOLDEN=1 to create it)", err)
	}
	if string(want) != got {
		t.Errorf("%s differs from the golden file (UPDATE_GOLDEN=1 rewrites it).\ngot:\n%s\nwant:\n%s", name, got, want)
	}
}

func planOf(t *testing.T, dir, target string) *Report {
	t.Helper()
	b, err := Resolve(dir, Options{Target: target, Env: noEnv})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Plan(b)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPlanGolden(t *testing.T) {
	for _, target := range []string{"aws", "aws-local"} {
		t.Run(target, func(t *testing.T) {
			golden(t, dailySales, "plan-"+target+".txt", planOf(t, dailySales, target).Text())
		})
	}
}

func TestPlanNotAClavesaTarget(t *testing.T) {
	b, err := Resolve(dailySales, Options{Target: "prod", Env: noEnv})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Plan(b); err == nil || !strings.Contains(err.Error(), "deploys to Databricks only") {
		t.Fatalf("want a not-a-clavesa-target error, got %v", err)
	}
}

// writeBundle writes a one-file bundle into a temp folder.
func writeBundle(t *testing.T, yml string, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "databricks.yml"), []byte(yml), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		p := filepath.Join(dir, name)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const tinyBundle = `
bundle: { name: tiny }
variables:
  table: { default: main.s.t }
  clavesa: { type: complex, default: {} }
resources:
  jobs:
    j:
      tasks:
        - task_key: a
          spark_python_task: { python_file: a.py, parameters: ["${var.table}"] }
          environment_key: e
        - task_key: b
          depends_on: [{ task_key: a }]
          %s
      environments: [ { environment_key: e, spec: { client: "2" } } ]
targets:
  local:
    default: true
    variables:
      clavesa: %s
`

func tinyPlan(t *testing.T, taskB, clavesa string) *Report {
	t.Helper()
	dir := writeBundle(t, fmt.Sprintf(tinyBundle, taskB, clavesa), map[string]string{"a.py": "print(1)", "b.sql": "SELECT 1"})
	return planOf(t, dir, "")
}

func messages(r *Report) string {
	var sb strings.Builder
	for _, f := range r.Findings {
		sb.WriteString(string(f.Severity) + " " + f.Message + "\n")
	}
	for _, j := range r.Jobs {
		for _, f := range j.Findings {
			sb.WriteString(string(f.Severity) + " " + f.Message + "\n")
		}
		for _, tp := range j.Tasks {
			for _, f := range tp.Findings {
				sb.WriteString(string(f.Severity) + " " + tp.Key + ": " + f.Message + "\n")
			}
		}
	}
	return sb.String()
}

func TestPlanTaskMapping(t *testing.T) {
	cases := []struct {
		name, taskB, clavesa, want string
		ready                      bool
	}{
		{"sql file", `sql_task: { file: { path: b.sql }, warehouse_id: w }`, `{ catalog: glue }`, "", true},
		{"sql query", `sql_task: { query: { query_id: q1 }, warehouse_id: w }`, `{ catalog: glue }`, "refuse b: sql_task query", false},
		{"pipeline without host", `pipeline_task: { pipeline_id: p1 }`, `{ catalog: glue }`, "refuse b: Lakeflow pipeline", false},
		{"jar", `spark_jar_task: { main_class_name: x.Main }`, `{ catalog: glue }`, "needs EMR Serverless", false},
		{"git source", `notebook_task: { notebook_path: nb, source: GIT }`, `{ catalog: glue }`, "source: GIT", false},
		{"run_job outside the bundle, no host", `run_job_task: { job_id: 123 }`, `{ catalog: glue }`, "needs a target with a Databricks host", false},
		{"for_each concurrency", `for_each_task: { inputs: "[1,2]", concurrency: 4, task: { task_key: inner, environment_key: e, spark_python_task: { python_file: a.py } } }`, `{ catalog: glue }`, "warn b: for_each_task concurrency 4", true},
		{"for_each of a pipeline", `for_each_task: { inputs: "[1]", task: { task_key: inner, pipeline_task: { pipeline_id: p } } }`, `{ catalog: glue }`, "refuse b: for_each_task runs a pipeline task", false},
		{"unknown clavesa key", `condition_task: { op: EQUAL_TO, left: "1", right: "1" }`, `{ catalog: glue, colour: red }`, `unknown key "colour"`, false},
		{"unknown task in place", `condition_task: { op: EQUAL_TO, left: "1", right: "1" }`, `{ catalog: glue, place: { nope: clavesa } }`, `place.nope: no task "nope"`, false},
		{"unity without host", `condition_task: { op: EQUAL_TO, left: "1", right: "1" }`, `{ catalog: unity }`, "catalog: unity needs a Databricks workspace host", false},
		{"bad table name", `condition_task: { op: EQUAL_TO, left: "1", right: "1" }`, `{ catalog: glue, tables: { a: { writes: [s.t] } } }`, "not a three-level name", false},
		{"retry forever", `max_retries: -1
          sql_task: { file: { path: b.sql }, warehouse_id: w }`, `{ catalog: glue }`, "warn b: max_retries -1", true},
		{"replaces_job on glue", `condition_task: { op: EQUAL_TO, left: "1", right: "1" }`, `{ catalog: glue, replaces_job: 5 }`, "replaces_job needs catalog: unity", false},
		{"bad compute kind", `condition_task: { op: EQUAL_TO, left: "1", right: "1" }`, `{ catalog: glue, compute: { a: { kind: emr } } }`, "compute.a.kind must be lambda or fargate", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := tinyPlan(t, c.taskB, c.clavesa)
			got := messages(r)
			if c.want != "" && !strings.Contains(got, c.want) {
				t.Errorf("want %q in:\n%s", c.want, got)
			}
			if c.want == "" && got != "" {
				t.Errorf("want no findings, got:\n%s", got)
			}
			if r.Ready != c.ready {
				t.Errorf("ready = %v, want %v\n%s", r.Ready, c.ready, r.Text())
			}
		})
	}
}

func TestPlanSegmentsWithHost(t *testing.T) {
	r := planOf(t, dailySales, "aws")
	j := r.Jobs[0]
	if j.Key != "daily_sales" || j.Segments != 2 {
		t.Fatalf("daily_sales on aws: want 2 segments, got %d", j.Segments)
	}
	for _, tp := range j.Tasks {
		if tp.Key == "refresh_gold" && tp.Place != "databricks" {
			t.Errorf("refresh_gold should stay on Databricks, got %s", tp.Place)
		}
	}
}

// FixDailySales applies the one edit of the ADR-031 acceptance test (step
// 2) to a copy of the example bundle: the %pip cell of publish goes, and
// its package becomes a pypi library of the task. It returns the copy.
func FixDailySales(t *testing.T) string {
	t.Helper()
	dir := copyBundle(t, dailySales, "https://dbc-00000000-0000.cloud.databricks.com")
	nb := filepath.Join(dir, "notebooks", "publish.py")
	b, err := os.ReadFile(nb)
	if err != nil {
		t.Fatal(err)
	}
	src := strings.Replace(string(b), "# MAGIC %pip install tabulate==0.9.0\n\n# COMMAND ----------\n\n", "", 1)
	if src == string(b) {
		t.Fatal("publish.py no longer has the pip cell the fix removes")
	}
	_ = os.WriteFile(nb, []byte(src), 0o644)
	job := filepath.Join(dir, "resources", "daily_sales.job.yml")
	b, _ = os.ReadFile(job)
	y := strings.Replace(string(b), "          notebook_task:\n            notebook_path: ../notebooks/publish.py\n",
		"          notebook_task:\n            notebook_path: ../notebooks/publish.py\n          libraries:\n            - pypi:\n                package: tabulate==0.9.0\n", 1)
	if y == string(b) {
		t.Fatal("daily_sales.job.yml no longer has the publish task the fix edits")
	}
	_ = os.WriteFile(job, []byte(y), 0o644)
	return dir
}

func TestAcceptanceFixMakesThePlanReady(t *testing.T) {
	dir := FixDailySales(t)
	for _, target := range []string{"aws", "aws-local"} {
		r := planOf(t, dir, target)
		if !r.Ready || !strings.Contains(r.Text(), "Ready: yes.") {
			t.Errorf("%s after the fix:\n%s", target, r.Text())
		}
		if !strings.Contains(strings.Join(r.Libraries, ","), "tabulate==0.9.0") {
			t.Errorf("%s: tabulate should be a runner requirement: %v", target, r.Libraries)
		}
	}
}

func TestSegmentsOfTheMixedExample(t *testing.T) {
	r := planOf(t, dailySales, "aws")
	j := r.Job("daily_sales")
	var jp JobPlan
	for _, x := range r.Jobs {
		if x.Key == "daily_sales" {
			jp = x
		}
	}
	segs := Segments(j, jp.Tasks)
	got := ""
	for _, s := range segs {
		got += fmt.Sprintf("%d %s %v; ", s.ID, s.Place, s.Tasks)
	}
	if want := "0 clavesa [ingest clean enrich check publish weekly]; 1 databricks [refresh_gold]; "; got != want {
		t.Errorf("segments:\n got %s\nwant %s", got, want)
	}
	// A clavesa task after the Databricks one makes a third segment.
	b := writeBundle(t, `
bundle: { name: m }
variables: { clavesa: { type: complex, default: {} } }
resources:
  jobs:
    j:
      tasks:
        - { task_key: a, condition_task: { op: EQUAL_TO, left: "1", right: "1" } }
        - { task_key: p, depends_on: [{ task_key: a }], pipeline_task: { pipeline_id: "1" } }
        - { task_key: c, depends_on: [{ task_key: p }], condition_task: { op: EQUAL_TO, left: "1", right: "1" } }
targets:
  t:
    default: true
    workspace: { host: "https://dbc-1.cloud.databricks.com" }
    variables: { clavesa: { catalog: unity } }
`, nil)
	r = planOf(t, b, "")
	jp = r.Jobs[0]
	if n := len(Segments(r.Job("j"), jp.Tasks)); n != 3 || jp.Segments != 3 {
		t.Errorf("a -> p (databricks) -> c: want 3 segments, got %d", n)
	}
}

func TestPipelineNameHasTheTarget(t *testing.T) {
	if got := PipelineName("daily_sales", "aws-local", "weekly_report"); got != "daily_sales_aws_local_weekly_report" {
		t.Errorf("got %s", got)
	}
	a, b := planOf(t, dailySales, "aws"), planOf(t, dailySales, "aws-local")
	if a.Jobs[0].Pipeline == b.Jobs[0].Pipeline {
		t.Errorf("two targets share pipeline %s", a.Jobs[0].Pipeline)
	}
}
