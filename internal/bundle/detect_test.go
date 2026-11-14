package bundle

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/pricing"
)

// TestDatabricksOnlyList checks every entry of databricks_only.txt: its
// example is found, and the same name used as a column is not.
func TestDatabricksOnlyList(t *testing.T) {
	if len(databricksOnly) < 15 {
		t.Fatalf("the list has %d entries; it did not load", len(databricksOnly))
	}
	for _, d := range databricksOnly {
		t.Run(d.Pattern, func(t *testing.T) {
			if fs := sqlFindings(d.Example, "x.sql", 1); len(fs) == 0 {
				t.Errorf("example not found: %s", d.Example)
			}
			if d.Kind == "function" {
				name := strings.TrimSuffix(d.Pattern, "*") + "x"
				benign := fmt.Sprintf("SELECT %s, '%s(1)' AS s FROM t -- %s(2)", name, name, name)
				if fs := sqlFindings(benign, "x.sql", 1); len(fs) != 0 {
					t.Errorf("found in a column, string or comment: %v", fs)
				}
			}
		})
	}
}

func TestSQLTables(t *testing.T) {
	sql := `
USE CATALOG main;
-- FROM not_a_table
WITH recent AS (SELECT * FROM sales.orders WHERE d > '2026-01-01')
INSERT INTO sales.daily
SELECT o.*, extract(YEAR FROM o.d) AS y FROM recent o JOIN other.c.customers c ON o.c = c.id;
CREATE OR REPLACE TEMP VIEW v AS SELECT 1 FROM range(10);
MERGE INTO main.sales.clean t USING (SELECT * FROM v) s ON t.id = s.id WHEN MATCHED THEN DELETE;
SELECT * FROM read_files('s3://x');
SELECT 1 FROM ` + "`main`.`sales`.`q`" + `;
SELECT a IS DISTINCT FROM b FROM main.sales.z;
`
	reads, writes, un := sqlTables(sql, "", "")
	got := fmt.Sprint(names(reads), names(writes), names(un))
	want := "[main.sales.orders other.c.customers main.sales.q main.sales.z] [main.sales.daily main.sales.clean] []"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	_, _, un = sqlTables("SELECT * FROM s.t", "", "")
	if len(un) != 1 {
		t.Errorf("two-level name without a catalog must be unresolved: %v", un)
	}
}

func TestIdentifierAndQuotedMustache(t *testing.T) {
	src := substituteMustache("USE CATALOG IDENTIFIER({{c}});\nUSE SCHEMA IDENTIFIER({{s}});\nINSERT INTO t SELECT * FROM IDENTIFIER('x.y.z')", StringMap{"c": "main", "s": "sales"})
	if !strings.Contains(src, "IDENTIFIER('main')") {
		t.Fatalf("mustache is a quoted literal: %s", src)
	}
	r, w, un := sqlTables(src, "", "")
	if fmt.Sprint(names(r), names(w), len(un)) != "[x.y.z] [main.sales.t] 0" {
		t.Errorf("got %v %v %v", names(r), names(w), un)
	}
}

func TestSQLStatements(t *testing.T) {
	got := SQLStatements("SELECT ';'; -- a;b\nSELECT 2;\n\n;")
	if len(got) != 2 || got[0] != "SELECT ';'" || !strings.HasPrefix(got[1], "-- a;b") {
		t.Errorf("%q", got)
	}
}

func TestPythonScan(t *testing.T) {
	src := `import dlt
# spark.table("commented.out.t")
df = spark.table("main.s.a")
spark.read.table(f"{cat}.s.b")
df.write.mode("overwrite").saveAsTable(name="main.s.c")
DeltaTable.forName(spark, "main.s.d").alias("t")
spark.sql("""
  INSERT INTO main.s.e SELECT * FROM main.s.a
""")
k = dbutils.secrets.get(scope="sc", key="k1")
p = "dbfs:/mnt/x"
s = "/dbfs/tmp"
dbutils.notebook.run("./x", 60)
x = "not cloudfiles"
spark.readStream.format("cloudFiles")
`
	ts := &taskScan{}
	scanPythonCode(src, "a.py", 1, "", "", ts)
	if got := fmt.Sprint(names(ts.reads), names(ts.writes)); got != "[main.s.a] [main.s.c main.s.d main.s.e]" {
		t.Errorf("tables: %s", got)
	}
	if len(ts.unseen) != 1 || ts.unseen[0].Line != 4 {
		t.Errorf("unseen: %+v", ts.unseen)
	}
	if fmt.Sprint(ts.secrets) != "[sc/k1]" {
		t.Errorf("secrets: %v", ts.secrets)
	}
	ts.sortFindings()
	var lines []int
	for _, f := range ts.findings {
		lines = append(lines, f.Line)
	}
	if fmt.Sprint(lines) != "[1 11 12 13 15]" {
		t.Errorf("finding lines: %v\n%+v", lines, ts.findings)
	}
}

func TestParseNotebookSource(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		_ = os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644)
	}
	write("nb.py", "# Databricks notebook source\n# MAGIC %md\n# MAGIC # Title\n\n# COMMAND ----------\n\nx = 1\n\n# COMMAND ----------\n\n# MAGIC %sql\n# MAGIC SELECT 1\n\n# COMMAND ----------\n\n# MAGIC %run ./lib\n\n# COMMAND ----------\n\n# MAGIC %pip install a==1 b\n")
	write("lib.py", "# Databricks notebook source\ny = 2\n")
	nb, err := ParseNotebook(dir, "nb.py")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range nb.Cells {
		got = append(got, fmt.Sprintf("%s/%s@%d:%q", c.Lang, c.Magic, c.Line, c.Body))
	}
	want := `[md/md@3:"# Title" python/@7:"x = 1" sql/sql@12:"SELECT 1" magic/run@17:"" magic/pip@21:""]`
	if fmt.Sprint(got) != want {
		t.Errorf("got  %v\nwant %s", got, want)
	}
	if target, err := RunTarget(dir, "nb.py", "./lib"); err != nil || target != "lib.py" {
		t.Errorf("run target: %s %v", target, err)
	}
	ts := &taskScan{}
	scanNotebook(dir, "nb.py", ts, map[string]bool{})
	if len(ts.findings) != 1 || !strings.Contains(ts.findings[0].Message, "move a==1, b to the task's libraries") || ts.findings[0].Line != 20 {
		t.Errorf("%+v", ts.findings)
	}
}

func TestParseNotebookIpynb(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "n.ipynb"), []byte(`{"cells":[
	 {"cell_type":"markdown","source":["# hi"]},
	 {"cell_type":"code","source":["%sql\n","SELECT 1"]},
	 {"cell_type":"code","source":"print(1)"}],
	 "metadata":{"application/vnd.databricks.v1+notebook":{"language":"python"}}}`), 0o644)
	nb, err := ParseNotebook(dir, "n.ipynb")
	if err != nil {
		t.Fatal(err)
	}
	if len(nb.Cells) != 3 || nb.Cells[1].Lang != "sql" || nb.Cells[1].Body != "SELECT 1" || nb.Cells[2].Body != "print(1)" {
		t.Errorf("%+v", nb.Cells)
	}
}

func TestFargateSizeAndNodes(t *testing.T) {
	n, ok := pricing.LookupNodeType("r6id.xlarge")
	if !ok || n.VCPU != 4 || n.MemoryGB != 32 {
		t.Fatalf("%v %v", n, ok)
	}
	for _, c := range []struct{ cpu, mem, wc, wm float64 }{
		{12, 96, 8, 60}, {4, 16, 4, 16}, {3, 5, 2, 5}, {16, 120, 16, 120}, {16, 100, 16, 96}, {1, 1, 1, 2},
	} {
		cpu, mem, ok := pricing.FargateSize(c.cpu, c.mem)
		if !ok || cpu != c.wc || mem != c.wm {
			t.Errorf("FargateSize(%g, %g) = %g, %g; want %g, %g", c.cpu, c.mem, cpu, mem, c.wc, c.wm)
		}
	}
	if _, _, ok := pricing.FargateSize(32, 64); ok {
		t.Error("32 vCPU must not fit Fargate")
	}
}

func TestPlanDetectors(t *testing.T) {
	cases := []struct{ name, taskB, files, clavesa, want string }{
		{"photon", `new_cluster: { spark_version: 17.3.x-scala2.13, node_type_id: m5d.large, num_workers: 1, runtime_engine: PHOTON }
          sql_task: { file: { path: b.sql }, warehouse_id: w }`, "", "{ catalog: glue }", "warn b: runtime_engine PHOTON"},
		{"old runtime", `new_cluster: { spark_version: 15.4.x-scala2.12, node_type_id: m5d.large, num_workers: 1 }
          sql_task: { file: { path: b.sql }, warehouse_id: w }`, "", "{ catalog: glue }", "runtime 15.4 runs Spark 3.5"},
		{"databricks conf", `new_cluster: { spark_version: 17.3.x-scala2.13, node_type_id: m5d.large, num_workers: 1, spark_conf: { spark.databricks.delta.preview.enabled: "true" } }
          sql_task: { file: { path: b.sql }, warehouse_id: w }`, "", "{ catalog: glue }", "spark_conf spark.databricks.delta.preview.enabled is Databricks only"},
		{"too big", `new_cluster: { spark_version: 17.3.x-scala2.13, node_type_id: r6id.4xlarge, num_workers: 4 }
          sql_task: { file: { path: b.sql }, warehouse_id: w }`, "", "{ catalog: glue }", "needs EMR Serverless"},
		{"copy into", `sql_task: { file: { path: c.sql }, warehouse_id: w }`, "c.sql=COPY INTO main.s.t FROM 's3://b'", "{ catalog: glue }", "refuse b: COPY INTO"},
		{"no compute", `spark_python_task: { python_file: a.py }`, "", "{ catalog: glue }", "needs a cluster or an environment"},
		{"unseen table", `environment_key: e
          spark_python_task: { python_file: d.py }`, "d.py=spark.table(name)", "{ catalog: glue }", "list the task's tables in clavesa.tables.b"},
		{"unseen but listed", `environment_key: e
          spark_python_task: { python_file: d.py }`, "d.py=spark.table(name)", "{ catalog: glue, tables: { b: { reads: [main.s.x] } } }", ""},
		{"bad ref", `sql_task: { file: { path: b.sql }, warehouse_id: w, parameters: { d: "{{tasks.a.output.rows}}" } }`, "", "{ catalog: glue }", "SQL task output is not carried"},
		{"unknown param", `sql_task: { file: { path: b.sql }, warehouse_id: w, parameters: { d: "{{job.parameters.nope}}" } }`, "", "{ catalog: glue }", "the job has no parameter nope"},
		{"jar lib", `sql_task: { file: { path: b.sql }, warehouse_id: w }
          libraries: [ { jar: x.jar } ]`, "", "{ catalog: glue }", "jar and maven libraries"},
		{"pin conflict", `sql_task: { file: { path: b.sql }, warehouse_id: w }
          libraries: [ { pypi: { package: "requests==2.31.0" } } ]`, "", "{ catalog: glue }", "package requests is pinned differently"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			files := map[string]string{"a.py": "import requests\nprint(1)", "b.sql": "SELECT 1"}
			if c.files != "" {
				k, v, _ := strings.Cut(c.files, "=")
				files[k] = v
			}
			yml := fmt.Sprintf(tinyBundle, c.taskB, c.clavesa)
			// task a pins requests 2.32.0 for the conflict case
			yml = strings.Replace(yml, `parameters: ["${var.table}"] }`, `parameters: ["${var.table}"] }
          libraries: [ { pypi: { package: "requests==2.32.0" } } ]`, 1)
			dir := writeBundle(t, yml, files)
			r := planOf(t, dir, "")
			got := messages(r)
			if c.want == "" {
				if got != "" {
					t.Errorf("want no findings, got:\n%s", got)
				}
				return
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("want %q in:\n%s", c.want, got)
			}
		})
	}
}
