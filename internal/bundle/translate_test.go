package bundle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTranslateNotebook(t *testing.T) {
	dir := FixDailySales(t)
	got, err := TranslateNotebook(dir, "notebooks/publish.py")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"import bundle_task as __clavesa_bt",
		"# %run ./common: inlined from notebooks/common.py",
		"def with_run_date(df, run_date):",
		"from tabulate import tabulate",
		"dbutils.notebook.exit(str(len(rows)))",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("translated publish.py lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "%pip") || strings.Contains(got, "# MAGIC") {
		t.Errorf("magic lines left in the script:\n%s", got)
	}

	ingest, err := TranslateNotebook(dir, "notebooks/ingest.py")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ingest, `__clavesa_bt.notebook_sql(spark, __clavesa_names__, dbutils, "CREATE OR REPLACE TABLE main.sales.orders_raw AS`) {
		t.Errorf("the %%sql cell is not a notebook_sql call:\n%s", ingest)
	}
	if strings.Contains(ingest, "# Ingest") {
		t.Errorf("the %%md cell should be dropped:\n%s", ingest)
	}
}

func TestTranslateNotebookFormats(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		_ = os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755)
		_ = os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644)
	}
	write("q.sql", "-- Databricks notebook source\nSELECT 1\n\n-- COMMAND ----------\n\n-- MAGIC %python\n-- MAGIC print('hi')\n")
	write("n.ipynb", `{"cells":[{"cell_type":"code","source":["%sql\n","SELECT :d"]},{"cell_type":"code","source":["%fs ls /tmp"]}],
	 "metadata":{"application/vnd.databricks.v1+notebook":{"language":"python"}}}`)
	write("bad.py", "# Databricks notebook source\n# MAGIC %sh ls\n")
	sqlNb, err := TranslateNotebook(dir, "q.sql")
	if err != nil || !strings.Contains(sqlNb, `notebook_sql(spark, __clavesa_names__, dbutils, "SELECT 1"`) || !strings.Contains(sqlNb, "print('hi')") {
		t.Errorf("sql notebook: %v\n%s", err, sqlNb)
	}
	ipynb, err := TranslateNotebook(dir, "n.ipynb")
	if err != nil || !strings.Contains(ipynb, `"SELECT :d"`) || !strings.Contains(ipynb, `__clavesa_bt.notebook_fs(dbutils, "ls /tmp")`) {
		t.Errorf("ipynb: %v\n%s", err, ipynb)
	}
	if _, err := TranslateNotebook(dir, "bad.py"); err == nil || !strings.Contains(err.Error(), "%sh runs only in Databricks") {
		t.Errorf("%%sh: %v", err)
	}
}
