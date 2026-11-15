package service

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// A pipeline generated from a bundle: optimize takes its tables from the
// job file, under the runner's Spark names, and leaves Databricks' tasks
// and disabled tasks alone.
func TestBundleOptimizeTargets(t *testing.T) {
	dir := t.TempDir()
	job := `{"bundle":"b","job":"j","catalog":"unity","shadow":true,"catalogs":{"main":"prod"},
	  "order":["raw","daily","compact","dbx","off"],
	  "tasks":{
	    "raw":{"task_key":"raw","place":"clavesa","writes":["team.sales.orders_raw"]},
	    "daily":{"task_key":"daily","place":"clavesa","writes":["main.sales.orders_daily"]},
	    "compact":{"task_key":"compact","place":"clavesa","writes":["team.sales.orders_raw"]},
	    "dbx":{"task_key":"dbx","place":"databricks","writes":["team.sales.report"]},
	    "off":{"task_key":"off","place":"clavesa","disabled":true,"writes":["team.sales.old"]}}}`
	if err := os.WriteFile(filepath.Join(dir, "bundle-job.json"), []byte(job), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := bundleOptimizeTargets(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	var ids, nodes []string
	for _, g := range got {
		ids = append(ids, g.tableID)
		nodes = append(nodes, g.node.ID)
	}
	if want := []string{"team__sales.orders_raw_clavesa", "prod__sales.orders_daily_clavesa"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("tables %v, want %v", ids, want)
	}
	if want := []string{"raw", "daily"}; !reflect.DeepEqual(nodes, want) {
		t.Fatalf("nodes %v, want %v", nodes, want)
	}
	got, _ = bundleOptimizeTargets(dir, "daily")
	if len(got) != 1 || got[0].tableID != "prod__sales.orders_daily_clavesa" {
		t.Fatalf("--node daily: %+v", got)
	}
	// Any other pipeline: no job file, no bundle targets.
	if got, err := bundleOptimizeTargets(t.TempDir(), ""); err != nil || got != nil {
		t.Fatalf("plain pipeline: %v %v", got, err)
	}
}
