package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMergeBoundCols_MatchesRunner(t *testing.T) {
	// Cases of tests/runner/test_merge_bound.py's _merge_bound_cols.
	cases := []struct {
		mk, cb, bb, want []string
	}{
		{[]string{"id"}, nil, nil, []string{"id"}},
		{[]string{"id"}, []string{"event_date"}, nil, nil},
		{[]string{"id"}, []string{"event_date"}, []string{"event_date"}, []string{"event_date"}},
		{[]string{"a", "b"}, []string{"b", "a"}, []string{"d"}, []string{"a", "b", "d"}},
		{[]string{"e"}, []string{"a", "b", "c", "d", "e"}, nil, nil},
	}
	for _, c := range cases {
		got := mergeBoundCols(c.mk, c.cb, c.bb)
		if strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("mergeBoundCols(%v, %v, %v) = %v, want %v", c.mk, c.cb, c.bb, got, c.want)
		}
	}
}

// The web-traffic outputs of 2026-11-21 (monorepo analytics/web-traffic):
// silver enriched is the one whose bound prunes nothing.
func TestMergeBoundWarning_WebTraffic(t *testing.T) {
	cases := []struct {
		name       string
		mk, cb, bb []string
		warn       string
	}{
		{"silver enriched", []string{"x_edge_request_id"}, nil, nil, "bounded only on x_edge_request_id"},
		{"fact_page_views", []string{"x_edge_request_id"}, []string{"event_date", "website_domain"}, []string{"event_date"}, ""},
		{"dim_session", []string{"session_id", "website_domain"}, nil, nil, ""},
		{"rollup_daily", []string{"website_domain", "event_date"}, []string{"event_date", "website_domain"}, nil, ""},
		{"no bound", []string{"user_id"}, []string{"country"}, nil, "has no bound"},
		{"uuid only", []string{"uuid"}, nil, nil, "bounded only on uuid"},
		{"id and time", []string{"order_id", "created_at"}, nil, nil, ""},
		{"plain key", []string{"sku"}, nil, nil, ""},
	}
	for _, c := range cases {
		got := mergeBoundWarning("n", "default", c.mk, c.cb, c.bb, false)
		if c.warn == "" && got != "" {
			t.Errorf("%s: unexpected warning %q", c.name, got)
		}
		if c.warn != "" && !strings.Contains(got, c.warn) {
			t.Errorf("%s: warning %q, want it to contain %q", c.name, got, c.warn)
		}
	}
}

func TestMergeBoundWarning_PartitionedGetsNoClusterAdvice(t *testing.T) {
	got := mergeBoundWarning("n", "default", []string{"event_id"}, nil, nil, true)
	if !strings.Contains(got, "partition_by on it") || strings.Contains(got, "cluster_by on it") {
		t.Errorf("partitioned output: %q", got)
	}
	if got := mergeBoundWarning("n", "default", []string{"event_id"}, nil, nil, false); !strings.Contains(got, "cluster_by on it") {
		t.Errorf("clustered output: %q", got)
	}
}

func TestMergeBoundWarnings_Pipeline(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "clavesa.json"), []byte(`{"name":"w","cloud":"aws","version":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "silver")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(`
module "enriched" {
  source             = "../.clavesa/modules/v2.27.14/transform/aws"
  name               = "enriched"
  language           = "sql"
  sql                = "SELECT 1 AS x_edge_request_id"
  output_definitions = { default = { mode = "merge", merge_keys = ["x_edge_request_id"] } }
}

module "daily" {
  source             = "../.clavesa/modules/v2.27.14/transform/aws"
  name               = "daily"
  language           = "sql"
  sql                = "SELECT 1 AS event_date"
  output_definitions = { default = { mode = "merge", merge_keys = ["event_date"] } }
}

module "plain" {
  source             = "../.clavesa/modules/v2.27.14/transform/aws"
  name               = "plain"
  language           = "sql"
  sql                = "SELECT 1 AS id"
  output_definitions = { default = {} }
}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := New(root).MergeBoundWarnings(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !strings.Contains(got[0], "node enriched, output default") || !strings.Contains(got[0], "x_edge_request_id") {
		t.Errorf("warnings = %q, want one for enriched", got)
	}
}
