package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// accessWorkspace lays out a workspace with one pipeline "taxi" of two
// transforms: trips (tagged) and revenue (untagged).
func accessWorkspace(t *testing.T, accessBlock, tripsDefs string) *Service {
	t.Helper()
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	manifest := `{"name":"smoke-x","cloud":"aws","version":1,"catalog":"clavesa_smoke_x","system_catalog":"clavesa_smoke_x_system"`
	if accessBlock != "" {
		manifest += `,"access":` + accessBlock
	}
	write("clavesa.json", manifest+"}")
	write("taxi/main.tf", `
module "trips" {
  source             = "../.clavesa/modules/v2.27.13/transform/aws"
  name               = "trips"
  language           = "sql"
  sql                = "SELECT 1 AS a"
  output_definitions = `+tripsDefs+`
}

module "revenue" {
  source             = "../.clavesa/modules/v2.27.13/transform/aws"
  name               = "revenue"
  language           = "sql"
  sql                = "SELECT a FROM trips"
  inputs             = { trips = module.trips.outputs["default"] }
  output_definitions = { default = {} }
}
`)
	return New(root)
}

const accessBlock = `{"tags":["open","ip","raw"],"audiences":{"raw":["open","ip","raw"]}}`

func TestAccessTags_ListsEveryTable(t *testing.T) {
	s := accessWorkspace(t, accessBlock, `{ default = { table_tag = "open", column_tags = { pickup_ip = "ip" } } }`)
	rep, err := s.AccessTags()
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Enabled || rep.TagKey != "clavesa_smoke_x" || len(rep.Problems) != 0 {
		t.Fatalf("report = %+v", rep)
	}
	if len(rep.Tables) != 2 {
		t.Fatalf("tables = %+v, want trips and revenue", rep.Tables)
	}
	rev, trips := rep.Tables[0], rep.Tables[1]
	if rev.Table != "revenue" || rev.TableTag != "unreviewed" || len(rev.Columns) != 0 {
		t.Errorf("revenue = %+v: an untagged table is unreviewed", rev)
	}
	if trips.Table != "trips" || trips.Database != "clavesa_smoke_x__taxi" || trips.TableTag != "open" ||
		len(trips.Columns) != 1 || trips.Columns[0] != (AccessColumn{Name: "pickup_ip", Tag: "ip"}) {
		t.Errorf("trips = %+v", trips)
	}
	if rep.Audiences["raw"][1] != "ip" {
		t.Errorf("audiences = %v", rep.Audiences)
	}
}

func TestAccessTags_Problems(t *testing.T) {
	cases := []struct {
		name, block, defs, want string
	}{
		{"undeclared value", accessBlock, `{ default = { column_tags = { email = "pii" } } }`, `column_tags["email"] = "pii" is not in`},
		{"tags with no block", "", `{ default = { table_tag = "open" } }`, `no "access" block`},
		{"invalid block", `{"tags":["ip"]}`, `{ default = {} }`, `must include "open"`},
		{"list value", accessBlock, `{ default = { column_tags = { c = ["ip"] } } }`, "one tag value"},
	}
	for _, c := range cases {
		s := accessWorkspace(t, c.block, c.defs)
		rep, err := s.AccessTags()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !strings.Contains(strings.Join(rep.Problems, "\n"), c.want) {
			t.Errorf("%s: problems %q, want %q", c.name, rep.Problems, c.want)
		}
	}
}

func TestAccessTags_NoBlockNoTags(t *testing.T) {
	s := accessWorkspace(t, "", `{ default = {} }`)
	rep, err := s.AccessTags()
	if err != nil {
		t.Fatal(err)
	}
	if rep.Enabled || len(rep.Tables) != 0 || len(rep.Problems) != 0 {
		t.Errorf("report = %+v, want nothing", rep)
	}
}
