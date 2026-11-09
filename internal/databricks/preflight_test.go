package databricks

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// fakeGetter answers GETs from a map of path prefix to JSON; anything
// else is a 404.
type fakeGetter map[string]string

func (f fakeGetter) GetJSON(_ context.Context, path string, out any) error {
	best := ""
	for p := range f {
		if strings.HasPrefix(path, p) && len(p) > len(best) {
			best = p
		}
	}
	if best == "" {
		return &APIError{Status: 404, Message: "not found", Path: path}
	}
	return json.Unmarshal([]byte(f[best]), out)
}

func find(t *testing.T, checks []Check, id string) Check {
	t.Helper()
	for _, c := range checks {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("no check %q in %+v", id, checks)
	return Check{}
}

func TestPreflightManagedTableMissingGrants(t *testing.T) {
	g := fakeGetter{
		"/api/2.0/preview/scim/v2/Me":            `{"userName":"vesa@example.com"}`,
		"/api/2.1/unity-catalog/metastore_summary": `{"metastore_id":"ms1","external_access_enabled":false}`,
		"/api/2.1/unity-catalog/effective-permissions/metastore/": `{"privilege_assignments":[]}`,
		"/api/2.1/unity-catalog/tables/main.sales.daily": `{"full_name":"main.sales.daily","catalog_name":"main","schema_name":"sales",
			"table_type":"MANAGED","data_source_format":"DELTA","storage_location":"s3://dbx/managed/daily","owner":"team",
			"columns":[{"name":"event_date","partition_index":0},{"name":"amount"}],
			"properties":{"delta.feature.deletionVectors":"supported"}}`,
		"/api/2.1/unity-catalog/effective-permissions/catalog/main":       `{"privilege_assignments":[{"privileges":[{"privilege":"USE_CATALOG"}]}]}`,
		"/api/2.1/unity-catalog/effective-permissions/schema/main.sales":  `{"privilege_assignments":[{"privileges":[{"privilege":"USE_SCHEMA"}]}]}`,
		"/api/2.1/unity-catalog/effective-permissions/table/main.sales.daily": `{"privilege_assignments":[{"privileges":[{"privilege":"SELECT"}]}]}`,
		"/api/2.1/jobs/get":            `{"settings":{"name":"daily","schedule":{"pause_status":"UNPAUSED"}}}`,
		"/api/2.0/permissions/jobs/7": `{"access_control_list":[{"service_principal_name":"sp-1","all_permissions":[{"permission_level":"CAN_VIEW"}]}]}`,
	}
	checks := Preflight(context.Background(), g, PreflightTarget{Host: "https://h", Principal: "sp-1", Table: "main.sales.daily", Job: 7})

	if c := find(t, checks, PreExternalAccess); c.Status != StatusFail {
		t.Errorf("external access: %+v", c)
	}
	if c := find(t, checks, PreLineage); c.Status != StatusFail || !strings.Contains(c.Fix, "GRANT CREATE EXTERNAL METADATA ON METASTORE TO `sp-1`;") {
		t.Errorf("lineage: %+v", c)
	}
	if c := find(t, checks, PreTable); c.Status != StatusPass || !strings.Contains(c.Detail, `partition_by = ["event_date"]`) || !strings.Contains(c.Detail, "shadow table") {
		t.Errorf("table: %+v", c)
	}
	c := find(t, checks, PreTableGrants)
	if c.Status != StatusFail {
		t.Fatalf("grants: %+v", c)
	}
	for _, want := range []string{
		"GRANT CREATE TABLE, EXTERNAL USE SCHEMA ON SCHEMA `main`.`sales` TO `sp-1`;",
		"GRANT MODIFY ON TABLE `main`.`sales`.`daily` TO `sp-1`;",
	} {
		if !strings.Contains(c.Fix, want) {
			t.Errorf("grants fix lacks %q:\n%s", want, c.Fix)
		}
	}
	if c := find(t, checks, PreJob); c.Status != StatusPass || !strings.Contains(c.Detail, "unpaused") {
		t.Errorf("job: %+v", c)
	}
}

func TestPreflightSignInFails(t *testing.T) {
	checks := Preflight(context.Background(), fakeGetter{}, PreflightTarget{Host: "https://h"})
	if len(checks) != 1 || checks[0].Status != StatusFail {
		t.Errorf("checks = %+v", checks)
	}
}

func TestFeatureCheckRefusesCatalogManagedCommits(t *testing.T) {
	c := featureCheck(ucTable{FullName: "a.b.c", TableType: "EXTERNAL", Properties: map[string]string{"delta.feature.catalogManaged": "supported"}})
	if c.Status != StatusFail {
		t.Errorf("%+v", c)
	}
	c = featureCheck(ucTable{FullName: "a.b.c", TableType: "EXTERNAL", Properties: map[string]string{"delta.feature.somethingNew": "supported"}})
	if c.Status != StatusUnknown {
		t.Errorf("%+v", c)
	}
}
