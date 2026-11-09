package databricks

import (
	"context"
	"testing"
)

func run(t *testing.T, f *fakeWorkspace, target Target) map[string]Check {
	t.Helper()
	srv := f.server(t)
	target.Host = srv.URL
	if target.ClientID == "" {
		target.ClientID = f.clientID
	}
	if target.Catalog == "" {
		target.Catalog = "analytics"
	}
	checks := RunChecks(context.Background(), newTestClient(srv, target.ClientID, f.secret), target)
	if len(checks) != 6 {
		t.Fatalf("got %d checks, want 6: %+v", len(checks), checks)
	}
	return byID(checks)
}

func wantStatus(t *testing.T, got map[string]Check, want map[string]string) {
	t.Helper()
	for id, st := range want {
		if got[id].Status != st {
			t.Errorf("%s: status %s (%s), want %s", id, got[id].Status, got[id].Detail, st)
		}
	}
}

// Default mode: the team's existing catalog, no --external-location.
func TestRunChecks_ExistingCatalog(t *testing.T) {
	got := run(t, readyWorkspace(), Target{})
	wantStatus(t, got, map[string]string{
		CheckLogin: StatusPass, CheckExternalAccess: StatusPass, CheckExternalMetadata: StatusPass,
		CheckCatalog: StatusPass, CheckCatalogGrants: StatusPass, CheckStorage: StatusSkipped,
	})
	for id, c := range got {
		if c.Status != StatusFail && c.Fix != "" {
			t.Errorf("%s: a non-failing check carries a fix: %q", id, c.Fix)
		}
	}
}

func TestRunChecks_ExistingCatalogMissing(t *testing.T) {
	f := readyWorkspace()
	got := run(t, f, Target{Catalog: "nope"})
	wantStatus(t, got, map[string]string{CheckCatalog: StatusFail, CheckCatalogGrants: StatusSkipped})
	if !contains(got[CheckCatalog].Fix, "--create-catalog") {
		t.Errorf("fix should offer --create-catalog: %q", got[CheckCatalog].Fix)
	}
}

func TestRunChecks_NoUseCatalog(t *testing.T) {
	f := readyWorkspace()
	f.catalogs["analytics"] = nil
	got := run(t, f, Target{})
	if c := got[CheckCatalogGrants]; c.Status != StatusFail || !contains(c.Fix, "GRANT USE CATALOG ON CATALOG `analytics` TO `sp-app-id`;") {
		t.Errorf("catalog-grants = %+v", c)
	}
}

func TestRunChecks_CreateCatalog(t *testing.T) {
	f := readyWorkspace()
	got := run(t, f, Target{Catalog: "fresh", CreateCatalog: true})
	wantStatus(t, got, map[string]string{
		CheckCatalog: StatusPass, CheckCatalogGrants: StatusSkipped, CheckStorage: StatusPass,
	})

	f.metastorePrivs = []string{"CREATE_EXTERNAL_METADATA"}
	got = run(t, f, Target{Catalog: "fresh", CreateCatalog: true})
	wantStatus(t, got, map[string]string{CheckCatalog: StatusFail, CheckStorage: StatusFail})
	if !contains(got[CheckCatalog].Fix, "GRANT CREATE CATALOG ON METASTORE TO `sp-app-id`;") {
		t.Errorf("catalog fix = %q", got[CheckCatalog].Fix)
	}
	if !contains(got[CheckStorage].Fix, "GRANT CREATE STORAGE CREDENTIAL, CREATE EXTERNAL LOCATION ON METASTORE") {
		t.Errorf("storage fix = %q", got[CheckStorage].Fix)
	}
}

func TestRunChecks_CreateCatalogThatExists(t *testing.T) {
	f := readyWorkspace()
	got := run(t, f, Target{CreateCatalog: true}) // analytics belongs to "admins"
	if c := got[CheckCatalog]; c.Status != StatusFail || !contains(c.Fix, "Leave out --create-catalog") {
		t.Errorf("catalog = %+v", c)
	}

	// After the first deploy the catalog exists and the SP owns it: a
	// re-run of connect must still pass.
	f.catalogOwner = map[string]string{"analytics": f.clientID}
	got = run(t, f, Target{CreateCatalog: true})
	wantStatus(t, got, map[string]string{CheckCatalog: StatusPass})
}

func TestRunChecks_WrongSecretSkipsTheRest(t *testing.T) {
	f := readyWorkspace()
	srv := f.server(t)
	checks := RunChecks(context.Background(), newTestClient(srv, f.clientID, "wrong"),
		Target{Host: srv.URL, ClientID: f.clientID, Catalog: "analytics"})
	got := byID(checks)
	if got[CheckLogin].Status != StatusFail || !contains(got[CheckLogin].Fix, "Generate secret") {
		t.Errorf("login = %+v, want fail with the secret fix", got[CheckLogin])
	}
	for _, id := range []string{CheckExternalAccess, CheckExternalMetadata, CheckCatalog, CheckCatalogGrants, CheckStorage} {
		if got[id].Status != StatusSkipped {
			t.Errorf("%s: status %s, want skipped after a failed sign-in", id, got[id].Status)
		}
	}
	if AllPass(checks) {
		t.Error("AllPass = true with a failed sign-in")
	}
}

func TestRunChecks_ExternalAccessOffAndNoLineageGrant(t *testing.T) {
	f := readyWorkspace()
	f.externalAccess = boolPtr(false)
	f.metastorePrivs = nil
	got := run(t, f, Target{})
	wantStatus(t, got, map[string]string{CheckExternalAccess: StatusFail, CheckExternalMetadata: StatusFail})
	if !contains(got[CheckExternalMetadata].Fix, "GRANT CREATE EXTERNAL METADATA ON METASTORE TO `sp-app-id`;") {
		t.Errorf("fix = %q", got[CheckExternalMetadata].Fix)
	}
}

// Found against the real test workspace: a service principal with no
// workspace entitlement signs in, then gets 403 on every UC call.
func TestRunChecks_NoEntitlement(t *testing.T) {
	f := readyWorkspace()
	f.noEntitlement = true
	got := run(t, f, Target{})
	c := got[CheckExternalAccess]
	if c.Status != StatusFail || !contains(c.Fix, "Workspace access entitlement") {
		t.Errorf("external-access = %+v, want the entitlement fix", c)
	}
}

func TestRunChecks_AllPrivilegesCounts(t *testing.T) {
	f := readyWorkspace()
	f.catalogs["analytics"] = []string{"ALL_PRIVILEGES"}
	if c := run(t, f, Target{})[CheckCatalogGrants]; c.Status != StatusPass {
		t.Errorf("catalog-grants with ALL_PRIVILEGES = %+v, want pass", c)
	}
}

func TestRunChecks_ExternalLocationGiven(t *testing.T) {
	f := readyWorkspace()
	f.externalLocation = map[string]string{"clavesa_bucket": "s3://clavesa-ws/"}
	c := run(t, f, Target{ExternalLocation: "clavesa_bucket"})[CheckStorage]
	if c.Status != StatusPass || c.Detail != "s3://clavesa-ws/" {
		t.Errorf("storage = %+v, want pass with the location URL", c)
	}
	c = run(t, f, Target{ExternalLocation: "missing"})[CheckStorage]
	if c.Status != StatusFail || !contains(c.Fix, "EXTERNAL USE LOCATION") {
		t.Errorf("storage with a missing location = %+v", c)
	}
}

func TestRunChecks_UnknownDoesNotBlock(t *testing.T) {
	f := readyWorkspace()
	f.externalAccess = nil
	f.metastoreForbid = true
	srv := f.server(t)
	checks := RunChecks(context.Background(), newTestClient(srv, f.clientID, f.secret),
		Target{Host: srv.URL, ClientID: f.clientID, Catalog: "fresh", CreateCatalog: true})
	got := byID(checks)
	for _, id := range []string{CheckExternalAccess, CheckExternalMetadata, CheckCatalog, CheckStorage} {
		if got[id].Status != StatusUnknown {
			t.Errorf("%s: status %s, want unknown", id, got[id].Status)
		}
	}
	if !AllPass(checks) {
		t.Error("AllPass = false; unknown checks must not block")
	}
}

func TestClient_TokenIsCached(t *testing.T) {
	f := readyWorkspace()
	srv := f.server(t)
	c := newTestClient(srv, f.clientID, f.secret)
	_ = RunChecks(context.Background(), c, Target{Host: srv.URL, ClientID: f.clientID, Catalog: "analytics"})
	if f.tokenCalls != 1 {
		t.Errorf("token endpoint called %d times, want 1", f.tokenCalls)
	}
}
