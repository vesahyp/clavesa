package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// stubDatabricks answers every check call; failCatalog makes the catalog
// lookup fail so ConnectDatabricks sees a failed check. The checks
// themselves are tested against a fake server in internal/databricks.
type stubDatabricks struct {
	gotSecret   string
	failCatalog bool
}

func (s *stubDatabricks) Token(context.Context) (string, error) { return "tok", nil }
func (s *stubDatabricks) MetastoreSummary(context.Context) (databricks.MetastoreSummary, error) {
	on := true
	return databricks.MetastoreSummary{MetastoreID: "ms-1", ExternalAccessEnabled: &on}, nil
}
func (s *stubDatabricks) GetCatalog(_ context.Context, name string) (databricks.Catalog, error) {
	if s.failCatalog {
		return databricks.Catalog{}, &databricks.APIError{Status: 404, Message: "no catalog"}
	}
	return databricks.Catalog{Name: name}, nil
}
func (s *stubDatabricks) GetExternalLocation(_ context.Context, name string) (databricks.ExternalLocation, error) {
	return databricks.ExternalLocation{Name: name, URL: "s3://b/"}, nil
}
func (s *stubDatabricks) EffectivePrivileges(context.Context, string, string, string) (map[string]bool, error) {
	return map[string]bool{"ALL_PRIVILEGES": true}, nil
}

func newDatabricksWorkspace(t *testing.T, stub *stubDatabricks) *Service {
	t.Helper()
	ws := t.TempDir()
	m := &workspace.Manifest{Name: "ws", Cloud: "aws", Version: 1, Catalog: "clavesa_ws", SystemCatalog: "clavesa_ws_system"}
	if err := workspace.SaveManifest(ws, m); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "main.tf"), []byte(workspace.WorkspaceMainTF(m, "v9.9.9")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(ws, ".clavesa", "credentials"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, ".clavesa", "credentials", "dbx.secret"), []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := New(ws)
	if _, err := svc.AddCredential(CredentialSpec{Name: "dbx-sp", Kind: "databricks-oauth", ClientID: "app-id", Secret: "file:.clavesa/credentials/dbx.secret"}); err != nil {
		t.Fatal(err)
	}
	prev := newDatabricksAPI
	newDatabricksAPI = func(host, clientID, secret string) databricks.API {
		stub.gotSecret = secret
		return stub
	}
	t.Cleanup(func() { newDatabricksAPI = prev })
	return svc
}

func TestConnectDatabricks_RecordsWhenChecksPass(t *testing.T) {
	stub := &stubDatabricks{}
	svc := newDatabricksWorkspace(t, stub)
	off := false
	st, err := svc.ConnectDatabricks(context.Background(), DatabricksConnectInput{
		Host: "https://dbc-1.cloud.databricks.com/", Catalog: "analytics", Credential: "dbx-sp", PushLineage: &off,
	})
	if err != nil {
		t.Fatalf("ConnectDatabricks: %v", err)
	}
	if !st.Saved || !st.Connected || len(st.Checks) != 6 {
		t.Errorf("status = %+v, want saved with 6 checks", st)
	}
	if stub.gotSecret != "s3cret" {
		t.Errorf("secret handed to the client = %q, want the file content trimmed", stub.gotSecret)
	}
	m, err := workspace.Load(svc.workspace)
	if err != nil {
		t.Fatal(err)
	}
	want := workspace.Databricks{Host: "https://dbc-1.cloud.databricks.com", Catalog: "analytics", Credential: "dbx-sp", PublishTables: true, PushLineage: false}
	if m.Databricks == nil || !reflect.DeepEqual(*m.Databricks, want) {
		t.Errorf("manifest databricks = %+v, want %+v", m.Databricks, want)
	}

	got, err := svc.GetDatabricks()
	if err != nil || !got.Connected || got.ClientID != "app-id" {
		t.Errorf("GetDatabricks = %+v / %v", got, err)
	}

	if err := svc.DeleteCredential("dbx-sp", false); err == nil || !strings.Contains(err.Error(), "Databricks connection") {
		t.Errorf("DeleteCredential of the connection's credential = %v, want refusal", err)
	}

	ucTF := filepath.Join(svc.workspace, workspace.UnityCatalogTFFile)
	data, err := os.ReadFile(ucTF)
	if err != nil {
		t.Fatalf("connect did not write %s: %v", workspace.UnityCatalogTFFile, err)
	}
	if !strings.Contains(string(data), "modules/v9.9.9/unity-catalog/aws") || !strings.Contains(string(data), `databricks_client_id = "app-id"`) {
		t.Errorf("%s:\n%s", workspace.UnityCatalogTFFile, data)
	}
	if strings.Contains(string(data), "s3cret") {
		t.Error("the OAuth secret leaked into unity_catalog.tf")
	}

	env, err := svc.TerraformEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(env, "DATABRICKS_CLIENT_SECRET=s3cret") {
		t.Error("TerraformEnv lacks DATABRICKS_CLIENT_SECRET for a connected workspace")
	}

	if _, err := svc.DisconnectDatabricks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m, _ := workspace.Load(svc.workspace); m.Databricks != nil {
		t.Errorf("databricks still recorded after disconnect: %+v", m.Databricks)
	}
	if _, err := os.Stat(ucTF); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s still present after disconnect: %v", workspace.UnityCatalogTFFile, err)
	}
	env, err = svc.TerraformEnv(context.Background())
	if err != nil || containsString(env, "DATABRICKS_CLIENT_SECRET=s3cret") {
		t.Errorf("TerraformEnv after disconnect: %v, secret present=%t", err, containsString(env, "DATABRICKS_CLIENT_SECRET=s3cret"))
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestUnityCatalogConfig(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	if _, err := svc.ConnectDatabricks(context.Background(), DatabricksConnectInput{
		Host: "https://dbc-1.cloud.databricks.com", Catalog: "analytics", Credential: "dbx-sp",
	}); err != nil {
		t.Fatal(err)
	}
	m, _ := workspace.Load(svc.workspace)
	cfg, err := svc.unityCatalogConfig(m, filepath.Join(svc.workspace, "web"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientID != "app-id" || cfg.Catalog != "analytics" || !strings.HasSuffix(cfg.ModuleSource, "/unity-catalog/aws/schema") {
		t.Errorf("config = %+v", cfg)
	}

	// An unreadable credential must stop the emit, not drop the schema.
	if err := os.Remove(filepath.Join(svc.workspace, ".clavesa", "credentials", "dbx-sp.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.unityCatalogConfig(m, filepath.Join(svc.workspace, "web")); err == nil {
		t.Error("missing credential gave no error")
	}
}

func TestConnectDatabricks_WritesNothingWhenACheckFails(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{failCatalog: true})
	st, err := svc.ConnectDatabricks(context.Background(), DatabricksConnectInput{
		Host: "https://dbc-1.cloud.databricks.com", Catalog: "analytics", Credential: "dbx-sp",
	})
	if !errors.Is(err, ErrDatabricksChecksFailed) {
		t.Fatalf("err = %v, want ErrDatabricksChecksFailed", err)
	}
	if st.Saved || len(st.Checks) != 6 {
		t.Errorf("status = %+v, want unsaved with every check listed", st)
	}
	if m, _ := workspace.Load(svc.workspace); m.Databricks != nil {
		t.Errorf("manifest written despite a failed check: %+v", m.Databricks)
	}
}

func TestConnectDatabricks_InputErrors(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	if _, err := svc.AddCredential(CredentialSpec{Name: "stripe", Kind: "header", HeaderName: "Authorization", Secret: "env:X"}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		in   DatabricksConnectInput
		want string
	}{
		{"http host", DatabricksConnectInput{Host: "http://dbc-1", Catalog: "a", Credential: "dbx-sp"}, "https URL"},
		{"host with path", DatabricksConnectInput{Host: "https://dbc-1/x", Catalog: "a", Credential: "dbx-sp"}, "no path"},
		{"bad catalog", DatabricksConnectInput{Host: "https://dbc-1", Catalog: "my-cat", Credential: "dbx-sp"}, "letters, digits and underscores"},
		{"missing credential", DatabricksConnectInput{Host: "https://dbc-1", Catalog: "a", Credential: "nope"}, "credential not registered"},
		{"header credential", DatabricksConnectInput{Host: "https://dbc-1", Catalog: "a", Credential: "stripe"}, "needs kind databricks-oauth"},
	}
	for _, tc := range cases {
		_, err := svc.ConnectDatabricks(context.Background(), tc.in)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tc.name, err, tc.want)
		}
	}
}

func TestAddSourceRefusesDatabricksCredential(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	_, err := svc.AddSource(SourceSpec{Name: "api", Kind: "http", URL: "https://example.com/x.json", Format: "json", Credentials: "dbx-sp"})
	if err == nil || !strings.Contains(err.Error(), "needs a header credential") {
		t.Errorf("AddSource with a databricks-oauth credential = %v, want refusal", err)
	}
}

// With --create-catalog, disconnect takes the Databricks objects out of
// the workspace stack's state before unity_catalog.tf goes, and keeps the
// runner secret in state so the next deploy deletes it.
func TestDisconnectDatabricks_CreateCatalogReleasesState(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	m, err := workspace.Load(svc.workspace)
	if err != nil {
		t.Fatal(err)
	}
	m.Databricks = &workspace.Databricks{Host: "https://dbc-1.cloud.databricks.com", Catalog: "analytics", Credential: "dbx-sp", CreateCatalog: true, PublishTables: true, PushLineage: true}
	if err := workspace.SaveManifest(svc.workspace, m); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(svc.workspace, "terraform.tfstate"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	prev := terraformStateCmd
	terraformStateCmd = func(_ context.Context, _ *Service, dir string, args ...string) (string, int, error) {
		calls = append(calls, args)
		if args[1] == "list" {
			return strings.Join([]string{
				"module.workspace.aws_s3_bucket.pipeline",
				"module.unity_catalog.data.databricks_aws_unity_catalog_assume_role_policy.workspace[0]",
				"module.unity_catalog.aws_iam_role.unity_catalog[0]",
				"module.unity_catalog.aws_secretsmanager_secret.client_secret[0]",
				"module.unity_catalog.databricks_catalog.workspace[0]",
			}, "\n"), 0, nil
		}
		return "", 0, nil
	}
	t.Cleanup(func() { terraformStateCmd = prev })

	released, err := svc.DisconnectDatabricks(context.Background())
	if err != nil {
		t.Fatalf("DisconnectDatabricks: %v", err)
	}
	want := []string{
		"module.unity_catalog.data.databricks_aws_unity_catalog_assume_role_policy.workspace[0]",
		"module.unity_catalog.aws_iam_role.unity_catalog[0]",
		"module.unity_catalog.databricks_catalog.workspace[0]",
	}
	if strings.Join(released, ",") != strings.Join(want, ",") {
		t.Errorf("released = %v, want %v", released, want)
	}
	if len(calls) != 2 || strings.Join(calls[1], " ") != "state rm "+strings.Join(want, " ") {
		t.Errorf("terraform calls = %v", calls)
	}
	if m, _ := workspace.Load(svc.workspace); m.Databricks != nil {
		t.Errorf("databricks still recorded after disconnect")
	}
}
