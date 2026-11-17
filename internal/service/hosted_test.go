package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

const hostedTestPrincipal = "arn:aws:iam::111122223333:role/clavesa-ui-hosted"

// hostedWorkspace lays out a workspace the way `workspace init` +
// `pipeline create` + a deploy leave it on disk, including every file the
// tree copy must never upload.
func hostedWorkspace(t *testing.T) string {
	t.Helper()
	ws := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(ws, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	m := &workspace.Manifest{Name: "smoke2", Cloud: "aws", Version: 1, Catalog: "clavesa_smoke2",
		SystemCatalog: "clavesa_smoke2_system", AWSNames: workspace.AWSNamesWorkspace,
		Backend: &workspace.Backend{Type: "s3", Bucket: "state-bucket", Region: "eu-north-1"}}
	if err := workspace.SaveManifest(ws, m); err != nil {
		t.Fatal(err)
	}
	write("main.tf", workspace.WorkspaceMainTF(m, "v9.9.9"))
	write("backend.tf", "terraform { backend \"s3\" {} }\n")
	write("terraform.tfstate.pre-migrate", `{"secret":"state"}`)
	write(".env", "AWS_SECRET_ACCESS_KEY=x\n")
	write(".clavesa/aws-profile.json", `{"profile":"personal"}`)
	// A cloud-warehouse workspace whose developer runs in cloud-local
	// mode: the tree copy must still say cloud.
	write(".clavesa/environment.json", `{"warehouse":"cloud","mode":"local"}`)
	write(".clavesa/credentials/api.json", `{"name":"api","secret":"env:API"}`)
	write(".clavesa/credentials/api.secret", "s3cret\n")
	write(".clavesa/sources/raw.json", `{"name":"raw"}`)
	write(".clavesa/dashboards/d.json", `{}`)
	write(".clavesa/dashboards/notes.txt", "x")
	write(".clavesa/dashboards/old/e.json", `{}`)
	write("dashboards/root.json", `{}`)
	write(".terraform/terraform.tfstate", `{}`)
	write("taxi/main.tf", summaryPipelineTF)
	write("taxi/variables.tf", "variable \"trigger_schedule\" { default = \"rate(1 day)\" }\n")
	write("taxi/terraform.tfvars", "trigger_schedule = \"rate(1 day)\"\n")
	write("taxi/extra.auto.tfvars", "x = 1\n")
	write("taxi/backend.tf", "terraform { backend \"s3\" {} }\n")
	write("taxi/terraform.tfstate", `{"secret":"state"}`)
	write("taxi/terraform.tfstate.backup", `{"secret":"state"}`)
	write("taxi/tfplan", "plan")
	write("taxi/.env.local", "X=1\n")
	write("taxi/.terraform.lock.hcl", "lock")
	write("taxi/.terraform/modules/modules.json", "{}")
	write("taxi/_clavesa_sidecar/poller.py", "print()")
	write("taxi/notes.md", "notes")
	// No stack is deployed: every state read answers "not found" and
	// none reaches S3.
	restore := workspace.SetStateGetterForTest(func(context.Context, string, string, string) ([]byte, error) { return nil, nil })
	t.Cleanup(restore)
	return ws
}

func TestHostedTreeFilesExclusions(t *testing.T) {
	ws := hostedWorkspace(t)
	files, err := HostedTreeFiles(ws, []string{"taxi"})
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, f := range files {
		keys = append(keys, f.Key)
	}
	sort.Strings(keys)
	// Dashboard specs go (ADR-034): *.json directly in .clavesa/dashboards/
	// and dashboards/, nothing else from there.
	want := []string{".clavesa/dashboards/d.json", ".clavesa/environment.json", ".clavesa/sources/raw.json", "clavesa.json", "dashboards/root.json", "taxi/extra.auto.tfvars", "taxi/main.tf", "taxi/terraform.tfvars", "taxi/variables.tf"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Errorf("tree keys = %v\nwant %v", keys, want)
	}
	for _, k := range keys {
		for _, bad := range []string{"tfstate", ".env", "credentials", ".secret", ".terraform", "tfplan", "aws-profile", "backend.tf", "_clavesa_sidecar", "notes.txt", "old/"} {
			if strings.Contains(k, bad) {
				t.Errorf("tree holds %s (matches %q)", k, bad)
			}
		}
	}
	// The environment file is generated (cloud), not the developer's own.
	for _, f := range files {
		if f.Key == ".clavesa/environment.json" && (f.Path != "" || !strings.Contains(string(f.Body), `"warehouse": "cloud"`)) {
			t.Errorf("environment.json = path %q body %q", f.Path, f.Body)
		}
	}
	if _, err := HostedTreeFiles(ws, []string{"../outside"}); err != nil {
		t.Errorf("a dir outside the workspace is skipped, not an error: %v", err)
	}
}

// stubWellKnown makes the well-known fetch answer wk (or err) for the
// test's duration.
func stubWellKnown(t *testing.T, wk HostedWellKnown, err error) *[]string {
	t.Helper()
	var urls []string
	prev := hostedWellKnown
	hostedWellKnown = func(_ context.Context, u string) (HostedWellKnown, error) {
		urls = append(urls, u)
		return wk, err
	}
	t.Cleanup(func() { hostedWellKnown = prev })
	return &urls
}

const hostedTestBus = "arn:aws:events:eu-north-1:111122223333:event-bus/clavesa-ui"

func TestEnableHosted(t *testing.T) {
	ws := hostedWorkspace(t)
	svc := New(ws)
	stubWellKnown(t, HostedWellKnown{}, errors.New("offline"))
	if _, err := svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{hostedTestPrincipal}}); err == nil || !strings.Contains(err.Error(), "offline") {
		t.Errorf("unreachable well-known: err = %v, want the fetch error", err)
	}
	urls := stubWellKnown(t, HostedWellKnown{ReaderPrincipal: hostedTestPrincipal, EventBusARN: hostedTestBus}, nil)
	if _, err := svc.EnableHosted(context.Background(), HostedInput{URL: "ftp://x", ReaderPrincipals: []string{hostedTestPrincipal}}); err == nil {
		t.Error("bad url must be refused")
	}
	if _, err := os.Stat(filepath.Join(ws, workspace.HostedTFFile)); !os.IsNotExist(err) {
		t.Fatalf("a refused enable must write nothing: %v", err)
	}
	st, err := svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev/", ReaderPrincipals: []string{" " + hostedTestPrincipal, hostedTestPrincipal}})
	if err != nil {
		t.Fatal(err)
	}
	if !st.Enabled || st.Hosted.URL != "https://ui.clavesa.dev" || len(st.Hosted.ReaderPrincipals) != 1 || st.ReaderRoleName != "clavesa-ui-reader-smoke2" || st.WorkGroup != "clavesa-ui-smoke2" {
		t.Errorf("status = %+v", st)
	}
	if st.Hosted.EventBusARN != hostedTestBus {
		t.Errorf("event bus = %q, want the well-known one", st.Hosted.EventBusARN)
	}
	if len(*urls) == 0 || (*urls)[len(*urls)-1] != "https://ui.clavesa.dev" {
		t.Errorf("well-known fetched from %v", *urls)
	}
	// No flags: the well-known principal alone. An extra principal is
	// kept after it.
	const extra = "arn:aws:iam::111122223333:user/Smoke"
	st, err = svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev"})
	if err != nil || len(st.Hosted.ReaderPrincipals) != 1 {
		t.Fatalf("no flags: %+v, %v", st, err)
	}
	st, err = svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{extra}})
	if err != nil || strings.Join(st.Hosted.ReaderPrincipals, ",") != hostedTestPrincipal+","+extra {
		t.Fatalf("extra principal: %+v, %v", st.Hosted, err)
	}
	m, _ := workspace.Load(ws)
	if m.Hosted == nil || m.Hosted.ReaderPrincipals[0] != hostedTestPrincipal {
		t.Errorf("manifest hosted = %+v", m.Hosted)
	}
	tf, err := os.ReadFile(filepath.Join(ws, workspace.HostedTFFile))
	if err != nil || !strings.Contains(string(tf), ModuleVersion+"/hosted-reader/aws") || !strings.Contains(string(tf), `"clavesa-smoke2-*"`) {
		t.Errorf("hosted.tf = %s, %v", tf, err)
	}
	if _, err := svc.DisableHosted(); err != nil {
		t.Fatal(err)
	}
	if m, _ := workspace.Load(ws); m.Hosted != nil {
		t.Error("disable left the block")
	}
	if _, err := os.Stat(filepath.Join(ws, workspace.HostedTFFile)); !os.IsNotExist(err) {
		t.Errorf("disable left hosted.tf: %v", err)
	}
}

type fakeTreeStore struct {
	puts    map[string][]byte
	existed []string
	deleted []string
}

func (f *fakeTreeStore) Region() string { return "eu-north-1" }
func (f *fakeTreeStore) Put(_ context.Context, bucket, key string, body []byte) error {
	if bucket != "smoke2-clavesa" {
		return errors.New("wrong bucket " + bucket)
	}
	f.puts[key] = body
	return nil
}
func (f *fakeTreeStore) List(context.Context, string, string) ([]string, error) {
	var keys []string
	for k := range f.puts {
		keys = append(keys, k)
	}
	return append(keys, f.existed...), nil
}
func (f *fakeTreeStore) Delete(_ context.Context, _ string, keys []string) error {
	f.deleted = append(f.deleted, keys...)
	return nil
}

func TestUploadHostedTree(t *testing.T) {
	ws := hostedWorkspace(t)
	stubWellKnown(t, HostedWellKnown{ReaderPrincipal: hostedTestPrincipal}, nil)
	// Not hosted: nothing happens.
	if res, err := New(ws).UploadHostedTree(context.Background()); err != nil || res != nil {
		t.Fatalf("not hosted = %+v, %v", res, err)
	}
	if _, err := New(ws).EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{hostedTestPrincipal}}); err != nil {
		t.Fatal(err)
	}
	sm := "arn:aws:states:eu-north-1:111122223333:stateMachine:clavesa-smoke2-taxi"
	restoreState := workspace.SetStateGetterForTest(func(_ context.Context, bucket, _, key string) ([]byte, error) {
		if strings.HasSuffix(key, "workspace.tfstate") {
			return []byte(`{"outputs":{"pipeline_bucket":{"value":"smoke2-clavesa"},"system_catalog":{"value":"clavesa_smoke2_system"},
"hosted_workgroup":{"value":"clavesa-ui-smoke2"},"hosted_reader_role_arn":{"value":"arn:aws:iam::111122223333:role/clavesa-ui-reader-smoke2"},
"runner_image":{"value":"secret-ish"}}}`), nil
		}
		return []byte(`{"resources":[{"type":"aws_sfn_state_machine","name":"pipeline","instances":[{"attributes":{"arn":"` + sm + `","definition":"x"}}]}]}`), nil
	})
	defer restoreState()
	store := &fakeTreeStore{puts: map[string][]byte{}, existed: []string{HostedTreePrefix + "gone/main.tf"}}
	prev := newHostedTreeStore
	newHostedTreeStore = func(context.Context, string) (HostedTreeStore, error) { return store, nil }
	defer func() { newHostedTreeStore = prev }()

	res, err := New(ws).UploadHostedTree(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.URI != "s3://smoke2-clavesa/_clavesa/tree/" || len(res.Removed) != 1 || res.Removed[0] != "gone/main.tf" {
		t.Errorf("result = %+v", res)
	}
	if len(store.deleted) != 1 || store.deleted[0] != HostedTreePrefix+"gone/main.tf" {
		t.Errorf("deleted = %v", store.deleted)
	}
	for k, body := range store.puts {
		if !strings.HasPrefix(k, HostedTreePrefix) || strings.Contains(string(body), "secret") {
			t.Errorf("uploaded %s = %q", k, body)
		}
	}
	var d workspace.Deployed
	if err := json.Unmarshal(store.puts[HostedTreePrefix+"deployed.json"], &d); err != nil {
		t.Fatal(err)
	}
	if d.Account != "111122223333" || d.Region != "eu-north-1" || d.Workspace.Outputs["pipeline_bucket"] != "smoke2-clavesa" ||
		d.Pipelines["taxi"].StateMachineARN != sm {
		t.Errorf("deployed.json = %+v", d)
	}
	if _, ok := d.Workspace.Outputs["runner_image"]; ok {
		t.Error("deployed.json carries an output not on the allow-list")
	}
}

// TestWorkspaceSummaryAccountFromDeployed: a hosted tree copy fills the
// summary's account from deployed.json.
func TestWorkspaceSummaryAccountFromDeployed(t *testing.T) {
	ws := summaryWorkspace(t, "")
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, workspace.DeployedFile), []byte(`{"version":1,"account":"111122223333","workspace":{"outputs":{}},"pipelines":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(ws).WithResolver(observability.NewResolver(ws, &fakeProvider{}, &fakeProvider{}))
	sum, err := svc.WorkspaceSummary(context.Background(), WorkspaceSummaryQuery{Now: summaryNow})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Account != "111122223333" {
		t.Errorf("account = %q", sum.Account)
	}
}

// A workspace pinned to a module version older than hosted-reader (new in
// v2.26.0) joins without a workspace upgrade: hosted.tf points at the
// binary's version, whose module tree is extracted when missing, and
// main.tf keeps its pin. `workspace upgrade` passes its own target.
func TestEnableHostedOlderPinnedWorkspace(t *testing.T) {
	ws := hostedWorkspace(t)
	m, _ := workspace.Load(ws)
	if err := os.WriteFile(filepath.Join(ws, "main.tf"), []byte(workspace.WorkspaceMainTF(m, "v2.22.0")), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(ws)
	stubWellKnown(t, HostedWellKnown{ReaderPrincipal: hostedTestPrincipal, EventBusARN: hostedTestBus}, nil)
	if _, err := svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev"}); err != nil {
		t.Fatal(err)
	}
	tf, err := os.ReadFile(filepath.Join(ws, workspace.HostedTFFile))
	if err != nil {
		t.Fatal(err)
	}
	want := `"./.clavesa/modules/` + ModuleVersion + `/hosted-reader/aws"`
	if !strings.Contains(string(tf), want) {
		t.Errorf("hosted.tf source: want %s in\n%s", want, tf)
	}
	if _, err := os.Stat(filepath.Join(ws, ".clavesa", "modules", ModuleVersion, "hosted-reader", "aws", "main.tf")); err != nil {
		t.Errorf("hosted-reader module not extracted at the binary's version: %v", err)
	}
	if v, _ := workspace.CurrentModuleVersion(ws); v != "v2.22.0" {
		t.Errorf("main.tf pin = %q, want v2.22.0 kept", v)
	}
	// The upgrade path: an explicit version wins.
	if err := svc.SyncHostedTF("v8.0.0"); err != nil {
		t.Fatal(err)
	}
	tf, _ = os.ReadFile(filepath.Join(ws, workspace.HostedTFFile))
	if !strings.Contains(string(tf), `"./.clavesa/modules/v8.0.0/hosted-reader/aws"`) {
		t.Errorf("SyncHostedTF(v8.0.0) wrote\n%s", tf)
	}
}

// A local-warehouse workspace's block: URL, publisher role, data bucket
// and region from the well-known document; no reader principals, no
// hosted.tf, and no tree copy on deploy.
func TestEnableHostedLocal(t *testing.T) {
	ws := hostedWorkspace(t)
	// No remote backend and no deployed stack: the warehouse setting
	// picks the kind.
	m0, _ := workspace.Load(ws)
	m0.Backend = nil
	if err := workspace.SaveManifest(ws, m0); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, ".clavesa", "environment.json"), []byte(`{"warehouse":"local","mode":"local"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(ws)
	const pub = "arn:aws:iam::111122223333:role/clavesa-ui-publisher"
	stubWellKnown(t, HostedWellKnown{ReaderPrincipal: hostedTestPrincipal, EventBusARN: hostedTestBus}, nil)
	if _, err := svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev"}); err == nil || !strings.Contains(err.Error(), "publisher role") {
		t.Errorf("no publisher role in the well-known: err = %v", err)
	}
	stubWellKnown(t, HostedWellKnown{ReaderPrincipal: hostedTestPrincipal, EventBusARN: hostedTestBus,
		PublisherRoleARN: pub, DataBucket: "clavesa-ui-data-111122223333", Region: "eu-north-1"}, nil)
	if _, err := svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{hostedTestPrincipal}}); err == nil {
		t.Error("a reader principal on a local workspace must be refused")
	}
	st, err := svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Kind != "local" || st.Hosted.PublisherRoleARN != pub || st.Hosted.DataBucket != "clavesa-ui-data-111122223333" ||
		st.Hosted.Region != "eu-north-1" || len(st.Hosted.ReaderPrincipals) != 0 || st.Hosted.EventBusARN != "" ||
		st.SummaryURI != "s3://clavesa-ui-data-111122223333/summaries/smoke2.json" || st.ReaderRoleName != "" {
		t.Errorf("status = %+v hosted %+v", st, st.Hosted)
	}
	if _, err := os.Stat(filepath.Join(ws, workspace.HostedTFFile)); !os.IsNotExist(err) {
		t.Errorf("a local workspace must get no hosted.tf: %v", err)
	}
	m, err := workspace.Load(ws)
	if err != nil || !m.Hosted.IsLocal() || m.Hosted.Kind != workspace.HostedKindLocal {
		t.Fatalf("manifest = %+v, %v", m, err)
	}
	if res, err := svc.UploadHostedTree(context.Background()); err != nil || res != nil {
		t.Errorf("tree copy of a local workspace = %+v, %v; want nothing", res, err)
	}
}

const hostedTestPublisher = "arn:aws:iam::111122223333:role/clavesa-ui-publisher"

// stubFullWellKnown answers with a document that serves both kinds.
func stubFullWellKnown(t *testing.T) {
	stubWellKnown(t, HostedWellKnown{ReaderPrincipal: hostedTestPrincipal, EventBusARN: hostedTestBus,
		PublisherRoleARN: hostedTestPublisher, DataBucket: "clavesa-ui-data-111122223333", Region: "eu-north-1"}, nil)
}

// TestHostedKindDefault: without --kind, a workspace shared as a cloud
// workspace (remote backend, or a deployed stack) is hosted as cloud
// even when this developer's warehouse setting is local (a fresh clone:
// .clavesa/environment.json is gitignored and defaults to local). Only a
// workspace with neither follows the warehouse setting. The kind is
// recorded in the block.
func TestHostedKindDefault(t *testing.T) {
	localEnv := func(t *testing.T, ws string) {
		t.Helper()
		if err := os.Remove(filepath.Join(ws, ".clavesa", "environment.json")); err != nil {
			t.Fatal(err)
		}
	}
	noBackend := func(t *testing.T, ws string) {
		t.Helper()
		m, _ := workspace.Load(ws)
		m.Backend = nil
		if err := workspace.SaveManifest(ws, m); err != nil {
			t.Fatal(err)
		}
	}
	cases := map[string]struct {
		setup func(t *testing.T, ws string)
		kind  string // explicit kind passed in
		want  string
	}{
		"remote backend, local warehouse": {func(t *testing.T, ws string) { localEnv(t, ws) }, "", "cloud"},
		"deployed stack in local state, local warehouse": {func(t *testing.T, ws string) {
			localEnv(t, ws)
			noBackend(t, ws)
			if err := os.WriteFile(filepath.Join(ws, "terraform.tfstate"), []byte(`{"outputs":{"pipeline_bucket":{"value":"smoke2-clavesa"}}}`), 0o644); err != nil {
				t.Fatal(err)
			}
		}, "", "cloud"},
		"deployed.json, local warehouse": {func(t *testing.T, ws string) {
			localEnv(t, ws)
			noBackend(t, ws)
			if err := os.WriteFile(filepath.Join(ws, workspace.DeployedFile), []byte(`{"version":1,"workspace":{"outputs":{"pipeline_bucket":"smoke2-clavesa"}},"pipelines":{}}`), 0o644); err != nil {
				t.Fatal(err)
			}
		}, "", "cloud"},
		"not deployed, local warehouse": {func(t *testing.T, ws string) { localEnv(t, ws); noBackend(t, ws) }, "", "local"},
		"not deployed, cloud warehouse": {func(t *testing.T, ws string) { noBackend(t, ws) }, "", "cloud"},
		"--kind local on a backend workspace": {func(t *testing.T, ws string) {}, "local", "local"},
		"--kind cloud, not deployed, local warehouse": {func(t *testing.T, ws string) { localEnv(t, ws); noBackend(t, ws) }, "cloud", "cloud"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ws := hostedWorkspace(t)
			c.setup(t, ws)
			stubFullWellKnown(t)
			st, err := New(ws).EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev", Kind: c.kind})
			if err != nil {
				t.Fatal(err)
			}
			m, _ := workspace.Load(ws)
			if st.Kind != c.want || m.Hosted.Kind != c.want {
				t.Errorf("kind = %q, recorded %q; want %q", st.Kind, m.Hosted.Kind, c.want)
			}
			_, err = os.Stat(filepath.Join(ws, workspace.HostedTFFile))
			if hasTF := err == nil; hasTF != (c.want == "cloud") {
				t.Errorf("hosted.tf present = %v for kind %s", hasTF, c.want)
			}
		})
	}
	ws := hostedWorkspace(t)
	stubFullWellKnown(t)
	if _, err := New(ws).EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev", Kind: "hybrid"}); err == nil || !strings.Contains(err.Error(), "kind") {
		t.Errorf("unknown kind: err = %v", err)
	}
}

// TestHostedRecordedKindSticks: once recorded, the kind does not follow a
// later change of the developer's warehouse setting.
func TestHostedRecordedKindSticks(t *testing.T) {
	ws := hostedWorkspace(t)
	stubFullWellKnown(t)
	if _, err := New(ws).EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev", Kind: "cloud"}); err != nil {
		t.Fatal(err)
	}
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseLocal); err != nil {
		t.Fatal(err)
	}
	st, err := New(ws).GetHosted()
	if err != nil || st.Kind != "cloud" {
		t.Errorf("kind after the warehouse change = %q, %v", st.Kind, err)
	}
}

// TestHostedTFLakeFormationDatabases: hosted.tf grants the reader on the
// system database and on the database of every deployed pipeline, read
// from the pipeline state; a pipeline with no state is left out until it
// is deployed, and ResyncHostedTF reports the change.
func TestHostedTFLakeFormationDatabases(t *testing.T) {
	ws := hostedWorkspace(t)
	for _, p := range []string{"orders", "events"} {
		if err := os.MkdirAll(filepath.Join(ws, p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(ws, p, "main.tf"), []byte(summaryPipelineTF), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	deployed := map[string]bool{"taxi": true, "events": true}
	restore := workspace.SetStateGetterForTest(func(_ context.Context, _, _, key string) ([]byte, error) {
		for p := range deployed {
			if strings.HasSuffix(key, "/pipelines/"+p+".tfstate") {
				return []byte(`{"resources":[{"mode":"managed","type":"aws_glue_catalog_database","name":"pipeline","instances":[{"attributes":{"name":"clavesa_smoke2__` + p + `"}}]}]}`), nil
			}
		}
		return nil, nil
	})
	defer restore()
	svc := New(ws)
	stubFullWellKnown(t)
	if _, err := svc.EnableHosted(context.Background(), HostedInput{URL: "https://ui.clavesa.dev", Kind: "cloud"}); err != nil {
		t.Fatal(err)
	}
	tf, _ := os.ReadFile(filepath.Join(ws, workspace.HostedTFFile))
	want := `lf_databases           = concat([module.workspace.system_pipelines_db], ["clavesa_smoke2__events", "clavesa_smoke2__taxi"])`
	if !strings.Contains(string(tf), want) {
		t.Errorf("hosted.tf: want %s in\n%s", want, tf)
	}
	if changed, err := svc.ResyncHostedTF(); err != nil || changed {
		t.Errorf("resync with nothing new = %v, %v", changed, err)
	}
	deployed["orders"] = true
	if changed, err := svc.ResyncHostedTF(); err != nil || !changed {
		t.Errorf("resync after orders deployed = %v, %v", changed, err)
	}
	tf, _ = os.ReadFile(filepath.Join(ws, workspace.HostedTFFile))
	if !strings.Contains(string(tf), `"clavesa_smoke2__events", "clavesa_smoke2__orders", "clavesa_smoke2__taxi"]`) {
		t.Errorf("hosted.tf after orders deployed:\n%s", tf)
	}
	// A state read that fails stops the sync: no grant is dropped silently.
	restoreErr := workspace.SetStateGetterForTest(func(context.Context, string, string, string) ([]byte, error) {
		return nil, errors.New("access denied")
	})
	defer restoreErr()
	if _, err := svc.ResyncHostedTF(); err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Errorf("failing state read: err = %v", err)
	}
}
