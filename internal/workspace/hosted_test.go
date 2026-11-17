package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testPrincipal = "arn:aws:iam::111122223333:role/clavesa-ui-hosted"

func TestHostedValidate(t *testing.T) {
	ok := Hosted{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{testPrincipal}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid block: %v", err)
	}
	sess := ok
	sess.ReaderPrincipals = []string{"arn:aws:sts::111122223333:assumed-role/admin/vesa"}
	if err := sess.Validate(); err != nil {
		t.Errorf("assumed-role principal: %v", err)
	}
	bus := ok
	bus.EventBusARN = "arn:aws:events:eu-north-1:111122223333:event-bus/clavesa-ui"
	if err := bus.Validate(); err != nil {
		t.Errorf("event bus: %v", err)
	}
	for name, h := range map[string]Hosted{
		"http url":      {URL: "http://ui.clavesa.dev", ReaderPrincipals: []string{testPrincipal}},
		"url with path": {URL: "https://ui.clavesa.dev/x", ReaderPrincipals: []string{testPrincipal}},
		"no principal":  {URL: "https://ui.clavesa.dev"},
		"bad principal": {URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{"vesa"}},
		"bad bus":       {URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{testPrincipal}, EventBusARN: "clavesa-ui"},
	} {
		if err := h.Validate(); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if got, err := NormalizeHostedURL("http://127.0.0.1:8123"); err != nil || got != "http://127.0.0.1:8123" {
		t.Errorf("loopback http: %q %v", got, err)
	}
	if _, err := NormalizeHostedURL("http://ui.clavesa.dev"); err == nil {
		t.Error("http to a remote host must be refused")
	}
	if got, _ := NormalizeHostedURL("https://ui.clavesa.dev/"); got != "https://ui.clavesa.dev" {
		t.Errorf("normalized = %q", got)
	}
}

func TestHostedNames(t *testing.T) {
	if got := HostedReaderRoleName("smoke2"); got != "clavesa-ui-reader-smoke2" {
		t.Errorf("role = %q", got)
	}
	if got := HostedWorkGroupName("smoke2"); got != "clavesa-ui-smoke2" {
		t.Errorf("workgroup = %q", got)
	}
	long := strings.Repeat("w", 70)
	r, w := HostedReaderRoleName(long), HostedWorkGroupName(long)
	if len(r) != 60 || len(w) != 55 || !strings.HasPrefix(r, "clavesa-ui-reader-") {
		t.Errorf("long names: %q (%d) %q (%d)", r, len(r), w, len(w))
	}
	if r == HostedReaderRoleName(long+"x") {
		t.Errorf("cut names must stay distinct")
	}
}

// TestHostedResourcePatterns: the prefix pattern covers every name
// PipelineAWSName gives in workspace naming, cut or not.
func TestHostedResourcePatterns(t *testing.T) {
	m := &Manifest{Name: "acme", AWSNames: AWSNamesWorkspace}
	sms, logs := HostedResourcePatterns(m, []string{"orders"})
	if strings.Join(sms, ",") != "clavesa-acme-*" || strings.Join(logs, ",") != "/aws/lambda/clavesa-acme-*,/clavesa/clavesa-acme-*" {
		t.Errorf("workspace patterns = %v %v", sms, logs)
	}
	for _, ws := range []string{"acme", strings.Repeat("a", 22), strings.Repeat("b", 40)} {
		m := &Manifest{Name: ws, AWSNames: AWSNamesWorkspace}
		sms, _ := HostedResourcePatterns(m, nil)
		prefix := strings.TrimSuffix(sms[0], "*")
		for _, p := range []string{"o", strings.Repeat("p", 60)} {
			if n := PipelineAWSName(m, p); !strings.HasPrefix(n, prefix) {
				t.Errorf("workspace %q pipeline %q: %q does not start with %q", ws, p, n, prefix)
			}
		}
	}
	sms, logs = HostedResourcePatterns(&Manifest{Name: "acme"}, []string{"orders", "events"})
	if strings.Join(sms, ",") != "clavesa-events,clavesa-orders" {
		t.Errorf("legacy state machines = %v", sms)
	}
	if strings.Join(logs, ",") != "/aws/lambda/clavesa-events-*,/clavesa/events/sfn,/aws/lambda/clavesa-orders-*,/clavesa/orders/sfn" {
		t.Errorf("legacy log groups = %v", logs)
	}
}

// TestRenderHostedTFGolden pins the emitted hosted.tf (UPDATE_GOLDEN=1
// rewrites the files under testdata/).
func TestRenderHostedTFGolden(t *testing.T) {
	cases := map[string]*Manifest{
		// smoke2's shape: workspace naming, S3 backend in its own bucket, no bus.
		"hosted_workspace_names": {
			Name: "smoke2", Catalog: "clavesa_smoke2", SystemCatalog: "clavesa_smoke2_system", AWSNames: AWSNamesWorkspace,
			Backend: &Backend{Type: "s3", Bucket: "clavesa-smoke-tfstate-111122223333", Region: "eu-north-1", KeyPrefix: "tfstate/clavesa-smoke/"},
			Hosted:  &Hosted{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{testPrincipal}},
		},
		// Legacy names, state in the pipeline bucket, forwarding to a bus.
		"hosted_legacy_bus": {
			Name: "acme", Catalog: "clavesa_acme", SystemCatalog: "clavesa_acme_system",
			Backend: &Backend{Type: "s3", Bucket: "acme-clavesa", Region: "eu-north-1"},
			Hosted: &Hosted{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{testPrincipal},
				EventBusARN: "arn:aws:events:eu-north-1:111122223333:event-bus/clavesa-ui"},
		},
		// Personal data (web-traffic): the raw pipelines are denied.
		"hosted_personal_data": {
			Name: "acme", Catalog: "clavesa_acme", SystemCatalog: "clavesa_acme_system",
			Backend: &Backend{Type: "s3", Bucket: "acme-clavesa", Region: "eu-north-1"},
			Hosted:  &Hosted{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{testPrincipal}, PersonalData: ptrTrue()},
		},
	}
	// The raw data is passed even where PersonalData is off: only the
	// personal_data case may emit it.
	raw := HostedRaw{ObjectPatterns: []string{"silver/*", "bronze/*"}, GlueDatabases: []string{"clavesa_acme__silver", "clavesa_acme__bronze"}}
	for name, m := range cases {
		// events is deployed, orders not yet; the system DB is never listed twice.
		got := RenderHostedTF(m, "v9.9.9", []string{"orders", "events"}, []string{m.Catalog + "__events", m.SystemCatalog + "__pipelines", m.Catalog + "__events"}, raw)
		path := filepath.Join("testdata", name+".golden")
		if os.Getenv("UPDATE_GOLDEN") == "1" {
			_ = os.MkdirAll("testdata", 0o755)
			_ = os.WriteFile(path, []byte(got), 0o644)
		}
		want, _ := os.ReadFile(path)
		if got != string(want) {
			t.Errorf("%s differs from %s (UPDATE_GOLDEN=1 rewrites):\n%s", name, path, got)
		}
	}
}

func TestSyncHostedTF(t *testing.T) {
	root := t.TempDir()
	m := &Manifest{Name: "ws", Catalog: "clavesa_ws", SystemCatalog: "clavesa_ws_system",
		Hosted: &Hosted{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{testPrincipal}}}
	if err := SyncHostedTF(root, m, "v1.0.0", nil, nil, HostedRaw{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, HostedTFFile))
	if err != nil || !strings.Contains(string(data), `./.clavesa/modules/v1.0.0/hosted-reader/aws`) {
		t.Fatalf("hosted.tf = %q, %v", data, err)
	}
	if err := SyncHostedTF(root, m, "", nil, nil, HostedRaw{}); err == nil {
		t.Error("no module version must be refused")
	}
	// Hosted off: the file goes, twice is fine.
	m.Hosted = nil
	for i := 0; i < 2; i++ {
		if err := SyncHostedTF(root, m, "", nil, nil, HostedRaw{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, HostedTFFile)); !os.IsNotExist(err) {
		t.Errorf("hosted.tf still there: %v", err)
	}
}

// TestReadStackStateFromDeployed: a hosted tree copy with deployed.json
// and a backend-configured clavesa.json answers every state read from
// deployed.json and never reaches for the state bucket.
func TestReadStackStateFromDeployed(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("clavesa.json", `{"name":"smoke2","cloud":"aws","version":1,"catalog":"clavesa_smoke2","system_catalog":"clavesa_smoke2_system",
"backend":{"type":"s3","bucket":"state-bucket","region":"eu-north-1"}}`)
	write("deployed.json", `{"version":1,"account":"111122223333","region":"eu-north-1",
"workspace":{"outputs":{"pipeline_bucket":"smoke2-clavesa","hosted_workgroup":"clavesa-ui-smoke2"}},
"pipelines":{"taxi":{"state_machine_arn":"arn:aws:states:eu-north-1:111122223333:stateMachine:clavesa-smoke2-taxi"}}}`)
	restore := SetStateGetterForTest(func(context.Context, string, string, string) ([]byte, error) {
		return nil, errors.New("the state bucket must not be read")
	})
	defer restore()

	if got := PipelineBucket(root); got != "smoke2-clavesa" {
		t.Errorf("PipelineBucket = %q", got)
	}
	data, err := ReadStackState(context.Background(), root, filepath.Join(root, "taxi"))
	if err != nil {
		t.Fatal(err)
	}
	if got := StateMachineARNFromState(data); got != "arn:aws:states:eu-north-1:111122223333:stateMachine:clavesa-smoke2-taxi" {
		t.Errorf("state machine ARN = %q", got)
	}
	if data, err := ReadStackState(context.Background(), root, filepath.Join(root, "other")); err != nil || data != nil {
		t.Errorf("unknown pipeline = %q, %v; want not deployed", data, err)
	}
	if got := HostedAthenaWorkGroup(root); got != "clavesa-ui-smoke2" {
		t.Errorf("workgroup = %q", got)
	}
	// A workspace on disk (no deployed.json) has no hosted workgroup.
	if got := HostedAthenaWorkGroup(t.TempDir()); got != "" {
		t.Errorf("plain workspace workgroup = %q", got)
	}
}

// TestHostedKind: the recorded kind decides IsLocal; a block written
// before the field falls back to the publisher-role rule.
func TestHostedKind(t *testing.T) {
	const pub = "arn:aws:iam::111122223333:role/clavesa-ui-publisher"
	for name, c := range map[string]struct {
		h     Hosted
		local bool
	}{
		"legacy cloud":           {Hosted{URL: "https://ui.clavesa.dev", ReaderPrincipals: []string{testPrincipal}}, false},
		"legacy local":           {Hosted{URL: "https://ui.clavesa.dev", PublisherRoleARN: pub, DataBucket: "b"}, true},
		"recorded cloud":         {Hosted{URL: "https://ui.clavesa.dev", Kind: HostedKindCloud, ReaderPrincipals: []string{testPrincipal}}, false},
		"recorded local":         {Hosted{URL: "https://ui.clavesa.dev", Kind: HostedKindLocal, PublisherRoleARN: pub, DataBucket: "b"}, true},
		"cloud with a publisher": {Hosted{URL: "https://ui.clavesa.dev", Kind: HostedKindCloud, ReaderPrincipals: []string{testPrincipal}, PublisherRoleARN: pub}, false},
	} {
		if got := c.h.IsLocal(); got != c.local {
			t.Errorf("%s: IsLocal = %v, want %v", name, got, c.local)
		}
		if err := c.h.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, h := range map[string]Hosted{
		"unknown kind":             {URL: "https://ui.clavesa.dev", Kind: "hybrid", ReaderPrincipals: []string{testPrincipal}},
		"cloud without principals": {URL: "https://ui.clavesa.dev", Kind: HostedKindCloud, PublisherRoleARN: pub, DataBucket: "b"},
		"local without publisher":  {URL: "https://ui.clavesa.dev", Kind: HostedKindLocal, ReaderPrincipals: []string{testPrincipal}},
		"local with principals":    {URL: "https://ui.clavesa.dev", Kind: HostedKindLocal, PublisherRoleARN: pub, DataBucket: "b", ReaderPrincipals: []string{testPrincipal}},
	} {
		if err := h.Validate(); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestGlueDatabaseFromState(t *testing.T) {
	state := `{"resources":[
{"mode":"data","type":"aws_glue_catalog_database","name":"pipeline","instances":[{"attributes":{"name":"wrong"}}]},
{"mode":"managed","type":"aws_glue_catalog_database","name":"pipeline","instances":[{"attributes":{"name":"clavesa_acme__orders"}}]}]}`
	if got := GlueDatabaseFromState([]byte(state)); got != "clavesa_acme__orders" {
		t.Errorf("database = %q", got)
	}
	for _, s := range []string{"", "{}", `{"resources":[{"mode":"managed","type":"aws_sfn_state_machine","name":"pipeline","instances":[{"attributes":{"name":"x"}}]}]}`} {
		if got := GlueDatabaseFromState([]byte(s)); got != "" {
			t.Errorf("%q: database = %q, want none", s, got)
		}
	}
}

func ptrTrue() *bool { v := true; return &v }
