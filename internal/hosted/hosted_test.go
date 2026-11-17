package hosted

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/ui"
	"github.com/vesahyp/clavesa/internal/wsdeps"
)

// ---------------------------------------------------------------------------
// The API Gateway v2 adapter
// ---------------------------------------------------------------------------

func TestAdapterRequestAndResponse(t *testing.T) {
	var got *http.Request
	var gotBody string
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		http.SetCookie(w, &http.Cookie{Name: "a", Value: "1"})
		w.Header().Add("X-Multi", "1")
		w.Header().Add("X-Multi", "2")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	ev := events.APIGatewayV2HTTPRequest{
		Version:         "2.0",
		RawPath:         "/w/smoke2/api/pipelines/run%2Fx",
		RawQueryString:  "dir=taxi&run=a%20b",
		Headers:         map[string]string{"authorization": "Bearer t", "host": "ui.clavesa.dev"},
		Cookies:         []string{"c=1", "d=2"},
		Body:            base64.StdEncoding.EncodeToString([]byte(`{"x":1}`)),
		IsBase64Encoded: true,
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: "POST", SourceIP: "1.2.3.4"},
		},
	}
	res, err := ServeAPIGatewayV2(context.Background(), h, ev)
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "POST" || got.URL.Path != "/w/smoke2/api/pipelines/run/x" || got.URL.EscapedPath() != "/w/smoke2/api/pipelines/run%2Fx" {
		t.Errorf("method/path = %s %s (%s)", got.Method, got.URL.Path, got.URL.EscapedPath())
	}
	if got.URL.Query().Get("run") != "a b" || got.URL.Query().Get("dir") != "taxi" {
		t.Errorf("query = %v", got.URL.Query())
	}
	if got.Header.Get("Authorization") != "Bearer t" || got.Host != "ui.clavesa.dev" || got.Header.Get("Cookie") != "c=1; d=2" {
		t.Errorf("headers = %v host %s", got.Header, got.Host)
	}
	if gotBody != `{"x":1}` {
		t.Errorf("body = %q", gotBody)
	}
	if res.StatusCode != http.StatusTeapot || res.Body != `{"ok":true}` || res.IsBase64Encoded {
		t.Errorf("response = %+v", res)
	}
	if res.Headers["Content-Type"] != "application/json" || len(res.MultiValueHeaders["X-Multi"]) != 2 {
		t.Errorf("response headers = %v / %v", res.Headers, res.MultiValueHeaders)
	}
	if len(res.Cookies) != 1 || !strings.HasPrefix(res.Cookies[0], "a=1") {
		t.Errorf("cookies = %v", res.Cookies)
	}

	// A binary body goes back base64 encoded.
	bin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte{0xff, 0xfe, 0x00}) })
	res, _ = ServeAPIGatewayV2(context.Background(), bin, events.APIGatewayV2HTTPRequest{RawPath: "/x"})
	if !res.IsBase64Encoded || res.Body != base64.StdEncoding.EncodeToString([]byte{0xff, 0xfe, 0x00}) {
		t.Errorf("binary response = %+v", res)
	}
}

// ---------------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------------

const smoke2Registry = `[
 {"name":"smoke2","kind":"cloud","account":"699166197771","region":"eu-north-1",
  "reader_role_arn":"arn:aws:iam::699166197771:role/clavesa-ui-reader-smoke2","bucket":"smoke2-clavesa"},
 {"name":"site-traffic","kind":"local","account":"699166197771","region":"eu-north-1","reader_role_arn":"","bucket":""}
]`

func TestParseRegistry(t *testing.T) {
	reg, err := ParseRegistry([]byte(smoke2Registry))
	if err != nil {
		t.Fatal(err)
	}
	if len(reg) != 2 || reg[0].Name != "site-traffic" || reg[1].Name != "smoke2" || reg[1].Bucket != "smoke2-clavesa" {
		t.Errorf("registry = %+v", reg)
	}
	// A local entry is just {name, kind}: it lives on a Mac, not in an
	// account. What hosted/infra/workspaces.tf emits for one: "" fields.
	local, err := ParseRegistry([]byte(`[{"name":"kivikko-traffic","kind":"local"},` +
		`{"name":"rakka-traffic","kind":"local","account":"","region":"","reader_role_arn":"","bucket":""}]`))
	if err != nil || len(local) != 2 || local[0].Kind != KindLocal {
		t.Errorf("local entries: %+v, %v", local, err)
	}
	if reg, err := ParseRegistry(nil); err != nil || len(reg) != 0 {
		t.Errorf("empty: %v %v", reg, err)
	}
	bad := map[string]string{
		"not json":         `{`,
		"bad kind":         `[{"name":"a","kind":"x","account":"699166197771","region":"r"}]`,
		"bad name":         `[{"name":"../a","kind":"local","account":"699166197771","region":"r"}]`,
		"bad account":      `[{"name":"a","kind":"local","account":"1","region":"r"}]`,
		"cloud no region":  `[{"name":"a","kind":"cloud","account":"699166197771","reader_role_arn":"arn:aws:iam::699166197771:role/x","bucket":"b-b"}]`,
		"cloud no account": `[{"name":"a","kind":"cloud","region":"r","reader_role_arn":"arn:aws:iam::699166197771:role/x","bucket":"b-b"}]`,
		"cloud no role":    `[{"name":"a","kind":"cloud","account":"699166197771","region":"r","bucket":"b-b"}]`,
		"cloud no bkt":     `[{"name":"a","kind":"cloud","account":"699166197771","region":"r","reader_role_arn":"arn:aws:iam::699166197771:role/x"}]`,
		"local has role":   `[{"name":"a","kind":"local","account":"699166197771","region":"r","reader_role_arn":"arn:aws:iam::699166197771:role/x"}]`,
		"duplicate":        `[{"name":"a","kind":"local","account":"699166197771","region":"r"},{"name":"a","kind":"local","account":"699166197771","region":"r"}]`,
	}
	for name, in := range bad {
		if _, err := ParseRegistry([]byte(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// ---------------------------------------------------------------------------
// Event → workspace
// ---------------------------------------------------------------------------

func sfnEvent(account, sm, status string) events.CloudWatchEvent {
	d, _ := json.Marshal(map[string]string{
		"executionArn":    strings.Replace(sm, ":stateMachine:", ":execution:", 1) + ":run-1",
		"stateMachineArn": sm,
		"status":          status,
	})
	return events.CloudWatchEvent{ID: "e1", Source: "aws.states", DetailType: "Step Functions Execution Status Change",
		AccountID: account, Time: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC), Detail: d}
}

func TestMapEvent(t *testing.T) {
	reg, _ := ParseRegistry([]byte(smoke2Registry))
	smArn := "arn:aws:states:eu-north-1:699166197771:stateMachine:clavesa-smoke2-taxi"

	got, err := MapEvent(reg, sfnEvent("699166197771", smArn, "SUCCEEDED"), nil)
	if err != nil || got.Workspace != "smoke2" || !got.Terminal() || !strings.HasSuffix(got.ExecutionARN, ":run-1") || got.Time.IsZero() {
		t.Errorf("sfn: %+v %v", got, err)
	}
	if got, _ := MapEvent(reg, sfnEvent("699166197771", smArn, "RUNNING"), nil); got.Terminal() {
		t.Error("RUNNING is not terminal")
	}
	// Legacy names: the only cloud workspace of the account.
	if got, err := MapEvent(reg, sfnEvent("699166197771", "arn:aws:states:eu-north-1:699166197771:stateMachine:clavesa-taxi", "FAILED"), nil); err != nil || got.Workspace != "smoke2" {
		t.Errorf("legacy single: %+v %v", got, err)
	}
	// Two cloud workspaces in one account: the deployed.json lookup decides.
	two := append(append([]Entry{}, reg...), Entry{Name: "other", Kind: KindCloud, Account: "699166197771", Region: "eu-north-1",
		ReaderRoleARN: "arn:aws:iam::699166197771:role/clavesa-ui-reader-other", Bucket: "other-clavesa"})
	legacy := "arn:aws:states:eu-north-1:699166197771:stateMachine:clavesa-orders"
	if _, err := MapEvent(two, sfnEvent("699166197771", legacy, "FAILED"), nil); err == nil {
		t.Error("ambiguous legacy state machine mapped with no lookup")
	}
	known := func(e Entry, arn string) bool { return e.Name == "other" && arn == legacy }
	if got, err := MapEvent(two, sfnEvent("699166197771", legacy, "FAILED"), known); err != nil || got.Workspace != "other" {
		t.Errorf("lookup: %+v %v", got, err)
	}
	// An account with no cloud workspace.
	if _, err := MapEvent(reg, sfnEvent("111122223333", smArn, "SUCCEEDED"), nil); err == nil {
		t.Error("unregistered account mapped")
	}
	// S3 run marker: by bucket.
	d, _ := json.Marshal(map[string]any{"bucket": map[string]string{"name": "smoke2-clavesa"}, "object": map[string]string{"key": "_progress/local-1/_run.json"}})
	s3ev := events.CloudWatchEvent{ID: "e2", Source: "aws.s3", DetailType: "Object Created", AccountID: "699166197771", Detail: d}
	if got, err := MapEvent(reg, s3ev, nil); err != nil || got.Workspace != "smoke2" || got.Terminal() {
		t.Errorf("s3: %+v %v", got, err)
	}
	d, _ = json.Marshal(map[string]any{"bucket": map[string]string{"name": "elsewhere"}})
	s3ev.Detail = d
	if _, err := MapEvent(reg, s3ev, nil); err == nil {
		t.Error("unknown bucket mapped")
	}
	// The daily schedule: everything.
	sched := events.CloudWatchEvent{Source: "aws.events", DetailType: "Scheduled Event", AccountID: "699166197771", Detail: json.RawMessage(`{}`)}
	if got, err := MapEvent(reg, sched, nil); err != nil || !got.All {
		t.Errorf("schedule: %+v %v", got, err)
	}
}

// ---------------------------------------------------------------------------
// The server: read-only always, routing, refresh
// ---------------------------------------------------------------------------

type memStore struct {
	mu   sync.Mutex
	objs map[string][]byte
	puts int
}

func newMemStore() *memStore { return &memStore{objs: map[string][]byte{}} }

func (m *memStore) Get(_ context.Context, name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.objs[name], nil
}
func (m *memStore) Put(_ context.Context, name string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[name] = body
	m.puts++
	return nil
}

type fakeOpener struct {
	ws    *Workspace
	opens int
}

func (f *fakeOpener) Open(context.Context, Entry) (*Workspace, error) {
	f.opens++
	if f.ws == nil {
		return nil, errors.New("no workspace")
	}
	return f.ws, nil
}

func httpEvent(method, path string) json.RawMessage {
	ev := events.APIGatewayV2HTTPRequest{
		Version: "2.0", RawPath: path, Body: `{}`,
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: method, Path: path},
		},
	}
	b, _ := json.Marshal(ev)
	return b
}

func invokeHTTP(t *testing.T, s *Server, method, path string) events.APIGatewayV2HTTPResponse {
	t.Helper()
	out, err := s.Invoke(context.Background(), httpEvent(method, path))
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return out.(events.APIGatewayV2HTTPResponse)
}

// TestLambdaAlwaysReadOnly walks the full route table of one workspace
// (the same table ui/readonly_test.go walks) and sends every non-GET
// route through the Lambda's own entry (an HTTP API event into Invoke)
// at /w/<name>/api/<route>. Each must answer 405 with a JSON error, and
// no write may reach the workspace. The workspace's mux here is a real
// BuildWorkspaceMux built WITHOUT ReadOnly, so the server's own guard is
// what is tested.
func TestLambdaAlwaysReadOnly(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	deps := wsdeps.New(context.Background(), t.TempDir(), wsdeps.Options{})
	wd := ui.WorkspaceDeps{Deps: deps}
	reached := 0
	inner := ui.BuildWorkspaceMux(wd)
	counting := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			reached++
		}
		inner.ServeHTTP(w, r)
	})
	reg, _ := ParseRegistry([]byte(smoke2Registry))
	op := &fakeOpener{ws: &Workspace{Handler: counting}}
	s := NewServer(reg, service.HostedWellKnown{ReaderPrincipal: "arn:aws:iam::699166197771:role/clavesa-ui-api"}, newMemStore(), op)

	writes := 0
	for _, p := range ui.RoutePatterns(wd) {
		method, path, ok := strings.Cut(p, " ")
		if !ok {
			t.Errorf("route %q has no method", p)
			continue
		}
		if method == http.MethodGet || method == http.MethodHead || p == ui.PostReadRoute {
			continue // the one POST read: ADR-034, ui.TestReadOnly_OnePostRead
		}
		writes++
		res := invokeHTTP(t, s, method, "/w/smoke2/api"+wildcard(path))
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s: status %d, want 405", p, res.StatusCode)
			continue
		}
		var body map[string]string
		if json.Unmarshal([]byte(res.Body), &body) != nil || body["error"] == "" {
			t.Errorf("%s: body %q is not a JSON error", p, res.Body)
		}
	}
	if writes < 40 {
		t.Errorf("only %d write routes walked", writes)
	}
	// The server's own paths refuse writes too.
	for _, p := range []string{"/api/workspaces", "/.well-known/clavesa.json", "/api/nope", "/w/smoke2/api/workspace/summary"} {
		if res := invokeHTTP(t, s, http.MethodPost, p); res.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("POST %s: %d, want 405", p, res.StatusCode)
		}
	}
	if reached != 0 || op.opens != 0 {
		t.Errorf("a write reached the workspace (%d) or opened it (%d)", reached, op.opens)
	}
	// A read does reach it.
	if res := invokeHTTP(t, s, http.MethodGet, "/w/smoke2/api/pipelines"); res.StatusCode != http.StatusOK || op.opens != 1 {
		t.Errorf("GET pipelines: %d, opens %d", res.StatusCode, op.opens)
	}
}

func wildcard(p string) string {
	var b strings.Builder
	in := false
	for _, r := range p {
		switch {
		case r == '{':
			in = true
			b.WriteString("x")
		case r == '}':
			in = false
		case !in:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func TestServerRoutes(t *testing.T) {
	reg, _ := ParseRegistry([]byte(smoke2Registry))
	store := newMemStore()
	store.objs["smoke2"] = []byte(`{"name":"smoke2","generated_at":"2026-11-15T12:00:00Z","status":"ok"}`)
	live := 0
	op := &fakeOpener{ws: &Workspace{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		live++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"live":"` + r.URL.Path + `"}`))
	})}}
	wk := service.HostedWellKnown{ReaderPrincipal: "arn:aws:iam::699166197771:role/clavesa-ui-api", DataBucket: "d"}
	s := NewServer(reg, wk, store, op)

	res := invokeHTTP(t, s, http.MethodGet, "/.well-known/clavesa.json")
	var gotWK service.HostedWellKnown
	if res.StatusCode != 200 || json.Unmarshal([]byte(res.Body), &gotWK) != nil || gotWK != wk {
		t.Errorf("well-known: %d %s", res.StatusCode, res.Body)
	}

	res = invokeHTTP(t, s, http.MethodGet, "/api/workspaces")
	var list struct {
		Workspaces []WorkspaceCard `json:"workspaces"`
	}
	if res.StatusCode != 200 || json.Unmarshal([]byte(res.Body), &list) != nil || len(list.Workspaces) != 2 {
		t.Fatalf("workspaces: %d %s", res.StatusCode, res.Body)
	}
	byName := map[string]WorkspaceCard{}
	for _, c := range list.Workspaces {
		byName[c.Name] = c
	}
	if c := byName["smoke2"]; c.APIBase != "/w/smoke2/api" || !strings.Contains(string(c.Summary), `"status":"ok"`) || c.Account != "699166197771" {
		t.Errorf("smoke2 card = %+v", c)
	}
	if c := byName["site-traffic"]; string(c.Summary) != "null" || c.Kind != KindLocal {
		t.Errorf("site-traffic card = %+v", c)
	}
	if op.opens != 0 {
		t.Error("GET /api/workspaces opened a workspace (it must read only the stored summaries)")
	}

	// The stored summary by default, live with ?live=1.
	res = invokeHTTP(t, s, http.MethodGet, "/w/smoke2/api/workspace/summary")
	if !strings.Contains(res.Body, `"status":"ok"`) || live != 0 {
		t.Errorf("stored summary: %s (live %d)", res.Body, live)
	}
	ev := httpEvent(http.MethodGet, "/w/smoke2/api/workspace/summary")
	var raw events.APIGatewayV2HTTPRequest
	_ = json.Unmarshal(ev, &raw)
	raw.RawQueryString = "live=1"
	ev, _ = json.Marshal(raw)
	out, _ := s.Invoke(context.Background(), ev)
	if r := out.(events.APIGatewayV2HTTPResponse); !strings.Contains(r.Body, `"live":"/workspace/summary"`) {
		t.Errorf("live summary: %s", r.Body)
	}

	// Unknown paths and workspaces: JSON 404, never HTML.
	op.ws.Handler = http.NewServeMux() // matches nothing: the ServeMux's own 404
	for _, p := range []string{"/api/nope", "/w/nope/api/pipelines", "/index.html", "/w/smoke2/pipelines", "/w/smoke2/api/nope"} {
		res := invokeHTTP(t, s, http.MethodGet, p)
		var body map[string]string
		if res.StatusCode != 404 || json.Unmarshal([]byte(res.Body), &body) != nil || body["error"] == "" {
			t.Errorf("GET %s: %d %q, want a JSON 404", p, res.StatusCode, res.Body)
		}
	}
	// A local workspace has no live API.
	if res := invokeHTTP(t, s, http.MethodGet, "/w/site-traffic/api/pipelines"); res.StatusCode != 404 {
		t.Errorf("local workspace live read: %d", res.StatusCode)
	}
}

type fakeStats struct {
	body []byte
	err  error
}

func (f fakeStats) GetStats(context.Context) ([]byte, error) { return f.body, f.err }

// TestServerStats: GET /api/stats answers stats/ui.json as stored, [] with
// none stored (or no reader), a JSON 502 on a read error, and refuses a
// write like every other route. It never opens a workspace.
func TestServerStats(t *testing.T) {
	reg, _ := ParseRegistry([]byte(smoke2Registry))
	op := &fakeOpener{}
	s := NewServer(reg, service.HostedWellKnown{}, newMemStore(), op)

	if res := invokeHTTP(t, s, http.MethodGet, "/api/stats"); res.StatusCode != 200 || res.Body != "[]" {
		t.Errorf("no reader: %d %q, want 200 []", res.StatusCode, res.Body)
	}
	s.Stats = fakeStats{}
	if res := invokeHTTP(t, s, http.MethodGet, "/api/stats"); res.StatusCode != 200 || res.Body != "[]" {
		t.Errorf("none stored: %d %q, want 200 []", res.StatusCode, res.Body)
	}
	stored := `[{"date":"2026-11-15","sessions":2,"pageviews":5,"errors":1}]`
	s.Stats = fakeStats{body: []byte(stored)}
	res := invokeHTTP(t, s, http.MethodGet, "/api/stats")
	if res.StatusCode != 200 || res.Body != stored || res.Headers["Content-Type"] != "application/json" {
		t.Errorf("stored: %d %q %v", res.StatusCode, res.Body, res.Headers)
	}
	s.Stats = fakeStats{err: errors.New("AccessDenied")}
	res = invokeHTTP(t, s, http.MethodGet, "/api/stats")
	var body map[string]string
	if res.StatusCode != http.StatusBadGateway || json.Unmarshal([]byte(res.Body), &body) != nil || body["error"] == "" {
		t.Errorf("read error: %d %q, want a JSON 502", res.StatusCode, res.Body)
	}
	if res := invokeHTTP(t, s, http.MethodPost, "/api/stats"); res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /api/stats: %d, want 405", res.StatusCode)
	}
	if op.opens != 0 {
		t.Error("GET /api/stats opened a workspace")
	}
}

func TestRefresh(t *testing.T) {
	reg, _ := ParseRegistry([]byte(smoke2Registry))
	store := newMemStore()
	calls := 0
	runIn := 3 // the run shows up in the third summary
	op := &fakeOpener{ws: &Workspace{Summarize: func(context.Context) (*service.WorkspaceSummary, error) {
		calls++
		sum := &service.WorkspaceSummary{Name: "smoke2", Status: "ok", GeneratedAt: time.Now().UTC().Format(time.RFC3339),
			Pipelines: []service.PipelineSummary{{Name: "taxi"}}}
		if calls >= runIn {
			sum.Pipelines[0].Runs = []service.SummaryRun{{RunID: "run-1", Result: "succeeded"}}
		}
		return sum, nil
	}}}
	s := NewServer(reg, service.HostedWellKnown{}, store, op)
	s.RetryWait = time.Millisecond
	var logs []string
	s.Logf = func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) }

	smArn := "arn:aws:states:eu-north-1:699166197771:stateMachine:clavesa-smoke2-taxi"
	ev := sfnEvent("699166197771", smArn, "SUCCEEDED")
	ev.Time = time.Now().Add(-time.Minute)
	raw, _ := json.Marshal(ev)
	if _, err := s.Invoke(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || store.puts != 1 || !strings.Contains(string(store.objs["smoke2"]), "run-1") {
		t.Errorf("terminal event: %d summaries, %d puts, stored %s", calls, store.puts, store.objs["smoke2"])
	}

	// Debounce: the stored summary is newer than this event.
	if _, err := s.Invoke(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || store.puts != 1 {
		t.Errorf("debounced event recomputed: %d summaries, %d puts", calls, store.puts)
	}

	// The daily schedule refreshes every cloud workspace, not debounced.
	sched, _ := json.Marshal(events.CloudWatchEvent{Source: "aws.events", DetailType: "Scheduled Event", AccountID: "699166197771", Time: time.Now()})
	if _, err := s.Invoke(context.Background(), sched); err != nil {
		t.Fatal(err)
	}
	if calls != 4 || store.puts != 2 {
		t.Errorf("schedule: %d summaries, %d puts", calls, store.puts)
	}

	// A failure is logged with the alarm's marker and fails the call.
	op.ws = nil
	logs = nil
	if _, err := s.Invoke(context.Background(), sched); err == nil {
		t.Error("a failed refresh must fail the invocation")
	}
	if len(logs) == 0 || !strings.HasPrefix(logs[0], RefreshFailedMarker) {
		t.Errorf("logs = %v, want a %s line", logs, RefreshFailedMarker)
	}

	// An event of no registered workspace fails too.
	other, _ := json.Marshal(sfnEvent("111122223333", smArn, "SUCCEEDED"))
	if _, err := s.Invoke(context.Background(), other); err == nil {
		t.Error("an unmapped event must fail")
	}
	if _, err := s.Invoke(context.Background(), json.RawMessage(`{"hello":1}`)); err == nil {
		t.Error("an unknown event shape must fail")
	}
}
