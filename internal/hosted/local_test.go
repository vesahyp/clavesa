package hosted

import (
	"context"

	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pricing"
	"github.com/vesahyp/clavesa/internal/runtiming"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/ui"
	"github.com/vesahyp/clavesa/internal/workspace"
	"github.com/vesahyp/clavesa/internal/wsdeps"
)

const localPipelineTF = `variable "pipeline_name" { type = string default = "orders" }
module "events" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  compute       = "lambda"
  language      = "sparksql"
  sql           = "SELECT 1"
}
module "rollup" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  compute       = "lambda"
  language      = "sparksql"
  sql           = "SELECT 2"
  inputs        = { events = module.events.outputs.default }
}
`

// localFixture lays out a local-warehouse workspace the way `pipeline
// run` leaves it: one pipeline `orders`, a run marker, per-node markers
// and a timing record per run, and the bundle log of the failed run.
// Runs: 12 nightly runs, the newest failed.
func localFixture(t *testing.T, now time.Time) string {
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
	write("clavesa.json", `{"name":"local-ws","cloud":"aws","version":1,"catalog":"clavesa_local_ws","system_catalog":"clavesa_local_ws_system"}`+"\n")
	write("orders/main.tf", localPipelineTF)
	write("orders/variables.tf", "variable \"trigger_schedule\" {\n  type    = string\n  default = \"rate(1 day)\"\n}\n")
	write(".env", "SECRET=1\n")
	write("orders/terraform.tfstate", `{"secret":"state"}`)

	store := observability.NewFileProgressStore(workspace.LocalWarehouseDir(ws))
	ctx := context.Background()
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("run%02d", i)
		start := now.Add(-time.Duration(i)*24*time.Hour - 2*time.Hour)
		end := start.Add(3 * time.Minute)
		status, failed := "SUCCEEDED", ""
		if i == 0 {
			status, failed = "FAILED", "rollup"
		}
		st, en := start.UnixMilli(), end.UnixMilli()
		if err := observability.WriteRunMarker(ctx, store, id, observability.RunMarker{
			Status: status, Trigger: "cron", StartedMs: &st, EndedMs: &en,
			FailedStep: failed, ErrorMsg: map[bool]string{true: "boom"}[failed != ""], Pipeline: "orders",
		}); err != nil {
			t.Fatal(err)
		}
		mid := start.Add(time.Minute).UnixMilli()
		write(filepath.Join(".clavesa/warehouse/_progress", id, "events.json"),
			fmt.Sprintf(`{"status":"succeeded","started_ms":%d,"ended_ms":%d,"output_rows":%d}`, st, mid, 100+i))
		rollup := fmt.Sprintf(`{"status":"succeeded","started_ms":%d,"ended_ms":%d,"output_rows":7}`, mid, en)
		if failed != "" {
			rollup = fmt.Sprintf(`{"status":"failed","started_ms":%d,"ended_ms":%d,"error":"boom"}`, mid, en)
		}
		write(filepath.Join(".clavesa/warehouse/_progress", id, "rollup.json"), rollup)
		rec := runtiming.Record{Schema: 1, RunID: id, Workspace: "local-ws", WorkspaceDir: ws, Pipeline: "orders",
			Status: status, Trigger: "cron", Compute: "local", StartedAt: start.Format(time.RFC3339), EndedAt: end.Format(time.RFC3339),
			TotalMs: end.Sub(start).Milliseconds(), Nodes: []runtiming.Node{{Node: "events", Status: "ok"}}}
		b, _ := json.Marshal(rec)
		write(filepath.Join(".clavesa/warehouse/_progress", id, runtiming.FileName), string(b))
	}
	var log strings.Builder
	for i := 0; i < 80; i++ {
		fmt.Fprintf(&log, "%s\tline %d\n", now.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano), i)
	}
	write(filepath.Join("orders/.clavesa/runs/run00/_bundle.log"), log.String())
	return ws
}

// localSummaryBody publishes the fixture's summary as the Mac does: the
// summary and its drill-down, through the real local provider.
func localSummaryBody(t *testing.T, ws string, now time.Time) []byte {
	t.Helper()
	deps := wsdeps.New(context.Background(), ws, wsdeps.Options{})
	t.Cleanup(deps.Close)
	sum, err := deps.Svc.WorkspaceSummary(context.Background(), service.WorkspaceSummaryQuery{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	sum.Drill = deps.Svc.SummaryDrill(context.Background(), sum)
	body, err := json.Marshal(sum)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// shape is the JSON's structure: every object key path with the kind of
// its value, arrays reduced to the union of their elements' shapes.
func shape(v any, path string, out map[string]string) {
	switch x := v.(type) {
	case map[string]any:
		out[path] = "object"
		for k, c := range x {
			// Map-valued fields keyed by data (states by node name) are
			// one shape whatever their keys.
			key := k
			if strings.HasSuffix(path, ".states") {
				key = "*"
			}
			shape(c, path+"."+key, out)
		}
	case []any:
		out[path] = "array"
		for _, c := range x {
			shape(c, path+"[]", out)
		}
	case nil:
		// null carries no shape of its own (omitempty pointers)
	case float64:
		out[path] = "number"
	case string:
		out[path] = "string"
	case bool:
		out[path] = "bool"
	}
}

func getJSON(t *testing.T, h http.Handler, path string) (int, any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	var v any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("GET %s: %d, not JSON: %s", path, rec.Code, rec.Body.String())
	}
	return rec.Code, v
}

// TestLocalSummaryBackedParity: the routes the hosted Pipeline and Run
// pages read, answered once by the live local workspace (the real
// BuildWorkspaceMux over the real local provider) and once by the
// summary-backed workspace the Lambda builds from the published file.
// Both answer 200 with the same JSON shape (ADR-014), and the rows the
// summary keeps are the live rows.
func TestLocalSummaryBackedParity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ws := localFixture(t, now)
	body := localSummaryBody(t, ws, now)

	liveDeps := wsdeps.New(context.Background(), ws, wsdeps.Options{})
	t.Cleanup(liveDeps.Close)
	live := ui.BuildWorkspaceMux(ui.WorkspaceDeps{Deps: liveDeps, ReadOnly: true})

	hostedWS, deps, err := OpenLocal(context.Background(), filepath.Join(t.TempDir(), "local-ws"), Entry{Name: "local-ws", Kind: KindLocal}, body)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(deps.Close)
	hosted := hostedWS.Handler

	routes := []string{
		"/workspace",
		"/workspace/environment",
		"/workspace/hosted",
		"/runtime/identity",
		"/pipelines",
		"/pipeline?dir=orders",
		"/pipeline/module-version?dir=orders",
		"/pipeline/status?dir=orders",
		"/pipeline/execution/states?dir=orders",
		"/pipeline/execution/states?dir=orders&run=run03",
		"/pipeline/execution/logs?dir=orders&run=run00&step=rollup",
		"/data/runs?pipeline=orders&limit=50&dir=orders",
		"/data/runs?pipeline=orders&limit=200&dir=orders",
		"/data/node-runs?pipeline=orders&limit=200&dir=orders",
		"/data/run-timings?dir=orders&limit=20",
		// The live local answer of these two reads node_runs through the
		// Spark runner (docker), which a unit test has none of: the
		// summary-backed answer is compared with the live answer of a
		// route that encodes the same type (node rows), or with the type
		// itself (the cost rollup).
		"/data/node-runs?pipeline=orders&limit=200&dir=orders&arn=run00|/data/node-runs?pipeline=orders&limit=200&dir=orders",
		"/data/pipeline-cost?dir=orders|cost",
	}
	for _, rt := range routes {
		rt, liveRT, _ := strings.Cut(rt, "|")
		if liveRT == "" {
			liveRT = rt
		}
		var lc int
		var lv any
		if liveRT == "cost" {
			b, _ := json.Marshal(pricing.AggregateCost("orders", []observability.NodeRun{{Node: "n", ComputeTarget: "local", Status: "ok", DurationMs: ptr(int64(1000)), OutputRows: ptr(int64(5))}}))
			_ = json.Unmarshal(b, &lv)
			lc = http.StatusOK
		} else {
			lc, lv = getJSON(t, live, liveRT)
		}
		hc, hv := getJSON(t, hosted, rt)
		if lc != http.StatusOK || hc != http.StatusOK {
			t.Errorf("GET %s: live %d, summary-backed %d (%v)", rt, lc, hc, hv)
			continue
		}
		ls, hs := map[string]string{}, map[string]string{}
		shape(lv, "", ls)
		shape(hv, "", hs)
		// Fields only one side has, ignoring the ones that name the Mac
		// or the hosted copy (root, workspace_dir and the like are
		// strings on both sides when present).
		var diff []string
		for k, v := range ls {
			if hs[k] != v {
				diff = append(diff, fmt.Sprintf("live %s=%s summary=%q", k, v, hs[k]))
			}
		}
		for k, v := range hs {
			if _, ok := ls[k]; !ok {
				diff = append(diff, fmt.Sprintf("summary-only %s=%s", k, v))
			}
		}
		sort.Strings(diff)
		allowed := func(d string) bool {
			// The summary-backed /workspace says summary_only.
			return strings.Contains(d, ".summary_only")
		}
		for _, d := range diff {
			if !allowed(d) {
				t.Errorf("GET %s: shape differs: %s", rt, d)
			}
		}
	}

	// The kept rows are the live rows: the newest runs, the node rows of
	// the newest DrillNodeRuns runs, the failed run's log tail.
	_, lr := getJSON(t, live, "/data/runs?pipeline=orders&limit=200&dir=orders")
	_, hr := getJSON(t, hosted, "/data/runs?pipeline=orders&limit=200&dir=orders")
	if n := len(hr.(map[string]any)["rows"].([]any)); n != 12 || n != len(lr.(map[string]any)["rows"].([]any)) {
		t.Errorf("runs: summary-backed %d rows, live %d", n, len(lr.(map[string]any)["rows"].([]any)))
	}
	_, hn := getJSON(t, hosted, "/data/node-runs?pipeline=orders&limit=200&dir=orders")
	if n := len(hn.(map[string]any)["rows"].([]any)); n != 2*service.DrillNodeRuns {
		t.Errorf("node rows kept = %d, want %d (2 nodes x %d runs)", n, 2*service.DrillNodeRuns, service.DrillNodeRuns)
	}
	_, hs := getJSON(t, hosted, "/pipeline/execution/states?dir=orders&run=run00")
	st := hs.(map[string]any)
	if st["status"] != "FAILED" || st["states"].(map[string]any)["rollup"].(map[string]any)["status"] != "FAILED" {
		t.Errorf("states of the failed run = %v", st)
	}
	_, hl := getJSON(t, hosted, "/pipeline/execution/logs?dir=orders&run=run00&step=rollup")
	ev := hl.(map[string]any)["events"].([]any)
	if len(ev) != service.DrillLogLines || !strings.Contains(fmt.Sprint(ev[len(ev)-1]), "line 79") {
		t.Errorf("log tail: %d lines, last %v", len(ev), ev[len(ev)-1])
	}
	if strings.Contains(fmt.Sprint(hl), ws) {
		t.Errorf("the log answer names the Mac's path: %v", hl)
	}
	_, ht := getJSON(t, hosted, "/data/run-timings?dir=orders&limit=20")
	if n := len(ht.(map[string]any)["runs"].([]any)); n != 12 {
		t.Errorf("timing records = %d, want 12", n)
	}
	_, hw := getJSON(t, hosted, "/workspace")
	if hw.(map[string]any)["summary_only"] != true {
		t.Errorf("/workspace = %v, want summary_only", hw)
	}

	// The published file names no path on the Mac and holds no secret.
	for _, bad := range []string{ws, "SECRET", `"secret"`} {
		if strings.Contains(string(body), bad) {
			t.Errorf("the published summary holds %q", bad)
		}
	}
	if len(body) > 300<<10 {
		t.Errorf("summary is %d bytes, over 300 KB", len(body))
	}

	// The reads the summary does not keep: a JSON 404 that says so.
	for _, rt := range []string{
		"/data/tables-state?pipeline=orders&limit=50&dir=orders",
		"/backfills?dir=orders",
		"/workspace/tables",
		"/pipeline/lineage?dir=orders",
		"/data/table?dir=orders",
		"/pipeline/execution?dir=orders&run=run00",
	} {
		code, v := getJSON(t, hosted, rt)
		if code != http.StatusNotFound || !strings.Contains(fmt.Sprint(v), "not kept for local workspaces") {
			t.Errorf("GET %s: %d %v, want a 404 that says it is not kept", rt, code, v)
		}
	}
}

// TestLocalWorkspaceThroughServer: the Lambda serves a local workspace
// from its stored summary alone, rebuilds it when a newer summary is
// published, answers 404 before one is, and marks a summary older than
// the expected interval plus grace as stale on GET /api/workspaces.
func TestLocalWorkspaceThroughServer(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ws := localFixture(t, now)
	body := localSummaryBody(t, ws, now)

	store := newMemStore()
	reg, err := ParseRegistry([]byte(`[{"name":"local-ws","kind":"local"}]`))
	if err != nil {
		t.Fatal(err)
	}
	op := &Workspaces{Root: t.TempDir(), Store: store, cache: map[string]*openWorkspace{}}
	s := NewServer(reg, service.HostedWellKnown{}, store, op)
	s.Logf = t.Logf

	if r := getHosted(t, s, "/w/local-ws/api/data/runs?pipeline=orders&dir=orders"); r.StatusCode != 404 || !strings.Contains(r.Body, "no summary") {
		t.Errorf("before a publish: %d %s", r.StatusCode, r.Body)
	}
	if r := getHosted(t, s, "/w/local-ws/api/workspace/summary?live=1"); r.StatusCode != 404 {
		t.Errorf("live summary of a local workspace before a publish: %d %s", r.StatusCode, r.Body)
	}
	_ = store.Put(context.Background(), "local-ws", body)
	r := getHosted(t, s, "/w/local-ws/api/data/runs?pipeline=orders&dir=orders&limit=5")
	if r.StatusCode != 200 || strings.Count(r.Body, `"run_id"`) != 5 {
		t.Errorf("runs from the stored summary: %d %s", r.StatusCode, r.Body)
	}
	if r := getHosted(t, s, "/w/local-ws/api/workspace/summary?live=1"); r.StatusCode != 200 || !strings.Contains(r.Body, `"local-ws"`) {
		t.Errorf("?live=1 on a local workspace reads the stored summary: %d", r.StatusCode)
	}
	if r := getHosted(t, s, "/w/local-ws/api/data/tables-state?pipeline=orders&dir=orders"); r.StatusCode != 404 {
		t.Errorf("tables-state: %d %s", r.StatusCode, r.Body)
	}

	// A summary with no drill (an older clavesa published it): 404.
	_ = store.Put(context.Background(), "local-ws", []byte(`{"name":"local-ws","kind":"local","generated_at":"`+now.Format(time.RFC3339)+`","pipelines":[]}`))
	op.cache["local-ws"].checked = time.Time{}
	if r := getHosted(t, s, "/w/local-ws/api/data/runs?pipeline=orders&dir=orders"); r.StatusCode != 404 || !strings.Contains(r.Body, "no drill-down") {
		t.Errorf("no drill: %d %s", r.StatusCode, r.Body)
	}

	// Staleness on the card: the summary was generated at `now`; the
	// pipeline runs daily, so it is overdue after 30 hours.
	_ = store.Put(context.Background(), "local-ws", body)
	for _, tc := range []struct {
		after time.Duration
		stale bool
	}{{29 * time.Hour, false}, {31 * time.Hour, true}} {
		s.Now = func() time.Time { return now.Add(tc.after) }
		r := getHosted(t, s, "/api/workspaces")
		var got struct {
			Workspaces []WorkspaceCard `json:"workspaces"`
		}
		if err := json.Unmarshal([]byte(r.Body), &got); err != nil || len(got.Workspaces) != 1 {
			t.Fatalf("workspaces: %v %s", err, r.Body)
		}
		c := got.Workspaces[0]
		if c.SummaryStale != tc.stale || c.SummaryIntervalSeconds != 86400 || c.Kind != KindLocal {
			t.Errorf("%v after the summary: card %+v, want stale=%v", tc.after, c, tc.stale)
		}
	}
}

func TestLocalSummaryStale(t *testing.T) {
	gen := time.Date(2026, 11, 15, 8, 0, 0, 0, time.UTC)
	sum := func(intervals ...int64) []byte {
		var ps []string
		for _, i := range intervals {
			ps = append(ps, fmt.Sprintf(`{"expected_interval_seconds":%d}`, i))
		}
		return []byte(`{"generated_at":"` + gen.Format(time.RFC3339) + `","pipelines":[` + strings.Join(ps, ",") + `]}`)
	}
	for _, tc := range []struct {
		name     string
		body     []byte
		after    time.Duration
		stale    bool
		interval time.Duration
	}{
		{"no schedule: one day + 6 h", sum(), 29 * time.Hour, false, 24 * time.Hour},
		{"no schedule, overdue", sum(), 30*time.Hour + time.Minute, true, 24 * time.Hour},
		{"shortest interval wins", sum(86400, 3600, 0), 74 * time.Minute, false, time.Hour},
		{"hourly, overdue", sum(86400, 3600), 76 * time.Minute, true, time.Hour},
		{"no generated_at", []byte(`{}`), 0, true, 24 * time.Hour},
	} {
		stale, interval := LocalSummaryStale(tc.body, gen.Add(tc.after))
		if stale != tc.stale || interval != tc.interval {
			t.Errorf("%s: stale=%v interval=%v, want %v %v", tc.name, stale, interval, tc.stale, tc.interval)
		}
	}
}

// getHosted sends one GET through the Lambda entry, with the query in
// the event's raw query string as API Gateway sends it.
func getHosted(t *testing.T, s *Server, target string) events.APIGatewayV2HTTPResponse {
	t.Helper()
	p, q, _ := strings.Cut(target, "?")
	ev := events.APIGatewayV2HTTPRequest{
		Version: "2.0", RawPath: p, RawQueryString: q,
		RequestContext: events.APIGatewayV2HTTPRequestContext{
			HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: "GET", Path: p},
		},
	}
	b, _ := json.Marshal(ev)
	out, err := s.Invoke(context.Background(), b)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	return out.(events.APIGatewayV2HTTPResponse)
}

func ptr[T any](v T) *T { return &v }
