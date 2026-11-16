package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// summaryNow is the fixed clock every summary test computes against.
var summaryNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

// helsinki is the zone TestMain sets through CLAVESA_TIME_ZONE.
var helsinki = func() *time.Location {
	loc, err := time.LoadLocation("Europe/Helsinki")
	if err != nil {
		panic(err)
	}
	return loc
}()

// summaryPipelineTF is one transform that declares a freshness_sla, the
// shape `node add` + `node edit --set freshness_sla=4h` writes.
const summaryPipelineTF = `variable "pipeline_name" { type = string default = "orders" }
module "xform" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  compute       = "lambda"
  language      = "sparksql"
  sql           = "SELECT 1"
  freshness_sla = "4h"
}
`

// summaryWorkspace lays out a workspace with one pipeline `orders` whose
// trigger_schedule default is schedule.
func summaryWorkspace(t *testing.T, schedule string) string {
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
	write("clavesa.json", `{"name":"summ-ws","cloud":"aws","version":1,"catalog":"clavesa_summ_ws"}`+"\n")
	write("orders/main.tf", summaryPipelineTF)
	write("orders/variables.tf", `variable "trigger_schedule" {
  type    = string
  default = "`+schedule+`"
}
`)
	return ws
}

// dropSLA removes the freshness_sla from the orders pipeline, for tests
// of the run and schedule rules that a stale table would mask.
func dropSLA(t *testing.T, ws string) {
	t.Helper()
	body := strings.Replace(summaryPipelineTF, "  freshness_sla = \"4h\"\n", "", 1)
	if err := os.WriteFile(filepath.Join(ws, "orders", "main.tf"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func ms(t time.Time) *int64 { v := t.UnixMilli(); return &v }

// deployedState writes the workspace stack state `workspace deploy`
// leaves: a pipeline_bucket output and a resource ARN in region.
func deployedState(t *testing.T, ws, region string) {
	t.Helper()
	body := `{"version":4,"outputs":{"pipeline_bucket":{"value":"summ-ws-clavesa","type":"string"}},` +
		`"resources":[{"type":"aws_athena_workgroup","instances":[{"attributes":{"arn":"arn:aws:athena:` +
		region + `:123456789012:workgroup/clavesa-summ-ws"}}]}]}`
	if err := os.WriteFile(filepath.Join(ws, "terraform.tfstate"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// cloudSummaryWorkspace is summaryWorkspace on the cloud warehouse.
func cloudSummaryWorkspace(t *testing.T, schedule string) string {
	t.Helper()
	ws := summaryWorkspace(t, schedule)
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
		t.Fatal(err)
	}
	return ws
}

// TestWorkspaceSummaryLocal reads a local warehouse through the real
// LocalProvider: run markers and per-node markers under `_progress`, the
// tree a local `pipeline run` writes.
func TestWorkspaceSummaryLocal(t *testing.T) {
	ws := summaryWorkspace(t, "rate(1 day)")
	store := observability.NewFileProgressStore(workspace.LocalWarehouseDir(ws))
	ctx := context.Background()

	type run struct {
		id      string
		start   time.Time
		status  string
		node    string // per-node marker body
		failed  string
		errText string
	}
	r1 := summaryNow.Add(-72 * time.Hour)
	r2 := summaryNow.Add(-24 * time.Hour)
	r3 := summaryNow.Add(-40 * 24 * time.Hour)
	for _, r := range []run{
		{id: "run1", start: r1, status: "SUCCEEDED", node: `{"status":"succeeded","started_ms":%d,"ended_ms":%d,"output_rows":1000}`},
		{id: "run2", start: r2, status: "FAILED", node: `{"status":"failed","started_ms":%d,"ended_ms":%d,"error":"boom"}`, failed: "xform", errText: "boom"},
		{id: "run3", start: r3, status: "SUCCEEDED", node: `{"status":"succeeded","started_ms":%d,"ended_ms":%d,"output_rows":5}`},
	} {
		end := r.start.Add(time.Minute)
		if err := observability.WriteRunMarker(ctx, store, r.id, observability.RunMarker{
			Status: r.status, Trigger: "manual", StartedMs: ms(r.start), EndedMs: ms(end),
			FailedStep: r.failed, ErrorMsg: r.errText, Pipeline: "orders",
		}); err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf(r.node, r.start.UnixMilli(), end.UnixMilli())
		if err := os.WriteFile(filepath.Join(workspace.LocalWarehouseDir(ws), "_progress", r.id, "xform.json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	local := observability.NewLocalProvider(ws)
	svc := New(ws).WithResolver(observability.NewResolver(ws, nil, local))
	sum, err := svc.WorkspaceSummary(ctx, WorkspaceSummaryQuery{Now: summaryNow})
	if err != nil {
		t.Fatalf("WorkspaceSummary: %v", err)
	}

	if sum.Name != "summ-ws" || sum.Kind != "local" || sum.GeneratedAt != "2026-11-15T12:00:00Z" {
		t.Errorf("header = %q %q %q", sum.Name, sum.Kind, sum.GeneratedAt)
	}
	if sum.Status != SummaryFail {
		t.Errorf("status = %q, want fail (last run failed); reasons %v", sum.Status, sum.StatusReasons)
	}
	if len(sum.Pipelines) != 1 {
		t.Fatalf("pipelines = %+v", sum.Pipelines)
	}
	p := sum.Pipelines[0]
	if p.Schedule != "rate(1 day)" || p.ExpectedIntervalSeconds != 86400 {
		t.Errorf("schedule = %q / %d", p.Schedule, p.ExpectedIntervalSeconds)
	}
	if got := strings.Join(p.StatusReasons, "|"); got != "last run failed|1 table(s) past their freshness target" {
		t.Errorf("reasons = %q", got)
	}
	if len(p.Runs) != 2 || p.Runs[0].RunID != "run2" || p.Runs[1].RunID != "run1" {
		t.Fatalf("30-day runs, newest first = %+v", p.Runs)
	}
	if p.LastRun == nil || p.LastRun.Result != "failed" || p.LastRun.FailedStep != "xform" || p.LastRun.Error != "boom" {
		t.Errorf("last run = %+v", p.LastRun)
	}
	if len(p.LastRunNodes) != 1 || p.LastRunNodes[0].Status != "failed" || p.LastRunNodes[0].Error != "boom" || p.LastRunNodes[0].DurationMs != 60000 {
		t.Errorf("last run nodes = %+v", p.LastRunNodes)
	}
	if p.Runs[1].Records != 1000 || p.Runs[1].DurationMs != 60000 || p.Runs[1].CostUSD != 0 {
		t.Errorf("run1 = %+v", p.Runs[1])
	}
	// The table was never written, so it is past its SLA.
	if len(p.Freshness) != 1 || !p.Freshness[0].Stale || p.Freshness[0].SLASeconds != 14400 {
		t.Errorf("freshness = %+v", p.Freshness)
	}
	if p.FreshnessState != FreshnessStale || sum.FreshnessState != FreshnessStale || sum.SLATables != 1 {
		t.Errorf("freshness state = %q / %q, sla tables %d", p.FreshnessState, sum.FreshnessState, sum.SLATables)
	}

	if len(sum.RunsPerDay14d) != 14 || len(sum.RunsPerDay30d) != 30 || len(sum.CostPerDay30d) != 30 {
		t.Fatalf("day series lengths = %d %d %d", len(sum.RunsPerDay14d), len(sum.RunsPerDay30d), len(sum.CostPerDay30d))
	}
	last := sum.RunsPerDay14d[13]
	if last.Date != "2026-11-15" {
		t.Errorf("last strip day = %q, want today", last.Date)
	}
	if d := sum.RunsPerDay14d[12]; d.Date != "2026-11-14" || d.Failed != 1 {
		t.Errorf("2026-11-14 = %+v, want one failed run", d)
	}
	if d := sum.RunsPerDay14d[10]; d.Date != "2026-11-12" || d.Succeeded != 1 {
		t.Errorf("2026-11-12 = %+v, want one succeeded run", d)
	}
	if sum.CostUSD30d != 0 || sum.Records30d != 1000 {
		t.Errorf("cost/records = %v / %d", sum.CostUSD30d, sum.Records30d)
	}
	if math.Abs(sum.RecordsPerSec-1000.0/60.0) > 1e-9 {
		t.Errorf("records/s = %v", sum.RecordsPerSec)
	}
	if sum.StaleTables != 1 || sum.LastRun == nil || sum.LastRun.RunID != "run2" {
		t.Errorf("stale = %d, last run = %+v", sum.StaleTables, sum.LastRun)
	}
}

// TestWorkspaceSummaryCloud dispatches to the cloud provider (a fake, as
// the other service tests do) and checks the shape is the local one, with
// Lambda cost and a table age from tables-state.
func TestWorkspaceSummaryCloud(t *testing.T) {
	ws := cloudSummaryWorkspace(t, "rate(1 hour)")
	deployedState(t, ws, "eu-north-1")
	arn := "arn:aws:states:eu-north-1:123456789012:execution:clavesa-orders:exec1"
	start := summaryNow.Add(-2 * time.Hour)
	dur := int64(60000)
	mem := int64(1024)
	recs := int64(1_000_000)
	iso := func(t time.Time) string { return t.Format("2006-01-02T15:04:05.000Z") }
	cloud := &fakeProvider{
		runs: &observability.RunsResult{Rows: []observability.Run{{
			RunID: "r-1", Pipeline: "orders", SfExecutionARN: arn, Status: "SUCCEEDED",
			StartedAt: iso(start), EndedAt: iso(start.Add(time.Minute)), DurationMs: &dur,
		}}},
		nodeRuns: []observability.NodeRun{{
			RunID: "inv-1", Pipeline: "orders", SfExecutionARN: arn, Node: "xform", Status: "ok",
			StartedAt: iso(start), DurationMs: &dur, ComputeTarget: "lambda", MemoryMB: &mem, InputRecords: &recs,
		}},
		tables: []observability.TableInfo{{
			Pipeline: "orders", Node: "xform", OutputKey: "default", TableName: "xform",
			SnapshotTS: iso(summaryNow.Add(-10 * time.Hour)),
		}},
	}
	svc := New(ws).WithResolver(observability.NewResolver(ws, cloud, &fakeProvider{}))
	sum, err := svc.WorkspaceSummary(context.Background(), WorkspaceSummaryQuery{Now: summaryNow})
	if err != nil {
		t.Fatalf("WorkspaceSummary: %v", err)
	}
	if sum.Region != "eu-north-1" || sum.Account != "123456789012" {
		t.Errorf("region/account = %q/%q, want them from the state", sum.Region, sum.Account)
	}
	if sum.Kind != "cloud" || sum.Status != SummaryWarn {
		t.Errorf("kind/status = %q/%q, reasons %v", sum.Kind, sum.Status, sum.StatusReasons)
	}
	p := sum.Pipelines[0]
	if got := strings.Join(p.StatusReasons, "|"); got != "1 table(s) past their freshness target|no run since 15.11 12:00, expected hourly" {
		t.Errorf("reasons = %q", got)
	}
	if len(p.Freshness) != 1 || p.Freshness[0].AgeSeconds != 36000 || !p.Freshness[0].Stale {
		t.Errorf("freshness = %+v", p.Freshness)
	}
	if sum.OldestTableAgeSeconds != 36000 {
		t.Errorf("oldest age = %d", sum.OldestTableAgeSeconds)
	}
	wantCost := 60 * 1.0 * 0.0000166667
	if math.Abs(sum.CostUSD30d-wantCost) > 1e-12 || math.Abs(p.Runs[0].CostUSD-wantCost) > 1e-12 {
		t.Errorf("cost = %v / run %v, want %v", sum.CostUSD30d, p.Runs[0].CostUSD, wantCost)
	}
	if math.Abs(sum.CostPerBillion-wantCost/1e6*1e9) > 1e-9 {
		t.Errorf("cost per billion = %v", sum.CostPerBillion)
	}
	if p.Runs[0].Records != recs || len(p.LastRunNodes) != 1 {
		t.Errorf("run = %+v nodes = %+v", p.Runs[0], p.LastRunNodes)
	}
	if c := sum.CostPerDay30d[29]; c.Date != "2026-11-15" || math.Abs(c.CostUSD-wantCost) > 1e-12 {
		t.Errorf("today's cost = %+v", c)
	}
	// The pipeline name and dir reach the provider the way the dashboard
	// sends them.
	if cloud.runsQ.PipelineName != "orders" || cloud.tablesQ.PipelineName != "orders" || cloud.nodeRunsQ.PipelineName != "orders" {
		t.Errorf("queries = %+v / %+v / %+v", cloud.runsQ, cloud.tablesQ, cloud.nodeRunsQ)
	}

	// Same JSON field set as the local shape (ADR-014).
	raw, _ := json.Marshal(sum)
	for _, k := range []string{`"runs_per_day_14d"`, `"cost_per_day_30d"`, `"cost_per_billion"`, `"records_per_sec"`, `"stale_tables"`, `"oldest_table_age_seconds"`, `"last_run_nodes"`} {
		if !strings.Contains(string(raw), k) {
			t.Errorf("JSON lacks %s", k)
		}
	}
}

// TestWorkspaceSummaryPausedSchedule: a daily pipeline whose schedule
// rule is disabled gets the note "schedule paused since <time>" and no
// overdue warning (v2.27.4). With the rule enabled, or when its state
// cannot be read, the same history is overdue as before.
func TestWorkspaceSummaryPausedSchedule(t *testing.T) {
	ws := cloudSummaryWorkspace(t, "cron(30 4 * * ? *)")
	dropSLA(t, ws)
	deployedState(t, ws, "eu-north-1")
	start := summaryNow.Add(-6 * 24 * time.Hour) // 9.11 12:00 UTC
	iso := func(t time.Time) string { return t.Format("2006-01-02T15:04:05.000Z") }
	arn := "arn:aws:states:eu-north-1:123456789012:execution:clavesa-orders:exec1"
	cloud := &fakeProvider{runs: &observability.RunsResult{Rows: []observability.Run{{
		RunID: "r-1", Pipeline: "orders", SfExecutionARN: arn, Status: "SUCCEEDED",
		StartedAt: iso(start), EndedAt: iso(start.Add(time.Minute)),
	}}}}
	for _, c := range []struct {
		name       string
		paused     bool
		since      string
		wantStatus string
		wantNote   string
	}{
		{"paused", true, "2026-11-09T07:21:11Z", SummaryOK, "schedule paused since 9.11 09:21"},
		{"paused, time unknown", true, "", SummaryOK, "schedule paused"},
		{"enabled", false, "", SummaryWarn, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			svc := New(ws).WithResolver(observability.NewResolver(ws, cloud, &fakeProvider{}))
			var asked string
			svc.scheduleStateFn = func(_ context.Context, rule string) (bool, string) {
				asked = rule
				return c.paused, c.since
			}
			sum, err := svc.WorkspaceSummary(context.Background(), WorkspaceSummaryQuery{Now: summaryNow})
			if err != nil {
				t.Fatalf("WorkspaceSummary: %v", err)
			}
			p := sum.Pipelines[0]
			if !strings.HasSuffix(asked, "orders-schedule") {
				t.Errorf("asked about rule %q, want the pipeline's schedule rule", asked)
			}
			if p.Status != c.wantStatus || sum.Status != c.wantStatus {
				t.Errorf("status = %q / workspace %q, want %q; reasons %v", p.Status, sum.Status, c.wantStatus, p.StatusReasons)
			}
			overdue := hasReason(p.StatusReasons, "no run since 9.11 14:00, expected daily")
			if overdue == c.paused {
				t.Errorf("overdue reason present = %v with paused = %v: %v", overdue, c.paused, p.StatusReasons)
			}
			if c.wantNote != "" {
				if len(p.Notes) != 1 || p.Notes[0] != c.wantNote || !hasReason(sum.Notes, "orders: "+c.wantNote) {
					t.Errorf("notes = %v / %v, want %q", p.Notes, sum.Notes, c.wantNote)
				}
				if !p.SchedulePaused || p.SchedulePausedSince != c.since {
					t.Errorf("paused fields = %v %q", p.SchedulePaused, p.SchedulePausedSince)
				}
			} else if len(p.Notes) != 0 {
				t.Errorf("notes = %v, want none", p.Notes)
			}
		})
	}
}

// TestWorkspaceSummaryCloudNoCredentials: a cloud warehouse with no cloud
// provider fails once with the resolver's error.
func TestWorkspaceSummaryCloudNoCredentials(t *testing.T) {
	ws := cloudSummaryWorkspace(t, "null")
	svc := New(ws).WithResolver(observability.NewResolver(ws, nil, &fakeProvider{}))
	if _, err := svc.WorkspaceSummary(context.Background(), WorkspaceSummaryQuery{Now: summaryNow}); err == nil || !strings.Contains(err.Error(), "no AWS credentials") {
		t.Fatalf("err = %v", err)
	}
}

func TestScheduleInterval(t *testing.T) {
	for expr, want := range map[string]time.Duration{
		"rate(1 day)":          24 * time.Hour,
		"rate(15 minutes)":     15 * time.Minute,
		"rate(2 hours)":        2 * time.Hour,
		"cron(0 2 * * ? *)":    24 * time.Hour,
		"cron(5 * * * ? *)":    time.Hour,
		"cron(0 3 ? * MON *)":  7 * 24 * time.Hour,
		"cron(0 2,14 * * ? *)": 0,
		"cron(0 2 1 * ? *)":    0,
		"null":                 0,
		"":                     0,
	} {
		if got := scheduleInterval(expr); got != want {
			t.Errorf("scheduleInterval(%q) = %v, want %v", expr, got, want)
		}
	}
}

// writePipeline adds a pipeline dir with one transform; sla is its
// freshness_sla ("" declares none).
func writePipeline(t *testing.T, ws, name, sla string) {
	t.Helper()
	tf := strings.Replace(summaryPipelineTF, `"orders"`, `"`+name+`"`, 1)
	if sla == "" {
		tf = strings.Replace(tf, "  freshness_sla = \"4h\"\n", "", 1)
	} else {
		tf = strings.Replace(tf, `"4h"`, `"`+sla+`"`, 1)
	}
	if err := os.MkdirAll(filepath.Join(ws, name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, name, "main.tf"), []byte(tf), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestWorkspaceSummaryUnreadableHistory: the Athena error a workspace
// deployed in another region gives is one problem shared by every
// pipeline: one status line with the count, a cause and action per
// pipeline, the raw error kept apart, and freshness unknown, never fresh.
func TestWorkspaceSummaryUnreadableHistory(t *testing.T) {
	ws := cloudSummaryWorkspace(t, "null")
	deployedState(t, ws, "us-east-2")
	writePipeline(t, ws, "events", "")
	raw := "StartQueryExecution: operation error Athena: StartQueryExecution, https response error StatusCode: 400, " +
		"InvalidRequestException: The S3 location provided to save your query results is invalid."
	cloud := &fakeProvider{runsErr: fmt.Errorf("%s", raw)}
	svc := New(ws).WithResolver(observability.NewResolver(ws, cloud, &fakeProvider{})).WithAWSRegion("eu-north-1")
	sum, err := svc.WorkspaceSummary(context.Background(), WorkspaceSummaryQuery{Now: summaryNow})
	if err != nil {
		t.Fatal(err)
	}
	want := "2 pipelines: workspace is deployed in us-east-2, read from eu-north-1. Set \"region\": \"us-east-2\" in clavesa.json."
	if got := strings.Join(sum.StatusReasons, "|"); got != want {
		t.Errorf("status reasons = %q, want %q", got, want)
	}
	if len(sum.Problems) != 1 || sum.Problems[0].Code != ProblemWrongRegion || strings.Join(sum.Problems[0].Pipelines, ",") != "events,orders" {
		t.Errorf("problems = %+v", sum.Problems)
	}
	for _, p := range sum.Pipelines {
		if p.Problem == nil || p.Problem.Code != ProblemWrongRegion || p.Error != raw || p.Status != SummaryWarn {
			t.Errorf("%s: problem %+v, error %q, status %q", p.Name, p.Problem, p.Error, p.Status)
		}
	}
	// orders declares an SLA it could not check; events declares none.
	byName := map[string]PipelineSummary{}
	for _, p := range sum.Pipelines {
		byName[p.Name] = p
	}
	if p := byName["orders"]; p.FreshnessState != FreshnessUnknown || !strings.Contains(p.FreshnessReason, "deployed in us-east-2") {
		t.Errorf("orders freshness = %q (%q)", p.FreshnessState, p.FreshnessReason)
	}
	if p := byName["events"]; p.FreshnessState != FreshnessNoSLA {
		t.Errorf("events freshness = %q, want no_sla", p.FreshnessState)
	}
	if sum.FreshnessState != FreshnessUnknown || sum.FreshnessReason != "orders: workspace is deployed in us-east-2, read from eu-north-1" {
		t.Errorf("workspace freshness = %q (%q)", sum.FreshnessState, sum.FreshnessReason)
	}
}

// TestWorkspaceSummaryNotDeployed: a cloud workspace with no Terraform
// state reads nothing and says so, once.
func TestWorkspaceSummaryNotDeployed(t *testing.T) {
	ws := cloudSummaryWorkspace(t, "null")
	writePipeline(t, ws, "events", "")
	cloud := &fakeProvider{runsErr: fmt.Errorf("must not be read")}
	svc := New(ws).WithResolver(observability.NewResolver(ws, cloud, &fakeProvider{}))
	sum, err := svc.WorkspaceSummary(context.Background(), WorkspaceSummaryQuery{Now: summaryNow})
	if err != nil {
		t.Fatal(err)
	}
	want := "2 pipelines: not deployed: no Terraform state. Run `clavesa workspace deploy`."
	if got := strings.Join(sum.StatusReasons, "|"); got != want || sum.Status != SummaryWarn {
		t.Errorf("status %q, reasons = %q, want %q", sum.Status, got, want)
	}
	if sum.Pipelines[0].Error != "" {
		t.Errorf("error = %q, want none (nothing was read)", sum.Pipelines[0].Error)
	}
}

// TestWorkspaceSummaryFreshnessStates: fresh only when every SLA table was
// read and is inside its SLA; a failed tables-state read is unknown; a
// pipeline with no SLA is no_sla.
func TestWorkspaceSummaryFreshnessStates(t *testing.T) {
	iso := func(t time.Time) string { return t.Format("2006-01-02T15:04:05.000Z") }
	runs := &observability.RunsResult{Rows: []observability.Run{{
		RunID: "r-1", Pipeline: "orders", Status: "SUCCEEDED",
		StartedAt: iso(summaryNow.Add(-time.Hour)), EndedAt: iso(summaryNow.Add(-59 * time.Minute)),
	}}}
	table := []observability.TableInfo{{Pipeline: "orders", Node: "xform", TableName: "xform", SnapshotTS: iso(summaryNow.Add(-time.Hour))}}
	for _, tc := range []struct {
		name       string
		sla        string
		tablesErr  error
		wantState  string
		wantReason string
	}{
		{name: "fresh", sla: "4h", wantState: FreshnessFresh},
		{name: "stale", sla: "30m", wantState: FreshnessStale},
		{name: "no sla", sla: "", wantState: FreshnessNoSLA},
		{name: "unreadable", sla: "4h", tablesErr: fmt.Errorf("AccessDeniedException: User: x is not authorized to perform: glue:GetTable"),
			wantState: FreshnessUnknown, wantReason: "orders: table state unreadable: no permission to read run history (glue:GetTable)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ws := cloudSummaryWorkspace(t, "null")
			deployedState(t, ws, "eu-north-1")
			writePipeline(t, ws, "orders", tc.sla)
			cloud := &fakeProvider{runs: runs, tables: table, tablesErr: tc.tablesErr}
			svc := New(ws).WithResolver(observability.NewResolver(ws, cloud, &fakeProvider{}))
			sum, err := svc.WorkspaceSummary(context.Background(), WorkspaceSummaryQuery{Now: summaryNow})
			if err != nil {
				t.Fatal(err)
			}
			if sum.FreshnessState != tc.wantState || sum.FreshnessReason != tc.wantReason {
				t.Errorf("freshness = %q (%q), want %q (%q)", sum.FreshnessState, sum.FreshnessReason, tc.wantState, tc.wantReason)
			}
			if tc.tablesErr != nil && (len(sum.Pipelines[0].Freshness) != 0 || sum.StaleTables != 0 || sum.Status != SummaryWarn) {
				t.Errorf("unreadable tables: freshness %+v, stale %d, status %q", sum.Pipelines[0].Freshness, sum.StaleTables, sum.Status)
			}
		})
	}
}

func TestClassifySummaryError(t *testing.T) {
	east := summaryEnv{DeployedRegion: "us-east-2", ReadRegion: "eu-north-1"}
	same := summaryEnv{DeployedRegion: "eu-north-1", ReadRegion: "eu-north-1"}
	for _, tc := range []struct {
		err  string
		env  summaryEnv
		code string
		line string
	}{
		{"StartQueryExecution: operation error Athena: StartQueryExecution, https response error StatusCode: 400, RequestID: 1, InvalidRequestException: The S3 location provided to save your query results is invalid. Please check your S3 location is correct and is in the same region and try again.",
			east, ProblemWrongRegion, "workspace is deployed in us-east-2, read from eu-north-1. Set \"region\": \"us-east-2\" in clavesa.json."},
		{"StartQueryExecution: operation error Athena: StartQueryExecution, https response error StatusCode: 400, InvalidRequestException: The S3 location provided to save your query results is invalid.",
			same, ProblemNotDeployed, "pipeline bucket not found: the workspace is not deployed. Run `clavesa workspace deploy`."},
		{"get object: operation error S3: GetObject, https response error StatusCode: 404, api error NoSuchBucket: The specified bucket does not exist",
			summaryEnv{}, ProblemNotDeployed, "pipeline bucket not found: the workspace is not deployed. Run `clavesa workspace deploy`."},
		{"StartQueryExecution: operation error Athena: StartQueryExecution, https response error StatusCode: 400, AccessDeniedException: User: arn:aws:iam::1:user/x is not authorized to perform: athena:StartQueryExecution on resource: arn:aws:athena:eu-north-1:1:workgroup/primary",
			same, ProblemAccessDenied, "no permission to read run history (athena:StartQueryExecution). Grant it to the AWS identity in use."},
		{"GetTable: operation error Glue: GetTable, https response error StatusCode: 400, AccessDeniedException: ",
			same, ProblemAccessDenied, "no permission to read run history (glue:GetTable). Grant it to the AWS identity in use."},
		{"glue.GetTable db.runs: operation error Glue: GetTable, https response error StatusCode: 400, EntityNotFoundException: Table runs not found.",
			same, ProblemNeverRun, "never run: no run history table yet. Run `clavesa pipeline run`."},
		{"Athena query FAILED: TABLE_NOT_FOUND: line 12:6: Table 'awsdatacatalog.x.node_runs' does not exist",
			same, ProblemNeverRun, "never run: no run history table yet. Run `clavesa pipeline run`."},
		{"operation error Athena: StartQueryExecution, get identity: get credentials: failed to refresh cached credentials, the SSO session has expired or is invalid",
			same, ProblemNoCredentials, "AWS credentials missing or expired. Sign in again (aws login) or set AWS_PROFILE."},
		{"operation error Athena: StartQueryExecution, https response error StatusCode: 400, api error ExpiredTokenException: The security token included in the request is expired",
			same, ProblemNoCredentials, "AWS credentials missing or expired. Sign in again (aws login) or set AWS_PROFILE."},
		{"Athena query FAILED: GENERIC_INTERNAL_ERROR: something else",
			same, ProblemUnreadable, "run history unreadable. See `clavesa workspace summary --json` for the error."},
	} {
		got := classifySummaryError(fmt.Errorf("%s", tc.err), tc.env)
		if got.Code != tc.code || got.Line() != tc.line {
			t.Errorf("classify(%.60q...) = %s %q, want %s %q", tc.err, got.Code, got.Line(), tc.code, tc.line)
		}
	}
}

func TestTFVarValue(t *testing.T) {
	for in, want := range map[string]string{
		`"cron(30 4 * * ? *)"   # 04:30 UTC every day`: "cron(30 4 * * ? *)",
		`"rate(1 day)"`:          "rate(1 day)",
		`"a # b"`:                "a # b",
		`null // none`:           "null",
		`3`:                      "3",
		`"say \"hi\"" # comment`: `say \"hi\"`,
	} {
		if got := tfvarValue(in); got != want {
			t.Errorf("tfvarValue(%q) = %q, want %q", in, got, want)
		}
	}
}
