package service

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// pollerWorkspace is the web-traffic shape: bronze is started by its
// source-queue poller once a day (trigger_batch_window =
// "cron(10 6 * * ? *)"), silver reads bronze and gold reads silver, so an
// upstream success starts each. "quiet" has a poller on the default
// one-minute window, which says nothing about how often it runs.
func pollerWorkspace(t *testing.T) string {
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
	vars := `variable "trigger_schedule" {
  type    = string
  default = null
}
variable "trigger_batch_window" {
  type    = string
  default = "rate(1 minute)"
}
`
	transform := func(pipeline, id, inputs string) string {
		return `variable "pipeline_name" { type = string default = "` + pipeline + `" }
module "` + id + `" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  compute       = "lambda"
  language      = "sparksql"
  sql           = "SELECT 1"
` + inputs + `}
`
	}
	poller := "locals {\n  _poller_queue_arns = [module.src_logs.trigger_queue_arn]\n}\n"
	write("clavesa.json", `{"name":"wt","cloud":"aws","version":1,"catalog":"clavesa_wt"}`+"\n")
	write("bronze/main.tf", transform("bronze", "cloudfront_raw", ""))
	write("bronze/variables.tf", vars)
	write("bronze/terraform.tfvars", `trigger_batch_window = "cron(10 6 * * ? *)"`+"\n")
	write("bronze/orchestration.tf", poller)
	write("silver/main.tf", transform("silver", "enriched", `  inputs = { bronze = "bronze.cloudfront_raw" }`+"\n"))
	write("silver/variables.tf", vars)
	write("silver/orchestration.tf", "locals {\n  _poller_queue_arns = []\n}\n")
	write("gold/main.tf", transform("gold", "daily", `  inputs = { enriched = "silver.enriched" }`+"\n"))
	write("gold/variables.tf", vars)
	write("quiet/main.tf", transform("quiet", "q", ""))
	write("quiet/variables.tf", vars)
	write("quiet/orchestration.tf", poller)
	if err := workspace.WriteWarehouse(ws, workspace.WarehouseCloud); err != nil {
		t.Fatal(err)
	}
	return ws
}

func TestPipelineTriggers(t *testing.T) {
	ws := pollerWorkspace(t)
	svc := New(ws)
	pipes, err := svc.ListPipelines()
	if err != nil {
		t.Fatal(err)
	}
	day := 24 * time.Hour
	got := svc.pipelineTriggers(pipes, true)
	want := map[string]pipelineTrigger{
		"bronze": {kind: TriggerPoller, expr: "cron(10 6 * * ? *)", interval: day},
		"silver": {kind: TriggerUpstream, interval: day, after: []string{"bronze"}},
		"gold":   {kind: TriggerUpstream, interval: day, after: []string{"silver"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("triggers = %+v\nwant %+v", got, want)
	}
	// A local workspace's pipelines are not started by an upstream rule.
	if local := svc.pipelineTriggers(pipes, false); len(local) != 1 || local["bronze"].kind != TriggerPoller {
		t.Errorf("local triggers = %+v, want only the bronze poller", local)
	}

	// Pauses: the poller rule disabled pauses bronze, and so silver and
	// gold after it, in any order.
	var asked []string
	svc.scheduleStateFn = func(_ context.Context, rule string) bool {
		asked = append(asked, rule)
		return true
	}
	svc.triggerPauses(context.Background(), got)
	if len(asked) != 1 || filepath.Base(asked[0]) != asked[0] || asked[0][len(asked[0])-len("bronze-poller"):] != "bronze-poller" {
		t.Errorf("asked about rules %v, want only bronze's poller rule", asked)
	}
	for _, n := range []string{"bronze", "silver", "gold"} {
		if !got[n].paused {
			t.Errorf("%s not paused: %+v", n, got[n])
		}
	}
}

// TestWorkspaceSummaryPollerDaily: web-traffic's pipelines are expected
// daily. A run 20 hours ago is healthy; with the newest run 2 days old,
// every one of them says "no run since ..., expected daily".
func TestWorkspaceSummaryPollerDaily(t *testing.T) {
	ws := pollerWorkspace(t)
	deployedState(t, ws, "eu-north-1")
	iso := func(t time.Time) string { return t.Format("2006-01-02T15:04:05.000Z") }
	for _, c := range []struct {
		name    string
		age     time.Duration
		overdue bool
	}{
		{"ran 20 h ago", 20 * time.Hour, false},
		{"ran 2 days ago", 48 * time.Hour, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			start := summaryNow.Add(-c.age)
			var rows []observability.Run
			for _, p := range []string{"bronze", "silver", "gold", "quiet"} {
				rows = append(rows, observability.Run{
					RunID: "r-" + p, Pipeline: p, Status: "SUCCEEDED",
					SfExecutionARN: "arn:aws:states:eu-north-1:123456789012:execution:clavesa-" + p + ":e",
					StartedAt:      iso(start), EndedAt: iso(start.Add(time.Minute)),
				})
			}
			cloud := &fakeProvider{runs: &observability.RunsResult{Rows: rows}}
			svc := New(ws).WithResolver(observability.NewResolver(ws, cloud, &fakeProvider{}))
			svc.scheduleStateFn = func(context.Context, string) bool { return false }
			sum, err := svc.WorkspaceSummary(context.Background(), WorkspaceSummaryQuery{Now: summaryNow})
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range sum.Pipelines {
				if p.Name == "quiet" {
					if p.ExpectedIntervalSeconds != 0 || p.Trigger != "" {
						t.Errorf("quiet: interval %d trigger %q, want none", p.ExpectedIntervalSeconds, p.Trigger)
					}
					continue
				}
				if p.ExpectedIntervalSeconds != 86400 {
					t.Errorf("%s: interval %d, want 86400 (trigger %q)", p.Name, p.ExpectedIntervalSeconds, p.Trigger)
				}
				if got := hasReason(p.StatusReasons, "no run since 13.11 14:00, expected daily"); got != c.overdue {
					t.Errorf("%s: overdue = %v, want %v; reasons %v", p.Name, got, c.overdue, p.StatusReasons)
				}
			}
			bronze := sum.Pipelines[0]
			if bronze.Name != "bronze" || bronze.Trigger != TriggerPoller || bronze.BatchWindow != "cron(10 6 * * ? *)" || bronze.Schedule != "" {
				t.Errorf("bronze = %q trigger %q window %q schedule %q", bronze.Name, bronze.Trigger, bronze.BatchWindow, bronze.Schedule)
			}
		})
	}
}
