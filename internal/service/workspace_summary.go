package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pricing"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// Workspace summary status values: the worst condition wins.
const (
	SummaryOK   = "ok"
	SummaryWarn = "warn"
	SummaryFail = "fail"
)

// Freshness states of a pipeline and of a workspace. Fresh is a claim
// that every table with a freshness_sla was read and is inside its SLA;
// a pipeline that declares no SLA is "no_sla", never "fresh"; one whose
// history or table state could not be read is "unknown".
const (
	FreshnessFresh   = "fresh"
	FreshnessStale   = "stale"
	FreshnessUnknown = "unknown"
	FreshnessNoSLA   = "no_sla"
)

// summaryWindowDays is how far back the summary's run history reaches;
// summaryStripDays is the short strip the workspace card draws.
const (
	summaryWindowDays = 30
	summaryStripDays  = 14
	// summaryRunLimit and summaryNodeRunLimit bound the provider reads per
	// pipeline. Well above 30 days of an hourly pipeline's runs.
	summaryRunLimit     = 1000
	summaryNodeRunLimit = 5000
	// summaryTablesLimit bounds the tables-state read per pipeline.
	summaryTablesLimit = 500
)

// WorkspaceSummaryQuery parameterizes WorkspaceSummary. Now is the clock the
// summary is computed against (zero means time.Now()), so tests are
// deterministic.
type WorkspaceSummaryQuery struct {
	Now time.Time
}

// WorkspaceSummary is the health summary of one workspace (ADR-032 section
// 2): the run history of the last 30 days, the last run's node timings,
// table freshness and the roll-ups the workspace card draws. The shape is
// the same for a local and a cloud warehouse (ADR-014).
type WorkspaceSummary struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"` // "cloud" | "local"
	Account     string `json:"account,omitempty"`
	Region      string `json:"region,omitempty"`
	GeneratedAt string `json:"generated_at"`
	// Status is the worst of: last run failed (fail), a table past its
	// freshness_sla (warn), no run inside the expected interval (warn).
	Status        string   `json:"status"`
	StatusReasons []string `json:"status_reasons"`
	// LastRun is the newest run across every pipeline.
	LastRun   *SummaryRun       `json:"last_run,omitempty"`
	Pipelines []PipelineSummary `json:"pipelines"`

	RunsPerDay14d []SummaryDayRuns `json:"runs_per_day_14d"`
	RunsPerDay30d []SummaryDayRuns `json:"runs_per_day_30d"`
	CostPerDay30d []SummaryDayCost `json:"cost_per_day_30d"`
	CostUSD30d    float64          `json:"cost_usd_30d"`
	Records30d    int64            `json:"records_30d"`
	// CostPerBillion is the 30-day cost per billion records processed;
	// RecordsPerSec the 30-day throughput (the local warehouse's figure).
	CostPerBillion float64 `json:"cost_per_billion"`
	RecordsPerSec  float64 `json:"records_per_sec"`

	StaleTables           int   `json:"stale_tables"`
	OldestTableAgeSeconds int64 `json:"oldest_table_age_seconds"`
	// FreshnessState rolls the pipelines up: stale when any is stale,
	// else unknown when any is unknown, else fresh when any declares an
	// SLA, else no_sla. FreshnessReason says why it is unknown;
	// SLATables counts the tables with a freshness_sla that were read.
	FreshnessState  string `json:"freshness_state"`
	FreshnessReason string `json:"freshness_reason,omitempty"`
	SLATables       int    `json:"sla_tables"`
	// Problems are the pipelines' read problems, one entry per distinct
	// problem with the pipelines that share it.
	Problems []SummaryProblemGroup `json:"problems"`
	// Drill is the drill-down a local-warehouse workspace publishes to
	// the hosted UI (PublishSummary); nil in every other summary.
	Drill *SummaryDrill `json:"drill,omitempty"`
}

// PipelineSummary is one pipeline's part of a WorkspaceSummary.
type PipelineSummary struct {
	Name string `json:"name"`
	Dir  string `json:"dir"`
	// Schedule is the declared trigger_schedule; ExpectedIntervalSeconds
	// is the interval it implies (0 when none is declared or the
	// expression is not one clavesa can read).
	Schedule                string   `json:"schedule,omitempty"`
	ExpectedIntervalSeconds int64    `json:"expected_interval_seconds,omitempty"`
	Status                  string   `json:"status"`
	StatusReasons           []string `json:"status_reasons"`
	// LastRun is the newest run; LastRunNodes its per-node timings.
	LastRun      *SummaryRun   `json:"last_run,omitempty"`
	LastRunNodes []SummaryNode `json:"last_run_nodes"`
	// Runs are the runs of the last 30 days, newest first.
	Runs      []SummaryRun     `json:"runs"`
	Freshness []TableFreshness `json:"freshness"`
	// FreshnessState is fresh, stale, unknown or no_sla (see the
	// constants); FreshnessReason says why it is unknown.
	FreshnessState  string `json:"freshness_state"`
	FreshnessReason string `json:"freshness_reason,omitempty"`

	CostUSD30d     float64 `json:"cost_usd_30d"`
	Records30d     int64   `json:"records_30d"`
	CostPerBillion float64 `json:"cost_per_billion"`
	RecordsPerSec  float64 `json:"records_per_sec"`
	// Problem says in one line why this pipeline's history could not be
	// read and what to do; Error is the raw provider error behind it, for
	// the drill-down. The rest of the summary still renders.
	Problem *SummaryProblem `json:"problem,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// SummaryRun is one pipeline run. Result is "succeeded", "failed" or
// "running". Records is the records processed (input_records, else
// output_rows, as pricing.AggregateCost counts them).
type SummaryRun struct {
	RunID      string  `json:"run_id"`
	Pipeline   string  `json:"pipeline"`
	Result     string  `json:"result"`
	StartedAt  string  `json:"started_at"`
	EndedAt    string  `json:"ended_at,omitempty"`
	DurationMs int64   `json:"duration_ms"`
	Records    int64   `json:"records"`
	CostUSD    float64 `json:"cost_usd"`
	FailedStep string  `json:"failed_step,omitempty"`
	Error      string  `json:"error,omitempty"`
}

// SummaryNode is one node of a run.
type SummaryNode struct {
	Node       string `json:"node"`
	Status     string `json:"status"`
	StartedAt  string `json:"started_at,omitempty"`
	DurationMs int64  `json:"duration_ms"`
	OutputRows *int64 `json:"output_rows,omitempty"`
	Error      string `json:"error,omitempty"`
}

// TableFreshness is one output table with a declared freshness_sla.
// LastWriteAt is empty (and the table stale) when the table has never
// been written.
type TableFreshness struct {
	Table       string `json:"table"`
	Node        string `json:"node"`
	LastWriteAt string `json:"last_write_at,omitempty"`
	AgeSeconds  int64  `json:"age_seconds"`
	SLASeconds  int64  `json:"sla_seconds"`
	Stale       bool   `json:"stale"`
}

// SummaryDayRuns counts the finished runs that started on one UTC day.
type SummaryDayRuns struct {
	Date      string `json:"date"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
}

// SummaryDayCost is the compute cost of the runs that started on one UTC day.
type SummaryDayCost struct {
	Date    string  `json:"date"`
	CostUSD float64 `json:"cost_usd"`
}

// WorkspaceSummary computes the health summary of the workspace. It reads
// through the same service calls the dashboard uses (Runs, NodeRuns,
// TablesStateForDir, the pricing aggregation), so it has no data path of
// its own. `clavesa workspace summary` and GET /workspace/summary both
// reach this one call (ADR-015).
func (s *Service) WorkspaceSummary(ctx context.Context, q WorkspaceSummaryQuery) (*WorkspaceSummary, error) {
	now := q.Now
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	if s.dashResolver == nil {
		return nil, fmt.Errorf("workspace summary: observability resolver not configured")
	}
	// Fail early and once when the warehouse's provider is unavailable
	// (cloud with no AWS credentials), rather than once per pipeline.
	if _, err := s.dashResolver.Workspace(); err != nil {
		return nil, fmt.Errorf("workspace summary: %w", err)
	}

	sum := &WorkspaceSummary{
		Name:          s.workspaceName(),
		Kind:          "cloud",
		GeneratedAt:   now.Format(time.RFC3339),
		StatusReasons: []string{},
		Pipelines:     []PipelineSummary{},
		Problems:      []SummaryProblemGroup{},
	}
	if sum.Name == "" {
		sum.Name = filepath.Base(s.workspace)
	}
	if workspace.LoadWarehouse(s.workspace) == workspace.WarehouseLocal {
		sum.Kind = "local"
	}
	// The workspace's own region (GH #107), not the state bucket's.
	sum.Region = workspace.ResolveRegion(ctx, s.workspace)
	env := summaryEnv{ReadRegion: s.awsRegion}
	undeployed := false
	if sum.Kind == "cloud" {
		dep := s.summaryDeployment(ctx)
		env.DeployedRegion = dep.region
		undeployed = dep.undeployed
		if dep.region != "" {
			sum.Region = dep.region
		}
		sum.Account = dep.account
	}
	// A hosted tree copy knows its account from deployed.json (ADR-032).
	if d, _ := workspace.LoadDeployed(s.workspace); d != nil && sum.Account == "" {
		sum.Account = d.Account
	}

	pipes, err := s.ListPipelines()
	if err != nil {
		return nil, fmt.Errorf("workspace summary: list pipelines: %w", err)
	}
	sort.Slice(pipes, func(i, j int) bool { return pipes[i].Dir < pipes[j].Dir })

	windowStart := now.AddDate(0, 0, -summaryWindowDays)
	var allRows []observability.NodeRun
	for _, p := range pipes {
		ps, rows := s.pipelineSummary(ctx, p, now, windowStart, env, undeployed)
		allRows = append(allRows, rows...)
		sum.Pipelines = append(sum.Pipelines, ps)
	}

	// Roll-ups across pipelines. A problem several pipelines share is one
	// status line ("5 pipelines: <cause>. <action>"), not one per pipeline.
	sum.Problems = groupProblems(sum.Pipelines)
	for _, g := range sum.Problems {
		sum.StatusReasons = append(sum.StatusReasons, g.Line())
	}
	var allRuns []SummaryRun
	for _, p := range sum.Pipelines {
		allRuns = append(allRuns, p.Runs...)
		if p.LastRun != nil && (sum.LastRun == nil || p.LastRun.StartedAt > sum.LastRun.StartedAt) {
			lr := *p.LastRun
			sum.LastRun = &lr
		}
		sum.Status = worseStatus(sum.Status, p.Status)
		for _, r := range p.StatusReasons {
			if p.Problem != nil && r == p.Problem.Line() {
				continue // already in the grouped line above
			}
			sum.StatusReasons = append(sum.StatusReasons, p.Name+": "+r)
		}
		for _, f := range p.Freshness {
			sum.SLATables++
			if f.Stale {
				sum.StaleTables++
			}
			if f.AgeSeconds > sum.OldestTableAgeSeconds {
				sum.OldestTableAgeSeconds = f.AgeSeconds
			}
		}
	}
	if sum.Status == "" {
		sum.Status = SummaryOK
	}
	sum.FreshnessState, sum.FreshnessReason = workspaceFreshness(sum.Pipelines)
	sum.RunsPerDay14d = runsPerDay(allRuns, now, summaryStripDays)
	sum.RunsPerDay30d = runsPerDay(allRuns, now, summaryWindowDays)
	sum.CostPerDay30d = costPerDay(allRuns, now, summaryWindowDays)
	agg := pricing.AggregateCost(sum.Name, allRows)
	sum.CostUSD30d = agg.TotalCostUSD
	sum.Records30d = agg.TotalRecords
	sum.CostPerBillion = agg.CostPerBillion
	sum.RecordsPerSec = agg.RecordsPerSec
	return sum, nil
}

// pipelineSummary builds one pipeline's summary. It also returns the
// pipeline's node_runs rows inside the window, for the workspace roll-up.
func (s *Service) pipelineSummary(ctx context.Context, p PipelineInfo, now, windowStart time.Time, env summaryEnv, undeployed bool) (PipelineSummary, []observability.NodeRun) {
	ps := PipelineSummary{
		Name:          p.Name,
		Dir:           p.Dir,
		StatusReasons: []string{},
		LastRunNodes:  []SummaryNode{},
		Runs:          []SummaryRun{},
		Freshness:     []TableFreshness{},
	}
	if vars, err := s.GetVars(p.Dir); err == nil {
		if sched := strings.TrimSpace(vars["trigger_schedule"]); sched != "" && sched != "null" {
			ps.Schedule = sched
			ps.ExpectedIntervalSeconds = int64(scheduleInterval(sched).Seconds())
		}
	}

	// fail marks the pipeline's history unreadable: the problem line is
	// its status reason, its freshness is unknown unless it declares no
	// SLA at all.
	fail := func(prob SummaryProblem, raw string) (PipelineSummary, []observability.NodeRun) {
		ps.Problem = &prob
		ps.Error = raw
		ps.Status = SummaryWarn
		ps.StatusReasons = append(ps.StatusReasons, prob.Line())
		ps.FreshnessState = FreshnessUnknown
		ps.FreshnessReason = prob.Cause
		if len(s.slaByNode(p)) == 0 {
			ps.FreshnessState, ps.FreshnessReason = FreshnessNoSLA, ""
		}
		return ps, nil
	}
	if undeployed {
		return fail(notDeployedProblem(), "")
	}
	runsRes, err := s.Runs(ctx, p.Dir, summaryRunLimit)
	if err != nil {
		return fail(classifySummaryError(err, env), err.Error())
	}
	// The runs read; a failed node_runs read keeps them and only loses the
	// per-node timings, cost and records.
	nodeRes, err := s.NodeRuns(ctx, p.Dir, "", summaryNodeRunLimit)
	if err != nil {
		prob := classifySummaryError(err, env)
		ps.Problem = &prob
		ps.Error = err.Error()
		ps.StatusReasons = append(ps.StatusReasons, prob.Line())
		nodeRes = &observability.NodeRunsResult{}
	}

	// Group the node rows by the run they belong to. sf_execution_arn is
	// the join key against runs (local: the run id; cloud: the SFN ARN).
	byRun := map[string][]observability.NodeRun{}
	for _, nr := range nodeRes.Rows {
		k := nr.SfExecutionARN
		if k == "" {
			k = nr.RunID
		}
		byRun[k] = append(byRun[k], nr)
	}

	runs := append([]observability.Run(nil), runsRes.Rows...)
	sort.SliceStable(runs, func(i, j int) bool { return isoMillis(runs[i].StartedAt) > isoMillis(runs[j].StartedAt) })

	var windowRows []observability.NodeRun
	var lastFinished *SummaryRun
	for i, r := range runs {
		k := r.SfExecutionARN
		if k == "" {
			k = r.RunID
		}
		nodes := byRun[k]
		sr := summaryRun(p.Name, r, nodes)
		if i == 0 {
			lr := sr
			ps.LastRun = &lr
			ps.LastRunNodes = summaryNodes(nodes)
		}
		if lastFinished == nil && sr.Result != "running" {
			lf := sr
			lastFinished = &lf
		}
		started := isoMillis(r.StartedAt)
		if started == 0 || time.UnixMilli(started).Before(windowStart) {
			continue
		}
		ps.Runs = append(ps.Runs, sr)
		windowRows = append(windowRows, nodes...)
	}
	agg := pricing.AggregateCost(p.Name, windowRows)
	ps.CostUSD30d = agg.TotalCostUSD
	ps.Records30d = agg.TotalRecords
	ps.CostPerBillion = agg.CostPerBillion
	ps.RecordsPerSec = agg.RecordsPerSec

	var freshErr error
	ps.Freshness, ps.FreshnessState, freshErr = s.pipelineFreshness(ctx, p, now, len(runs) > 0)
	if freshErr != nil {
		prob := classifySummaryError(freshErr, env)
		ps.FreshnessReason = "table state unreadable: " + prob.Cause
		ps.Error = freshErr.Error()
	}

	// Status: the worst of the three conditions.
	status := SummaryOK
	if lastFinished != nil && lastFinished.Result == "failed" {
		status = SummaryFail
		ps.StatusReasons = append(ps.StatusReasons, "last run failed")
	}
	stale := 0
	for _, f := range ps.Freshness {
		if f.Stale {
			stale++
		}
	}
	if stale > 0 {
		status = worseStatus(status, SummaryWarn)
		ps.StatusReasons = append(ps.StatusReasons, fmt.Sprintf("%d table(s) past their freshness target", stale))
	}
	if ps.ExpectedIntervalSeconds > 0 {
		interval := time.Duration(ps.ExpectedIntervalSeconds) * time.Second
		// A quarter of the interval as grace, so a run that starts a few
		// minutes late does not flip the dot.
		deadline := interval + interval/4
		if ps.LastRun == nil || now.Sub(time.UnixMilli(isoMillis(ps.LastRun.StartedAt))) > deadline {
			status = worseStatus(status, SummaryWarn)
			ps.StatusReasons = append(ps.StatusReasons, "no run inside the expected interval")
		}
	}
	if ps.Problem != nil {
		status = worseStatus(status, SummaryWarn)
	}
	if freshErr != nil {
		status = worseStatus(status, SummaryWarn)
		ps.StatusReasons = append(ps.StatusReasons, "freshness unknown: "+ps.FreshnessReason)
	}
	ps.Status = status
	return ps, windowRows
}

// slaByNode reads the pipeline's freshness_sla declarations: sanitized
// node id to SLA seconds. Empty when none declares one (or the pipeline
// does not parse).
func (s *Service) slaByNode(p PipelineInfo) map[string]int64 {
	out := map[string]int64{}
	g, err := hclparser.Parse(s.resolveDir(p.Dir))
	if err != nil {
		return out
	}
	for _, n := range g.Nodes {
		raw, _ := n.Config["freshness_sla"].(string)
		if sec, ok := observability.ParseFreshnessSLA(raw); ok {
			out[identutil.Sanitize(n.ID)] = sec
		}
	}
	return out
}

// pipelineFreshness compares each output table that declares a
// freshness_sla with its latest write, and returns the pipeline's
// freshness state. Only pipelines that declare an SLA and have run read
// tables-state, so a pipeline without one costs no read. (A pipeline
// that never ran has no tables: on a fresh local warehouse the
// tables-state read would otherwise start Spark.) A failed tables-state
// read is returned as the error with state unknown and no rows: the
// summary does not claim a table it could not read is fresh or stale.
func (s *Service) pipelineFreshness(ctx context.Context, p PipelineInfo, now time.Time, hasRuns bool) ([]TableFreshness, string, error) {
	out := []TableFreshness{}
	slaByNode := s.slaByNode(p)
	if len(slaByNode) == 0 {
		return out, FreshnessNoSLA, nil
	}
	nodes := make([]string, 0, len(slaByNode))
	for n := range slaByNode {
		nodes = append(nodes, n)
	}
	res := &observability.TablesResult{}
	if hasRuns {
		r, err := s.TablesStateForDir(ctx, p.Name, p.Dir, summaryTablesLimit)
		if err != nil {
			return out, FreshnessUnknown, err
		}
		if r != nil {
			res = r
		}
	}
	seen := map[string]bool{}
	for _, t := range res.Rows {
		node := identutil.Sanitize(t.Node)
		sla, ok := slaByNode[node]
		if !ok {
			continue
		}
		seen[node] = true
		f := TableFreshness{Table: t.TableName, Node: t.Node, LastWriteAt: t.SnapshotTS, SLASeconds: sla}
		if ms := isoMillis(t.SnapshotTS); ms > 0 {
			f.AgeSeconds = int64(now.Sub(time.UnixMilli(ms)).Seconds())
			if f.AgeSeconds < 0 {
				f.AgeSeconds = 0
			}
			f.Stale = f.AgeSeconds > sla
		} else {
			f.Stale = true
		}
		out = append(out, f)
	}
	// A node with an SLA whose table was never written is stale too.
	sort.Strings(nodes)
	for _, node := range nodes {
		if !seen[node] {
			out = append(out, TableFreshness{Table: node, Node: node, SLASeconds: slaByNode[node], Stale: true})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	state := FreshnessFresh
	for _, f := range out {
		if f.Stale {
			state = FreshnessStale
			break
		}
	}
	return out, state, nil
}

// workspaceFreshness rolls the pipelines' freshness up: stale when any is
// stale, else unknown when any is unknown (with the reason, said once
// when the pipelines share it), else fresh when any declares an SLA,
// else no_sla.
func workspaceFreshness(pipes []PipelineSummary) (string, string) {
	var stale, fresh bool
	var unknown []PipelineSummary
	for _, p := range pipes {
		switch p.FreshnessState {
		case FreshnessStale:
			stale = true
		case FreshnessFresh:
			fresh = true
		case FreshnessUnknown:
			unknown = append(unknown, p)
		}
	}
	switch {
	case stale:
		return FreshnessStale, ""
	case len(unknown) == 1:
		return FreshnessUnknown, unknown[0].Name + ": " + unknown[0].FreshnessReason
	case len(unknown) > 1:
		reason := unknown[0].FreshnessReason
		for _, p := range unknown[1:] {
			if p.FreshnessReason != reason {
				reason = "history or table state unreadable"
				break
			}
		}
		return FreshnessUnknown, fmt.Sprintf("%d pipelines: %s", len(unknown), reason)
	case fresh:
		return FreshnessFresh, ""
	}
	return FreshnessNoSLA, ""
}

// summaryDeploymentInfo is what the workspace's Terraform state says about
// where it is deployed.
type summaryDeploymentInfo struct {
	// undeployed: the state was read and has no pipeline_bucket output
	// (or there is no state at all).
	undeployed bool
	region     string
	account    string
}

// arnRe matches the region and account of the first regional ARN.
var arnRe = regexp.MustCompile(`"arn:aws[a-z-]*:[a-z0-9-]+:([a-z]{2}(?:-[a-z]+)+-\d):(\d{12}):`)

// summaryDeployment reads the workspace stack's Terraform state once. A
// state read that fails (no credentials for a remote backend) is not
// "undeployed": the per-pipeline reads then name the real cause.
func (s *Service) summaryDeployment(ctx context.Context) summaryDeploymentInfo {
	var out summaryDeploymentInfo
	data, err := workspace.ReadStackState(ctx, s.workspace, s.workspace)
	if err != nil {
		return out
	}
	if os.Getenv("ATHENA_OUTPUT_BUCKET") == "" && workspace.StateOutput(data, "pipeline_bucket") == "" {
		out.undeployed = true
		return out
	}
	if m := arnRe.FindSubmatch(data); m != nil {
		out.region, out.account = string(m[1]), string(m[2])
	}
	return out
}

// summaryRun projects one runs row and its node rows onto a SummaryRun.
func summaryRun(pipeline string, r observability.Run, nodes []observability.NodeRun) SummaryRun {
	sr := SummaryRun{
		RunID:      r.RunID,
		Pipeline:   pipeline,
		Result:     runResult(r.Status),
		StartedAt:  r.StartedAt,
		EndedAt:    r.EndedAt,
		FailedStep: r.FailedStep,
		Error:      r.ErrorMsg,
	}
	if r.DurationMs != nil {
		sr.DurationMs = *r.DurationMs
	} else if st, en := isoMillis(r.StartedAt), isoMillis(r.EndedAt); st > 0 && en >= st {
		sr.DurationMs = en - st
	}
	agg := pricing.AggregateCost(pipeline, nodes)
	sr.Records = agg.TotalRecords
	sr.CostUSD = agg.TotalCostUSD
	return sr
}

// summaryNodes orders a run's node rows by start time.
func summaryNodes(rows []observability.NodeRun) []SummaryNode {
	out := make([]SummaryNode, 0, len(rows))
	for _, nr := range rows {
		n := SummaryNode{
			Node:       nr.Node,
			Status:     nr.Status,
			StartedAt:  nr.StartedAt,
			OutputRows: nr.OutputRows,
			Error:      nr.ErrorMsg,
		}
		if nr.DurationMs != nil {
			n.DurationMs = *nr.DurationMs
		}
		out = append(out, n)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].StartedAt != out[j].StartedAt {
			return isoMillis(out[i].StartedAt) < isoMillis(out[j].StartedAt)
		}
		return out[i].Node < out[j].Node
	})
	return out
}

// runResult maps a run status (SFN or run-marker vocabulary) onto the
// summary's three results.
func runResult(status string) string {
	switch strings.ToUpper(status) {
	case "SUCCEEDED":
		return "succeeded"
	case "RUNNING", "PENDING", "":
		return "running"
	default: // FAILED, TIMED_OUT, ABORTED
		return "failed"
	}
}

// worseStatus returns the more severe of two summary statuses ("" is
// below ok).
func worseStatus(a, b string) string {
	rank := map[string]int{"": 0, SummaryOK: 1, SummaryWarn: 2, SummaryFail: 3}
	if rank[b] > rank[a] {
		return b
	}
	return a
}

// summaryDays returns the `days` UTC dates ending today, oldest first.
func summaryDays(now time.Time, days int) []string {
	out := make([]string, days)
	for i := 0; i < days; i++ {
		out[i] = now.AddDate(0, 0, -(days - 1 - i)).Format("2006-01-02")
	}
	return out
}

// runsPerDay counts finished runs per UTC start day for the last `days`
// days, oldest first. Every day is present, with zeros on days with no run.
func runsPerDay(runs []SummaryRun, now time.Time, days int) []SummaryDayRuns {
	dates := summaryDays(now, days)
	idx := make(map[string]int, len(dates))
	out := make([]SummaryDayRuns, len(dates))
	for i, d := range dates {
		idx[d] = i
		out[i] = SummaryDayRuns{Date: d}
	}
	for _, r := range runs {
		ms := isoMillis(r.StartedAt)
		if ms == 0 {
			continue
		}
		i, ok := idx[time.UnixMilli(ms).UTC().Format("2006-01-02")]
		if !ok {
			continue
		}
		switch r.Result {
		case "succeeded":
			out[i].Succeeded++
		case "failed":
			out[i].Failed++
		}
	}
	return out
}

// costPerDay sums run compute cost per UTC start day for the last `days`
// days, oldest first.
func costPerDay(runs []SummaryRun, now time.Time, days int) []SummaryDayCost {
	dates := summaryDays(now, days)
	idx := make(map[string]int, len(dates))
	out := make([]SummaryDayCost, len(dates))
	for i, d := range dates {
		idx[d] = i
		out[i] = SummaryDayCost{Date: d}
	}
	for _, r := range runs {
		ms := isoMillis(r.StartedAt)
		if ms == 0 {
			continue
		}
		if i, ok := idx[time.UnixMilli(ms).UTC().Format("2006-01-02")]; ok {
			out[i].CostUSD += r.CostUSD
		}
	}
	return out
}

var (
	rateExprRe = regexp.MustCompile(`^rate\(\s*(\d+)\s+(minute|minutes|hour|hours|day|days)\s*\)$`)
	cronExprRe = regexp.MustCompile(`^cron\(([^)]*)\)$`)
)

// scheduleInterval reads the interval an EventBridge schedule expression
// implies: rate(N unit) exactly; cron(...) as hourly when the hour field
// is "*", daily when the hour is fixed and every day matches, weekly when
// one weekday is named. 0 when the expression is anything else.
func scheduleInterval(expr string) time.Duration {
	expr = strings.TrimSpace(expr)
	if m := rateExprRe.FindStringSubmatch(expr); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil || n <= 0 {
			return 0
		}
		switch strings.TrimSuffix(m[2], "s") {
		case "minute":
			return time.Duration(n) * time.Minute
		case "hour":
			return time.Duration(n) * time.Hour
		case "day":
			return time.Duration(n) * 24 * time.Hour
		}
		return 0
	}
	m := cronExprRe.FindStringSubmatch(expr)
	if m == nil {
		return 0
	}
	// AWS cron: minutes hours day-of-month month day-of-week year.
	f := strings.Fields(m[1])
	if len(f) != 6 {
		return 0
	}
	every := func(s string) bool { return s == "*" || s == "?" }
	if !every(f[2]) || !every(f[3]) {
		return 0
	}
	switch {
	case f[1] == "*":
		return time.Hour
	case strings.ContainsAny(f[1], ",/-"):
		return 0
	case every(f[4]):
		return 24 * time.Hour
	case !strings.ContainsAny(f[4], ",-/#L"):
		return 7 * 24 * time.Hour
	}
	return 0
}
