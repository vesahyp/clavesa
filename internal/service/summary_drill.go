package service

import (
	"context"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/runtiming"
)

// Bounds of the drill-down a local-warehouse workspace publishes with its
// summary (ADR-032 section 2): enough for the hosted Pipeline and Run
// pages with no live call, and a summary well under 300 KB.
const (
	// DrillRuns is how many runs per pipeline keep their runs row: the
	// Pipeline page's run list and duration chart read 50.
	DrillRuns = 50
	// DrillNodeRuns is how many of the newest runs keep their per-node
	// rows (the Run page's node timeline and rows in and out).
	DrillNodeRuns = 10
	// DrillTimings is how many run-timing records per pipeline are kept
	// (the "Where the time goes" card reads 20).
	DrillTimings = 20
	// DrillLogLines is the log tail kept for each failed run among the
	// DrillNodeRuns newest runs.
	DrillLogLines = 50
	// DrillVersion is the version of the drill format.
	DrillVersion = 1
)

// SummaryDrill is the drill-down a local-warehouse workspace publishes
// beside its summary, so the hosted UI can answer the Pipeline and Run
// pages from the stored file alone. Every row uses the live handlers'
// own types (observability.Run, observability.NodeRun, runtiming.Record,
// observability.ExecutionLogsResult), so the hosted answers have the
// shapes the UI already reads (ADR-014). Nothing in it names a path on
// the Mac.
type SummaryDrill struct {
	Version int `json:"version"`
	// CLIVersion is the clavesa that published it.
	CLIVersion string `json:"cli_version"`
	// Tree is the read-only tree the hosted pages read, keyed by
	// workspace-relative path: clavesa.json and each pipeline's .tf and
	// .tfvars files (the allow-list of the cloud tree copy,
	// HostedTreeFiles). Never state, .env files or credentials.
	Tree map[string]string `json:"tree"`
	// Pipelines is keyed by the pipeline's workspace-relative dir.
	Pipelines map[string]PipelineDrill `json:"pipelines"`
	// Snapshot is the catalog and dashboard snapshot (ADR-036); nil from
	// a clavesa older than it.
	Snapshot *DrillSnapshot `json:"snapshot,omitempty"`
}

// PipelineDrill is one pipeline's part of a SummaryDrill.
type PipelineDrill struct {
	Name string `json:"name"`
	// Runs are the newest DrillRuns runs rows, newest first.
	Runs []observability.Run `json:"runs"`
	// NodeRuns are the node rows of the newest DrillNodeRuns runs.
	NodeRuns []observability.NodeRun `json:"node_runs"`
	// Timings are the newest DrillTimings run-timing records, oldest
	// first, with workspace_dir cleared.
	Timings []runtiming.Record `json:"timings"`
	// Logs holds the last DrillLogLines log lines of each failed run among
	// the DrillNodeRuns newest, keyed by run id. A local run has one log
	// for all its nodes.
	Logs map[string]observability.ExecutionLogsResult `json:"logs,omitempty"`
}

// SummaryDrill collects the drill-down of the workspace. A read that
// fails leaves that part empty; the summary itself is already computed,
// so the drill never fails the publish.
func (s *Service) SummaryDrill(ctx context.Context, sum *WorkspaceSummary) *SummaryDrill {
	d := &SummaryDrill{
		Version:    DrillVersion,
		CLIVersion: ModuleVersion,
		Tree:       map[string]string{},
		Pipelines:  map[string]PipelineDrill{},
	}
	dirs := make([]string, 0, len(sum.Pipelines))
	for _, p := range sum.Pipelines {
		dirs = append(dirs, p.Dir)
	}
	if files, err := HostedTreeFiles(s.workspace, dirs); err == nil {
		for _, f := range files {
			if f.Path == "" {
				continue // the generated environment.json: the reader writes its own
			}
			// The workspace's shape, and its dashboard specs (ADR-036:
			// their stored widget results are in Snapshot). Sources,
			// notebooks and the runner requirements stay on the Mac.
			isSpec := strings.HasPrefix(f.Key, ".clavesa/dashboards/") || strings.HasPrefix(f.Key, "dashboards/")
			if !strings.HasSuffix(f.Key, ".tf") && !strings.HasSuffix(f.Key, ".tfvars") && f.Key != "clavesa.json" && !isSpec {
				continue
			}
			if body, err := os.ReadFile(f.Path); err == nil {
				d.Tree[f.Key] = string(body)
			}
		}
	}
	for _, p := range sum.Pipelines {
		d.Pipelines[p.Dir] = s.pipelineDrill(ctx, p)
	}
	d.Snapshot = s.drillSnapshot(ctx, time.Now())
	return d
}

func (s *Service) pipelineDrill(ctx context.Context, p PipelineSummary) PipelineDrill {
	pd := PipelineDrill{
		Name:     p.Name,
		Runs:     []observability.Run{},
		NodeRuns: []observability.NodeRun{},
		Timings:  []runtiming.Record{},
	}
	if res, err := s.Runs(ctx, p.Dir, 0); err == nil {
		runs := append([]observability.Run(nil), res.Rows...)
		sort.SliceStable(runs, func(i, j int) bool { return isoMillis(runs[i].StartedAt) > isoMillis(runs[j].StartedAt) })
		if len(runs) > DrillRuns {
			runs = runs[:DrillRuns]
		}
		pd.Runs = runs
	}
	keep := map[string]bool{}
	var failed []string
	for i, r := range pd.Runs {
		if i >= DrillNodeRuns {
			break
		}
		keep[r.RunID] = true
		if r.SfExecutionARN != "" {
			keep[r.SfExecutionARN] = true
		}
		if runResult(r.Status) == "failed" {
			failed = append(failed, r.RunID)
		}
	}
	if res, err := s.NodeRuns(ctx, p.Dir, "", summaryNodeRunLimit); err == nil {
		for _, nr := range res.Rows {
			if keep[nr.RunID] || (nr.SfExecutionARN != "" && keep[nr.SfExecutionARN]) {
				pd.NodeRuns = append(pd.NodeRuns, nr)
			}
		}
	}
	if recs, err := s.RunTimings(ctx, p.Dir, RunTimingsQuery{Limit: DrillTimings}); err == nil {
		for _, r := range recs {
			r.WorkspaceDir = ""
			pd.Timings = append(pd.Timings, r)
		}
	}
	for _, run := range failed {
		res, err := s.ExecutionLogs(ctx, p.Dir, run, "run", DrillLogLines)
		if err != nil || res == nil {
			continue
		}
		ev := res.Events
		if len(ev) > DrillLogLines {
			ev = ev[len(ev)-DrillLogLines:]
		}
		if pd.Logs == nil {
			pd.Logs = map[string]observability.ExecutionLogsResult{}
		}
		// LogGroup and FunctionName name the log file on the Mac: replaced
		// by labels that say where the lines came from.
		pd.Logs[run] = observability.ExecutionLogsResult{
			Source:       res.Source,
			LogGroup:     path.Join(p.Dir, "_bundle.log"),
			FunctionName: run,
			Events:       ev,
			Truncated:    res.Truncated || len(res.Events) > len(ev),
		}
	}
	return pd
}
