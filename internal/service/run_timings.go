package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"time"

	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/pathutil"
	"github.com/vesahyp/clavesa/internal/runtiming"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// RunTimingsQuery bounds a run-timing history read. Limit keeps the most
// recent Limit runs (0 = all); Since keeps runs that started within Since
// of now (0 = no bound).
type RunTimingsQuery struct {
	Limit int
	Since time.Duration
}

// cloudTimingRowCap is how many node_runs rows a cloud timing read asks
// Athena for. Runs are assembled from rows, so the row count bounds how
// far back a cloud history reaches.
const cloudTimingRowCap = 2000

// RunTimings returns the timing records of the pipeline at dir, oldest
// first. The CLI (`clavesa pipeline timings`) and GET /data/run-timings
// both reach this one read (ADR-015).
//
// On a local warehouse each run's `_progress/<run>/_timing.json` is the
// source. On a cloud warehouse there are no Go-side run phases, so one
// record per run is assembled from the node_runs rows: every run-level
// phase is 0 except `nodes`, compute is "cloud", and each node's Timing is
// the `timing` column verbatim (ADR-014: same shape either way).
func (s *Service) RunTimings(ctx context.Context, dir string, q RunTimingsQuery) ([]runtiming.Record, error) {
	pipeline := filepath.Base(pathutil.ResolveDir(s.workspace, dir))
	var recs []runtiming.Record
	if workspace.LoadWarehouse(s.workspace) == workspace.WarehouseLocal {
		recs = s.localRunTimings(pipeline)
	} else {
		var err error
		recs, err = s.cloudRunTimings(ctx, dir, pipeline)
		if err != nil {
			return nil, err
		}
	}
	return runtiming.Window(recs, q.Limit, q.Since, time.Now()), nil
}

// localRunTimings reads every `_timing.json` under the workspace
// warehouse's `_progress` tree and keeps the named pipeline's records.
func (s *Service) localRunTimings(pipeline string) []runtiming.Record {
	pattern := filepath.Join(workspace.LocalWarehouseDir(s.workspace), "_progress", "*", runtiming.FileName)
	paths, _ := filepath.Glob(pattern)
	all := runtiming.ReadFiles(paths)
	out := make([]runtiming.Record, 0, len(all))
	for _, r := range all {
		if r.Pipeline == pipeline {
			out = append(out, r)
		}
	}
	return out
}

// cloudRunTimings assembles one record per run from the pipeline's
// node_runs rows.
func (s *Service) cloudRunTimings(ctx context.Context, dir, pipeline string) ([]runtiming.Record, error) {
	prov, err := s.dashboardProvider()
	if err != nil {
		return nil, err
	}
	res, err := prov.NodeRuns(ctx, observability.NodeRunsQuery{
		PipelineName:   pipeline,
		Database:       s.systemGlueDB(),
		PipelineDir:    dir,
		Limit:          cloudTimingRowCap,
		IncludeMetrics: true,
	})
	if err != nil {
		return nil, err
	}
	wsName := s.workspaceName()
	if wsName == "" {
		wsName = filepath.Base(s.workspace)
	}
	wsDir, err := filepath.Abs(s.workspace)
	if err != nil {
		wsDir = s.workspace
	}
	return recordsFromNodeRuns(res.Rows, wsName, wsDir, pipeline), nil
}

// recordsFromNodeRuns groups node_runs rows by run_id into timing
// records, oldest first. Nodes within a run are ordered by start time.
func recordsFromNodeRuns(rows []observability.NodeRun, wsName, wsDir, pipeline string) []runtiming.Record {
	byRun := map[string][]observability.NodeRun{}
	var order []string
	for _, r := range rows {
		if r.RunID == "" {
			continue
		}
		if _, ok := byRun[r.RunID]; !ok {
			order = append(order, r.RunID)
		}
		byRun[r.RunID] = append(byRun[r.RunID], r)
	}
	out := make([]runtiming.Record, 0, len(order))
	for _, runID := range order {
		nrs := byRun[runID]
		nodes := make([]runtiming.Node, 0, len(nrs))
		var minStart, maxEnd int64
		var nodesMs int64
		status := "SUCCEEDED"
		for _, nr := range nrs {
			n := runtiming.Node{Node: nr.Node, Status: nr.Status, OutputRows: nr.OutputRows}
			n.StartedMs = isoMillis(nr.StartedAt)
			n.EndedMs = isoMillis(nr.EndedAt)
			if nr.DurationMs != nil {
				n.DurationMs = *nr.DurationMs
			} else if n.StartedMs > 0 && n.EndedMs >= n.StartedMs {
				n.DurationMs = n.EndedMs - n.StartedMs
			}
			if runtiming.HasTiming(json.RawMessage(nr.Timing)) {
				n.Timing = json.RawMessage(nr.Timing)
			}
			if nr.Status == "failed" {
				status = "FAILED"
			}
			if n.StartedMs > 0 && (minStart == 0 || n.StartedMs < minStart) {
				minStart = n.StartedMs
			}
			if n.EndedMs > maxEnd {
				maxEnd = n.EndedMs
			}
			nodesMs += n.DurationMs
			nodes = append(nodes, n)
		}
		sort.SliceStable(nodes, func(i, j int) bool { return nodes[i].StartedMs < nodes[j].StartedMs })
		rec := runtiming.Record{
			Schema:       runtiming.SchemaVersion,
			RunID:        runID,
			Workspace:    wsName,
			WorkspaceDir: wsDir,
			Pipeline:     pipeline,
			Status:       status,
			Compute:      "cloud",
			Phases:       runtiming.Phases{Nodes: nodesMs},
			Nodes:        nodes,
		}
		if minStart > 0 {
			rec.StartedAt = time.UnixMilli(minStart).Format(time.RFC3339)
		}
		if maxEnd > 0 {
			rec.EndedAt = time.UnixMilli(maxEnd).Format(time.RFC3339)
		}
		if minStart > 0 && maxEnd >= minStart {
			rec.TotalMs = maxEnd - minStart
		}
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].StartedAt != out[j].StartedAt {
			return isoMillis(out[i].StartedAt) < isoMillis(out[j].StartedAt)
		}
		return out[i].RunID < out[j].RunID
	})
	return out
}

// isoMillis parses an ISO-8601 timestamp (Athena's to_iso8601, with or
// without fractional seconds) to Unix ms; 0 when it does not parse.
func isoMillis(s string) int64 {
	if s == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0
	}
	return t.UnixMilli()
}

// AllRunTimings reads every record in the cross-workspace history dir
// (runtiming.HistoryDir), so it needs no workspace. Backs
// `clavesa pipeline timings --all`.
func AllRunTimings(q RunTimingsQuery) (runtiming.History, error) {
	dir := runtiming.HistoryDir()
	h := runtiming.History{
		GeneratedAt: time.Now().Format(time.RFC3339),
		HistoryDir:  dir,
		Runs:        []runtiming.Record{},
	}
	if dir == "" {
		return h, nil
	}
	recs, err := runtiming.ReadDir(dir)
	if err != nil {
		return h, err
	}
	h.Runs = runtiming.Window(recs, q.Limit, q.Since, time.Now())
	return h, nil
}
