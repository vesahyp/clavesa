package hosted

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/runtiming"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/ui"
	"github.com/vesahyp/clavesa/internal/workspace"
	"github.com/vesahyp/clavesa/internal/wsdeps"
)

// A local-warehouse workspace (ADR-032 section 2, "pushed from the Mac")
// has no live read path: the hosted UI has only the summary the Mac
// published, with its drill-down (service.SummaryDrill). The Lambda turns
// that file into a workspace it can serve with the SAME handlers as any
// other workspace (ui.BuildWorkspaceMux), so the JSON the pages read has
// the live shapes by construction (ADR-014):
//
//   - the drill's tree (clavesa.json and the pipeline .tf files) is
//     written to a directory, with a local-warehouse environment.json and
//     the run-timing records under .clavesa/warehouse/_progress/<run>/;
//   - the local-warehouse observability provider is summaryProvider,
//     which answers runs, node runs, run states and log tails from the
//     drill;
//   - only the routes the summary backs answer (localRoutes); every other
//     path is a JSON 404 that says the read is not kept.

// ErrNoSummary means no summary is stored for the workspace yet.
var ErrNoSummary = errors.New("no summary has been published for this workspace yet")

// ErrNoDrill means the stored summary carries no drill-down (published by
// a clavesa older than the drill format).
var ErrNoDrill = errors.New("the published summary carries no drill-down; publish it again with a newer clavesa")

// localRoutes are the GET routes a local workspace answers, by path. They
// are what the Workspace, Pipeline and Run pages read; the summary keeps
// the data behind each. GET /workspace/summary is served by Server from
// the stored file.
var localRoutes = map[string]bool{
	"/workspace":                 true,
	"/workspace/environment":     true,
	"/workspace/hosted":          true,
	"/workspace/aws-profile":     true,
	"/runtime/identity":          true,
	"/runtime/workers":           true,
	"/pipelines":                 true,
	"/pipeline":                  true,
	"/pipeline/vars":             true,
	"/pipeline/module-version":   true,
	"/pipeline/status":           true,
	"/pipeline/execution/states": true,
	"/pipeline/execution/logs":   true,
	"/data/runs":                 true,
	"/data/node-runs":            true,
	"/data/run-timings":          true,
	"/data/pipeline-cost":        true,
	"/data/rightsize":            true,
}

// LocalRoutes returns the paths a local workspace answers, sorted.
func LocalRoutes() []string {
	out := make([]string, 0, len(localRoutes))
	for p := range localRoutes {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// localOnly answers 404 for every path outside localRoutes. Mounted
// behind StripPrefix, so paths are bare (/data/runs).
func localOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !localRoutes[r.URL.Path] {
			httputil.WriteError(w, http.StatusNotFound,
				"not kept for local workspaces: the hosted UI has only the summary the Mac publishes ("+r.URL.Path+")")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ParseLocalSummary decodes a stored summary and its drill-down.
func ParseLocalSummary(body []byte) (*service.WorkspaceSummary, error) {
	if body == nil {
		return nil, ErrNoSummary
	}
	var sum service.WorkspaceSummary
	if err := json.Unmarshal(body, &sum); err != nil {
		return nil, fmt.Errorf("stored summary: %w", err)
	}
	if sum.Drill == nil {
		return &sum, ErrNoDrill
	}
	return &sum, nil
}

// OpenLocal builds the summary-backed workspace of entry e from its
// stored summary body, in root (replaced). The caller caches it.
func OpenLocal(ctx context.Context, root string, e Entry, body []byte) (*Workspace, *wsdeps.Deps, error) {
	sum, err := ParseLocalSummary(body)
	if err != nil {
		return nil, nil, err
	}
	if err := writeLocalTree(root, sum.Drill); err != nil {
		return nil, nil, err
	}
	prov := newSummaryProvider(sum.Drill)
	deps := wsdeps.New(ctx, root, wsdeps.Options{LocalProvider: prov})
	ws := &Workspace{
		Entry: e,
		Root:  root,
		Handler: localOnly(ui.BuildWorkspaceMux(ui.WorkspaceDeps{
			Deps:        deps,
			ReadOnly:    true,
			SummaryOnly: true,
		})),
		Summarize: func(context.Context) (*service.WorkspaceSummary, error) {
			return sum, nil
		},
	}
	return ws, deps, nil
}

// writeLocalTree writes the drill's tree to root: the tree files, an
// environment.json on the local warehouse, and each run-timing record
// where the local run-timings read finds it.
func writeLocalTree(root string, d *service.SummaryDrill) error {
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	clean := filepath.Clean(root) + string(filepath.Separator)
	write := func(rel string, body []byte) error {
		dst := filepath.Join(root, filepath.FromSlash(rel))
		if !strings.HasPrefix(dst, clean) {
			return nil // a key that climbs out of the tree
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dst, body, 0o644)
	}
	if _, ok := d.Tree["clavesa.json"]; !ok {
		return fmt.Errorf("the published summary's tree has no clavesa.json")
	}
	for rel, body := range d.Tree {
		if err := write(rel, []byte(body)); err != nil {
			return err
		}
	}
	if err := workspace.WriteWarehouse(root, workspace.WarehouseLocal); err != nil {
		return err
	}
	for _, p := range d.Pipelines {
		for _, rec := range p.Timings {
			if rec.RunID == "" || strings.ContainsAny(rec.RunID, `/\`) {
				continue
			}
			b, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			rel := filepath.Join(".clavesa", "warehouse", "_progress", rec.RunID, runtiming.FileName)
			if err := write(filepath.ToSlash(rel), b); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// summaryProvider
// ---------------------------------------------------------------------------

// errNotKept is what summaryProvider answers for reads the summary does
// not keep. localOnly keeps those routes from reaching it.
var errNotKept = errors.New("not kept for local workspaces: the hosted UI has only the published summary")

// summaryProvider is the observability.Provider of a local workspace in
// the hosted UI: runs, node runs, run states and log tails from the
// published drill-down.
type summaryProvider struct {
	drill *service.SummaryDrill
}

func newSummaryProvider(d *service.SummaryDrill) *summaryProvider {
	return &summaryProvider{drill: d}
}

// pipeline finds the pipeline a query is about: by its dir (absolute in
// the tree or workspace-relative) when given, else by its name.
func (p *summaryProvider) pipeline(name, dir string) (service.PipelineDrill, bool) {
	if dir != "" {
		base := filepath.Base(dir)
		for k, pd := range p.drill.Pipelines {
			if k == dir || filepath.Base(k) == base {
				return pd, true
			}
		}
	}
	for k, pd := range p.drill.Pipelines {
		if pd.Name == name || filepath.Base(k) == name {
			return pd, true
		}
	}
	return service.PipelineDrill{}, false
}

func (p *summaryProvider) Runs(_ context.Context, q observability.RunsQuery) (*observability.RunsResult, error) {
	pd, _ := p.pipeline(q.PipelineName, q.PipelineDir)
	rows := append([]observability.Run{}, pd.Runs...)
	trunc := false
	if q.Limit > 0 && len(rows) > q.Limit {
		rows, trunc = rows[:q.Limit], true
	}
	return &observability.RunsResult{Rows: rows, Truncated: trunc}, nil
}

func (p *summaryProvider) NodeRuns(_ context.Context, q observability.NodeRunsQuery) (*observability.NodeRunsResult, error) {
	pd, _ := p.pipeline(q.PipelineName, q.PipelineDir)
	rows := []observability.NodeRun{}
	for _, nr := range pd.NodeRuns {
		if q.Node != "" && nr.Node != q.Node {
			continue
		}
		if q.SfExecutionARN != "" && nr.SfExecutionARN != q.SfExecutionARN && nr.RunID != q.SfExecutionARN {
			continue
		}
		rows = append(rows, nr)
	}
	trunc := false
	if q.Limit > 0 && len(rows) > q.Limit {
		rows, trunc = rows[:q.Limit], true
	}
	return &observability.NodeRunsResult{Rows: rows, Truncated: trunc}, nil
}

// stateStatus maps a node row's status onto the run-state vocabulary.
func stateStatus(s string) string {
	switch strings.ToLower(s) {
	case "ok", "succeeded", "success":
		return "SUCCEEDED"
	case "failed", "error":
		return "FAILED"
	case "running":
		return "RUNNING"
	}
	return strings.ToUpper(s)
}

func (p *summaryProvider) ExecutionStates(_ context.Context, q observability.ExecutionStatesQuery) (*observability.ExecutionStatesResult, error) {
	dir, runID := observability.SplitExecRef(q.ExecutionRef)
	out := &observability.ExecutionStatesResult{States: map[string]observability.StateStatus{}}
	var pd service.PipelineDrill
	var ok bool
	if dir != "" {
		pd, ok = p.pipeline("", dir)
	} else if runID != "" {
		for _, c := range p.drill.Pipelines {
			for _, r := range c.Runs {
				if r.RunID == runID {
					pd, ok = c, true
				}
			}
		}
	}
	if !ok || len(pd.Runs) == 0 {
		return out, nil
	}
	run := pd.Runs[0] // newest first
	if runID != "" {
		found := false
		for _, r := range pd.Runs {
			if r.RunID == runID || r.SfExecutionARN == runID {
				run, found = r, true
				break
			}
		}
		if !found {
			return out, nil
		}
	}
	out.Status, out.RunID, out.StartedAt = run.Status, run.RunID, run.StartedAt
	for _, nr := range pd.NodeRuns {
		if nr.RunID != run.RunID && (nr.SfExecutionARN == "" || nr.SfExecutionARN != run.SfExecutionARN) {
			continue
		}
		out.States[nr.Node] = observability.StateStatus{Status: stateStatus(nr.Status)}
	}
	return out, nil
}

func (p *summaryProvider) ExecutionLogs(_ context.Context, q observability.ExecutionLogsQuery) (*observability.ExecutionLogsResult, error) {
	dir, runID := observability.SplitExecRef(q.ExecutionRef)
	pd, _ := p.pipeline("", dir)
	if runID == "" && len(pd.Runs) > 0 {
		runID = pd.Runs[0].RunID
	}
	res, ok := pd.Logs[runID]
	if !ok {
		// Log tails are kept only for failed runs: an empty answer, in
		// the shape the page already renders as "no log lines".
		return &observability.ExecutionLogsResult{Source: observability.LogSourceLocal, Events: []observability.LogEvent{}}, nil
	}
	if res.Events == nil {
		res.Events = []observability.LogEvent{}
	}
	if q.MaxLines > 0 && len(res.Events) > q.MaxLines {
		res.Events = res.Events[len(res.Events)-q.MaxLines:]
		res.Truncated = true
	}
	return &res, nil
}

func (p *summaryProvider) Tables(context.Context, observability.TablesQuery) (*observability.TablesResult, error) {
	return nil, errNotKept
}

func (p *summaryProvider) Snapshots(context.Context, observability.SnapshotsQuery) (*observability.SnapshotsResult, error) {
	return nil, errNotKept
}

func (p *summaryProvider) ColumnStats(context.Context, observability.ColumnStatsQuery) (*observability.ColumnStatsResult, error) {
	return nil, errNotKept
}

func (p *summaryProvider) SampleTable(context.Context, observability.SampleTableQuery) (*observability.SampleTableResult, error) {
	return nil, errNotKept
}

func (p *summaryProvider) Query(context.Context, observability.QueryQuery) (*observability.QueryResult, error) {
	return nil, errNotKept
}

func (p *summaryProvider) Exec(context.Context, observability.ExecQuery) error {
	return errNotKept
}
