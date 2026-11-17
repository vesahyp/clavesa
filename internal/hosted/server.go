package hosted

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	pathpkg "path"
	"strings"
	"sync"
	"time"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/ui"
)

// SummaryStore holds one stored summary per workspace
// (s3://<data bucket>/summaries/<name>.json).
type SummaryStore interface {
	// Get returns the stored summary, or (nil, nil) when there is none.
	Get(ctx context.Context, name string) ([]byte, error)
	Put(ctx context.Context, name string, body []byte) error
}

// StatsKey is the data-bucket key of the hosted UI's own traffic rollup:
// one JSON array of per-day rows, written by the site-traffic workspace's
// `traffic` pipeline (node ui_json, site/analytics/clavesa) from the
// distribution's CloudFront logs. GET /api/stats serves it unchanged.
const StatsKey = "stats/ui.json"

// StatsReader reads the stored traffic rollup; (nil, nil) when there is
// none yet.
type StatsReader interface {
	GetStats(ctx context.Context) ([]byte, error)
}

// Workspace is one opened cloud workspace: its tree copy on local disk,
// its read-only API and its service (for the summary refresh). Every AWS
// client behind both reads through the workspace's reader role.
type Workspace struct {
	Entry   Entry
	Root    string
	Handler http.Handler
	// Summarize is service.WorkspaceSummary over the tree copy.
	Summarize func(ctx context.Context) (*service.WorkspaceSummary, error)
}

// Opener opens a cloud workspace; *Workspaces is the real one.
type Opener interface {
	Open(ctx context.Context, e Entry) (*Workspace, error)
}

// Server is the hosted Lambda: the HTTP API and the refresh handler.
type Server struct {
	registry  []Entry
	byName    map[string]Entry
	wellKnown service.HostedWellKnown
	store     SummaryStore
	opener    Opener
	handler   http.Handler

	// Stats reads stats/ui.json for GET /api/stats; nil answers an empty
	// list.
	Stats StatsReader

	// RetryWait is the pause between summary attempts while a finished
	// run is not in the summary yet; RetryAttempts how many attempts.
	RetryWait     time.Duration
	RetryAttempts int
	Logf          func(format string, args ...any)
	// Now is the clock of the staleness check; nil is time.Now.
	Now func() time.Time
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// NewServer returns the server over the registry.
func NewServer(reg []Entry, wk service.HostedWellKnown, store SummaryStore, opener Opener) *Server {
	s := &Server{
		registry:      reg,
		byName:        make(map[string]Entry, len(reg)),
		wellKnown:     wk,
		store:         store,
		opener:        opener,
		RetryWait:     15 * time.Second,
		RetryAttempts: 5,
		Logf:          log.Printf,
	}
	for _, e := range reg {
		s.byName[e.Name] = e
	}
	// ReadOnlyGuard on the whole server, and BuildWorkspaceMux applies it
	// again per workspace: the Lambda never answers a write (ADR-032).
	s.handler = ui.ReadOnlyGuard(http.HandlerFunc(s.serve))
	return s
}

// ServeHTTP serves the hosted API, always behind ui.ReadOnlyGuard:
//
//	GET /.well-known/clavesa.json   the join settings (public)
//	GET /api/workspaces             the registry with the stored summaries
//	GET /api/stats                  the hosted UI's traffic rollup (stats/ui.json)
//	/w/<name>/api/workspace/summary the stored summary; ?live=1 computes it
//	/w/<name>/api/...               that workspace's BuildWorkspaceMux
//
// Anything else is a JSON 404.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	path := r.URL.Path
	switch {
	case path == service.HostedWellKnownPath:
		httputil.WriteJSON(w, http.StatusOK, s.wellKnown)
		return
	case path == "/api/workspaces":
		s.serveWorkspaces(w, r)
		return
	case path == "/api/stats":
		s.serveStats(w, r)
		return
	}
	name, rest, ok := splitWorkspacePath(path)
	if !ok || !(rest == "/api" || strings.HasPrefix(rest, "/api/")) {
		httputil.WriteError(w, http.StatusNotFound, "not found: "+path)
		return
	}
	e, known := s.byName[name]
	if !known {
		httputil.WriteError(w, http.StatusNotFound, "unknown workspace: "+name)
		return
	}
	if refusesPreview(pathpkg.Clean(strings.TrimPrefix(rest, "/api"))) {
		httputil.WriteError(w, http.StatusForbidden, previewRefusal)
		return
	}
	if e.PersonalData && refusesPersonalData(pathpkg.Clean(strings.TrimPrefix(rest, "/api")), r) {
		httputil.WriteError(w, http.StatusForbidden, personalDataRefusal)
		return
	}
	// A local workspace has only its stored summary: ?live=1 reads it too.
	if rest == "/api/workspace/summary" && (r.URL.Query().Get("live") != "1" || e.Kind == KindLocal) {
		if body, err := s.store.Get(r.Context(), name); err == nil && body != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
			return
		} else if err != nil {
			s.Logf("summary %s: read stored: %v", name, err)
		}
		// No stored summary yet: a cloud workspace computes it live.
		if e.Kind == KindLocal {
			httputil.WriteError(w, http.StatusNotFound, "workspace "+name+": "+ErrNoSummary.Error())
			return
		}
	}
	ws, err := s.opener.Open(r.Context(), e)
	if errors.Is(err, ErrNoSummary) || errors.Is(err, ErrNoDrill) {
		httputil.WriteError(w, http.StatusNotFound, "workspace "+name+": "+err.Error())
		return
	}
	if err != nil {
		s.Logf("open %s: %v", name, err)
		httputil.WriteError(w, http.StatusBadGateway, "workspace "+name+": "+err.Error())
		return
	}
	http.StripPrefix("/w/"+name+"/api", ws.Handler).ServeHTTP(&jsonNotFound{ResponseWriter: w, path: path}, r)
}

// jsonNotFound turns the ServeMux's plain-text "404 page not found" (a
// path no route of the workspace matches) into the JSON error every
// other API answer uses.
type jsonNotFound struct {
	http.ResponseWriter
	path    string
	swallow bool
}

func (j *jsonNotFound) WriteHeader(code int) {
	if code == http.StatusNotFound && strings.HasPrefix(j.Header().Get("Content-Type"), "text/plain") {
		j.swallow = true
		httputil.WriteError(j.ResponseWriter, http.StatusNotFound, "not found: "+j.path)
		return
	}
	j.ResponseWriter.WriteHeader(code)
}

func (j *jsonNotFound) Write(b []byte) (int, error) {
	if j.swallow {
		return len(b), nil
	}
	return j.ResponseWriter.Write(b)
}

// WorkspaceCard is one entry of GET /api/workspaces: the registry entry
// and its stored summary (null when none is stored yet). The fields the
// local multi-workspace router sends (name, slug, kind, region,
// api_base) keep their names, so the Workspaces page reads both.
type WorkspaceCard struct {
	Name         string          `json:"name"`
	Slug         string          `json:"slug"`
	Kind         string          `json:"kind"`
	Account      string          `json:"account"`
	Region       string          `json:"region"`
	APIBase      string          `json:"api_base"`
	Summary      json.RawMessage `json:"summary"`
	SummaryError string          `json:"summary_error,omitempty"`
	// SummaryStale is set for a local workspace whose stored summary is
	// older than its expected interval plus a quarter of it: the Mac did
	// not run (or could not publish). The card turns amber and says
	// "No summary from the Mac since <generated_at>".
	// SummaryIntervalSeconds is that expected interval: the shortest
	// pipeline trigger_schedule interval, else one day.
	SummaryStale           bool  `json:"summary_stale,omitempty"`
	SummaryIntervalSeconds int64 `json:"summary_interval_seconds,omitempty"`
}

// DefaultLocalInterval is the expected interval of a local workspace
// whose pipelines declare no trigger_schedule: the nightly cron.
const DefaultLocalInterval = 24 * time.Hour

// LocalSummaryStale reports whether a local workspace's stored summary
// is overdue at now, and the expected interval it was measured against:
// the shortest pipeline interval (DefaultLocalInterval when none
// declares one) plus a quarter of it as grace, the same grace as a
// pipeline's "no run inside the expected interval".
func LocalSummaryStale(body []byte, now time.Time) (bool, time.Duration) {
	var v struct {
		GeneratedAt string `json:"generated_at"`
		Pipelines   []struct {
			ExpectedIntervalSeconds int64 `json:"expected_interval_seconds"`
		} `json:"pipelines"`
	}
	interval := time.Duration(0)
	if json.Unmarshal(body, &v) == nil {
		for _, p := range v.Pipelines {
			if d := time.Duration(p.ExpectedIntervalSeconds) * time.Second; d > 0 && (interval == 0 || d < interval) {
				interval = d
			}
		}
	}
	if interval == 0 {
		interval = DefaultLocalInterval
	}
	g, err := time.Parse(time.RFC3339, v.GeneratedAt)
	if err != nil {
		return true, interval
	}
	return now.Sub(g) > interval+interval/4, interval
}

func (s *Server) serveWorkspaces(w http.ResponseWriter, r *http.Request) {
	cards := make([]WorkspaceCard, len(s.registry))
	var wg sync.WaitGroup
	for i, e := range s.registry {
		cards[i] = WorkspaceCard{Name: e.Name, Slug: e.Name, Kind: e.Kind, Account: e.Account,
			Region: e.Region, APIBase: "/w/" + e.Name + "/api", Summary: json.RawMessage("null")}
		wg.Add(1)
		go func(i int, name, kind string) {
			defer wg.Done()
			body, err := s.store.Get(r.Context(), name)
			switch {
			case err != nil:
				cards[i].SummaryError = "could not read the stored summary"
				s.Logf("summary %s: read stored: %v", name, err)
			case body != nil && json.Valid(body):
				cards[i].Summary = body
				if kind == KindLocal {
					stale, interval := LocalSummaryStale(body, s.now())
					cards[i].SummaryStale = stale
					cards[i].SummaryIntervalSeconds = int64(interval / time.Second)
				}
			}
		}(i, e.Name, e.Kind)
	}
	wg.Wait()
	httputil.WriteJSON(w, http.StatusOK, map[string]any{"workspaces": cards})
}

// serveStats answers the stored rollup as it is, or [] when none is
// stored yet. Only the data bucket is read: no Athena, no workspace.
func (s *Server) serveStats(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if s.Stats != nil {
		b, err := s.Stats.GetStats(r.Context())
		if err != nil {
			s.Logf("stats: read stored: %v", err)
			httputil.WriteError(w, http.StatusBadGateway, "could not read the stored stats")
			return
		}
		body = b
	}
	if body == nil || !json.Valid(body) {
		body = []byte("[]")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// splitWorkspacePath splits "/w/<name>/<rest>" into name and "/<rest>".
func splitWorkspacePath(path string) (name, rest string, ok bool) {
	tail, found := strings.CutPrefix(path, "/w/")
	if !found {
		return "", "", false
	}
	name, after, _ := strings.Cut(tail, "/")
	if name == "" {
		return "", "", false
	}
	return name, "/" + after, true
}

// ---------------------------------------------------------------------------
// Refresh
// ---------------------------------------------------------------------------

// RefreshFailedMarker starts the log line of every failed refresh; the
// CloudWatch metric filter in hosted/infra counts it for the alarm.
const RefreshFailedMarker = "REFRESH_FAILED"

// Refresh computes and stores the summaries an event asks for. A failed
// workspace is logged with RefreshFailedMarker and makes the call fail,
// after the other workspaces are done.
func (s *Server) Refresh(ctx context.Context, t RefreshTarget) error {
	var names []string
	if t.All {
		for _, e := range s.registry {
			if e.Kind == KindCloud {
				names = append(names, e.Name)
			}
		}
	} else {
		names = []string{t.Workspace}
	}
	var errs []error
	for _, n := range names {
		if err := s.refreshOne(ctx, n, t); err != nil {
			s.Logf("%s workspace=%s: %v", RefreshFailedMarker, n, err)
			errs = append(errs, fmt.Errorf("%s: %w", n, err))
		}
	}
	return errors.Join(errs...)
}

// storedGeneratedAt reads generated_at of a stored summary.
func storedGeneratedAt(body []byte) time.Time {
	var v struct {
		GeneratedAt string `json:"generated_at"`
	}
	if json.Unmarshal(body, &v) != nil {
		return time.Time{}
	}
	ts, _ := time.Parse(time.RFC3339, v.GeneratedAt)
	return ts
}

func (s *Server) refreshOne(ctx context.Context, name string, t RefreshTarget) error {
	e, ok := s.byName[name]
	if !ok {
		return fmt.Errorf("not in the registry")
	}
	if e.Kind != KindCloud {
		return fmt.Errorf("a local-warehouse workspace is published from the Mac, not refreshed here")
	}
	// Debounce: a stored summary computed after the event already shows
	// it. A burst of events for one run costs one computation.
	if !t.All && !t.Time.IsZero() {
		if body, err := s.store.Get(ctx, name); err == nil && body != nil {
			if g := storedGeneratedAt(body); g.After(t.Time) {
				s.Logf("refresh %s: skipped, stored summary from %s is newer than the event (%s)", name, g.Format(time.RFC3339), t.Time.Format(time.RFC3339))
				return nil
			}
		}
	}
	ws, err := s.opener.Open(ctx, e)
	if err != nil {
		return err
	}
	// Athena result reuse would hand back the result of a refresh minutes
	// ago, without the run that just ended.
	ctx = observability.WithFreshReads(ctx)
	var sum *service.WorkspaceSummary
	for attempt := 1; ; attempt++ {
		sum, err = ws.Summarize(ctx)
		if err != nil {
			return err
		}
		// A finished run reaches the runs table through the runs_writer,
		// which runs on the same event: wait until it is there.
		if !t.Terminal() || t.ExecutionARN == "" || summaryHasRun(sum, t.ExecutionARN) || attempt >= s.RetryAttempts {
			if t.Terminal() && t.ExecutionARN != "" && !summaryHasRun(sum, t.ExecutionARN) {
				s.Logf("refresh %s: run %s not in the summary after %d attempts; storing what there is", name, t.ExecutionARN, attempt)
			}
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.RetryWait):
		}
	}
	body, err := json.Marshal(sum)
	if err != nil {
		return err
	}
	if err := s.store.Put(ctx, name, body); err != nil {
		return fmt.Errorf("store summary: %w", err)
	}
	s.Logf("refresh %s: stored summary %s (status %s)", name, sum.GeneratedAt, sum.Status)
	return nil
}

// summaryHasRun reports whether the summary holds the execution as a
// finished run. A cloud run's id is its execution name.
func summaryHasRun(sum *service.WorkspaceSummary, executionARN string) bool {
	id := executionARN[strings.LastIndex(executionARN, ":")+1:]
	for _, p := range sum.Pipelines {
		for _, r := range p.Runs {
			if (r.RunID == id || r.RunID == executionARN) && r.Result != "running" {
				return true
			}
		}
	}
	return false
}
