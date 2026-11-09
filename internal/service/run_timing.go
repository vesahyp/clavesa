package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vesahyp/clavesa/internal/runtiming"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// runClock accumulates the run-level phase timings for one local run. The
// fields are written from the run's own goroutine only (prepareRunWith, then
// executeRun), so no lock.
type runClock struct {
	start       time.Time // where the run's total begins
	inputsStart time.Time
	inputsDone  bool
	phases      runtiming.Phases
	// resp is the runner's final aggregate response (nil when no bundle ran
	// or it produced none). Carries per-node timing + runner.boot_ms.
	resp map[string]any
}

// prepareRemainder is the `prepare` phase: everything prepareRunWith spent
// that is not lock, metastore or image.
func (c *runClock) prepareRemainder(entry time.Time) int64 {
	d := msSince(entry) - c.phases.Lock - c.phases.Metastore - c.phases.Image
	if d < 0 {
		return 0
	}
	return d
}

// markInputsDone closes the `inputs` phase. Idempotent; a no-op when the
// phase never started.
func (c *runClock) markInputsDone() {
	if c.inputsDone || c.inputsStart.IsZero() {
		return
	}
	c.inputsDone = true
	c.phases.Inputs = msSince(c.inputsStart)
}

// absorbBundle copies the bundle's container/bundle spans and keeps the
// runner response for the record assembly.
func (c *runClock) absorbBundle(br *bundleRun) {
	c.phases.Container = br.ContainerMs
	c.phases.Bundle = br.BundleMs
	c.resp = br.Response
	if runner, ok := br.Response["runner"].(map[string]any); ok {
		c.phases.Boot = jsonInt(runner["boot_ms"])
	}
}

// bundleRun is what one bundle container run produced: the per-transform
// statuses (topo order), the runner's raw final response, and the two
// Go-side spans measured around the container.
type bundleRun struct {
	Statuses    []NodeRunStatus
	Response    map[string]any
	ContainerMs int64 // cmd.Start to the first byte on stdout or stderr
	BundleMs    int64 // cmd.Start to cmd.Wait
}

// runPrepBundle is the default Service.runBundle: the real docker bundle.
func (s *Service) runPrepBundle(ctx context.Context, prep *runPrep, bundle []bundleTransformConfig) (*bundleRun, error) {
	return s.runPipelineBundle(ctx, prep.image, prep.abs, prep.workdir, bundle, prep.runID, prep.outcome.trigger, prep.catalog, prep.schema, prep.systemCatalog, prep.force, prep.forceNodes, prep.metastoreNetwork, prep.metastoreAddr)
}

// firstByte stamps the first time either wrapped stream carries a byte.
type firstByte struct {
	once sync.Once
	at   time.Time
	mu   sync.Mutex
}

func (f *firstByte) mark() {
	f.once.Do(func() {
		f.mu.Lock()
		f.at = time.Now()
		f.mu.Unlock()
	})
}

// msSince is first byte - start in ms, or 0 when no byte arrived.
func (f *firstByte) msSince(start time.Time) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.at.IsZero() {
		return 0
	}
	return msBetween(start, f.at)
}

func (f *firstByte) writer(w io.Writer) io.Writer { return firstByteWriter{f, w} }
func (f *firstByte) reader(r io.Reader) io.Reader { return firstByteReader{f, r} }

type firstByteWriter struct {
	f *firstByte
	w io.Writer
}

func (w firstByteWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		w.f.mark()
	}
	return w.w.Write(p)
}

type firstByteReader struct {
	f *firstByte
	r io.Reader
}

func (r firstByteReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.f.mark()
	}
	return n, err
}

func msSince(t time.Time) int64 { return time.Since(t).Milliseconds() }

func msBetween(a, b time.Time) int64 {
	d := b.Sub(a).Milliseconds()
	if d < 0 {
		return 0
	}
	return d
}

// jsonInt reads a JSON number decoded into any as int64 (0 otherwise).
func jsonInt(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case json.Number:
		x, _ := n.Int64()
		return x
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}

// nodeMarker is the subset of the runner's per-node `<node>.json` progress
// marker the timing record reads.
type nodeMarker struct {
	StartedMs  *int64          `json:"started_ms"`
	EndedMs    *int64          `json:"ended_ms"`
	OutputRows *int64          `json:"output_rows"`
	Metrics    json.RawMessage `json:"metrics"`
}

// buildRunTiming assembles the run's timing record from the clock, the
// outcome, the per-node markers and the runner response. Nodes are the
// transforms in result.Nodes (topo order).
func (s *Service) buildRunTiming(ctx context.Context, prep *runPrep, result *RunResult) *runtiming.Record {
	clock := prep.clock
	ended := time.Now()

	respNodes := map[string]map[string]any{}
	if clock.resp != nil {
		ts, _ := clock.resp["transforms"].([]any)
		for _, raw := range ts {
			if t, ok := raw.(map[string]any); ok {
				if id, _ := t["node"].(string); id != "" {
					respNodes[id] = t
				}
			}
		}
	}

	phases := clock.phases
	nodes := []runtiming.Node{}
	for _, ns := range result.Nodes {
		if ns.Type != "transform" {
			continue
		}
		n := runtiming.Node{Node: ns.NodeID, Status: ns.Status}
		var m nodeMarker
		if prep.store != nil {
			if data, err := prep.store.ReadKey(ctx, fmt.Sprintf("_progress/%s/%s.json", prep.runID, ns.NodeID)); err == nil {
				_ = json.Unmarshal(data, &m)
			}
		}
		if m.StartedMs != nil {
			n.StartedMs = *m.StartedMs
		}
		if m.EndedMs != nil {
			n.EndedMs = *m.EndedMs
		}
		if n.StartedMs > 0 && n.EndedMs >= n.StartedMs {
			n.DurationMs = n.EndedMs - n.StartedMs
		}
		n.OutputRows = m.OutputRows
		if rn := respNodes[ns.NodeID]; rn != nil {
			if v, ok := rn["output_rows"]; ok && v != nil {
				x := jsonInt(v)
				n.OutputRows = &x
			}
			if t, ok := rn["timing"]; ok && t != nil {
				if raw, err := json.Marshal(t); err == nil && runtiming.HasTiming(raw) {
					n.Timing = raw
				}
			}
		}
		if n.Timing == nil && runtiming.HasTiming(m.Metrics) {
			n.Timing = append(json.RawMessage(nil), m.Metrics...)
		}
		phases.Nodes += n.DurationMs
		nodes = append(nodes, n)
	}

	wsName := s.workspaceName()
	if wsName == "" {
		wsName = filepath.Base(s.workspace)
	}
	wsDir, err := filepath.Abs(s.workspace)
	if err != nil {
		wsDir = s.workspace
	}
	prep.outcome.mu.Lock()
	status, trigger, pipeline := prep.outcome.status, prep.outcome.trigger, prep.outcome.pipeline
	prep.outcome.mu.Unlock()

	return &runtiming.Record{
		Schema:       runtiming.SchemaVersion,
		RunID:        prep.runID,
		Workspace:    wsName,
		WorkspaceDir: wsDir,
		Pipeline:     pipeline,
		Status:       status,
		Trigger:      trigger,
		Compute:      "local",
		StartedAt:    clock.start.Format(time.RFC3339),
		EndedAt:      ended.Format(time.RFC3339),
		TotalMs:      msBetween(clock.start, ended),
		Phases:       phases,
		Nodes:        nodes,
	}
}

// writeRunTiming builds the record and writes it to
// `<workspace>/.clavesa/warehouse/_progress/<run>/_timing.json` plus the
// history dir. Best effort: failures log to stderr. Returns the record.
func (s *Service) writeRunTiming(ctx context.Context, prep *runPrep, result *RunResult) *runtiming.Record {
	if prep.clock == nil {
		return nil
	}
	rec := s.buildRunTiming(ctx, prep, result)
	path := filepath.Join(workspace.LocalWarehouseDir(filepath.Dir(prep.abs)), "_progress", prep.runID, runtiming.FileName)
	if err := runtiming.Write(rec, path); err != nil {
		fmt.Fprintf(os.Stderr, "[clavesa] run timing record: %v\n", err)
	}
	return rec
}
