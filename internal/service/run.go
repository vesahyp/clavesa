package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vesahyp/clavesa/internal/credentials"
	"github.com/vesahyp/clavesa/internal/errs"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/sources"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// RunResult summarises a `pipeline run` invocation.
type RunResult struct {
	Workdir string          `json:"workdir"`
	RunID   string          `json:"run_id"`
	Nodes   []NodeRunStatus `json:"nodes"`
}

// NodeRunStatus is the per-node outcome of RunPipeline.
type NodeRunStatus struct {
	NodeID string `json:"node_id"`
	Type   string `json:"type"`
	Status string `json:"status"`           // ok | skipped | failed
	Output string `json:"output,omitempty"` // path to the data this node produced
	Note   string `json:"note,omitempty"`   // human-readable detail
}

// runPrep is the synchronous setup for a pipeline run — everything that
// must happen before the run id is meaningful (parse, topo-sort, the
// initial RUNNING progress channel). StartRun returns the run id right
// after prepareRun so the UI can navigate to the run page instantly;
// executeRun then walks the DAG (synchronously for the CLI, in a
// background goroutine for the UI).
type runPrep struct {
	abs           string
	graph         graph.PipelineGraph
	order         []string
	workdir       string
	image         string
	catalog       string
	systemCatalog string
	schema        string
	runID         string
	channel       *runChannel
	// metastoreNetwork / metastoreAddr carry the shared local Derby
	// metastore wiring resolved once per run in prepareRun. When non-empty
	// the per-node bundle / transform / record-run containers join
	// metastoreNetwork and connect to metastoreAddr over JDBC instead of
	// each opening the embedded single-writer Derby DB — the local analog
	// of cloud's shared Glue. Empty when EnsureMetastore failed; the
	// containers then fall back to embedded Derby (safe, since no server
	// is serving).
	metastoreNetwork string
	metastoreAddr    string
	// force / forceNodes carry the operator's --force / --force-node intent
	// from the CLI into runPipelineBundle, which embeds them in the bundle
	// event so the runner's per-transform _is_forced() check fires.
	// force=true with forceNodes empty == every node in this dispatch.
	force      bool
	forceNodes []string
}

// ErrRunInFlight is returned by StartRun when the pipeline already has a
// run executing. The synchronous RunPipeline naturally serialized runs
// because the HTTP request blocked; async dispatch needs this explicit
// guard against a double-click / concurrent run of the same pipeline.
// Re-exported from internal/errs so both the service and the HTTP layer
// answer the same sentinel without bridge code (C10, 2026-05-24).
var ErrRunInFlight = errs.ErrRunInFlight

// RunPipeline executes every transform node in `dir` in topological order,
// using the same handler() that Lambda invokes. Source nodes contribute
// their configured local-FS paths; destination nodes are reported but not
// written to in this v1. Synchronous — blocks until the run finishes;
// used by `clavesa pipeline run`. The UI uses StartRun instead.
//
// During execution the runner writes a filesystem progress channel under
// <pipelineDir>/.clavesa/runs/<runID>/ so observability.LocalProvider can
// surface the same live state + per-node logs the cloud provider sources from
// SFN + CloudWatch (ADR-014).
//
// Local FS only — S3-style source paths return an error. Remote S3 sources
// will land when scheduled-remote support is wired.
func (s *Service) RunPipeline(ctx context.Context, dir string) (*RunResult, error) {
	return s.RunPipelineWithOpts(ctx, dir, RunOpts{})
}

// RunOpts carries optional run-time controls for RunPipeline /
// RunPipelineWithOpts. Zero-value behaves identically to the legacy
// signature.
//
//   - Force      — bypass incremental-skip checks for this run. The runner
//                  re-reads the full source range on partitioned_path inputs
//                  and Delta CDF inputs that otherwise would have skipped
//                  because the cursor didn't advance. Watermarks still
//                  advance on success — the next unforced run resumes
//                  normal incremental dispatch.
//   - ForceNodes — narrows Force to the named node IDs. Empty (with Force
//                  true) means "every node in this dispatch."
type RunOpts struct {
	Force      bool
	ForceNodes []string
}

// RunPipelineWithOpts is RunPipeline with operator-controlled bypass
// switches. Used by `clavesa pipeline run --force / --force-node`.
func (s *Service) RunPipelineWithOpts(ctx context.Context, dir string, opts RunOpts) (*RunResult, error) {
	prep, err := s.prepareRun(dir)
	if err != nil {
		return nil, err
	}
	// ForceNodes implies Force. The CLI normalizes this too, but the
	// service layer is the only contract entrypoint with HCL access, so
	// defend in depth.
	prep.force = opts.Force || len(opts.ForceNodes) > 0
	prep.forceNodes = append([]string(nil), opts.ForceNodes...)
	return s.executeRun(ctx, prep)
}

// StartRun begins a local pipeline run asynchronously. It prepares the
// run synchronously — so the run id is immediately meaningful and the
// RUNNING progress channel exists — then walks the DAG in a background
// goroutine and returns the run id. The UI navigates to the run page
// with this id instead of blocking for the whole run. Returns
// ErrRunInFlight if the pipeline already has a run executing.
func (s *Service) StartRun(dir string) (string, error) {
	return s.StartRunWithOpts(dir, RunOpts{})
}

// StartRunWithOpts is StartRun with operator-controlled bypass switches
// (force / force-node). Used by POST /api/pipeline/run when the UI's
// Run button passes through the force-checkbox / force-nodes input —
// keeps CLI and UI calling the same execution-input shape (ADR-015).
func (s *Service) StartRunWithOpts(dir string, opts RunOpts) (string, error) {
	abs := s.resolveDir(dir)
	s.runsMu.Lock()
	if s.runsInFlight[abs] {
		s.runsMu.Unlock()
		return "", ErrRunInFlight
	}
	// Reserve before prepareRun so two racing callers can't both prepare.
	s.runsInFlight[abs] = true
	s.runsMu.Unlock()

	clear := func() {
		s.runsMu.Lock()
		delete(s.runsInFlight, abs)
		s.runsMu.Unlock()
	}

	prep, err := s.prepareRun(dir)
	if err != nil {
		clear()
		return "", err
	}
	// ForceNodes implies Force (mirrors RunPipelineWithOpts).
	prep.force = opts.Force || len(opts.ForceNodes) > 0
	prep.forceNodes = append([]string(nil), opts.ForceNodes...)
	go func() {
		defer clear()
		// context.Background(): the run outlives the HTTP request that
		// dispatched it (and any browser tab that closes).
		_, _ = s.executeRun(context.Background(), prep)
	}()
	return prep.runID, nil
}

// prepareRun does the synchronous setup: parse + validate, topo-sort,
// evict the warm worker, resolve the runner image + ADR-016 (catalog,
// schema), generate the run id, and write the initial RUNNING progress
// channel. After it returns, GET /pipeline/execution/states reports the
// run as RUNNING with every node PENDING.
func (s *Service) prepareRun(dir string) (*runPrep, error) {
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}
	if len(g.Validation.Errors) > 0 {
		return nil, fmt.Errorf("pipeline has validation errors: %s", g.Validation.Errors[0].Message)
	}

	// GH #6: a string-form input ref "<own-schema>.<sibling-table>" parses into
	// external_inputs with no graph edge. Rewrite those into real edges before
	// the topo sort so the consumer is ordered after its sibling producer and
	// resolves the table as an intra-pipeline read.
	reclassifyIntraPipelineEdges(&g, resolvePipelineSchema(abs, filepath.Base(abs)))

	order, err := topoSort(&g)
	if err != nil {
		return nil, err
	}

	// Shared local Derby metastore (the local analog of cloud's shared
	// Glue Data Catalog). Bring up (or reuse) the per-workspace Derby
	// Network Server once per run, then thread the network + addr onto
	// every per-node container below so they connect to it over JDBC
	// instead of each opening the embedded single-writer DB. Concurrency
	// between the run's containers and the UI's warm query worker /
	// notebooks is now INTENDED — the shared metastore is what lets them
	// coexist, so we no longer evict the warm worker for memory headroom
	// the way the embedded-Derby-lock era required (slice 5 covers any
	// follow-on heap tuning). EnsureMetastore is idempotent and fast, so
	// one ensure per run is enough; the per-node builders just reuse the
	// stashed (network, addr). Best-effort: the injected ensurer logs and
	// returns both empty on failure so the containers fall back to
	// embedded Derby (the no-op default in unit tests returns empty too).
	metastoreNetwork, metastoreAddr := s.metastoreEnsure(context.Background(), s.workspace, s.workspaceName())

	workdir, err := os.MkdirTemp("", "clavesa-run-")
	if err != nil {
		return nil, fmt.Errorf("create workdir: %w", err)
	}

	// Resolve the workspace-scoped runner image name and the ADR-016
	// (catalog, schema) pair for this pipeline run. Catalog comes from
	// the workspace manifest (empty for legacy / pre-ADR workspaces);
	// schema is read from the pipeline's variables.tf default with
	// terraform.tfvars override applied. Both flow into the runner via
	// CLAVESA_CATALOG / CLAVESA_SCHEMA env vars and end up
	// encoded as the Glue DB name `_glue_db()` writes to — keeps local
	// runs writing to the same backend names as their cloud twin.
	image := runner.LocalImageName("") + ":latest"
	pipelineName := filepath.Base(abs)
	catalog := ""
	systemCatalog := ""
	workspaceRoot := filepath.Dir(abs)
	if m, _ := workspace.Load(workspaceRoot); m != nil {
		// Auto-refresh the workspace runner image if a CLI upgrade
		// shipped new runner code — bypasses the silent-stale-image
		// trap users would otherwise hit after `brew upgrade`.
		ensured, err := workspace.EnsureLocalRunnerImage(workspaceRoot)
		if err != nil {
			return nil, fmt.Errorf("ensure runner image: %w", err)
		}
		image = ensured
		catalog = m.CatalogIdentifier()
		systemCatalog = m.SystemCatalogIdentifier()
	}
	schema := resolvePipelineSchema(abs, pipelineName)

	runID := newRunID()
	channel := newRunChannel(abs, runID, pipelineName)
	if err := channel.start(order, &g); err != nil {
		return nil, fmt.Errorf("init run channel: %w", err)
	}

	return &runPrep{
		abs: abs, graph: g, order: order, workdir: workdir,
		image: image, catalog: catalog, systemCatalog: systemCatalog,
		schema: schema, runID: runID, channel: channel,
		metastoreNetwork: metastoreNetwork, metastoreAddr: metastoreAddr,
	}, nil
}

// executeRun walks the prepared DAG, invoking the runner per transform.
// Shared by RunPipeline (synchronous) and StartRun (background goroutine).
func (s *Service) executeRun(ctx context.Context, prep *runPrep) (*RunResult, error) {
	g := prep.graph
	abs := prep.abs
	workdir := prep.workdir
	image := prep.image
	catalog := prep.catalog
	systemCatalog := prep.systemCatalog
	schema := prep.schema
	runID := prep.runID
	channel := prep.channel

	// Belt-and-suspenders: every reachable exit from executeRun runs the
	// `done:` block (channel.finish + recordLocalRun), but a panic inside
	// the bundle walker or any other unexpected early return would skip
	// it — leaving the run's state.json frozen at RUNNING and no terminal
	// row in the `runs` Iceberg table. The dashboard then paints a
	// phantom Running row with `—` duration until the OrphanThreshold
	// timer (60s) downgrades it to FAILED on the next read. The defer
	// closes that gap by force-finishing the channel and writing the
	// terminal row on panic. channel.finish is idempotent on endedMs
	// (the explicit `done:` call still wins when it ran first).
	var terminalFired bool
	defer func() {
		if r := recover(); r != nil {
			if !terminalFired {
				channel.finish(fmt.Errorf("pipeline runner panicked: %v", r))
				recordLocalRun(ctx, image, abs, channel, nil, s.metastoreEnsure)
			}
			panic(r) // re-raise so the caller / Go runtime sees it
		}
		if !terminalFired {
			// Reached only when an unexpected return path bypasses
			// `done:`. Synthesise a SUCCEEDED terminal row to match
			// the cloud runs_writer Lambda's shape (ADR-014 parity);
			// channel.state.Status stays "RUNNING" until finish() is
			// called.
			channel.finish(nil)
			recordLocalRun(ctx, image, abs, channel, nil, s.metastoreEnsure)
		}
	}()

	result := &RunResult{Workdir: workdir, RunID: runID}
	outputPath := map[string]string{}   // nodeID -> path containing this node's output data
	outputFormat := map[string]string{} // nodeID -> format for downstream readers ("parquet" for transforms; source-declared otherwise)
	skippedThisRun := map[string]bool{} // nodeID -> true when the node skipped this run (cascade-skip source of truth)
	// Cascade-skipped nodes bypass the runner entirely, so they leave no
	// node_runs row behind. Collect them here in topological order so
	// recordLocalRun can backfill one row per node in its single end-of-run
	// runner invocation (UI Runs grid would otherwise show "missing").
	var cascadeSkipped []string
	nodeByID := map[string]*graph.Node{}
	for i := range g.Nodes {
		nodeByID[g.Nodes[i].ID] = &g.Nodes[i]
	}
	// Reverse adjacency for cascade-skip: parents[nodeID] = upstream node IDs
	// (only intra-pipeline edges; `sources.<name>` references are workspace-
	// level registry entries and don't appear in g.Edges).
	parents := map[string][]string{}
	for _, e := range g.Edges {
		parents[e.ToNode] = append(parents[e.ToNode], e.FromNode)
	}

	finalErr := error(nil)

	// Phase A bundle execution (v2.2.0): all transforms run in one
	// container so the JVM cold-start is paid once per `pipeline run`,
	// not once per transform. Sources and destinations stay inline
	// (sources resolve to local paths with no container; destinations
	// are no-ops in v1). The bundle path:
	//   1. First pass: resolve sources (cheap), pre-compute every
	//      transform's outputPath/format from autoDeltaTableID, collect
	//      the per-transform bundle configs.
	//   2. Issue one `docker run` with the pipeline event.
	//   3. The runner's pipeline_handler emits per-transform progress
	//      events to stdout; runPipelineBundle scans them, fires
	//      channel events in real time so the UI state.json updates
	//      live (no UX regression vs the per-transform loop).
	//   4. Destinations afterward (always leaves in v1).
	var bundle []bundleTransformConfig
	transformResults := map[string]NodeRunStatus{}

	// First pass — sources, pre-compute transform tableIDs/outputs so
	// downstream buildInputs sees every upstream-transform's tableID
	// before the bundle runs.
	for _, nodeID := range prep.order {
		node := nodeByID[nodeID]
		if node == nil {
			continue
		}
		switch node.Type {
		case "source":
			path, perr := localSourcePath(node)
			if perr != nil {
				status := NodeRunStatus{NodeID: nodeID, Type: node.Type, Status: "failed", Note: perr.Error()}
				channel.nodeFailed(nodeID, "source_error", perr.Error())
				result.Nodes = append(result.Nodes, status)
				finalErr = fmt.Errorf("source %s: %w", nodeID, perr)
				goto done
			}
			outputPath[nodeID] = path
			outputFormat[nodeID] = sourceFormat(node)
			channel.nodeSucceeded(nodeID, nil)
			result.Nodes = append(result.Nodes, NodeRunStatus{NodeID: nodeID, Type: node.Type, Status: "ok", Output: path})

		case "transform":
			outputPath[nodeID] = autoDeltaTableID(catalog, schema, nodeID)
			outputFormat[nodeID] = "iceberg"
		}
	}

	// Second pass — assemble transform bundle entries. Each one knows
	// its inputs (resolved against the now-fully-populated outputPath),
	// its outputs HCL, and its parent list (for the cascade-skip rule
	// pipeline_handler enforces internally).
	for _, nodeID := range prep.order {
		node := nodeByID[nodeID]
		if node == nil || node.Type != "transform" {
			continue
		}
		// Disabled node: skip execution but keep its output-path registration
		// (first pass) so downstream consumers resolve to its existing table.
		// Mirrors the orchestration emitter's nodeEnabled filter (ADR-014).
		if !nodeEnabled(*node) {
			continue
		}
		tableID := outputPath[nodeID]
		inputs, ierr := s.buildInputs(&g, nodeID, outputPath, outputFormat, catalog)
		if ierr != nil {
			status := NodeRunStatus{NodeID: nodeID, Type: node.Type, Status: "failed", Note: ierr.Error()}
			channel.nodeFailed(nodeID, "input_error", ierr.Error())
			result.Nodes = append(result.Nodes, status)
			finalErr = fmt.Errorf("transform %s inputs: %w", nodeID, ierr)
			goto done
		}
		language, _ := node.Config["language"].(string)
		if language == "" {
			language = "sql"
		}
		logic, lerr := readNodeLogic(node, language, abs)
		if lerr != nil {
			status := NodeRunStatus{NodeID: nodeID, Type: node.Type, Status: "failed", Note: lerr.Error()}
			channel.nodeFailed(nodeID, "logic_error", lerr.Error())
			result.Nodes = append(result.Nodes, status)
			finalErr = fmt.Errorf("transform %s logic: %w", nodeID, lerr)
			goto done
		}
		logicPath := filepath.Join(workdir, nodeID, "logic.txt")
		if err := os.MkdirAll(filepath.Dir(logicPath), 0o755); err != nil {
			finalErr = err
			goto done
		}
		if err := os.WriteFile(logicPath, []byte(logic), 0o644); err != nil {
			finalErr = err
			goto done
		}
		// Exclude disabled upstreams from the cascade-skip parent list — a
		// disabled node never runs, so it's never in pipeline_handler's
		// skipped_set, and leaving it in would block legitimate cascade-skip
		// (mirror of the orchestration emitter's Parents filter).
		var enabledParents []string
		for _, p := range parents[nodeID] {
			if up := nodeByID[p]; up != nil && up.Type == "transform" && !nodeEnabled(*up) {
				continue
			}
			enabledParents = append(enabledParents, p)
		}
		bundle = append(bundle, bundleTransformConfig{
			NodeID:    nodeID,
			Node:      node,
			Inputs:    inputs,
			Outputs:   buildLocalOutputs(node, tableID),
			LogicPath: logicPath,
			Language:  language,
			Parents:   enabledParents,
			TableID:   tableID,
		})
	}

	// Run the whole pipeline in one container. Per-transform channel
	// events stream out of pipeline_handler via stdout JSON lines; the
	// scanner inside runPipelineBundle dispatches them to the channel
	// in real time. Returns the per-transform terminal statuses for
	// the destination-pass loop below.
	if len(bundle) > 0 {
		bundleStatuses, berr := s.runPipelineBundle(ctx, image, abs, workdir, bundle, runID, catalog, schema, systemCatalog, channel, prep.force, prep.forceNodes, prep.metastoreNetwork, prep.metastoreAddr)
		// Even on bundle error, walk whatever per-node results we got
		// before the failure so result.Nodes carries the partial picture
		// (the UI uses this for the per-node breakdown).
		for _, bs := range bundleStatuses {
			transformResults[bs.NodeID] = bs
			if bs.Status == "skipped" && bs.Note == "all upstreams skipped" {
				cascadeSkipped = append(cascadeSkipped, bs.NodeID)
			}
			if bs.Status == "skipped" {
				skippedThisRun[bs.NodeID] = true
			}
		}
		// Append in topo order so result.Nodes matches today's ordering
		// guarantee.
		for _, bt := range bundle {
			if bs, ok := transformResults[bt.NodeID]; ok {
				result.Nodes = append(result.Nodes, bs)
			}
		}
		if berr != nil {
			finalErr = berr
			goto done
		}
	}

	// Destinations (always leaves in v1): report what would be written.
	for _, nodeID := range prep.order {
		node := nodeByID[nodeID]
		if node == nil || node.Type != "destination" {
			continue
		}
		upstream := upstreamOutput(&g, nodeID, outputPath)
		status := NodeRunStatus{NodeID: nodeID, Type: node.Type, Status: "skipped", Output: upstream, Note: "destinations not executed in v1; data left at upstream output path"}
		channel.nodeSkipped(nodeID, status.Note)
		result.Nodes = append(result.Nodes, status)
	}

done:
	channel.finish(finalErr)
	// Append the run-level rollup row to <pipeline>.runs so the dashboard's
	// "Run history" panel works for local pipelines (cloud uses an EventBridge
	// → runs-writer Lambda that does the same write through Athena). Best-effort:
	// a failure here logs to stderr but doesn't change the run's outcome — the
	// data the user cares about already landed via runTransform.
	recordLocalRun(ctx, image, abs, channel, cascadeSkipped, s.metastoreEnsure)
	terminalFired = true
	if finalErr != nil {
		return result, finalErr
	}
	return result, nil
}

// topoSort returns node IDs in dependency order (sources first, destinations last).
// Errors on a cycle.
func topoSort(g *graph.PipelineGraph) ([]string, error) {
	indegree := map[string]int{}
	children := map[string][]string{}
	for _, n := range g.Nodes {
		indegree[n.ID] = 0
	}
	for _, e := range g.Edges {
		indegree[e.ToNode]++
		children[e.FromNode] = append(children[e.FromNode], e.ToNode)
	}

	var queue []string
	for id, d := range indegree {
		if d == 0 {
			queue = append(queue, id)
		}
	}

	order := make([]string, 0, len(g.Nodes))
	for len(queue) > 0 {
		// Stable order: pop smallest ID for determinism across runs.
		// Tiny graphs — bubble down isn't a problem.
		minIdx := 0
		for i, id := range queue {
			if id < queue[minIdx] {
				minIdx = i
			}
		}
		curr := queue[minIdx]
		queue = append(queue[:minIdx], queue[minIdx+1:]...)
		order = append(order, curr)

		for _, child := range children[curr] {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(order) != len(g.Nodes) {
		return nil, fmt.Errorf("pipeline has a cycle (visited %d/%d nodes)", len(order), len(g.Nodes))
	}
	return order, nil
}

// localSourcePath returns the local filesystem path for a source node, or an
// error if the configured `bucket` isn't a local path.
func localSourcePath(node *graph.Node) (string, error) {
	bucket, _ := node.Config["bucket"].(string)
	prefix, _ := node.Config["prefix"].(string)
	if bucket == "" {
		return "", fmt.Errorf("source has no `bucket` config")
	}
	// Mirror the rule used in service/preview: anything not starting with
	// /, ./, or ../ is treated as an S3 bucket name.
	if !strings.HasPrefix(bucket, "/") && !strings.HasPrefix(bucket, "./") && !strings.HasPrefix(bucket, "../") {
		return "", fmt.Errorf("source bucket %q looks like S3; v1 of `pipeline run` is local-only", bucket)
	}
	return filepath.Join(bucket, prefix), nil
}

// buildInputs resolves each parent edge to a descriptor the runner can read.
//
// String form (back-compat) is emitted whenever the upstream output is plain
// Parquet — covers every transform→transform edge plus source→transform edges
// where the source declared `format = "parquet"` (or left it unset). Anything
// else (csv, json, …) emits the `{"kind":"path","path":...,"format":...}` dict
// form so the runner can branch its `spark.read.<fmt>(...)` call accordingly.
// The dict form deliberately does not collide with the existing
// `{"kind":"partitioned_path", ...}` shape used for v0.12.0 incremental S3
// sources — those still go through their own path.
//
// ADR-017 slice 1: also resolves workspace-registry `sources.<name>`
// references on the target transform into kind-discriminated descriptors
// (currently `{"kind":"http","url":...,"format":...}`). Workspace-level
// registry lookup is the reason this is a method on Service.
//
// ADR-016: cross-pipeline `external_inputs` (`<schema>.<table>` references
// authored via `node connect --from-table`) resolve to the bare-string
// catalog identifier `clavesa.<catalog>__<schema>.<table>` — the same
// translation the cloud orchestration emitter applies, shared via
// identutil.EncodeExternalTableRef so the two surfaces can't drift. `catalog`
// is the workspace catalog identifier.
func (s *Service) buildInputs(g *graph.PipelineGraph, nodeID string, outputPath, outputFormat map[string]string, catalog string) (map[string]any, error) {
	inputs := map[string]any{}
	// Workspace-source references first.
	for _, n := range g.Nodes {
		if n.ID != nodeID {
			continue
		}
		if srcInputs, ok := n.Config["source_inputs"].(map[string]interface{}); ok {
			store := sources.New(s.workspace)
			credStore := credentials.New(s.workspace)
			for alias, raw := range srcInputs {
				name := ""
				switch v := raw.(type) {
				case string:
					name = strings.TrimPrefix(v, "sources.")
				case map[string]interface{}:
					if sn, ok := v["spec_name"].(string); ok {
						name = sn
					}
				}
				if name == "" {
					return nil, fmt.Errorf("input %q: malformed source_inputs entry %v", alias, raw)
				}
				spec, err := store.Get(name)
				if err != nil {
					return nil, fmt.Errorf("input %q references source %q which is not registered: %w", alias, name, err)
				}
				var credDescriptor map[string]any
				if spec.Credentials != "" {
					cred, err := credStore.Get(spec.Credentials)
					if err != nil {
						return nil, fmt.Errorf("source %q references credential %q which is not registered", name, spec.Credentials)
					}
					credDescriptor = map[string]any{
						"kind":         cred.Kind,
						"header_name":  cred.HeaderName,
						"value_prefix": cred.ValuePrefix,
						"secret":       cred.Secret,
					}
				}
				switch spec.Kind {
				case "http":
					descriptor := map[string]any{
						"kind":   "http",
						"url":    spec.URL,
						"format": spec.Format,
					}
					if credDescriptor != nil {
						descriptor["credentials"] = credDescriptor
					}
					inputs[alias] = descriptor
				case "s3":
					if len(spec.Partitions) > 0 {
						// Incremental read — runner walks the partition
						// tree under the prefix, advances a watermark on
						// each run. partitioned_path expects an s3://
						// path; bucket+prefix → "s3://<bucket>/<prefix>".
						parts := make([]any, len(spec.Partitions))
						for i, p := range spec.Partitions {
							parts[i] = p
						}
						startFrom := spec.StartFrom
						if startFrom == "" {
							startFrom = "all"
						}
						inputs[alias] = map[string]any{
							"kind":       "partitioned_path",
							"path":       "s3://" + spec.Bucket + "/" + spec.Prefix,
							"partitions": parts,
							"start_from": startFrom,
						}
						break
					}
					descriptor := map[string]any{
						"kind":   "s3",
						"bucket": spec.Bucket,
						"prefix": spec.Prefix,
						"format": spec.Format,
					}
					if credDescriptor != nil {
						descriptor["credentials"] = credDescriptor
					}
					inputs[alias] = descriptor
				default:
					return nil, fmt.Errorf("source %q kind %q not supported", name, spec.Kind)
				}
			}
		}
		// Cross-pipeline reads (ADR-016). `external_inputs` holds
		// `alias -> "<schema>.<table>"`; resolve each to the runner Delta
		// table identifier `<catalog>__<schema>.<table>`. The runner reads a
		// slashless bare string via `spark.table()`, and the workspace-shared
		// local warehouse means the producing pipeline's table is in the
		// same Hadoop catalog the consumer's Spark sees.
		if extInputs, ok := n.Config["external_inputs"].(map[string]interface{}); ok {
			for alias, raw := range extInputs {
				ref, _ := raw.(string)
				id, err := identutil.EncodeExternalTableRef(catalog, ref)
				if err != nil {
					return nil, fmt.Errorf("input %q: %w", alias, err)
				}
				inputs[alias] = id
			}
		}
		break
	}
	var consumer *graph.Node
	for i := range g.Nodes {
		if g.Nodes[i].ID == nodeID {
			consumer = &g.Nodes[i]
			break
		}
	}
	incremental := map[string]bool{}
	if consumer != nil {
		incremental = incrementalInputAliases(*consumer)
	}
	for _, e := range g.Edges {
		if e.ToNode != nodeID {
			continue
		}
		alias := e.ToInput
		if alias == "" || alias == "default" {
			alias = e.FromNode
		}
		path, ok := outputPath[e.FromNode]
		if !ok {
			return nil, fmt.Errorf("upstream node %s has not produced output yet", e.FromNode)
		}
		format := outputFormat[e.FromNode]
		if format == "iceberg" && incremental[alias] {
			// v2.0.0 (ADR-018): CDF-bounded read on a Delta upstream.
			// Runner discovers the current Delta version via
			// `DESCRIBE HISTORY`, compares against the stored watermark,
			// and reads the (last, current] range through readChangeFeed.
			// Local watermarks live under the pipeline's
			// `.clavesa/watermarks/` (mounted into the container by
			// runTransform); cloud uses the pipeline's S3 bucket.
			//
			// When the upstream producer declares merge_keys (mode=merge),
			// stamp them onto the descriptor so the runner dedupes the CDF
			// range to the latest row per key by `_commit_version DESC`
			// (mirror of the cloud-side wiring in orchestration.go's
			// buildNodeInputsExpr).
			desc := map[string]any{
				"kind":  "delta_table_cdf",
				"table": path,
				"alias": nodeID + "__" + alias,
			}
			var fromNode *graph.Node
			for i := range g.Nodes {
				if g.Nodes[i].ID == e.FromNode {
					fromNode = &g.Nodes[i]
					break
				}
			}
			if fromNode != nil {
				fromOutput := "default"
				if mk := outputMergeKeys(*fromNode, fromOutput); len(mk) > 0 {
					desc["merge_keys"] = mk
				}
			}
			inputs[alias] = desc
			continue
		}
		// Bare-string descriptor handles two cases the runner already
		// dispatches correctly: Delta table ids (no slash → spark.table)
		// and parquet paths (slash → spark.read.parquet). The dict-form
		// descriptor is reserved for non-Parquet path-form sources.
		if format == "" || format == "parquet" || format == "iceberg" {
			inputs[alias] = path
			continue
		}
		inputs[alias] = map[string]any{
			"kind":   "path",
			"path":   path,
			"format": format,
		}
	}
	return inputs, nil
}

// resolvePipelineSchema returns the effective ADR-016 schema identifier
// for a pipeline, with the same precedence Terraform applies at apply
// time: terraform.tfvars / *.auto.tfvars overrides take priority over
// the variable's default in variables.tf. Falls back to the sanitized
// pipeline name when neither exists — matches the legacy
// `clavesa_<pipeline>` Glue DB naming used before ADR-016.
//
// Returning the same value Terraform would resolve at apply time keeps
// local runs writing to the same Glue DB as their cloud twin
// (ADR-014 parity).
func resolvePipelineSchema(pipelineDir, pipelineName string) string {
	if v, ok := readSchemaFromTFVars(pipelineDir); ok && v != "" {
		return v
	}
	if v := readSchemaDefault(pipelineDir); v != "" {
		return v
	}
	return identutil.Sanitize(pipelineName)
}

// readSchemaFromTFVars looks for a `schema = "..."` assignment in any
// tfvars file (terraform.tfvars or *.auto.tfvars) and returns it.
// Conservative implementation: line-prefix match, no full HCL parse —
// matches the readVariableDecls pattern used elsewhere.
func readSchemaFromTFVars(dir string) (string, bool) {
	for _, name := range tfvarsCandidates(dir) {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			t := strings.TrimSpace(line)
			if !strings.HasPrefix(t, "schema") {
				continue
			}
			_, val, ok := strings.Cut(t, "=")
			if !ok {
				continue
			}
			v := strings.TrimSpace(val)
			if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
				return v[1 : len(v)-1], true
			}
		}
	}
	return "", false
}

// readSchemaDefault returns the default value of `variable "schema"`
// in variables.tf, or "" if the variable isn't declared / has no
// default. Pipelines created post-ADR-016 always declare it; legacy
// pipelines don't and fall through to the sanitize(pipeline_name)
// fallback in resolvePipelineSchema.
func readSchemaDefault(dir string) string {
	data, err := os.ReadFile(filepath.Join(dir, "variables.tf"))
	if err != nil {
		return ""
	}
	inSchemaBlock := false
	for _, line := range strings.Split(string(data), "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, `variable "schema"`) {
			inSchemaBlock = true
			continue
		}
		if inSchemaBlock {
			if strings.HasPrefix(t, "}") {
				inSchemaBlock = false
				continue
			}
			if strings.HasPrefix(t, "default") {
				_, val, ok := strings.Cut(t, "=")
				if !ok {
					continue
				}
				v := strings.TrimSpace(val)
				if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
					return v[1 : len(v)-1]
				}
			}
		}
	}
	return ""
}

// tfvarsCandidates returns the tfvars filenames Terraform consumes,
// in precedence order (terraform.tfvars, then *.auto.tfvars sorted).
func tfvarsCandidates(dir string) []string {
	out := []string{"terraform.tfvars"}
	matches, _ := filepath.Glob(filepath.Join(dir, "*.auto.tfvars"))
	sort.Strings(matches)
	for _, m := range matches {
		out = append(out, filepath.Base(m))
	}
	return out
}

// autoDeltaTableID mirrors runner.py::_table_id_for so Go can pass an
// explicit Delta table id into the runner's ``outputs.default`` field
// (instead of letting the runner auto-generate, which Go can't observe
// for downstream-input wiring). Same ADR-016 encoding as the runner's
// ``_glue_db()`` and ``internal/identutil.EncodeGlueDatabase``. Delta
// tables live under Spark's default ``spark_catalog``, so the identifier
// is the bare ``<db>.<table>`` two-segment form.
//
// ADR-019 Slice 3 drops the ``__default`` suffix from single-output
// transforms. The Go-side service only ever feeds the runner the
// ``outputs.default`` slot from this pipeline-run path, so the
// bare-node-name table segment is always correct here.
//
// Slice 4 leaves this shape unchanged. The architectural goal of native
// three-level ``<catalog>.<schema>.<table>`` addressing is blocked on
// Delta 4.0's session-only DeltaCatalog implementation (see
// runner/spark_conf.py); the on-disk warehouse layout moves to the V2
// tree via ``_ensure_database``'s LOCATION clause regardless.
func autoDeltaTableID(catalog, schema, nodeID string) string {
	nodeSafe := identutil.Sanitize(nodeID)
	return fmt.Sprintf("%s.%s",
		identutil.EncodeGlueDatabase(catalog, schema), nodeSafe)
}

// sourceFormat returns the source node's declared `format` attribute,
// defaulting to "parquet" when unset (matches the runner's historical
// assumption — and what most produced-by-an-upstream-transform sources are).
func sourceFormat(node *graph.Node) string {
	if v, ok := node.Config["format"].(string); ok && v != "" {
		return v
	}
	return "parquet"
}

// upstreamOutput finds the first connected upstream node's output, used by
// destination reporting.
func upstreamOutput(g *graph.PipelineGraph, nodeID string, outputPath map[string]string) string {
	for _, e := range g.Edges {
		if e.ToNode == nodeID {
			if p, ok := outputPath[e.FromNode]; ok {
				return p
			}
		}
	}
	return ""
}

// runTransform writes the node's logic to disk, builds the docker-run argv,
// pipes the event JSON via stdin, and waits for completion. logPath, when
// non-empty, receives a copy of stdout+stderr so the local progress channel
// can surface per-node logs to the UI (mirrors CloudWatch in cloud).
//
// outputTarget is passed straight through to the runner as `outputs.default`.
// An empty string triggers auto-Iceberg-table mode (the runner generates
// `clavesa.clavesa_<pipeline>.<node>__default`); a path-form string
// would route plain-Parquet output (reserved for destination overrides; not
// emitted by the local pipeline-run flow today).
// runTransform runs one node through the runner container and reports the
// outcome with three states:
//   - (skipReason="", err=nil)    → ran successfully, downstream may proceed
//   - (skipReason!="", err=nil)   → runner reported {"status":"skipped",...}
//     (e.g. no new partitions / no new snapshots) — not a failure, caller
//     should mark the node skipped and continue the DAG.
//   - (skipReason="", err!=nil)   → real failure (container error, malformed
//     output, runner error message, unexpected status value).
//
// outputRows is non-nil when the runner wrote at least one Iceberg-mode
// output and reported the added-records sum across them; the caller
// stamps it onto state.json so the dashboard's node-detail drawer can
// show "Rows written" without a Spark roundtrip.
//
// extraEvent, when non-nil, gets shallow-merged into the runner event after
// the standard inputs/outputs/_sf_execution_arn/_trigger keys are set. This
// is the seam BackfillStage uses to thread its `_backfill` block (and to
// override `_trigger` from "manual" to "backfill"/"backfill-direct") without
// `pipeline run --env local` having to know anything about backfill.
func (s *Service) runTransform(ctx context.Context, image, pipelineDir, workdir string, node *graph.Node, inputs map[string]any, outputTarget, logPath, pipelineRunID, catalog, schema, systemCatalog string, extraEvent map[string]any) (string, *int64, error) {
	language, _ := node.Config["language"].(string)
	if language == "" {
		language = "sql"
	}

	logic, lerr := readNodeLogic(node, language, pipelineDir)
	if lerr != nil {
		return "", nil, lerr
	}

	logicPath := filepath.Join(workdir, node.ID, "logic.txt")
	if err := os.MkdirAll(filepath.Dir(logicPath), 0o755); err != nil {
		return "", nil, err
	}
	if err := os.WriteFile(logicPath, []byte(logic), 0o644); err != nil {
		return "", nil, err
	}

	// Pass the explicit table_id through. The runner's _resolve_output sees
	// a non-path-form string and treats it as an Iceberg table id (the same
	// string the runner would have auto-generated from CLAVESA_PIPELINE +
	// CLAVESA_NODE). Single source of truth lives Go-side now so
	// downstream transforms in this run see the exact same table id when
	// they spark.table() against it.
	//
	// `_sf_execution_arn` carries the pipeline-run id into the runner so
	// every node_runs row stamps the same value the runs writer puts on
	// `runs.sf_execution_arn`. Without this, node_runs.run_id (per-runner-
	// invocation uuid) and runs.run_id (per-pipeline-execution uuid) drift
	// apart and the join breaks. Cloud uses the literal SFN ARN; local
	// uses the pipeline-run uuid — semantically identical for the join.
	//
	// `_trigger` mirrors the value the cloud start paths stamp into the SFN
	// execution input. A local `pipeline run` is always operator-initiated,
	// so it stamps `manual` — the runner copies it into each Iceberg
	// snapshot's summary so the table timeline shows where the rows came from.
	event := map[string]any{
		"inputs":            inputs,
		"outputs":           buildLocalOutputs(node, outputTarget),
		"_sf_execution_arn": pipelineRunID,
		"_trigger":          "manual",
	}
	for k, v := range extraEvent {
		event[k] = v
	}
	eventJSON, _ := json.Marshal(event)

	// Workspace-shared Hadoop-catalog warehouse holds every local pipeline's
	// Iceberg tables under separate `<catalog>__<schema>` namespaces. One
	// warehouse per workspace is what lets a consumer transform read a
	// sibling pipeline's table (cross-pipeline reads on `compute = "local"`);
	// it mirrors the cloud model where one S3 bucket holds every pipeline's
	// tables under DB-per-schema namespaces.
	pipelineName := filepath.Base(pipelineDir)
	warehouse := workspace.LocalWarehouseDir(s.workspace)
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		return "", nil, fmt.Errorf("create warehouse: %w", err)
	}
	// Local-side watermark directory for v2.0.0's delta_table_cdf kind
	// (and any future watermark-tracking inputs). The runner reads / writes
	// `<watermarks>/<consumer>__<alias>.json`; cloud uses `s3://<bucket>/<pipeline>/_watermarks/`
	// for the same purpose. ADR-014 local-cloud parity.
	watermarks := filepath.Join(pipelineDir, ".clavesa", "watermarks")
	if err := os.MkdirAll(watermarks, 0o755); err != nil {
		return "", nil, fmt.Errorf("create watermarks dir: %w", err)
	}

	args := []string{"run", "--rm", "-i"}
	args = append(args, "-e", "CLAVESA_RUN=1")
	// Shared local Derby metastore. runTransform is reached by the
	// single-node backfill path (the bundle path resolves the metastore
	// once in prepareRun and threads it in); resolve it here too so a
	// backfill container connects to the Derby Network Server as a client
	// rather than opening the embedded single-writer DB. EnsureMetastore
	// is idempotent + fast. Best-effort: the injected ensurer returns
	// empty on failure (and in unit tests), so we fall back to embedded
	// Derby (safe, since no server is serving).
	if network, addr := s.metastoreEnsure(ctx, s.workspace, s.workspaceName()); addr != "" {
		args = appendMetastoreArgs(args, network, addr)
	}
	args = append(args, "-e", "CLAVESA_LOGIC_S3_PATH="+logicPath)
	args = append(args, "-e", "CLAVESA_LANGUAGE="+language)
	args = append(args, "-e", "CLAVESA_WAREHOUSE="+warehouse)
	args = append(args, "-e", "CLAVESA_WATERMARKS="+watermarks)
	args = append(args, "-e", "CLAVESA_PIPELINE="+pipelineName)
	args = append(args, "-e", "CLAVESA_NODE="+node.ID)
	// Three-level namespace (ADR-016). Empty catalog passes through as
	// the legacy signal — the runner's _glue_db() falls back to today's
	// `clavesa_<schema>` literal so pre-ADR pipelines keep finding
	// their data without migration.
	args = append(args, "-e", "CLAVESA_CATALOG="+catalog)
	args = append(args, "-e", "CLAVESA_SCHEMA="+schema)
	// Workspace system catalog (ADR-016 v0.20.0). Runner writes
	// node_runs / runs / tables here regardless of the pipeline schema.
	args = append(args, "-e", "CLAVESA_SYSTEM_CATALOG="+systemCatalog)
	// ADR-017 slice 2: forward env vars referenced by env:-backend
	// credentials so the runner's _resolve_secret can read them. We only
	// forward names referenced by inputs on this transform — no
	// indiscriminate env passthrough. file:-backed credentials need the
	// path mounted (see input mount loop below); arn: backends fetch via
	// boto3 and need AWS creds (out-of-scope, the user's docker AWS env
	// covers that if any).
	for _, name := range envVarsForCredentials(inputs) {
		v, present := os.LookupEnv(name)
		if !present {
			return "", nil, fmt.Errorf("credential references env var %q which is not set in the current shell", name)
		}
		args = append(args, "-e", name+"="+v)
	}
	// ADR-017 slice 3: forward AWS credentials when any input on this
	// transform is kind=s3. Spark's S3A reads use the
	// DefaultAWSCredentialsProviderChain, which checks env vars first;
	// pass through whatever the host shell has so `compute=local`
	// pipelines work against same-account S3 sources without extra
	// configuration. No-op for transforms with no s3 inputs.
	if hasS3Input(inputs) {
		for _, name := range []string{
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
			"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE",
			// Test-infra: lets users point S3A at moto/MinIO without
			// rebuilding the runner image.
			"CLAVESA_S3_ENDPOINT",
		} {
			if v, ok := os.LookupEnv(name); ok {
				args = append(args, "-e", name+"="+v)
			}
		}
		// Mount ~/.aws read-only so AWS_PROFILE-driven credentials
		// (the common dev shape — `aws configure sso`, named
		// profiles in ~/.aws/config) resolve inside the container.
		// boto3 / hadoop-aws's profile chain needs the actual file.
		if home, err := os.UserHomeDir(); err == nil {
			awsDir := filepath.Join(home, ".aws")
			if st, err := os.Stat(awsDir); err == nil && st.IsDir() {
				args = append(args, "-v", awsDir+":/root/.aws:ro")
			}
		}
	}
	// Triage-column env: which exact image and module version produced this
	// row. The digest comes from `docker image inspect` and changes every
	// time the runner is rebuilt. Failures here are non-fatal — passing empty
	// strings degrades to the older runner behavior of leaving the columns
	// blank.
	//
	// CLAVESA_MODULE_VERSION is baked as an ENV inside the image at build
	// time, but the cache-retag path in `workspace.EnsureLocalRunnerImage`
	// can rebrand an image built at a different version (same runner SHA,
	// different `--build-arg CLAVESA_MODULE_VERSION`). Override at run-time
	// so `node_runs.module_version` always reflects the CLI version that
	// orchestrated the run, regardless of what the image was originally
	// built with.
	args = append(args, "-e", "CLAVESA_MODULE_VERSION="+ModuleVersion)
	if digest := dockerImageDigest(image); digest != "" {
		args = append(args, "-e", "CLAVESA_RUNNER_IMAGE_DIGEST="+digest)
	}

	// Mount the workdir so logic + outputs are accessible.
	args = append(args, "-v", workdir+":"+workdir)
	// Mount the per-pipeline warehouse so Iceberg metadata + data files
	// persist across runs. Same path inside and outside the container so
	// table identifiers remain stable.
	args = append(args, "-v", warehouse+":"+warehouse)
	// Mount the per-pipeline watermarks dir for v0.24.0's
	// iceberg_table_incremental kind. Runner reads/writes
	// `<watermarks>/<consumer>__<alias>.json` to track the last-seen
	// upstream snapshot id.
	args = append(args, "-v", watermarks+":"+watermarks)
	// ADR-017 slice 2: file:-backed credential payloads live in the
	// workspace credentials dir; mount read-only so the runner's
	// _resolve_secret can open them at the host-absolute path the
	// orchestration emitter inlined into the descriptor. No-op cost
	// when no file: backends are used; the dir always exists once
	// `workspace init` has run.
	credsDir := filepath.Join(s.workspace, ".clavesa", "credentials")
	if st, err := os.Stat(credsDir); err == nil && st.IsDir() {
		args = append(args, "-v", credsDir+":"+credsDir+":ro")
	}
	// Mount each input path so the runner can read it. Sources may live
	// outside the workdir. Inputs may be string-form (back-compat: parquet) or
	// dict-form (`{"kind":"path","path":...,"format":...}`) once we threaded
	// source format through buildInputs.
	mounted := map[string]bool{workdir: true, warehouse: true}
	for _, v := range inputs {
		p := inputLocalPath(v)
		if p == "" {
			continue
		}
		root := mountRoot(p)
		if !mounted[root] {
			args = append(args, "-v", root+":"+root+":ro")
			mounted[root] = true
		}
	}

	args = append(args, image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(eventJSON)
	var stdout, stderr bytes.Buffer
	// Tee to the per-node log file when requested. The runner's JSON result
	// is the last stdout line; multiwriter doesn't reorder bytes so the JSON
	// parse below still works against the in-memory buffer.
	var stdoutWriters []io.Writer = []io.Writer{&stdout}
	var stderrWriters []io.Writer = []io.Writer{&stderr}
	var logFile *os.File
	var logTS io.WriteCloser
	if logPath != "" {
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err == nil {
			f, ferr := os.Create(logPath)
			if ferr == nil {
				logFile = f
				// Wrap once so stdout and stderr lines share a single
				// timestamping writer with line buffering — interleaved
				// writes are split at newline boundaries and each
				// completed line gets its own ISO timestamp at write
				// time. ExecutionLogs splits the prefix back off when
				// reading so the response carries real per-line
				// timestamps (ADR-014 parity with cloud's CloudWatch).
				logTS = observability.NewTimestampedLogWriter(f)
				stdoutWriters = append(stdoutWriters, logTS)
				stderrWriters = append(stderrWriters, logTS)
			}
		}
	}
	cmd.Stdout = io.MultiWriter(stdoutWriters...)
	cmd.Stderr = io.MultiWriter(stderrWriters...)

	runErr := cmd.Run()
	if logTS != nil {
		_ = logTS.Close()
	}
	if logFile != nil {
		_ = logFile.Close()
	}
	if runErr != nil {
		return "", nil, fmt.Errorf("docker run: %w\nstderr: %s", runErr, stderr.String())
	}

	// Runner writes a single JSON object to stdout. Parse to surface errors.
	resp := map[string]any{}
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &resp); err != nil {
		return "", nil, fmt.Errorf("parse runner output: %w\nstdout: %s\nstderr: %s",
			err, stdout.String(), stderr.String())
	}
	if errMsg, _ := resp["error"].(string); errMsg != "" {
		return "", nil, fmt.Errorf("runner error: %s", errMsg)
	}
	// output_rows is the added-records sum across this invocation's
	// Iceberg outputs (runner.py builds it). Optional — path-mode-only
	// transforms and skipped runs leave it unset.
	var outputRows *int64
	if v, ok := resp["output_rows"]; ok {
		switch n := v.(type) {
		case float64:
			x := int64(n)
			outputRows = &x
		case json.Number:
			if x, err := n.Int64(); err == nil {
				outputRows = &x
			}
		}
	}
	switch status, _ := resp["status"].(string); status {
	case "ok":
		return "", outputRows, nil
	case "skipped":
		// Runner-emitted skip carries a human-readable reason (see runner.py
		// — "input X has no new partitions", "backfill targets …"). Pass it
		// up so the run record + UI can render it inline.
		reason, _ := resp["reason"].(string)
		if reason == "" {
			reason = "skipped"
		}
		return reason, outputRows, nil
	default:
		return "", nil, fmt.Errorf("runner status: %v\nstdout: %s", resp["status"], stdout.String())
	}
}

// bundleTransformConfig is what runPipelineBundle needs per transform —
// kept as an internal contract between executeRun and the bundle runner.
// Mirrors the per-transform fields of the runner's pipeline_handler event.
type bundleTransformConfig struct {
	NodeID    string
	Node      *graph.Node
	Inputs    map[string]any
	Outputs   map[string]any
	LogicPath string
	Language  string
	Parents   []string
	TableID   string
}

// runPipelineBundle invokes the runner image once with CLAVESA_RUN=1 and
// a pipeline-bundle event (presence of `_pipeline_run = true` routes to
// runner.pipeline_handler instead of the per-transform handler). The
// runner shares one Spark session across every transform in the bundle,
// paying the ~3-5s JVM cold-start once instead of once per node.
//
// Progress streaming: pipeline_handler emits one JSON line per
// per-transform state transition to stdout, prefixed with `_event`.
// runPipelineBundle scans those lines as they arrive and fires channel
// events in real time so the UI's per-run state.json reflects progress
// without waiting for the whole pipeline to finish. Non-event stdout
// lines (Spark log noise, anything that fails to JSON-parse) get teed
// to the per-run log file under the pipeline's first node. The final
// aggregate response is the last line on stdout with no `_event` key.
//
// Returns one NodeRunStatus per transform that ran (or skipped) — in
// the order events arrived — plus an error when the bundle itself
// failed (docker exit non-zero, unparseable response, transform
// failure). On a transform failure the bundle stops; statuses for
// downstream transforms are not returned (they never ran).
func (s *Service) runPipelineBundle(ctx context.Context, image, pipelineDir, workdir string, bundle []bundleTransformConfig, runID, catalog, schema, systemCatalog string, channel *runChannel, force bool, forceNodes []string, metastoreNetwork, metastoreAddr string) ([]NodeRunStatus, error) {
	pipelineName := filepath.Base(pipelineDir)
	warehouse := workspace.LocalWarehouseDir(s.workspace)
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		return nil, fmt.Errorf("create warehouse: %w", err)
	}
	watermarks := filepath.Join(pipelineDir, ".clavesa", "watermarks")
	if err := os.MkdirAll(watermarks, 0o755); err != nil {
		return nil, fmt.Errorf("create watermarks dir: %w", err)
	}

	// Build the pipeline event payload. Mirrors the per-transform event
	// shape but carries an array of transforms with the parents map
	// pipeline_handler needs for cascade-skip.
	transforms := make([]map[string]any, 0, len(bundle))
	for _, bt := range bundle {
		entry := map[string]any{
			"node":       bt.NodeID,
			"language":   bt.Language,
			"logic_path": bt.LogicPath,
			"inputs":     bt.Inputs,
			"outputs":    bt.Outputs,
			"parents":    bt.Parents,
		}
		transforms = append(transforms, entry)
	}
	event := map[string]any{
		"_pipeline_run":     true,
		"run_id":            runID,
		"transforms":        transforms,
		"_sf_execution_arn": runID,
		"_trigger":          "manual",
	}
	if force {
		// pipeline_handler unpacks these into each per-transform sub_event;
		// the runner's _is_forced() check fires when a node is in the set
		// (or always, when force_nodes is empty).
		event["_force"] = true
		if len(forceNodes) > 0 {
			event["_force_nodes"] = forceNodes
		}
	}
	eventJSON, _ := json.Marshal(event)

	args := []string{"run", "--rm", "-i"}
	args = append(args, "-e", "CLAVESA_RUN=1")
	args = append(args, "-e", "CLAVESA_WAREHOUSE="+warehouse)
	args = append(args, "-e", "CLAVESA_WATERMARKS="+watermarks)
	args = append(args, "-e", "CLAVESA_PIPELINE="+pipelineName)
	args = append(args, "-e", "CLAVESA_CATALOG="+catalog)
	args = append(args, "-e", "CLAVESA_SCHEMA="+schema)
	args = append(args, "-e", "CLAVESA_SYSTEM_CATALOG="+systemCatalog)
	args = append(args, "-e", "CLAVESA_MODULE_VERSION="+ModuleVersion)
	// Shared local Derby metastore (resolved once in prepareRun). Joins the
	// per-workspace network + sets CLAVESA_METASTORE_ADDR so this run's
	// Spark connects to the Derby Network Server as a client rather than
	// opening the embedded single-writer DB — lets the UI warm worker keep
	// serving queries while this run writes to the same warehouse. No-op
	// when EnsureMetastore failed (falls back to embedded Derby).
	args = appendMetastoreArgs(args, metastoreNetwork, metastoreAddr)
	if digest := dockerImageDigest(image); digest != "" {
		args = append(args, "-e", "CLAVESA_RUNNER_IMAGE_DIGEST="+digest)
	}

	// Credential env passthrough — union across every transform's
	// inputs, since one container reads them all. Mirrors the per-
	// transform logic in runTransform.
	seenEnv := map[string]bool{}
	for _, bt := range bundle {
		for _, name := range envVarsForCredentials(bt.Inputs) {
			if seenEnv[name] {
				continue
			}
			seenEnv[name] = true
			v, present := os.LookupEnv(name)
			if !present {
				return nil, fmt.Errorf("credential references env var %q which is not set in the current shell", name)
			}
			args = append(args, "-e", name+"="+v)
		}
	}
	// AWS env passthrough when any input is kind=s3 (same union policy).
	needsAWS := false
	for _, bt := range bundle {
		if hasS3Input(bt.Inputs) {
			needsAWS = true
			break
		}
	}
	if needsAWS {
		for _, name := range []string{
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
			"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE",
			"CLAVESA_S3_ENDPOINT",
		} {
			if v, ok := os.LookupEnv(name); ok {
				args = append(args, "-e", name+"="+v)
			}
		}
		if home, err := os.UserHomeDir(); err == nil {
			awsDir := filepath.Join(home, ".aws")
			if st, err := os.Stat(awsDir); err == nil && st.IsDir() {
				args = append(args, "-v", awsDir+":/root/.aws:ro")
			}
		}
	}

	// Mount workdir + warehouse + watermarks unconditionally.
	args = append(args, "-v", workdir+":"+workdir)
	args = append(args, "-v", warehouse+":"+warehouse)
	args = append(args, "-v", watermarks+":"+watermarks)
	credsDir := filepath.Join(s.workspace, ".clavesa", "credentials")
	if st, err := os.Stat(credsDir); err == nil && st.IsDir() {
		args = append(args, "-v", credsDir+":"+credsDir+":ro")
	}
	// Union of input mount roots across every transform's inputs.
	mounted := map[string]bool{workdir: true, warehouse: true, watermarks: true}
	for _, bt := range bundle {
		for _, v := range bt.Inputs {
			p := inputLocalPath(v)
			if p == "" {
				continue
			}
			root := mountRoot(p)
			if !mounted[root] {
				args = append(args, "-v", root+":"+root+":ro")
				mounted[root] = true
			}
		}
	}

	args = append(args, image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(eventJSON)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	// Per-run log file under the pipeline's run dir — channel events stream
	// to the per-node log via logPathFor, but the bundle has no single
	// owning node for "everything else stdout"; route non-event stdout AND
	// all stderr into a pipeline-level log so Spark output (and the real
	// stack trace when the session dies) is recoverable for debugging.
	// Created before cmd.Stderr so stderr can be teed into it.
	var bundleLog *os.File
	bundleLogPath := filepath.Join(pipelineDir, ".clavesa", "runs", runID, "_bundle.log")
	if err := os.MkdirAll(filepath.Dir(bundleLogPath), 0o755); err == nil {
		if f, ferr := os.Create(bundleLogPath); ferr == nil {
			bundleLog = f
			defer bundleLog.Close()
		}
	}

	// Keep a bounded in-memory copy for the inline error tail; tee the full
	// stream to the bundle log so the Spark stack trace isn't swallowed.
	var stderrBuf bytes.Buffer
	if bundleLog != nil {
		cmd.Stderr = io.MultiWriter(&stderrBuf, bundleLog)
	} else {
		cmd.Stderr = &stderrBuf
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("docker start: %w", err)
	}

	statuses := make([]NodeRunStatus, 0, len(bundle))
	statusByNode := map[string]NodeRunStatus{}
	nodeTypeByID := map[string]string{}
	for _, bt := range bundle {
		nodeTypeByID[bt.NodeID] = "transform"
	}

	var finalResp map[string]any
	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var msg map[string]any
		if json.Unmarshal(line, &msg) != nil {
			// Non-JSON: Spark log noise. Tee to the per-bundle log.
			if bundleLog != nil {
				fmt.Fprintln(bundleLog, scanner.Text())
			}
			continue
		}
		evRaw, isEvent := msg["_event"]
		if !isEvent {
			// Last JSON object without _event key = the aggregate
			// pipeline result. Overwrite on each occurrence so we keep
			// the last one (defensive against future shape changes).
			finalResp = msg
			continue
		}
		ev, _ := evRaw.(string)
		node, _ := msg["node"].(string)
		switch ev {
		case "entered":
			channel.nodeEntered(node)
		case "succeeded":
			var outputRows *int64
			if v, ok := msg["output_rows"]; ok && v != nil {
				if f, ok := v.(float64); ok {
					n := int64(f)
					outputRows = &n
				}
			}
			channel.nodeSucceeded(node, outputRows)
			st := NodeRunStatus{NodeID: node, Type: "transform", Status: "ok"}
			if t, ok := statusByNode[node]; ok {
				st = t
			}
			st.Status = "ok"
			statusByNode[node] = st
		case "progress":
			channel.nodeProgress(node, NodeProgress{
				StagesTotal:     msgInt64(msg, "stages_total"),
				StagesCompleted: msgInt64(msg, "stages_completed"),
				TasksTotal:      msgInt64(msg, "tasks_total"),
				TasksCompleted:  msgInt64(msg, "tasks_completed"),
				TasksFailed:     msgInt64(msg, "tasks_failed"),
			})
		case "skipped":
			note, _ := msg["note"].(string)
			channel.nodeSkipped(node, note)
			statusByNode[node] = NodeRunStatus{NodeID: node, Type: "transform", Status: "skipped", Note: note}
		case "failed":
			errMsg, _ := msg["error_msg"].(string)
			errClass, _ := msg["error_class"].(string)
			if errClass == "" {
				errClass = "runner_error"
			}
			channel.nodeFailed(node, errClass, errMsg)
			statusByNode[node] = NodeRunStatus{NodeID: node, Type: "transform", Status: "failed", Note: errMsg}
		}
	}
	waitErr := cmd.Wait()

	// Materialize statuses in topo (bundle) order.
	for _, bt := range bundle {
		if st, ok := statusByNode[bt.NodeID]; ok {
			// Wire the table id onto the status's Output so the UI's
			// per-node breakdown shows the produced table.
			if st.Output == "" {
				st.Output = bt.TableID
			}
			statuses = append(statuses, st)
		}
	}

	if waitErr != nil {
		return statuses, fmt.Errorf("pipeline runner: %w\nstderr: %s", waitErr, boundedStderrTail(stderrBuf.String()))
	}
	if finalResp == nil {
		return statuses, fmt.Errorf("pipeline runner produced no result JSON\nstderr: %s", boundedStderrTail(stderrBuf.String()))
	}
	if status, _ := finalResp["status"].(string); status == "failed" {
		failed, _ := finalResp["failed_node"].(string)
		// Docker exited 0 (the runner returned a clean dict), but a node
		// failed. Append the stderr tail so the real Spark stack trace
		// reaches the caller/dashboard rather than blaming a node by name
		// alone.
		return statuses, fmt.Errorf("pipeline failed at node %q\nstderr: %s", failed, boundedStderrTail(stderrBuf.String()))
	}
	return statuses, nil
}

// boundedStderrTail trims captured stderr to the last 2 KiB so the real
// Spark stack trace surfaces in the returned error without dumping an
// unbounded buffer into the error string.
func boundedStderrTail(s string) string {
	const max = 2048
	if len(s) > max {
		return "…" + s[len(s)-max:]
	}
	return s
}

// msgInt64 reads an integer field out of a decoded runner event. JSON
// numbers decode as float64 into map[string]any; this mirrors the
// output_rows conversion above. Returns nil when the key is absent, null,
// or not a number so a partial event decodes cleanly.
func msgInt64(msg map[string]any, key string) *int64 {
	v, ok := msg[key]
	if !ok || v == nil {
		return nil
	}
	f, ok := v.(float64)
	if !ok {
		return nil
	}
	n := int64(f)
	return &n
}

// recordLocalRun invokes the runner image once with CLAVESA_RECORD_RUN=1
// to append a row to <pipeline>.runs in the local Hadoop-catalog warehouse.
// Mirrors the EventBridge-driven runs_writer Lambda used by the cloud path —
// same Iceberg schema, same column order, so observability.LocalProvider.Runs()
// projects identically against both warehouses.
//
// Best-effort: any failure (image missing, container exec error, Spark crash)
// logs to stderr and returns nil. The run already finished and its data is
// already on disk; failing here would punish the user for an observability
// concern they didn't ask for.
func recordLocalRun(ctx context.Context, image, pipelineDir string, channel *runChannel, cascadeSkipped []string, ensureMetastore func(ctx context.Context, workspaceRoot, workspaceName string) (network, addr string)) {
	pipelineName := filepath.Base(pipelineDir)
	// Resolve ADR-016 (catalog, schema) the same way RunPipeline does so
	// the runs row lands in the same Glue DB as the node_runs row this
	// pipeline just produced. Catalog from workspace manifest (empty for
	// legacy); schema from pipeline variables.tf with tfvars override.
	catalog := ""
	systemCatalog := ""
	if m, _ := workspace.Load(filepath.Dir(pipelineDir)); m != nil {
		catalog = m.CatalogIdentifier()
		systemCatalog = m.SystemCatalogIdentifier()
	}
	schema := resolvePipelineSchema(pipelineDir, pipelineName)
	warehouse := workspace.LocalWarehouseDir(filepath.Dir(pipelineDir))
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "[clavesa] runs row: create warehouse: %v\n", err)
		return
	}

	channel.mu.Lock()
	payload := map[string]any{
		"run_id":   channel.state.RunID,
		"pipeline": channel.state.Pipeline,
		// Same value lives on every node_runs row this run produced (via
		// the runner's `_sf_execution_arn` thread-through). The join key
		// is `runs.sf_execution_arn = node_runs.sf_execution_arn`,
		// uniform across local and cloud.
		"sf_execution_arn": channel.state.RunID,
		"status":           channel.state.Status,
		"trigger":          channel.state.Trigger,
		"started_at_ms":    channel.startMs,
		"ended_at_ms":      channel.endedMs,
		"failed_step":      channel.state.FailedStep,
		"error_class":      channel.state.ErrorClass,
		"error_msg":        channel.state.ErrorMsg,
	}
	if channel.state.DurationMs != nil {
		payload["duration_ms"] = *channel.state.DurationMs
	} else {
		payload["duration_ms"] = channel.endedMs - channel.startMs
	}
	// Backfill node_runs rows for cascade-skipped nodes — the cascade path
	// bypassed the runner entirely so they wouldn't otherwise appear in the
	// Runs grid. One row per node, all sharing the run's sf_execution_arn so
	// the dashboard joins them to this execution.
	if len(cascadeSkipped) > 0 {
		nowMs := channel.endedMs
		skipped := make([]map[string]any, 0, len(cascadeSkipped))
		for _, nodeID := range cascadeSkipped {
			skipped = append(skipped, map[string]any{
				"node":          nodeID,
				"reason":        "all upstreams skipped",
				"started_at_ms": nowMs,
				"ended_at_ms":   nowMs,
			})
		}
		payload["cascade_skipped_nodes"] = skipped
	}
	channel.mu.Unlock()

	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[clavesa] runs row: marshal payload: %v\n", err)
		return
	}

	args := []string{"run", "--rm", "-i"}
	args = append(args, "-e", "CLAVESA_RECORD_RUN=1")
	args = append(args, "-e", "CLAVESA_WAREHOUSE="+warehouse)
	// Shared local Derby metastore — the runs-row write hits the same
	// hive→Derby warehouse the transforms just wrote, and runs while the
	// UI warm worker may still be serving, so it must speak to the Derby
	// Network Server as a client rather than open the embedded DB.
	// workspaceRoot is `filepath.Dir(pipelineDir)` (matches LocalWarehouseDir
	// above). Best-effort: fall back to embedded Derby on Ensure failure.
	recordWorkspaceRoot := filepath.Dir(pipelineDir)
	recordWorkspaceName := ""
	if m, _ := workspace.Load(recordWorkspaceRoot); m != nil {
		recordWorkspaceName = m.Name
	}
	if network, addr := ensureMetastore(ctx, recordWorkspaceRoot, recordWorkspaceName); addr != "" {
		args = appendMetastoreArgs(args, network, addr)
	}
	args = append(args, "-e", "CLAVESA_PIPELINE="+pipelineName)
	args = append(args, "-e", "CLAVESA_CATALOG="+catalog)
	args = append(args, "-e", "CLAVESA_SCHEMA="+schema)
	args = append(args, "-e", "CLAVESA_SYSTEM_CATALOG="+systemCatalog)
	// Override the baked-in version — see runTransform for the rationale.
	args = append(args, "-e", "CLAVESA_MODULE_VERSION="+ModuleVersion)
	if digest := dockerImageDigest(image); digest != "" {
		args = append(args, "-e", "CLAVESA_RUNNER_IMAGE_DIGEST="+digest)
	}
	args = append(args, "-v", warehouse+":"+warehouse)
	args = append(args, image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(body)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if runErr := cmd.Run(); runErr != nil {
		fmt.Fprintf(os.Stderr, "[clavesa] runs row write failed: %v\nstderr: %s\n",
			runErr, stderr.String())
	}
}

// readNodeLogic returns the SQL or Python source for a transform node, with
// file("...") references resolved against the pipeline directory.
func readNodeLogic(node *graph.Node, language, pipelineDir string) (string, error) {
	var raw string
	if language == "python" {
		raw, _ = node.Config["python"].(string)
	} else {
		// Mirror preview.extractSQL — node.PreviewSQL is set by the parser.
		if node.PreviewSQL != "" {
			raw = node.PreviewSQL
		} else {
			raw, _ = node.Config["sql"].(string)
		}
	}
	if raw == "" {
		return "", fmt.Errorf("node has no %s configuration", language)
	}
	if path, ok := parseFileRef(raw); ok {
		data, err := os.ReadFile(filepath.Join(pipelineDir, path))
		if err != nil {
			return "", fmt.Errorf("read %s file %s: %w", language, path, err)
		}
		return string(data), nil
	}
	return raw, nil
}

// parseFileRef recognises `file("relative/path")` HCL idiom.
func parseFileRef(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "file(") || !strings.HasSuffix(expr, ")") {
		return "", false
	}
	inner := strings.TrimSpace(expr[len("file(") : len(expr)-1])
	inner = strings.Trim(inner, `"`)
	return inner, true
}

// dockerImageDigest returns the local docker image's content-addressable ID
// (`sha256:<hex>`) for `ref`, or "" if the image isn't present or docker
// isn't reachable. Used to stamp every node_runs / runs row with the exact
// image build that produced it — same intent as the cloud Lambda's
// data.aws_ecr_image.runner.image_digest, just sourced from the local daemon
// instead of ECR.
func dockerImageDigest(ref string) string {
	out, err := exec.Command("docker", "image", "inspect", "--format", "{{.Id}}", ref).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// hasS3Input reports whether any descriptor in inputs reads from S3 — the
// signal for forwarding AWS credentials into the runner container. Covers
// both `kind=s3` (raw s3 source) and `kind=partitioned_path` whose path is
// s3:// (the v0.12.0 incremental shape registered s3 sources resolve to).
func hasS3Input(inputs map[string]any) bool {
	for _, v := range inputs {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		k, _ := m["kind"].(string)
		if k == "s3" {
			return true
		}
		if k == "partitioned_path" {
			if p, _ := m["path"].(string); strings.HasPrefix(p, "s3://") {
				return true
			}
		}
	}
	return false
}

// envVarsForCredentials walks an inputs map looking for env:-backed
// credential references and returns the variable names. Used by the
// docker-exec arg builder to forward only the variables that are
// actually needed (no indiscriminate env passthrough).
func envVarsForCredentials(inputs map[string]any) []string {
	var out []string
	for _, v := range inputs {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		cred, ok := m["credentials"].(map[string]any)
		if !ok {
			continue
		}
		secret, _ := cred["secret"].(string)
		if strings.HasPrefix(secret, "env:") {
			out = append(out, strings.TrimPrefix(secret, "env:"))
		}
	}
	return out
}

// inputLocalPath extracts the local-FS path from an input descriptor, returning
// "" for descriptors that don't carry one (e.g. Iceberg-table id strings, S3
// paths, partitioned_path dicts — those resolve inside the runner). Used for
// deciding what bind-mounts the runner container needs.
func inputLocalPath(v any) string {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "/") || strings.HasPrefix(x, "./") || strings.HasPrefix(x, "../") {
			return x
		}
		return ""
	case map[string]any:
		if x["kind"] != "path" {
			return ""
		}
		p, _ := x["path"].(string)
		return p
	}
	return ""
}

// mountRoot returns the deepest directory we can mount that contains `path`.
// Files: parent dir. Directories: the directory itself.
func mountRoot(path string) string {
	st, err := os.Stat(path)
	if err == nil && st.IsDir() {
		return path
	}
	return filepath.Dir(path)
}

// ---------------------------------------------------------------------------
// Local progress channel
// ---------------------------------------------------------------------------

// runChannel writes the on-disk progress state observability.LocalProvider
// reads from. One per pipeline run; methods are safe to call from a single
// goroutine (RunPipeline is sequential).
type runChannel struct {
	pipelineDir string
	state       *observability.RunStateFile
	startMs     int64
	endedMs     int64            // set in finish() — drives the runs row's ended_at_ms
	nodeStarts  map[string]int64 // nodeID → entered_at unix-millis (for duration calc)
	mu          sync.Mutex       // guards state writes against concurrent reads
}

func newRunChannel(pipelineDir, runID, pipelineName string) *runChannel {
	now := time.Now().UTC()
	return &runChannel{
		pipelineDir: pipelineDir,
		startMs:     now.UnixMilli(),
		state: &observability.RunStateFile{
			RunID:     runID,
			Pipeline:  pipelineName,
			Status:    "RUNNING",
			StartedAt: now.Format(time.RFC3339Nano),
			Trigger:   "manual",
			States:    map[string]observability.NodeRunState{},
		},
		nodeStarts: map[string]int64{},
	}
}

// start writes the initial state.json with one map entry per ordered node.
// Sources/destinations get marked SUCCEEDED/SKIPPED inline below; this just
// surfaces the topology before execution begins.
func (c *runChannel) start(order []string, _ *graph.PipelineGraph) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range order {
		c.state.States[id] = observability.NodeRunState{Status: "PENDING"}
	}
	return observability.WriteRunState(c.pipelineDir, c.state)
}

func (c *runChannel) logPathFor(nodeID string) string {
	return observability.RunLogPath(c.pipelineDir, c.state.RunID, nodeID)
}

func (c *runChannel) nodeEntered(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	c.nodeStarts[nodeID] = now.UnixMilli()
	c.state.States[nodeID] = observability.NodeRunState{
		Status:    "RUNNING",
		EnteredAt: now.Format(time.RFC3339Nano),
	}
	_ = observability.WriteRunState(c.pipelineDir, c.state)
}

// NodeProgress carries the in-flight Spark counters the runner emits on a
// `progress` event. All fields nullable so a partial event (or a future
// runner that drops a field) decodes cleanly.
type NodeProgress struct {
	StagesTotal     *int64
	StagesCompleted *int64
	TasksTotal      *int64
	TasksCompleted  *int64
	TasksFailed     *int64
}

// nodeProgress folds an in-flight progress tick onto the node's RUNNING
// state and rewrites state.json. Defensive: only applies when the node
// entry exists and is still RUNNING, so a late tick arriving after
// succeeded/failed (or before entered) is dropped rather than resurrecting
// a terminal node.
func (c *runChannel) nodeProgress(nodeID string, p NodeProgress) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, ok := c.state.States[nodeID]
	if !ok || prev.Status != "RUNNING" {
		return
	}
	prev.StagesTotal = p.StagesTotal
	prev.StagesCompleted = p.StagesCompleted
	prev.TasksTotal = p.TasksTotal
	prev.TasksCompleted = p.TasksCompleted
	prev.TasksFailed = p.TasksFailed
	c.state.States[nodeID] = prev
	_ = observability.WriteRunState(c.pipelineDir, c.state)
}

func (c *runChannel) nodeSucceeded(nodeID string, outputRows *int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := c.state.States[nodeID]
	now := time.Now().UTC()
	if prev.EnteredAt == "" {
		prev.EnteredAt = now.Format(time.RFC3339Nano)
	}
	dur := c.durationFor(nodeID, now)
	c.state.States[nodeID] = observability.NodeRunState{
		Status:     "SUCCEEDED",
		EnteredAt:  prev.EnteredAt,
		ExitedAt:   now.Format(time.RFC3339Nano),
		DurationMs: dur,
		OutputRows: outputRows,
	}
	_ = observability.WriteRunState(c.pipelineDir, c.state)
}

func (c *runChannel) nodeFailed(nodeID, errClass, errMsg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := c.state.States[nodeID]
	now := time.Now().UTC()
	if prev.EnteredAt == "" {
		prev.EnteredAt = now.Format(time.RFC3339Nano)
	}
	dur := c.durationFor(nodeID, now)
	c.state.States[nodeID] = observability.NodeRunState{
		Status:     "FAILED",
		EnteredAt:  prev.EnteredAt,
		ExitedAt:   now.Format(time.RFC3339Nano),
		DurationMs: dur,
		ErrorClass: errClass,
		ErrorMsg:   truncate(errMsg, 4096),
	}
	if c.state.FailedStep == "" {
		c.state.FailedStep = nodeID
	}
	_ = observability.WriteRunState(c.pipelineDir, c.state)
}

func (c *runChannel) nodeSkipped(nodeID, note string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.States[nodeID] = observability.NodeRunState{
		Status:   "SKIPPED",
		ErrorMsg: note,
	}
	_ = observability.WriteRunState(c.pipelineDir, c.state)
}

func (c *runChannel) finish(finalErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now().UTC()
	c.endedMs = now.UnixMilli()
	c.state.EndedAt = now.Format(time.RFC3339Nano)
	dur := c.endedMs - c.startMs
	c.state.DurationMs = &dur
	if finalErr != nil {
		c.state.Status = "FAILED"
		if c.state.ErrorClass == "" {
			c.state.ErrorClass = "PipelineFailed"
		}
		if c.state.ErrorMsg == "" {
			c.state.ErrorMsg = truncate(finalErr.Error(), 4096)
		}
	} else {
		c.state.Status = "SUCCEEDED"
	}
	_ = observability.WriteRunState(c.pipelineDir, c.state)
}

// durationFor returns the millisecond duration since the node entered, or
// nil when no entered_at was recorded (source/destination shortcuts).
func (c *runChannel) durationFor(nodeID string, now time.Time) *int64 {
	startedMs, ok := c.nodeStarts[nodeID]
	if !ok {
		return nil
	}
	d := now.UnixMilli() - startedMs
	return &d
}

// truncate caps a string to maxLen, appending an ellipsis to signal cut.
// Matches the runner's 4 KB error-message cap so cloud and local errors
// surface uniformly bounded.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// buildLocalOutputs builds the runner-event outputs map for `pipeline run`.
// Default key gets the caller-resolved target (auto-Iceberg table id);
// any additional keys declared in output_definitions go in as empty
// strings so the runner falls back to its auto-table derivation
// (`clavesa.<db>.<node>__<key>`). Per-key mode + merge_keys flow
// through as dict descriptors when the user declared them. This mirrors
// the cloud-side orchestration emit, so local and cloud carry the same
// payload shape for multi-output transforms (ADR-014).
func buildLocalOutputs(node *graph.Node, defaultTarget string) map[string]any {
	out := map[string]any{"default": defaultTarget}
	defs, _ := node.Config["output_definitions"].(map[string]interface{})
	if len(defs) == 0 {
		return out
	}
	for key, raw := range defs {
		def, _ := raw.(map[string]interface{})
		mode, _ := def["mode"].(string)
		stats, _ := def["stats"].(bool)
		var mergeKeys []string
		if mk, ok := def["merge_keys"].([]interface{}); ok {
			for _, v := range mk {
				if s, ok := v.(string); ok {
					mergeKeys = append(mergeKeys, s)
				}
			}
		}
		var clusterKeys []string
		if ck, ok := def["cluster_by"].([]interface{}); ok {
			for _, v := range ck {
				if s, ok := v.(string); ok {
					clusterKeys = append(clusterKeys, s)
				}
			}
		}
		mergeUpdate := map[string]string{}
		if mu, ok := def["merge_update"].(map[string]interface{}); ok {
			for col, v := range mu {
				if s, ok := v.(string); ok {
					mergeUpdate[col] = s
				}
			}
		}
		if mode == "" && len(mergeKeys) == 0 && len(clusterKeys) == 0 && len(mergeUpdate) == 0 && !stats {
			// Replace + no merge keys + stats off: leave bare string.
			// "default" carries the caller's explicit target;
			// everything else gets "" → runner auto-table.
			if _, ok := out[key]; !ok {
				out[key] = ""
			}
			continue
		}
		target := ""
		if key == "default" {
			target = defaultTarget
		}
		// Match the runner's _resolve_output default (runner/runner.py
		// L775) AND the cloud-side `outputMode` resolution in
		// orchestration.go: merge_keys present + mode unset → "merge".
		// Without this, the recipe shape `--output-merge-keys customer_id`
		// (without an explicit --output-mode) silently falls back to
		// "replace" on local pipelines — keeping the row count flat but
		// running full-table createOrReplace instead of MERGE INTO, so
		// snapshot history shows append+full-replace ops instead of the
		// COW-merge overwrites the recipe documents.
		resolvedMode := mode
		if resolvedMode == "" {
			if len(mergeKeys) > 0 {
				resolvedMode = "merge"
			} else {
				resolvedMode = "replace"
			}
		}
		desc := map[string]any{
			"kind":       "delta_table",
			"table_id":   target,
			"mode":       resolvedMode,
			"merge_keys": mergeKeys,
		}
		if stats {
			desc["stats"] = true
		}
		if len(clusterKeys) > 0 {
			desc["cluster_by"] = clusterKeys
		}
		if len(mergeUpdate) > 0 {
			desc["merge_update"] = mergeUpdate
		}
		out[key] = desc
	}
	return out
}

// runOperation invokes the runner image with an `_operation` event — the
// non-transform control-plane path the runner exposes for backfill promote /
// discard. Cloud uses `lambda.Invoke` with the same payload; local mode has
// no Lambda, so we run the same image directly against the workspace's
// Hadoop catalog. The container reads its event from stdin (CLAVESA_RUN=1)
// and the operation handler routes on the `_operation` key.
//
// Returns the parsed runner response map (keys vary by operation; the caller
// reads what it needs). An error covers both transport failures and runner-
// reported `{"error": "..."}` envelopes — the caller treats both the same.
func (s *Service) runOperation(ctx context.Context, op map[string]any) (map[string]any, error) {
	image := runner.LocalImageName("") + ":latest"
	if _, err := workspace.Load(s.workspace); err == nil {
		// Auto-refresh the workspace runner image if a CLI upgrade
		// shipped new runner code (see EnsureLocalRunnerImage).
		ensured, err := workspace.EnsureLocalRunnerImage(s.workspace)
		if err != nil {
			return nil, fmt.Errorf("ensure runner image: %w", err)
		}
		image = ensured
	}

	warehouse := workspace.LocalWarehouseDir(s.workspace)
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		return nil, fmt.Errorf("create warehouse: %w", err)
	}

	body, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}

	args := []string{"run", "--rm", "-i"}
	args = append(args, "-e", "CLAVESA_RUN=1")
	args = append(args, "-e", "CLAVESA_WAREHOUSE="+warehouse)
	// Shared local Derby metastore. The operation handler (backfill
	// promote / discard) reads + rewrites Delta tables in the same
	// hive→Derby warehouse and can run while the UI warm worker is live,
	// so it connects to the Derby Network Server as a client when one is
	// up. Best-effort: fall back to embedded Derby on Ensure failure.
	if network, addr := s.metastoreEnsure(ctx, s.workspace, s.workspaceName()); addr != "" {
		args = appendMetastoreArgs(args, network, addr)
	}
	// Override the baked-in version — see runTransform for the rationale.
	args = append(args, "-e", "CLAVESA_MODULE_VERSION="+ModuleVersion)
	args = append(args, "-v", warehouse+":"+warehouse)
	args = append(args, image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(body)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	out := bytes.TrimSpace(stdout.Bytes())
	if len(out) > 0 {
		var resp map[string]any
		if err := json.Unmarshal(out, &resp); err == nil {
			if msg, _ := resp["error"].(string); msg != "" {
				return nil, fmt.Errorf("runner operation %v: %s", op["_operation"], msg)
			}
			return resp, nil
		}
	}
	if runErr != nil {
		return nil, fmt.Errorf("docker run operation: %w\nstdout: %s\nstderr: %s", runErr, stdout.String(), stderr.String())
	}
	return nil, fmt.Errorf("runner operation %v: no parseable output\nstdout: %s\nstderr: %s", op["_operation"], stdout.String(), stderr.String())
}

// workspaceName resolves the workspace manifest name EnsureMetastore needs
// to pick the runner image the metastore container reuses. Empty when the
// manifest isn't readable (legacy / uninitialized) — EnsureMetastore then
// falls back to the empty-name image.
func (s *Service) workspaceName() string {
	if m, _ := workspace.Load(s.workspace); m != nil {
		return m.Name
	}
	return ""
}

// appendMetastoreArgs appends the `--network` + CLAVESA_METASTORE_ADDR run
// args that point a local Spark client container at the shared Derby
// metastore. No-op when network/addr are empty (EnsureMetastore failed or
// hasn't run), in which case the container falls back to embedded Derby.
// Centralizes the wiring so every local-Spark launch site in this package
// stays consistent.
func appendMetastoreArgs(args []string, network, addr string) []string {
	if network == "" || addr == "" {
		return args
	}
	args = append(args, "--network", network)
	args = append(args, "-e", "CLAVESA_METASTORE_ADDR="+addr)
	return args
}

// newRunID returns a 32-char hex run identifier matching the runner's
// uuid.uuid4().hex shape — keeps both halves of the system speaking the
// same identifier vocabulary.
func newRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure on Linux/macOS would be exceptional; fall back
		// to time-based bits rather than panic during a user pipeline run.
		t := time.Now().UnixNano()
		for i := 0; i < 16; i++ {
			b[i] = byte(t >> (8 * (i % 8)))
		}
	}
	return hex.EncodeToString(b[:])
}
