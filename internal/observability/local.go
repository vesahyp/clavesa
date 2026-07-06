package observability

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/delta"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/pathutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// LocalProvider satisfies Provider for workspaces on the local warehouse
// (ADR-024: the Resolver dispatches by workspace.LoadWarehouse, never by a
// per-pipeline attribute).
//
// Run history (NodeRuns/Runs/Snapshots/Table) and execution state share the
// same response shapes as CloudProvider so the UI cannot tell which backend
// served a request — ADR-014.
//
// Execution states come from the warehouse `_progress/<run>/` marker tree
// the runner + dispatch layer write (3be08e3); execution logs come from the
// per-run `_bundle.log` under <pipelineDir>/.clavesa/runs/<runID>/. The
// Delta-backed surfaces (NodeRuns, Runs, Snapshots) reuse the runner
// container in CLAVESA_QUERY=1 mode to read against the local Hadoop
// catalog — same SQL surface the cloud provider uses through Athena.
type LocalProvider struct {
	workspaceRoot string
	query         QueryRunner // override for tests; nil → docker shell-out
}

// NewLocalProvider wires a provider against a workspace root. PipelineDir
// resolution uses pathutil.ResolveDir so callers can pass either an absolute
// path or a workspace-relative one.
func NewLocalProvider(workspaceRoot string) *LocalProvider {
	return &LocalProvider{workspaceRoot: workspaceRoot}
}

// ---------------------------------------------------------------------------
// ExecutionStates
// ---------------------------------------------------------------------------

// ExecutionStates reads the warehouse `_progress` tree for one run.
//
// ExecutionRef is an exec-ref token (see execref.go). When it carries no run
// id, returns the most recent run in the warehouse — matches how
// CloudProvider treats the "latest in-flight execution" case via the SFN
// ListExecutions path.
func (p *LocalProvider) ExecutionStates(ctx context.Context, q ExecutionStatesQuery) (*ExecutionStatesResult, error) {
	store := p.progressStore()
	_, runID := SplitExecRef(q.ExecutionRef)
	if runID == "" {
		// No explicit run id: inspect the newest run in the warehouse
		// `_progress` tree. Empty when nothing has run yet — the cloud
		// provider's "no executions" parity (clean empty state, not a 500).
		ids, err := ListProgressRunIDs(p.warehouseDir())
		if err != nil {
			return nil, fmt.Errorf("list local runs: %w", err)
		}
		if len(ids) == 0 {
			return &ExecutionStatesResult{States: map[string]StateStatus{}}, nil
		}
		runID = ids[0]
	}

	// Per-node states from the warehouse progress markers — the same shared
	// helper the cloud provider uses (ADR-014 parity).
	states := progressStates(ctx, store, runID, time.Now().UnixMilli())

	out := &ExecutionStatesResult{
		Status: "RUNNING",
		States: states,
		RunID:  runID,
	}
	// Overall status + start time come from the run marker. Absent marker on
	// a freshly dispatched run reads as RUNNING so the dashboard renders an
	// in-flight column rather than flashing empty.
	if m, ok, _ := readRunMarker(ctx, store, runID); ok && m != nil {
		if m.Status != "" {
			out.Status = m.Status
		}
		if m.StartedMs != nil {
			out.StartedAt = formatMillis(*m.StartedMs)
		}
	}
	return out, nil
}

// RunDetail reads one run's `_run.json` run marker and projects its
// failure context. Found=false when no marker exists (a fresh / pre-marker
// run). Backs the GET /pipeline/execution detail endpoint for local runs.
func (p *LocalProvider) RunDetail(ctx context.Context, run string) (RunDetail, error) {
	m, ok, err := readRunMarker(ctx, p.progressStore(), run)
	if err != nil {
		return RunDetail{}, err
	}
	if !ok || m == nil {
		return RunDetail{}, nil
	}
	return RunDetail{
		Status:     m.Status,
		FailedStep: m.FailedStep,
		ErrorClass: m.ErrorClass,
		ErrorMsg:   m.ErrorMsg,
		Found:      true,
	}, nil
}

// warehouseDir is the workspace-shared local warehouse the runner + dispatch
// layer write the `_progress` tree under (ADR-024).
func (p *LocalProvider) warehouseDir() string {
	return workspace.LocalWarehouseDir(p.workspaceRoot)
}

// progressStore builds a filesystem ProgressStore rooted at the local
// warehouse — the read path for ExecutionStates / Runs / nodeRunsFromProgress.
func (p *LocalProvider) progressStore() ProgressStore {
	return NewFileProgressStore(p.warehouseDir())
}

// ---------------------------------------------------------------------------
// ExecutionLogs
// ---------------------------------------------------------------------------

// logsLineCap caps how many lines one /pipeline/execution/logs response
// returns, mirroring CloudProvider's logsLimit. Captured runner output for
// one run rarely exceeds a few hundred lines; the cap protects against a
// runaway log file (e.g. a Spark plan dump).
const logsLineCap = 500

// ExecutionLogs serves the run's `_bundle.log` — the full runner
// stdout/stderr the bundle path tees to
// <pipelineDir>/.clavesa/runs/<runID>/_bundle.log (GH #64).
//
// The bundle runner shares one container and one Spark session across every
// node in the run, so the captured log is per-RUN, not per-step: the same
// events are returned whichever step the caller asks about, labeled per-run
// (FunctionName = run id, LogGroup = the on-disk path). Step is still
// required for interface parity with the cloud provider, which windows
// CloudWatch by step.
//
// Line format on disk is `<RFC3339Nano>\t<message>` (NewTimestampedLogWriter),
// so per-event Timestamps match the cloud CloudWatch payload (ADR-014
// parity). Lines without the prefix (pre-timestamping log files) fall
// through with Timestamp:"" and the raw line as Message.
func (p *LocalProvider) ExecutionLogs(ctx context.Context, q ExecutionLogsQuery) (*ExecutionLogsResult, error) {
	if q.Step == "" {
		return nil, errors.New("step is required")
	}
	dir, runID := SplitExecRef(q.ExecutionRef)
	if runID == "" {
		// No explicit run id — newest run in the warehouse `_progress` tree.
		ids, err := ListProgressRunIDs(p.warehouseDir())
		if err != nil {
			return nil, fmt.Errorf("list local runs: %w", err)
		}
		if len(ids) == 0 {
			return &ExecutionLogsResult{
				Source:       LogSourceLocal,
				FunctionName: q.Step,
				Events:       []LogEvent{},
			}, nil
		}
		runID = ids[0]
	}
	if dir == "" {
		// Bare run-id ref (e.g. `arn=<uuid>` from the drawer): recover the
		// owning pipeline from the run marker — pipeline name == dir basename
		// by convention.
		if m, ok, _ := readRunMarker(ctx, p.progressStore(), runID); ok && m != nil {
			dir = m.Pipeline
		}
	}
	if dir == "" {
		// Can't locate a pipeline dir for this run — no log to serve.
		// Empty events, not an error (matches CloudProvider's "not an SFN
		// execution" fail-soft).
		return &ExecutionLogsResult{
			Source:       LogSourceLocal,
			FunctionName: runID,
			Events:       []LogEvent{},
		}, nil
	}

	logPath := RunBundleLogPath(pathutil.ResolveDir(p.workspaceRoot, dir), runID)
	f, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No bundle log yet — the run hasn't started writing, or predates
			// the bundle runner. Empty events, not an error.
			return &ExecutionLogsResult{
				Source:       LogSourceLocal,
				LogGroup:     logPath,
				FunctionName: runID,
				Events:       []LogEvent{},
			}, nil
		}
		return nil, fmt.Errorf("open log file: %w", err)
	}
	defer f.Close()

	events := make([]LogEvent, 0, 64)
	truncated := false
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		if len(events) >= logsLineCap {
			truncated = true
			break
		}
		ts, msg := ParseLogLine(scanner.Text())
		events = append(events, LogEvent{Timestamp: ts, Message: msg})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read log file: %w", err)
	}

	return &ExecutionLogsResult{
		Source:       LogSourceLocal,
		LogGroup:     logPath,
		FunctionName: runID,
		Events:       events,
		Truncated:    truncated,
	}, nil
}

// ---------------------------------------------------------------------------
// Delta-backed surfaces (NodeRuns / Runs / Tables / Snapshots / ColumnStats /
// SampleTable / Query). Fast paths read the warehouse `_progress` tree or
// `_delta_log/` directly from the filesystem; the rest run SQL against the
// local warehouse via the runner container in CLAVESA_QUERY=1 mode — the
// same SQL surface the cloud provider drives through Athena (ADR-014).
// ---------------------------------------------------------------------------

// NodeRuns issues the same SQL CloudProvider runs against Athena, but against
// the local Hadoop catalog via the runner image. Same row shape; the UI sees
// no difference.
func (p *LocalProvider) NodeRuns(ctx context.Context, q NodeRunsQuery) (*NodeRunsResult, error) {
	if !validPipelineName(q.PipelineName) {
		return nil, fmt.Errorf("invalid pipeline name: %q", q.PipelineName)
	}
	if q.Node != "" && !validIdentifier(q.Node) {
		return nil, fmt.Errorf("invalid node name: %q", q.Node)
	}

	// Fast path: the dashboard's grid hits this endpoint with no
	// `arn` filter and just needs per-cell status + duration. The warehouse
	// `_progress/<run>/<node>.json` markers carry both — sourcing them direct
	// from the filesystem avoids the 1.5s-warm / 30s-cold Spark roundtrip that
	// was making the grid look like it lost its data on every refresh. The
	// Sheet's drill-down (arn-filtered) still goes through Spark below to pick
	// up the richer columns (image digest, module version, the per-invocation
	// Spark metrics) the progress markers don't carry.
	if q.SfExecutionARN == "" && !q.IncludeMetrics {
		return p.nodeRunsFromProgress(ctx, q)
	}

	dbName := q.Database
	// Workspace-wide system DB (ADR-016 v0.20.0) is multi-writer — every
	// pipeline appends to the same node_runs table, distinguished by the
	// `pipeline` column. Filter at the SQL boundary so the row shape
	// returned to the UI is unchanged.
	conds := []string{fmt.Sprintf("pipeline = '%s'", strings.ReplaceAll(q.PipelineName, "'", "''"))}
	if q.Node != "" {
		conds = append(conds, fmt.Sprintf("node = '%s'", q.Node))
	}
	if q.SfExecutionARN != "" {
		// Validated by the handler layer (hex / dotted-ARN charset only) so
		// literal-substitution is safe. We escape single-quotes defensively
		// in case a future caller forgets — Iceberg/Spark don't support
		// SQL parameter binding through DataFrameWriterV2.
		safe := strings.ReplaceAll(q.SfExecutionARN, "'", "''")
		conds = append(conds, fmt.Sprintf("sf_execution_arn = '%s'", safe))
	}
	whereClause := "WHERE " + strings.Join(conds, " AND ")
	// Limit+1 detects truncation. Spark / Iceberg's date_format works the
	// same as Athena's to_iso8601 for our timestamp columns; keep both
	// providers' SQL surfaces aligned modulo the function name.
	sql := fmt.Sprintf(
		`SELECT
  run_id,
  pipeline,
  node,
  concat(date_format(started_at, 'yyyy-MM-dd'), 'T', date_format(started_at, 'HH:mm:ss.SSSXXX')) AS started_at,
  concat(date_format(ended_at,   'yyyy-MM-dd'), 'T', date_format(ended_at,   'HH:mm:ss.SSSXXX')) AS ended_at,
  duration_ms,
  status,
  compute_target,
  memory_mb,
  cold_start,
  lambda_request_id,
  error_class,
  error_msg,
  runner_image_digest,
  module_version,
  output_rows,
  sf_execution_arn,
  peak_rss_mb,
  peak_execution_memory_mb,
  memory_spilled_bytes,
  disk_spilled_bytes,
  shuffle_read_bytes,
  shuffle_write_bytes,
  input_bytes,
  input_records,
  num_stages,
  num_tasks,
  num_failed_tasks,
  jvm_gc_time_ms,
  executor_cpu_time_ms,
  executor_run_time_ms,
  max_task_duration_ms
FROM %s.node_runs
%s
ORDER BY started_at DESC
LIMIT %d`, dbName, whereClause, q.Limit+1)

	res, err := p.runQueryFor(ctx, q.PipelineDir, sql)
	if err != nil {
		// Fresh workspace where no pipeline has produced node_runs yet —
		// the workspace system DB exists in the Hadoop catalog but the
		// table is created lazily on first runner write. Surface an
		// empty result so the pipeline dashboard renders cleanly on
		// step 0 instead of flashing a Spark stack trace.
		if isMissingTableErr(err) {
			return &NodeRunsResult{Rows: []NodeRun{}}, nil
		}
		return nil, err
	}
	idx := columnIndex(res.Columns)

	rows := res.Rows
	truncated := false
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		truncated = true
	}

	out := make([]NodeRun, 0, len(rows))
	for _, r := range rows {
		out = append(out, NodeRun{
			RunID:             stringValue(rowAt(r, idx, "run_id")),
			Pipeline:          stringValue(rowAt(r, idx, "pipeline")),
			Node:              stringValue(rowAt(r, idx, "node")),
			StartedAt:         stringValue(rowAt(r, idx, "started_at")),
			EndedAt:           stringValue(rowAt(r, idx, "ended_at")),
			DurationMs:        int64Pointer(rowAt(r, idx, "duration_ms")),
			Status:            stringValue(rowAt(r, idx, "status")),
			ComputeTarget:     stringValue(rowAt(r, idx, "compute_target")),
			MemoryMB:          int64Pointer(rowAt(r, idx, "memory_mb")),
			ColdStart:         boolPointer(rowAt(r, idx, "cold_start")),
			LambdaRequestID:   stringValue(rowAt(r, idx, "lambda_request_id")),
			ErrorClass:        stringValue(rowAt(r, idx, "error_class")),
			ErrorMsg:          stringValue(rowAt(r, idx, "error_msg")),
			RunnerImageDigest: stringValue(rowAt(r, idx, "runner_image_digest")),
			ModuleVersion:     stringValue(rowAt(r, idx, "module_version")),
			OutputRows:        int64Pointer(rowAt(r, idx, "output_rows")),
			SfExecutionARN:    stringValue(rowAt(r, idx, "sf_execution_arn")),

			PeakRSSMB:             int64Pointer(rowAt(r, idx, "peak_rss_mb")),
			PeakExecutionMemoryMB: int64Pointer(rowAt(r, idx, "peak_execution_memory_mb")),
			MemorySpilledBytes:    int64Pointer(rowAt(r, idx, "memory_spilled_bytes")),
			DiskSpilledBytes:      int64Pointer(rowAt(r, idx, "disk_spilled_bytes")),
			ShuffleReadBytes:      int64Pointer(rowAt(r, idx, "shuffle_read_bytes")),
			ShuffleWriteBytes:     int64Pointer(rowAt(r, idx, "shuffle_write_bytes")),
			InputBytes:            int64Pointer(rowAt(r, idx, "input_bytes")),
			InputRecords:          int64Pointer(rowAt(r, idx, "input_records")),
			NumStages:             int64Pointer(rowAt(r, idx, "num_stages")),
			NumTasks:              int64Pointer(rowAt(r, idx, "num_tasks")),
			NumFailedTasks:        int64Pointer(rowAt(r, idx, "num_failed_tasks")),
			JVMGCTimeMs:           int64Pointer(rowAt(r, idx, "jvm_gc_time_ms")),
			ExecutorCPUTimeMs:     int64Pointer(rowAt(r, idx, "executor_cpu_time_ms")),
			ExecutorRunTimeMs:     int64Pointer(rowAt(r, idx, "executor_run_time_ms")),
			MaxTaskDurationMs:     int64Pointer(rowAt(r, idx, "max_task_duration_ms")),
		})
	}
	return &NodeRunsResult{Rows: out, Truncated: truncated}, nil
}

// Runs reads the per-pipeline-execution rollup directly from the warehouse
// `_progress` tree — one entry per run directory, projected from its
// `_run.json` run marker. The SQL path the cloud provider uses (Athena over
// the workspace `runs` table) doesn't apply here: reading the marker
// filesystem gives the Run history surface the data parity ADR-014 expects
// without the round-trip cost of spinning up a runner container per request.
//
// SfExecutionARN is set to the run ID so the node-runs join key (cloud:
// SFN ARN; local: run uuid) stays consistent with NodeRuns and the
// UI's "drill from a run to its node-runs" pivot keeps working.
func (p *LocalProvider) Runs(ctx context.Context, q RunsQuery) (*RunsResult, error) {
	if !validPipelineName(q.PipelineName) {
		return nil, fmt.Errorf("invalid pipeline name: %q", q.PipelineName)
	}
	store := p.progressStore()

	// The warehouse `_progress` tree is workspace-wide — every pipeline's
	// runs land there, distinguished by the run marker's `pipeline` field.
	// Filter to the queried pipeline so the Runs surface stays per-pipeline.
	wantPipeline := p.pipelineNameForQuery(q.PipelineName, q.PipelineDir)

	runIDs, err := ListProgressRunIDs(p.warehouseDir())
	if err != nil {
		return nil, fmt.Errorf("list local runs: %w", err)
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}

	out := make([]Run, 0, len(runIDs))
	truncated := false
	for _, rid := range runIDs {
		m, ok, _ := readRunMarker(ctx, store, rid)
		if !ok || m == nil {
			// Skip unreadable / absent markers (in-flight, corrupt) rather
			// than failing the whole listing.
			continue
		}
		if wantPipeline != "" && m.Pipeline != wantPipeline {
			continue
		}
		if len(out) >= limit {
			truncated = true
			break
		}
		out = append(out, Run{
			RunID:          rid,
			Pipeline:       m.Pipeline,
			SfExecutionARN: rid,
			Status:         m.Status,
			Trigger:        m.Trigger,
			StartedAt:      millisToISO(m.StartedMs),
			EndedAt:        millisToISO(m.EndedMs),
			DurationMs:     durationMs(m.StartedMs, m.EndedMs),
			FailedStep:     m.FailedStep,
			ErrorClass:     m.ErrorClass,
			ErrorMsg:       m.ErrorMsg,
		})
	}
	return &RunsResult{Rows: out, Truncated: truncated}, nil
}

// pipelineNameForQuery derives the pipeline identifier used to filter the
// shared `_progress` tree's run markers down to one pipeline. Prefers the
// explicit PipelineName; falls back to the basename of the pipeline dir
// (the convention Resolver.PipelineName uses).
func (p *LocalProvider) pipelineNameForQuery(name, dir string) string {
	if name != "" {
		return name
	}
	if dir == "" {
		return ""
	}
	return filepath.Base(pathutil.ResolveDir(p.workspaceRoot, dir))
}

// millisToISO renders epoch milliseconds as ISO-8601 UTC (the wire shape the
// runs/node-runs rows carry), or "" when nil.
func millisToISO(ms *int64) string {
	if ms == nil {
		return ""
	}
	return formatMillis(*ms)
}

// durationMs computes ended-started in milliseconds, or nil when either
// bound is unknown.
func durationMs(startedMs, endedMs *int64) *int64 {
	if startedMs == nil || endedMs == nil {
		return nil
	}
	d := *endedMs - *startedMs
	return &d
}

// tablesFromMetadata walks the warehouse directly: one subdirectory
// per output table, each carrying a `_delta_log/` directory whose JSON
// commit files Delta's transaction-log protocol describes. The reader
// gets the row counts we surface (per-commit operationMetrics from
// commitInfo) plus the snapshot timestamp without a Spark roundtrip.
//
// ADR-018: pre-Delta this read Iceberg's metadata.json + version-hint;
// the file paths changed, the response shape did not. Local-cloud
// parity (ADR-014) means cloud's CloudProvider.Snapshots took the same
// _delta_log path in this sub-slice.
//
// Returns (nil, false) when the warehouse or per-pipeline namespace
// dir is missing; callers fall through to the Spark path (which
// likewise treats missing tables as an empty success). Reads are
// independent per table so one malformed commit doesn't poison the
// whole listing — that table is skipped and logging stays at the
// caller.
func (p *LocalProvider) tablesFromMetadata(q TablesQuery) (*TablesResult, bool) {
	warehouse := workspace.LocalWarehouseDir(p.workspaceRoot)
	if _, err := os.Stat(warehouse); err != nil {
		return nil, false
	}
	// `<catalog>__<schema>` is the Iceberg-on-Hadoop encoding of the
	// pipeline's three-level namespace. Identical formula to the
	// runner's `_glue_db()` so we read what the runner wrote.
	pipelineDir := q.PipelineDir
	if pipelineDir == "" {
		pipelineDir = q.PipelineName
	}
	abs := pathutil.ResolveDir(p.workspaceRoot, pipelineDir)
	catalog := ""
	if m, _ := workspace.Load(p.workspaceRoot); m != nil {
		catalog = m.CatalogIdentifier()
	}
	schema := readSchemaDefault(abs)
	if schema == "" {
		schema = filepath.Base(abs)
	}
	// ADR-019 Slice 4: V2 multi-catalog writes land at
	// ``<warehouse>/<catalog>/<schema>/<table>/`` (no ``.db`` suffix).
	// Legacy Hive layout (pre-Slice-4) put them at
	// ``<warehouse>/<catalog>__<schema>.db/<table>/``. Read both so a
	// workspace mid-migration shows tables from either layout under the
	// same logical DB key.
	namespaceDir := ""
	if catalog != "" {
		v2 := filepath.Join(warehouse, catalog, schema)
		if _, err := os.Stat(v2); err == nil {
			namespaceDir = v2
		}
	}
	if namespaceDir == "" {
		legacy := filepath.Join(warehouse, identutil.EncodeGlueDatabase(catalog, schema)+".db")
		if _, err := os.Stat(legacy); err == nil {
			namespaceDir = legacy
		}
	}
	if namespaceDir == "" {
		// Pipeline namespace not present yet (fresh pipeline, no
		// successful run): empty rows is the correct cloud-parity
		// answer, not a 500.
		return &TablesResult{Rows: []TableInfo{}}, true
	}
	entries, err := os.ReadDir(namespaceDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &TablesResult{Rows: []TableInfo{}}, true
		}
		return nil, false
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	out := make([]TableInfo, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		tableName := e.Name()
		info, ok := readDeltaCurrentSnapshot(filepath.Join(namespaceDir, tableName))
		if !ok {
			continue
		}
		node, outputKey := splitTableName(tableName)
		info.Pipeline = q.PipelineName
		info.Node = node
		info.OutputKey = outputKey
		info.TableName = tableName
		info.TableID = fmt.Sprintf("%s.%s", identutil.EncodeGlueDatabase(catalog, schema), tableName)
		out = append(out, info)
	}
	// Newest snapshot first — matches the Spark path's `ORDER BY
	// snapshot_ts DESC` so the dashboard's "freshest first" sort stays
	// stable regardless of which provider answered.
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].SnapshotTS > out[j].SnapshotTS
	})
	truncated := false
	if len(out) > limit {
		out = out[:limit]
		truncated = true
	}
	return &TablesResult{Rows: out, Truncated: truncated}, true
}

// splitCatalogSchema decodes a “<catalog>__<schema>“ Glue-flat
// database string back into its catalog + schema parts. Returns
// (_, _, false) when the input doesn't carry the “__“ boundary —
// caller falls back to the legacy single-segment path.
func splitCatalogSchema(database string) (catalog, schema string, ok bool) {
	idx := strings.Index(database, "__")
	if idx < 0 {
		return "", "", false
	}
	return database[:idx], database[idx+2:], true
}

// ResolveLocalTablePath returns the on-disk path for a Delta table under
// the workspace's local warehouse. Probes three layouts in order:
//
//  1. ADR-019 V2 (Slice 4): “<warehouse>/<catalog>/<schema>/<table>/“
//  2. Legacy Hive ADR-016: “<warehouse>/<catalog>__<schema>.db/<table>/“
//  3. Legacy + “__default“ suffix (pre-Slice-3 single-output tables)
//
// Returns the V2 layout path when none of the probes find a
// “_delta_log/“ so downstream errors point at the canonical location.
// Exported for the service layer (pipeline reset resolves the same
// layout question when computing what a drop will delete).
func ResolveLocalTablePath(warehouse, database, table string) string {
	if catalog, schema, ok := splitCatalogSchema(database); ok {
		v2 := filepath.Join(warehouse, catalog, schema, table)
		if _, err := os.Stat(filepath.Join(v2, "_delta_log")); err == nil {
			return v2
		}
		legacy := filepath.Join(warehouse, database+".db", table)
		if _, err := os.Stat(filepath.Join(legacy, "_delta_log")); err == nil {
			return legacy
		}
		if !strings.Contains(table, "__") {
			legacyDefault := filepath.Join(warehouse, database+".db", table+"__default")
			if _, err := os.Stat(filepath.Join(legacyDefault, "_delta_log")); err == nil {
				return legacyDefault
			}
			// Slice 4 may have written the bare form into the V2 layout
			// while the same workspace still carries a legacy ``__default``
			// peer; tried above. Fall through to the V2 default below.
		}
		return v2
	}
	primary := filepath.Join(warehouse, database+".db", table)
	if _, err := os.Stat(filepath.Join(primary, "_delta_log")); err == nil {
		return primary
	}
	if !strings.Contains(table, "__") {
		legacy := filepath.Join(warehouse, database+".db", table+"__default")
		if _, err := os.Stat(filepath.Join(legacy, "_delta_log")); err == nil {
			return legacy
		}
	}
	return primary
}

// resolveLocalTableName picks the on-disk table-name variant for SQL.
// Same back-compat rule as ResolveLocalTablePath: prefer the asked name,
// fall back to `<asked>__default` when only the legacy directory exists.
// The asked name is correct under the V2 layout (no “__default“ peer
// on writes after Slice 3), so the legacy probe is the only fallback.
func resolveLocalTableName(workspaceRoot, database, table string) string {
	warehouse := workspace.LocalWarehouseDir(workspaceRoot)
	if catalog, schema, ok := splitCatalogSchema(database); ok {
		if _, err := os.Stat(filepath.Join(warehouse, catalog, schema, table, "_delta_log")); err == nil {
			return table
		}
		if _, err := os.Stat(filepath.Join(warehouse, database+".db", table, "_delta_log")); err == nil {
			return table
		}
		if !strings.Contains(table, "__") {
			if _, err := os.Stat(filepath.Join(warehouse, database+".db", table+"__default", "_delta_log")); err == nil {
				return table + "__default"
			}
		}
		return table
	}
	primary := filepath.Join(warehouse, database+".db", table)
	if _, err := os.Stat(filepath.Join(primary, "_delta_log")); err == nil {
		return table
	}
	if !strings.Contains(table, "__") {
		legacy := filepath.Join(warehouse, database+".db", table+"__default")
		if _, err := os.Stat(filepath.Join(legacy, "_delta_log")); err == nil {
			return table + "__default"
		}
	}
	return table
}

// splitTableName separates `<node>__<output_key>` into its parts. The
// runner writes outputs as `{node}__{key}` (default key is `default`),
// so the rightmost `__` is the splitter. Tables that don't follow the
// pattern are returned with node = full name + outputKey = "".
func splitTableName(name string) (node, outputKey string) {
	i := strings.LastIndex(name, "__")
	if i < 0 {
		return name, ""
	}
	return name[:i], name[i+2:]
}

// readDeltaCurrentSnapshot reads <table>/_delta_log/ and projects the
// latest commit's metadata into a TableInfo. Returns ok=false when the
// directory isn't a Delta table (no `_delta_log/`, empty log, malformed
// commit — happens transiently while a writer is committing).
//
// Maps Delta commitInfo.operationMetrics + userMetadata into the same
// TableInfo fields the v1.x Iceberg path filled from snapshot.summary,
// keeping the dashboard / catalog UI agnostic of which storage format
// the table uses (ADR-018, ADR-014).
func readDeltaCurrentSnapshot(tableDir string) (TableInfo, bool) {
	_, commits, err := delta.ReadCurrentFromPath(tableDir)
	if err != nil {
		// Missing log, malformed commit, or any other read failure —
		// the catalog walker silently skips this table.
		_ = errors.Is(err, delta.ErrNotDelta)
		return TableInfo{}, false
	}
	if len(commits) == 0 {
		return TableInfo{}, false
	}
	latest := commits[0]
	info := TableInfo{
		SnapshotID: strconv.FormatInt(latest.Version, 10),
	}
	if latest.TimestampMs > 0 {
		info.SnapshotTS = time.UnixMilli(latest.TimestampMs).UTC().Format("2006-01-02T15:04:05.000Z")
	}
	if latest.AddedRecords != nil {
		v := *latest.AddedRecords
		info.RowCount = &v
	}
	// File-count / total-bytes come from enumerating the live data-file
	// set in the Delta log (add minus remove at the latest version). The
	// cloud side reads the same numbers out of the workspace `tables`
	// system table the runner stamps; enumerating locally keeps the metric
	// available in local mode too (ADR-014). Best-effort — a file-stats
	// read failure leaves the fields nil so the row still renders its row
	// count, and the UI shows an "unknown" badge cleanly.
	if fs, err := delta.ReadFileStatsFromPath(tableDir); err == nil && fs != nil {
		fc := fs.FileCount
		info.FileCount = &fc
		tb := fs.TotalBytes
		info.TotalBytes = &tb
	}
	if latest.UserMetadata != "" {
		var m map[string]string
		if err := json.Unmarshal([]byte(latest.UserMetadata), &m); err == nil {
			if v, ok := m["clavesa.run-id"]; ok {
				info.LastWriterRunID = v
			}
		}
	}
	return info, true
}

// nodeRunsFromProgress fans out one row per (run, node) by reading the
// warehouse `_progress/<run>/<node>.json` markers — the dashboard grid's
// bulk-fetch fast path. Skips the Spark roundtrip the Delta-backed node_runs
// path needs (1.5s warm, 15-30s cold). Doesn't carry the richer columns the
// runner stamps onto the node_runs row (runner_image_digest, module_version,
// cold_start, memory_mb, lambda_request_id, the Spark metrics) — the Sheet's
// drill-down, which passes an arn filter, falls back through the Spark path
// above to pick those up. It does carry output_rows, which the terminal
// marker stamps.
func (p *LocalProvider) nodeRunsFromProgress(ctx context.Context, q NodeRunsQuery) (*NodeRunsResult, error) {
	store := p.progressStore()
	wantPipeline := p.pipelineNameForQuery(q.PipelineName, q.PipelineDir)

	runIDs, err := ListProgressRunIDs(p.warehouseDir())
	if err != nil {
		return nil, fmt.Errorf("list local runs: %w", err)
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 200
	}

	out := make([]NodeRun, 0, len(runIDs)*4)
	for _, rid := range runIDs {
		// The run marker carries the owning pipeline; filter the workspace-
		// wide tree down to the queried pipeline.
		m, ok, _ := readRunMarker(ctx, store, rid)
		if ok && m != nil && wantPipeline != "" && m.Pipeline != wantPipeline {
			continue
		}

		keys, err := store.ListKeys(ctx, progressPrefix(rid))
		if err != nil {
			continue
		}
		for _, key := range keys {
			prefix := progressPrefix(rid)
			if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, ".json") {
				continue
			}
			nodeID := strings.TrimSuffix(key[len(prefix):], ".json")
			if nodeID == "" || nodeID == "_run" {
				continue
			}
			if q.Node != "" && nodeID != q.Node {
				continue
			}
			snap := readProgressSnapshot(ctx, store, key)
			if snap == nil {
				continue
			}
			// A still-"running" marker (or empty status, pre-status runner)
			// isn't a "this happened" record — the dashboard expects its
			// absence so the in-flight overlay (liveStates) drives the cell
			// color. Only emit terminal markers here.
			status := nodeRunStatusFromProgress(snap.Status)
			if status == "" || status == "running" {
				continue
			}
			pipeline := ""
			if m != nil {
				pipeline = m.Pipeline
			}
			out = append(out, NodeRun{
				RunID:          rid,
				Pipeline:       pipeline,
				Node:           nodeID,
				Status:         status,
				StartedAt:      millisToISO(snap.StartedMs),
				EndedAt:        millisToISO(snap.EndedMs),
				DurationMs:     durationMs(snap.StartedMs, snap.EndedMs),
				ComputeTarget:  "local",
				ErrorMsg:       snap.Error,
				OutputRows:     snap.OutputRows,
				SfExecutionARN: rid,
			})
		}
	}

	truncated := false
	if len(out) > limit {
		out = out[:limit]
		truncated = true
	}
	return &NodeRunsResult{Rows: out, Truncated: truncated}, nil
}

// nodeRunStatusFromProgress maps the lowercase progress-marker status
// (running/succeeded/failed/skipped) to the node_runs convention the
// dashboard's nodeCellState expects: succeeded → "ok" (the runner stamps
// "ok" onto node_runs rows). An empty status maps to "" so the caller can
// skip pre-status / in-flight markers.
func nodeRunStatusFromProgress(status string) string {
	switch status {
	case "succeeded":
		return "ok"
	case "failed":
		return "failed"
	case "running":
		return "running"
	case "skipped":
		return "skipped"
	case "":
		return ""
	default:
		return status
	}
}

// Tables reads current-state-per-table from <pipeline>.tables. The runner
// appends one row per Iceberg-output write; we project the latest row per
// table_id so the UI surfaces "where is each table now?" without each
// viewer scanning the full append history. Returns empty rows (not an
// error) when the table doesn't exist yet — fresh pipelines need their
// first run to materialize it.
func (p *LocalProvider) Tables(ctx context.Context, q TablesQuery) (*TablesResult, error) {
	if !validPipelineName(q.PipelineName) {
		return nil, fmt.Errorf("invalid pipeline name: %q", q.PipelineName)
	}
	// Fast path: read each output table's Delta transaction log
	// directly (ADR-018; swapped from Iceberg metadata.json in this
	// sub-slice). The dashboard's left-rail row counts come from here;
	// the Spark-backed system-catalog read below would take 1-30s and
	// made every node read "no data yet" until Spark warmed. Falls
	// back to Spark when the warehouse / pipeline dir is missing —
	// preserves cloud-like behaviour for fresh workspaces where the
	// `_delta_log/` directory hasn't been written yet.
	if res, ok := p.tablesFromMetadata(q); ok {
		return res, nil
	}
	dbName := q.Database
	// Workspace-wide system DB (ADR-016 v0.20.0): the `tables` table holds
	// rows for every pipeline; filter inside the inner SELECT so the
	// per-table latest-snapshot picker doesn't accidentally pick another
	// pipeline's row for the same node__key (theoretically possible if two
	// pipelines name a transform identically — keep them isolated).
	safePipeline := strings.ReplaceAll(q.PipelineName, "'", "''")
	sql := fmt.Sprintf(
		`SELECT pipeline, node, output_key, table_name, table_id,
  CAST(snapshot_id AS string) AS snapshot_id,
  concat(date_format(snapshot_ts, 'yyyy-MM-dd'), 'T', date_format(snapshot_ts, 'HH:mm:ss.SSSXXX')) AS snapshot_ts,
  row_count, file_count, total_bytes, last_writer_run_id
FROM (
  SELECT *, row_number() OVER (PARTITION BY table_id ORDER BY snapshot_ts DESC) AS _rn
  FROM %s.tables
  WHERE pipeline = '%s'
) WHERE _rn = 1
ORDER BY snapshot_ts DESC
LIMIT %d`, dbName, safePipeline, q.Limit+1)

	res, err := p.runQueryFor(ctx, q.PipelineDir, sql)
	if err != nil {
		if isMissingTableErr(err) {
			return &TablesResult{Rows: []TableInfo{}}, nil
		}
		return nil, err
	}
	idx := columnIndex(res.Columns)

	rows := res.Rows
	truncated := false
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		truncated = true
	}

	out := make([]TableInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, TableInfo{
			Pipeline:        stringValue(rowAt(r, idx, "pipeline")),
			Node:            stringValue(rowAt(r, idx, "node")),
			OutputKey:       stringValue(rowAt(r, idx, "output_key")),
			TableName:       stringValue(rowAt(r, idx, "table_name")),
			TableID:         stringValue(rowAt(r, idx, "table_id")),
			SnapshotID:      stringValue(rowAt(r, idx, "snapshot_id")),
			SnapshotTS:      stringValue(rowAt(r, idx, "snapshot_ts")),
			RowCount:        int64Pointer(rowAt(r, idx, "row_count")),
			FileCount:       int64Pointer(rowAt(r, idx, "file_count")),
			TotalBytes:      int64Pointer(rowAt(r, idx, "total_bytes")),
			LastWriterRunID: stringValue(rowAt(r, idx, "last_writer_run_id")),
		})
	}
	return &TablesResult{Rows: out, Truncated: truncated}, nil
}

// Snapshots reads the Delta `_delta_log/` commit history for any table in
// the workspace-shared local warehouse (ADR-018). The warehouse is one per
// workspace (ADR-016); PipelineDir is no longer needed to locate it but is
// still accepted for caller symmetry with the cloud provider.
func (p *LocalProvider) Snapshots(ctx context.Context, q SnapshotsQuery) (*SnapshotsResult, error) {
	if !validIdentifier(q.Database) {
		return nil, fmt.Errorf("invalid database name: %q", q.Database)
	}
	if !validIdentifier(q.Table) {
		return nil, fmt.Errorf("invalid table name: %q", q.Table)
	}

	// ADR-018: read Delta's `_delta_log/` directly. Same posture as
	// CloudProvider.Snapshots — the v1.x Iceberg `<table>$snapshots`
	// SQL is gone, and `DESCRIBE HISTORY` would force a Spark spawn
	// per snapshot fetch. The local-mode Hive layout puts the table at
	// `<warehouse>/<db>.db/<table>/_delta_log/`.
	warehouse := workspace.LocalWarehouseDir(p.workspaceRoot)
	tablePath := ResolveLocalTablePath(warehouse, q.Database, q.Table)
	st, err := delta.ReadTableStateFromPath(tablePath)
	if err != nil {
		if errors.Is(err, delta.ErrNotDelta) {
			return &SnapshotsResult{Snapshots: []SnapshotInfo{}}, nil
		}
		return nil, fmt.Errorf("read delta log: %w", err)
	}

	// Projection (incl. the LatestRecordCount derivation) is shared with
	// CloudProvider.Snapshots so local and cloud agree (ADR-014).
	return snapshotsResultFromCommits(st.Commits, q.Limit, st.RowCount), nil
}

// ColumnStats reads the latest-snapshot row per column from the workspace
// system column_stats Iceberg table. Identical SQL shape to CloudProvider
// modulo function names (Spark `to_json` vs Athena `CAST AS json`,
// `date_format` vs `to_iso8601`).
func (p *LocalProvider) ColumnStats(ctx context.Context, q ColumnStatsQuery) (*ColumnStatsResult, error) {
	if q.Database == "" {
		return &ColumnStatsResult{Stats: []ColumnStat{}}, nil
	}
	if !validIdentifier(q.Database) {
		return nil, fmt.Errorf("invalid system database name: %q", q.Database)
	}
	if q.TableIdentifier == "" {
		return nil, fmt.Errorf("table_identifier is required")
	}
	pipelineRef := q.PipelineDir
	if pipelineRef == "" {
		// Same workspace warehouse regardless of pipeline; pass the
		// system DB name as the ref so the empty-guard in runQueryFor
		// passes. The warehouse resolves to the same path.
		pipelineRef = q.Database
	}

	safeIdent := strings.ReplaceAll(q.TableIdentifier, "'", "''")
	sql := fmt.Sprintf(
		`SELECT column_name, column_type,
  row_count, null_count, null_pct,
  approx_count_distinct,
  min_value, max_value,
  approx_p50, approx_p95,
  to_json(top_10) AS top_10_json,
  CAST(snapshot_id AS string) AS snapshot_id,
  concat(date_format(computed_at, 'yyyy-MM-dd'), 'T', date_format(computed_at, 'HH:mm:ss.SSSXXX')) AS computed_at
FROM (
  SELECT *,
    row_number() OVER (PARTITION BY column_name ORDER BY computed_at DESC) AS _rn
  FROM %s.column_stats
  WHERE table_identifier = '%s'
) WHERE _rn = 1
ORDER BY column_name`, q.Database, safeIdent)

	res, err := p.runQueryFor(ctx, pipelineRef, sql)
	if err != nil {
		if isMissingTableErr(err) {
			return &ColumnStatsResult{Stats: []ColumnStat{}}, nil
		}
		return nil, err
	}
	idx := columnIndex(res.Columns)

	out := make([]ColumnStat, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, ColumnStat{
			ColumnName:          stringValue(rowAt(r, idx, "column_name")),
			ColumnType:          stringValue(rowAt(r, idx, "column_type")),
			RowCount:            int64Pointer(rowAt(r, idx, "row_count")),
			NullCount:           int64Pointer(rowAt(r, idx, "null_count")),
			NullPct:             float64Pointer(rowAt(r, idx, "null_pct")),
			ApproxCountDistinct: int64Pointer(rowAt(r, idx, "approx_count_distinct")),
			MinValue:            stringValue(rowAt(r, idx, "min_value")),
			MaxValue:            stringValue(rowAt(r, idx, "max_value")),
			ApproxP50:           float64Pointer(rowAt(r, idx, "approx_p50")),
			ApproxP95:           float64Pointer(rowAt(r, idx, "approx_p95")),
			Top10:               parseTop10JSON(stringValue(rowAt(r, idx, "top_10_json"))),
			SnapshotID:          stringValue(rowAt(r, idx, "snapshot_id")),
			ComputedAt:          stringValue(rowAt(r, idx, "computed_at")),
		})
	}
	result := &ColumnStatsResult{Stats: out}
	if len(out) > 0 {
		result.SnapshotID = out[0].SnapshotID
	}
	return result, nil
}

// SampleTable runs `SELECT * FROM <db>.<table> LIMIT N+1` against the
// pipeline's local Hadoop catalog via the runner-Spark container.
// Stringifies row values for transport — same shape the cloud Athena
// path emits (ADR-014 parity).
func (p *LocalProvider) SampleTable(ctx context.Context, q SampleTableQuery) (*SampleTableResult, error) {
	if !validIdentifier(q.Database) {
		return nil, fmt.Errorf("invalid database name: %q", q.Database)
	}
	if !validIdentifier(q.Table) {
		return nil, fmt.Errorf("invalid table name: %q", q.Table)
	}
	pipelineRef := q.PipelineDir
	if pipelineRef == "" {
		pipelineRef = strings.TrimPrefix(q.Database, "clavesa_")
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}

	// Legacy tables on disk still carry the `__default` suffix (pre-ADR-019).
	// If the request asks for the bare form and only the legacy variant is
	// materialised in this workspace, redirect the SQL to the legacy name.
	tableForSQL := resolveLocalTableName(p.workspaceRoot, q.Database, q.Table)
	sql := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d", q.Database, tableForSQL, limit+1)
	res, err := p.runQueryFor(ctx, pipelineRef, sql)
	if err != nil {
		// Fresh table that hasn't been written yet — surface an empty
		// result so the UI shows the "Table is empty" state instead of an
		// error box. Same treatment Snapshots / NodeRuns already give.
		if isMissingTableErr(err) {
			return &SampleTableResult{Columns: []SampleTableColumn{}, Rows: [][]string{}, Served: servedSparkLocal()}, nil
		}
		return nil, err
	}

	cols := buildSampleColumns(res)

	rows := res.Rows
	truncated := false
	if len(rows) > limit {
		rows = rows[:limit]
		truncated = true
	}
	stringRows := make([][]string, len(rows))
	for i, row := range rows {
		r := make([]string, len(row))
		for j, v := range row {
			r[j] = sampleCellString(v)
		}
		stringRows[i] = r
	}
	return &SampleTableResult{
		Columns:   cols,
		Rows:      stringRows,
		RowCount:  len(stringRows),
		Truncated: truncated,
		Served:    servedSparkLocal(),
	}, nil
}

// buildSampleColumns pairs CLAVESA_QUERY=1 column names with the runner's
// per-column Spark type (DataType.simpleString()). When the runner is older
// than the column-types-in-query-mode change ColumnTypes is empty and Type
// degrades to "" — same as what the cloud Athena response surfaces when the
// metadata layer can't resolve a type.
func buildSampleColumns(res *QueryRunnerResult) []SampleTableColumn {
	cols := make([]SampleTableColumn, len(res.Columns))
	for i, name := range res.Columns {
		var t string
		if i < len(res.ColumnTypes) {
			t = res.ColumnTypes[i]
		}
		cols[i] = SampleTableColumn{Name: name, Type: t}
	}
	return cols
}

// sampleCellString stringifies one value out of the runner's JSON result.
// nil → "" (Athena emits empty for nulls; mirror that). Numbers and bools
// use Go's default formatting, which matches what fmt.Sprintf("%v", x)
// would produce — boring on purpose.
func sampleCellString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		// JSON numbers come back as float64; format integer-valued ones
		// without the trailing ".0" so id columns read naturally.
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		// 'f' (not %g) so a revenue figure reads as 79456384.28, not
		// 7.945638428e+07 — sample rows are for eyeballing, not for
		// compact transport.
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Query runs the caller-supplied SQL through the runner-Spark container
// against the pipeline's local Hadoop catalog. No identifier validation —
// dashboard widgets author free-form SQL.
func (p *LocalProvider) Query(ctx context.Context, q QueryQuery) (*QueryResult, error) {
	if q.SQL == "" {
		return nil, fmt.Errorf("query: sql is required")
	}
	if q.PipelineDir == "" {
		return nil, fmt.Errorf("local: pipeline_dir is required")
	}

	res, err := p.runQueryFor(ctx, q.PipelineDir, q.SQL)
	if err != nil {
		// Empty rather than error for missing tables — matches the
		// SampleTable / NodeRuns convention so a dashboard widget over
		// a fresh table renders as an empty chart instead of a stack
		// trace. StrictMissing opts out (interactive query / render
		// smoke test), where a missing table must surface as an error.
		if !q.StrictMissing && isMissingTableErr(err) {
			return &QueryResult{Columns: []SampleTableColumn{}, Rows: [][]string{}, Served: servedSparkLocal()}, nil
		}
		return nil, err
	}

	cols := buildSampleColumns(res)
	rows := res.Rows
	truncated := false
	if q.MaxRows > 0 && len(rows) > q.MaxRows {
		rows = rows[:q.MaxRows]
		truncated = true
	}
	stringRows := make([][]string, len(rows))
	for i, row := range rows {
		r := make([]string, len(row))
		for j, v := range row {
			r[j] = sampleCellString(v)
		}
		stringRows[i] = r
	}
	return &QueryResult{
		Columns:   cols,
		Rows:      stringRows,
		RowCount:  len(stringRows),
		Truncated: truncated,
		Served:    servedSparkLocal(),
	}, nil
}

// Exec runs a write (CREATE TABLE / MERGE / DELETE) against the local
// warehouse via the runner. A DML/DDL statement run through the runner's
// SQL path returns no rows — query mode doubles as the exec path, so no
// separate runner mode is needed. The result set is discarded; only the
// error is surfaced.
func (p *LocalProvider) Exec(ctx context.Context, q ExecQuery) error {
	if q.SQL == "" {
		return fmt.Errorf("exec: sql is required")
	}
	if q.PipelineDir == "" {
		return fmt.Errorf("local: pipeline_dir is required")
	}
	_, err := p.runQueryFor(ctx, q.PipelineDir, q.SQL)
	return err
}

// validIdentifier mirrors observability.IsValidIdentifier (defined in cloud.go)
// without forcing local.go to import yet another constant — same regex.
func validIdentifier(s string) bool { return identifierRE.MatchString(s) }

// validPipelineName mirrors IsValidPipelineName — pipeline names may carry
// hyphens (what `pipeline create` accepts), unlike Glue identifiers.
func validPipelineName(s string) bool { return pipelineNameRE.MatchString(s) }

// isMissingTableErr classifies a runner error as "table does not exist" so
// the UI can render a fresh-pipeline empty state. Spark surfaces these via
// AnalysisException with a "Table or view not found" / "Path does not
// exist" / "TABLE_OR_VIEW_NOT_FOUND" substring.
func isMissingTableErr(err error) bool {
	s := err.Error()
	for _, marker := range []string{
		"Table or view not found",
		"TABLE_OR_VIEW_NOT_FOUND",
		"Path does not exist",
		"NoSuchTableException",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

// The exec-ref encode/decode pair (FormatExecRef / SplitExecRef) lives in
// execref.go — one pair shared by every execution endpoint (GH #78).
