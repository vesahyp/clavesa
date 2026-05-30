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
	"github.com/vesahyp/clavesa/internal/errs"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/pathutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// LocalProvider satisfies Provider for compute = "local" pipelines.
//
// Run history (NodeRuns/Runs/Snapshots/Table) and execution state share the
// same response shapes as CloudProvider so the UI cannot tell which backend
// served a request — ADR-014.
//
// Execution states + logs come straight from the filesystem progress channel
// at <pipelineDir>/.clavesa/runs/<runID>/. The Iceberg-backed surfaces
// (NodeRuns, Runs, Snapshots) reuse the runner container in CLAVESA_QUERY=1
// mode to read against the local Hadoop catalog — same SQL surface the cloud
// provider uses through Athena.
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

// ExecutionStates reads the filesystem progress channel for one run.
//
// ExecutionRef is a local run-id (hex). When empty, returns the most recent
// run for the inspected pipeline — matches how CloudProvider treats the
// "latest in-flight execution" case via the SFN ListExecutions path.
func (p *LocalProvider) ExecutionStates(ctx context.Context, q ExecutionStatesQuery) (*ExecutionStatesResult, error) {
	dir, err := p.pipelineDirForQuery(q.ExecutionRef)
	if err != nil {
		return nil, err
	}
	runID, err := p.resolveRunID(dir, q.ExecutionRef)
	if err != nil {
		// Fresh pipeline that hasn't been run yet — the run directory
		// doesn't exist. Returning empty matches the cloud provider's
		// "no executions yet" path, so the dashboard renders a clean
		// empty state instead of flashing a 500.
		if errors.Is(err, os.ErrNotExist) {
			return &ExecutionStatesResult{States: map[string]StateStatus{}}, nil
		}
		return nil, err
	}
	st, err := ReadRunState(dir, runID)
	if err != nil {
		if os.IsNotExist(err) {
			return &ExecutionStatesResult{States: map[string]StateStatus{}}, nil
		}
		return nil, err
	}

	out := &ExecutionStatesResult{
		Status:    st.Status,
		States:    make(map[string]StateStatus, len(st.States)),
		RunID:     st.RunID,
		StartedAt: st.StartedAt,
	}
	for nodeID, s := range st.States {
		out.States[nodeID] = StateStatus{
			Status:          s.Status,
			EnteredAt:       s.EnteredAt,
			StagesTotal:     s.StagesTotal,
			StagesCompleted: s.StagesCompleted,
			TasksTotal:      s.TasksTotal,
			TasksCompleted:  s.TasksCompleted,
			TasksFailed:     s.TasksFailed,
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// ExecutionLogs
// ---------------------------------------------------------------------------

// logsLineCap caps how many lines one /pipeline/execution/logs response
// returns, mirroring CloudProvider's logsLimit. Captured runner stdout for
// one node rarely exceeds a few hundred lines; the cap protects against a
// runaway log file (e.g. a Spark plan dump).
const logsLineCap = 500

// ExecutionLogs reads the captured stdout/stderr file for one node within
// one run. Format on disk is `<RFC3339Nano>\t<message>` per line — written
// by NewTimestampedLogWriter at the moment each line was emitted, so the
// per-event Timestamp matches the cloud CloudWatch payload (ADR-014
// parity). Lines without the timestamp prefix (older log files written
// before the writer wrap landed) fall through with Timestamp:"" and the
// raw line as Message — backward-compatible.
func (p *LocalProvider) ExecutionLogs(ctx context.Context, q ExecutionLogsQuery) (*ExecutionLogsResult, error) {
	if q.Step == "" {
		return nil, errors.New("step is required")
	}
	dir, err := p.pipelineDirForQuery(q.ExecutionRef)
	if err != nil {
		return nil, err
	}
	runID, err := p.resolveRunID(dir, q.ExecutionRef)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &ExecutionLogsResult{
				Source:       LogSourceLocal,
				FunctionName: q.Step,
				Events:       []LogEvent{},
			}, nil
		}
		return nil, err
	}

	logPath := RunLogPath(dir, runID, q.Step)
	f, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No log file yet — step hasn't been entered, or the run never
			// reached it. Empty events, not an error (matches CloudProvider).
			return &ExecutionLogsResult{
				Source:       LogSourceLocal,
				LogGroup:     logPath,
				FunctionName: q.Step,
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
		FunctionName: q.Step,
		Events:       events,
		Truncated:    truncated,
	}, nil
}

// ---------------------------------------------------------------------------
// Iceberg-backed surfaces (delegate to Spark via the runner container).
//
// Phase 2 ships the filesystem progress channel above (states + logs); the
// methods below return errors with a stable sentinel so callers can detect
// "not yet implemented in this slice" without parsing strings. The next slice
// wires CLAVESA_QUERY=1 to read node_runs, runs, snapshots, and table
// rows against the local Hadoop catalog.
// ---------------------------------------------------------------------------

// ErrLocalNotImplemented is re-exported from internal/errs so callers in
// pipelinestatus + dataquery answer the same sentinel (C10, 2026-05-24).
var ErrLocalNotImplemented = errs.ErrLocalNotImplemented

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
	// `arn` filter and just needs per-cell status + duration. state.json
	// already has both — sourcing them direct from the filesystem avoids
	// the 1.5s-warm / 30s-cold Spark roundtrip that was making the grid
	// look like it lost its data on every refresh. The Sheet's drill-
	// down (arn-filtered) still goes through Spark below to pick up the
	// richer columns (image digest, module version, output rows) that
	// state.json doesn't carry.
	if q.SfExecutionARN == "" && !q.IncludeMetrics {
		return p.nodeRunsFromState(q)
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

// Runs reads the per-pipeline-execution rollup directly from the local
// progress channel — one entry per RunDir, projected from state.json. The
// SQL path the cloud provider uses (Athena over an Iceberg <pipeline>.runs
// table) doesn't apply here: the local orchestrator never writes that
// table, so a Spark-via-runner query would always come back empty even on
// pipelines with dozens of completed runs. Reading the channel filesystem
// gives the Run history surface the data parity ADR-014 expects without
// the round-trip cost of spinning up a runner container per request.
//
// SfExecutionARN is set to the run ID so the node-runs join key (cloud:
// SFN ARN; local: run uuid) stays consistent with NodeRuns and the
// UI's "drill from a run to its node-runs" pivot keeps working.
func (p *LocalProvider) Runs(ctx context.Context, q RunsQuery) (*RunsResult, error) {
	if !validPipelineName(q.PipelineName) {
		return nil, fmt.Errorf("invalid pipeline name: %q", q.PipelineName)
	}
	pipelineRef := q.PipelineDir
	if pipelineRef == "" {
		pipelineRef = q.PipelineName
	}
	dir := pipelineRef
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(p.workspaceRoot, dir)
	}

	runIDs, err := ListRunIDs(dir)
	if err != nil {
		return nil, fmt.Errorf("list local runs: %w", err)
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	truncated := len(runIDs) > limit
	if truncated {
		runIDs = runIDs[:limit]
	}

	out := make([]Run, 0, len(runIDs))
	for _, rid := range runIDs {
		st, err := ReadRunState(dir, rid)
		if err != nil {
			// Skip unreadable runs (in-flight, corrupt) rather than failing
			// the whole listing.
			continue
		}
		out = append(out, Run{
			RunID:          st.RunID,
			Pipeline:       st.Pipeline,
			SfExecutionARN: st.RunID,
			Status:         st.Status,
			Trigger:        st.Trigger,
			StartedAt:      st.StartedAt,
			EndedAt:        st.EndedAt,
			DurationMs:     st.DurationMs,
			FailedStep:     st.FailedStep,
			ErrorClass:     st.ErrorClass,
			ErrorMsg:       st.ErrorMsg,
		})
	}
	return &RunsResult{Rows: out, Truncated: truncated}, nil
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

// resolveLocalTablePath returns the on-disk path for a Delta table under
// the workspace's local warehouse. Probes three layouts in order:
//
//  1. ADR-019 V2 (Slice 4): “<warehouse>/<catalog>/<schema>/<table>/“
//  2. Legacy Hive ADR-016: “<warehouse>/<catalog>__<schema>.db/<table>/“
//  3. Legacy + “__default“ suffix (pre-Slice-3 single-output tables)
//
// Returns the V2 layout path when none of the probes find a
// “_delta_log/“ so downstream errors point at the canonical location.
func resolveLocalTablePath(warehouse, database, table string) string {
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
// Same back-compat rule as resolveLocalTablePath: prefer the asked name,
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
	// Delta's per-commit metrics don't expose total-data-files or
	// total-files-size the way Iceberg's snapshot.summary did. The
	// runner stamps these into the workspace `tables` system table
	// via spark.catalog.listTables for cloud reads; the local
	// fast-path leaves them nil rather than guessing — the UI
	// renders an "unknown" badge cleanly.
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

// nodeStatusFromState normalises the channel's UPPERCASE state enum
// (PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED) to the lowercase strings
// the runner stamps onto node_runs Iceberg rows and the dashboard cell
// renderer expects. SUCCEEDED → "ok" is the only non-obvious mapping;
// it predates this codepath (the runner emits "ok" because Iceberg
// rows are written from runner.py).
func nodeStatusFromState(state string) string {
	switch state {
	case "SUCCEEDED":
		return "ok"
	case "FAILED":
		return "failed"
	case "RUNNING":
		return "running"
	case "SKIPPED":
		return "skipped"
	default:
		return strings.ToLower(state)
	}
}

// nodeRunsFromState fans out one row per (run, node) by reading every
// state.json on disk — the dashboard grid's bulk-fetch fast path. Skips
// the Spark roundtrip the Iceberg-backed node_runs path needs (1.5s
// warm, 15-30s cold). Doesn't carry the richer columns the runner
// stamps onto the Iceberg row (runner_image_digest, module_version,
// output_rows, cold_start, memory_mb, lambda_request_id) — the Sheet's
// drill-down, which passes an arn filter, falls back through the Spark
// path above to pick those up.
func (p *LocalProvider) nodeRunsFromState(q NodeRunsQuery) (*NodeRunsResult, error) {
	pipelineRef := q.PipelineDir
	if pipelineRef == "" {
		pipelineRef = q.PipelineName
	}
	dir := pipelineRef
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(p.workspaceRoot, dir)
	}

	runIDs, err := ListRunIDs(dir)
	if err != nil {
		return nil, fmt.Errorf("list local runs: %w", err)
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 200
	}

	out := make([]NodeRun, 0, len(runIDs)*4)
	for _, rid := range runIDs {
		st, err := ReadRunState(dir, rid)
		if err != nil {
			continue
		}
		for nodeID, ns := range st.States {
			if q.Node != "" && nodeID != q.Node {
				continue
			}
			// PENDING entries are seeded by the orchestrator before any
			// node enters — they're not "this happened" records and the
			// dashboard expects their absence so its in-flight overlay
			// (liveStates) drives the cell color. Emitting them here
			// would paint queued cells as if they had finished.
			if ns.Status == "" || ns.Status == "PENDING" {
				continue
			}
			started := ns.EnteredAt
			if started == "" {
				started = st.StartedAt
			}
			out = append(out, NodeRun{
				RunID:    st.RunID,
				Pipeline: st.Pipeline,
				Node:     nodeID,
				// Map state.json's uppercase enum to the runner's
				// lowercase convention used by node_runs Iceberg rows
				// and the dashboard's nodeCellState. Plain lowercasing
				// would emit "succeeded", which doesn't match the
				// grid's `=== "ok"` check and would silently render
				// every success as a skip.
				Status:         nodeStatusFromState(ns.Status),
				StartedAt:      started,
				EndedAt:        ns.ExitedAt,
				DurationMs:     ns.DurationMs,
				ComputeTarget:  "local",
				ErrorClass:     ns.ErrorClass,
				ErrorMsg:       ns.ErrorMsg,
				OutputRows:     ns.OutputRows,
				SfExecutionARN: st.RunID,
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

// Snapshots reads <table>.snapshots for any Iceberg table in the
// workspace-shared local warehouse. The warehouse is one per workspace
// (ADR-016); PipelineDir is no longer needed to locate it but is still
// accepted for caller symmetry with the cloud provider.
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
	tablePath := resolveLocalTablePath(warehouse, q.Database, q.Table)
	schema, commits, err := delta.ReadCurrentFromPath(tablePath)
	_ = schema // unused — caller is asking for snapshots only
	if err != nil {
		if errors.Is(err, delta.ErrNotDelta) {
			return &SnapshotsResult{Snapshots: []SnapshotInfo{}}, nil
		}
		return nil, fmt.Errorf("read delta log: %w", err)
	}

	// Compute LatestRecordCount by walking commits oldest-first. Three
	// shapes matter for table-state row count:
	//   - Replaces=true commit (CTAS, CREATE OR REPLACE, WRITE Overwrite):
	//     reset to this commit's AddedRecords.
	//   - MERGE: net delta is (added - updated - deleted). The log_reader
	//     folds inserts+updates into AddedRecords for the timeline display;
	//     subtract UpdatedRecords here because updates don't change row
	//     count.
	//   - Otherwise (APPEND, DELETE): net delta is (added - deleted).
	// commits is newest-first from delta.ReadCurrentFromPath; walk in
	// reverse to apply oldest-first.
	var latestCount *int64
	if len(commits) > 0 {
		var total int64
		for i := len(commits) - 1; i >= 0; i-- {
			ci := commits[i]
			added := int64(0)
			if ci.AddedRecords != nil {
				added = *ci.AddedRecords
			}
			updated := int64(0)
			if ci.UpdatedRecords != nil {
				updated = *ci.UpdatedRecords
			}
			deleted := int64(0)
			if ci.DeletedRecords != nil {
				deleted = *ci.DeletedRecords
			}
			netDelta := added - updated - deleted
			if ci.Replaces {
				total = netDelta
			} else {
				total += netDelta
			}
		}
		if total < 0 {
			total = 0
		}
		latestCount = &total
	}

	limit := q.Limit
	if limit <= 0 {
		limit = len(commits)
	}
	truncated := false
	if len(commits) > limit {
		commits = commits[:limit]
		truncated = true
	}

	out := make([]SnapshotInfo, 0, len(commits))
	for _, ci := range commits {
		trigger, runID := extractProvenance(ci.UserMetadata)
		info := SnapshotInfo{
			SnapshotID:     strconv.FormatInt(ci.Version, 10),
			CommittedAt:    formatMillis(ci.TimestampMs),
			Operation:      ci.Operation,
			AddedRecords:   ci.AddedRecords,
			DeletedRecords: ci.DeletedRecords,
			TotalRecords:   ci.TotalRecords,
			Trigger:        trigger,
			WriterRunID:    runID,
		}
		if ci.Version > 0 {
			info.ParentID = strconv.FormatInt(ci.Version-1, 10)
		}
		out = append(out, info)
	}
	result := &SnapshotsResult{Snapshots: out, Truncated: truncated}
	if len(out) > 0 && out[0].TotalRecords != nil {
		v := *out[0].TotalRecords
		result.LatestRecordCount = &v
	} else if latestCount != nil {
		result.LatestRecordCount = latestCount
	}
	return result, nil
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
			return &SampleTableResult{Columns: []SampleTableColumn{}, Rows: [][]string{}}, nil
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
		// trace.
		if isMissingTableErr(err) {
			return &QueryResult{Columns: []SampleTableColumn{}, Rows: [][]string{}}, nil
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// pipelineDirForQuery extracts the pipeline directory from an execution
// reference. Format: "<pipelineDir>:<runID>" or just "<pipelineDir>" (latest
// run is implied). Falls back to a workspace-rooted relative path when the
// caller passes only a directory hint.
func (p *LocalProvider) pipelineDirForQuery(execRef string) (string, error) {
	dir, _ := splitExecRef(execRef)
	if dir == "" {
		return "", errors.New("local provider: execution_ref must be \"<dir>\" or \"<dir>:<runID>\"")
	}
	return pathutil.ResolveDir(p.workspaceRoot, dir), nil
}

// resolveRunID returns the runID portion of execRef when provided, else the
// most recent run found on disk. Empty result is treated as "no runs yet"
// upstream rather than an error.
func (p *LocalProvider) resolveRunID(pipelineDir, execRef string) (string, error) {
	_, rid := splitExecRef(execRef)
	if rid != "" {
		return rid, nil
	}
	ids, err := ListRunIDs(pipelineDir)
	if err != nil {
		return "", err
	}
	if len(ids) == 0 {
		return "", os.ErrNotExist
	}
	return ids[0], nil
}

// splitExecRef splits "dir:runID" into ("dir", "runID"). Strings without ':'
// are treated as a bare directory (no runID supplied). Round-trip-safe with
// FormatExecRef below.
func splitExecRef(s string) (dir, runID string) {
	if s == "" {
		return "", ""
	}
	// Use the LAST colon so absolute paths with drive letters or Windows
	// `C:\…\dir` shapes still split correctly.
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return s, ""
	}
	return s[:idx], s[idx+1:]
}

// FormatExecRef encodes (dir, runID) back into the on-the-wire format. The
// HTTP handler uses this when constructing dispatch references for local
// providers; cloud uses raw SFN ARNs for the same field.
//
// Cloud runs are addressed by their full SFN execution ARN (the value
// `pipeline run` prints and the UI carries in the `run=` query param). Such
// a runID is already self-contained: prefixing it with `dir:` would shift
// the colon-split in StateMachineNameFromExecutionARN, so the ARN no longer
// parses and the cloud provider returns an empty result. Pass a full
// execution ARN through unchanged. Local run IDs are never ARN-shaped, so
// this branch never affects the local round-trip with splitExecRef.
func FormatExecRef(dir, runID string) string {
	if runID == "" {
		return dir
	}
	if StateMachineNameFromExecutionARN(runID) != "" {
		return runID
	}
	return dir + ":" + runID
}
