// Package observability provides a per-pipeline Provider abstraction over the
// run-history, snapshot, and execution-state surfaces the UI consumes.
//
// ADR-014 makes local–cloud parity binding: every observability feature must
// work for compute = "local" pipelines as well as deployed ones. The Provider
// interface here is the seam — cloudProvider talks to Athena/SFN/CloudWatch,
// localProvider reads filesystem-backed Iceberg metadata + a per-run progress
// channel. Backends differ; response shapes do not, so the UI layer stays
// agnostic.
//
// Resolver picks per request based on the inspected pipeline's `compute` attr,
// not host AWS availability.
package observability

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// NodeRunsQuery selects rows from <database>.node_runs.
type NodeRunsQuery struct {
	// PipelineName is the SQL-identifier form of the pipeline name (dashes
	// replaced with underscores by the runner's pipeline_safe rule).
	// Historically also acted as the Glue DB name via `clavesa_<name>`;
	// post-ADR-016 the DB name is supplied separately via Database (see
	// below) — PipelineName remains for the `pipeline = '...'` row filter
	// and for legacy callers that don't yet plumb Database through.
	PipelineName string
	// Database is the encoded Glue DB / Iceberg namespace this query
	// reads from (ADR-016 `<catalog>__<schema>`). Required as of
	// v0.18.0 — callers must supply the resolved DB name (e.g. via
	// `Resolver.GlueDBFor`); empty queries return an empty result set
	// since no DB matches. The pre-v0.18 legacy fallback to
	// `clavesa_<PipelineName>` was removed once the only production
	// user (cloudfront-analytics) migrated to the encoded form.
	Database string
	// PipelineDir is the workspace-relative path to the pipeline directory,
	// used by the local provider to locate <dir>/.clavesa/warehouse/. May
	// differ from PipelineName by `-` vs `_` (e.g. dir `cloudfront-analytics`
	// → name `cloudfront_analytics`). Empty for cloud-only callers.
	PipelineDir string
	// Node optionally filters to one node within the pipeline.
	Node string
	// SfExecutionARN optionally filters to one execution. The column name is
	// historical (matches cloud's SFN execution ARN); locally it's the
	// pipeline-run uuid threaded into the runner via _sf_execution_arn.
	// Either way it's the join key against `runs.sf_execution_arn`.
	// Validated at the handler layer before literal-substitution into SQL.
	SfExecutionARN string
	// Limit caps how many rows are returned (cloudProvider bumps by 1
	// internally to detect truncation).
	Limit int
	// IncludeMetrics forces the metrics-bearing SQL scan even with no arn
	// filter; the dashboard grid's state.json fast path omits the
	// Spark-metric columns.
	IncludeMetrics bool
}

// NodeRun is one parsed row from <pipeline>.node_runs. Nullable fields use
// pointers so JSON omits them when the runner didn't report a value.
type NodeRun struct {
	RunID    string `json:"run_id"`
	Pipeline string `json:"pipeline"`
	// SfExecutionARN is the join key against `runs.sf_execution_arn` — the
	// pipeline-execution this node invocation belongs to (a per-invocation
	// id like `run_id` would not group nodes into a run). Empty for runner
	// rows that predate the column.
	SfExecutionARN    string `json:"sf_execution_arn,omitempty"`
	Node              string `json:"node"`
	StartedAt         string `json:"started_at"`
	EndedAt           string `json:"ended_at,omitempty"`
	DurationMs        *int64 `json:"duration_ms,omitempty"`
	Status            string `json:"status"`
	ComputeTarget     string `json:"compute_target,omitempty"`
	MemoryMB          *int64 `json:"memory_mb,omitempty"`
	ColdStart         *bool  `json:"cold_start,omitempty"`
	LambdaRequestID   string `json:"lambda_request_id,omitempty"`
	ErrorClass        string `json:"error_class,omitempty"`
	ErrorMsg          string `json:"error_msg,omitempty"`
	RunnerImageDigest string `json:"runner_image_digest,omitempty"`
	ModuleVersion     string `json:"module_version,omitempty"`
	// OutputRows is the sum of added-records across this run's Iceberg
	// outputs at write time. Nullable: path-mode-only runs and skipped
	// runs leave it null, and rows from older runners that didn't write
	// the column read as null too.
	OutputRows *int64 `json:"output_rows,omitempty"`
	// Spark-observability metrics (v0.14.x). peak_rss_mb is a process-lifetime
	// high-water mark; the rest are per-invocation aggregates over the run's
	// Spark stages/tasks. All nullable: older runners and skipped/path-mode
	// runs read as null.
	PeakRSSMB             *int64 `json:"peak_rss_mb,omitempty"`
	PeakExecutionMemoryMB *int64 `json:"peak_execution_memory_mb,omitempty"`
	MemorySpilledBytes   *int64 `json:"memory_spilled_bytes,omitempty"`
	DiskSpilledBytes     *int64 `json:"disk_spilled_bytes,omitempty"`
	ShuffleReadBytes     *int64 `json:"shuffle_read_bytes,omitempty"`
	ShuffleWriteBytes    *int64 `json:"shuffle_write_bytes,omitempty"`
	InputBytes           *int64 `json:"input_bytes,omitempty"`
	InputRecords         *int64 `json:"input_records,omitempty"`
	NumStages            *int64 `json:"num_stages,omitempty"`
	NumTasks             *int64 `json:"num_tasks,omitempty"`
	NumFailedTasks       *int64 `json:"num_failed_tasks,omitempty"`
	JVMGCTimeMs          *int64 `json:"jvm_gc_time_ms,omitempty"`
	ExecutorCPUTimeMs    *int64 `json:"executor_cpu_time_ms,omitempty"`
	ExecutorRunTimeMs    *int64 `json:"executor_run_time_ms,omitempty"`
	MaxTaskDurationMs    *int64 `json:"max_task_duration_ms,omitempty"`
}

// NodeRunsResult is the body of GET /data/node-runs.
type NodeRunsResult struct {
	Rows      []NodeRun `json:"rows"`
	Truncated bool      `json:"truncated"`
}

// RunsQuery selects rows from <database>.runs.
type RunsQuery struct {
	PipelineName string
	// Database — see NodeRunsQuery.Database. Required.
	Database string
	// PipelineDir locates the pipeline on disk for the local provider; see
	// NodeRunsQuery.PipelineDir.
	PipelineDir string
	Limit       int
}

// Run is one parsed row from <pipeline>.runs (per Step Functions execution).
type Run struct {
	RunID          string `json:"run_id"`
	Pipeline       string `json:"pipeline"`
	SfExecutionARN string `json:"sf_execution_arn,omitempty"`
	Status         string `json:"status"`
	Trigger        string `json:"trigger,omitempty"`
	StartedAt      string `json:"started_at,omitempty"`
	EndedAt        string `json:"ended_at,omitempty"`
	DurationMs     *int64 `json:"duration_ms,omitempty"`
	FailedStep     string `json:"failed_step,omitempty"`
	ErrorClass     string `json:"error_class,omitempty"`
	ErrorMsg       string `json:"error_msg,omitempty"`
}

// RunsResult is the body of GET /data/runs.
type RunsResult struct {
	Rows      []Run `json:"rows"`
	Truncated bool  `json:"truncated"`
}

// SnapshotsQuery requests the snapshot history for one Iceberg table. Database
// is "clavesa_<pipeline>"; Table is "<node>__<output_key>".
type SnapshotsQuery struct {
	Database string
	Table    string
	// PipelineDir locates the pipeline on disk for the local provider; see
	// NodeRunsQuery.PipelineDir.
	PipelineDir string
	Limit       int
}

// SnapshotInfo describes one Iceberg snapshot.
type SnapshotInfo struct {
	SnapshotID     string `json:"snapshot_id"`
	ParentID       string `json:"parent_id,omitempty"`
	CommittedAt    string `json:"committed_at"`
	Operation      string `json:"operation,omitempty"`
	AddedRecords   *int64 `json:"added_records,omitempty"`
	DeletedRecords *int64 `json:"deleted_records,omitempty"`
	TotalRecords   *int64 `json:"total_records,omitempty"`
	// Trigger and WriterRunID are clavesa provenance copied out of the
	// Iceberg snapshot `summary` map (keys `clavesa.trigger` /
	// `clavesa.run-id`). Empty when the snapshot was written outside
	// clavesa or by a pre-provenance runner image.
	Trigger     string `json:"trigger,omitempty"`
	WriterRunID string `json:"writer_run_id,omitempty"`
}

// SnapshotsResult is the body of GET /data/tables/{db}/{table}/snapshots.
type SnapshotsResult struct {
	Snapshots         []SnapshotInfo `json:"snapshots"`
	LatestRecordCount *int64         `json:"latest_record_count,omitempty"`
	Truncated         bool           `json:"truncated"`
	// Total is the full commit count for the table, independent of the
	// returned (possibly limit-truncated) Snapshots slice — so the catalog
	// can show the real number of commits instead of "<limit>+".
	Total int `json:"total"`
}

// ColumnStatsQuery selects per-column statistics for one Iceberg table from
// the workspace system `column_stats` table. The handler resolves the
// system DB via Resolver.SystemGlueDB() and stamps it onto Database;
// callers without a resolver get an empty result rather than a 500.
//
// TableIdentifier is the full Spark identifier (`clavesa.<db>.<table>`),
// used as the row filter. Latest snapshot wins (one row per column).
type ColumnStatsQuery struct {
	// Database is the workspace system DB
	// (`<system_catalog>__pipelines`). Required.
	Database string
	// TableIdentifier is the full Iceberg identifier the stats describe
	// (`clavesa.<catalog>__<schema>.<table>`). Required.
	TableIdentifier string
	// PipelineDir locates the pipeline on disk for the local provider;
	// see NodeRunsQuery.PipelineDir.
	PipelineDir string
}

// ColumnStat is one row of the per-column profile surfaced to the UI.
// Nullable numerics use pointers so JSON omits them when the runner
// skipped a stat for that column (e.g. percentiles on non-numerics).
type ColumnStat struct {
	ColumnName          string             `json:"column_name"`
	ColumnType          string             `json:"column_type"`
	RowCount            *int64             `json:"row_count,omitempty"`
	NullCount           *int64             `json:"null_count,omitempty"`
	NullPct             *float64           `json:"null_pct,omitempty"`
	ApproxCountDistinct *int64             `json:"approx_count_distinct,omitempty"`
	MinValue            string             `json:"min_value,omitempty"`
	MaxValue            string             `json:"max_value,omitempty"`
	ApproxP50           *float64           `json:"approx_p50,omitempty"`
	ApproxP95           *float64           `json:"approx_p95,omitempty"`
	Top10               []ColumnStatBucket `json:"top_10,omitempty"`
	SnapshotID          string             `json:"snapshot_id,omitempty"`
	ComputedAt          string             `json:"computed_at,omitempty"`
}

// ColumnStatBucket is one entry in a column's top-K list.
type ColumnStatBucket struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

// ColumnStatsResult is the body of GET
// /data/tables/{database}/{table}/column-stats.
type ColumnStatsResult struct {
	// Stats holds one row per profiled column, in the order returned by
	// the provider. Empty (not an error) when the table has never been
	// profiled — the UI hides the card and shows a "turn on stats"
	// hint in that case.
	Stats []ColumnStat `json:"stats"`
	// SnapshotID is the snapshot the stats describe. Empty when no
	// rows exist; matches Stats[*].SnapshotID otherwise.
	SnapshotID string `json:"snapshot_id,omitempty"`
}

// parseTop10JSON decodes the JSON-encoded array<struct<value,count>> shape
// both providers emit (Athena via CAST(top_10 AS json), Spark via
// to_json(top_10)). Returns nil on empty input / decode failure so the
// UI hides the bucket bars cleanly without surfacing a parse error.
func parseTop10JSON(s string) []ColumnStatBucket {
	s = strings.TrimSpace(s)
	if s == "" || s == "null" {
		return nil
	}
	var raw []map[string]any
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil
	}
	out := make([]ColumnStatBucket, 0, len(raw))
	for _, m := range raw {
		var b ColumnStatBucket
		if v, ok := m["value"].(string); ok {
			b.Value = v
		} else if v, ok := m["value"]; ok && v != nil {
			b.Value = fmt.Sprintf("%v", v)
		}
		switch c := m["count"].(type) {
		case float64:
			b.Count = int64(c)
		case string:
			n, err := strconv.ParseInt(c, 10, 64)
			if err == nil {
				b.Count = n
			}
		}
		out = append(out, b)
	}
	return out
}

// SampleTableQuery requests a `SELECT * FROM <db>.<table> LIMIT <n>` against
// the catalog. Cloud routes through Athena; local through the runner-Spark
// container reading the Hadoop catalog. Both providers stringify rows for
// transport — the UI's TableDetail "Sample" panel only needs strings.
type SampleTableQuery struct {
	Database string
	Table    string
	// PipelineDir locates the pipeline on disk for the local provider; see
	// NodeRunsQuery.PipelineDir.
	PipelineDir string
	Limit       int
}

// SampleTableColumn is one column in a SampleTableResult.
type SampleTableColumn struct {
	Name string `json:"name"`
	// Type is best-effort. Cloud reads it from Athena's ResultSetMetadata;
	// local omits it (the runner's CLAVESA_QUERY mode only emits column
	// names today). The UI tolerates an empty string.
	Type string `json:"type"`
}

// SampleTableResult is the body of GET /data/table.
type SampleTableResult struct {
	Columns   []SampleTableColumn `json:"columns"`
	Rows      [][]string          `json:"rows"`
	RowCount  int                 `json:"row_count"`
	Truncated bool                `json:"truncated"`
}

// QueryQuery is the request for an arbitrary SQL query against the
// pipeline's warehouse. Used by the dashboards UI: each widget carries
// free-form SQL the user authored. Cloud routes through Athena; local
// through the runner-Spark container.
type QueryQuery struct {
	SQL         string
	PipelineDir string
	// MaxRows caps the number of rows returned to the UI even if the
	// caller's SQL has no LIMIT. Zero means "no UI-side cap" — the SQL
	// is trusted to bound itself. Defaults applied at the handler.
	MaxRows int
}

// QueryResult mirrors SampleTableResult — same JSON shape, distinct type
// because the contract differs (free-form SQL vs constrained `SELECT *
// FROM <db>.<table>`).
type QueryResult struct {
	Columns   []SampleTableColumn `json:"columns"`
	Rows      [][]string          `json:"rows"`
	RowCount  int                 `json:"row_count"`
	Truncated bool                `json:"truncated"`
}

// ExecQuery is a write — DDL or DML — against the pipeline's warehouse.
// Used by the dashboards service to CREATE/MERGE/DELETE rows of the
// `dashboards` system table. Cloud routes through Athena; local through
// the runner-Spark container. There is no result set: Exec returns only
// an error.
type ExecQuery struct {
	SQL string
	// PipelineDir scopes the provider dispatch and, for local, satisfies
	// the runner's non-empty-reference guard. The warehouse is workspace
	// shared, so any pipeline dir in the workspace resolves the same
	// catalog — the dashboards service passes the workspace root.
	PipelineDir string
}

// TablesQuery selects current-state-per-table from <database>.tables.
// One row per Iceberg-output table — the latest snapshot wins.
type TablesQuery struct {
	PipelineName string
	// Database — see NodeRunsQuery.Database. Required.
	Database    string
	PipelineDir string
	Limit       int
}

// TableInfo describes the latest snapshot of one Iceberg output table.
type TableInfo struct {
	Pipeline        string `json:"pipeline"`
	Node            string `json:"node"`
	OutputKey       string `json:"output_key"`
	TableName       string `json:"table_name"`
	TableID         string `json:"table_id"`
	SnapshotID      string `json:"snapshot_id,omitempty"`
	SnapshotTS      string `json:"snapshot_ts,omitempty"`
	RowCount        *int64 `json:"row_count,omitempty"`
	FileCount       *int64 `json:"file_count,omitempty"`
	TotalBytes      *int64 `json:"total_bytes,omitempty"`
	LastWriterRunID string `json:"last_writer_run_id,omitempty"`
}

// TablesResult is the body of GET /data/tables-state (current snapshot
// summary across every Iceberg output the pipeline produces). Distinct
// from /data/tables/{db}/{table}/snapshots, which lists the snapshot
// history for one table.
type TablesResult struct {
	Rows      []TableInfo `json:"rows"`
	Truncated bool        `json:"truncated"`
}

// ExecutionStatesQuery selects per-node state for one execution. ExecutionRef
// is an SFN ARN for cloud and a local run-id for local pipelines.
type ExecutionStatesQuery struct {
	ExecutionRef string
}

// StateStatus is one per-node entry in ExecutionStatesResult.
type StateStatus struct {
	// Status is "RUNNING", "SUCCEEDED", or "FAILED". Nodes that never entered
	// are absent (the UI treats absent as PENDING).
	Status string `json:"status"`
	// EnteredAt is the latest time the state was entered (ISO 8601 UTC).
	EnteredAt string `json:"entered_at,omitempty"`
	// In-flight Spark progress for a RUNNING node, mirrored from the
	// runner's periodic `progress` event. All nullable: nil until the
	// first tick, and absent once the node reaches a terminal state.
	// Cloud doesn't populate these yet (a later slice fills them from SFN
	// map-run metadata); local copies them from NodeRunState.
	StagesTotal     *int64 `json:"stages_total,omitempty"`
	StagesCompleted *int64 `json:"stages_completed,omitempty"`
	TasksTotal      *int64 `json:"tasks_total,omitempty"`
	TasksCompleted  *int64 `json:"tasks_completed,omitempty"`
	TasksFailed     *int64 `json:"tasks_failed,omitempty"`
}

// ExecutionStatesResult is the body of GET /pipeline/execution/states.
type ExecutionStatesResult struct {
	// Status is the overall execution status (RUNNING/SUCCEEDED/FAILED/...).
	Status string `json:"status"`
	// States maps node name → per-node status.
	States map[string]StateStatus `json:"states"`
	// RunID identifies the execution this state belongs to. Empty when
	// the provider couldn't resolve one (no runs yet, malformed state
	// file). Lets the dashboard synthesise a run column for an in-flight
	// execution that hasn't yet landed in the runs table.
	RunID string `json:"run_id,omitempty"`
	// StartedAt is the ISO-8601 UTC moment the execution began. Empty
	// when unknown; the dashboard falls back to "now" so the synthetic
	// column still gets a sortable position.
	StartedAt string `json:"started_at,omitempty"`
}

// ExecutionLogsQuery selects log events for one node within one execution.
type ExecutionLogsQuery struct {
	ExecutionRef string
	Step         string
}

// LogEvent is one log line surfaced by the provider.
type LogEvent struct {
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
}

// ExecutionLogsResult is the body of GET /pipeline/execution/logs.
//
// LogGroup and FunctionName are operator-debug strings — opaque labels the
// UI surfaces as "where these logs came from" so a developer eyeballing
// the panel can find the same logs in the underlying tooling. They have
// different semantics per backend (CloudWatch group + Lambda function on
// cloud; filesystem path + step ID on local) and must not be parsed.
// Source is the typed discriminator the UI branches on for backend-specific
// affordances ("query CloudWatch directly" vs. "tail the file"); LogGroup
// and FunctionName are the human-friendly text to render alongside.
type ExecutionLogsResult struct {
	// Source is "cloudwatch" or "local" — tells the UI which backend served
	// the response so it can render the right "where to find more" hint.
	Source       string     `json:"source"`
	LogGroup     string     `json:"log_group"`
	FunctionName string     `json:"function_name"`
	Events       []LogEvent `json:"events"`
	Truncated    bool       `json:"truncated"`
}

// Source values for ExecutionLogsResult.Source.
const (
	LogSourceCloudWatch = "cloudwatch"
	LogSourceLocal      = "local"
)
