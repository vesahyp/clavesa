package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/vesahyp/clavesa/internal/delta"
	"github.com/vesahyp/clavesa/internal/delta/s3fs"
)

// AthenaClient is the subset of the AWS Athena API the cloud provider uses.
type AthenaClient interface {
	StartQueryExecution(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
	GetQueryExecution(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
	GetQueryResults(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
}

// SFNClient is the subset of Step Functions the cloud provider uses.
type SFNClient interface {
	DescribeExecution(ctx context.Context, params *sfn.DescribeExecutionInput, optFns ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error)
	GetExecutionHistory(ctx context.Context, params *sfn.GetExecutionHistoryInput, optFns ...func(*sfn.Options)) (*sfn.GetExecutionHistoryOutput, error)
}

// CWLClient is the subset of CloudWatch Logs the cloud provider uses.
type CWLClient interface {
	FilterLogEvents(ctx context.Context, params *cloudwatchlogs.FilterLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error)
}

// GlueClient is the subset of Glue this provider uses. ADR-018: Athena
// can't read Delta's commit history (`DESCRIBE HISTORY` isn't supported
// for Delta tables in Athena), so we resolve the table's S3 LOCATION via
// Glue and read `_delta_log/` directly. v1.x's `<table>$snapshots`
// Athena query is gone with Iceberg.
type GlueClient interface {
	GetTable(ctx context.Context, params *glue.GetTableInput, optFns ...func(*glue.Options)) (*glue.GetTableOutput, error)
}

// CloudProvider satisfies Provider for deployed pipelines: Athena for
// row reads + run history, SFN for execution state, CloudWatch for logs,
// and Glue+S3+delta for the snapshot timeline (ADR-018 swap from Iceberg
// `<table>$snapshots`).
type CloudProvider struct {
	athena             AthenaClient
	athenaOutputBucket string
	sfn                SFNClient
	cwl                CWLClient
	// Glue + S3 are optional — when both are wired, Snapshots() reads
	// Delta `_delta_log/` directly from S3. When either is missing
	// (legacy callers without WithGlue/WithS3), Snapshots returns an
	// empty result rather than blowing up — same fail-soft contract as
	// `undeployed()` above.
	glue GlueClient
	s3   s3fs.S3API
	// clock returns "now" for freshness windowing of live progress
	// snapshots. Overridable in tests for deterministic assertions;
	// defaults to time.Now (set in NewCloudProvider, and defended against
	// nil via nowFn for providers built by struct literal).
	clock func() time.Time
}

// NewCloudProvider wires a provider against AWS SDK clients. Any subset of
// clients may be nil; methods that require an unset client return a typed
// error rather than panicking. Snapshots() additionally needs both Glue
// and S3 — set them via WithGlue / WithS3 before calling.
func NewCloudProvider(athenaC AthenaClient, athenaOutputBucket string, sfnC SFNClient, cwlC CWLClient) *CloudProvider {
	return &CloudProvider{
		athena:             athenaC,
		athenaOutputBucket: athenaOutputBucket,
		sfn:                sfnC,
		cwl:                cwlC,
		clock:              time.Now,
	}
}

// nowFn returns the provider's clock, falling back to time.Now when
// unset. Providers built via NewCloudProvider always have clock wired;
// the fallback covers struct-literal construction in tests.
func (c *CloudProvider) nowFn() time.Time {
	if c.clock != nil {
		return c.clock()
	}
	return time.Now()
}

// WithGlue attaches a Glue client for table-location lookup. Required
// (together with WithS3) for Snapshots() to read Delta commit history.
func (c *CloudProvider) WithGlue(g GlueClient) *CloudProvider {
	c.glue = g
	return c
}

// WithS3 attaches an S3 client for reading `_delta_log/` directly.
// Required (together with WithGlue) for Snapshots() — see ADR-018.
func (c *CloudProvider) WithS3(s3c s3fs.S3API) *CloudProvider {
	c.s3 = s3c
	return c
}

// undeployed reports whether the workspace has no deployed Athena
// warehouse — the auto-derived results bucket is empty because there is
// no `pipeline_bucket` output in terraform.tfstate yet. Athena-backed
// reads short-circuit to an empty result in that case: switching a
// not-yet-deployed workspace to cloud mode is a valid empty state, not
// a 500 (TODO bucket 16).
func (c *CloudProvider) undeployed() bool { return c.athenaOutputBucket == "" }

// isAthenaMissingTableErr classifies an Athena error as "table or
// database does not exist yet" so the provider can return an empty
// result with a nil error — matching the Provider contract documented
// at provider.go and the local-side `isMissingTableErr` precedent.
// Triggered by a workspace that's been deployed but never run, or one
// whose system-catalog Glue DB was rebuilt: the system Delta tables
// (runs / node_runs / tables / column_stats / dashboard datasets)
// haven't been created yet, and Athena 500s the SELECT.
func isAthenaMissingTableErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{
		"TABLE_NOT_FOUND",
		"Table not found",
		"does not exist",
		"database does not exist",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

const (
	athenaMaxPollAttempts = 60
	athenaPollInterval    = 500 * time.Millisecond
	// athenaResultReuseMinutes bounds how stale a reused query result may be.
	// Short enough that a just-finished run's rollup shows up promptly on the
	// next poll, long enough to collapse the repeated per-page cold-starts.
	athenaResultReuseMinutes = 5
	logsLimit                = 500
)

var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// IsValidIdentifier reports whether s is safe to embed in a SQL identifier.
// Glue / Iceberg names are constrained to this shape; anything else is a 400
// from the HTTP layer's perspective.
func IsValidIdentifier(s string) bool { return identifierRE.MatchString(s) }

// pipelineNameRE matches the shape `pipeline create` accepts as a directory
// / display name: leading letter or underscore, then letters / digits /
// underscores / hyphens. Pipeline names are NOT Glue identifiers — they
// land as literal values in system-table columns (node_runs.pipeline,
// runs.pipeline, column_stats.pipeline). Validating with the stricter
// identifier regex 400s every hyphenated pipeline; the SQL boundary
// already single-quote-escapes literals.
var pipelineNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

// IsValidPipelineName reports whether s is a valid pipeline-name literal —
// matching what `clavesa pipeline create` accepts.
func IsValidPipelineName(s string) bool { return pipelineNameRE.MatchString(s) }

// legacyDBFallback returns db when set; otherwise the legacy
// `clavesa_<pipeline>` database name. The fallback exists for
// handler-without-resolver mode (tests and bare /data/runs?pipeline=foo
// curls without a `dir` param). Production code always wires a Resolver
// and sets Database via it. The fallback uses today's
// `clavesa_<PipelineName>` shape — a no-op for v0.17 schemas, broken for
// post-migration workspaces (whose DB names start `<catalog>__`). Tests
// pass a pipeline name that matches a `clavesa_<pipeline>` DB; real
// callers should pass `dir` so the resolver can compute the encoded DB.
func legacyDBFallback(db, pipelineName string) string {
	if db != "" {
		return db
	}
	return "clavesa_" + pipelineName
}

// ---------------------------------------------------------------------------
// NodeRuns
// ---------------------------------------------------------------------------

func (c *CloudProvider) NodeRuns(ctx context.Context, q NodeRunsQuery) (*NodeRunsResult, error) {
	if c.undeployed() {
		return &NodeRunsResult{Rows: []NodeRun{}}, nil
	}
	if c.athena == nil {
		return nil, fmt.Errorf("cloud: athena client not configured")
	}
	if !IsValidPipelineName(q.PipelineName) {
		return nil, fmt.Errorf("invalid pipeline name: %q", q.PipelineName)
	}
	if q.Node != "" && !IsValidIdentifier(q.Node) {
		return nil, fmt.Errorf("invalid node name: %q", q.Node)
	}

	dbName := legacyDBFallback(q.Database, q.PipelineName)
	// Workspace-wide system DB (ADR-016 v0.20.0) — multi-writer; filter
	// by the `pipeline` column so the per-pipeline UI surface keeps the
	// same row shape.
	conds := []string{fmt.Sprintf("pipeline = '%s'", strings.ReplaceAll(q.PipelineName, "'", "''"))}
	if q.Node != "" {
		conds = append(conds, fmt.Sprintf("node = '%s'", q.Node))
	}
	if q.SfExecutionARN != "" {
		safe := strings.ReplaceAll(q.SfExecutionARN, "'", "''")
		conds = append(conds, fmt.Sprintf("sf_execution_arn = '%s'", safe))
	}
	whereClause := "WHERE " + strings.Join(conds, " AND ")

	sql := fmt.Sprintf(
		`SELECT
  run_id,
  pipeline,
  node,
  to_iso8601(started_at) AS started_at,
  to_iso8601(ended_at) AS ended_at,
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
FROM "%s"."node_runs"
%s
ORDER BY started_at DESC
LIMIT %d`, dbName, whereClause, q.Limit+1)

	rows, truncated, _, err := c.runAthenaSelect(ctx, sql, q.Limit)
	if err != nil {
		if isAthenaMissingTableErr(err) {
			return &NodeRunsResult{Rows: []NodeRun{}}, nil
		}
		return nil, err
	}

	out := make([]NodeRun, 0, len(rows))
	for _, row := range rows {
		if len(row.Data) < 32 {
			continue
		}
		out = append(out, NodeRun{
			RunID:             stringDatum(row.Data[0]),
			Pipeline:          stringDatum(row.Data[1]),
			Node:              stringDatum(row.Data[2]),
			StartedAt:         stringDatum(row.Data[3]),
			EndedAt:           stringDatum(row.Data[4]),
			DurationMs:        intDatum(row.Data[5]),
			Status:            stringDatum(row.Data[6]),
			ComputeTarget:     stringDatum(row.Data[7]),
			MemoryMB:          intDatum(row.Data[8]),
			ColdStart:         boolDatum(row.Data[9]),
			LambdaRequestID:   stringDatum(row.Data[10]),
			ErrorClass:        stringDatum(row.Data[11]),
			ErrorMsg:          stringDatum(row.Data[12]),
			RunnerImageDigest: stringDatum(row.Data[13]),
			ModuleVersion:     stringDatum(row.Data[14]),
			OutputRows:        intDatum(row.Data[15]),
			SfExecutionARN:    stringDatum(row.Data[16]),

			PeakRSSMB:             intDatum(row.Data[17]),
			PeakExecutionMemoryMB: intDatum(row.Data[18]),
			MemorySpilledBytes:    intDatum(row.Data[19]),
			DiskSpilledBytes:      intDatum(row.Data[20]),
			ShuffleReadBytes:      intDatum(row.Data[21]),
			ShuffleWriteBytes:     intDatum(row.Data[22]),
			InputBytes:            intDatum(row.Data[23]),
			InputRecords:          intDatum(row.Data[24]),
			NumStages:             intDatum(row.Data[25]),
			NumTasks:              intDatum(row.Data[26]),
			NumFailedTasks:        intDatum(row.Data[27]),
			JVMGCTimeMs:           intDatum(row.Data[28]),
			ExecutorCPUTimeMs:     intDatum(row.Data[29]),
			ExecutorRunTimeMs:     intDatum(row.Data[30]),
			MaxTaskDurationMs:     intDatum(row.Data[31]),
		})
	}
	return &NodeRunsResult{Rows: out, Truncated: truncated}, nil
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

func (c *CloudProvider) Runs(ctx context.Context, q RunsQuery) (*RunsResult, error) {
	if c.undeployed() {
		return &RunsResult{Rows: []Run{}}, nil
	}
	if c.athena == nil {
		return nil, fmt.Errorf("cloud: athena client not configured")
	}
	if !IsValidPipelineName(q.PipelineName) {
		return nil, fmt.Errorf("invalid pipeline name: %q", q.PipelineName)
	}

	dbName := legacyDBFallback(q.Database, q.PipelineName)
	safePipeline := strings.ReplaceAll(q.PipelineName, "'", "''")
	sql := fmt.Sprintf(
		`SELECT
  run_id,
  pipeline,
  sf_execution_arn,
  status,
  trigger,
  to_iso8601(started_at) AS started_at,
  to_iso8601(ended_at)   AS ended_at,
  duration_ms,
  failed_step,
  error_class,
  error_msg
FROM "%s"."runs"
WHERE pipeline = '%s'
ORDER BY started_at DESC
LIMIT %d`, dbName, safePipeline, q.Limit+1)

	rows, truncated, _, err := c.runAthenaSelect(ctx, sql, q.Limit)
	if err != nil {
		if isAthenaMissingTableErr(err) {
			return &RunsResult{Rows: []Run{}}, nil
		}
		return nil, err
	}

	out := make([]Run, 0, len(rows))
	for _, row := range rows {
		if len(row.Data) < 11 {
			continue
		}
		out = append(out, Run{
			RunID:          stringDatum(row.Data[0]),
			Pipeline:       stringDatum(row.Data[1]),
			SfExecutionARN: stringDatum(row.Data[2]),
			Status:         stringDatum(row.Data[3]),
			Trigger:        stringDatum(row.Data[4]),
			StartedAt:      stringDatum(row.Data[5]),
			EndedAt:        stringDatum(row.Data[6]),
			DurationMs:     intDatum(row.Data[7]),
			FailedStep:     stringDatum(row.Data[8]),
			ErrorClass:     stringDatum(row.Data[9]),
			ErrorMsg:       stringDatum(row.Data[10]),
		})
	}
	return &RunsResult{Rows: out, Truncated: truncated}, nil
}

// ---------------------------------------------------------------------------
// Tables — current-state-per-table from clavesa_<pipeline>.tables
// ---------------------------------------------------------------------------

func (c *CloudProvider) Tables(ctx context.Context, q TablesQuery) (*TablesResult, error) {
	if c.undeployed() {
		return &TablesResult{Rows: []TableInfo{}}, nil
	}
	if c.athena == nil {
		return nil, fmt.Errorf("cloud: athena client not configured")
	}
	if !IsValidPipelineName(q.PipelineName) {
		return nil, fmt.Errorf("invalid pipeline name: %q", q.PipelineName)
	}

	dbName := legacyDBFallback(q.Database, q.PipelineName)
	// Latest snapshot per table_id via ROW_NUMBER. Same idiom as the local
	// provider's Spark version — the SQL surface stays uniform.
	safePipeline := strings.ReplaceAll(q.PipelineName, "'", "''")
	sql := fmt.Sprintf(
		`SELECT pipeline, node, output_key, table_name, table_id,
  CAST(snapshot_id AS varchar) AS snapshot_id,
  to_iso8601(snapshot_ts)      AS snapshot_ts,
  row_count, file_count, total_bytes, last_writer_run_id
FROM (
  SELECT *, row_number() OVER (PARTITION BY table_id ORDER BY snapshot_ts DESC) AS rn
  FROM "%s"."tables"
  WHERE pipeline = '%s'
)
WHERE rn = 1
ORDER BY snapshot_ts DESC
LIMIT %d`, dbName, safePipeline, q.Limit+1)

	rows, truncated, _, err := c.runAthenaSelect(ctx, sql, q.Limit)
	if err != nil {
		if isAthenaMissingTableErr(err) {
			return &TablesResult{Rows: []TableInfo{}}, nil
		}
		return nil, err
	}

	out := make([]TableInfo, 0, len(rows))
	for _, row := range rows {
		if len(row.Data) < 11 {
			continue
		}
		out = append(out, TableInfo{
			Pipeline:        stringDatum(row.Data[0]),
			Node:            stringDatum(row.Data[1]),
			OutputKey:       stringDatum(row.Data[2]),
			TableName:       stringDatum(row.Data[3]),
			TableID:         stringDatum(row.Data[4]),
			SnapshotID:      stringDatum(row.Data[5]),
			SnapshotTS:      stringDatum(row.Data[6]),
			RowCount:        intDatum(row.Data[7]),
			FileCount:       intDatum(row.Data[8]),
			TotalBytes:      intDatum(row.Data[9]),
			LastWriterRunID: stringDatum(row.Data[10]),
		})
	}
	return &TablesResult{Rows: out, Truncated: truncated}, nil
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// Snapshots reads the Delta transaction log for one table and projects
// the recent commit history into the same JSON shape v1.x's Iceberg
// `<table>$snapshots` Athena query produced. ADR-018: Athena's Delta
// support is read-only and lacks `DESCRIBE HISTORY`, so we resolve the
// table's S3 LOCATION via Glue and read `_delta_log/` ourselves via
// internal/delta + internal/delta/s3fs.
//
// Empty result on undeployed workspaces, missing Glue/S3 clients, or a
// table that hasn't materialised yet — same fail-soft contract the
// Iceberg path used. UI gates the snapshot timeline on a non-empty
// result; an empty list renders as "no snapshots yet" rather than an
// error toast.
func (c *CloudProvider) Snapshots(ctx context.Context, q SnapshotsQuery) (*SnapshotsResult, error) {
	if c.undeployed() {
		return &SnapshotsResult{Snapshots: []SnapshotInfo{}}, nil
	}
	if !IsValidIdentifier(q.Database) {
		return nil, fmt.Errorf("invalid database name: %q", q.Database)
	}
	if !IsValidIdentifier(q.Table) {
		return nil, fmt.Errorf("invalid table name: %q", q.Table)
	}
	if c.glue == nil || c.s3 == nil {
		// No Glue/S3 wiring means this provider was built by an older
		// caller (dataquery tests with NewHandler) — degrade to empty
		// rather than 500 so handler-level tests keep working without
		// learning the new dependency.
		return &SnapshotsResult{Snapshots: []SnapshotInfo{}}, nil
	}

	bucket, prefix, err := c.tableS3Location(ctx, q.Database, q.Table)
	if err != nil {
		if errors.Is(err, errTableNotFound) {
			return &SnapshotsResult{Snapshots: []SnapshotInfo{}}, nil
		}
		return nil, err
	}
	if bucket == "" || prefix == "" {
		return &SnapshotsResult{Snapshots: []SnapshotInfo{}}, nil
	}
	logPrefix := strings.TrimSuffix(prefix, "/") + "/_delta_log/"

	fsys := s3fs.New(ctx, c.s3, bucket, logPrefix)
	_, commits, err := delta.ReadCurrent(fsys)
	if err != nil {
		if errors.Is(err, delta.ErrNotDelta) {
			return &SnapshotsResult{Snapshots: []SnapshotInfo{}}, nil
		}
		return nil, fmt.Errorf("read delta log: %w", err)
	}

	// Projection (incl. the LatestRecordCount fold) is shared with
	// LocalProvider.Snapshots so local and cloud agree (ADR-014).
	return snapshotsResultFromCommits(commits, q.Limit), nil
}

// errTableNotFound is the sentinel tableS3Location returns when Glue's
// GetTable surfaces an EntityNotFoundException. Callers (Snapshots) map
// it to an empty success rather than a 500.
var errTableNotFound = errors.New("table not found in Glue catalog")

// tableS3Location resolves the table's S3 (bucket, key prefix) via Glue.
// The StorageDescriptor.Location field is an s3:// URI Spark/Delta wrote
// at create time — for Delta tables it's the table root, so callers
// append `_delta_log/` to read the transaction log.
//
// Accepts both the post-ADR-019 bare form (`<node>`) and the legacy
// `<node>__default` form: on EntityNotFoundException for a bare name,
// retries with the `__default` suffix so pre-cutover Glue tables still
// resolve under the new URL scheme.
func (c *CloudProvider) tableS3Location(ctx context.Context, db, table string) (bucket, prefix string, err error) {
	bucket, prefix, err = c.glueGetLocation(ctx, db, table)
	if errors.Is(err, errTableNotFound) && !strings.Contains(table, "__") {
		if b2, p2, err2 := c.glueGetLocation(ctx, db, table+"__default"); err2 == nil {
			return b2, p2, nil
		}
	}
	return bucket, prefix, err
}

func (c *CloudProvider) glueGetLocation(ctx context.Context, db, table string) (bucket, prefix string, err error) {
	out, err := c.glue.GetTable(ctx, &glue.GetTableInput{
		DatabaseName: aws.String(db),
		Name:         aws.String(table),
	})
	if err != nil {
		// Glue surfaces missing tables as EntityNotFoundException; the
		// SDK v2 wraps it in a typed error we can match on the ErrorCode
		// string. Fall back to a substring check for older SDK
		// versions / direct HTTP errors.
		var coder interface{ ErrorCode() string }
		if errors.As(err, &coder) && coder.ErrorCode() == "EntityNotFoundException" {
			return "", "", errTableNotFound
		}
		if strings.Contains(err.Error(), "EntityNotFoundException") {
			return "", "", errTableNotFound
		}
		return "", "", fmt.Errorf("glue.GetTable %s.%s: %w", db, table, err)
	}
	if out.Table == nil || out.Table.StorageDescriptor == nil {
		return "", "", errTableNotFound
	}
	loc := aws.ToString(out.Table.StorageDescriptor.Location)
	return parseS3URI(loc)
}

// parseS3URI splits `s3://bucket/key/path` into (bucket, key). Returns
// empty strings for non-S3 or malformed URIs — caller treats that as
// "no Delta log here" and surfaces an empty result.
func parseS3URI(uri string) (bucket, prefix string, err error) {
	const scheme = "s3://"
	if !strings.HasPrefix(uri, scheme) {
		return "", "", nil
	}
	rest := uri[len(scheme):]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return rest, "", nil
	}
	bucket = rest[:slash]
	prefix = rest[slash+1:]
	return bucket, prefix, nil
}

// extractProvenance unmarshals the JSON object commit's userMetadata
// field carries (`{"clavesa.trigger": "...", "clavesa.run-id": "..."}`,
// stamped by sub-slice 3 via _apply_snapshot_props in the runner) and
// returns (trigger, runID). Non-JSON / missing fields gracefully
// degrade to empty strings — the UI hides empty values cleanly.
//
// Mirrors the v1.x Iceberg path which read the same two keys from
// snapshot.summary at Athena query time; the shape on the wire is
// identical so the UI doesn't have to learn about the new storage
// site.
func extractProvenance(userMetadata string) (trigger, runID string) {
	if userMetadata == "" {
		return "", ""
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(userMetadata), &m); err != nil {
		return "", ""
	}
	return m["clavesa.trigger"], m["clavesa.run-id"]
}

// formatMillis renders epoch milliseconds as the same ISO-8601 UTC
// shape Athena's `to_iso8601(committed_at)` produced — the UI parses
// this string directly, so a divergent format would break the snapshot
// timeline's relative-time rendering.
func formatMillis(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
}

// ---------------------------------------------------------------------------
// ColumnStats — opt-in per-column profile (latest snapshot per column)
// ---------------------------------------------------------------------------

// ColumnStats reads the latest-snapshot row per column from the workspace
// system column_stats Delta table. Empty result on undeployed
// workspaces or fresh tables that haven't been profiled yet — same
// "no rows means no card" contract the UI gates on.
func (c *CloudProvider) ColumnStats(ctx context.Context, q ColumnStatsQuery) (*ColumnStatsResult, error) {
	if c.undeployed() {
		return &ColumnStatsResult{Stats: []ColumnStat{}}, nil
	}
	if c.athena == nil {
		return nil, fmt.Errorf("cloud: athena client not configured")
	}
	if q.Database == "" {
		return &ColumnStatsResult{Stats: []ColumnStat{}}, nil
	}
	if !IsValidIdentifier(q.Database) {
		return nil, fmt.Errorf("invalid system database name: %q", q.Database)
	}
	if q.TableIdentifier == "" {
		return nil, fmt.Errorf("table_identifier is required")
	}

	safeIdent := strings.ReplaceAll(q.TableIdentifier, "'", "''")
	sql := fmt.Sprintf(
		`SELECT column_name, column_type,
  row_count, null_count, null_pct,
  approx_count_distinct,
  min_value, max_value,
  approx_p50, approx_p95,
  CAST(top_10 AS json) AS top_10_json,
  CAST(snapshot_id AS varchar) AS snapshot_id,
  to_iso8601(computed_at) AS computed_at
FROM (
  SELECT *,
    row_number() OVER (PARTITION BY column_name ORDER BY computed_at DESC) AS rn
  FROM "%s"."column_stats"
  WHERE table_identifier = '%s'
)
WHERE rn = 1
ORDER BY column_name`, q.Database, safeIdent)

	rows, _, _, err := c.runAthenaSelect(ctx, sql, -1)
	if err != nil {
		// Athena returns SYNTAX_ERROR for "no such table" — surface as
		// empty so a workspace that's never run a stats-on transform
		// renders cleanly instead of flashing a 500.
		if isAthenaMissingTableErr(err) {
			return &ColumnStatsResult{Stats: []ColumnStat{}}, nil
		}
		return nil, err
	}

	out := make([]ColumnStat, 0, len(rows))
	for _, row := range rows {
		if len(row.Data) < 13 {
			continue
		}
		stat := ColumnStat{
			ColumnName:          stringDatum(row.Data[0]),
			ColumnType:          stringDatum(row.Data[1]),
			RowCount:            intDatum(row.Data[2]),
			NullCount:           intDatum(row.Data[3]),
			NullPct:             floatDatum(row.Data[4]),
			ApproxCountDistinct: intDatum(row.Data[5]),
			MinValue:            stringDatum(row.Data[6]),
			MaxValue:            stringDatum(row.Data[7]),
			ApproxP50:           floatDatum(row.Data[8]),
			ApproxP95:           floatDatum(row.Data[9]),
			Top10:               parseTop10JSON(stringDatum(row.Data[10])),
			SnapshotID:          stringDatum(row.Data[11]),
			ComputedAt:          stringDatum(row.Data[12]),
		}
		out = append(out, stat)
	}
	res := &ColumnStatsResult{Stats: out}
	if len(out) > 0 {
		res.SnapshotID = out[0].SnapshotID
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// SampleTable
// ---------------------------------------------------------------------------

// SampleTable runs `SELECT * FROM <db>.<table> LIMIT N+1` via Athena and
// returns the rows as strings. Mirrors the legacy dataquery.queryTable —
// the dataquery handler now dispatches through this so local-only tables
// work via the LocalProvider implementation (ADR-014 parity).
func (c *CloudProvider) SampleTable(ctx context.Context, q SampleTableQuery) (*SampleTableResult, error) {
	if c.undeployed() {
		return &SampleTableResult{Columns: []SampleTableColumn{}, Rows: [][]string{}, Served: servedAthenaCloud()}, nil
	}
	if c.athena == nil {
		return nil, fmt.Errorf("cloud: athena client not configured")
	}
	if !IsValidIdentifier(q.Database) {
		return nil, fmt.Errorf("invalid database name: %q", q.Database)
	}
	if !IsValidIdentifier(q.Table) {
		return nil, fmt.Errorf("invalid table name: %q", q.Table)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}

	// Back-compat for legacy `<node>__default` tables: if Glue lacks the
	// bare table, retry against the suffixed variant before issuing SQL.
	tableForSQL := q.Table
	if c.glue != nil && !strings.Contains(q.Table, "__") {
		if _, _, err := c.glueGetLocation(ctx, q.Database, q.Table); errors.Is(err, errTableNotFound) {
			if _, _, err2 := c.glueGetLocation(ctx, q.Database, q.Table+"__default"); err2 == nil {
				tableForSQL = q.Table + "__default"
			}
		}
	}

	sql := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d", q.Database, tableForSQL, limit+1)
	dataRows, truncated, rs, err := c.runAthenaSelect(ctx, sql, limit)
	if err != nil {
		return nil, err
	}

	cols := make([]SampleTableColumn, 0)
	if rs.ResultSetMetadata != nil {
		for _, ci := range rs.ResultSetMetadata.ColumnInfo {
			cols = append(cols, SampleTableColumn{
				Name: aws.ToString(ci.Name),
				Type: aws.ToString(ci.Type),
			})
		}
	}

	rows := make([][]string, len(dataRows))
	for i, row := range dataRows {
		r := make([]string, len(row.Data))
		for j, d := range row.Data {
			r[j] = stringDatum(d)
		}
		rows[i] = r
	}
	return &SampleTableResult{
		Columns:   cols,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
		Served:    servedAthenaCloud(),
	}, nil
}

// ---------------------------------------------------------------------------
// Query — free-form SQL for the dashboards UI
// ---------------------------------------------------------------------------

// Query runs the caller-supplied SQL via Athena and returns string-rendered
// rows. No identifier validation — the SQL is whatever the user authored
// in their dashboard widget JSON. Same security boundary as Athena's
// IAM: the executing role's grants control what's reachable.
func (c *CloudProvider) Query(ctx context.Context, q QueryQuery) (*QueryResult, error) {
	if c.undeployed() {
		return &QueryResult{Columns: []SampleTableColumn{}, Rows: [][]string{}, Served: servedAthenaCloud()}, nil
	}
	if c.athena == nil {
		return nil, fmt.Errorf("cloud: athena client not configured")
	}
	if q.SQL == "" {
		return nil, fmt.Errorf("query: sql is required")
	}

	// MaxRows <= 0 means "no cap" — runAthenaSelect treats a negative
	// limit as trim-disabled.
	maxRows := q.MaxRows
	if maxRows <= 0 {
		maxRows = -1
	}
	dataRows, truncated, rs, err := c.runAthenaSelect(ctx, q.SQL, maxRows)
	if err != nil {
		if isAthenaMissingTableErr(err) {
			// Dashboards widget against a workspace that's been deployed
			// but never run — system Delta tables don't exist yet.
			// Surface as an empty result so the widget renders cleanly
			// instead of 500-ing the whole dashboard.
			return &QueryResult{Columns: []SampleTableColumn{}, Rows: [][]string{}, Served: servedAthenaCloud()}, nil
		}
		return nil, err
	}

	cols := make([]SampleTableColumn, 0)
	if rs.ResultSetMetadata != nil {
		for _, ci := range rs.ResultSetMetadata.ColumnInfo {
			cols = append(cols, SampleTableColumn{
				Name: aws.ToString(ci.Name),
				Type: aws.ToString(ci.Type),
			})
		}
	}

	rows := make([][]string, len(dataRows))
	for i, row := range dataRows {
		r := make([]string, len(row.Data))
		for j, d := range row.Data {
			r[j] = stringDatum(d)
		}
		rows[i] = r
	}
	return &QueryResult{
		Columns:   cols,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
		Served:    servedAthenaCloud(),
	}, nil
}

// Exec runs a write (CREATE TABLE / MERGE / DELETE) through Athena. Iceberg
// DML is supported by Athena; the result set is empty and discarded — only
// the error matters.
func (c *CloudProvider) Exec(ctx context.Context, q ExecQuery) error {
	if c.undeployed() {
		return fmt.Errorf("cloud: workspace is not deployed — no Athena catalog to write to")
	}
	if c.athena == nil {
		return fmt.Errorf("cloud: athena client not configured")
	}
	if q.SQL == "" {
		return fmt.Errorf("exec: sql is required")
	}
	_, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, q.SQL)
	return err
}

// ---------------------------------------------------------------------------
// ExecutionStates
// ---------------------------------------------------------------------------

func (c *CloudProvider) ExecutionStates(ctx context.Context, q ExecutionStatesQuery) (*ExecutionStatesResult, error) {
	if q.ExecutionRef == "" {
		return nil, fmt.Errorf("execution ref is required")
	}

	// The ExecutionRef arrives in one of two on-the-wire shapes (FormatExecRef):
	// a full SFN execution ARN (real cloud run), or a "dir:runID" composite for
	// a non-ARN run id — a cloud-local run (ADR-024) carries `dir:local-…`. The
	// warehouse progress tree is keyed by the run id ALONE (the dir is a local
	// addressing detail, irrelevant to the cloud bucket), so extract it; an ARN
	// is self-contained and used as-is.
	isARN := StateMachineNameFromExecutionARN(q.ExecutionRef) != ""
	runID := q.ExecutionRef
	if !isARN {
		if _, rid := splitExecRef(q.ExecutionRef); rid != "" {
			runID = rid
		}
	}

	// Per-node status always comes from the `_progress/<run>/<node>.json`
	// markers the runner writes into the S3 warehouse: a "running" snapshot
	// rewritten each poll tick while a node is in flight, then a single
	// terminal marker ("succeeded"/"failed"/"skipped") once it finishes. We
	// LIST those and let each marker's own `status` field drive the per-node
	// StateStatus — authoritative for both live and completed nodes (ADR-014
	// parity with local). Backend-blind via the shared progressStates helper.
	store := c.progressStore()
	states := progressStates(ctx, store, runID, c.nowFn().UnixMilli())

	// Overall status: prefer SFN DescribeExecution when the ref is a real
	// execution ARN and SFN is wired. A cloud-local run has a `local-…` run id
	// and no SFN execution; its overall status comes from the `_run.json` run
	// marker instead. The old "bare dir / undeployed → empty" bail is gone — it
	// broke cloud-local by returning empty before the progress markers could be
	// read.
	if isARN && c.sfn != nil {
		desc, err := c.sfn.DescribeExecution(ctx, &sfn.DescribeExecutionInput{
			ExecutionArn: aws.String(q.ExecutionRef),
		})
		if err == nil {
			startedAt := ""
			if desc.StartDate != nil {
				startedAt = desc.StartDate.UTC().Format("2006-01-02T15:04:05.000Z")
			}
			return &ExecutionStatesResult{
				Status:    string(desc.Status),
				States:    states,
				RunID:     q.ExecutionRef,
				StartedAt: startedAt,
			}, nil
		}
		// ExecutionDoesNotExist (or any describe failure) falls through to the
		// run-marker path — a non-SFN run id whose progress lives only in the
		// warehouse markers.
	}

	// No SFN execution to inspect: derive overall status from `_run.json`.
	// Absent marker on a freshly-dispatched run reads as RUNNING so the
	// dashboard renders an in-flight column instead of flashing empty.
	overall := "RUNNING"
	startedAt := ""
	if store != nil {
		if m, ok, _ := readRunMarker(ctx, store, runID); ok && m != nil {
			if m.Status != "" {
				overall = m.Status
			}
			if m.StartedMs != nil {
				startedAt = formatMillis(*m.StartedMs)
			}
		}
	}
	return &ExecutionStatesResult{
		Status:    overall,
		States:    states,
		RunID:     runID,
		StartedAt: startedAt,
	}, nil
}

// progressStore builds an S3-backed ProgressStore from the provider's S3
// client + warehouse bucket. Returns nil when neither is wired (a provider
// constructed without WithS3, or an undeployed workspace) — progressStates
// and readRunMarker both tolerate a nil store.
func (c *CloudProvider) progressStore() ProgressStore {
	if c.s3 == nil || c.athenaOutputBucket == "" {
		return nil
	}
	return NewS3ProgressStore(c.s3, c.athenaOutputBucket)
}

// RunDetail reads one run's `_run.json` run marker from the S3 warehouse and
// projects its failure context. Found=false when no marker exists (a
// fully-cloud SFN run carries its failure context in SFN history, not a run
// marker; cloud-local runs do write the marker). Backs the
// GET /pipeline/execution detail endpoint.
func (c *CloudProvider) RunDetail(ctx context.Context, run string) (RunDetail, error) {
	store := c.progressStore()
	if store == nil {
		return RunDetail{}, nil
	}
	m, ok, err := readRunMarker(ctx, store, run)
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

// progressSnapshot is the JSON the runner writes to
// _progress/<execARN>/<node>.json each poll tick. Fields mirror the
// StateStatus progress counters; all optional. UpdatedMs is the epoch
// millisecond the runner stamped the snapshot, used to filter out stale
// objects left behind by a previous node or a crashed run.
type progressSnapshot struct {
	StagesTotal     *int64          `json:"stages_total"`
	StagesCompleted *int64          `json:"stages_completed"`
	TasksTotal      *int64          `json:"tasks_total"`
	TasksCompleted  *int64          `json:"tasks_completed"`
	TasksFailed     *int64          `json:"tasks_failed"`
	UpdatedMs       int64           `json:"updated_ms"`
	Status          string          `json:"status"`
	StartedMs       *int64          `json:"started_ms"`
	EndedMs         *int64          `json:"ended_ms"`
	// OutputRows is the runner-reported added-records sum across this node's
	// Delta outputs, stamped on the terminal marker. Nil while running and
	// for path-mode-only / skipped nodes. The local node-runs fast path
	// (nodeRunsFromProgress) carries it onto the NodeRun row so the dashboard
	// drawer reads it without a Spark roundtrip.
	OutputRows *int64          `json:"output_rows"`
	Error      string          `json:"error"`
	Metrics    json.RawMessage `json:"metrics"`
}

// freshnessWindowMs bounds how recent a `_progress/<node>.json` snapshot
// must be (relative to now) to count as "currently in flight". The runner
// rewrites the object every few seconds while a node runs; an object older
// than this window belongs to a node that already finished (or a stale run)
// and is dropped so the UI doesn't show a ghost progress bar.
const freshnessWindowMs = 12000

// liveProgressStates LISTs the per-node `_progress/<execARN>/<node>.json`
// markers via the S3-backed ProgressStore and delegates to the shared
// progressStates helper — the same read path the local provider uses
// (ADR-014). Kept as a thin method for the cloud-specific tests that drive
// it directly; production code goes through ExecutionStates.
func (c *CloudProvider) liveProgressStates(ctx context.Context, execARN string, now time.Time) map[string]StateStatus {
	return progressStates(ctx, c.progressStore(), execARN, now.UnixMilli())
}

// ---------------------------------------------------------------------------
// ExecutionLogs
// ---------------------------------------------------------------------------

func (c *CloudProvider) ExecutionLogs(ctx context.Context, q ExecutionLogsQuery) (*ExecutionLogsResult, error) {
	if c.sfn == nil {
		return nil, fmt.Errorf("cloud: sfn client not configured")
	}
	if c.cwl == nil {
		return nil, fmt.Errorf("cloud: cloudwatch logs client not configured")
	}
	if q.ExecutionRef == "" || q.Step == "" {
		return nil, fmt.Errorf("execution ref and step are required")
	}

	smName := StateMachineNameFromExecutionARN(q.ExecutionRef)
	if smName == "" {
		// Not a real SFN execution ARN (undeployed workspace, or a
		// dir-mode ref) — no logs to fetch. Empty, not a 500.
		return &ExecutionLogsResult{Source: LogSourceCloudWatch, Events: []LogEvent{}}, nil
	}
	pipelineName := PipelineNameFromStateMachineName(smName)
	functionName := pipelineName + "-" + q.Step
	logGroup := "/aws/lambda/" + functionName

	hist, err := c.sfn.GetExecutionHistory(ctx, &sfn.GetExecutionHistoryInput{
		ExecutionArn:         aws.String(q.ExecutionRef),
		IncludeExecutionData: aws.Bool(false),
	})
	if err != nil {
		return nil, fmt.Errorf("get history: %w", err)
	}

	startT, endT := StepTimeWindow(hist.Events, q.Step)
	if startT.IsZero() {
		// Step never entered — empty log set, not an error.
		return &ExecutionLogsResult{
			Source:       LogSourceCloudWatch,
			LogGroup:     logGroup,
			FunctionName: functionName,
			Events:       []LogEvent{},
		}, nil
	}
	if endT.IsZero() {
		endT = time.Now()
	}
	startMs := startT.Add(-2 * time.Second).UnixMilli()
	endMs := endT.Add(2 * time.Second).UnixMilli()

	out, err := c.cwl.FilterLogEvents(ctx, &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName: aws.String(logGroup),
		StartTime:    aws.Int64(startMs),
		EndTime:      aws.Int64(endMs),
		Limit:        aws.Int32(logsLimit),
	})
	if err != nil {
		return nil, fmt.Errorf("filter log events: %w", err)
	}

	events := make([]LogEvent, 0, len(out.Events))
	for _, ev := range out.Events {
		ts := ""
		if ev.Timestamp != nil {
			ts = time.UnixMilli(*ev.Timestamp).UTC().Format(time.RFC3339Nano)
		}
		events = append(events, LogEvent{
			Timestamp: ts,
			Message:   aws.ToString(ev.Message),
		})
	}

	return &ExecutionLogsResult{
		Source:       LogSourceCloudWatch,
		LogGroup:     logGroup,
		FunctionName: functionName,
		Events:       events,
		Truncated:    out.NextToken != nil,
	}, nil
}

// StateMachineNameFromExecutionARN extracts the state machine name from
// arn:aws:states:<region>:<acct>:execution:<sm-name>:<exec-id>. Returns ""
// when the shape doesn't match.
func StateMachineNameFromExecutionARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) < 8 || parts[5] != "execution" {
		return ""
	}
	return parts[6]
}

// PipelineNameFromStateMachineName strips the orchestration emitter's
// "clavesa-" prefix to recover the pipeline name. The runner Lambda is
// named "<pipelineName>-<step>", so we need this to construct the log group
// path.
func PipelineNameFromStateMachineName(sm string) string {
	const prefix = "clavesa-"
	if !strings.HasPrefix(sm, prefix) {
		return sm
	}
	return strings.TrimPrefix(sm, prefix)
}

// StepTimeWindow finds the [start, end] time window for the named state in
// the given execution history. Returns zero times if the state never entered.
// End is the latest TaskFailed / TaskStateExited timestamp; if the step is
// still running, end is left zero (caller pads with "now").
func StepTimeWindow(events []sfntypes.HistoryEvent, step string) (start, end time.Time) {
	currentState := ""
	for _, ev := range events {
		switch ev.Type {
		case sfntypes.HistoryEventTypeTaskStateEntered:
			if ev.StateEnteredEventDetails == nil {
				continue
			}
			name := aws.ToString(ev.StateEnteredEventDetails.Name)
			currentState = name
			if name == step && ev.Timestamp != nil {
				if start.IsZero() || ev.Timestamp.Before(start) {
					start = *ev.Timestamp
				}
			}
		case sfntypes.HistoryEventTypeTaskStateExited:
			if ev.StateExitedEventDetails == nil {
				continue
			}
			if aws.ToString(ev.StateExitedEventDetails.Name) == step && ev.Timestamp != nil {
				if ev.Timestamp.After(end) {
					end = *ev.Timestamp
				}
			}
		case sfntypes.HistoryEventTypeTaskFailed:
			if currentState == step && ev.Timestamp != nil {
				if ev.Timestamp.After(end) {
					end = *ev.Timestamp
				}
			}
		}
	}
	return start, end
}

// ---------------------------------------------------------------------------
// Athena query helpers
// ---------------------------------------------------------------------------

// runAthenaSelect runs one SELECT through runAthenaQuery and applies the
// shared result-set dance every Athena-backed read repeats: drop the header
// row Athena returns first, then truncate the data rows to limit (callers
// query LIMIT limit+1 so overflow signals truncation; limit < 0 disables
// truncation for full-result reads like ColumnStats). Returns the data rows,
// whether truncation happened, and the raw result set for callers that also
// read ResultSetMetadata (SampleTable / Query column info). Error mapping
// (isAthenaMissingTableErr → empty result) stays with each caller — the
// methods disagree on it deliberately.
func (c *CloudProvider) runAthenaSelect(ctx context.Context, sql string, limit int) ([]athenatypes.Row, bool, *athenatypes.ResultSet, error) {
	rs, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, sql)
	if err != nil {
		return nil, false, nil, err
	}
	rows := rs.Rows
	if len(rows) > 0 {
		rows = rows[1:] // drop Athena header row
	}
	truncated := false
	if limit >= 0 && len(rows) > limit {
		rows = rows[:limit]
		truncated = true
	}
	return rows, truncated, rs, nil
}

// runAthenaQuery starts a query, polls for completion, and returns the result
// set. Shared across NodeRuns/Runs/Snapshots.
func runAthenaQuery(ctx context.Context, ac AthenaClient, outputBucket, sql string) (*athenatypes.ResultSet, error) {
	startOut, err := ac.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		ResultConfiguration: &athenatypes.ResultConfiguration{
			OutputLocation: aws.String("s3://" + outputBucket + "/athena-results/"),
		},
		// Reuse a cached result for an identical query within the window
		// instead of re-scanning. The observability tables (runs, node_runs,
		// snapshots) are append-mostly and the UI polls the same queries
		// repeatedly across the catalog, pipelines, and dashboard pages — so
		// a short reuse window collapses the per-query Athena cold-start
		// (seconds each) to a near-instant cache hit on repeat loads. A run
		// completing invalidates naturally once the window lapses; the live
		// status channel (S3 _progress) carries in-flight state regardless.
		ResultReuseConfiguration: &athenatypes.ResultReuseConfiguration{
			ResultReuseByAgeConfiguration: &athenatypes.ResultReuseByAgeConfiguration{
				Enabled:         true,
				MaxAgeInMinutes: aws.Int32(athenaResultReuseMinutes),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("StartQueryExecution: %w", err)
	}
	queryID := aws.ToString(startOut.QueryExecutionId)

	for attempt := 0; attempt < athenaMaxPollAttempts; attempt++ {
		execOut, err := ac.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return nil, fmt.Errorf("GetQueryExecution: %w", err)
		}
		state := execOut.QueryExecution.Status.State
		switch state {
		case athenatypes.QueryExecutionStateSucceeded:
			resOut, err := ac.GetQueryResults(ctx, &athena.GetQueryResultsInput{
				QueryExecutionId: aws.String(queryID),
			})
			if err != nil {
				return nil, fmt.Errorf("GetQueryResults: %w", err)
			}
			if resOut.ResultSet == nil {
				return &athenatypes.ResultSet{}, nil
			}
			return resOut.ResultSet, nil
		case athenatypes.QueryExecutionStateFailed, athenatypes.QueryExecutionStateCancelled:
			reason := ""
			if execOut.QueryExecution.Status.StateChangeReason != nil {
				reason = aws.ToString(execOut.QueryExecution.Status.StateChangeReason)
			}
			return nil, fmt.Errorf("Athena query %s: %s", state, reason)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(athenaPollInterval):
		}
	}
	return nil, fmt.Errorf("Athena query %s timed out after %d polls", queryID, athenaMaxPollAttempts)
}

func stringDatum(d athenatypes.Datum) string {
	if d.VarCharValue == nil {
		return ""
	}
	return aws.ToString(d.VarCharValue)
}

func intDatum(d athenatypes.Datum) *int64 {
	if d.VarCharValue == nil {
		return nil
	}
	s := strings.TrimSpace(aws.ToString(d.VarCharValue))
	if s == "" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

// floatDatum parses a numeric Athena varchar into *float64. Same nil
// semantics as intDatum — unparseable or empty values surface as nil
// so JSON omits the field.
func floatDatum(d athenatypes.Datum) *float64 {
	if d.VarCharValue == nil {
		return nil
	}
	s := strings.TrimSpace(aws.ToString(d.VarCharValue))
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

// boolDatum parses an Athena "true"/"false" varchar into a *bool. Empty or
// unrecognized values return nil so the JSON omits the field.
func boolDatum(d athenatypes.Datum) *bool {
	s := stringDatum(d)
	switch s {
	case "true":
		t := true
		return &t
	case "false":
		f := false
		return &f
	default:
		return nil
	}
}

// FormatTime renders an optional AWS SDK timestamp as RFC3339 UTC, or ""
// when nil. Shared with the pipelinestatus handler (it renders the same
// SFN timestamps) so the wire format cannot drift between the two.
func FormatTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
