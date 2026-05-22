package observability

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
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

// CloudProvider satisfies Provider for deployed pipelines: Athena over Iceberg
// metadata for run history, SFN for execution state, CloudWatch for logs.
type CloudProvider struct {
	athena             AthenaClient
	athenaOutputBucket string
	sfn                SFNClient
	cwl                CWLClient
}

// NewCloudProvider wires a provider against AWS SDK clients. Any subset of
// clients may be nil; methods that require an unset client return a typed
// error rather than panicking.
func NewCloudProvider(athenaC AthenaClient, athenaOutputBucket string, sfnC SFNClient, cwlC CWLClient) *CloudProvider {
	return &CloudProvider{
		athena:             athenaC,
		athenaOutputBucket: athenaOutputBucket,
		sfn:                sfnC,
		cwl:                cwlC,
	}
}

// undeployed reports whether the workspace has no deployed Athena
// warehouse — the auto-derived results bucket is empty because there is
// no `pipeline_bucket` output in terraform.tfstate yet. Athena-backed
// reads short-circuit to an empty result in that case: switching a
// not-yet-deployed workspace to cloud mode is a valid empty state, not
// a 500 (TODO bucket 16).
func (c *CloudProvider) undeployed() bool { return c.athenaOutputBucket == "" }

const (
	athenaMaxPollAttempts = 60
	athenaPollInterval    = 500 * time.Millisecond
	logsLimit             = 500
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

	dbName := q.Database
	if dbName == "" {
		// Defensive fallback for handler-without-resolver mode (tests
		// and bare /data/runs?pipeline=foo curls without a `dir` param).
		// Production code always wires a Resolver and sets Database via
		// it. The fallback uses today's `clavesa_<PipelineName>`
		// shape — a no-op for v0.17 schemas, broken for post-migration
		// workspaces (whose DB names start `<catalog>__`). Tests pass a
		// pipeline name that matches an `clavesa_<pipeline>` DB; real
		// callers should pass `dir` so the resolver can compute the
		// encoded DB.
		dbName = "clavesa_" + q.PipelineName
	}
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
  sf_execution_arn
FROM "%s"."node_runs"
%s
ORDER BY started_at DESC
LIMIT %d`, dbName, whereClause, q.Limit+1)

	rs, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, sql)
	if err != nil {
		return nil, err
	}

	rows := rs.Rows
	if len(rows) > 0 {
		rows = rows[1:] // drop Athena header row
	}
	truncated := false
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		truncated = true
	}

	out := make([]NodeRun, 0, len(rows))
	for _, row := range rows {
		if len(row.Data) < 17 {
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

	dbName := q.Database
	if dbName == "" {
		// Defensive fallback for handler-without-resolver mode (tests
		// and bare /data/runs?pipeline=foo curls without a `dir` param).
		// Production code always wires a Resolver and sets Database via
		// it. The fallback uses today's `clavesa_<PipelineName>`
		// shape — a no-op for v0.17 schemas, broken for post-migration
		// workspaces (whose DB names start `<catalog>__`). Tests pass a
		// pipeline name that matches an `clavesa_<pipeline>` DB; real
		// callers should pass `dir` so the resolver can compute the
		// encoded DB.
		dbName = "clavesa_" + q.PipelineName
	}
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

	rs, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, sql)
	if err != nil {
		return nil, err
	}

	rows := rs.Rows
	if len(rows) > 0 {
		rows = rows[1:]
	}
	truncated := false
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		truncated = true
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

	dbName := q.Database
	if dbName == "" {
		// Defensive fallback for handler-without-resolver mode (tests
		// and bare /data/runs?pipeline=foo curls without a `dir` param).
		// Production code always wires a Resolver and sets Database via
		// it. The fallback uses today's `clavesa_<PipelineName>`
		// shape — a no-op for v0.17 schemas, broken for post-migration
		// workspaces (whose DB names start `<catalog>__`). Tests pass a
		// pipeline name that matches an `clavesa_<pipeline>` DB; real
		// callers should pass `dir` so the resolver can compute the
		// encoded DB.
		dbName = "clavesa_" + q.PipelineName
	}
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

	rs, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, sql)
	if err != nil {
		return nil, err
	}

	rows := rs.Rows
	if len(rows) > 0 {
		rows = rows[1:]
	}
	truncated := false
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		truncated = true
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

func (c *CloudProvider) Snapshots(ctx context.Context, q SnapshotsQuery) (*SnapshotsResult, error) {
	if c.undeployed() {
		return &SnapshotsResult{Snapshots: []SnapshotInfo{}}, nil
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

	sql := fmt.Sprintf(
		`SELECT
  CAST(snapshot_id AS varchar) AS snapshot_id,
  CAST(parent_id AS varchar) AS parent_id,
  to_iso8601(committed_at) AS committed_at,
  operation,
  summary['added-records'] AS added_records,
  summary['deleted-records'] AS deleted_records,
  summary['total-records'] AS total_records,
  summary['clavesa.trigger'] AS trigger,
  summary['clavesa.run-id'] AS writer_run_id
FROM "%s"."%s$snapshots"
ORDER BY committed_at DESC
LIMIT %d`, q.Database, q.Table, q.Limit+1)

	rs, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, sql)
	if err != nil {
		return nil, err
	}

	rows := rs.Rows
	if len(rows) > 0 {
		rows = rows[1:]
	}
	truncated := false
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		truncated = true
	}

	out := make([]SnapshotInfo, 0, len(rows))
	for _, row := range rows {
		if len(row.Data) < 9 {
			continue
		}
		out = append(out, SnapshotInfo{
			SnapshotID:     stringDatum(row.Data[0]),
			ParentID:       stringDatum(row.Data[1]),
			CommittedAt:    stringDatum(row.Data[2]),
			Operation:      stringDatum(row.Data[3]),
			AddedRecords:   intDatum(row.Data[4]),
			DeletedRecords: intDatum(row.Data[5]),
			TotalRecords:   intDatum(row.Data[6]),
			Trigger:        stringDatum(row.Data[7]),
			WriterRunID:    stringDatum(row.Data[8]),
		})
	}
	res := &SnapshotsResult{Snapshots: out, Truncated: truncated}
	if len(out) > 0 && out[0].TotalRecords != nil {
		v := *out[0].TotalRecords
		res.LatestRecordCount = &v
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// ColumnStats — opt-in per-column profile (latest snapshot per column)
// ---------------------------------------------------------------------------

// ColumnStats reads the latest-snapshot row per column from the workspace
// system column_stats Iceberg table. Empty result on undeployed
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

	rs, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, sql)
	if err != nil {
		// Athena returns SYNTAX_ERROR for "no such table" — surface as
		// empty so a workspace that's never run a stats-on transform
		// renders cleanly instead of flashing a 500.
		if strings.Contains(err.Error(), "Table") && strings.Contains(err.Error(), "not found") {
			return &ColumnStatsResult{Stats: []ColumnStat{}}, nil
		}
		return nil, err
	}

	rows := rs.Rows
	if len(rows) > 0 {
		rows = rows[1:]
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
		return &SampleTableResult{Columns: []SampleTableColumn{}, Rows: [][]string{}}, nil
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

	sql := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d", q.Database, q.Table, limit+1)
	rs, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, sql)
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

	// Athena returns the header row as the first row; skip it.
	dataRows := rs.Rows
	if len(dataRows) > 0 {
		dataRows = dataRows[1:]
	}
	truncated := false
	if len(dataRows) > limit {
		dataRows = dataRows[:limit]
		truncated = true
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
		return &QueryResult{Columns: []SampleTableColumn{}, Rows: [][]string{}}, nil
	}
	if c.athena == nil {
		return nil, fmt.Errorf("cloud: athena client not configured")
	}
	if q.SQL == "" {
		return nil, fmt.Errorf("query: sql is required")
	}

	rs, err := runAthenaQuery(ctx, c.athena, c.athenaOutputBucket, q.SQL)
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

	// Athena returns the header row first; skip it.
	dataRows := rs.Rows
	if len(dataRows) > 0 {
		dataRows = dataRows[1:]
	}
	truncated := false
	if q.MaxRows > 0 && len(dataRows) > q.MaxRows {
		dataRows = dataRows[:q.MaxRows]
		truncated = true
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
	// A ref that isn't a real SFN execution ARN — a bare `dir` (the
	// dashboard's dir-mode poll), or anything from a workspace with no
	// deployed state machine — has no execution to inspect. Empty
	// result, not a 500: switching an undeployed workspace to cloud
	// mode is a valid empty state (TODO bucket 16). Checked before the
	// client guard — a bare dir has no execution regardless of wiring.
	if StateMachineNameFromExecutionARN(q.ExecutionRef) == "" {
		return &ExecutionStatesResult{States: map[string]StateStatus{}}, nil
	}
	if c.sfn == nil {
		return nil, fmt.Errorf("cloud: sfn client not configured")
	}

	desc, err := c.sfn.DescribeExecution(ctx, &sfn.DescribeExecutionInput{
		ExecutionArn: aws.String(q.ExecutionRef),
	})
	if err != nil {
		return nil, fmt.Errorf("describe execution: %w", err)
	}
	hist, err := c.sfn.GetExecutionHistory(ctx, &sfn.GetExecutionHistoryInput{
		ExecutionArn:         aws.String(q.ExecutionRef),
		IncludeExecutionData: aws.Bool(false),
	})
	if err != nil {
		return nil, fmt.Errorf("get history: %w", err)
	}
	startedAt := ""
	if desc.StartDate != nil {
		startedAt = desc.StartDate.UTC().Format("2006-01-02T15:04:05.000Z")
	}
	return &ExecutionStatesResult{
		Status:    string(desc.Status),
		States:    StateStatusesFromHistory(hist.Events),
		RunID:     q.ExecutionRef,
		StartedAt: startedAt,
	}, nil
}

// StateStatusesFromHistory walks SFN history events in order and computes the
// latest known status for every state name observed. Exported because the
// pipelinestatus package's older /pipeline/execution detail endpoint also
// uses it during failure-step lookup.
//
// Rules (events arrive newest-last in default SFN ordering):
//   - TaskStateEntered: status[name] = RUNNING; record entered time.
//   - TaskStateExited:  status[name] = SUCCEEDED.
//   - TaskFailed:       status[currentState] = FAILED. Retries re-enter and
//     reset to RUNNING.
func StateStatusesFromHistory(events []sfntypes.HistoryEvent) map[string]StateStatus {
	out := make(map[string]StateStatus)
	currentState := ""

	for _, ev := range events {
		switch ev.Type {
		case sfntypes.HistoryEventTypeTaskStateEntered:
			if ev.StateEnteredEventDetails == nil {
				continue
			}
			name := derefStr(ev.StateEnteredEventDetails.Name)
			if name == "" {
				continue
			}
			currentState = name
			out[name] = StateStatus{
				Status:    "RUNNING",
				EnteredAt: formatTime(ev.Timestamp),
			}
		case sfntypes.HistoryEventTypeTaskStateExited:
			if ev.StateExitedEventDetails == nil {
				continue
			}
			name := derefStr(ev.StateExitedEventDetails.Name)
			if name == "" {
				continue
			}
			prev := out[name]
			out[name] = StateStatus{
				Status:    "SUCCEEDED",
				EnteredAt: prev.EnteredAt,
			}
		case sfntypes.HistoryEventTypeTaskFailed:
			if currentState == "" {
				continue
			}
			prev := out[currentState]
			out[currentState] = StateStatus{
				Status:    "FAILED",
				EnteredAt: prev.EnteredAt,
			}
		}
	}
	return out
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
			Message:   derefStr(ev.Message),
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
			name := derefStr(ev.StateEnteredEventDetails.Name)
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
			if derefStr(ev.StateExitedEventDetails.Name) == step && ev.Timestamp != nil {
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

// runAthenaQuery starts a query, polls for completion, and returns the result
// set. Shared across NodeRuns/Runs/Snapshots.
func runAthenaQuery(ctx context.Context, ac AthenaClient, outputBucket, sql string) (*athenatypes.ResultSet, error) {
	startOut, err := ac.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		ResultConfiguration: &athenatypes.ResultConfiguration{
			OutputLocation: aws.String("s3://" + outputBucket + "/athena-results/"),
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

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func formatTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
