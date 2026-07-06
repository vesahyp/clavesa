package observability

import "context"

// Provider is the seam between the HTTP layer and the observability
// backends, selected per workspace warehouse (ADR-024). CloudProvider talks
// to Athena/SFN/CloudWatch plus the S3 warehouse's `_progress` marker tree;
// LocalProvider reads filesystem Delta transaction logs plus the local
// warehouse's `_progress` tree. Response shapes match — the UI cannot tell
// which backend served a request.
//
// Methods may return an empty result with nil error when the underlying table
// does not exist yet (e.g. fresh pipeline that has never run). The UI renders
// that as "no runs yet" rather than an error.
type Provider interface {
	NodeRuns(ctx context.Context, q NodeRunsQuery) (*NodeRunsResult, error)
	Runs(ctx context.Context, q RunsQuery) (*RunsResult, error)
	Tables(ctx context.Context, q TablesQuery) (*TablesResult, error)
	Snapshots(ctx context.Context, q SnapshotsQuery) (*SnapshotsResult, error)
	ColumnStats(ctx context.Context, q ColumnStatsQuery) (*ColumnStatsResult, error)
	SampleTable(ctx context.Context, q SampleTableQuery) (*SampleTableResult, error)
	Query(ctx context.Context, q QueryQuery) (*QueryResult, error)
	// Exec runs a write (DDL or DML) against the warehouse. Unlike the
	// read methods it surfaces backend errors directly — a failed write
	// must not look like an empty result. Cloud runs it through Athena;
	// local through the runner's SQL path (a DML statement returns no
	// rows there, so query mode doubles as the exec path — no separate
	// runner mode needed).
	Exec(ctx context.Context, q ExecQuery) error
	ExecutionStates(ctx context.Context, q ExecutionStatesQuery) (*ExecutionStatesResult, error)
	ExecutionLogs(ctx context.Context, q ExecutionLogsQuery) (*ExecutionLogsResult, error)
}
