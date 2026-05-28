package dataquery

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

// validFormats is the set of accepted source file format values.
var validFormats = map[string]bool{
	"csv":     true,
	"json":    true,
	"parquet": true,
}

// Handler is the data-query HTTP surface. Constructed via NewHandler; the
// per-pipeline observability resolver is wired separately via WithResolver
// so existing tests continue to compile against the original signature.
type Handler struct {
	mux      *http.ServeMux
	cloud    observability.Provider
	resolver *observability.Resolver
}

// NewHandler returns a Handler that serves:
//
//	GET /data/source                                — read sample rows from an S3 object
//	GET /data/table                                 — query an Iceberg table via Athena
//	GET /data/tables/{database}/{table}/snapshots   — Iceberg snapshot history (freshness/rowcount)
//	GET /data/node-runs                             — typed query of <pipeline>.node_runs
//	GET /data/runs                                  — typed query of <pipeline>.runs (per-execution rollup)
//
// The observability endpoints (snapshots, node-runs, runs) delegate to a
// CloudProvider so the local provider can implement the same shapes; ADR-014.
// Pass a Resolver via WithResolver to enable per-request cloud/local dispatch
// based on the `dir` query param.
func NewHandler(s3Client S3Client, athenaClient observability.AthenaClient, athenaOutputBucket string) http.Handler {
	h := &Handler{
		cloud: observability.NewCloudProvider(athenaClient, athenaOutputBucket, nil, nil),
	}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /data/source", func(w http.ResponseWriter, r *http.Request) {
		handleSource(w, r, s3Client)
	})

	mux.HandleFunc("GET /data/table", func(w http.ResponseWriter, r *http.Request) {
		p, ok := h.providerFor(w, r)
		if !ok {
			return
		}
		handleTable(w, r, p)
	})

	mux.HandleFunc("GET /data/tables/{database}/{table}/snapshots", func(w http.ResponseWriter, r *http.Request) {
		p, ok := h.providerFor(w, r)
		if !ok {
			return
		}
		handleSnapshots(w, r, p)
	})

	mux.HandleFunc("GET /data/tables/{database}/{table}/column-stats", func(w http.ResponseWriter, r *http.Request) {
		p, ok := h.providerFor(w, r)
		if !ok {
			return
		}
		handleColumnStats(w, r, p, h.systemGlueDBFor(r))
	})

	mux.HandleFunc("GET /data/node-runs", func(w http.ResponseWriter, r *http.Request) {
		p, ok := h.providerFor(w, r)
		if !ok {
			return
		}
		handleNodeRuns(w, r, p, h.systemGlueDBFor(r))
	})

	mux.HandleFunc("GET /data/runs", func(w http.ResponseWriter, r *http.Request) {
		p, ok := h.providerFor(w, r)
		if !ok {
			return
		}
		handleRuns(w, r, p, h.systemGlueDBFor(r))
	})
	mux.HandleFunc("GET /data/tables-state", func(w http.ResponseWriter, r *http.Request) {
		p, ok := h.providerFor(w, r)
		if !ok {
			return
		}
		handleTables(w, r, p, h.systemGlueDBFor(r))
	})

	// POST /data/query — workspace-level ad-hoc SQL editor. Body shape:
	//   { "sql": "SELECT …", "dir": "<pipeline-dir>" }
	// `dir` scopes the provider dispatch (local-warehouse vs Athena);
	// any pipeline dir in the workspace will route to the same local
	// Hadoop catalog so the UI just picks the first pipeline it knows
	// about. The /query page and the per-table SQL pane on
	// /tables/:db/:table both call this endpoint.
	mux.HandleFunc("POST /data/query", func(w http.ResponseWriter, r *http.Request) {
		p, ok := h.providerFor(w, r)
		if !ok {
			return
		}
		handleAdhocQuery(w, r, p)
	})

	h.mux = mux
	return h
}

// ServeHTTP makes Handler an http.Handler. Routing is delegated to the
// embedded mux; the wrapper exists only so WithResolver can mutate state on
// the same instance the cli/ui.go owns.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// WithResolver enables per-request cloud/local provider dispatch on the
// observability endpoints. When the request carries `dir=…`, the resolver
// inspects that pipeline's compute attr; without `dir`, the cloud path
// (Athena over Glue) is used unchanged.
func (h *Handler) WithResolver(r *observability.Resolver) *Handler {
	h.resolver = r
	return h
}

// providerFor picks the provider for one request. Returns (provider, true)
// when routing succeeded; on failure writes a 400 to w and returns
// (nil, false) so the caller can early-out.
//
// In local mode the cloud provider has a nil Athena client — silently
// falling back there 500s the request when ?dir is missing (B P1-3 from
// 2026-05-24). Local mode now requires `dir` and surfaces a 400 instead.
// Tests without a resolver still get the legacy cloud provider.
func (h *Handler) providerFor(w http.ResponseWriter, r *http.Request) (observability.Provider, bool) {
	if h.resolver == nil {
		return h.cloud, true
	}
	dir := strings.TrimSpace(r.URL.Query().Get("dir"))
	if dir == "" {
		if h.resolver.IsLocal() {
			httputil.WriteError(w, http.StatusBadRequest, "missing required query param: dir (local workspaces dispatch per-pipeline)")
			return nil, false
		}
		return h.cloud, true
	}
	p, err := h.resolver.For(dir)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	return p, true
}

// glueDBFor resolves the encoded Glue DB / Iceberg namespace name for
// the pipeline at `?dir=…`, mirroring the runner's `_glue_db()` and
// `internal/identutil.EncodeGlueDatabase`. Empty when no resolver is
// configured or no `dir` was supplied — observability handlers fall
// back to the legacy `clavesa_<PipelineName>` form in that case.
func (h *Handler) glueDBFor(r *http.Request) string {
	if h.resolver == nil {
		return ""
	}
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		return ""
	}
	return h.resolver.GlueDBFor(dir)
}

// systemGlueDBFor resolves the workspace's observability DB
// (`<system_catalog>__pipelines`, ADR-016 v0.20.0). Observability
// handlers — node-runs, runs, tables-state — read against this DB
// regardless of which pipeline is being inspected, and filter by the
// `pipeline` column on each row. Empty when no resolver is configured.
func (h *Handler) systemGlueDBFor(r *http.Request) string {
	if h.resolver == nil {
		return ""
	}
	return h.resolver.SystemGlueDB()
}

// handleSource serves GET /data/source?bucket=<b>&prefix=<p>&format=<f>&limit=<n>.
func handleSource(w http.ResponseWriter, r *http.Request, s3c S3Client) {
	q := r.URL.Query()

	bucket := q.Get("bucket")
	if bucket == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: bucket")
		return
	}

	format := q.Get("format")
	if !validFormats[format] {
		httputil.WriteError(w, http.StatusBadRequest, "format must be one of: csv, json, parquet")
		return
	}

	prefix := q.Get("prefix")
	jsonPath := q.Get("json_path")
	limit, ok := parseLimit(q.Get("limit"))
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, "limit must be a positive integer ≤ 1000")
		return
	}

	result, err := readSource(r.Context(), s3c, bucket, prefix, format, jsonPath, limit)
	if err != nil {
		var nfe *notFoundError
		if errors.As(err, &nfe) {
			httputil.WriteError(w, http.StatusNotFound, nfe.Error())
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	httputil.WriteJSON(w, http.StatusOK, result)
}

// handleTable serves GET /data/table?catalog_db=<db>&catalog_table=<t>&limit=<n>[&dir=<d>].
//
// ADR-020: optionally accepts `catalog` + `schema` query params instead of
// `catalog_db`; when both are present they compose into the wire form
// `<catalog>__<schema>` the provider expects. `catalog_db` wins when also
// supplied so legacy callers keep working byte-for-byte.
//
// Dispatches through observability.Provider so local pipelines (compute =
// "local") query their per-pipeline Hadoop catalog instead of falling back
// to Athena, which has nothing to read from. Cloud pipelines unchanged
// (Athena over Glue). Same response shape from both providers.
func handleTable(w http.ResponseWriter, r *http.Request, p observability.Provider) {
	q := r.URL.Query()

	catalogDB := composeDatabase(q.Get("catalog_db"), q.Get("catalog"), q.Get("schema"))
	if catalogDB == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: catalog_db (or catalog+schema)")
		return
	}

	catalogTable := q.Get("catalog_table")
	if catalogTable == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: catalog_table")
		return
	}

	limit, ok := parseLimit(q.Get("limit"))
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, "limit must be a positive integer ≤ 1000")
		return
	}

	res, err := p.SampleTable(r.Context(), observability.SampleTableQuery{
		Database:    catalogDB,
		Table:       catalogTable,
		PipelineDir: q.Get("dir"),
		Limit:       limit,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert to the legacy QueryResult shape so the existing UI hook
	// (TableQueryResult in queries.ts) keeps parsing without a change.
	result := &QueryResult{
		Columns:   make([]graph.Column, 0, len(res.Columns)),
		Rows:      res.Rows,
		RowCount:  res.RowCount,
		Truncated: res.Truncated,
	}
	for _, c := range res.Columns {
		result.Columns = append(result.Columns, graph.Column{
			Name:     c.Name,
			Type:     c.Type,
			Nullable: true,
		})
	}
	httputil.WriteJSON(w, http.StatusOK, result)
}

// handleAdhocQuery serves POST /data/query?dir=<pipeline-dir> with body
// {"sql": "..."}. Executes free-form SparkSQL through the resolved provider
// (local Hadoop catalog or Athena) and returns the legacy QueryResult shape
// so the UI's existing data-grid renderer parses without changes.
//
// `dir` selects the provider (compute=local vs cloud); the SQL itself is
// against the workspace catalog. Any pipeline dir in the workspace routes
// to the same local warehouse, so the UI sends whichever pipeline dir it
// can see — the /query page picks the first one it knows about.
func handleAdhocQuery(w http.ResponseWriter, r *http.Request, p observability.Provider) {
	var body struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}
	sql := strings.TrimSpace(body.SQL)
	if sql == "" {
		httputil.WriteError(w, http.StatusBadRequest, "sql is required")
		return
	}

	res, err := p.Query(r.Context(), observability.QueryQuery{
		SQL:         sql,
		PipelineDir: r.URL.Query().Get("dir"),
		MaxRows:     1000,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Same legacy QueryResult shape /data/table returns — the UI grid
	// parses both with the same hook.
	out := &QueryResult{
		Columns:   make([]graph.Column, 0, len(res.Columns)),
		Rows:      res.Rows,
		RowCount:  res.RowCount,
		Truncated: res.Truncated,
	}
	for _, c := range res.Columns {
		out.Columns = append(out.Columns, graph.Column{
			Name:     c.Name,
			Type:     c.Type,
			Nullable: true,
		})
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

// parseLimit parses the limit query parameter. Returns (defaultLimit, true) if
// the string is empty, and (0, false) if the value is invalid or exceeds the max.
func parseLimit(s string) (int, bool) {
	if s == "" {
		return defaultLimit, true
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 || n > maxLimit {
		return 0, false
	}
	return n, true
}

// snapshotsDefaultLimit / snapshotsMaxLimit cap how many Iceberg snapshots
// /data/tables/.../snapshots will return per call. Snapshot rows are tiny,
// but we still cap to keep Athena bytes-scanned predictable.
const (
	snapshotsDefaultLimit = 20
	snapshotsMaxLimit     = 200
)

// handleSnapshots serves GET /data/tables/{database}/{table}/snapshots[?limit=N].
//
// ADR-020: accepts optional `catalog` + `schema` query params; the path
// `{database}` segment wins when supplied (always, for this route), so
// the params are effectively decorative here. They exist so callers can
// pass the three-piece form uniformly without case-splitting on the
// endpoint.
func handleSnapshots(w http.ResponseWriter, r *http.Request, p observability.Provider) {
	db := r.PathValue("database")
	if db == "" {
		db = composeDatabase("", r.URL.Query().Get("catalog"), r.URL.Query().Get("schema"))
	}
	tbl := r.PathValue("table")
	if db == "" || tbl == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing database or table path segment")
		return
	}
	if !observability.IsValidIdentifier(db) || !observability.IsValidIdentifier(tbl) {
		httputil.WriteError(w, http.StatusBadRequest, "database and table must match [A-Za-z_][A-Za-z0-9_]*")
		return
	}

	limit := snapshotsDefaultLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 || n > snapshotsMaxLimit {
			httputil.WriteError(w, http.StatusBadRequest, "limit must be a positive integer ≤ 200")
			return
		}
		limit = n
	}

	res, err := p.Snapshots(r.Context(), observability.SnapshotsQuery{
		Database:    db,
		Table:       tbl,
		PipelineDir: r.URL.Query().Get("dir"),
		Limit:       limit,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

const (
	nodeRunsDefaultLimit = 50
	nodeRunsMaxLimit     = 500
)

// handleColumnStats serves GET /data/tables/{database}/{table}/column-stats[?dir=…].
//
// Reads opt-in per-column stats for the latest snapshot of one Iceberg
// table. The system DB (`<system_catalog>__pipelines`) is supplied by
// the Resolver; without it the provider returns an empty result so a
// pre-resolver caller renders cleanly instead of 500ing.
func handleColumnStats(w http.ResponseWriter, r *http.Request, p observability.Provider, systemDatabase string) {
	db := r.PathValue("database")
	if db == "" {
		db = composeDatabase("", r.URL.Query().Get("catalog"), r.URL.Query().Get("schema"))
	}
	tbl := r.PathValue("table")
	if db == "" || tbl == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing database or table path segment")
		return
	}
	if !observability.IsValidIdentifier(db) || !observability.IsValidIdentifier(tbl) {
		httputil.WriteError(w, http.StatusBadRequest, "database and table must match [A-Za-z_][A-Za-z0-9_]*")
		return
	}

	// ADR-018: Delta tables use the two-part `<db>.<table>` form under
	// spark_catalog. The runner writes `table_identifier = <db>.<table>`
	// into column_stats — prefixing with the legacy `clavesa.` Iceberg
	// catalog name here made the filter never match.
	tableIdentifier := db + "." + tbl
	res, err := p.ColumnStats(r.Context(), observability.ColumnStatsQuery{
		Database:        systemDatabase,
		TableIdentifier: tableIdentifier,
		PipelineDir:     r.URL.Query().Get("dir"),
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

// handleNodeRuns serves GET /data/node-runs?pipeline=<name>[&node=<name>][&limit=N].
//
// Returns the most recent N invocations of the named pipeline (optionally
// filtered to one node) from the runner-populated node_runs table. Empty or
// non-existent tables surface as an empty rows array — the UI treats that as
// "no runs yet" rather than an error.
func handleNodeRuns(w http.ResponseWriter, r *http.Request, p observability.Provider, database string) {
	pipeline := r.URL.Query().Get("pipeline")
	if pipeline == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: pipeline")
		return
	}
	if !observability.IsValidPipelineName(pipeline) {
		httputil.WriteError(w, http.StatusBadRequest, "pipeline must match [A-Za-z_][A-Za-z0-9_-]*")
		return
	}

	node := r.URL.Query().Get("node")
	if node != "" && !observability.IsValidIdentifier(node) {
		httputil.WriteError(w, http.StatusBadRequest, "node must match [A-Za-z_][A-Za-z0-9_]*")
		return
	}

	// arn filters to one execution by sf_execution_arn — the join key
	// against runs.sf_execution_arn for both cloud (full SFN ARN) and
	// local (pipeline-run uuid threaded as _sf_execution_arn). Validated
	// against an ARN-or-hex charset so the value can be safely
	// literal-substituted into SQL after escape.
	execARN := r.URL.Query().Get("arn")
	if execARN != "" && !isValidExecARN(execARN) {
		httputil.WriteError(w, http.StatusBadRequest, "arn must contain only [A-Za-z0-9_:./-], max 256 chars")
		return
	}

	limit := nodeRunsDefaultLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 || n > nodeRunsMaxLimit {
			httputil.WriteError(w, http.StatusBadRequest, "limit must be a positive integer ≤ 500")
			return
		}
		limit = n
	}

	res, err := p.NodeRuns(r.Context(), observability.NodeRunsQuery{
		PipelineName:   pipeline,
		Database:       database,
		PipelineDir:    r.URL.Query().Get("dir"),
		Node:           node,
		SfExecutionARN: execARN,
		Limit:          limit,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

const (
	runsDefaultLimit = 50
	runsMaxLimit     = 500
)

// handleRuns serves GET /data/runs?pipeline=<name>[&limit=N].
//
// Pairs with /data/node-runs — node_runs has one row per Lambda invocation,
// runs has one row per Step Functions execution. Joining on sf_execution_arn
// answers "which nodes ran in this execution?".
func handleRuns(w http.ResponseWriter, r *http.Request, p observability.Provider, database string) {
	pipeline := r.URL.Query().Get("pipeline")
	if pipeline == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: pipeline")
		return
	}
	if !observability.IsValidPipelineName(pipeline) {
		httputil.WriteError(w, http.StatusBadRequest, "pipeline must match [A-Za-z_][A-Za-z0-9_-]*")
		return
	}

	limit := runsDefaultLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 || n > runsMaxLimit {
			httputil.WriteError(w, http.StatusBadRequest, "limit must be a positive integer ≤ 500")
			return
		}
		limit = n
	}

	res, err := p.Runs(r.Context(), observability.RunsQuery{
		PipelineName: pipeline,
		Database:     database,
		PipelineDir:  r.URL.Query().Get("dir"),
		Limit:        limit,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

// handleTables serves GET /data/tables-state?pipeline=<name>[&dir=<dir>][&limit=N].
//
// Returns one row per Iceberg-output table with the latest snapshot's row /
// file count, byte size, and refresh time. Distinct from
// /data/tables/{db}/{table}/snapshots, which lists snapshot history for one
// specific table.
func handleTables(w http.ResponseWriter, r *http.Request, p observability.Provider, database string) {
	pipeline := r.URL.Query().Get("pipeline")
	if pipeline == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing required query param: pipeline")
		return
	}
	if !observability.IsValidPipelineName(pipeline) {
		httputil.WriteError(w, http.StatusBadRequest, "pipeline must match [A-Za-z_][A-Za-z0-9_-]*")
		return
	}

	limit := runsDefaultLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 || n > runsMaxLimit {
			httputil.WriteError(w, http.StatusBadRequest, "limit must be a positive integer ≤ 500")
			return
		}
		limit = n
	}

	res, err := p.Tables(r.Context(), observability.TablesQuery{
		PipelineName: pipeline,
		Database:     database,
		PipelineDir:  r.URL.Query().Get("dir"),
		Limit:        limit,
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, res)
}

// composeDatabase resolves the wire-form `<catalog>__<schema>` Glue
// database name from either the legacy single `database`/`catalog_db`
// param or the ADR-020 three-piece `catalog`+`schema` pair. The legacy
// form wins so byte-for-byte old callers keep working; the three-piece
// form composes via the same encoder the runner uses (ADR-016).
func composeDatabase(legacy, catalog, schema string) string {
	if legacy != "" {
		return legacy
	}
	if catalog != "" && schema != "" {
		return catalog + "__" + schema
	}
	return ""
}

// isValidExecARN checks that `s` only contains characters that occur in SFN
// execution ARNs ([A-Za-z0-9_:./-]) plus our local-uuid hex chars. Capped at
// 256 chars (longest reasonable SFN ARN is well under this). Run-level join
// values come either from `runs.sf_execution_arn` (full ARN in cloud, uuid
// in local) or from typed user input on debug surfaces; reject anything
// outside the allow-set so callers can SQL-literal-substitute safely.
func isValidExecARN(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for _, c := range s {
		ok := (c >= '0' && c <= '9') ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			c == '_' || c == ':' || c == '.' || c == '/' || c == '-'
		if !ok {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Sentinel error types
// ---------------------------------------------------------------------------

// notFoundError signals that the requested resource does not exist.
type notFoundError struct{ msg string }

func (e *notFoundError) Error() string { return e.msg }

func errNotFound(msg string) error { return &notFoundError{msg: msg} }

// badRequestError signals that the caller supplied invalid parameters.
type badRequestError struct{ msg string }

func (e *badRequestError) Error() string { return e.msg }

func errBadRequest(msg string) error { return &badRequestError{msg: msg} }
