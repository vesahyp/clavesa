package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vesahyp/clavesa/internal/delta"
	"github.com/vesahyp/clavesa/internal/delta/s3fs"
	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// GlueClient is the subset of the AWS Glue API used by the catalog handler.
type GlueClient interface {
	GetDatabases(ctx context.Context, params *glue.GetDatabasesInput, optFns ...func(*glue.Options)) (*glue.GetDatabasesOutput, error)
	GetTables(ctx context.Context, params *glue.GetTablesInput, optFns ...func(*glue.Options)) (*glue.GetTablesOutput, error)
}

// S3API is the subset of the AWS SDK v2 S3 client used to read Delta logs
// for column enrichment. Same two methods s3fs.S3API needs (the real
// *s3.Client satisfies both); narrow on purpose so the cloud-enrichment
// test stub stays small.
type S3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// catalogEnrichConcurrency bounds the worker pool that reads each cloud
// Delta table's `_delta_log/` from S3. The catalog lists every table up
// front, so serializing per-table round-trips would scale badly; a small
// fixed cap keeps the burst on S3 polite without leaving the page slow.
const catalogEnrichConcurrency = 8

// catalogSchemaCacheTTL bounds how long a Delta-log schema read is reused
// before being re-read from S3. The `_delta_log/` round-trip is the slow
// part of GET /workspace/tables (every request re-reads every cloud table,
// pushing the endpoint to ~14s with ~16 tables); a process-lifetime cache
// keyed by S3 location makes repeat loads instant. Schema is very stable
// (column changes are rare), so 5 minutes is safe — a stale entry just gets
// re-read after the TTL, and a column add lands on the next miss.
const catalogSchemaCacheTTL = 5 * time.Minute

// catalogSchemaEntry is one cached Delta-log schema read. fetchedAt is the
// wall-clock stamp the TTL check compares against; cols is the resolved
// schema served on a hit.
type catalogSchemaEntry struct {
	cols      []CatalogColumn
	fetchedAt time.Time
}

// CatalogHandler serves data-catalog endpoints — the user-facing view of
// Iceberg tables produced by deployed and local pipelines (ADR-014).
type CatalogHandler struct {
	glue          GlueClient
	s3            S3API
	workspaceRoot string

	// schemaCache memoizes per-table Delta-log schema reads keyed by the
	// table's S3 location. Mutex-guarded for both read and write: the enrich
	// worker pool writes different locations concurrently. Process-lifetime
	// (TTL-invalidated, never evicted by size) — the table set is small and
	// bounded by the workspace's pipelines.
	schemaMu    sync.Mutex
	schemaCache map[string]catalogSchemaEntry

	// now is the clock the TTL check reads. Defaults to time.Now; overridden
	// in tests so a seeded entry can be aged past the TTL without sleeping.
	now func() time.Time
}

// NewCatalogHandler returns a handler. Pass a nil GlueClient when AWS is
// unavailable — cloud tables become empty, but local pipelines still surface
// via the workspace walk.
func NewCatalogHandler(g GlueClient) *CatalogHandler {
	return &CatalogHandler{
		glue:        g,
		schemaCache: map[string]catalogSchemaEntry{},
		now:         time.Now,
	}
}

// WithS3 wires the S3 client used to read each cloud Delta table's real
// schema from its `_delta_log/`. Glue's StorageDescriptor.Columns is a stub
// for Delta tables (a single `col array<string>`), so without this the cloud
// catalog mis-reports every Delta schema; the local path already reads the
// Delta log directly (ADR-014 parity). Nil-safe — like the nil-Glue
// degradation, a nil client just leaves the Glue stub columns in place.
func (h *CatalogHandler) WithS3(s S3API) *CatalogHandler {
	h.s3 = s
	return h
}

// WithWorkspace wires the workspace root used to enumerate local Hadoop-catalog
// tables. Without it the catalog only surfaces Glue (cloud) entries; with it
// every compute = "local" pipeline's warehouse contributes tables to the
// same flat list.
func (h *CatalogHandler) WithWorkspace(root string) *CatalogHandler {
	h.workspaceRoot = root
	return h
}

// RegisterRoutes wires the handler's endpoints into mux.
func (h *CatalogHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /workspace/tables", h.ListTables)
}

// CatalogColumn is the JSON shape of one column in the catalog response.
type CatalogColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// CatalogTable is the JSON shape of one table in the catalog response. The
// owning_pipeline, owning_node, and output_key fields are derived from
// clavesa's naming convention (`clavesa_<pipeline>.<node>__<key>`) and
// are best-effort — caller should not rely on them being non-empty.
type CatalogTable struct {
	Database string `json:"database"`
	// ADR-020: the three-piece namespace surfaced separately so the UI can
	// render three-level without client-side splitDatabase('__') parsing.
	// `Database` stays one release as a back-compat alias for the wire
	// `<catalog>__<schema>` flat encoding (ADR-016). `Table` duplicates
	// `Name` for the three-piece readers.
	Catalog        string `json:"catalog,omitempty"`
	Schema         string `json:"schema,omitempty"`
	Table          string `json:"table,omitempty"`
	Name           string `json:"name"`
	OwningPipeline string `json:"owning_pipeline,omitempty"`
	OwningNode     string `json:"owning_node,omitempty"`
	OutputKey      string `json:"output_key,omitempty"`
	// Dir is the on-disk pipeline directory (relative to workspace root)
	// owning this table. Set for both cloud and local pipelines so the UI
	// can thread `?dir=` through every per-pipeline query — the server's
	// observability.Resolver picks the right Provider from the pipeline's
	// `compute` attr. Empty when the table can't be matched to a workspace
	// pipeline (e.g. a Glue table named outside our convention).
	Dir string `json:"dir,omitempty"`
	// FreshnessSLASeconds is the staleness budget declared on the owning
	// node's HCL (`freshness_sla = "4h"` becomes 14400). Zero/absent means
	// "not declared" and the UI hides the chip rather than picking an
	// arbitrary default. Sourced from the user's workspace .tf for both
	// cloud and local pipelines (ADR-014 parity) — Glue table tags aren't
	// involved.
	FreshnessSLASeconds int64           `json:"freshness_sla_seconds,omitempty"`
	Location            string          `json:"location,omitempty"`
	TableType           string          `json:"table_type,omitempty"`
	Columns             []CatalogColumn `json:"columns"`
	UpdateTime          *time.Time      `json:"update_time,omitempty"`
}

// CatalogResponse is the body of GET /workspace/tables.
type CatalogResponse struct {
	Tables []CatalogTable `json:"tables"`
	// AWSAvailable is false when the server started without an AWS session
	// (local-only mode). UI uses this to render an explanatory empty state.
	AWSAvailable bool `json:"aws_available"`
}

// ListTables is the HTTP wrapper over Tables — GET /workspace/tables.
func (h *CatalogHandler) ListTables(w http.ResponseWriter, r *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, h.Tables(r.Context()))
}

// Tables enumerates clavesa tables from two sources — Glue databases
// (cloud) and per-pipeline local Hadoop catalogs — and returns a flat
// list. AWSAvailable reflects only the cloud half; local tables are
// unaffected. Shared by the HTTP handler and the `workspace tables` CLI
// command so both surfaces report the identical catalog (ADR-015).
func (h *CatalogHandler) Tables(ctx context.Context) CatalogResponse {
	out := make([]CatalogTable, 0, 16)

	// Workspace pipeline scan — used to stamp `Dir` and `FreshnessSLASeconds`
	// on tables from both sides. Cloud parity (ADR-014) needs this: the chips
	// in the Catalog tile and the snapshots query routing both depend on per-
	// pipeline metadata that lives in the user's local .tf, not in Glue.
	pipByName := map[string]discoveredPipeline{}
	var localPipelines []discoveredPipeline
	if h.workspaceRoot != "" {
		localPipelines = scanWorkspacePipelines(h.workspaceRoot)
		for _, p := range localPipelines {
			pipByName[identutil.Sanitize(p.name)] = p
		}
	}

	// Workspace catalog identifier (ADR-016) — both halves need it. Empty
	// for legacy / pre-ADR workspaces and for malformed manifests; the
	// cloud filter and the local-warehouse encoder both interpret empty as
	// "fall back to pre-ADR `clavesa_<pipeline>` shape" so legacy
	// pipelines keep finding their data byte-for-byte.
	workspaceCatalog := ""
	systemCatalog := ""
	if h.workspaceRoot != "" {
		if m, _ := workspace.Load(h.workspaceRoot); m != nil {
			workspaceCatalog = m.CatalogIdentifier()
			systemCatalog = m.SystemCatalogIdentifier()
		}
	}

	// The env mode picks which world the catalog reads, the same contract
	// every other endpoint honours (the EnvModeToggle: "the backend already
	// dispatches by the mode"). Local mode lists the on-disk Hadoop-catalog
	// tables; cloud mode lists Glue. Reading both double-listed every table
	// that is deployed AND run locally (one row per materialization), and
	// also paid the slow Glue + `_delta_log` enrichment in local mode for
	// tables the user wasn't looking at. With no workspace root we default
	// to cloud — the legacy AWS-only catalog.
	mode := workspace.ModeCloud
	if h.workspaceRoot != "" {
		mode = workspace.LoadEnvironmentMode(h.workspaceRoot)
	}

	// Cloud half. AWS errors degrade gracefully — local-only workspaces
	// (no AWS creds, expired SSO, IMDS lookup failing on a laptop) shouldn't
	// 500 the whole catalog. We log to stderr and flip aws_available to
	// false; the UI renders the "AWS unavailable" affordance and shows the
	// local pipelines unaffected.
	awsAvailable := h.glue != nil
	if mode == workspace.ModeCloud && h.glue != nil {
		dbs, err := h.listClavesaDatabases(ctx, workspaceCatalog, systemCatalog)
		if err != nil {
			fmt.Fprintf(os.Stderr, "catalog: skip cloud half (Glue unreachable): %v\n", err)
			awsAvailable = false
		} else {
			for _, db := range dbs {
				tables, err := h.listTablesInDatabase(ctx, db)
				if err != nil {
					fmt.Fprintf(os.Stderr, "catalog: skip cloud db %s (Glue error): %v\n", db, err)
					continue
				}
				// System DBs (`<system_catalog>__pipelines`) hold the workspace-wide
				// observability tables — node_runs, runs, column_stats, tables.
				// Their names don't follow the `<node>__<key>` convention; blank
				// owning_node/output_key so the UI doesn't render them as
				// transform outputs. Mirrors the local-side treatment.
				isSystemDB := systemCatalog != "" && dbBelongsToWorkspace(db, systemCatalog)
				for i := range tables {
					if isSystemDB {
						tables[i].OwningPipeline = ""
						tables[i].OwningNode = ""
						tables[i].OutputKey = ""
					} else if pip, ok := pipByName[identutil.Sanitize(tables[i].OwningPipeline)]; ok {
						stampPipelineMeta(&tables[i], h.workspaceRoot, pip)
					}
				}
				out = append(out, tables...)
			}
		}
	}

	// Local half — Delta tables under the workspace warehouse. Both the
	// per-pipeline user DB and the workspace-wide system DB (ADR-016
	// v0.20.0) get scanned; system entries de-dup so runs/node_runs/tables
	// surface once even in multi-pipeline workspaces.
	if mode == workspace.ModeLocal && h.workspaceRoot != "" {
		out = append(out, listLocalTables(h.workspaceRoot, workspaceCatalog, systemCatalog, localPipelines)...)
	}

	return CatalogResponse{
		Tables:       out,
		AWSAvailable: awsAvailable,
	}
}

// listClavesaDatabases enumerates Glue DBs that belong to the current
// workspace. The filter scopes to the workspace's catalog identifier:
// match DBs whose name starts with `<catalog>__` OR `<system_catalog>__`
// (the latter holds the workspace-wide observability tables — ADR-016
// v0.20.0). The `__` is the level-boundary marker the runner /
// `EncodeGlueDatabase` use to encode `<catalog>__<schema>`. Other
// workspaces' DBs (different catalog prefix) are excluded; pre-v0.18
// single-underscore DBs in the same account are also excluded — those
// belong to un-migrated workspaces that need to migrate before being
// viewable.
func (h *CatalogHandler) listClavesaDatabases(ctx context.Context, workspaceCatalog, systemCatalog string) ([]string, error) {
	var out []string
	var nextToken *string
	for {
		resp, err := h.glue.GetDatabases(ctx, &glue.GetDatabasesInput{
			NextToken:         nextToken,
			ResourceShareType: gluetypes.ResourceShareTypeAll,
		})
		if err != nil {
			return nil, err
		}
		for _, db := range resp.DatabaseList {
			name := aws.ToString(db.Name)
			if dbBelongsToWorkspace(name, workspaceCatalog) || dbBelongsToWorkspace(name, systemCatalog) {
				out = append(out, name)
				continue
			}
			if isLegacyClavesaDB(name) {
				// Pre-ADR-016 workspaces named their DB `clavesa_<pipeline>`
				// (single underscore, no `__`). Surface them so the user
				// isn't left with "AWS available, 0 tables" — but warn so
				// the migration is visible.
				fmt.Fprintf(os.Stderr,
					"catalog: surfacing legacy single-underscore DB %s; run `clavesa pipeline upgrade <dir>` to migrate to the <catalog>__<schema> form\n",
					name)
				out = append(out, name)
			}
		}
		if resp.NextToken == nil {
			break
		}
		nextToken = resp.NextToken
	}
	return out, nil
}

// dbBelongsToWorkspace decides whether a Glue DB name belongs to the
// current workspace given its catalog identifier. Workspace catalog is
// always non-empty post-v0.18.0 (Manifest.Load auto-populates legacy
// manifests). The `__` boundary is required so a workspace
// `clavesa_demo_ws` doesn't accidentally claim
// `clavesa_demo_ws_other` (a different workspace whose catalog
// happens to share a prefix).
func dbBelongsToWorkspace(dbName, workspaceCatalog string) bool {
	if workspaceCatalog == "" {
		return false
	}
	return strings.HasPrefix(dbName, workspaceCatalog+"__")
}

// isLegacyClavesaDB matches the pre-ADR-016 single-underscore form
// `clavesa_<pipeline>` (no `__` separator). Mirrors the local-side
// fallback at catalog_local.go (TrimPrefix "clavesa_"). Without this
// branch in listClavesaDatabases, un-migrated cloud workspaces show
// "AWS available, 0 tables" with no diagnostic (A P1-3, 2026-05-24).
func isLegacyClavesaDB(dbName string) bool {
	if !strings.HasPrefix(dbName, "clavesa_") {
		return false
	}
	// Skip the new <catalog>__<schema> form: by construction
	// <workspaceCatalog> starts with "clavesa_" too, so a `__` further
	// in the name means new form (already handled above) — only the
	// single-underscore form lands here.
	return !strings.Contains(strings.TrimPrefix(dbName, "clavesa_"), "__")
}

func (h *CatalogHandler) listTablesInDatabase(ctx context.Context, db string) ([]CatalogTable, error) {
	var out []CatalogTable
	var nextToken *string
	for {
		resp, err := h.glue.GetTables(ctx, &glue.GetTablesInput{
			DatabaseName: aws.String(db),
			NextToken:    nextToken,
		})
		if err != nil {
			return nil, err
		}
		for i := range resp.TableList {
			t := resp.TableList[i]
			out = append(out, glueTableToCatalog(db, t))
		}
		if resp.NextToken == nil {
			break
		}
		nextToken = resp.NextToken
	}
	// Glue's StorageDescriptor.Columns is a stub for Delta tables — the
	// real schema lives in `_delta_log/`, so read it back the way the local
	// path does (ADR-014 parity). Best-effort: failures leave the stub.
	h.enrichDeltaColumns(ctx, out)
	return out, nil
}

// enrichDeltaColumns replaces the stub Glue columns on each Delta table with
// the real schema read from its `_delta_log/` on S3. Glue records Delta
// tables with a single `col array<string>` stub (the schema is owned by the
// transaction log, not Glue), so without this the cloud catalog mis-renders
// every Delta table's columns. The local catalog path already reads the
// Delta log via delta.ReadCurrentFromPath; this is the cloud mirror.
//
// We do NOT gate on TableType: Spark's Delta saveAsTable registers the Glue
// table without a `table_type=DELTA` parameter (that tag is an Iceberg
// convention), so the only reliable Delta signal is the presence of a
// readable `_delta_log/` under the table's S3 location. We attempt the read
// for every located table; a non-Delta table (plain-Parquet destination
// override) simply has no `_delta_log/`, the read errors, and its Glue
// columns survive. clavesa writes Delta by default, so nearly every table
// hits the fast success path. A successful read also stamps TableType =
// "DELTA" — the authoritative format signal the Glue parameter lacked.
//
// Best-effort by design: any error (bad Location, S3 miss, malformed log)
// leaves the Glue stub columns untouched and logs the table name to stderr —
// the catalog never fails because of one unreadable table.
//
// The per-table S3 round-trips run on a bounded worker pool; each goroutine
// writes only its own slice index so the shared slice needs no lock. A fresh
// cache hit is resolved synchronously in the loop before the decision to
// spawn, so it consumes neither a worker slot nor a goroutine.
func (h *CatalogHandler) enrichDeltaColumns(ctx context.Context, tables []CatalogTable) {
	if h.s3 == nil {
		return
	}
	sem := make(chan struct{}, catalogEnrichConcurrency)
	var wg sync.WaitGroup
	for i := range tables {
		if tables[i].Location == "" {
			continue
		}
		// Cache hit short-circuits the S3 read. A cached entry by definition
		// came from a successful Delta-log read, so it both supplies the
		// columns and re-stamps TableType = "DELTA" (it IS Delta).
		if cols, ok := h.cachedSchema(tables[i].Location); ok {
			tables[i].Columns = cols
			tables[i].TableType = "DELTA"
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			cols, err := h.readDeltaColumns(ctx, tables[i].Location)
			if err != nil {
				// Not a Delta table (no `_delta_log/`) or an unreadable
				// one — keep whatever columns Glue gave us. Debug-level
				// noise, so log only the table identity, not per-table
				// stack. Do NOT cache the failure: a transient S3 error
				// must not pin the Glue stub for the whole TTL.
				fmt.Fprintf(os.Stderr,
					"catalog: keep Glue columns for %s.%s (no readable Delta log): %v\n",
					tables[i].Database, tables[i].Name, err)
				return
			}
			tables[i].Columns = cols
			tables[i].TableType = "DELTA"
			h.storeSchema(tables[i].Location, cols)
		}(i)
	}
	wg.Wait()
}

// cachedSchema returns the cached columns for a location when a fresh
// (within-TTL) entry exists. Mutex-guarded read.
func (h *CatalogHandler) cachedSchema(location string) ([]CatalogColumn, bool) {
	h.schemaMu.Lock()
	defer h.schemaMu.Unlock()
	if h.schemaCache == nil {
		return nil, false
	}
	e, ok := h.schemaCache[location]
	if !ok {
		return nil, false
	}
	if h.now().Sub(e.fetchedAt) >= catalogSchemaCacheTTL {
		return nil, false
	}
	return e.cols, true
}

// storeSchema records a successful Delta-log read in the cache, stamped with
// the current time for the TTL check. Mutex-guarded write.
func (h *CatalogHandler) storeSchema(location string, cols []CatalogColumn) {
	h.schemaMu.Lock()
	defer h.schemaMu.Unlock()
	if h.schemaCache == nil {
		h.schemaCache = map[string]catalogSchemaEntry{}
	}
	h.schemaCache[location] = catalogSchemaEntry{cols: cols, fetchedAt: h.now()}
}

// readDeltaColumns parses an `s3://bucket/key` table root, reads the
// `_delta_log/` under it via s3fs + delta.ReadCurrent, and projects the
// current schema into CatalogColumns. Mirrors readLocalTable's column
// projection so the two surfaces produce identical shapes (ADR-014).
func (h *CatalogHandler) readDeltaColumns(ctx context.Context, location string) ([]CatalogColumn, error) {
	bucket, key := parseCatalogS3URI(location)
	if bucket == "" {
		return nil, fmt.Errorf("not an s3:// location: %q", location)
	}
	logPrefix := strings.TrimSuffix(key, "/") + "/_delta_log/"
	fsys := s3fs.New(ctx, h.s3, bucket, logPrefix)
	// ReadSchema, not ReadCurrent: the catalog only needs the column list,
	// and the schema-only path is checkpoint-aware so an append-only table
	// (e.g. node_runs) no longer replays its whole commit history per page
	// load. The discarded commit history is exactly the other expensive
	// half this avoids.
	schema, err := delta.ReadSchema(fsys)
	if err != nil {
		return nil, err
	}
	cols := make([]CatalogColumn, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		cols = append(cols, CatalogColumn{Name: c.Name, Type: c.Type})
	}
	return cols, nil
}

// parseCatalogS3URI splits `s3://bucket/key/path` into (bucket, key).
// Returns empty strings for non-S3 or bucket-only URIs so callers treat
// that as "no Delta log here". Local to this package on purpose — the
// observability sibling keeps its own unexported parseS3URI.
func parseCatalogS3URI(uri string) (bucket, key string) {
	const scheme = "s3://"
	if !strings.HasPrefix(uri, scheme) {
		return "", ""
	}
	rest := uri[len(scheme):]
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return rest, ""
	}
	return rest[:slash], rest[slash+1:]
}

func glueTableToCatalog(db string, t gluetypes.Table) CatalogTable {
	name := aws.ToString(t.Name)
	// `<catalog>__<schema>` post-v0.18 — the part after `__` is the
	// pipeline schema (which equals the sanitized pipeline name unless
	// the user opted into a custom schema via `pipeline create
	// --schema <id>`). Falls back to the whole DB name when the
	// boundary marker is absent (defensive — shouldn't happen against
	// post-v0.18 producers).
	catalog, schema := splitGlueDB(db)
	owningPipeline := schema
	if owningPipeline == "" {
		owningPipeline = db
	}

	// Clavesa convention: <node>__<output_key>.
	owningNode, outputKey := splitNodeOutput(name)

	var cols []CatalogColumn
	if t.StorageDescriptor != nil {
		cols = make([]CatalogColumn, 0, len(t.StorageDescriptor.Columns))
		for _, c := range t.StorageDescriptor.Columns {
			cols = append(cols, CatalogColumn{
				Name: aws.ToString(c.Name),
				Type: aws.ToString(c.Type),
			})
		}
	}

	tableType := ""
	if t.Parameters != nil {
		// Iceberg tables tag themselves via "table_type=ICEBERG".
		if v, ok := t.Parameters["table_type"]; ok {
			tableType = strings.ToUpper(v)
		}
	}

	location := ""
	if t.StorageDescriptor != nil {
		location = aws.ToString(t.StorageDescriptor.Location)
	}

	return CatalogTable{
		Database:       db,
		Catalog:        catalog,
		Schema:         schema,
		Table:          name,
		Name:           name,
		OwningPipeline: owningPipeline,
		OwningNode:     owningNode,
		OutputKey:      outputKey,
		Location:       location,
		TableType:      tableType,
		Columns:        cols,
		UpdateTime:     t.UpdateTime,
	}
}

// splitGlueDB splits an ADR-016 wire-form database name on the first `__`
// boundary into (catalog, schema). Single-occurrence rule: catalog is
// sanitized so it cannot contain `__`; schema in theory could, so we
// honour the first marker as the level boundary. Returns ("", db) when
// no separator is present so legacy single-underscore DBs still
// populate `schema` for the UI fallback.
func splitGlueDB(db string) (catalog, schema string) {
	i := strings.Index(db, "__")
	if i < 0 {
		return "", db
	}
	return db[:i], db[i+2:]
}

// splitNodeOutput parses "<node>__<output_key>" per clavesa's auto-table
// naming. ADR-019 dropped the `__default` suffix from single-output
// transforms; a bare table name with no `__` now means key = "default".
// Tables that contain a `__` keep the legacy split. Returns ("", "") only
// for system tables that surface via this helper but live outside the
// node namespace.
func splitNodeOutput(tableName string) (node, outputKey string) {
	idx := strings.Index(tableName, "__")
	if idx < 0 {
		if tableName == "" {
			return "", ""
		}
		return tableName, "default"
	}
	return tableName[:idx], tableName[idx+2:]
}
