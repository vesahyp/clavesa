package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// GlueClient is the subset of the AWS Glue API used by the catalog handler.
type GlueClient interface {
	GetDatabases(ctx context.Context, params *glue.GetDatabasesInput, optFns ...func(*glue.Options)) (*glue.GetDatabasesOutput, error)
	GetTables(ctx context.Context, params *glue.GetTablesInput, optFns ...func(*glue.Options)) (*glue.GetTablesOutput, error)
}

// CatalogHandler serves data-catalog endpoints — the user-facing view of
// Iceberg tables produced by deployed and local pipelines (ADR-014).
type CatalogHandler struct {
	glue          GlueClient
	workspaceRoot string
}

// NewCatalogHandler returns a handler. Pass a nil GlueClient when AWS is
// unavailable — cloud tables become empty, but local pipelines still surface
// via the workspace walk.
func NewCatalogHandler(g GlueClient) *CatalogHandler {
	return &CatalogHandler{glue: g}
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
	Database       string `json:"database"`
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

	// Cloud half. AWS errors degrade gracefully — local-only workspaces
	// (no AWS creds, expired SSO, IMDS lookup failing on a laptop) shouldn't
	// 500 the whole catalog. We log to stderr and flip aws_available to
	// false; the UI renders the "AWS unavailable" affordance and shows the
	// local pipelines unaffected.
	awsAvailable := h.glue != nil
	if h.glue != nil {
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
				for i := range tables {
					if pip, ok := pipByName[identutil.Sanitize(tables[i].OwningPipeline)]; ok {
						stampPipelineMeta(&tables[i], h.workspaceRoot, pip)
					}
				}
				out = append(out, tables...)
			}
		}
	}

	// Local half — Iceberg tables under each compute=local pipeline's
	// warehouse. Both the per-pipeline user DB and the workspace-wide
	// system DB (ADR-016 v0.20.0) get scanned; system entries de-dup so
	// runs/node_runs/tables surface once even in multi-pipeline
	// workspaces.
	if h.workspaceRoot != "" {
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
	return out, nil
}

func glueTableToCatalog(db string, t gluetypes.Table) CatalogTable {
	name := aws.ToString(t.Name)
	// `<catalog>__<schema>` post-v0.18 — the part after `__` is the
	// pipeline schema (which equals the sanitized pipeline name unless
	// the user opted into a custom schema via `pipeline create
	// --schema <id>`). Falls back to the whole DB name when the
	// boundary marker is absent (defensive — shouldn't happen against
	// post-v0.18 producers).
	owningPipeline := db
	if i := strings.Index(db, "__"); i >= 0 {
		owningPipeline = db[i+2:]
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

// splitNodeOutput parses "<node>__<output_key>" per clavesa's auto-table
// naming. Returns ("", "") if the convention doesn't match (e.g. tables added
// outside clavesa).
func splitNodeOutput(tableName string) (node, outputKey string) {
	idx := strings.Index(tableName, "__")
	if idx < 0 {
		return "", ""
	}
	return tableName[:idx], tableName[idx+2:]
}
