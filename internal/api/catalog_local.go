package api

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/delta"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// listLocalTables surfaces every Delta table in the workspace-shared
// local Hadoop warehouse at <workspaceRoot>/.clavesa/warehouse/<glue_db>/,
// where <glue_db> is `identutil.EncodeGlueDatabase(catalog, schema)`. One
// warehouse per workspace holds every local pipeline's tables under separate
// namespaces (ADR-016) — the same model the cloud path uses with one S3
// bucket and a Glue DB per schema.
//
// Each `<catalog>__<schema>` namespace is matched back to its producing
// pipeline (for `Dir` / freshness stamping) via the schema; the system
// catalog's `pipelines` namespace has no single owner, so its tables get
// blank owning fields and a `Dir` pointing at any local pipeline.
//
// The encoding MUST match what the runner's `_glue_db()` writes — same
// sanitization rule. If they drift, local tables stop showing up.
//
// Errors are swallowed at each level — a bad `_delta_log/` on one table
// shouldn't hide the rest of the workspace.
func listLocalTables(workspaceRoot, workspaceCatalog, systemCatalog string, pipelines []discoveredPipeline) []CatalogTable {
	var out []CatalogTable
	systemDB := ""
	if systemCatalog != "" {
		systemDB = identutil.EncodeGlueDatabase(systemCatalog, "pipelines")
	}
	// Map each user namespace (`<catalog>__<schema>`) back to its producing
	// pipeline, and pick any local pipeline to carry the `Dir` for the
	// owner-less system tables.
	userDBToPipeline := map[string]discoveredPipeline{}
	var systemDir discoveredPipeline
	haveSystemDir := false
	for _, pip := range pipelines {
		if !pip.isLocal {
			continue
		}
		schema := pip.schema
		if schema == "" {
			schema = pip.name
		}
		userDBToPipeline[identutil.EncodeGlueDatabase(workspaceCatalog, schema)] = pip
		if !haveSystemDir {
			systemDir = pip
			haveSystemDir = true
		}
	}

	warehouse := workspace.LocalWarehouseDir(workspaceRoot)

	// ADR-019 Slice 4: new on-disk layout is
	// ``<warehouse>/<catalog>/<schema>/<table>/`` (V2 multi-catalog
	// DeltaCatalog with per-catalog warehouse). Legacy layout from pre-
	// Slice-4 (Hive metastore federation) was
	// ``<warehouse>/<catalog>__<schema>.db/<table>/``. Walk both — the JSON
	// shape stays at ``Database = <catalog>__<schema>`` for this slice; the
	// UI keeps its existing splitDatabase decoder until Slice 6 rewires
	// API + UI to three-level fields natively.
	for _, ns := range listWarehouseNamespaces(warehouse, workspaceCatalog, systemCatalog) {
		var (
			pip       discoveredPipeline
			isUserDB  bool
			logicalDB = ns.logicalDB
		)
		if pip, isUserDB = userDBToPipeline[logicalDB]; !isUserDB && logicalDB != systemDB {
			continue
		}
		entries, err := os.ReadDir(ns.dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			t, ok := readLocalTable(logicalDB, ns.catalog, ns.schema, e.Name(), filepath.Join(ns.dir, e.Name()))
			if !ok {
				continue
			}
			if isUserDB {
				stampPipelineMeta(&t, workspaceRoot, pip)
			} else {
				// System tables roll up across the workspace; they have no
				// single owning pipeline. Blank those fields so the UI reads
				// "—" instead of a misleading value, but still stamp `Dir` to
				// some local pipeline so the observability Resolver can
				// dispatch the local provider for snapshots / sample queries.
				t.OwningPipeline = ""
				t.OwningNode = ""
				t.OutputKey = ""
				if haveSystemDir {
					if rel, err := filepath.Rel(workspaceRoot, systemDir.dir); err == nil {
						t.Dir = rel
					} else {
						t.Dir = systemDir.dir
					}
				}
			}
			out = append(out, t)
		}
	}
	return out
}

// localNamespace is one resolved namespace location on disk plus the
// logical ``<catalog>__<schema>`` key listLocalTables uses to look up the
// owning pipeline. `catalog` and `schema` carry the three-piece form
// (ADR-020) populated from on-disk path components when the V2 layout
// is in use, and from splitting `logicalDB` on `__` for the two
// legacy layouts. `Database` in the JSON response stays at the wire
// form for one-release back-compat.
type localNamespace struct {
	dir       string // absolute path to walk for `<table>/_delta_log/` directories
	logicalDB string // ``<catalog>__<schema>`` form (key into userDBToPipeline)
	catalog   string
	schema    string
}

// listWarehouseNamespaces enumerates every (catalog, schema) namespace
// directory in the workspace warehouse across the three on-disk
// layouts the catalog page surfaces:
//
//  1. ADR-019 V2 (Slice 4): ``<warehouse>/<catalog>/<schema>/<table>/``.
//     Restricted to known workspace + system catalogs so unrelated
//     warehouse-root entries (e.g. Derby's ``_metastore/``) don't get
//     mistakenly probed as catalogs.
//  2. Legacy Hive with ``.db`` suffix:
//     ``<warehouse>/<catalog>__<schema>.db/<table>/`` — what v2.0.0
//     through Slice-3 wrote via the persistent Hive metastore.
//  3. Legacy flat without ``.db`` suffix:
//     ``<warehouse>/<catalog>__<schema>/<table>/`` — pre-v2.0.0
//     in-memory-Hive layout, still showing up in workspaces migrated
//     from per-pipeline warehouses (migrateLocalWarehouses keeps the
//     namespace dir name as-is).
func listWarehouseNamespaces(warehouse, workspaceCatalog, systemCatalog string) []localNamespace {
	var out []localNamespace
	entries, err := os.ReadDir(warehouse)
	if err != nil {
		return out
	}
	known := map[string]bool{}
	if workspaceCatalog != "" {
		known[workspaceCatalog] = true
	}
	if systemCatalog != "" {
		known[systemCatalog] = true
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".db") {
			logical := strings.TrimSuffix(name, ".db")
			cat, sch := splitGlueDB(logical)
			out = append(out, localNamespace{
				dir:       filepath.Join(warehouse, name),
				logicalDB: logical,
				catalog:   cat,
				schema:    sch,
			})
			continue
		}
		if strings.Contains(name, "__") {
			// Pre-v2.0.0 flat layout. The flat-encoded
			// ``<catalog>__<schema>`` name is its own logicalDB —
			// listLocalTables filters by exact match against the
			// known catalog/schema set so noise gets skipped there.
			cat, sch := splitGlueDB(name)
			out = append(out, localNamespace{
				dir:       filepath.Join(warehouse, name),
				logicalDB: name,
				catalog:   cat,
				schema:    sch,
			})
			continue
		}
		if !known[name] {
			continue
		}
		schemas, err := os.ReadDir(filepath.Join(warehouse, name))
		if err != nil {
			continue
		}
		for _, sc := range schemas {
			if !sc.IsDir() {
				continue
			}
			out = append(out, localNamespace{
				dir:       filepath.Join(warehouse, name, sc.Name()),
				logicalDB: identutil.EncodeGlueDatabase(name, sc.Name()),
				catalog:   name,
				schema:    sc.Name(),
			})
		}
	}
	return out
}

// stampPipelineMeta annotates a CatalogTable with workspace-derived facts that
// don't come from Glue or Iceberg metadata — `Dir` so the UI can thread
// `?dir=` through to the right Provider, and `FreshnessSLASeconds` from the
// owning node's HCL. Applied to both cloud and local tables for parity.
func stampPipelineMeta(t *CatalogTable, workspaceRoot string, pip discoveredPipeline) {
	if rel, err := filepath.Rel(workspaceRoot, pip.dir); err == nil {
		t.Dir = rel
	} else {
		t.Dir = pip.dir
	}
	if t.OwningNode != "" {
		if sec, ok := pip.freshnessSLABySanitizedNode[identutil.Sanitize(t.OwningNode)]; ok {
			t.FreshnessSLASeconds = sec
		}
	}
}

// discoveredPipeline is a small reimplementation of workspace.scanPipelines
// scoped to the catalog handler's need — duplication is cheaper than
// threading the full scan through service.Service.
type discoveredPipeline struct {
	name string
	dir  string
	// schema is the ADR-016 pipeline schema identifier read from the
	// pipeline's `variable "schema"` default in variables.tf. Empty for
	// pre-ADR-016 pipelines; consumers fall back to the sanitized
	// pipeline name (matching what the runner's _glue_db() does).
	schema string
	// isLocal flags that this pipeline dispatches local — the workspace
	// warehouse is local, or (transitional fallback) a transform
	// still carries compute = "local". Cloud pipelines still get scanned:
	// their freshness/dir metadata stamps onto Glue-sourced tables for
	// ADR-014 parity.
	isLocal bool
	// Per-node freshness SLA in seconds, keyed by sanitized node id (the
	// same form used by table names — `<node>__<key>` where node is the
	// sanitized id). Populated from each transform/source module call's
	// `freshness_sla = "..."` attribute when present; absent nodes don't
	// appear in the map and the UI hides the chip for their tables.
	freshnessSLABySanitizedNode map[string]int64
}

// scanWorkspacePipelines walks every candidate dir under root, parses any
// clavesa pipeline it finds, and records the freshness/dir metadata used
// to stamp catalog tables. Returns both cloud and local pipelines — callers
// filter by `isLocal` when they want only one side.
func scanWorkspacePipelines(root string) []discoveredPipeline {
	var out []discoveredPipeline
	// The workspace warehouse is the sole local-vs-cloud key
	// (TODO bucket 16) — every pipeline in the workspace dispatches the
	// same way.
	isLocal := workspace.LoadWarehouse(root) == workspace.WarehouseLocal
	candidates := candidateDirs(root)
	for _, dir := range candidates {
		g, err := hclparser.Parse(dir)
		if err != nil || len(g.Nodes) == 0 {
			continue
		}
		slaByNode := map[string]int64{}
		for _, n := range g.Nodes {
			raw, _ := n.Config["freshness_sla"].(string)
			if raw == "" {
				continue
			}
			if sec, ok := parseFreshnessSLA(raw); ok {
				slaByNode[identutil.Sanitize(n.ID)] = sec
			}
		}
		out = append(out, discoveredPipeline{
			name:                        filepath.Base(dir),
			dir:                         dir,
			schema:                      readPipelineSchemaDefault(dir),
			isLocal:                     isLocal,
			freshnessSLABySanitizedNode: slaByNode,
		})
	}
	return out
}

// readPipelineSchemaDefault parses the pipeline's variables.tf for the
// default value of `variable "schema"`. Same naive scan readVariableDecls
// uses elsewhere in this package — full HCL parsing isn't needed for a
// single string variable. Returns empty when the variable isn't declared
// (legacy pipelines created before ADR-016) — listLocalTables falls back
// to the sanitized pipeline name in that case so byte-for-byte today's
// `clavesa_<pipeline>` Glue DB resolution still works.
func readPipelineSchemaDefault(dir string) string {
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
				return ""
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

// parseFreshnessSLA converts an HCL "4h" / "30m" / "7d" / "120s" budget into
// seconds. Mirrors the validation regex on the Terraform variable. Returns
// (0, false) for malformed strings — callers treat that as "no SLA"
// (graceful, the UI hides the chip rather than blocking the catalog).
func parseFreshnessSLA(s string) (int64, bool) {
	if len(s) < 2 {
		return 0, false
	}
	suffix := s[len(s)-1]
	numStr := s[:len(s)-1]
	if numStr == "" {
		return 0, false
	}
	var n int64
	for _, c := range numStr {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	switch suffix {
	case 's':
		return n, true
	case 'm':
		return n * 60, true
	case 'h':
		return n * 3600, true
	case 'd':
		return n * 86400, true
	}
	return 0, false
}

// candidateDirs lists the workspace root + immediate subdirs + one level
// deeper. Same shape workspace.scanPipelines uses; mirrored here so this
// package doesn't take a build dependency on workspace's full discovery.
func candidateDirs(root string) []string {
	out := []string{root}
	entries, err := os.ReadDir(root)
	if err != nil {
		return out
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") || strings.HasPrefix(e.Name(), "_") {
			continue
		}
		sub := filepath.Join(root, e.Name())
		out = append(out, sub)
		subEntries, err := os.ReadDir(sub)
		if err != nil {
			continue
		}
		for _, se := range subEntries {
			if se.IsDir() && !strings.HasPrefix(se.Name(), ".") && !strings.HasPrefix(se.Name(), "_") {
				out = append(out, filepath.Join(sub, se.Name()))
			}
		}
	}
	return out
}

// readLocalTable reads the Delta transaction log under tableDir/_delta_log/
// and projects the current schema + latest commit into a CatalogTable.
// Returns (_, false) when the directory is not a valid Delta table — no
// `_delta_log/`, an empty log, or any commit fails to parse. Per-table
// errors swallow rather than surface; the walker is best-effort.
//
// `catalog` and `schema` are the three-piece pieces (ADR-020); they come
// from on-disk path components for V2 layouts and from splitting the
// wire-form `db` for the two legacy layouts.
func readLocalTable(db, catalog, schemaID, name, tableDir string) (CatalogTable, bool) {
	schema, commits, err := delta.ReadCurrentFromPath(tableDir)
	if err != nil {
		// ErrNotDelta is the expected "directory isn't a table" signal;
		// any other error (malformed commit, IO failure) gets the same
		// treatment — the catalog page degrades gracefully rather than
		// 500ing because of one bad table.
		_ = errors.Is(err, delta.ErrNotDelta) // documented signal, not load-bearing here
		return CatalogTable{}, false
	}

	cols := make([]CatalogColumn, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		cols = append(cols, CatalogColumn{
			Name: c.Name,
			Type: c.Type,
		})
	}

	// `<catalog>__<schema>` post-v0.18 — the part after `__` is the
	// pipeline schema (= sanitized pipeline name unless the user opted
	// into a custom schema). Keep the legacy `clavesa_` strip as a
	// defensive fallback for the test fixture path.
	owningPipeline := db
	if i := strings.Index(db, "__"); i >= 0 {
		owningPipeline = db[i+2:]
	} else {
		owningPipeline = strings.TrimPrefix(db, "clavesa_")
	}
	owningNode, outputKey := splitNodeOutput(name)

	t := CatalogTable{
		Database:       db,
		Catalog:        catalog,
		Schema:         schemaID,
		Table:          name,
		Name:           name,
		OwningPipeline: owningPipeline,
		OwningNode:     owningNode,
		OutputKey:      outputKey,
		// Delta's transaction log doesn't carry a separate "location"
		// field the way Iceberg's metadata.json did — the location IS
		// the directory we're reading from. Hand it back so the UI's
		// existing rendering keeps working.
		Location:  tableDir,
		TableType: "DELTA",
		Columns:   cols,
	}
	if len(commits) > 0 && commits[0].TimestampMs > 0 {
		ts := time.UnixMilli(commits[0].TimestampMs).UTC()
		t.UpdateTime = &ts
	}
	return t, true
}
