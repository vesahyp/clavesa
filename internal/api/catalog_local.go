package api

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// listLocalTables surfaces every Iceberg table in the workspace-shared
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
// Errors are swallowed at each level — a bad metadata.json on one table
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
	dbDirs, err := os.ReadDir(warehouse)
	if err != nil {
		return out // no local pipeline has run yet — nothing to surface
	}
	for _, dbEntry := range dbDirs {
		if !dbEntry.IsDir() {
			continue
		}
		db := dbEntry.Name()
		pip, isUserDB := userDBToPipeline[db]
		if !isUserDB && db != systemDB {
			continue // stray namespace (e.g. a destroyed pipeline) — skip
		}
		dbRoot := filepath.Join(warehouse, db)
		entries, err := os.ReadDir(dbRoot)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			t, ok := readLocalTable(db, e.Name(), filepath.Join(dbRoot, e.Name()))
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
	// environment mode is local, or (transitional fallback) a transform
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
	// The workspace environment mode is the sole local-vs-cloud key
	// (TODO bucket 16) — every pipeline in the workspace dispatches the
	// same way.
	isLocal := workspace.LoadEnvironmentMode(root) == workspace.ModeLocal
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

// readLocalTable reads the latest metadata.json under tableDir/metadata/ and
// projects it into a CatalogTable. Returns (_, false) when the directory is
// not a valid Iceberg table (no metadata files, unparseable JSON, etc.).
func readLocalTable(db, name, tableDir string) (CatalogTable, bool) {
	metaDir := filepath.Join(tableDir, "metadata")
	metaPath, ok := latestMetadataFile(metaDir)
	if !ok {
		return CatalogTable{}, false
	}
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return CatalogTable{}, false
	}

	var meta struct {
		Location        string `json:"location"`
		LastUpdatedMs   int64  `json:"last-updated-ms"`
		CurrentSchemaID int    `json:"current-schema-id"`
		Schemas         []struct {
			SchemaID int `json:"schema-id"`
			Fields   []struct {
				Name     string      `json:"name"`
				Type     interface{} `json:"type"` // string for primitives, object for nested
				Required bool        `json:"required"`
			} `json:"fields"`
		} `json:"schemas"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return CatalogTable{}, false
	}

	cols := make([]CatalogColumn, 0)
	for _, sch := range meta.Schemas {
		if sch.SchemaID != meta.CurrentSchemaID {
			continue
		}
		for _, f := range sch.Fields {
			cols = append(cols, CatalogColumn{
				Name: f.Name,
				Type: typeString(f.Type),
			})
		}
		break
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
		Name:           name,
		OwningPipeline: owningPipeline,
		OwningNode:     owningNode,
		OutputKey:      outputKey,
		Location:       meta.Location,
		TableType:      "ICEBERG",
		Columns:        cols,
	}
	if meta.LastUpdatedMs > 0 {
		ts := time.UnixMilli(meta.LastUpdatedMs).UTC()
		t.UpdateTime = &ts
	}
	return t, true
}

// latestMetadataFile returns the highest-versioned vN.metadata.json file in
// metaDir. Iceberg's Hadoop catalog rewrites with strictly increasing N on
// every commit; sort + last gives us the current one.
func latestMetadataFile(metaDir string) (string, bool) {
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		return "", false
	}
	best := ""
	for _, e := range entries {
		n := e.Name()
		if !strings.HasPrefix(n, "v") || !strings.HasSuffix(n, ".metadata.json") {
			continue
		}
		if best == "" || lexLess(best, n) {
			best = n
		}
	}
	if best == "" {
		return "", false
	}
	return filepath.Join(metaDir, best), true
}

// lexLess compares two metadata filenames by their numeric prefix so
// "v10.metadata.json" sorts after "v9.metadata.json" — purely lexicographic
// comparison would put "v10" before "v9".
func lexLess(a, b string) bool {
	na := numericVersion(a)
	nb := numericVersion(b)
	if na != nb {
		return na < nb
	}
	return a < b
}

func numericVersion(name string) int {
	if !strings.HasPrefix(name, "v") {
		return 0
	}
	end := strings.Index(name, ".")
	if end < 0 {
		return 0
	}
	n := 0
	for _, ch := range name[1:end] {
		if ch < '0' || ch > '9' {
			return 0
		}
		n = n*10 + int(ch-'0')
	}
	return n
}

// typeString flattens an Iceberg field type. Primitive fields encode the
// type as a JSON string ("string", "long", etc.); nested types come through
// as objects. We surface the primitive name verbatim and "<nested>" for
// anything else — the catalog UI doesn't render struct details today.
func typeString(t interface{}) string {
	if s, ok := t.(string); ok {
		return s
	}
	return "<nested>"
}

