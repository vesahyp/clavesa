package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/delta"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// Local-mode backfill replaces Glue table tagging with a sidecar JSON file
// next to the staging Delta directory. Same key/value shape as the cloud
// Glue Parameters map so BackfillList reconstructs an identical BackfillRun.
//
// The staging Delta table and its sidecar live under the pipeline's
// namespace directory in the local warehouse. Since ADR-019 Slice 4 that is
// the V2 nested layout:
//
//	<warehouse>/<catalog>/<schema>/<staging_table>/            (Delta table)
//	<warehouse>/<catalog>/<schema>/<staging_table>.backfill.json (sidecar)
//
// with the legacy Hive layout (<warehouse>/<catalog>__<schema>.db/) probed
// as a fallback for workspaces written before the migration. Either way the
// sidecar sits beside the table dir, so a single readdir scan of the
// namespace directory finds both the staging directories and their metadata.
// Table-dir resolution mirrors observability.ResolveLocalTablePath, the
// canonical local Delta-path resolver (v2 → legacy → `__default` probe).

// stagingSidecar is the on-disk shape of one staging table's metadata.
type stagingSidecar struct {
	RunID          string    `json:"run_id"`
	Node           string    `json:"node"`
	OutputKey      string    `json:"output_key"`
	From           []string  `json:"from"`
	To             []string  `json:"to"`
	CanonicalTable string    `json:"canonical_table"`
	StartedAt      time.Time `json:"started_at"`
	StoppedAt      time.Time `json:"stopped_at,omitempty"`
}

// localNamespaceDir returns the on-disk directory holding this pipeline's
// tables (and their backfill sidecars) for the encoded `<catalog>__<schema>`
// Glue DB. Probes the ADR-019 V2 nested layout
// `<warehouse>/<catalog>/<schema>/` first and falls back to the legacy Hive
// `<warehouse>/<catalog>__<schema>.db/` when V2 doesn't exist — the same
// two-layout probe the observability catalog walker and ResolveLocalTablePath
// use. Returns the legacy path when neither exists so a first write MkdirAll's
// a deterministic location (the runner has already created the V2 namespace by
// the time a sidecar is written, so in practice V2 wins).
func (s *Service) localNamespaceDir(glueDB string) string {
	warehouse := workspace.LocalWarehouseDir(s.workspace)
	if i := strings.Index(glueDB, "__"); i >= 0 {
		catalog, schema := glueDB[:i], glueDB[i+2:]
		v2 := filepath.Join(warehouse, catalog, schema)
		if fi, err := os.Stat(v2); err == nil && fi.IsDir() {
			return v2
		}
	}
	return filepath.Join(warehouse, glueDB+".db")
}

// stagingSidecarPath returns the absolute path of the sidecar JSON for the
// given staging table. glueDB is the encoded `<catalog>__<schema>` namespace;
// stagingTable is just the table-name segment (no db prefix). The sidecar
// sits beside the table dir in the pipeline's namespace directory.
func (s *Service) stagingSidecarPath(glueDB, stagingTable string) string {
	return filepath.Join(s.localNamespaceDir(glueDB), stagingTable+".backfill.json")
}

func (s *Service) writeStagingSidecar(glueDB, stagingTable string, sc stagingSidecar) error {
	path := s.stagingSidecarPath(glueDB, stagingTable)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create warehouse db dir: %w", err)
	}
	body, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, body, 0o644)
}

func (s *Service) readStagingSidecar(glueDB, stagingTable string) (stagingSidecar, error) {
	path := s.stagingSidecarPath(glueDB, stagingTable)
	var sc stagingSidecar
	body, err := os.ReadFile(path)
	if err != nil {
		return sc, err
	}
	if err := json.Unmarshal(body, &sc); err != nil {
		return sc, fmt.Errorf("parse %s: %w", path, err)
	}
	return sc, nil
}

func (s *Service) deleteStagingSidecar(glueDB, stagingTable string) error {
	path := s.stagingSidecarPath(glueDB, stagingTable)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// listLocalStagingTables scans the pipeline's namespace directory for staging
// Delta table directories (`*__backfill__*`) paired with their sidecar JSON.
// Returns (stagingTable, sidecar) pairs in directory order. A staging dir with
// no sidecar — and a sidecar with no staging dir — are both skipped (would
// never pair into a usable BackfillRun, so listing them just confuses the
// user).
func (s *Service) listLocalStagingTables(glueDB string) ([]localStagingEntry, error) {
	dbDir := s.localNamespaceDir(glueDB)
	entries, err := os.ReadDir(dbDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]localStagingEntry, 0)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.Contains(name, stagingSuffix) {
			continue
		}
		sc, err := s.readStagingSidecar(glueDB, name)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		out = append(out, localStagingEntry{StagingTable: name, Sidecar: sc})
	}
	return out, nil
}

type localStagingEntry struct {
	StagingTable string
	Sidecar      stagingSidecar
}

// backfillStageLocal is the env=local twin of the cloud BackfillStage path.
// Resolves the canonical target from workspace + node config (canonicalTargetFor),
// reads inputs from the pipeline graph (buildInputs / external_inputs), and
// invokes the runner container with a transform event carrying the `_backfill`
// override block. Writes a sidecar JSON next to the staging Delta dir so
// BackfillList can find it later without a Glue lookup.
func (s *Service) backfillStageLocal(
	ctx context.Context,
	req BackfillStageRequest,
	g *graph.PipelineGraph,
	node *graph.Node,
	pipelineDir, pipelineName, runID string,
) (*BackfillRun, error) {
	canonicalTable, glueDB, outputKey, err := s.canonicalTargetFor(node, pipelineDir, pipelineName)
	if err != nil {
		return nil, fmt.Errorf("resolve canonical target: %w", err)
	}

	var stagingTableID string // two-part `<glueDB>.<node>__backfill__<id>` (canonical is bare for default-only outputs; see canonicalTableSegment)
	if req.Direct {
		stagingTableID = canonicalTable
	} else {
		stagingTableID = canonicalTable + stagingSuffix + runID
	}

	trigger := "backfill"
	if req.Direct {
		trigger = "backfill-direct"
	}

	// `_backfill.target_outputs` overrides the runner's auto-table for the
	// configured output key — same shape the cloud path threads through.
	// `_trigger` overrides runTransform's default "manual" so the snapshot
	// summary the runner stamps carries the backfill provenance.
	extraEvent := map[string]any{
		"_trigger": trigger,
		"_backfill": map[string]any{
			"node":           req.Node,
			"run_id":         runID,
			"from_cursor":    req.From,
			"to_cursor":      req.To,
			"target_outputs": map[string]string{outputKey: stagingTableID},
		},
	}

	// Resolve image + ADR-016 catalog/schema the same way prepareRun does,
	// so the runner writes to the same Hadoop catalog `pipeline run --env
	// local` would have written to.
	image := workspace.LocalRunnerImageTag(s.workspace)
	catalog := ""
	systemCatalog := ""
	if m, _ := workspace.Load(s.workspace); m != nil {
		catalog = m.CatalogIdentifier()
		systemCatalog = m.SystemCatalogIdentifier()
	}
	schema := resolvePipelineSchema(pipelineDir, pipelineName)

	// Seed outputPath / outputFormat for every transitive intra-pipeline
	// upstream of req.Node. The normal-run path (runPipelineBundle) populates
	// these as it walks the DAG in topo order; the single-node backfill skips
	// the walk because it targets one node directly, so we reconstruct the
	// seed map here from autoDeltaTableID — the same function the runner
	// uses to derive each transform's output id. Without this, buildInputs
	// errors `"upstream node X has not produced output yet"` for any edge
	// into req.Node, which breaks single-node backfills on multi-stage DAGs.
	outputPath, outputFormat := seedUpstreamPathsForBackfill(g, req.Node, catalog, schema)

	inputs, ierr := s.buildInputs(g, req.Node, outputPath, outputFormat, catalog)
	if ierr != nil {
		return nil, fmt.Errorf("resolve inputs: %w", ierr)
	}

	workdir, err := os.MkdirTemp("", "clavesa-backfill-")
	if err != nil {
		return nil, fmt.Errorf("create workdir: %w", err)
	}
	defer os.RemoveAll(workdir)

	started := time.Now()
	// Empty outputTarget — the `_backfill.target_outputs` override above
	// rewires the "default" key to the staging table; the default
	// outputTarget would never be consulted by the runner in backfill mode.
	_, _, rerr := s.runTransform(ctx, image, pipelineDir, workdir, node, inputs, "", runID, catalog, schema, systemCatalog, extraEvent)
	stopped := time.Now()

	run := &BackfillRun{
		RunID:          runID,
		Pipeline:       pipelineName,
		Node:           req.Node,
		OutputKey:      outputKey,
		From:           req.From,
		To:             req.To,
		Direct:         req.Direct,
		TargetTable:    stagingTableID,
		CanonicalTable: canonicalTable,
		StartedAt:      started,
		StoppedAt:      stopped,
		// A local-warehouse stage always ran on the local docker runner.
		Compute: "local",
		Status:  "ok",
	}
	if rerr != nil {
		run.Status = "failed"
		run.ErrorMsg = rerr.Error()
		return run, fmt.Errorf("runner: %w", rerr)
	}

	// Sidecar replaces the cloud Glue table parameters. Skip on --direct: a
	// direct backfill writes straight to the canonical table — no staging
	// artifact to track. Same convention as the cloud tagStagingTable skip.
	if !req.Direct {
		sc := stagingSidecar{
			RunID:          runID,
			Node:           req.Node,
			OutputKey:      outputKey,
			From:           req.From,
			To:             req.To,
			CanonicalTable: canonicalTable,
			StartedAt:      started,
			StoppedAt:      stopped,
		}
		if err := s.writeStagingSidecar(glueDB, lastSegment(stagingTableID), sc); err != nil {
			// Non-fatal: the staging table exists on disk; list-by-pattern
			// would still find the dir. Surface the warning via ErrorMsg
			// like the cloud tagStagingTable path does.
			run.ErrorMsg = fmt.Sprintf("staging table written but sidecar write failed: %v", err)
		}
	}
	return run, nil
}

// localTableDir maps a two-part local table id `<glueDB>.<table>` (the id
// form the local backfill path produces everywhere — see canonicalTargetFor
// and backfillListLocal) to its on-disk warehouse path. The Glue DB segment
// is the flat `<catalog>__<schema>` encoding, which contains no `.`, and the
// table segment (`<node>`, `<node>__<key>`, `<node>__backfill__<id>`) contains
// no `.` either, so the first `.` is the db↔table boundary. Resolution goes
// through observability.ResolveLocalTablePath — the canonical resolver that
// probes the ADR-019 V2 nested layout, the legacy Hive `.db` layout, and the
// legacy `__default` suffix, in that order. Returns ("", false) on a malformed
// id so callers can render a clean error.
func (s *Service) localTableDir(fullTableID string) (string, bool) {
	i := strings.IndexByte(fullTableID, '.')
	if i <= 0 || i >= len(fullTableID)-1 {
		return "", false
	}
	db, table := fullTableID[:i], fullTableID[i+1:]
	warehouse := workspace.LocalWarehouseDir(s.workspace)
	return observability.ResolveLocalTablePath(warehouse, db, table), true
}

// readLocalDeltaColumns reads the current Delta schema under tableDir and
// returns the column list (name + Spark-shaped type string). Same shape the
// cloud path gets from Athena's INFORMATION_SCHEMA.columns, so BackfillDiff
// can compare schemas across both modes with identical formatting — delta's
// reader already renders the canonical Spark type strings (`bigint`,
// `decimal(10,2)`, `array<string>`, `struct<a:int>`) Glue returns for cloud.
//
// Returns (_, false) when the dir isn't a valid Delta table (no `_delta_log/`,
// missing dir, malformed commit) — the caller distinguishes "no such table"
// (canonical not yet created) from "schema lookup failed" by walking the
// bool, not the error.
func readLocalDeltaColumns(tableDir string) ([]BackfillColumnInfo, bool) {
	schema, _, err := delta.ReadCurrentFromPath(tableDir)
	if err != nil || schema == nil {
		return nil, false
	}
	cols := make([]BackfillColumnInfo, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		cols = append(cols, BackfillColumnInfo{Name: c.Name, Type: c.Type})
	}
	return cols, true
}

// localRowCount runs `SELECT COUNT(*) FROM <table>` through the runner's
// query mode and parses the single-cell int. Returns (-1, nil) when the
// table does not exist — matches the cloud path's CanonicalRows=-1
// signaling for a not-yet-created canonical.
func localRowCount(ctx context.Context, prov *observability.LocalProvider, pipelineDir, table string) (int64, error) {
	res, err := prov.Query(ctx, observability.QueryQuery{
		SQL:         fmt.Sprintf("SELECT COUNT(*) FROM %s", table),
		PipelineDir: pipelineDir,
	})
	if err != nil {
		return 0, err
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return 0, nil
	}
	var n int64
	_, _ = fmt.Sscanf(res.Rows[0][0], "%d", &n)
	return n, nil
}

// formatSchema renders a column list as the same multi-line "  <name> <type>\n"
// shape the cloud path produces via athenaSchema. Sorted by column order
// (preserved by the metadata-file reader), so the string-compare in the
// caller stays stable across runs.
func formatSchema(cols []BackfillColumnInfo) string {
	var b strings.Builder
	for _, c := range cols {
		fmt.Fprintf(&b, "  %s %s\n", c.Name, c.Type)
	}
	return b.String()
}

// localMergeKeyCounts mirrors athenaMergeKeyCounts: how many staging rows
// match canonical on the keys (would UPDATE) vs how many are new (would
// INSERT). Two EXISTS queries — same SQL the cloud path runs, just routed
// through the runner instead of Athena.
func localMergeKeyCounts(ctx context.Context, prov *observability.LocalProvider, pipelineDir, staging, canonical string, keys []string) (int64, int64, error) {
	keyEq := make([]string, len(keys))
	for i, k := range keys {
		keyEq[i] = fmt.Sprintf("t.%s = s.%s", k, k)
	}
	on := strings.Join(keyEq, " AND ")

	matchSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s s WHERE EXISTS (SELECT 1 FROM %s t WHERE %s)", staging, canonical, on)
	newSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s s WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE %s)", staging, canonical, on)

	match, err := queryCount(ctx, prov, pipelineDir, matchSQL)
	if err != nil {
		return 0, 0, fmt.Errorf("merge-key match count: %w", err)
	}
	newKey, err := queryCount(ctx, prov, pipelineDir, newSQL)
	if err != nil {
		return match, 0, fmt.Errorf("merge-key new count: %w", err)
	}
	return match, newKey, nil
}

// queryCount runs a SQL statement that returns a single COUNT(*) row and
// parses the single cell. Used by the merge-key match / new-key queries
// where the SQL is already shaped to return one int column.
func queryCount(ctx context.Context, prov *observability.LocalProvider, pipelineDir, sql string) (int64, error) {
	res, err := prov.Query(ctx, observability.QueryQuery{SQL: sql, PipelineDir: pipelineDir})
	if err != nil {
		return 0, err
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return 0, nil
	}
	var n int64
	_, _ = fmt.Sscanf(res.Rows[0][0], "%d", &n)
	return n, nil
}

// backfillDiffLocal compares staging vs canonical for one local backfill
// run. Schema reads go straight to the Iceberg metadata.json files (no
// Spark roundtrip); row counts and merge-key counts go through the runner
// query path because they need live data.
func (s *Service) backfillDiffLocal(ctx context.Context, dir, runID string) (*BackfillDiff, error) {
	runs, err := s.BackfillList(ctx, dir)
	if err != nil {
		return nil, err
	}
	var run *BackfillRun
	for i := range runs {
		if runs[i].RunID == runID {
			run = &runs[i]
			break
		}
	}
	if run == nil {
		return nil, fmt.Errorf("backfill: run_id %q not found", runID)
	}

	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, err
	}
	mode, mergeKeys := outputModeAndKeys(&g, run.Node, run.OutputKey)

	diff := &BackfillDiff{
		RunID:          run.RunID,
		StagingTable:   run.TargetTable,
		CanonicalTable: run.CanonicalTable,
		OutputMode:     mode,
		MergeKeys:      mergeKeys,
	}

	prov := observability.NewLocalProvider(s.workspace)

	// Staging schema from metadata (cheap, no Spark).
	stagingDir, ok := s.localTableDir(run.TargetTable)
	if !ok {
		return nil, fmt.Errorf("backfill: malformed staging table id %q", run.TargetTable)
	}
	if cols, ok := readLocalDeltaColumns(stagingDir); ok {
		diff.StagingColumns = cols
	}

	stagingRows, err := localRowCount(ctx, prov, abs, run.TargetTable)
	if err != nil {
		return nil, fmt.Errorf("count staging rows: %w", err)
	}
	diff.StagingRows = stagingRows

	canonicalDir, ok := s.localTableDir(run.CanonicalTable)
	if !ok {
		return nil, fmt.Errorf("backfill: malformed canonical table id %q", run.CanonicalTable)
	}
	canonicalCols, canonicalExists := readLocalDeltaColumns(canonicalDir)
	if !canonicalExists {
		diff.CanonicalRows = -1
		diff.SchemaMatches = true
		return diff, nil
	}
	canonicalRows, err := localRowCount(ctx, prov, abs, run.CanonicalTable)
	if err != nil {
		return nil, err
	}
	diff.CanonicalRows = canonicalRows

	stagingFormatted := formatSchema(stableSort(diff.StagingColumns))
	canonicalFormatted := formatSchema(stableSort(canonicalCols))
	if stagingFormatted == canonicalFormatted {
		diff.SchemaMatches = true
	} else {
		diff.SchemaMatches = false
		diff.SchemaDiff = fmt.Sprintf("staging:\n%scanonical:\n%s", stagingFormatted, canonicalFormatted)
	}

	if len(mergeKeys) > 0 {
		match, newKey, err := localMergeKeyCounts(ctx, prov, abs, run.TargetTable, run.CanonicalTable, mergeKeys)
		if err == nil {
			diff.MatchingKeyRows = match
			diff.NewKeyRows = newKey
		}
	}
	return diff, nil
}

// backfillDedupCheckLocal mirrors BackfillDedupCheck's body for env=local:
// validate the column against the staging schema, then ask the runner for
// matching / new-key counts on that one column (treated as an ad-hoc
// merge key the user is auditing before they pick it for `promote
// --force-dedup`).
func (s *Service) backfillDedupCheckLocal(ctx context.Context, dir string, run *BackfillRun, col string) (*BackfillDedupCheckResult, error) {
	stagingDir, ok := s.localTableDir(run.TargetTable)
	if !ok {
		return nil, fmt.Errorf("backfill: malformed staging table id %q", run.TargetTable)
	}
	cols, ok := readLocalDeltaColumns(stagingDir)
	if !ok {
		return nil, fmt.Errorf("backfill: lookup staging columns for %q", run.TargetTable)
	}
	valid := false
	for _, c := range cols {
		if c.Name == col {
			valid = true
			break
		}
	}
	if !valid {
		return nil, fmt.Errorf("column %q not found in staging table", col)
	}

	abs := s.resolveDir(dir)
	prov := observability.NewLocalProvider(s.workspace)

	canonicalDir, ok := s.localTableDir(run.CanonicalTable)
	if !ok {
		return nil, fmt.Errorf("backfill: malformed canonical table id %q", run.CanonicalTable)
	}
	if _, exists := readLocalDeltaColumns(canonicalDir); !exists {
		n, err := localRowCount(ctx, prov, abs, run.TargetTable)
		if err != nil {
			return nil, err
		}
		return &BackfillDedupCheckResult{MatchingRows: 0, NewRows: n}, nil
	}
	matching, newKey, err := localMergeKeyCounts(ctx, prov, abs, run.TargetTable, run.CanonicalTable, []string{col})
	if err != nil {
		return nil, err
	}
	return &BackfillDedupCheckResult{MatchingRows: matching, NewRows: newKey}, nil
}

// seedUpstreamPathsForBackfill reverse-BFS walks the intra-pipeline edges
// of g from targetNode and returns the (outputPath, outputFormat) maps the
// normal-run path would have built by the time it reached targetNode. Only
// transform-typed upstreams contribute Delta table identifiers — source
// nodes resolve through the `source_inputs` map in node Config (handled by
// buildInputs separately, not through outputPath), and external Glue / cross-
// pipeline refs resolve through `external_inputs`. The format value
// "iceberg" mirrors what runPipelineBundle stamps in run.go so buildInputs
// takes the same `spark.table(<id>)` path it does for an ordinary run.
func seedUpstreamPathsForBackfill(g *graph.PipelineGraph, targetNode, catalog, schema string) (map[string]string, map[string]string) {
	outputPath := map[string]string{}
	outputFormat := map[string]string{}
	if g == nil {
		return outputPath, outputFormat
	}
	nodeType := make(map[string]string, len(g.Nodes))
	for _, n := range g.Nodes {
		nodeType[n.ID] = n.Type
	}
	visited := map[string]bool{}
	queue := []string{targetNode}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if visited[cur] {
			continue
		}
		visited[cur] = true
		for _, e := range g.Edges {
			if e.ToNode != cur {
				continue
			}
			upstream := e.FromNode
			if visited[upstream] {
				continue
			}
			// Only transform-typed upstreams contribute a Delta table id.
			// Source nodes flow through node.Config["source_inputs"] in
			// buildInputs and don't need an outputPath entry.
			if nodeType[upstream] != "transform" {
				continue
			}
			outputPath[upstream] = autoDeltaTableID(catalog, schema, upstream)
			outputFormat[upstream] = "iceberg"
			queue = append(queue, upstream)
		}
	}
	return outputPath, outputFormat
}

// stableSort sorts a column list by name. The cloud path's
// information_schema.columns query is ORDER BY ordinal_position; on local
// the metadata.json field order *is* the ordinal order. Sorting both sides
// the same way here is the cheap way to make schema diffs stable across
// columns added in different orders.
func stableSort(in []BackfillColumnInfo) []BackfillColumnInfo {
	out := make([]BackfillColumnInfo, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// backfillListLocal scans the workspace warehouse for staging Iceberg
// directories that belong to this pipeline. The pipeline scopes the schema
// (ADR-016), and the schema scopes the Glue DB — so all of this pipeline's
// staging tables live under one warehouse subdirectory regardless of which
// transform produced them.
func (s *Service) backfillListLocal(dir string) ([]BackfillRun, error) {
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}
	pipelineName := strings.TrimSuffix(filepathBase(abs), "/")

	// Any transform resolves the same Glue DB (one DB per pipeline schema).
	var anyTransform *graph.Node
	for i := range g.Nodes {
		if g.Nodes[i].Type == "transform" {
			anyTransform = &g.Nodes[i]
			break
		}
	}
	if anyTransform == nil {
		return []BackfillRun{}, nil
	}
	_, glueDB, _, err := s.canonicalTargetFor(anyTransform, abs, pipelineName)
	if err != nil {
		return nil, err
	}

	entries, err := s.listLocalStagingTables(glueDB)
	if err != nil {
		return nil, err
	}
	runs := make([]BackfillRun, 0, len(entries))
	for _, e := range entries {
		runs = append(runs, BackfillRun{
			RunID:          e.Sidecar.RunID,
			Pipeline:       pipelineName,
			Node:           e.Sidecar.Node,
			OutputKey:      e.Sidecar.OutputKey,
			From:           e.Sidecar.From,
			To:             e.Sidecar.To,
			TargetTable:    fmt.Sprintf("%s.%s", glueDB, e.StagingTable),
			CanonicalTable: e.Sidecar.CanonicalTable,
			StartedAt:      e.Sidecar.StartedAt,
			StoppedAt:      e.Sidecar.StoppedAt,
			Compute:        "local",
			Status:         "ok",
		})
	}
	return runs, nil
}
