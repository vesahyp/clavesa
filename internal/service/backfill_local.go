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

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// Local-mode backfill replaces Glue table tagging with a sidecar JSON file
// next to the staging Iceberg directory. Same key/value shape as the cloud
// Glue Parameters map so BackfillList reconstructs an identical BackfillRun.
//
// File layout:
//
//	<workspace>/.clavesa/warehouse/<glueDB>/<staging_table>.backfill.json
//
// The staging Iceberg directory lives at
//
//	<workspace>/.clavesa/warehouse/<glueDB>/<staging_table>/
//
// — same parent dir, so a single readdir scan finds both the staging
// directories and their metadata.

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

// stagingSidecarPath returns the absolute path of the sidecar JSON for the
// given staging table. glueDB is the encoded `<catalog>__<schema>` namespace;
// stagingTable is just the table-name segment (no `clavesa.<db>.` prefix).
func (s *Service) stagingSidecarPath(glueDB, stagingTable string) string {
	return filepath.Join(workspace.LocalWarehouseDir(s.workspace), glueDB, stagingTable+".backfill.json")
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

// listLocalStagingTables scans the warehouse db directory for staging Iceberg
// table directories (`*__backfill__*`) paired with their sidecar JSON. Returns
// (stagingTable, sidecar) pairs in directory order. A staging dir with no
// sidecar — and a sidecar with no staging dir — are both skipped (would never
// pair into a usable BackfillRun, so listing them just confuses the user).
func (s *Service) listLocalStagingTables(glueDB string) ([]localStagingEntry, error) {
	dbDir := filepath.Join(workspace.LocalWarehouseDir(s.workspace), glueDB)
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
// override block. Writes a sidecar JSON next to the staging Iceberg dir so
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

	var stagingTableID string // fully-qualified, e.g. clavesa.<db>.<node>__default__backfill__<id>
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
	image := runner.LocalImageName("") + ":latest"
	catalog := ""
	systemCatalog := ""
	if m, _ := workspace.Load(s.workspace); m != nil {
		image = runner.LocalImageName(m.Name) + ":latest"
		catalog = m.CatalogIdentifier()
		systemCatalog = m.SystemCatalogIdentifier()
	}
	schema := resolvePipelineSchema(pipelineDir, pipelineName)

	inputs, ierr := s.buildInputs(g, req.Node, map[string]string{}, map[string]string{}, catalog)
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
	_, _, rerr := s.runTransform(ctx, image, pipelineDir, workdir, node, inputs, "", "", runID, catalog, schema, systemCatalog, extraEvent)
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
		Status:         "ok",
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

// localTableDir maps a fully-qualified table id `clavesa.<db>.<name>` to its
// on-disk warehouse path. Returns ("", false) on a malformed id so callers
// can render a clean error.
func (s *Service) localTableDir(fullTableID string) (string, bool) {
	parts := strings.SplitN(fullTableID, ".", 3)
	if len(parts) != 3 {
		return "", false
	}
	return filepath.Join(workspace.LocalWarehouseDir(s.workspace), parts[1], parts[2]), true
}

// readLocalIcebergColumns parses the latest metadata.json under tableDir and
// returns the column list (name + Spark-shaped type string). Same shape the
// cloud path gets from Athena's INFORMATION_SCHEMA.columns, so BackfillDiff
// can compare schemas across both modes with identical formatting.
//
// Returns (_, false) when the dir isn't a valid Iceberg table — caller
// distinguishes "no such table" (canonical not yet created) from "schema
// lookup failed" (transient I/O) by walking the bool, not the error.
func readLocalIcebergColumns(tableDir string) ([]BackfillColumnInfo, bool) {
	metaDir := filepath.Join(tableDir, "metadata")
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		return nil, false
	}
	best := ""
	for _, e := range entries {
		n := e.Name()
		if !strings.HasPrefix(n, "v") || !strings.HasSuffix(n, ".metadata.json") {
			continue
		}
		if best == "" || lexLessIceberg(best, n) {
			best = n
		}
	}
	if best == "" {
		return nil, false
	}
	body, err := os.ReadFile(filepath.Join(metaDir, best))
	if err != nil {
		return nil, false
	}
	var meta struct {
		CurrentSchemaID int `json:"current-schema-id"`
		Schemas         []struct {
			SchemaID int `json:"schema-id"`
			Fields   []struct {
				Name string      `json:"name"`
				Type interface{} `json:"type"`
			} `json:"fields"`
		} `json:"schemas"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, false
	}
	for _, sch := range meta.Schemas {
		if sch.SchemaID != meta.CurrentSchemaID {
			continue
		}
		cols := make([]BackfillColumnInfo, 0, len(sch.Fields))
		for _, f := range sch.Fields {
			cols = append(cols, BackfillColumnInfo{Name: f.Name, Type: icebergTypeString(f.Type)})
		}
		return cols, true
	}
	return nil, false
}

// lexLessIceberg compares two metadata filenames so v10 sorts after v9.
// Strips the leading "v" and trailing ".metadata.json", parses the rest as
// an int, and compares numerically — same algorithm catalog_local.go uses.
func lexLessIceberg(a, b string) bool {
	num := func(s string) int {
		s = strings.TrimPrefix(s, "v")
		s = strings.TrimSuffix(s, ".metadata.json")
		var n int
		_, _ = fmt.Sscanf(s, "%d", &n)
		return n
	}
	return num(a) < num(b)
}

// icebergTypeString flattens an Iceberg type JSON value to a printable
// Spark-shaped type string. Primitives serialize as bare strings ("long",
// "string"); structs/lists/maps as JSON objects. We render the object form
// as a compact JSON string — surface diffs work either way, the user only
// needs a stable representation.
func icebergTypeString(t interface{}) string {
	if s, ok := t.(string); ok {
		return s
	}
	b, _ := json.Marshal(t)
	return string(b)
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
	if cols, ok := readLocalIcebergColumns(stagingDir); ok {
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
	canonicalCols, canonicalExists := readLocalIcebergColumns(canonicalDir)
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
	cols, ok := readLocalIcebergColumns(stagingDir)
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
	if _, exists := readLocalIcebergColumns(canonicalDir); !exists {
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
			TargetTable:    fmt.Sprintf("clavesa.%s.%s", glueDB, e.StagingTable),
			CanonicalTable: e.Sidecar.CanonicalTable,
			StartedAt:      e.Sidecar.StartedAt,
			StoppedAt:      e.Sidecar.StoppedAt,
			Status:         "ok",
		})
	}
	return runs, nil
}
