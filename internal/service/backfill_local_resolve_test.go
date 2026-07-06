package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/identutil"
)

// writeDeltaFixture lays out a minimal Delta table at tableDir: a
// `_delta_log/00000000000000000000.json` carrying a protocol + metaData +
// commitInfo, where the metaData's schemaString encodes the given columns as
// a Spark struct. Mirrors the fixture shape internal/delta's own tests build,
// so readLocalDeltaColumns exercises the real delta.ReadCurrentFromPath path.
func writeDeltaFixture(t *testing.T, tableDir string, cols [][2]string) {
	t.Helper()
	logDir := filepath.Join(tableDir, "_delta_log")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("mkdir _delta_log: %v", err)
	}
	fields := make([]map[string]any, 0, len(cols))
	for _, c := range cols {
		fields = append(fields, map[string]any{
			"name": c[0], "type": c[1], "nullable": true, "metadata": map[string]any{},
		})
	}
	schemaObj := map[string]any{"type": "struct", "fields": fields}
	schemaBytes, err := json.Marshal(schemaObj)
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}
	// schemaString is a JSON string whose value is itself the JSON struct.
	schemaQuoted, err := json.Marshal(string(schemaBytes))
	if err != nil {
		t.Fatalf("marshal schemaString: %v", err)
	}
	body := `{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}` + "\n" +
		`{"metaData":{"id":"t","format":{"provider":"parquet"},"schemaString":` + string(schemaQuoted) + `,"partitionColumns":[],"configuration":{}}}` + "\n" +
		`{"commitInfo":{"timestamp":1700000000000,"operation":"CREATE TABLE"}}` + "\n"
	if err := os.WriteFile(filepath.Join(logDir, "00000000000000000000.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("write commit: %v", err)
	}
}

// warehouseFor returns the local warehouse dir for a scratch workspace root.
func warehouseFor(root string) string {
	return filepath.Join(root, ".clavesa", "warehouse")
}

// TestLocalTableDirResolvesV2AndLegacy asserts localTableDir resolves a
// two-part `<glueDB>.<table>` id to the on-disk Delta directory under both the
// ADR-019 V2 nested layout and the legacy Hive `<db>.db/` layout — the two
// layouts observability.ResolveLocalTablePath probes.
func TestLocalTableDirResolvesV2AndLegacy(t *testing.T) {
	t.Parallel()
	catalog, schema := "clavesa_ws", "demo"
	glueDB := identutil.EncodeGlueDatabase(catalog, schema) // clavesa_ws__demo

	t.Run("v2", func(t *testing.T) {
		root := t.TempDir()
		s := New(root)
		wh := warehouseFor(root)
		v2 := filepath.Join(wh, catalog, schema, "trips")
		writeDeltaFixture(t, v2, [][2]string{{"id", "long"}, {"amount", "double"}})

		got, ok := s.localTableDir(glueDB + ".trips")
		if !ok {
			t.Fatalf("localTableDir(%q) returned ok=false", glueDB+".trips")
		}
		if got != v2 {
			t.Fatalf("localTableDir = %q, want %q", got, v2)
		}
		cols, ok := readLocalDeltaColumns(got)
		if !ok {
			t.Fatalf("readLocalDeltaColumns(%q) returned ok=false", got)
		}
		if len(cols) != 2 || cols[0].Name != "id" || cols[0].Type != "long" || cols[1].Name != "amount" || cols[1].Type != "double" {
			t.Fatalf("columns = %+v, want [{id long} {amount double}]", cols)
		}
	})

	t.Run("legacy", func(t *testing.T) {
		root := t.TempDir()
		s := New(root)
		wh := warehouseFor(root)
		legacy := filepath.Join(wh, glueDB+".db", "trips")
		writeDeltaFixture(t, legacy, [][2]string{{"id", "long"}})

		got, ok := s.localTableDir(glueDB + ".trips")
		if !ok {
			t.Fatalf("localTableDir returned ok=false")
		}
		if got != legacy {
			t.Fatalf("localTableDir = %q, want %q", got, legacy)
		}
		cols, ok := readLocalDeltaColumns(got)
		if !ok || len(cols) != 1 || cols[0].Name != "id" {
			t.Fatalf("columns = %+v ok=%v, want [{id long}]", cols, ok)
		}
	})
}

// TestLocalTableDirRejectsMalformedID guards the arity contract: a bare table
// name with no `<glueDB>.` prefix (and an empty-segment id) must return
// ok=false so callers render a clean "malformed staging table id" error rather
// than a bogus path.
func TestLocalTableDirRejectsMalformedID(t *testing.T) {
	t.Parallel()
	s := New(t.TempDir())
	for _, id := range []string{"nodot", ".", "db.", ".table", ""} {
		if _, ok := s.localTableDir(id); ok {
			t.Errorf("localTableDir(%q) = ok=true, want false", id)
		}
	}
}

// TestReadLocalDeltaColumnsMissingTable asserts readLocalDeltaColumns reports
// ok=false (not an error) when the directory isn't a Delta table — the signal
// backfillDiffLocal uses to distinguish a not-yet-created canonical from a
// live one.
func TestReadLocalDeltaColumnsMissingTable(t *testing.T) {
	t.Parallel()
	if cols, ok := readLocalDeltaColumns(filepath.Join(t.TempDir(), "does-not-exist")); ok {
		t.Errorf("readLocalDeltaColumns on missing dir = ok=true (%+v), want false", cols)
	}
}

// TestStagingIDRoundTrip is the arity regression test: the id the stage path
// produces (canonicalTable + stagingSuffix + runID) must parse back through
// localTableDir to the on-disk staging directory. Before the fix the stage
// path emitted a two-part id while localTableDir expected three parts, so
// every diff/dedup call failed with "malformed staging table id".
func TestStagingIDRoundTrip(t *testing.T) {
	t.Parallel()
	catalog, schema, node, runID := "clavesa_ws", "demo", "trips", "20260705T000000"
	glueDB := identutil.EncodeGlueDatabase(catalog, schema)
	// Default-only output → bare canonical table name (canonicalTableSegment).
	canonicalTable := glueDB + "." + node
	stagingID := canonicalTable + stagingSuffix + runID

	root := t.TempDir()
	s := New(root)
	wh := warehouseFor(root)
	stagingDir := filepath.Join(wh, catalog, schema, node+stagingSuffix+runID)
	writeDeltaFixture(t, stagingDir, [][2]string{{"id", "long"}, {"payment", "string"}})

	got, ok := s.localTableDir(stagingID)
	if !ok {
		t.Fatalf("localTableDir(%q) ok=false", stagingID)
	}
	if got != stagingDir {
		t.Fatalf("localTableDir = %q, want %q", got, stagingDir)
	}
	cols, ok := readLocalDeltaColumns(got)
	if !ok || len(cols) != 2 {
		t.Fatalf("columns = %+v ok=%v, want 2 columns", cols, ok)
	}
}

// TestListLocalStagingTablesV2 asserts the staging-dir scan finds a staging
// table + sidecar written under the V2 nested namespace, and that the sidecar
// write/read/list round-trips. Before the fix listLocalStagingTables scanned
// the retired `<db>.db/` layout and returned nothing on a V2 workspace.
func TestListLocalStagingTablesV2(t *testing.T) {
	t.Parallel()
	catalog, schema, node, runID := "clavesa_ws", "demo", "trips", "run-1"
	glueDB := identutil.EncodeGlueDatabase(catalog, schema)
	staging := node + stagingSuffix + runID

	root := t.TempDir()
	s := New(root)
	wh := warehouseFor(root)

	// V2 staging table dir must exist for the scan to pair it with a sidecar.
	stagingDir := filepath.Join(wh, catalog, schema, staging)
	writeDeltaFixture(t, stagingDir, [][2]string{{"id", "long"}})

	sc := stagingSidecar{
		RunID:          runID,
		Node:           node,
		OutputKey:      "default",
		From:           []string{"2026-01-01"},
		To:             []string{"2026-01-02"},
		CanonicalTable: glueDB + "." + node,
	}
	if err := s.writeStagingSidecar(glueDB, staging, sc); err != nil {
		t.Fatalf("writeStagingSidecar: %v", err)
	}
	// The sidecar must land beside the table dir under the V2 namespace.
	if _, err := os.Stat(filepath.Join(wh, catalog, schema, staging+".backfill.json")); err != nil {
		t.Fatalf("sidecar not written to V2 namespace: %v", err)
	}

	entries, err := s.listLocalStagingTables(glueDB)
	if err != nil {
		t.Fatalf("listLocalStagingTables: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1 (%+v)", len(entries), entries)
	}
	if entries[0].StagingTable != staging {
		t.Errorf("StagingTable = %q, want %q", entries[0].StagingTable, staging)
	}
	if entries[0].Sidecar.RunID != runID || entries[0].Sidecar.Node != node {
		t.Errorf("sidecar = %+v, want RunID=%q Node=%q", entries[0].Sidecar, runID, node)
	}

	// A staging dir with no sidecar must be skipped.
	orphanDir := filepath.Join(wh, catalog, schema, node+stagingSuffix+"orphan")
	writeDeltaFixture(t, orphanDir, [][2]string{{"id", "long"}})
	entries, err = s.listLocalStagingTables(glueDB)
	if err != nil {
		t.Fatalf("listLocalStagingTables (with orphan): %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("orphan staging dir should be skipped, entries = %d", len(entries))
	}
}
