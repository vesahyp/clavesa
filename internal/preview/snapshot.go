package preview

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// ResolveUpstreamFromSnapshot returns sample rows from an upstream
// transform's already-materialized Iceberg snapshot when the snapshot
// is fresh — saving a full upstream re-execution (and the source
// re-fetch behind it) per Preview.
//
// "Fresh" means: the snapshot exists in the local warehouse, and no
// pipeline file (.tf, .sql, .py) has been modified since the snapshot
// was committed. The mtime check is intentionally coarse — any edit
// in the pipeline dir invalidates every node's snapshot — because the
// runner does not stamp a per-node content hash today. A more
// surgical signal is a separate slice; the conservative version never
// serves wrong rows.
//
// Returns (rows, true, nil) on a hit, (nil, false, nil) on a cold or
// stale path (caller falls back to executing the upstream), or
// (nil, false, err) only on a hard failure that should bubble up.
//
// `dir` is the absolute pipeline directory (workspace root joined
// with the pipeline subdir); `root` is the workspace root.
func ResolveUpstreamFromSnapshot(
	ctx context.Context,
	root, dir string,
	parent *graph.Node,
	rowCount int,
) ([]map[string]interface{}, bool, error) {
	if parent.Type != "transform" {
		return nil, false, nil
	}
	glueDB := pipelineGlueDB(root, dir)
	if glueDB == "" {
		return nil, false, nil
	}
	// v1 chains only the default output. A downstream that reads a
	// non-default output (multi-output transform) falls back to
	// re-execute — preserves correctness without expanding scope.
	table := identutil.Sanitize(parent.ID) + "__default"

	warehouse := workspace.LocalWarehouseDir(root)
	if warehouse == "" {
		return nil, false, nil
	}
	metaDir := filepath.Join(warehouse, glueDB, table, "metadata")
	snapMtime, ok := latestSnapshotMtime(metaDir)
	if !ok {
		return nil, false, nil
	}

	dirMtime, err := latestPipelineSourceMtime(dir)
	if err != nil {
		return nil, false, nil
	}
	if !dirMtime.IsZero() && dirMtime.After(snapMtime) {
		return nil, false, nil
	}

	localTag := runner.LocalImageName("") + ":latest"
	if _, err := workspace.Load(root); err == nil {
		ensured, err := workspace.EnsureLocalRunnerImage(root)
		if err != nil {
			// Snapshot path is best-effort fallback to re-execute — don't
			// surface a docker rebuild failure here; let the caller's
			// re-exec path try (and likely fail with the same error,
			// loudly). Matches the surrounding error-swallowing style.
			return nil, false, nil
		}
		localTag = ensured
	}
	image, _ := parent.Config["runner_image"].(string)
	sql := fmt.Sprintf(
		"SELECT * FROM clavesa.%s.%s LIMIT %d",
		glueDB, table, rowCount,
	)
	rows, err := QueryWarehouseTable(ctx, localTag, image, warehouse, sql)
	if err != nil {
		// The runner can fail for many reasons (missing image, Docker
		// not running). None of these should sink the preview — the
		// caller's re-execute path will exercise the same runner and
		// surface a useful error there.
		return nil, false, nil
	}
	// Stringify cell values so the downstream preview runner infers a
	// stable per-column type. QueryWarehouseTable JSON-decodes raw
	// runner output, so a numeric column with both 0 (int) and 22.7
	// (float) becomes mixed int64/float64 in the rows; Spark then
	// fails with CANNOT_MERGE_TYPE during schema inference. Source-
	// fetch rows are already string-valued (preview.Convert), so this
	// matches that shape.
	for _, r := range rows {
		for k, v := range r {
			r[k] = stringifyCell(v)
		}
	}
	log.Printf("preview: using snapshot for upstream %s (%s.%s)", parent.ID, glueDB, table)
	return rows, true, nil
}

// stringifyCell renders a runner-emitted cell value as its JSON-like
// string. Nil stays nil so the downstream sees a real NULL.
func stringifyCell(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		// Integer-valued floats render without a fractional part to
		// match how the source-fetch path stringifies the same value.
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	}
	return fmt.Sprintf("%v", v)
}

// pipelineGlueDB encodes the Glue/Iceberg namespace for a pipeline's
// outputs — mirror of observability.Resolver.GlueDBFor's lookup, but
// inlined here so this package doesn't depend on observability.
func pipelineGlueDB(root, dir string) string {
	catalog := ""
	if m, _ := workspace.Load(root); m != nil {
		catalog = m.CatalogIdentifier()
	}
	schema := readPipelineSchemaDefault(dir)
	if schema == "" {
		schema = filepath.Base(dir)
	}
	return identutil.EncodeGlueDatabase(catalog, schema)
}

// readPipelineSchemaDefault scans variables.tf for the default value
// of `variable "schema"`. Same shape as observability.readSchemaDefault
// and service.resolvePipelineSchema; replicated to avoid an
// observability dependency here.
func readPipelineSchemaDefault(dir string) string {
	data, err := os.ReadFile(filepath.Join(dir, "variables.tf"))
	if err != nil {
		return ""
	}
	inBlock := false
	for _, line := range strings.Split(string(data), "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, `variable "schema"`) {
			inBlock = true
			continue
		}
		if !inBlock {
			continue
		}
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
	return ""
}

// latestSnapshotMtime returns the newest mtime among the
// metadata.json files in an Iceberg table's metadata dir, which
// reliably bumps on every commit. Returns false when the dir or any
// metadata.json is missing — caller treats that as "no snapshot yet".
func latestSnapshotMtime(metaDir string) (time.Time, bool) {
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		return time.Time{}, false
	}
	var latest time.Time
	found := false
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".metadata.json") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(latest) {
			latest = info.ModTime()
			found = true
		}
	}
	return latest, found
}

// latestPipelineSourceMtime returns the newest mtime among the
// pipeline's authoring files — .tf, .sql, .py — used as the staleness
// signal. .json files (orchestration emit, lineage) are derived and
// excluded; subdirectories (.clavesa/, scripts/) are not scanned
// because the watermarks they own bump on each run, which would make
// every snapshot look stale immediately after writing.
func latestPipelineSourceMtime(absDir string) (time.Time, error) {
	entries, err := os.ReadDir(absDir)
	if err != nil {
		return time.Time{}, err
	}
	var latest time.Time
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".tf") &&
			!strings.HasSuffix(name, ".sql") &&
			!strings.HasSuffix(name, ".py") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().After(latest) {
			latest = info.ModTime()
		}
	}
	return latest, nil
}
