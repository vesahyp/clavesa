// Package delta reads the minimal subset of a Delta Lake transaction log
// that clavesa's catalog page and observability layer need — the current
// schema and the recent commit history. It is not a full Delta protocol
// implementation; data file reads, predicate pushdown, deletion vectors,
// column mapping, and protocol upgrades all live outside this package.
//
// The reader operates against an io/fs.FS rooted at a table's
// `_delta_log/` directory. Local callers wrap with os.DirFS;
// cloud callers wrap a small S3-backed FS (see internal/delta/s3fs).
//
// Protocol references:
//   - Delta transaction log spec:
//     https://github.com/delta-io/delta/blob/master/PROTOCOL.md
//   - commitInfo userMetadata (clavesa stamps provenance here in sub-slice 3):
//     https://docs.delta.io/latest/delta-utility.html#retrieve-delta-table-history
package delta

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	parquetgo "github.com/parquet-go/parquet-go"
)

// ErrNotDelta is returned by ReadCurrent when the supplied filesystem has
// no readable commit files — i.e. the directory isn't a Delta `_delta_log`
// at all, or is an empty `_delta_log` shell with only sidecar files.
// Callers (e.g. catalog_local.go) typically swallow this and skip the
// directory rather than surfacing it.
var ErrNotDelta = errors.New("not a Delta table")

// Column is one field in a Delta table's schema, rendered into the
// canonical Spark SQL type string the rest of clavesa already consumes
// (the same shape Glue's StorageDescriptor.Columns returns for cloud
// tables — `bigint`, `decimal(10,2)`, `array<string>`, `struct<a:int>`).
type Column struct {
	Name     string
	Type     string
	Nullable bool
}

// Schema is the column list of a Delta table at its latest commit.
// Partition columns (Delta tracks them separately on the metaData
// action) are included in Columns the same way Spark's `DESCRIBE TABLE`
// reports them — the catalog UI doesn't distinguish partition from
// non-partition columns today.
type Schema struct {
	Columns []Column
}

// Commit is one entry in a Delta table's history. Version is parsed from
// the commit file name; TimestampMs comes from the commitInfo action's
// `timestamp` field (millis since epoch). Operation, UserMetadata, and
// the metric counts are all optional; older commits or commits written
// by tools that don't stamp them leave the fields empty / nil.
//
// AddedRecords / DeletedRecords / TotalRecords are derived from Delta's
// commitInfo.operationMetrics map and mirror the columns the legacy
// Iceberg `<table>$snapshots` query exposed — so callers (snapshot
// timeline UI, observability SnapshotInfo response) don't have to
// re-encode the mapping per site. The runner's `_record_table_state`
// uses the same operation → metric mapping; this is the Go-side port.
type Commit struct {
	Version        int64
	TimestampMs    int64
	Operation      string
	UserMetadata   string
	AddedRecords   *int64
	DeletedRecords *int64
	TotalRecords   *int64
	// UpdatedRecords is the MERGE `numTargetRowsUpdated` value when the
	// commit was a MERGE that touched rows in place. Updates don't change
	// the table-state row count even though they're folded into
	// AddedRecords for the timeline display; LatestRecordCount aggregation
	// needs the discriminator to net them out.
	UpdatedRecords *int64
	// Replaces is true when the commit overwrites the table state (CTAS,
	// CREATE OR REPLACE, WRITE with mode=Overwrite). Running-sum row-count
	// math resets to this commit's AddedRecords when true.
	Replaces bool
}

// maxCommitsScanned bounds the history walk so a long-lived table with
// thousands of commits doesn't pin the catalog page on disk reads. 200
// matches the default Delta UI shows for `DESCRIBE HISTORY` and covers
// the recency-trend window the snapshot timeline renders.
const maxCommitsScanned = 200

// commitFileRe matches the 20-digit numeric prefix Delta uses for commit
// JSON files (`00000000000000000000.json`, etc.). Anything else in
// `_delta_log/` — `.crc` companions, `_commits/`, `_changelog/`, etc. —
// is skipped.
var commitFileRe = regexp.MustCompile(`^([0-9]{20})\.json$`)

// checkpointSingleRe matches a single-part Delta checkpoint:
// `<20-digit-version>.checkpoint.parquet`. The checkpoint snapshots the
// table state (every live `add`, the active `metaData`, the `protocol`)
// at that version so a reader doesn't have to replay the commits before
// it. Delta writes one every checkpointInterval commits (10 by default).
var checkpointSingleRe = regexp.MustCompile(`^([0-9]{20})\.checkpoint\.parquet$`)

// checkpointMultiRe matches one part of a multi-part Delta checkpoint:
// `<20-digit-version>.checkpoint.<10-digit-part>.<10-digit-numParts>.parquet`.
// Large tables split the checkpoint across N parts; all N share the same
// version prefix and carry the same numParts. The active `metaData` lives
// in exactly one part (whichever row group it landed in), so the reader
// scans parts until it finds it. Capture groups: version, part, numParts.
var checkpointMultiRe = regexp.MustCompile(`^([0-9]{20})\.checkpoint\.([0-9]{10})\.([0-9]{10})\.parquet$`)

// ReadSchema loads ONLY the latest schema from a `_delta_log` filesystem,
// the single thing the catalog page needs. `logFS` must be rooted at the
// `_delta_log` directory (NOT the table root); ReadDir(".") on it lists
// the commit and checkpoint files. Returns ErrNotDelta when the directory
// holds no commit files AND no checkpoint — the same "silently skip
// non-Delta directories" contract ReadCurrent honors.
//
// It is checkpoint-aware: on an append-only table that only ever wrote
// `metaData` at version 0 (e.g. `node_runs`), the schema resolver reads
// the schema out of the latest checkpoint parquet and walks at most the
// handful of JSON commits written after that checkpoint, rather than
// replaying every commit back to version 0. On a 2551-commit table this
// turns ~2551 sequential S3 GetObject calls into a handful. Use this in
// preference to ReadCurrent wherever the commit history is not needed.
func ReadSchema(logFS fs.FS) (*Schema, error) {
	idx, err := listLog(logFS)
	if err != nil {
		return nil, err
	}
	if len(idx.versions) == 0 && !idx.hasCheckpoint() {
		return nil, ErrNotDelta
	}
	getActions := func(v int64) ([]rawAction, error) {
		return readCommitActions(logFS, idx.versionToFile[v])
	}
	return resolveSchema(idx, getActions, checkpointLoader(logFS, idx))
}

// ReadCurrent loads the latest schema + recent commit history from a
// `_delta_log` filesystem. `logFS` must be rooted at the `_delta_log`
// directory (NOT the table root); ReadDir(".") on it lists the commit
// files. Returns ErrNotDelta when no valid commit files are found —
// matches the catalog walker's "silently skip non-Delta directories"
// contract.
//
// Thin compatibility wrapper over ReadTableState for callers that don't
// need the row count. A malformed commit file surfaces as an error rather
// than a silent skip — silent skips would hide schema-evolution bugs that
// a future refactor introduces.
func ReadCurrent(logFS fs.FS) (*Schema, []Commit, error) {
	st, err := ReadTableState(logFS)
	if err != nil {
		return nil, nil, err
	}
	return st.Schema, st.Commits, nil
}

// TableState bundles everything ReadTableState resolves in one pass over a
// `_delta_log`: the current schema, the recent commit history (newest
// first, capped at maxCommitsScanned), and — when derivable — the table's
// exact current row count.
type TableState struct {
	Schema  *Schema
	Commits []Commit
	// RowCount is the exact number of rows in the table at its latest
	// version, computed from Delta snapshot state (checkpoint
	// `add.stats.numRecords` seeded, post-checkpoint commits replayed) —
	// NOT by summing per-commit operation metrics, so it stays correct for
	// MERGE / append tables whose history outruns the commit window and
	// log retention (GH #66). nil when the log doesn't pin down the live
	// file set (no checkpoint and the surviving window doesn't reach
	// version 0, or a version gap) or when any live file lacks readable
	// numRecords stats (stats collection disabled, or a stats-as-struct
	// checkpoint without the JSON stats column). Callers fall back to
	// their own estimate in that case.
	RowCount *int64
}

// ReadTableState loads schema + recent history + exact row count in a
// single pass over the log. It fetches nothing beyond what ReadCurrent
// already fetched: the window's commit files are read exactly once and
// shared by the history projection, the schema resolver, and the row-count
// replay (the pre-state reader used to read the post-checkpoint commits
// twice), and the checkpoint parts are read once and shared between the
// schema and row-count projections. The only case that reads an extra file
// is a schema-evolution commit after the latest checkpoint, where the
// schema alone wouldn't have needed the checkpoint but the row count does.
func ReadTableState(logFS fs.FS) (*TableState, error) {
	idx, err := listLog(logFS)
	if err != nil {
		return nil, err
	}
	// History needs commit files; a `_delta_log` carrying only a
	// checkpoint with no surviving JSON commits can't produce a timeline.
	// In practice Delta never deletes the checkpoint's own commit, so
	// versions is non-empty whenever a real table exists; the guard keeps
	// the ErrNotDelta contract identical to the pre-checkpoint reader.
	if len(idx.versions) == 0 {
		return nil, ErrNotDelta
	}

	win, err := readWindow(logFS, idx)
	if err != nil {
		return nil, err
	}
	getActions := func(v int64) ([]rawAction, error) {
		// Serve from the pre-read window when possible; only versions
		// below the window (no-checkpoint tables with >maxCommitsScanned
		// commits) fall through to a filesystem read.
		for i := len(win) - 1; i >= 0; i-- {
			if win[i].version == v {
				return win[i].actions, nil
			}
			if win[i].version < v {
				break
			}
		}
		return readCommitActions(logFS, idx.versionToFile[v])
	}
	loadCP := checkpointLoader(logFS, idx)

	schema, err := resolveSchema(idx, getActions, loadCP)
	if err != nil {
		return nil, err
	}

	rowCount, err := rowCountFromLog(idx, win, loadCP)
	if err != nil {
		return nil, err
	}

	return &TableState{
		Schema:   schema,
		Commits:  commitsFromWindow(logFS, win),
		RowCount: rowCount,
	}, nil
}

// checkpointLoader returns a memoizing loader for the latest checkpoint's
// part bytes. Both ReadSchema and ReadTableState hand it to resolveSchema
// (and the latter to rowCountFromLog) so a checkpoint is fetched at most
// once per read regardless of how many projections consume it. The loader
// returns (nil, nil) when the log has no checkpoint.
func checkpointLoader(logFS fs.FS, ix *logIndex) func() (*checkpointData, error) {
	var cd *checkpointData
	return func() (*checkpointData, error) {
		if cd != nil {
			return cd, nil
		}
		_, parts, ok := ix.latestCheckpoint()
		if !ok {
			return nil, nil
		}
		loaded, err := loadCheckpoint(logFS, parts)
		if err != nil {
			return nil, err
		}
		cd = loaded
		return cd, nil
	}
}

// FileStats is the live-data-file summary of a Delta table at its latest
// version — the count of active files and their total byte size. "Active"
// follows the Delta protocol: a file is live once it has been `add`ed and
// stays live until a matching `remove` retires it. This is the local
// fast-path source for the per-table file-count / average-file-size
// observability the cloud side reads out of the workspace `tables` system
// table (GH #26), and it closes the local–cloud parity gap where the local
// reader used to leave those metrics nil (ADR-014).
type FileStats struct {
	// FileCount is the number of live data files at the latest version.
	FileCount int64
	// TotalBytes is the sum of `add.size` over those live files.
	TotalBytes int64
}

// ReadFileStats enumerates the live data files at a Delta table's latest
// version and returns their count and total byte size. `logFS` must be
// rooted at the `_delta_log` directory (NOT the table root), the same
// contract ReadSchema / ReadCurrent honor. Returns ErrNotDelta when the
// listing holds no commit files and no checkpoint.
//
// The live set is computed per the Delta protocol — every `add`ed file
// minus every `remove`d file, resolved at the latest version:
//
//   - When a checkpoint exists at version CV, its parquet snapshots the
//     live add-set at CV; we seed the live map from the checkpoint's `add`
//     rows (and apply any `remove` rows it carries), then replay only the
//     JSON commits with version > CV in ascending order. On a long-lived
//     table this reads a handful of files instead of every commit back to
//     version 0 — the same short-cut resolveSchema takes.
//   - Absent a checkpoint, we replay every JSON commit from version 0
//     ascending, applying each `add` / `remove` in turn.
//
// The map is keyed by the file path string (`add.path` / `remove.path`);
// `add.size` carries the byte size Delta records for each file. A
// malformed commit surfaces as an error rather than a silent skip,
// matching ReadCurrent's contract — a swallowed parse error would let a
// file-accounting regression ship green.
func ReadFileStats(logFS fs.FS) (*FileStats, error) {
	idx, err := listLog(logFS)
	if err != nil {
		return nil, err
	}
	if len(idx.versions) == 0 && !idx.hasCheckpoint() {
		return nil, ErrNotDelta
	}

	// live maps a data-file path to its byte size. A checkpoint seeds it;
	// post-checkpoint (or all, when there's no checkpoint) JSON commits
	// mutate it.
	live := make(map[string]int64)
	var startAfter int64 = -1 // replay commits strictly greater than this
	if cv, parts, ok := idx.latestCheckpoint(); ok {
		cd, err := loadCheckpoint(logFS, parts)
		if err != nil {
			return nil, err
		}
		if err := applyCheckpointFiles(cd, live); err != nil {
			return nil, err
		}
		startAfter = cv
	}

	// idx.versions is ascending; replay every commit past the checkpoint.
	for _, v := range idx.versions {
		if v <= startAfter {
			continue
		}
		actions, err := readCommitActions(logFS, idx.versionToFile[v])
		if err != nil {
			return nil, fmt.Errorf("read commit %d: %w", v, err)
		}
		for _, a := range actions {
			if a.Add != nil && a.Add.Path != "" {
				live[a.Add.Path] = a.Add.Size
			}
			if a.Remove != nil && a.Remove.Path != "" {
				delete(live, a.Remove.Path)
			}
		}
	}

	stats := &FileStats{}
	for _, size := range live {
		stats.FileCount++
		stats.TotalBytes += size
	}
	return stats, nil
}

// checkpointData holds the raw bytes of every part of one checkpoint,
// read off the filesystem exactly once. ReadTableState shares one
// checkpointData between the schema projection and the row-count
// projection so an S3-backed FS pays one GetObject per part, not one per
// projection.
type checkpointData struct {
	names []string
	data  [][]byte
}

// loadCheckpoint fetches every part of a checkpoint into memory. partFiles
// come pre-sorted by part number from logIndex.latestCheckpoint.
func loadCheckpoint(logFS fs.FS, partFiles []string) (*checkpointData, error) {
	cd := &checkpointData{names: partFiles}
	for _, name := range partFiles {
		data, err := fs.ReadFile(logFS, name)
		if err != nil {
			return nil, fmt.Errorf("read checkpoint part %s: %w", name, err)
		}
		cd.data = append(cd.data, data)
	}
	return cd, nil
}

// scanCheckpoint runs one typed parquet projection over every row of every
// checkpoint part, in part order. Row is the projection struct; parquet-go
// decodes only the leaf columns the struct references, and zero-fills
// fields whose columns are absent from the file (how pre-deletion-vector or
// stats-less checkpoints degrade rather than error). visit returns false to
// stop the scan early (the schema scan stops at the first metaData row).
func scanCheckpoint[Row any](cd *checkpointData, visit func(Row) bool) error {
	for i, data := range cd.data {
		name := cd.names[i]
		pf, err := parquetgo.OpenFile(newBytesReaderAt(data), int64(len(data)))
		if err != nil {
			return fmt.Errorf("open checkpoint part %s: %w", name, err)
		}
		reader := parquetgo.NewGenericReader[Row](pf)
		buf := make([]Row, 64)
		for {
			n, readErr := reader.Read(buf)
			for j := 0; j < n; j++ {
				if !visit(buf[j]) {
					reader.Close()
					return nil
				}
			}
			if readErr == io.EOF || n == 0 {
				break
			}
			if readErr != nil {
				reader.Close()
				return fmt.Errorf("read checkpoint part %s: %w", name, readErr)
			}
		}
		reader.Close()
	}
	return nil
}

// applyCheckpointFiles seeds live with the checkpoint's file view — its
// `add` rows become live entries (path → size) and any `remove` rows it
// carries retire the matching path. It projects to only the `add.path`,
// `add.size`, and `remove.path` leaf columns so a checkpoint dominated by
// live-file rows still reads cheaply, and it deliberately uses a separate
// projection struct from schemaFromCheckpoint's checkpointRow so the
// schema-only readers stay untouched and never decode file columns.
func applyCheckpointFiles(cd *checkpointData, live map[string]int64) error {
	return scanCheckpoint(cd, func(r checkpointFileRow) bool {
		if r.Add != nil && r.Add.Path != "" {
			live[r.Add.Path] = r.Add.Size
		}
		if r.Remove != nil && r.Remove.Path != "" {
			delete(live, r.Remove.Path)
		}
		return true
	})
}

// checkpointFileRow is the file-accounting projection over a Delta
// checkpoint parquet — the `add` {path, size} and `remove` {path} leaf
// columns. It is intentionally separate from checkpointRow (which projects
// only `metaData.schemaString`) so the cheap schema-only readers never pull
// the bulky file columns and this reader never pulls the schema string.
// Add and Remove are pointers so a row that populates neither group (a
// protocol / metaData / txn row) deserializes to nil rather than a zero
// struct.
type checkpointFileRow struct {
	Add *struct {
		Path string `parquet:"path"`
		Size int64  `parquet:"size"`
	} `parquet:"add"`
	Remove *struct {
		Path string `parquet:"path"`
	} `parquet:"remove"`
}

// logIndex is the one-pass inventory of a `_delta_log/` listing: the JSON
// commit versions (sorted ascending) with their file names, plus the
// checkpoint version → its part file name(s). Both ReadSchema and
// ReadCurrent build this once from a single ReadDir so the checkpoint and
// commit views agree.
type logIndex struct {
	versions      []int64
	versionToFile map[int64]string
	// checkpointParts maps a checkpoint version to its part file names,
	// already sorted by part number (single-part checkpoints have one).
	checkpointParts map[int64][]string
}

// hasCheckpoint reports whether the listing carried at least one
// checkpoint. Used by the ErrNotDelta guard so a `_delta_log` that holds
// only a checkpoint (no surviving JSON commit) still reads as a table.
func (ix *logIndex) hasCheckpoint() bool { return len(ix.checkpointParts) > 0 }

// latestCheckpoint returns the highest checkpoint version and its part
// files (sorted by part number), ok=false when no checkpoint exists.
func (ix *logIndex) latestCheckpoint() (version int64, parts []string, ok bool) {
	for v, p := range ix.checkpointParts {
		if !ok || v > version {
			version, parts, ok = v, p, true
		}
	}
	return version, parts, ok
}

// listLog performs the single fs.ReadDir(".") and classifies every entry
// into commit JSON files and checkpoint parquet parts. Missing-directory
// failures degrade to ErrNotDelta so callers can swallow them with one
// errors.Is check; genuine I/O failures (permission denied, S3 network
// errors) come back wrapped.
func listLog(logFS fs.FS) (*logIndex, error) {
	entries, err := fs.ReadDir(logFS, ".")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotDelta
		}
		return nil, fmt.Errorf("read _delta_log: %w", err)
	}

	ix := &logIndex{
		versionToFile:   make(map[int64]string, len(entries)),
		checkpointParts: make(map[int64][]string),
	}
	// partOrder records each checkpoint part's 1-based part number so we
	// can return parts sorted: a multi-part checkpoint's metaData lives in
	// one part and scanning them in order is deterministic.
	partOrder := make(map[string]int64)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if m := commitFileRe.FindStringSubmatch(name); m != nil {
			n, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				continue // unreachable per the regex, but defensive
			}
			ix.versions = append(ix.versions, n)
			ix.versionToFile[n] = name
			continue
		}
		if m := checkpointMultiRe.FindStringSubmatch(name); m != nil {
			v, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				continue
			}
			part, _ := strconv.ParseInt(m[2], 10, 64)
			ix.checkpointParts[v] = append(ix.checkpointParts[v], name)
			partOrder[name] = part
			continue
		}
		if m := checkpointSingleRe.FindStringSubmatch(name); m != nil {
			v, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				continue
			}
			ix.checkpointParts[v] = append(ix.checkpointParts[v], name)
			partOrder[name] = 1
			continue
		}
	}
	sort.Slice(ix.versions, func(i, j int) bool { return ix.versions[i] < ix.versions[j] })
	for v := range ix.checkpointParts {
		parts := ix.checkpointParts[v]
		sort.Slice(parts, func(i, j int) bool { return partOrder[parts[i]] < partOrder[parts[j]] })
	}
	return ix, nil
}

// resolveSchema returns the table's current schema checkpoint-aware.
//
// When a checkpoint exists at version CV, the active schema is whatever
// the latest metaData carries. A metaData fired after CV (schema
// evolution post-checkpoint) wins, so we first walk the JSON commits with
// version > CV newest→oldest looking for one. Absent that, the schema is
// the one snapshotted in the checkpoint parquet itself, which we read
// without touching any commit before CV.
//
// When no checkpoint exists the table is small or new, the full backward
// walk over every commit is cheap, and we fall back to it.
//
// Commit actions come through getActions (ReadTableState serves them from
// its pre-read window; ReadSchema reads them off the filesystem) and the
// checkpoint bytes through loadCP (memoized — see checkpointLoader) so the
// schema and row-count projections never fetch the same file twice.
func resolveSchema(ix *logIndex, getActions func(int64) ([]rawAction, error), loadCP func() (*checkpointData, error)) (*Schema, error) {
	cv, _, ok := ix.latestCheckpoint()
	if !ok {
		return findLatestSchema(ix.versions, getActions)
	}
	// Schema-evolution-after-checkpoint: scan only the post-checkpoint
	// JSON commits, newest first. These are at most checkpointInterval-1
	// files in the common case (Delta checkpoints every ~10 commits).
	for i := len(ix.versions) - 1; i >= 0; i-- {
		v := ix.versions[i]
		if v <= cv {
			break // versions is ascending; everything below is pre-checkpoint
		}
		actions, err := getActions(v)
		if err != nil {
			return nil, err
		}
		if sch, ok, err := schemaFromActions(actions, v); err != nil {
			return nil, err
		} else if ok {
			return sch, nil
		}
	}
	// No post-checkpoint metaData: the checkpoint's snapshot is current.
	cd, err := loadCP()
	if err != nil {
		return nil, err
	}
	return schemaFromCheckpoint(cd)
}

// schemaFromActions returns the schema carried on the first metaData
// action in a commit, ok=false when the commit has none. Shared by the
// post-checkpoint scan and the full backward walk so both decode the
// metaData identically.
func schemaFromActions(actions []rawAction, version int64) (*Schema, bool, error) {
	for _, a := range actions {
		if a.MetaData == nil {
			continue
		}
		sch, err := parseSchemaString(a.MetaData.SchemaString)
		if err != nil {
			return nil, false, fmt.Errorf("parse schema_string at version %d: %w", version, err)
		}
		return sch, true, nil
	}
	return nil, false, nil
}

// findLatestSchema walks versions from newest to oldest and returns the
// schema carried on the most recent commit that includes a `metaData`
// action. Delta's initial commit always has one; subsequent metaData
// actions only fire on schema-evolution writes. This is the no-checkpoint
// fallback; the checkpoint-aware path in resolveSchema avoids walking
// past the latest checkpoint version.
func findLatestSchema(versions []int64, getActions func(int64) ([]rawAction, error)) (*Schema, error) {
	for i := len(versions) - 1; i >= 0; i-- {
		actions, err := getActions(versions[i])
		if err != nil {
			return nil, err
		}
		if sch, ok, err := schemaFromActions(actions, versions[i]); err != nil {
			return nil, err
		} else if ok {
			return sch, nil
		}
	}
	return nil, fmt.Errorf("no metaData action found in transaction log")
}

// checkpointRow is a minimal projection over a Delta checkpoint parquet.
// A checkpoint carries one top-level group column per action kind
// (`add`, `remove`, `metaData`, `protocol`, `txn`); each row populates
// exactly one. We reference only `metaData.schemaString`, so parquet-go's
// generic reader projects to that single leaf column and never decodes
// the large `add` column (the bulk of a checkpoint, one row per live data
// file). MetaData is a pointer so a row whose metaData group is entirely
// null deserializes to nil rather than a zero struct.
type checkpointRow struct {
	MetaData *struct {
		SchemaString string `parquet:"schemaString"`
	} `parquet:"metaData"`
}

// schemaFromCheckpoint reads the active table schema out of a checkpoint
// parquet. Exactly one row in a checkpoint has a non-null metaData (the
// current table metadata); the rest are add/remove/protocol/txn rows. We
// scan parts in order and return the first metaData.schemaString we find,
// projecting to only that leaf column so the read stays cheap regardless
// of how many data-file `add` rows the checkpoint holds. An error is
// returned when no part carries a metaData.
func schemaFromCheckpoint(cd *checkpointData) (*Schema, error) {
	var found *Schema
	var parseErr error
	err := scanCheckpoint(cd, func(r checkpointRow) bool {
		md := r.MetaData
		if md == nil || md.SchemaString == "" {
			return true
		}
		sch, err := parseSchemaString(md.SchemaString)
		if err != nil {
			parseErr = fmt.Errorf("parse schema_string in checkpoint: %w", err)
			return false
		}
		found = sch
		return false
	})
	if err != nil {
		return nil, err
	}
	if parseErr != nil {
		return nil, parseErr
	}
	if found == nil {
		return nil, fmt.Errorf("no metaData action found in checkpoint")
	}
	return found, nil
}

// bytesReaderAt wraps a []byte so it satisfies io.ReaderAt, which
// parquet-go's OpenFile requires. Mirrors the helper in
// internal/dataquery/source.go; kept package-local to avoid coupling the
// delta reader to the dataquery package.
type bytesReaderAt struct {
	data []byte
}

func newBytesReaderAt(data []byte) *bytesReaderAt {
	return &bytesReaderAt{data: data}
}

func (b *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// windowCommit is one recent-window commit: its version, its file name
// (kept for the mtime timestamp fallback), and its parsed actions. The
// window is read off the filesystem once per ReadTableState and shared by
// the history projection, the schema resolver, and the row-count replay.
type windowCommit struct {
	version int64
	file    string
	actions []rawAction
}

// readWindow reads the last maxCommitsScanned commit files, ascending by
// version. A malformed commit surfaces as an error, matching the
// package-wide "no silent skips" contract.
func readWindow(logFS fs.FS, ix *logIndex) ([]windowCommit, error) {
	start := 0
	if len(ix.versions) > maxCommitsScanned {
		start = len(ix.versions) - maxCommitsScanned
	}
	out := make([]windowCommit, 0, len(ix.versions)-start)
	for _, v := range ix.versions[start:] {
		name := ix.versionToFile[v]
		actions, err := readCommitActions(logFS, name)
		if err != nil {
			return nil, fmt.Errorf("read commit %d: %w", v, err)
		}
		out = append(out, windowCommit{version: v, file: name, actions: actions})
	}
	return out, nil
}

// commitsFromWindow projects the pre-read window into Commit records,
// newest first (the snapshot timeline renders top-down). Versions with no
// commitInfo action (rare but permitted by the protocol) get a Commit with
// empty Operation and UserMetadata — the version + a best-effort timestamp
// from file mtime (where available) is still surfaced so the snapshot
// timeline has a row to render.
func commitsFromWindow(logFS fs.FS, win []windowCommit) []Commit {
	out := make([]Commit, 0, len(win))
	for i := len(win) - 1; i >= 0; i-- {
		wc := win[i]
		c := Commit{Version: wc.version}
		for _, a := range wc.actions {
			if a.CommitInfo == nil {
				continue
			}
			c.TimestampMs = a.CommitInfo.Timestamp
			c.Operation = a.CommitInfo.Operation
			c.UserMetadata = a.CommitInfo.UserMetadata
			fillRecordCounts(&c, a.CommitInfo)
			break
		}
		if c.TimestampMs == 0 {
			c.TimestampMs = mtimeMs(logFS, wc.file)
		}
		out = append(out, c)
	}
	return out
}

// checkpointStatsRow is the row-count projection over a checkpoint
// parquet: each live file's path, its per-file stats JSON (numRecords
// lives there), and its deletion vector's cardinality. Kept separate from
// checkpointFileRow / checkpointRow so each reader decodes only the leaf
// columns it needs. Checkpoints written without the stats or
// deletionVector columns zero-fill (see scanCheckpoint), which
// rowCountFromLog treats as "stats unavailable" and degrades to nil rather
// than returning a wrong count.
type checkpointStatsRow struct {
	Add *struct {
		Path           string `parquet:"path"`
		Stats          string `parquet:"stats"`
		DeletionVector *struct {
			Cardinality int64 `parquet:"cardinality"`
		} `parquet:"deletionVector"`
	} `parquet:"add"`
	Remove *struct {
		Path string `parquet:"path"`
	} `parquet:"remove"`
}

// rowCountFromLog computes the table's exact current row count from Delta
// snapshot state: the sum of every live file's stats.numRecords, net of
// deletion-vector cardinalities. The live set is seeded from the latest
// checkpoint (when one exists) and the window's post-checkpoint commits
// are replayed over it — the same protocol walk ReadFileStats does for
// file sizes, sharing the window and checkpoint bytes ReadTableState
// already fetched.
//
// It returns (nil, nil) — "not derivable" — rather than a wrong number
// when the log doesn't pin down the live set or its row counts:
//   - no checkpoint and the surviving window doesn't start at version 0;
//   - a surviving commit after the checkpoint falls below the window, or
//     the replayed versions aren't contiguous (a retention gap would hide
//     adds/removes);
//   - any live file lacks readable numRecords stats.
func rowCountFromLog(ix *logIndex, win []windowCommit, loadCP func() (*checkpointData, error)) (*int64, error) {
	anchor := int64(-1) // replay commits strictly greater than this
	cv, _, hasCP := ix.latestCheckpoint()
	if hasCP {
		anchor = cv
	} else if len(win) == 0 || win[0].version != 0 {
		return nil, nil
	}

	// Versions below the window (truncated by maxCommitsScanned) must all
	// be covered by the checkpoint, and the replayed tail must be
	// contiguous — Delta versions are, so a gap means retention ate a
	// commit we needed.
	start := len(ix.versions) - len(win)
	if start > 0 && ix.versions[start-1] > anchor {
		return nil, nil
	}
	expected := anchor + 1
	var replay []windowCommit
	for _, wc := range win {
		if wc.version <= anchor {
			continue
		}
		if wc.version != expected {
			return nil, nil
		}
		expected++
		replay = append(replay, wc)
	}

	// live maps a data-file path to its row count; known=false marks a
	// live file whose stats were unreadable (poisons the total only if the
	// file is still live at the latest version).
	type liveFile struct {
		rows  int64
		known bool
	}
	live := make(map[string]liveFile)
	if hasCP {
		cd, err := loadCP()
		if err != nil {
			return nil, err
		}
		if err := scanCheckpoint(cd, func(r checkpointStatsRow) bool {
			if r.Add != nil && r.Add.Path != "" {
				var card int64
				if r.Add.DeletionVector != nil {
					card = r.Add.DeletionVector.Cardinality
				}
				rows, known := liveFileRows(r.Add.Stats, card)
				live[r.Add.Path] = liveFile{rows: rows, known: known}
			}
			if r.Remove != nil && r.Remove.Path != "" {
				delete(live, r.Remove.Path)
			}
			return true
		}); err != nil {
			return nil, err
		}
	}
	for _, wc := range replay {
		for _, a := range wc.actions {
			if a.Add != nil && a.Add.Path != "" {
				var card int64
				if a.Add.DeletionVector != nil {
					card = a.Add.DeletionVector.Cardinality
				}
				rows, known := liveFileRows(a.Add.Stats, card)
				live[a.Add.Path] = liveFile{rows: rows, known: known}
			}
			if a.Remove != nil && a.Remove.Path != "" {
				delete(live, a.Remove.Path)
			}
		}
	}

	var total int64
	for _, lf := range live {
		if !lf.known {
			return nil, nil
		}
		total += lf.rows
	}
	return &total, nil
}

// liveFileRows parses an add action's stats JSON into the file's live row
// count, netting out an attached deletion vector's cardinality (rows the
// vector soft-deleted). known=false when the stats are absent or don't
// carry numRecords — the writer didn't collect stats, or the checkpoint
// stores stats as a struct column instead of JSON.
func liveFileRows(stats string, dvCardinality int64) (rows int64, known bool) {
	if strings.TrimSpace(stats) == "" {
		return 0, false
	}
	var s struct {
		NumRecords *int64 `json:"numRecords"`
	}
	if err := json.Unmarshal([]byte(stats), &s); err != nil || s.NumRecords == nil {
		return 0, false
	}
	rows = *s.NumRecords - dvCardinality
	if rows < 0 {
		rows = 0
	}
	return rows, true
}

// fillRecordCounts mirrors the runner's _record_table_state mapping —
// commit metrics keys vary by operation, and this keeps the Athena-shaped
// added/deleted/total columns populated regardless of backend.
//
//	WRITE / APPEND / OVERWRITE → numOutputRows
//	MERGE → numTargetRowsInserted + numTargetRowsUpdated (added);
//	        numTargetRowsDeleted (deleted)
//
// "Total" was an Iceberg snapshot-summary-wide value that Delta doesn't
// expose per commit; we leave it nil and let the timeline render added /
// deleted only. The runner's `tables` system-table writer still tracks
// the row count via spark.catalog when callers need it.
func fillRecordCounts(c *Commit, ci *rawCommitInfo) {
	if ci == nil || len(ci.OperationMetrics) == 0 {
		return
	}
	get := func(key string) *int64 {
		raw, ok := ci.OperationMetrics[key]
		if !ok || raw == "" {
			return nil
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil
		}
		return &n
	}
	op := strings.ToUpper(ci.Operation)
	if strings.Contains(op, "MERGE") {
		ins := get("numTargetRowsInserted")
		upd := get("numTargetRowsUpdated")
		c.UpdatedRecords = upd
		if ins != nil || upd != nil {
			var sum int64
			if ins != nil {
				sum += *ins
			}
			if upd != nil {
				sum += *upd
			}
			c.AddedRecords = &sum
		}
		c.DeletedRecords = get("numTargetRowsDeleted")
	} else {
		c.AddedRecords = get("numOutputRows")
	}
	// Replaces=true for commit shapes that overwrite the table state.
	// CTAS, REPLACE TABLE, CREATE OR REPLACE TABLE, and WRITE with
	// operationParameters.mode=Overwrite all reset the running row count.
	if isReplaceOp(op, ci) {
		c.Replaces = true
	}
}

// isReplaceOp decides whether a commit overwrites the table state. Used
// only for LatestRecordCount aggregation; per-snapshot display continues
// to read AddedRecords / DeletedRecords as-is. Conservative: anything we
// recognize as a fresh-write resets, MERGE and APPEND don't.
func isReplaceOp(op string, ci *rawCommitInfo) bool {
	switch op {
	case "CREATE TABLE", "REPLACE TABLE", "CREATE OR REPLACE TABLE",
		"CREATE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT":
		return true
	}
	if op == "WRITE" && ci != nil && len(ci.Parameters) > 0 {
		// operationParameters is `json.RawMessage` (Delta stores values as
		// quoted strings, sometimes as nested objects). A substring match
		// avoids parsing the dynamic shape; the trade is occasional false
		// positives on tables that literally have a column named "mode" with
		// value "Overwrite" — vanishingly unlikely.
		if strings.Contains(string(ci.Parameters), `"mode":"Overwrite"`) {
			return true
		}
	}
	return false
}

// mtimeMs falls back to filesystem mtime when a commit has no
// commitInfo.timestamp — best effort, returns 0 when unavailable
// (S3 FS impls may decline to support it, in which case the timeline
// row still renders with timestamp=0).
func mtimeMs(logFS fs.FS, name string) int64 {
	fi, err := fs.Stat(logFS, name)
	if err != nil {
		return 0
	}
	t := fi.ModTime()
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// rawAction is the union shape of one line of a Delta commit file. Each
// line of `<version>.json` is exactly one JSON object with exactly one
// of these keys populated (`add`, `remove`, `metaData`, `protocol`,
// `commitInfo`, `txn`, `cdc`, `domainMetadata`); the rest are nil.
type rawAction struct {
	MetaData   *rawMetaData   `json:"metaData,omitempty"`
	CommitInfo *rawCommitInfo `json:"commitInfo,omitempty"`
	// Add / Remove drive the live-file accounting in ReadFileStats — an
	// `add` inserts a live file (path → size), a `remove` retires one. The
	// schema / history readers ignore them; only the file-stats reader
	// consumes them. Spec:
	// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
	Add    *rawAdd    `json:"add,omitempty"`
	Remove *rawRemove `json:"remove,omitempty"`
	// Other action kinds (protocol, txn, cdc, domainMetadata) — the reader
	// doesn't need them, so the fields are intentionally absent.
}

// rawAdd is the `add` action's subset the file-accounting readers consume:
// the data file's path (the live-set key), its byte size (ReadFileStats),
// and its row-count inputs (rowCountFromLog) — the per-file stats JSON
// carrying numRecords, plus the deletion vector netting out soft-deleted
// rows.
type rawAdd struct {
	Path           string             `json:"path"`
	Size           int64              `json:"size"`
	Stats          string             `json:"stats"`
	DeletionVector *rawDeletionVector `json:"deletionVector"`
}

// rawDeletionVector is the `add.deletionVector` subset rowCountFromLog
// consumes: Cardinality is the number of rows the vector marks deleted in
// its file.
type rawDeletionVector struct {
	Cardinality int64 `json:"cardinality"`
}

// rawRemove is the `remove` action's subset ReadFileStats consumes: the
// path of the data file being retired from the live set.
type rawRemove struct {
	Path string `json:"path"`
}

type rawMetaData struct {
	ID               string            `json:"id"`
	Format           json.RawMessage   `json:"format"`
	SchemaString     string            `json:"schemaString"`
	PartitionColumns []string          `json:"partitionColumns"`
	Configuration    map[string]string `json:"configuration"`
}

type rawCommitInfo struct {
	Timestamp        int64             `json:"timestamp"`
	Operation        string            `json:"operation"`
	UserMetadata     string            `json:"userMetadata,omitempty"`
	IsBlindAppend    *bool             `json:"isBlindAppend,omitempty"`
	EngineInfo       string            `json:"engineInfo,omitempty"`
	Parameters       json.RawMessage   `json:"operationParameters,omitempty"`
	OperationMetrics map[string]string `json:"operationMetrics,omitempty"`
}

// readCommitActions parses one commit file. Each file is newline-
// delimited JSON; blank lines are tolerated. A parse failure on any
// line surfaces immediately — a malformed commit is exactly the kind
// of regression silent skipping would hide.
func readCommitActions(logFS fs.FS, name string) ([]rawAction, error) {
	f, err := logFS.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var actions []rawAction
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var a rawAction
		if err := json.Unmarshal([]byte(line), &a); err != nil {
			return nil, fmt.Errorf("malformed JSON in %s: %w", filepath.Base(name), err)
		}
		actions = append(actions, a)
	}
	return actions, nil
}

// parseSchemaString unpacks the JSON-encoded Spark SQL schema that
// metaData carries. The shape is:
//
//	{"type": "struct", "fields": [
//	  {"name": "id", "type": "long", "nullable": true, "metadata": {}},
//	  {"name": "amounts", "type": {"type": "array", "elementType": "decimal(10,2)", "containsNull": true}, ...}
//	]}
//
// Each field's type is either a primitive string ("string", "long",
// "decimal(10,2)") or a nested object describing array / map / struct.
// We render every shape back to the canonical Spark SQL string the rest
// of clavesa consumes — Glue's StorageDescriptor.Columns uses the same
// string form, so the catalog page renders a single column type column
// regardless of source.
func parseSchemaString(s string) (*Schema, error) {
	if s == "" {
		return nil, fmt.Errorf("empty schemaString")
	}
	var root struct {
		Type   string           `json:"type"`
		Fields []rawSchemaField `json:"fields"`
	}
	if err := json.Unmarshal([]byte(s), &root); err != nil {
		return nil, fmt.Errorf("decode schemaString: %w", err)
	}
	if root.Type != "struct" {
		return nil, fmt.Errorf("schemaString root type = %q, want struct", root.Type)
	}
	cols := make([]Column, 0, len(root.Fields))
	for _, f := range root.Fields {
		cols = append(cols, Column{
			Name:     f.Name,
			Type:     renderType(f.Type),
			Nullable: f.Nullable,
		})
	}
	return &Schema{Columns: cols}, nil
}

// rawSchemaField is one entry in the `fields` array of a Spark struct
// schema. Type is `json.RawMessage` because Spark encodes it as either a
// string (primitive) or an object (compound type).
type rawSchemaField struct {
	Name     string          `json:"name"`
	Type     json.RawMessage `json:"type"`
	Nullable bool            `json:"nullable"`
	Metadata json.RawMessage `json:"metadata"`
}

// renderType collapses Spark's JSON-encoded type into the canonical
// SQL string Spark / Glue / Athena all agree on. For nested types this
// follows the same recursion Spark's DataType.fromJson uses — array,
// map, struct, decimal all carry their parameters in a sidecar object.
//
// Unrecognized shapes fall back to "<unknown>" rather than panicking;
// the catalog UI doesn't render type-specific affordances today so a
// graceful degrade is enough.
func renderType(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "<unknown>"
	}
	// Primitive: a JSON string. Render verbatim.
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	// Compound: a JSON object with a `type` discriminator.
	var hdr struct {
		Type         string           `json:"type"`
		ElementType  json.RawMessage  `json:"elementType"`
		KeyType      json.RawMessage  `json:"keyType"`
		ValueType    json.RawMessage  `json:"valueType"`
		ContainsNull bool             `json:"containsNull"`
		Fields       []rawSchemaField `json:"fields"`
		Precision    int              `json:"precision"`
		Scale        int              `json:"scale"`
	}
	if err := json.Unmarshal(raw, &hdr); err != nil {
		return "<unknown>"
	}
	switch hdr.Type {
	case "array":
		return "array<" + renderType(hdr.ElementType) + ">"
	case "map":
		return "map<" + renderType(hdr.KeyType) + "," + renderType(hdr.ValueType) + ">"
	case "struct":
		parts := make([]string, 0, len(hdr.Fields))
		for _, f := range hdr.Fields {
			parts = append(parts, f.Name+":"+renderType(f.Type))
		}
		return "struct<" + strings.Join(parts, ",") + ">"
	case "decimal":
		// Spark also accepts decimal as a string `decimal(p,s)` in the
		// primitive branch; the object form shows up when the writer
		// produces it explicitly.
		return "decimal(" + strconv.Itoa(hdr.Precision) + "," + strconv.Itoa(hdr.Scale) + ")"
	default:
		if hdr.Type != "" {
			return hdr.Type
		}
		return "<unknown>"
	}
}

// ReadCurrentFromPath is a convenience wrapper for local callers that
// hold a table-root path and don't want to fiddle with os.DirFS at the
// call site. The fs.FS-based ReadCurrent is the canonical API; this
// wrapper exists because the local catalog walker has a string in hand
// and shouldn't have to learn io/fs to call us.
//
// Returns ErrNotDelta both when the table directory itself is missing
// and when `_delta_log/` is absent — the catalog walker treats them
// identically (it's a non-Delta directory either way).
func ReadCurrentFromPath(tablePath string) (*Schema, []Commit, error) {
	logDir := filepath.Join(tablePath, "_delta_log")
	if _, err := os.Stat(logDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, ErrNotDelta
		}
		return nil, nil, fmt.Errorf("stat _delta_log: %w", err)
	}
	return ReadCurrent(os.DirFS(logDir))
}

// ReadTableStateFromPath is the table-root convenience wrapper for
// ReadTableState, mirroring ReadCurrentFromPath. Returns ErrNotDelta both
// when the table directory is missing and when `_delta_log/` is absent.
func ReadTableStateFromPath(tablePath string) (*TableState, error) {
	logDir := filepath.Join(tablePath, "_delta_log")
	if _, err := os.Stat(logDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotDelta
		}
		return nil, fmt.Errorf("stat _delta_log: %w", err)
	}
	return ReadTableState(os.DirFS(logDir))
}

// ReadFileStatsFromPath is the table-root convenience wrapper for
// ReadFileStats, mirroring ReadCurrentFromPath — local callers hold a
// table directory string and shouldn't have to wrap os.DirFS themselves.
// Returns ErrNotDelta both when the table directory is missing and when
// `_delta_log/` is absent.
func ReadFileStatsFromPath(tablePath string) (*FileStats, error) {
	logDir := filepath.Join(tablePath, "_delta_log")
	if _, err := os.Stat(logDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotDelta
		}
		return nil, fmt.Errorf("stat _delta_log: %w", err)
	}
	return ReadFileStats(os.DirFS(logDir))
}
