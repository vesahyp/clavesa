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

// ReadCurrent loads the latest schema + recent commit history from a
// `_delta_log` filesystem. `logFS` must be rooted at the `_delta_log`
// directory (NOT the table root); ReadDir(".") on it lists the commit
// files. Returns ErrNotDelta when no valid commit files are found —
// matches the catalog walker's "silently skip non-Delta directories"
// contract.
//
// A malformed commit file surfaces as an error rather than a silent
// skip — silent skips would hide schema-evolution bugs that a future
// refactor introduces.
func ReadCurrent(logFS fs.FS) (*Schema, []Commit, error) {
	entries, err := fs.ReadDir(logFS, ".")
	if err != nil {
		// Missing-directory and similar "no table here" failures all
		// degrade to ErrNotDelta so callers can swallow them with one
		// errors.Is check. Genuine I/O failures (permission denied on
		// a present directory, network errors on S3) come back wrapped.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotDelta
		}
		return nil, nil, fmt.Errorf("read _delta_log: %w", err)
	}

	versions := make([]int64, 0, len(entries))
	versionToFile := make(map[int64]string, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := commitFileRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		n, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			continue // unreachable per the regex, but defensive
		}
		versions = append(versions, n)
		versionToFile[n] = e.Name()
	}
	if len(versions) == 0 {
		return nil, nil, ErrNotDelta
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	// Walk forward to find the last metaData action — schema evolution
	// rewrites it on subsequent commits, so we can't short-circuit on
	// the first one. Bound the read to the most recent
	// maxCommitsScanned files (history beyond that is fine, but we
	// still need to find a metaData; in practice the initial commit
	// always carries one, and we read commits backwards from the tip
	// looking for one if needed).
	schema, err := findLatestSchema(logFS, versions, versionToFile)
	if err != nil {
		return nil, nil, err
	}

	commits, err := readRecentCommits(logFS, versions, versionToFile)
	if err != nil {
		return nil, nil, err
	}

	return schema, commits, nil
}

// findLatestSchema walks versions from newest to oldest and returns the
// schema carried on the most recent commit that includes a `metaData`
// action. Delta's initial commit always has one; subsequent metaData
// actions only fire on schema-evolution writes.
func findLatestSchema(logFS fs.FS, versions []int64, files map[int64]string) (*Schema, error) {
	for i := len(versions) - 1; i >= 0; i-- {
		actions, err := readCommitActions(logFS, files[versions[i]])
		if err != nil {
			return nil, err
		}
		for _, a := range actions {
			if a.MetaData == nil {
				continue
			}
			sch, err := parseSchemaString(a.MetaData.SchemaString)
			if err != nil {
				return nil, fmt.Errorf("parse schema_string at version %d: %w", versions[i], err)
			}
			return sch, nil
		}
	}
	return nil, fmt.Errorf("no metaData action found in transaction log")
}

// readRecentCommits returns Commit records for the last maxCommitsScanned
// versions, newest first. Versions with no commitInfo action (rare but
// permitted by the protocol) get a Commit with empty Operation and
// UserMetadata — the version + a best-effort timestamp from file mtime
// (where available) is still surfaced so the snapshot timeline has a
// row to render.
func readRecentCommits(logFS fs.FS, versions []int64, files map[int64]string) ([]Commit, error) {
	start := 0
	if len(versions) > maxCommitsScanned {
		start = len(versions) - maxCommitsScanned
	}
	out := make([]Commit, 0, len(versions)-start)
	// Newest first — the snapshot timeline renders top-down.
	for i := len(versions) - 1; i >= start; i-- {
		v := versions[i]
		path := files[v]
		actions, err := readCommitActions(logFS, path)
		if err != nil {
			return nil, fmt.Errorf("read commit %d: %w", v, err)
		}
		c := Commit{Version: v}
		for _, a := range actions {
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
			c.TimestampMs = mtimeMs(logFS, path)
		}
		out = append(out, c)
	}
	return out, nil
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
	// Other action kinds (add, remove, protocol, txn, cdc) — the reader
	// doesn't need them, so the fields are intentionally absent. Spec:
	// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
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
