// Package sources implements the workspace-level source registry introduced
// by ADR-017. A source is a named entry that records *where raw data lives*
// — for slice 1, an HTTP(S) URL with an inferred format. The registry is the
// single canonical home for raw-data declarations: pipelines reference
// sources by name (`inputs = { x = "sources.<name>" }`) instead of declaring
// inline `module "src_X"` blocks.
//
// Storage shape: one JSON file per source under
// `<workspace>/.clavesa/sources/<name>.json`. Same authoring location as
// dashboards. JSON for ergonomics — readable, diffable, machine-writable
// from CLI and UI without an HCL writer.
//
// Slice 1 supports `kind = "http"` only (no auth). The `s3` and credentials-
// backed `http` kinds land in slices 2 and 3.
package sources

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RelDir is the workspace-relative directory holding source JSON files.
// Mirrors `<workspace>/.clavesa/dashboards/` for symmetry.
const RelDir = ".clavesa/sources"

// Spec is the on-disk shape of one registered source.
//
// Future kinds (eventually JDBC / Kafka / REST) extend the discriminator
// without breaking the registry shape — keep new fields optional so
// older registry files keep parsing after schema additions.
type Spec struct {
	// Name is the registry identifier — also the JSON filename
	// (`<name>.json`). Set on Load from the filename so callers don't
	// double-source it.
	Name string `json:"-"`

	// Kind is the discriminator. Slice 1: "http". Slice 3: + "s3".
	Kind string `json:"kind"`

	// URL is the resource URL for kind="http". Empty for other kinds.
	URL string `json:"url,omitempty"`

	// Bucket is the S3 bucket name for kind="s3". Empty for other kinds.
	Bucket string `json:"bucket,omitempty"`

	// Prefix is the S3 key prefix for kind="s3" (typically
	// "path/to/data/"). Empty means whole-bucket scan, which is rarely
	// what users want — slice 3 accepts it but the CLI shortcut from
	// `s3://bucket/key` always derives a non-empty prefix.
	Prefix string `json:"prefix,omitempty"`

	// Format is the data format the runner reads with
	// (`spark.read.<format>`). One of: parquet, csv, json. Inferred
	// from the URL/key filename extension when omitted at register time.
	Format string `json:"format,omitempty"`

	// Credentials, when set, names a credential in the workspace
	// credentials registry (slice 2). For `kind=http` it injects an
	// auth header; for `kind=s3` slice 3 leaves it unused (same-account
	// reads use the deploy role) — slice 5+ wires it for cross-account
	// `assume-role` credentials.
	Credentials string `json:"credentials,omitempty"`

	// Partitions are the Hive-style partition keys present in the bucket
	// layout (`year=2024/month=01/day=03/…` → ["year","month","day"]).
	// kind=s3 only. When set, the runner does an incremental read driven
	// by a workspace-stored watermark instead of a full prefix scan.
	Partitions []string `json:"partitions,omitempty"`

	// StartFrom seeds the watermark on the source's first run.
	//   - "" / "all"  : read every partition that exists.
	//   - "now"       : start at the newest partition (skip history).
	//   - "<cursor>"  : literal "/"-joined cursor, e.g. "2024-01-01"
	//                   or "2024/01" for nested partition keys.
	// Ignored when Partitions is empty.
	StartFrom string `json:"start_from,omitempty"`

	// ManageBucketNotifications, when true on a kind=s3 source, has the
	// emitted `module "src_<name>"` instance create an authoritative
	// `aws_s3_bucket_notification` on var.bucket with `eventbridge =
	// true`. Lets users skip the out-of-band `aws s3api
	// put-bucket-notification-configuration` step when clavesa owns
	// the bucket. Opt-in because the resource replaces any other
	// notification config on the bucket.
	ManageBucketNotifications bool `json:"manage_bucket_notifications,omitempty"`
}

// Store is the file-backed source registry rooted at a workspace directory.
// Methods are filesystem-naive (no locking) — intended for the single-user
// CLI / single-process UI shape clavesa runs in today.
type Store struct {
	workspaceRoot string
}

// New returns a Store rooted at workspaceRoot. The directory itself is
// created lazily on first write, so callers don't need to pre-create it.
func New(workspaceRoot string) *Store {
	return &Store{workspaceRoot: workspaceRoot}
}

// Dir returns the absolute path of the registry directory.
func (s *Store) Dir() string {
	return filepath.Join(s.workspaceRoot, RelDir)
}

// Path returns the absolute path of a source's JSON file.
func (s *Store) Path(name string) string {
	return filepath.Join(s.Dir(), name+".json")
}

// validNameRE-style guard inlined to avoid a regex import — name rules:
// 1–64 chars, lowercase alnum + `-` + `_`, must start with a letter.
// Mirrors `validSlug` in api/dashboards.go and the same guarantee dashboards
// give: the name doubles as a filename, so reject anything that could
// traverse paths or surprise a shell.
//
// Names are lowercase to match the Hive identifier convention the runner
// uses for table names — registered sources surface as input descriptors;
// keeping their names lowercase avoids a separate sanitize step downstream.
func validName(s string) error {
	if s == "" {
		return fmt.Errorf("name is required")
	}
	if len(s) > 64 {
		return fmt.Errorf("name must be <=64 chars (got %d)", len(s))
	}
	first := s[0]
	if !(first >= 'a' && first <= 'z') {
		return fmt.Errorf("name must start with a lowercase letter")
	}
	for i, c := range s {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '-' || c == '_':
		default:
			return fmt.Errorf("name has invalid char %q at index %d (allowed: a-z 0-9 - _)", c, i)
		}
	}
	return nil
}

// validKind enforces what the runner can actually read today. Slice 1
// added "http"; slice 3 added "s3". Future kinds (JDBC, Kafka, REST,
// glue cross-account, ...) extend this list — each gated behind the
// runner having a matching read path.
func validKind(kind string) error {
	switch kind {
	case "http", "s3":
		return nil
	default:
		return fmt.Errorf("unsupported source kind %q (supported: http, s3)", kind)
	}
}

// validate runs name/kind/spec consistency checks before write.
func (s Spec) validate() error {
	if err := validName(s.Name); err != nil {
		return err
	}
	if err := validKind(s.Kind); err != nil {
		return err
	}
	if s.Kind != "s3" && (len(s.Partitions) > 0 || s.StartFrom != "") {
		return fmt.Errorf("partitions / start_from are only valid for kind=s3 (got kind=%q)", s.Kind)
	}
	if s.Kind != "s3" && s.ManageBucketNotifications {
		return fmt.Errorf("manage_bucket_notifications is only valid for kind=s3 (got kind=%q)", s.Kind)
	}
	switch s.Kind {
	case "http":
		if s.URL == "" {
			return fmt.Errorf("url is required for kind=http")
		}
		if !strings.HasPrefix(s.URL, "http://") && !strings.HasPrefix(s.URL, "https://") {
			return fmt.Errorf("url must be http:// or https:// (got %q)", s.URL)
		}
	case "s3":
		if s.Bucket == "" {
			return fmt.Errorf("bucket is required for kind=s3")
		}
		// Bucket-name sanity: S3 buckets are 3-63 chars, lowercase
		// letters/digits/dots/dashes. Light check — Spark/S3A will
		// surface the precise error on read anyway, but catching obvious
		// typos at register time saves a docker round-trip.
		for _, c := range s.Bucket {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
				return fmt.Errorf("bucket %q contains invalid char %q (allowed: a-z 0-9 - .)", s.Bucket, c)
			}
		}
		// Prefix must end in "/" when non-empty so spark.read picks up
		// the whole keyspace, not just keys *starting with* the literal
		// (which would miss any subkey). Auto-normalize on Add.
		for i, p := range s.Partitions {
			if p == "" {
				return fmt.Errorf("partitions[%d] is empty (partition key names cannot be blank)", i)
			}
		}
	}
	if s.StartFrom != "" && len(s.Partitions) == 0 {
		return fmt.Errorf("start_from set without partitions — start_from is a watermark seed and requires Hive-style partition keys")
	}
	if len(s.Partitions) > 0 && s.Format != "parquet" {
		// Runner's partitioned_path read hardcodes spark.read.parquet — CSV
		// and JSON partitioned reads aren't wired yet. Reject here so the
		// failure surfaces at register time, not deep in a runner log line.
		return fmt.Errorf("partitioned reads require format=parquet (got %q)", s.Format)
	}
	if s.Format == "" {
		return fmt.Errorf("format is required (parquet, csv, or json)")
	}
	switch s.Format {
	case "parquet", "csv", "json":
	default:
		return fmt.Errorf("unsupported format %q (parquet, csv, json)", s.Format)
	}
	return nil
}

// Add writes a new source spec to disk. Refuses to overwrite an existing
// source — call Delete first or use a different name.
func (s *Store) Add(spec Spec) error {
	// Normalize: kind=s3 prefixes always end in "/" so the runner walks
	// the whole keyspace, not just keys starting with the literal.
	// Empty prefix stays empty (whole-bucket scan).
	if spec.Kind == "s3" && spec.Prefix != "" && !strings.HasSuffix(spec.Prefix, "/") {
		spec.Prefix += "/"
	}
	if err := spec.validate(); err != nil {
		return err
	}
	path := s.Path(spec.Name)
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("source %q already exists", spec.Name)
	}
	if err := os.MkdirAll(s.Dir(), 0o755); err != nil {
		return fmt.Errorf("create sources dir: %w", err)
	}
	return writeJSON(path, spec)
}

// Update overwrites an existing source spec. Unlike Add it requires the
// source to already exist — a missing file is an error, not a silent
// create. There is no rename operation: a source's name is its file key
// and pipelines reference it by that name, so renaming is a delete +
// register, not an edit.
func (s *Store) Update(spec Spec) error {
	// Same prefix normalization as Add — kind=s3 prefixes end in "/".
	if spec.Kind == "s3" && spec.Prefix != "" && !strings.HasSuffix(spec.Prefix, "/") {
		spec.Prefix += "/"
	}
	if err := spec.validate(); err != nil {
		return err
	}
	path := s.Path(spec.Name)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source %q does not exist", spec.Name)
		}
		return err
	}
	return writeJSON(path, spec)
}

// Get reads one source by name. Returns os.ErrNotExist when absent.
func (s *Store) Get(name string) (Spec, error) {
	if err := validName(name); err != nil {
		return Spec{}, err
	}
	data, err := os.ReadFile(s.Path(name))
	if err != nil {
		return Spec{}, err
	}
	var spec Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return Spec{}, fmt.Errorf("parse %s.json: %w", name, err)
	}
	spec.Name = name
	return spec, nil
}

// List returns every registered source, sorted by name. A missing registry
// directory returns an empty slice (the empty-state) rather than an error,
// matching how dashboards list works for first-run workspaces.
func (s *Store) List() ([]Spec, error) {
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		if os.IsNotExist(err) {
			return []Spec{}, nil
		}
		return nil, err
	}
	out := make([]Spec, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		if err := validName(name); err != nil {
			continue // skip files whose names couldn't have been registered
		}
		spec, err := s.Get(name)
		if err != nil {
			continue // skip unreadable / malformed files; surface via Get on demand
		}
		out = append(out, spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Delete removes a source from the registry. Caller is responsible for the
// pipeline-scan deletion guard (see service.DeleteSource) — this layer just
// owns the file.
func (s *Store) Delete(name string) error {
	if err := validName(name); err != nil {
		return err
	}
	return os.Remove(s.Path(name))
}

// writeJSON marshals v with indent and atomically writes to path via rename.
// Atomic so a crash between truncate and write doesn't leave a half-written
// file the next List would skip.
func writeJSON(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
