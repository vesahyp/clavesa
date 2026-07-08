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
	"strings"

	"github.com/vesahyp/clavesa/internal/registry"
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
	// (`spark.read.<format>`). One of: parquet, csv, json, tsv. Inferred
	// from the URL/key filename extension when omitted at register time.
	Format string `json:"format,omitempty"`

	// ReadOptions are optional Spark read options for delimited text:
	// delimiter, comment, header, columns. Interpreted by the runner.
	ReadOptions map[string]string `json:"read_options,omitempty"`

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
	reg *registry.Store[Spec]
}

// New returns a Store rooted at workspaceRoot. The directory itself is
// created lazily on first write, so callers don't need to pre-create it.
//
// Name rules come from registry.ValidName (1–64 chars, lowercase alnum +
// `-` `_`, must start with a lowercase letter): the name doubles as a
// filename and stays lowercase to match the Hive identifier convention the
// runner uses for table names.
func New(workspaceRoot string) *Store {
	return &Store{reg: registry.New(workspaceRoot, registry.Config[Spec]{
		Kind:   "source",
		RelDir: RelDir,
		Ext:    ".json",
		Marshal: func(spec Spec) ([]byte, error) {
			return registry.MarshalIndentJSON(spec)
		},
		Unmarshal: func(name string, data []byte) (Spec, error) {
			var spec Spec
			if err := json.Unmarshal(data, &spec); err != nil {
				return Spec{}, fmt.Errorf("parse %s.json: %w", name, err)
			}
			spec.Name = name
			return spec, nil
		},
	})}
}

// Dir returns the absolute path of the registry directory.
func (s *Store) Dir() string {
	return s.reg.Dir()
}

// Path returns the absolute path of a source's JSON file.
func (s *Store) Path(name string) string {
	return s.reg.Path(name)
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

// normalize applies the write-time canonicalizations shared by Add and
// Update: kind=s3 prefixes always end in "/" so the runner walks the whole
// keyspace, not just keys starting with the literal. Empty prefix stays
// empty (whole-bucket scan).
func (s *Spec) normalize() {
	if s.Kind == "s3" && s.Prefix != "" && !strings.HasSuffix(s.Prefix, "/") {
		s.Prefix += "/"
	}
}

// validate runs name/kind/spec consistency checks before write.
func (s Spec) validate() error {
	if err := registry.ValidName(s.Name); err != nil {
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
		return fmt.Errorf("format is required (parquet, csv, json, or tsv)")
	}
	switch s.Format {
	case "parquet", "csv", "json", "tsv":
	default:
		return fmt.Errorf("unsupported format %q (parquet, csv, json, tsv)", s.Format)
	}
	return nil
}

// Add writes a new source spec to disk. Refuses to overwrite an existing
// source — call Delete first or use a different name.
func (s *Store) Add(spec Spec) error {
	spec.normalize()
	if err := spec.validate(); err != nil {
		return err
	}
	return s.reg.Create(spec.Name, spec)
}

// Update overwrites an existing source spec. Unlike Add it requires the
// source to already exist — a missing file is an error, not a silent
// create. There is no rename operation: a source's name is its file key
// and pipelines reference it by that name, so renaming is a delete +
// register, not an edit.
func (s *Store) Update(spec Spec) error {
	spec.normalize()
	if err := spec.validate(); err != nil {
		return err
	}
	return s.reg.Update(spec.Name, spec)
}

// Get reads one source by name. Returns os.ErrNotExist when absent.
func (s *Store) Get(name string) (Spec, error) {
	return s.reg.Get(name)
}

// List returns every registered source, sorted by name. A missing registry
// directory returns an empty slice (the empty-state) rather than an error,
// matching how dashboards list works for first-run workspaces. Unreadable /
// malformed files are skipped; the error surfaces via Get on demand.
func (s *Store) List() ([]Spec, error) {
	return s.reg.List()
}

// Delete removes a source from the registry. Caller is responsible for the
// pipeline-scan deletion guard (see service.DeleteSource) — this layer just
// owns the file.
func (s *Store) Delete(name string) error {
	return s.reg.Delete(name)
}
