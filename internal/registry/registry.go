// Package registry implements the shared one-file-per-item workspace
// registry that sources, credentials, notebooks, and dashboards are all
// built on: each item lives at `<workspace>/<relDir>/<name><ext>`, written
// atomically, with the name doubling as the filename (so name validation is
// also path-traversal protection).
//
// The Store is generic over the item type; the owning package supplies the
// marshal/unmarshal pair (JSON spec structs, nbformat notebooks, raw byte
// documents) plus an optional name validator. Domain concerns — spec
// validation, deletion guards, normalization — stay in the owning package;
// this layer only owns the file.
//
// Methods are filesystem-naive (no locking) — intended for the single-user
// CLI / single-process UI shape clavesa runs in today, the same contract
// the four registries individually documented before consolidation.
package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ValidName is the canonical registry identifier rule: 1–64 chars,
// lowercase alnum + `-` + `_`, must start with a lowercase letter. The name
// doubles as a filename, so reject anything that could traverse paths or
// surprise a shell.
//
// Names are lowercase to match the Hive identifier convention the runner
// uses for table names — registered items can surface as input descriptors;
// keeping their names lowercase avoids a separate sanitize step downstream.
//
// Note: dashboards deliberately keep their own ValidSlug (same rule minus
// the first-char clause) until the I-P3-2 decision on digit-leading slugs;
// changing that here would invalidate existing dashboard files.
func ValidName(s string) error {
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

// WriteFileAtomic writes data to path via a temp file + rename, so a crash
// between truncate and write never leaves a half-written file the next List
// would skip.
func WriteFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// MarshalIndentJSON marshals v with two-space indent and a trailing newline
// — the on-disk JSON shape of the spec-backed registries (sources,
// credentials). Kept as a helper so the format is defined once.
func MarshalIndentJSON(v any) ([]byte, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// Config parameterizes a Store for one registry kind.
type Config[T any] struct {
	// Kind is the singular noun used in error messages ("source",
	// "credential", "notebook", "dashboard"). The registry directory noun
	// is derived as Kind + "s".
	Kind string

	// RelDir is the workspace-relative directory holding the item files.
	RelDir string

	// Ext is the item file extension including the dot (".json", ".ipynb").
	Ext string

	// ValidName validates a registry identifier. Nil means the canonical
	// ValidName rule. Dashboards pass their divergent ValidSlug here.
	ValidName func(string) error

	// Marshal renders an item to its on-disk bytes (including any trailing
	// newline convention the format carries).
	Marshal func(T) ([]byte, error)

	// Unmarshal parses on-disk bytes back into an item. It receives the
	// registry name (filename sans Ext) so implementations can stamp it on
	// the returned value and word parse errors the way their package always
	// has.
	Unmarshal func(name string, data []byte) (T, error)
}

// Store is a file-backed registry of T rooted at a workspace directory.
// The registry directory is created lazily on first write, so callers don't
// need to pre-create it.
type Store[T any] struct {
	root string
	cfg  Config[T]
}

// New returns a Store rooted at workspaceRoot.
func New[T any](workspaceRoot string, cfg Config[T]) *Store[T] {
	if cfg.ValidName == nil {
		cfg.ValidName = ValidName
	}
	return &Store[T]{root: workspaceRoot, cfg: cfg}
}

// Dir returns the absolute path of the registry directory.
func (s *Store[T]) Dir() string {
	return filepath.Join(s.root, s.cfg.RelDir)
}

// Path returns the absolute path of an item's file.
func (s *Store[T]) Path(name string) string {
	return filepath.Join(s.Dir(), name+s.cfg.Ext)
}

// ValidName runs the store's name validator (the canonical rule unless the
// owning package overrode it).
func (s *Store[T]) ValidName(name string) error {
	return s.cfg.ValidName(name)
}

// Create writes a new item. Refuses to overwrite an existing one — call
// Delete first or use a different name.
func (s *Store[T]) Create(name string, v T) error {
	if err := s.cfg.ValidName(name); err != nil {
		return err
	}
	if _, err := os.Stat(s.Path(name)); err == nil {
		return fmt.Errorf("%s %q already exists", s.cfg.Kind, name)
	}
	return s.write(name, v)
}

// Update overwrites an existing item. Unlike Create it requires the item to
// already exist — a missing file is an error, not a silent create. There is
// no rename operation: an item's name is its file key, so renaming is a
// delete + create, not an edit.
func (s *Store[T]) Update(name string, v T) error {
	if err := s.cfg.ValidName(name); err != nil {
		return err
	}
	if _, err := os.Stat(s.Path(name)); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%s %q does not exist", s.cfg.Kind, name)
		}
		return err
	}
	return s.write(name, v)
}

// Put upserts an item: create and replace are the same operation, the name
// is the key. Used by registries whose save semantics are overwrite-by-slug
// (dashboards) or write-back-after-read (notebook ClearOutputs).
func (s *Store[T]) Put(name string, v T) error {
	if err := s.cfg.ValidName(name); err != nil {
		return err
	}
	return s.write(name, v)
}

func (s *Store[T]) write(name string, v T) error {
	if err := os.MkdirAll(s.Dir(), 0o755); err != nil {
		return fmt.Errorf("create %ss dir: %w", s.cfg.Kind, err)
	}
	data, err := s.cfg.Marshal(v)
	if err != nil {
		return err
	}
	return WriteFileAtomic(s.Path(name), data)
}

// Get reads one item by name. Returns os.ErrNotExist when absent so callers
// can dispatch 404 vs parse errors.
func (s *Store[T]) Get(name string) (T, error) {
	var zero T
	if err := s.cfg.ValidName(name); err != nil {
		return zero, err
	}
	data, err := os.ReadFile(s.Path(name))
	if err != nil {
		return zero, err
	}
	return s.cfg.Unmarshal(name, data)
}

// ListNames returns every registered item's name, sorted. A missing
// registry directory returns an empty slice (the empty-state) rather than
// an error, matching how first-run workspaces list. Files whose names could
// not have been registered (wrong extension, invalid name) are skipped.
func (s *Store[T]) ListNames() ([]string, error) {
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), s.cfg.Ext) {
			continue
		}
		name := strings.TrimSuffix(e.Name(), s.cfg.Ext)
		if err := s.cfg.ValidName(name); err != nil {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// List returns every registered item, sorted by name. Unreadable or
// malformed files are skipped — the actual error surfaces via Get on
// demand.
func (s *Store[T]) List() ([]T, error) {
	names, err := s.ListNames()
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(names))
	for _, name := range names {
		v, err := s.Get(name)
		if err != nil {
			continue
		}
		out = append(out, v)
	}
	return out, nil
}

// Delete removes an item from the registry. Returns os.ErrNotExist when the
// name is unknown. Domain deletion guards (e.g. credentials' source-scan)
// belong to the owning package's service layer, not here.
func (s *Store[T]) Delete(name string) error {
	if err := s.cfg.ValidName(name); err != nil {
		return err
	}
	return os.Remove(s.Path(name))
}
