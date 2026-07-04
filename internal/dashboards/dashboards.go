// Package dashboards implements the workspace-level dashboard registry
// introduced by ADR-021. A dashboard is a *definition* (title, controls,
// datasets, widgets), not runtime data, so it lives as code in the
// workspace alongside the source and credential registries (ADR-017) and
// pipeline `.tf` files: version-controlled, dev'd locally, promoted via
// the repo.
//
// Storage shape: one JSON file per dashboard under
// `<workspace>/.clavesa/dashboards/<slug>.json`. The file is the single
// source of truth across environments and across warehouse rebuilds. The
// prior model (a system Delta table written through the active Provider)
// is gone: its cloud write path was Athena, which cannot write Delta.
//
// The Store is byte-oriented: it owns the file (atomic write, slug
// validation, the registry directory) but leaves marshal/parse to the
// service layer, which already owns the dashboard domain types and their
// legacy-shape normalization. This keeps the package a thin filesystem
// mirror of internal/sources with no dashboard-shape duplication.
package dashboards

import (
	"fmt"

	"github.com/vesahyp/clavesa/internal/registry"
)

// RelDir is the workspace-relative directory holding dashboard JSON files.
// Mirrors `<workspace>/.clavesa/sources/` and `.../credentials/`.
const RelDir = ".clavesa/dashboards"

// Store is the file-backed dashboard registry rooted at a workspace
// directory. Methods are filesystem-naive (no locking), intended for the
// single-user CLI / single-process UI shape clavesa runs in today, the
// same contract internal/sources gives.
type Store struct {
	reg *registry.Store[[]byte]
}

// New returns a Store rooted at workspaceRoot. The directory itself is
// created lazily on first write, so callers don't need to pre-create it.
func New(workspaceRoot string) *Store {
	return &Store{reg: registry.New(workspaceRoot, registry.Config[[]byte]{
		Kind:      "dashboard",
		RelDir:    RelDir,
		Ext:       ".json",
		ValidName: ValidSlug,
		// The caller has already marshaled the document; only ensure a
		// trailing newline for diff-friendliness (copying so the caller's
		// slice isn't mutated).
		Marshal: func(data []byte) ([]byte, error) {
			if len(data) == 0 || data[len(data)-1] != '\n' {
				data = append(append([]byte(nil), data...), '\n')
			}
			return data, nil
		},
		Unmarshal: func(_ string, data []byte) ([]byte, error) {
			return data, nil
		},
	})}
}

// Dir returns the absolute path of the registry directory.
func (s *Store) Dir() string {
	return s.reg.Dir()
}

// Path returns the absolute path of a dashboard's JSON file.
func (s *Store) Path(slug string) string {
	return s.reg.Path(slug)
}

// ValidSlug reports whether slug is a legal dashboard identifier: 1-64
// chars, lowercase letters, digits, dash, underscore. The slug doubles as
// a filename, so this rejects anything that could traverse paths. Kept
// consistent with service.validDashboardSlug (the service layer mirrors
// the same rule for its own validation entry points).
//
// Deliberately NOT registry.ValidName: this rule lacks the must-start-with-
// a-lowercase-letter clause the other three registries enforce (review
// finding I-P3-2). Unifying would invalidate digit/dash-leading dashboards
// that already exist on disk, so the divergence is preserved until that
// decision is made; the generic store takes this as its validator.
func ValidSlug(slug string) error {
	if slug == "" {
		return fmt.Errorf("slug is required")
	}
	if len(slug) > 64 {
		return fmt.Errorf("slug must be <=64 chars (got %d)", len(slug))
	}
	for i, c := range slug {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '-' || c == '_':
		default:
			return fmt.Errorf("slug has invalid char %q at index %d (allowed: a-z 0-9 - _)", c, i)
		}
	}
	return nil
}

// Save writes the JSON document for slug, creating the registry directory
// on first write. Create and replace are the same operation: the slug is
// the key, so a save with an existing slug overwrites. The caller owns
// validation of the document body; this layer only guards the slug.
func (s *Store) Save(slug string, data []byte) error {
	return s.reg.Put(slug, data)
}

// Get reads one dashboard's JSON document by slug. Returns os.ErrNotExist
// when absent so callers dispatch 404.
func (s *Store) Get(slug string) ([]byte, error) {
	return s.reg.Get(slug)
}

// List returns every registered dashboard's slug, sorted. A missing
// registry directory returns an empty slice (the empty-state) rather than
// an error, matching how the sources registry lists for first-run
// workspaces.
func (s *Store) List() ([]string, error) {
	return s.reg.ListNames()
}

// Delete removes a dashboard from the registry. Returns os.ErrNotExist
// when the slug is unknown.
func (s *Store) Delete(slug string) error {
	return s.reg.Delete(slug)
}
