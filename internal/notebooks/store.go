package notebooks

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RelDir is the workspace-relative directory holding notebook .ipynb files.
// Top-level (not under .clavesa/) per the plan — notebooks are user-authored
// artifacts, sibling of pipelines/. Git-friendly: users can `git ls-files
// notebooks/` and see what they have.
const RelDir = "notebooks"

// Ext is the file extension. Always .ipynb for GitHub-native rendering.
const Ext = ".ipynb"

// Store is the file-backed notebook registry rooted at a workspace directory.
// Mirrors internal/sources.Store — filesystem-naive (no locking) for the
// single-user CLI / single-process UI shape clavesa runs in today.
type Store struct {
	workspaceRoot string
}

// New returns a Store rooted at workspaceRoot. The directory is created
// lazily on first write so empty workspaces stay empty.
func New(workspaceRoot string) *Store {
	return &Store{workspaceRoot: workspaceRoot}
}

// Dir returns the absolute path of the notebooks directory.
func (s *Store) Dir() string {
	return filepath.Join(s.workspaceRoot, RelDir)
}

// Path returns the absolute path of a notebook's .ipynb file.
func (s *Store) Path(name string) string {
	return filepath.Join(s.Dir(), name+Ext)
}

// validName mirrors sources.validName: 1–64 chars, lowercase letters/digits
// + `-` `_`, must start with a letter. Notebook names double as filenames
// (and stable IDs for URL deep links), so reject anything that could traverse
// or surprise a shell.
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

// Create writes a fresh empty notebook for the given name. Refuses to
// overwrite an existing notebook — call Delete first or pick a different name.
func (s *Store) Create(name string) (*Notebook, error) {
	if err := validName(name); err != nil {
		return nil, err
	}
	path := s.Path(name)
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("notebook %q already exists", name)
	}
	if err := os.MkdirAll(s.Dir(), 0o755); err != nil {
		return nil, fmt.Errorf("create notebooks dir: %w", err)
	}
	nb := NewEmpty(name)
	if err := s.writeAtomic(path, nb); err != nil {
		return nil, err
	}
	return nb, nil
}

// Save persists an updated notebook. Requires the notebook to already exist
// — a missing file is an error, not a silent create (use Create for that).
// The name on the notebook is authoritative; callers that want a rename must
// Delete + Create + Save.
func (s *Store) Save(nb *Notebook) error {
	if nb == nil {
		return fmt.Errorf("notebook is nil")
	}
	if err := validName(nb.Name); err != nil {
		return err
	}
	path := s.Path(nb.Name)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("notebook %q does not exist", nb.Name)
		}
		return err
	}
	return s.writeAtomic(path, nb)
}

// Get reads one notebook by name. Returns os.ErrNotExist when absent so
// callers can distinguish 404 from parse errors.
func (s *Store) Get(name string) (*Notebook, error) {
	if err := validName(name); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.Path(name))
	if err != nil {
		return nil, err
	}
	return Parse(data, name)
}

// List returns every notebook by name (not the full file content), sorted.
// A missing notebooks dir returns an empty slice — matches the empty-state
// pattern of sources.List for first-run workspaces.
//
// The returned slice contains lightweight metadata only (name + cell count
// via a stat-skim style read) so the UI list page doesn't pay the full-parse
// cost for every notebook just to render the sidebar.
func (s *Store) List() ([]Summary, error) {
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		if os.IsNotExist(err) {
			return []Summary{}, nil
		}
		return nil, err
	}
	out := make([]Summary, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), Ext) {
			continue
		}
		name := strings.TrimSuffix(e.Name(), Ext)
		if err := validName(name); err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		// Parse the file so the summary can include cell count and last-
		// run-at — both are cheap relative to the file size (notebooks
		// are typically <100 KB even with outputs). If parse fails the
		// notebook is skipped in the list but `Get(name)` will still
		// surface the actual error to the user.
		nb, perr := s.Get(name)
		if perr != nil {
			continue
		}
		out = append(out, Summary{
			Name:      name,
			CellCount: len(nb.Cells),
			ModTime:   info.ModTime().UTC().Format("2006-01-02T15:04:05Z"),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Delete removes a notebook from the registry. The service layer doesn't
// gate deletion (notebooks aren't referenced by pipelines or .tf files —
// they're pure user artifacts, unlike sources).
func (s *Store) Delete(name string) error {
	if err := validName(name); err != nil {
		return err
	}
	return os.Remove(s.Path(name))
}

// ClearOutputs zeroes outputs[] and execution_count on every code cell of
// `name` and saves the result back. Matches `jupyter nbconvert --clear-output`
// for git-friendly commits. Also clears the per-cell `last_*` metadata so the
// "last run X ago" badges don't lie about absent outputs.
func (s *Store) ClearOutputs(name string) (*Notebook, error) {
	nb, err := s.Get(name)
	if err != nil {
		return nil, err
	}
	for i := range nb.Cells {
		c := &nb.Cells[i]
		if c.CellType != CellTypeCode {
			continue
		}
		c.Outputs = nil
		c.ExecutionCount = nil
		c.Metadata.Clavesa = nil
	}
	if err := s.writeAtomic(s.Path(name), nb); err != nil {
		return nil, err
	}
	return nb, nil
}

// Summary is the per-notebook entry in the list response. Keeps the list
// surface cheap to render without re-parsing every notebook on every page
// load — see List for the populated fields.
type Summary struct {
	Name      string `json:"name"`
	CellCount int    `json:"cell_count"`
	ModTime   string `json:"updated_at"`
}

// writeAtomic marshals nb to disk via temp-file rename so a crash between
// truncate and write doesn't leave a half-written .ipynb that List would
// skip and Get would surface as a parse error.
func (s *Store) writeAtomic(path string, nb *Notebook) error {
	data, err := nb.Marshal()
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
