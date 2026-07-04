package notebooks

import (
	"fmt"
	"os"

	"github.com/vesahyp/clavesa/internal/registry"
)

// RelDir is the workspace-relative directory holding notebook .ipynb files.
// Top-level (not under .clavesa/) per the plan — notebooks are user-authored
// artifacts, sibling of pipelines/. Git-friendly: users can `git ls-files
// notebooks/` and see what they have.
const RelDir = "notebooks"

// Ext is the file extension. Always .ipynb for GitHub-native rendering.
const Ext = ".ipynb"

// Store is the file-backed notebook registry rooted at a workspace directory.
// Built on internal/registry — filesystem-naive (no locking) for the
// single-user CLI / single-process UI shape clavesa runs in today.
type Store struct {
	reg *registry.Store[*Notebook]
}

// New returns a Store rooted at workspaceRoot. The directory is created
// lazily on first write so empty workspaces stay empty.
//
// Name rules come from registry.ValidName: 1–64 chars, lowercase
// letters/digits + `-` `_`, must start with a letter. Notebook names double
// as filenames (and stable IDs for URL deep links), so reject anything that
// could traverse or surprise a shell.
func New(workspaceRoot string) *Store {
	return &Store{reg: registry.New(workspaceRoot, registry.Config[*Notebook]{
		Kind:   "notebook",
		RelDir: RelDir,
		Ext:    Ext,
		Marshal: func(nb *Notebook) ([]byte, error) {
			return nb.Marshal()
		},
		Unmarshal: func(name string, data []byte) (*Notebook, error) {
			return Parse(data, name)
		},
	})}
}

// Dir returns the absolute path of the notebooks directory.
func (s *Store) Dir() string {
	return s.reg.Dir()
}

// Path returns the absolute path of a notebook's .ipynb file.
func (s *Store) Path(name string) string {
	return s.reg.Path(name)
}

// Create writes a fresh empty notebook for the given name. Refuses to
// overwrite an existing notebook — call Delete first or pick a different name.
func (s *Store) Create(name string) (*Notebook, error) {
	nb := NewEmpty(name)
	if err := s.reg.Create(name, nb); err != nil {
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
	return s.reg.Update(nb.Name, nb)
}

// Get reads one notebook by name. Returns os.ErrNotExist when absent so
// callers can distinguish 404 from parse errors.
func (s *Store) Get(name string) (*Notebook, error) {
	return s.reg.Get(name)
}

// List returns every notebook by name (not the full file content), sorted.
// A missing notebooks dir returns an empty slice — matches the empty-state
// pattern of sources.List for first-run workspaces.
//
// The returned slice contains lightweight metadata only (name + cell count)
// so the UI list page doesn't pay the full-parse cost for every notebook
// just to render the sidebar. Parsing here is cheap relative to the file
// size (notebooks are typically <100 KB even with outputs); if parse fails
// the notebook is skipped in the list but `Get(name)` will still surface
// the actual error to the user.
func (s *Store) List() ([]Summary, error) {
	names, err := s.reg.ListNames()
	if err != nil {
		return nil, err
	}
	out := make([]Summary, 0, len(names))
	for _, name := range names {
		nb, err := s.reg.Get(name)
		if err != nil {
			continue
		}
		info, err := os.Stat(s.reg.Path(name))
		if err != nil {
			continue
		}
		out = append(out, Summary{
			Name:      name,
			CellCount: len(nb.Cells),
			ModTime:   info.ModTime().UTC().Format("2006-01-02T15:04:05Z"),
		})
	}
	return out, nil
}

// Delete removes a notebook from the registry. The service layer doesn't
// gate deletion (notebooks aren't referenced by pipelines or .tf files —
// they're pure user artifacts, unlike sources).
func (s *Store) Delete(name string) error {
	return s.reg.Delete(name)
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
	if err := s.reg.Put(name, nb); err != nil {
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
