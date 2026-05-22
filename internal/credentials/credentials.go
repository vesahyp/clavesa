// Package credentials implements ADR-017's workspace-level credentials
// registry — separate from the sources registry the same way Databricks
// keeps Storage Credentials separate from External Locations.
//
// One JSON file per credential under
// `<workspace>/.clavesa/credentials/<name>.json`. The file records
// the credential *kind* and a *secret reference* (URL-style prefix) —
// never the secret material itself. Slice 2 supports `kind=header` only
// and three secret backends: `arn:aws:secretsmanager:...`, `env:VAR`,
// and `file:<workspace-relative-path>`.
//
// The package is filesystem-naive (no locking) — same shape as the
// sibling sources package; both are intended for the single-user CLI /
// single-process UI clavesa runs in today.
package credentials

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RelDir is the workspace-relative directory holding credential JSON
// files. Mirrors `<workspace>/.clavesa/sources/` for symmetry.
const RelDir = ".clavesa/credentials"

// SecretFileGlob is the gitignore pattern `workspace init` adds for
// file:-backed credential payloads. Lives next to RelDir so the workspace
// can ship .gitignore'd plaintext secrets without leaking them.
const SecretFileGlob = ".clavesa/credentials/*.secret"

// Spec is the on-disk shape of one credential. The Secret field is a
// URL-style reference (`arn:aws:secretsmanager:...`, `env:VAR`,
// `file:<rel>`); secret material is fetched at runtime by the backend
// the prefix selects.
type Spec struct {
	// Name is set on Load from the filename so callers don't double-source.
	Name string `json:"-"`

	// Kind is the credential discriminator. Slice 2: only "header".
	// Future kinds (`assume-role`, multi-header / OAuth / signed-request
	// flows) extend the discriminator without breaking the registry.
	Kind string `json:"kind"`

	// HeaderName is the HTTP header to inject (kind="header"). Typically
	// "Authorization".
	HeaderName string `json:"header_name,omitempty"`

	// ValuePrefix is prepended to the resolved secret value before
	// header injection — covers "Bearer " token schemes without making
	// the user encode the prefix in the secret itself.
	ValuePrefix string `json:"value_prefix,omitempty"`

	// Secret is the backend reference. One of:
	//   arn:aws:secretsmanager:<region>:<account>:secret:<name>
	//   env:VAR_NAME
	//   file:<workspace-relative-path>
	Secret string `json:"secret"`
}

// SecretBackend returns the prefix portion of Spec.Secret used to pick
// a runtime resolver. Empty string when the secret reference doesn't
// match any known backend (validation rejects this at register time;
// callers can use this to dispatch with confidence).
func (s Spec) SecretBackend() string {
	switch {
	case strings.HasPrefix(s.Secret, "arn:aws:secretsmanager:"):
		return "arn"
	case strings.HasPrefix(s.Secret, "env:"):
		return "env"
	case strings.HasPrefix(s.Secret, "file:"):
		return "file"
	default:
		return ""
	}
}

// Store is the file-backed credentials registry rooted at a workspace
// directory.
type Store struct {
	workspaceRoot string
}

// New returns a Store rooted at workspaceRoot.
func New(workspaceRoot string) *Store {
	return &Store{workspaceRoot: workspaceRoot}
}

// Dir returns the absolute path of the registry directory.
func (s *Store) Dir() string {
	return filepath.Join(s.workspaceRoot, RelDir)
}

// Path returns the absolute path of a credential's JSON file.
func (s *Store) Path(name string) string {
	return filepath.Join(s.Dir(), name+".json")
}

// validName mirrors sources.validName — same rules so users learn one.
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

func (s Spec) validate() error {
	if err := validName(s.Name); err != nil {
		return err
	}
	switch s.Kind {
	case "header":
		if s.HeaderName == "" {
			return fmt.Errorf("header_name is required for kind=header")
		}
	default:
		return fmt.Errorf("unsupported credential kind %q (slice 2: only header)", s.Kind)
	}
	if s.Secret == "" {
		return fmt.Errorf("secret reference is required (arn:aws:secretsmanager:..., env:VAR, or file:<path>)")
	}
	if s.SecretBackend() == "" {
		return fmt.Errorf("secret reference must use a known backend prefix (arn:aws:secretsmanager:..., env:, file:); got %q", s.Secret)
	}
	return nil
}

// Add writes a new credential spec. Refuses to overwrite — call Delete
// first or use a different name.
func (s *Store) Add(spec Spec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	path := s.Path(spec.Name)
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("credential %q already exists", spec.Name)
	}
	if err := os.MkdirAll(s.Dir(), 0o755); err != nil {
		return fmt.Errorf("create credentials dir: %w", err)
	}
	return writeJSON(path, spec)
}

// Get reads one credential by name. Returns os.ErrNotExist when absent.
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

// List returns every registered credential, sorted by name.
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
			continue
		}
		spec, err := s.Get(name)
		if err != nil {
			continue
		}
		out = append(out, spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Delete removes a credential. Caller is responsible for the source-scan
// deletion guard (see service.DeleteCredential) — this layer just owns
// the file.
func (s *Store) Delete(name string) error {
	if err := validName(name); err != nil {
		return err
	}
	return os.Remove(s.Path(name))
}

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
