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
	"strings"

	"github.com/vesahyp/clavesa/internal/registry"
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
	reg *registry.Store[Spec]
}

// New returns a Store rooted at workspaceRoot. Name rules come from
// registry.ValidName — same rules as sources so users learn one.
func New(workspaceRoot string) *Store {
	return &Store{reg: registry.New(workspaceRoot, registry.Config[Spec]{
		Kind:   "credential",
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

// Path returns the absolute path of a credential's JSON file.
func (s *Store) Path(name string) string {
	return s.reg.Path(name)
}

func (s Spec) validate() error {
	if err := registry.ValidName(s.Name); err != nil {
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
	return s.reg.Create(spec.Name, spec)
}

// Get reads one credential by name. Returns os.ErrNotExist when absent.
func (s *Store) Get(name string) (Spec, error) {
	return s.reg.Get(name)
}

// List returns every registered credential, sorted by name.
func (s *Store) List() ([]Spec, error) {
	return s.reg.List()
}

// Delete removes a credential. Caller is responsible for the source-scan
// deletion guard (see service.DeleteCredential) — this layer just owns
// the file.
func (s *Store) Delete(name string) error {
	return s.reg.Delete(name)
}
