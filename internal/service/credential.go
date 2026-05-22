package service

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/credentials"
)

// CredentialSpec mirrors credentials.Spec at the service boundary.
type CredentialSpec = credentials.Spec

// CredentialUsage names a registered source that references the credential.
type CredentialUsage struct {
	SourceName string `json:"source_name"`
}

// ErrCredentialInUse is returned by DeleteCredential when one or more
// sources still reference the credential. Mirrors ErrSourceInUse so the
// HTTP layer can surface a 409 with a structured body identically.
type ErrCredentialInUse struct {
	Name   string
	Usages []CredentialUsage
}

func (e *ErrCredentialInUse) InUseUsages() []CredentialUsage { return e.Usages }

func (e *ErrCredentialInUse) Error() string {
	parts := make([]string, 0, len(e.Usages))
	for _, u := range e.Usages {
		parts = append(parts, u.SourceName)
	}
	return fmt.Sprintf("credential %q is in use by: %s", e.Name, strings.Join(parts, ", "))
}

// ErrCredentialNotFound is returned by AddSource when the source's
// `credentials` field references an unregistered credential. Surfaced
// by the API layer as 400 (user-correctable inside the request shape).
var ErrCredentialNotFound = errors.New("credential not registered")

// credentialStore returns the workspace-rooted storage instance.
func (s *Service) credentialStore() *credentials.Store {
	return credentials.New(s.workspace)
}

// AddCredential registers a new credential in the workspace registry.
func (s *Service) AddCredential(spec CredentialSpec) (CredentialSpec, error) {
	if err := s.credentialStore().Add(spec); err != nil {
		return CredentialSpec{}, err
	}
	return s.credentialStore().Get(spec.Name)
}

// ListCredentials returns every registered credential, sorted by name.
func (s *Service) ListCredentials() ([]CredentialSpec, error) {
	return s.credentialStore().List()
}

// GetCredential reads one credential by name.
func (s *Service) GetCredential(name string) (CredentialSpec, error) {
	return s.credentialStore().Get(name)
}

// DeleteCredential removes a credential after a source-scan deletion
// guard. `force` skips the guard.
//
// A delete of an unregistered name returns a wrapped os.ErrNotExist
// (CLI/HTTP layers translate it to "credential X not registered" /
// 404). Without this wrap, the CLI used to print the raw filesystem
// path, leaking workspace internals.
func (s *Service) DeleteCredential(name string, force bool) error {
	if _, err := s.GetCredential(name); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &notRegisteredError{kind: "credential", name: name}
		}
		return err
	}
	if !force {
		usages, err := s.findCredentialUsages(name)
		if err != nil {
			return fmt.Errorf("scan sources for credential usage: %w", err)
		}
		if len(usages) > 0 {
			return &ErrCredentialInUse{Name: name, Usages: usages}
		}
	}
	return s.credentialStore().Delete(name)
}

// findCredentialUsages returns every source spec whose `credentials`
// field references the named credential.
func (s *Service) findCredentialUsages(name string) ([]CredentialUsage, error) {
	out := []CredentialUsage{}
	srcs, err := s.ListSources()
	if err != nil {
		return nil, err
	}
	for _, src := range srcs {
		if src.Credentials == name {
			out = append(out, CredentialUsage{SourceName: src.Name})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SourceName < out[j].SourceName })
	return out, nil
}
