package runlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// fileBackend stores the lease at `<warehouse>/_locks/<pipeline>.run.json`
// (local warehouse). Create is atomic via write-temp + hard link (exclusive
// like O_EXCL, but readers never see partial content — see create); replace
// is a nonce-checked write-temp + rename CAS. The nonce check has a small
// read-to-rename window cross-process (POSIX rename can't compare), which
// is acceptable here: the atomic O_EXCL create covers the hot contention
// path (two fresh runs racing a free lock), and takeover/release only race
// against an already-expired or finished holder. mu serializes the CAS
// in-process so two goroutines in one binary never interleave it.
type fileBackend struct {
	path string
	mu   sync.Mutex
}

func (b *fileBackend) where() string { return b.path }

func (b *fileBackend) get(_ context.Context) (*leaseDoc, string, error) {
	data, err := os.ReadFile(b.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, "", errNotExist
	}
	if err != nil {
		return nil, "", err
	}
	var doc leaseDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, "", fmt.Errorf("parse lease %s: %w", b.path, err)
	}
	// The lease nonce doubles as the file backend's CAS token (the file
	// system has no etag); it rotates on every acquisition/takeover.
	return &doc, doc.Nonce, nil
}

// create is the atomic create-if-absent: the doc is fully written to a
// private temp file first, then hard-linked into place. link(2) fails with
// EEXIST when the lease already exists — the same exclusive-create
// semantics as O_CREATE|O_EXCL, but a concurrent get never observes a
// half-written lease (the visible file appears with its content complete).
func (b *fileBackend) create(_ context.Context, doc *leaseDoc) (string, error) {
	if err := os.MkdirAll(filepath.Dir(b.path), 0o755); err != nil {
		return "", err
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	tmp := b.path + ".tmp." + doc.Nonce
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", err
	}
	defer os.Remove(tmp)
	if err := os.Link(tmp, b.path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", errRaced
		}
		return "", err
	}
	return doc.Nonce, nil
}

func (b *fileBackend) replace(ctx context.Context, token string, doc *leaseDoc) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, curToken, err := b.get(ctx)
	if errors.Is(err, errNotExist) {
		return "", errRaced
	}
	if err != nil {
		return "", err
	}
	if curToken != token {
		return "", errRaced
	}
	tmp := b.path + ".tmp." + doc.Nonce
	data, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, b.path); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return doc.Nonce, nil
}
