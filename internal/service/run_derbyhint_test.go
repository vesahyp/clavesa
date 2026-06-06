package service

import (
	"errors"
	"strings"
	"testing"
)

// TestDerbyLockHint covers #21's clear-error path: when a local run fails
// because something else holds the embedded Derby metastore lock, the
// returned error must lead with an actionable remedy (stop `clavesa ui` /
// `docker stop <container>`) instead of only the raw XSDB6 stack trace.
func TestDerbyLockHint(t *testing.T) {
	s := &Service{workspace: t.TempDir()}

	xsdb6 := "Caused by: ERROR XSDB6: Another instance of Derby may have already booted the database /ws/.clavesa/warehouse/_metastore/metastore_db"
	unrelated := "py4j.protocol.Py4JJavaError: An error occurred while calling o123.saveAsTable"

	if h := s.derbyLockHint(xsdb6); h == "" {
		t.Fatalf("derbyLockHint should fire on an XSDB6 'already booted' trace")
	}
	if h := s.derbyLockHint(unrelated); h != "" {
		t.Errorf("derbyLockHint should not fire on an unrelated Spark error, got %q", h)
	}

	// withDerbyHint prepends the remedy and preserves the wrapped error.
	base := errors.New("pipeline runner: exit status 1")
	wrapped := s.withDerbyHint(base, xsdb6)
	if !errors.Is(wrapped, base) {
		t.Errorf("withDerbyHint must keep the original error unwrappable")
	}
	if !strings.Contains(wrapped.Error(), "Derby metastore lock") {
		t.Errorf("hint text missing from wrapped error:\n%s", wrapped.Error())
	}
	if !strings.Contains(wrapped.Error(), "clavesa-metastore-") {
		t.Errorf("hint should name the metastore container:\n%s", wrapped.Error())
	}

	// No-op when the signature is absent or the error is nil.
	if got := s.withDerbyHint(base, unrelated); got.Error() != base.Error() {
		t.Errorf("withDerbyHint should pass an unrelated error through unchanged, got %q", got.Error())
	}
	if got := s.withDerbyHint(nil, xsdb6); got != nil {
		t.Errorf("withDerbyHint(nil, …) should be nil, got %v", got)
	}
}
