package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMigrateBackendRefusesConcurrentRun: while one migration holds the
// handler, a second POST gets 409 and never reaches MigrateState.
func TestMigrateBackendRefusesConcurrentRun(t *testing.T) {
	wh := NewWorkspaceHandler(t.TempDir())
	wh.migrating.Lock()
	defer wh.migrating.Unlock()

	rr := httptest.NewRecorder()
	wh.MigrateBackend(rr, httptest.NewRequest(http.MethodPost, "/workspace/backend/migrate", nil))
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 while a migration is running; body %s", rr.Code, rr.Body.String())
	}
}
