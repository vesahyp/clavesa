package httputil_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vesahyp/clavesa/internal/errs"
	"github.com/vesahyp/clavesa/internal/httputil"
)

// TestWriteServiceError pins the sentinel→status mapping and the fallback
// behavior: wrapped internal/errs sentinels pick their documented status,
// opaque errors keep the caller's legacy per-route status, and every
// response uses the canonical {"error": ...} envelope.
func TestWriteServiceError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		fallback   int
		wantStatus int
	}{
		{
			name:       "not found sentinel maps to 404",
			err:        fmt.Errorf("pipeline %q: %w", "demo", errs.ErrNotFound),
			fallback:   http.StatusBadGateway,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid input sentinel maps to 400",
			err:        fmt.Errorf("bad ref shape: %w", errs.ErrInvalidInput),
			fallback:   http.StatusBadGateway,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "conflict sentinel maps to 409",
			err:        fmt.Errorf("slug taken: %w", errs.ErrConflict),
			fallback:   http.StatusInternalServerError,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "run in flight maps to 409",
			err:        fmt.Errorf("dispatch: %w", errs.ErrRunInFlight),
			fallback:   http.StatusInternalServerError,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "upstream sentinel maps to 502",
			err:        fmt.Errorf("athena: %w", errs.ErrUpstream),
			fallback:   http.StatusInternalServerError,
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "local not implemented maps to 501",
			err:        fmt.Errorf("column stats: %w", errs.ErrLocalNotImplemented),
			fallback:   http.StatusInternalServerError,
			wantStatus: http.StatusNotImplemented,
		},
		{
			name:       "opaque error keeps the fallback status",
			err:        fmt.Errorf("something opaque broke"),
			fallback:   http.StatusBadGateway,
			wantStatus: http.StatusBadGateway,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			httputil.WriteServiceError(rr, tt.err, tt.fallback)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantStatus)
			}
			if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
			var body map[string]string
			if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}
			if body["error"] != tt.err.Error() {
				t.Errorf("error message = %q, want %q", body["error"], tt.err.Error())
			}
		})
	}
}
