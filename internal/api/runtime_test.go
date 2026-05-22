package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/observability"
)

type fakeWorkerLister struct {
	workers []observability.WorkerStatus
}

func (f *fakeWorkerLister) Workers() []observability.WorkerStatus { return f.workers }

// TestRuntimeWorkersEndpoint — GET /runtime/workers reports the worker
// list, and degrades to an empty list for a nil lister (no warm runner
// wired) and a runner with no workers.
func TestRuntimeWorkersEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		lister  api.WorkerLister
		wantLen int
	}{
		{"nil lister → empty", nil, 0},
		{"no workers → empty", &fakeWorkerLister{}, 0},
		{"two workers", &fakeWorkerLister{workers: []observability.WorkerStatus{
			{Warehouse: "/wh/a", State: "spawning", AgeMS: 1200},
			{Warehouse: "/wh/b", State: "ready", AgeMS: 30000},
		}}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			api.NewRuntimeHandler(tt.lister, api.AWSIdentity{}).RegisterRoutes(mux)

			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/runtime/workers", nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			var resp struct {
				Workers []observability.WorkerStatus `json:"workers"`
			}
			if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(resp.Workers) != tt.wantLen {
				t.Errorf("workers = %d, want %d", len(resp.Workers), tt.wantLen)
			}
		})
	}
}

// TestRuntimeIdentityEndpoint — GET /runtime/identity echoes the
// resolved AWS identity, and reports Available=false for the zero value
// (local-only mode / no credentials).
func TestRuntimeIdentityEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		identity api.AWSIdentity
	}{
		{"unavailable", api.AWSIdentity{}},
		{"resolved", api.AWSIdentity{
			Available: true,
			AccountID: "123456789012",
			ARN:       "arn:aws:iam::123456789012:user/dev",
			Profile:   "webbaa",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			api.NewRuntimeHandler(nil, tt.identity).RegisterRoutes(mux)

			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/runtime/identity", nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			var got api.AWSIdentity
			if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != tt.identity {
				t.Errorf("identity = %+v, want %+v", got, tt.identity)
			}
		})
	}
}
