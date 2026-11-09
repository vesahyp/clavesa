package databricks

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeWorkspace is an httptest Databricks workspace. Each field is one
// admin step; the zero value is a workspace where none was done.
type fakeWorkspace struct {
	clientID, secret string

	metastoreID      string
	externalAccess   *bool // nil: the field is absent from metastore_summary
	metastorePrivs   []string
	metastoreForbid  bool // effective-permissions on the metastore answers 403
	catalogs         map[string][]string // catalog -> privileges the SP holds
	catalogOwner     map[string]string   // catalog -> owner (default "admins")
	externalLocation map[string]string   // name -> url

	noEntitlement bool // workspace member without workspace-access: UC calls 403

	tokenCalls int
}

func boolPtr(b bool) *bool { return &b }

func (f *fakeWorkspace) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /oidc/v1/token", func(w http.ResponseWriter, r *http.Request) {
		f.tokenCalls++
		id, secret, ok := r.BasicAuth()
		if !ok || id != f.clientID || secret != f.secret {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
			return
		}
		writeJSON(w, map[string]any{"access_token": "tok", "expires_in": 3600})
	})
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer tok" {
				apiErr(w, http.StatusUnauthorized, "UNAUTHENTICATED", "no token")
				return
			}
			h(w, r)
		}
	}
	mux.HandleFunc("GET /api/2.1/unity-catalog/metastore_summary", auth(func(w http.ResponseWriter, r *http.Request) {
		if f.noEntitlement {
			apiErr(w, http.StatusForbidden, "403", "This API is disabled for users without the databricks-sql-access or workspace-access or workspace-consume entitlements.")
			return
		}
		if f.metastoreID == "" {
			apiErr(w, http.StatusNotFound, "METASTORE_DOES_NOT_EXIST", "no metastore assigned")
			return
		}
		body := map[string]any{"metastore_id": f.metastoreID, "name": "ms", "region": "us-east-2"}
		if f.externalAccess != nil {
			body["external_access_enabled"] = *f.externalAccess
		}
		writeJSON(w, body)
	}))
	mux.HandleFunc("GET /api/2.1/unity-catalog/catalogs/{name}", auth(func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if _, ok := f.catalogs[name]; !ok {
			apiErr(w, http.StatusNotFound, "CATALOG_DOES_NOT_EXIST", "Catalog '"+name+"' does not exist.")
			return
		}
		owner := "admins"
		if o, ok := f.catalogOwner[name]; ok {
			owner = o
		}
		writeJSON(w, map[string]any{"name": name, "owner": owner})
	}))
	mux.HandleFunc("GET /api/2.1/unity-catalog/external-locations/{name}", auth(func(w http.ResponseWriter, r *http.Request) {
		u, ok := f.externalLocation[r.PathValue("name")]
		if !ok {
			apiErr(w, http.StatusNotFound, "EXTERNAL_LOCATION_DOES_NOT_EXIST", "not found")
			return
		}
		writeJSON(w, map[string]any{"name": r.PathValue("name"), "url": u, "credential_name": "c"})
	}))
	mux.HandleFunc("GET /api/2.1/unity-catalog/effective-permissions/{type}/{name}", auth(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("principal"); got != f.clientID {
			t.Errorf("effective-permissions principal = %q, want %q", got, f.clientID)
		}
		var privs []string
		switch r.PathValue("type") {
		case "metastore":
			if f.metastoreForbid {
				apiErr(w, http.StatusForbidden, "PERMISSION_DENIED", "denied")
				return
			}
			privs = f.metastorePrivs
		case "catalog":
			privs = f.catalogs[r.PathValue("name")]
		}
		var list []map[string]any
		for _, p := range privs {
			list = append(list, map[string]any{"privilege": p})
		}
		writeJSON(w, map[string]any{"privilege_assignments": []map[string]any{{"principal": f.clientID, "privileges": list}}})
	}))
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func apiErr(w http.ResponseWriter, status int, code, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error_code": code, "message": msg})
}

// readyWorkspace is a fake where the admin did every step.
func readyWorkspace() *fakeWorkspace {
	return &fakeWorkspace{
		clientID: "sp-app-id", secret: "s3cret",
		metastoreID:    "ms-1",
		externalAccess: boolPtr(true),
		metastorePrivs: []string{"CREATE_EXTERNAL_METADATA", "CREATE_CATALOG", "CREATE_STORAGE_CREDENTIAL", "CREATE_EXTERNAL_LOCATION"},
		catalogs:       map[string][]string{"analytics": {"USE_CATALOG"}},
	}
}

func newTestClient(srv *httptest.Server, id, secret string) *Client {
	c := NewClient(srv.URL, id, secret)
	c.http = srv.Client()
	return c
}

func byID(checks []Check) map[string]Check {
	m := map[string]Check{}
	for _, c := range checks {
		m[c.ID] = c
	}
	return m
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
