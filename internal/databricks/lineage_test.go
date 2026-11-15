package databricks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The list follows next_page_token; a delete of an object that is gone
// is not an error.
func TestExternalMetadataListAndDelete(t *testing.T) {
	var deleted []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /oidc/v1/token", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"access_token": "tok", "expires_in": 3600})
	})
	mux.HandleFunc("GET /api/2.0/lineage-tracking/external-metadata", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page_token") == "" {
			writeJSON(w, map[string]any{"external_metadata": []map[string]any{{"name": "a"}}, "next_page_token": "p2"})
			return
		}
		writeJSON(w, map[string]any{"external_metadata": []map[string]any{{"name": "b", "entity_type": "clavesa transform",
			"properties": map[string]string{"clavesa.pipeline": "p"}}}})
	})
	mux.HandleFunc("DELETE /api/2.0/lineage-tracking/external-metadata/{name}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("name") == "gone" {
			apiErr(w, http.StatusNotFound, "NOT_FOUND", "no such object")
			return
		}
		deleted = append(deleted, r.PathValue("name"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewClient(srv.URL, "id", "secret")
	got, err := c.ListExternalMetadata(context.Background())
	if err != nil || len(got) != 2 || got[0].Name != "a" || got[1].Properties["clavesa.pipeline"] != "p" || got[1].EntityType != "clavesa transform" {
		t.Fatalf("list: %+v, %v", got, err)
	}
	if err := c.DeleteExternalMetadata(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteExternalMetadata(context.Background(), "gone"); err != nil {
		t.Fatalf("delete of a missing object: %v", err)
	}
	if strings.Join(deleted, ",") != "b" {
		t.Fatalf("deleted %v", deleted)
	}
}
