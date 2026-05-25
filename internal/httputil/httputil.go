// Package httputil provides shared HTTP response helpers used by all API handlers.
package httputil

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// WriteJSON serialises v as JSON and writes it with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes a JSON error response: {"error": "<message>"}.
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]string{"error": message})
}

// DecodeJSON reads and JSON-decodes the request body into a fresh T.
// On failure it writes a 400 with a canonical "invalid request body"
// message and returns (zero, false) — the caller should return early.
// Generic over T so each handler stays a one-liner: `req, ok :=
// httputil.DecodeJSON[myRequest](w, r); if !ok { return }`.
func DecodeJSON[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return v, false
	}
	return v, true
}

// RequireQuery reads the trimmed value of one query parameter. Writes a
// 400 and returns ("", false) when the parameter is missing or empty.
func RequireQuery(w http.ResponseWriter, r *http.Request, name string) (string, bool) {
	v := strings.TrimSpace(r.URL.Query().Get(name))
	if v == "" {
		WriteError(w, http.StatusBadRequest, "missing required query param: "+name)
		return "", false
	}
	return v, true
}

// RequireFields validates that every named field has a non-empty value.
// Writes a 400 listing the missing keys (sorted) and returns false; in
// that case the caller should return early. Replaces the open-coded
// `if req.X == "" || req.Y == "" { ... "X, Y, Z are required" }` blocks.
func RequireFields(w http.ResponseWriter, fields map[string]string) bool {
	var missing []string
	for k, v := range fields {
		if strings.TrimSpace(v) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return true
	}
	sort.Strings(missing)
	if len(missing) == 1 {
		WriteError(w, http.StatusBadRequest, missing[0]+" is required")
	} else {
		WriteError(w, http.StatusBadRequest, strings.Join(missing, ", ")+" are required")
	}
	return false
}
