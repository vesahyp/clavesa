// Package httputil provides shared HTTP response helpers used by all API handlers.
package httputil

import (
	"encoding/json"
	"net/http"
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
