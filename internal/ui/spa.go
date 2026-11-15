package ui

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"net/http"
	"strings"
)

// RuntimeConfig is the frontend's runtime configuration, written into
// index.html as `window.__CLAVESA__` at serve time (ADR-032 section 3).
// It replaces the build-time VITE_API_BASE_URL, so one ui/ build serves
// the local UI, the multi-workspace UI and the hosted UI.
type RuntimeConfig struct {
	// Mode is "local" (one workspace), "multi" (`clavesa ui --workspaces`)
	// or "hosted" (ui.clavesa.dev; written by the hosted deploy, not here).
	Mode string `json:"mode"`
	// Basename is the React Router basename: "" at the root,
	// "/w/<name>" inside one workspace of a multi-workspace server.
	Basename string `json:"basename"`
	// APIBase is the prefix every API call starts with.
	APIBase string `json:"apiBase"`
	// ReadOnly hides the controls that change state (useReadOnly()).
	ReadOnly bool `json:"readOnly"`
	// Workspace is the path-safe workspace name inside a multi-workspace
	// server; empty at the root and in local mode.
	Workspace string `json:"workspace,omitempty"`
}

// spaHandler serves the embedded SPA. Paths that name a file serve it;
// every other path serves index.html (client-side routing) with the
// runtime config for that request injected before </head>.
type spaHandler struct {
	static fs.FS
	config func(r *http.Request) RuntimeConfig
}

// SPAHandler returns the handler for the embedded SPA. config picks the
// runtime config for each request (multi mode derives the workspace from
// the path).
func SPAHandler(static fs.FS, config func(r *http.Request) RuntimeConfig) http.Handler {
	return spaHandler{static: static, config: config}
}

func (h spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p != "" && p != "index.html" {
		if f, err := h.static.Open(p); err == nil {
			st, statErr := f.Stat()
			f.Close()
			if statErr == nil && !st.IsDir() {
				http.FileServer(http.FS(h.static)).ServeHTTP(w, r)
				return
			}
		}
	}
	h.serveIndex(w, r)
}

func (h spaHandler) serveIndex(w http.ResponseWriter, r *http.Request) {
	raw, err := fs.ReadFile(h.static, "index.html")
	if err != nil {
		http.Error(w, "index.html missing from the embedded UI", http.StatusInternalServerError)
		return
	}
	body := InjectRuntimeConfig(raw, h.config(r))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The config differs per path and per server flag; never cache it.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// InjectRuntimeConfig puts a `window.__CLAVESA__ = {...}` script before
// </head> (or at the start when there is no </head>). json.Marshal escapes
// <, > and &, so a value cannot close the script tag.
func InjectRuntimeConfig(index []byte, cfg RuntimeConfig) []byte {
	js, _ := json.Marshal(cfg)
	tag := []byte("<script>window.__CLAVESA__=" + string(js) + ";</script>")
	if i := bytes.Index(index, []byte("</head>")); i >= 0 {
		out := make([]byte, 0, len(index)+len(tag))
		out = append(out, index[:i]...)
		out = append(out, tag...)
		return append(out, index[i:]...)
	}
	return append(tag, index...)
}
