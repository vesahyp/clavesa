package ui

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/vesahyp/clavesa/internal/httputil"
	wspkg "github.com/vesahyp/clavesa/internal/workspace"
)

// WorkspaceEntry is one workspace of a multi-workspace server, as
// GET /api/workspaces lists it. Everything in it comes from files in the
// workspace directory, so the list costs no AWS call and no query; the
// Workspaces page fetches each card's health from
// /w/<slug>/api/workspace/summary in parallel.
type WorkspaceEntry struct {
	// Name is the workspace name from clavesa.json.
	Name string `json:"name"`
	// Slug is the path-safe name in /w/<slug>/: the directory name.
	Slug string `json:"slug"`
	// Kind is "local" or "cloud" (the active warehouse, ADR-024).
	Kind string `json:"kind"`
	// Region is the Terraform backend region, when the workspace has one.
	Region string `json:"region,omitempty"`
	// APIBase is where this workspace's API is mounted.
	APIBase string `json:"api_base"`
	// Path is the workspace directory on this machine. Not serialized:
	// the hosted UI must not leak local paths.
	Path string `json:"-"`
}

// slugRE is what a directory name must match to be served under /w/.
var slugRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// DiscoverWorkspaces lists the workspaces directly under dir: each
// subdirectory with a clavesa.json, sorted by slug. A directory whose
// name is not path-safe is skipped.
func DiscoverWorkspaces(dir string) ([]WorkspaceEntry, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read workspaces dir: %w", err)
	}
	var out []WorkspaceEntry
	for _, e := range ents {
		if !e.IsDir() || !slugRE.MatchString(e.Name()) {
			continue
		}
		root := filepath.Join(dir, e.Name())
		if _, err := os.Stat(filepath.Join(root, "clavesa.json")); err != nil {
			continue
		}
		entry := WorkspaceEntry{
			Name:    e.Name(),
			Slug:    e.Name(),
			Kind:    "cloud",
			APIBase: "/w/" + e.Name() + "/api",
			Path:    root,
		}
		if wspkg.LoadWarehouse(root) == wspkg.WarehouseLocal {
			entry.Kind = "local"
		}
		if m, _ := wspkg.Load(root); m != nil {
			if m.Name != "" {
				entry.Name = m.Name
			}
			if m.Backend != nil {
				entry.Region = m.Backend.Region
			}
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slug < out[j].Slug })
	return out, nil
}

// MuxFactory builds the API handler for one workspace. The router calls
// it once per workspace, on first use, and caches the result.
type MuxFactory func(e WorkspaceEntry) (http.Handler, error)

// MultiRouter serves several workspaces from one server (ADR-032
// section 3):
//
//	GET /api/workspaces      the workspace list
//	/w/<slug>/api/...        that workspace's BuildWorkspaceMux
//	everything else          the SPA, with the runtime config for the path
type MultiRouter struct {
	entries  []WorkspaceEntry
	bySlug   map[string]WorkspaceEntry
	factory  MuxFactory
	spa      http.Handler
	readOnly bool

	mu    sync.Mutex
	muxes map[string]http.Handler
}

// NewMultiRouter returns the router over entries. static is the embedded
// SPA filesystem (nil serves no SPA, for tests of the API half).
func NewMultiRouter(entries []WorkspaceEntry, factory MuxFactory, static fs.FS, readOnly bool) *MultiRouter {
	m := &MultiRouter{
		entries:  entries,
		bySlug:   make(map[string]WorkspaceEntry, len(entries)),
		factory:  factory,
		readOnly: readOnly,
		muxes:    map[string]http.Handler{},
	}
	for _, e := range entries {
		m.bySlug[e.Slug] = e
	}
	if static != nil {
		m.spa = SPAHandler(static, m.Config)
	}
	return m
}

// Config returns the SPA runtime config for a request path: inside
// /w/<slug>/ of a known workspace the basename and API base point at it;
// anywhere else they are the root.
func (m *MultiRouter) Config(r *http.Request) RuntimeConfig {
	// One page shows every workspace, so the zone is the machine's (or
	// $CLAVESA_TIME_ZONE), not one workspace's.
	zone, _ := wspkg.TimeZone("")
	cfg := RuntimeConfig{Mode: "multi", Basename: "", APIBase: "/api", ReadOnly: m.readOnly, TimeZone: zone}
	if slug, _, ok := splitWorkspacePath(r.URL.Path); ok {
		if _, known := m.bySlug[slug]; known {
			cfg.Basename = "/w/" + slug
			cfg.APIBase = "/w/" + slug + "/api"
			cfg.Workspace = slug
		}
	}
	return cfg
}

func (m *MultiRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/api/workspaces":
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			httputil.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		list := m.entries
		if list == nil {
			list = []WorkspaceEntry{}
		}
		httputil.WriteJSON(w, http.StatusOK, map[string]any{"workspaces": list})
		return
	case path == "/api" || strings.HasPrefix(path, "/api/"):
		httputil.WriteError(w, http.StatusNotFound, "not found: "+path)
		return
	}
	if slug, rest, ok := splitWorkspacePath(path); ok && (rest == "/api" || strings.HasPrefix(rest, "/api/")) {
		h, err := m.workspaceMux(slug)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if h == nil {
			httputil.WriteError(w, http.StatusNotFound, "unknown workspace: "+slug)
			return
		}
		http.StripPrefix("/w/"+slug+"/api", h).ServeHTTP(w, r)
		return
	}
	if m.spa == nil {
		httputil.WriteError(w, http.StatusNotFound, "not found: "+path)
		return
	}
	m.spa.ServeHTTP(w, r)
}

// workspaceMux returns the cached mux for slug, building it on first use.
// (nil, nil) means no such workspace.
func (m *MultiRouter) workspaceMux(slug string) (http.Handler, error) {
	e, ok := m.bySlug[slug]
	if !ok {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.muxes[slug]; ok {
		return h, nil
	}
	h, err := m.factory(e)
	if err != nil {
		return nil, fmt.Errorf("workspace %s: %w", slug, err)
	}
	m.muxes[slug] = h
	return h, nil
}

// splitWorkspacePath splits "/w/<slug>/<rest>" into slug and "/<rest>".
func splitWorkspacePath(path string) (slug, rest string, ok bool) {
	if !strings.HasPrefix(path, "/w/") {
		return "", "", false
	}
	tail := strings.TrimPrefix(path, "/w/")
	slug, after, found := strings.Cut(tail, "/")
	if slug == "" {
		return "", "", false
	}
	if !found {
		return slug, "", true
	}
	return slug, "/" + after, true
}
