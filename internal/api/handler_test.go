package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/service"
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

// twoNodeTF is a minimal two-node pipeline (source → transform). The
// transform uses the canonical `inputs = { alias = … }` map shape that
// service.AddEdge writes — not the legacy singular `input`. Tests that
// previously masked DeleteEdge bugs by using the singular form belong on
// this shape.
const twoNodeTF = `module "s3_source" {
  source = "clavesa/source/aws"
  name   = "s3_source"
  bucket = "my-data"
}

module "validate" {
  source = "clavesa/transform/aws"
  name   = "validate"
  inputs = {
    s3_source = module.s3_source.outputs["default"]
  }
  sql    = "SELECT * FROM raw WHERE amount > 0"
}
`

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func setupDir(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if content != "" {
		if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	return dir
}

func newMux(t *testing.T) *http.ServeMux {
	t.Helper()
	fo := fileops.New()
	h := api.New(fo, "")
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func doRequest(t *testing.T, mux http.Handler, method, target string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("json.Encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, target, &buf)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func decodeGraph(t *testing.T, rr *httptest.ResponseRecorder) graph.PipelineGraph {
	t.Helper()
	var g graph.PipelineGraph
	if err := json.NewDecoder(rr.Body).Decode(&g); err != nil {
		t.Fatalf("decode graph: %v (body: %s)", err, rr.Body.String())
	}
	return g
}

func decodeError(t *testing.T, rr *httptest.ResponseRecorder) string {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&e); err != nil {
		t.Fatalf("decode error: %v (body: %s)", err, rr.Body.String())
	}
	return e.Error
}

func nodeIDs(g graph.PipelineGraph) []string {
	ids := make([]string, len(g.Nodes))
	for i, n := range g.Nodes {
		ids[i] = n.ID
	}
	return ids
}

func hasNode(g graph.PipelineGraph, id string) bool {
	for _, n := range g.Nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

func hasEdge(g graph.PipelineGraph, fromNode, toNode string) bool {
	for _, e := range g.Edges {
		if e.FromNode == fromNode && e.ToNode == toNode {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// GET /pipeline
// ---------------------------------------------------------------------------

func TestGetPipeline(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		query      string
		setup      func(t *testing.T) string // returns dir
		wantStatus int
		wantNodes  int
		wantEdges  int
	}{
		{
			name:       "success two-node pipeline",
			query:      "?dir=DIR",
			setup:      func(t *testing.T) string { return setupDir(t, twoNodeTF) },
			wantStatus: http.StatusOK,
			wantNodes:  2,
			wantEdges:  1,
		},
		{
			name:       "success empty directory",
			query:      "?dir=DIR",
			setup:      func(t *testing.T) string { return setupDir(t, "") },
			wantStatus: http.StatusOK,
			wantNodes:  0,
			wantEdges:  0,
		},
		{
			name:       "missing dir param",
			query:      "",
			setup:      func(t *testing.T) string { return "" },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-existent directory",
			query:      "?dir=/tmp/clavesa-nonexistent-99999",
			setup:      func(t *testing.T) string { return "" },
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newMux(t)
			dir := tt.setup(t)

			target := "/pipeline" + tt.query
			if dir != "" {
				target = "/pipeline?dir=" + dir
			}

			rr := doRequest(t, mux, http.MethodGet, target, nil)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK {
				g := decodeGraph(t, rr)
				if len(g.Nodes) != tt.wantNodes {
					t.Errorf("nodes = %d, want %d", len(g.Nodes), tt.wantNodes)
				}
				if len(g.Edges) != tt.wantEdges {
					t.Errorf("edges = %d, want %d", len(g.Edges), tt.wantEdges)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// POST /pipeline/node
// ---------------------------------------------------------------------------

func TestAddNode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		body       interface{}
		wantStatus int
		wantNode   string
	}{
		{
			name: "success add destination node",
			body: map[string]interface{}{
				// dir injected at runtime
				"file":       "main.tf", // relative; resolved in test
				"block_name": "warehouse",
				"attributes": map[string]interface{}{
					"source": "clavesa/destination/aws",
					"name":   "warehouse",
					"bucket": "warehouse",
					"prefix": "clean/",
				},
			},
			wantStatus: http.StatusOK,
			wantNode:   "warehouse",
		},
		{
			name:       "missing dir",
			body:       map[string]interface{}{"file": "main.tf", "block_name": "x"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing block_name",
			body:       map[string]interface{}{"dir": "/tmp", "file": "main.tf"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate block name",
			body: map[string]interface{}{
				// dir and file injected at runtime
				"file":       "main.tf", // relative; resolved below
				"block_name": "s3_source",
				"attributes": map[string]interface{}{"source": "clavesa/source/aws"},
			},
			wantStatus: http.StatusConflict,
		},
		{
			name:       "malformed body",
			body:       "not json",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newMux(t)
			dir := setupDir(t, twoNodeTF)

			// Inject dir and absolute file path into body map where needed.
			if m, ok := tt.body.(map[string]interface{}); ok {
				if _, hasDir := m["dir"]; !hasDir && tt.name != "missing dir" {
					m["dir"] = dir
				}
				if f, ok := m["file"].(string); ok && !filepath.IsAbs(f) {
					m["file"] = filepath.Join(dir, f)
				}
			}

			rr := doRequest(t, mux, http.MethodPost, "/pipeline/nodes", tt.body)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK {
				g := decodeGraph(t, rr)
				if !hasNode(g, tt.wantNode) {
					t.Errorf("node %q not found in graph; nodes: %v", tt.wantNode, nodeIDs(g))
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// POST /pipeline/typed-nodes
// ---------------------------------------------------------------------------

// stubNodeAdder satisfies api.NodeAdder. It records the call args and
// optionally appends a minimal module block to <dir>/main.tf so the
// post-call re-parse succeeds.
type stubNodeAdder struct {
	gotDir, gotType, gotName string
	writeBlock               bool
	returnErr                error
}

func (s *stubNodeAdder) AddNode(dir, nodeType, name string) error {
	s.gotDir, s.gotType, s.gotName = dir, nodeType, name
	if s.returnErr != nil {
		return s.returnErr
	}
	if s.writeBlock {
		extra := []byte("\nmodule \"" + name + "\" {\n  source = \"clavesa/" + nodeType + "/aws\"\n  name   = \"" + name + "\"\n}\n")
		f, err := os.OpenFile(filepath.Join(dir, "main.tf"), os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.Write(extra)
		return err
	}
	return nil
}

func TestTypedAddNode(t *testing.T) {
	t.Parallel()

	t.Run("missing dir", func(t *testing.T) {
		fo := fileops.New()
		h := api.New(fo, "").WithNodeAdder(&stubNodeAdder{})
		mux := http.NewServeMux()
		h.RegisterRoutes(mux)
		rr := doRequest(t, mux, http.MethodPost, "/pipeline/typed-nodes",
			map[string]string{"type": "transform"})
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body: %s)", rr.Code, rr.Body.String())
		}
	})

	t.Run("missing type", func(t *testing.T) {
		fo := fileops.New()
		h := api.New(fo, "").WithNodeAdder(&stubNodeAdder{})
		mux := http.NewServeMux()
		h.RegisterRoutes(mux)
		rr := doRequest(t, mux, http.MethodPost, "/pipeline/typed-nodes",
			map[string]string{"dir": "/tmp"})
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body: %s)", rr.Code, rr.Body.String())
		}
	})

	t.Run("no NodeAdder wired returns 501", func(t *testing.T) {
		mux := newMux(t) // no WithNodeAdder
		rr := doRequest(t, mux, http.MethodPost, "/pipeline/typed-nodes",
			map[string]string{"dir": "/tmp", "type": "transform"})
		if rr.Code != http.StatusNotImplemented {
			t.Fatalf("status = %d, want 501 (body: %s)", rr.Code, rr.Body.String())
		}
	})

	t.Run("NodeAdder error surfaces as 400", func(t *testing.T) {
		dir := setupDir(t, twoNodeTF)
		adder := &stubNodeAdder{returnErr: errStub("inline source nodes have been removed")}
		fo := fileops.New()
		h := api.New(fo, "").WithNodeAdder(adder)
		mux := http.NewServeMux()
		h.RegisterRoutes(mux)
		rr := doRequest(t, mux, http.MethodPost, "/pipeline/typed-nodes",
			map[string]string{"dir": dir, "type": "source"})
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body: %s)", rr.Code, rr.Body.String())
		}
	})

	t.Run("happy path threads args and returns graph", func(t *testing.T) {
		dir := setupDir(t, twoNodeTF)
		adder := &stubNodeAdder{writeBlock: true}
		fo := fileops.New()
		h := api.New(fo, "").WithNodeAdder(adder)
		mux := http.NewServeMux()
		h.RegisterRoutes(mux)
		rr := doRequest(t, mux, http.MethodPost, "/pipeline/typed-nodes",
			map[string]string{"dir": dir, "type": "transform", "name": "agg"})
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
		}
		if adder.gotDir != dir || adder.gotType != "transform" || adder.gotName != "agg" {
			t.Errorf("adder got (%q, %q, %q), want (%q, transform, agg)",
				adder.gotDir, adder.gotType, adder.gotName, dir)
		}
		g := decodeGraph(t, rr)
		if !hasNode(g, "agg") {
			t.Errorf("returned graph missing node %q; nodes: %v", "agg", nodeIDs(g))
		}
	})
}

type errStub string

func (e errStub) Error() string { return string(e) }

// ---------------------------------------------------------------------------
// PUT /pipeline/node/{id}
// ---------------------------------------------------------------------------

func TestUpdateNode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		id         string
		body       interface{}
		wantStatus int
		checkGraph func(t *testing.T, g graph.PipelineGraph)
	}{
		{
			name: "success update sql attribute",
			id:   "validate",
			body: map[string]interface{}{
				// dir injected at runtime
				"attributes": map[string]interface{}{
					"sql": "SELECT id FROM raw",
				},
			},
			wantStatus: http.StatusOK,
			checkGraph: func(t *testing.T, g graph.PipelineGraph) {
				for _, n := range g.Nodes {
					if n.ID == "validate" {
						if got, ok := n.Config["sql"].(string); !ok || got != "SELECT id FROM raw" {
							t.Errorf("validate.sql = %v, want %q", n.Config["sql"], "SELECT id FROM raw")
						}
						return
					}
				}
				t.Error("validate node not found")
			},
		},
		{
			name:       "missing dir",
			id:         "validate",
			body:       map[string]interface{}{"attributes": map[string]interface{}{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown node id",
			id:   "does_not_exist",
			body: map[string]interface{}{
				// dir injected at runtime
				"attributes": map[string]interface{}{"sql": "x"},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "malformed body",
			id:         "validate",
			body:       "bad",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newMux(t)
			dir := setupDir(t, twoNodeTF)

			if m, ok := tt.body.(map[string]interface{}); ok {
				if _, hasDir := m["dir"]; !hasDir && tt.name != "missing dir" {
					m["dir"] = dir
				}
			}

			rr := doRequest(t, mux, http.MethodPut, "/pipeline/nodes/"+tt.id, tt.body)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK && tt.checkGraph != nil {
				g := decodeGraph(t, rr)
				tt.checkGraph(t, g)
			}
		})
	}
}

// recordingParser stubs service.SQLParser: it records what it was asked
// to parse and rejects anything containing "BROKEN".
type recordingParser struct {
	got []string
}

func (p *recordingParser) Parse(_ context.Context, sql string) error {
	p.got = append(p.got, sql)
	if strings.Contains(sql, "BROKEN") {
		return &observability.ParseError{Message: "syntax error near BROKEN"}
	}
	return nil
}

// TestUpdateNodeFileRefSQL covers the UI editor's save shape: the PUT
// carries `sql = file("<node>.sql")` as a plain string, with the script
// already written. The parse-check must validate the file's content —
// never the file() expression itself (which can't parse as SQL) — and
// skip silently when the referenced file is unreadable.
func TestUpdateNodeFileRefSQL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		fileContent string // "" = don't write the file
		wantStatus  int
		wantParsed  string // "" = parser must not be called
	}{
		{
			name:        "valid file content passes and is what gets parsed",
			fileContent: "SELECT id FROM raw",
			wantStatus:  http.StatusOK,
			wantParsed:  "SELECT id FROM raw",
		},
		{
			name:        "broken file content is rejected with the parser message",
			fileContent: "BROKEN sql",
			wantStatus:  http.StatusBadRequest,
			wantParsed:  "BROKEN sql",
		},
		{
			name:       "dangling ref skips the check and writes",
			wantStatus: http.StatusOK,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			dir := setupDir(t, twoNodeTF)
			if tt.fileContent != "" {
				if err := os.WriteFile(filepath.Join(dir, "validate.sql"), []byte(tt.fileContent), 0o644); err != nil {
					t.Fatalf("WriteFile: %v", err)
				}
			}
			parser := &recordingParser{}
			fo := fileops.New()
			h := api.New(fo, "").WithService(service.New("").WithSQLParser(parser))
			mux := http.NewServeMux()
			h.RegisterRoutes(mux)

			body := map[string]interface{}{
				"dir": dir,
				"attributes": map[string]interface{}{
					"sql": `file("validate.sql")`,
				},
			}
			rr := doRequest(t, mux, http.MethodPut, "/pipeline/nodes/validate", body)
			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantParsed == "" {
				if len(parser.got) != 0 {
					t.Errorf("parser called with %q, want no call", parser.got)
				}
			} else if len(parser.got) != 1 || parser.got[0] != tt.wantParsed {
				t.Errorf("parser got %q, want exactly [%q]", parser.got, tt.wantParsed)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DELETE /pipeline/node/{id}
// ---------------------------------------------------------------------------

func TestDeleteNode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		id         string
		body       interface{}
		wantStatus int
		checkGraph func(t *testing.T, g graph.PipelineGraph)
	}{
		{
			name: "success delete source node and clean edges",
			id:   "s3_source",
			body: map[string]interface{}{
				// dir injected at runtime
			},
			wantStatus: http.StatusOK,
			checkGraph: func(t *testing.T, g graph.PipelineGraph) {
				if hasNode(g, "s3_source") {
					t.Error("s3_source still present after delete")
				}
				// The edge from s3_source → validate should be removed (input attr cleaned up).
				if hasEdge(g, "s3_source", "validate") {
					t.Error("edge s3_source→validate still present after node delete")
				}
			},
		},
		{
			name:       "missing dir",
			id:         "s3_source",
			body:       map[string]interface{}{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown node id",
			id:   "ghost_node",
			body: map[string]interface{}{
				// dir injected at runtime
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "malformed body",
			id:         "s3_source",
			body:       "bad",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newMux(t)
			dir := setupDir(t, twoNodeTF)

			if m, ok := tt.body.(map[string]interface{}); ok {
				if _, hasDir := m["dir"]; !hasDir && tt.name != "missing dir" {
					m["dir"] = dir
				}
			}

			rr := doRequest(t, mux, http.MethodDelete, "/pipeline/nodes/"+tt.id, tt.body)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK && tt.checkGraph != nil {
				g := decodeGraph(t, rr)
				tt.checkGraph(t, g)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// POST /pipeline/edge
// ---------------------------------------------------------------------------

func TestAddEdge(t *testing.T) {
	t.Parallel()
	// Start with two isolated (unconnected) nodes.
	const isolatedTF = `module "src" {
  source = "clavesa/source/aws"
  name   = "src"
  bucket = "data"
}

module "dst" {
  source = "clavesa/destination/aws"
  name   = "dst"
  bucket = "out"
}
`
	tests := []struct {
		name       string
		body       interface{}
		wantStatus int
		checkGraph func(t *testing.T, g graph.PipelineGraph)
	}{
		{
			name: "success connect src to dst",
			body: map[string]interface{}{
				// dir injected at runtime
				"from_node":   "src",
				"from_output": "default",
				"to_node":     "dst",
				"to_input":    "default",
			},
			wantStatus: http.StatusOK,
			checkGraph: func(t *testing.T, g graph.PipelineGraph) {
				if !hasEdge(g, "src", "dst") {
					t.Errorf("edge src→dst not found; edges: %v", g.Edges)
				}
			},
		},
		{
			name: "missing dir",
			body: map[string]interface{}{
				"from_node": "src",
				"to_node":   "dst",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "to_node not found",
			body: map[string]interface{}{
				// dir injected at runtime
				"from_node": "src",
				"to_node":   "nonexistent",
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "malformed body",
			body:       "bad",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newMux(t)
			dir := setupDir(t, isolatedTF)

			if m, ok := tt.body.(map[string]interface{}); ok {
				if _, hasDir := m["dir"]; !hasDir && tt.name != "missing dir" {
					m["dir"] = dir
				}
			}

			rr := doRequest(t, mux, http.MethodPost, "/pipeline/edges", tt.body)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK && tt.checkGraph != nil {
				g := decodeGraph(t, rr)
				tt.checkGraph(t, g)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// POST /pipeline/nodes/{id}/rename
// ---------------------------------------------------------------------------

func TestRenameNode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		nodeID     string
		body       interface{}
		wantStatus int
		checkGraph func(t *testing.T, g graph.PipelineGraph)
	}{
		{
			name:       "rename rewrites the downstream edge",
			nodeID:     "s3_source",
			body:       map[string]interface{}{"new_id": "raw_events"},
			wantStatus: http.StatusOK,
			checkGraph: func(t *testing.T, g graph.PipelineGraph) {
				if hasNode(g, "s3_source") {
					t.Errorf("old node s3_source still present; nodes: %v", g.Nodes)
				}
				if !hasNode(g, "raw_events") {
					t.Errorf("renamed node raw_events missing; nodes: %v", g.Nodes)
				}
				if !hasEdge(g, "raw_events", "validate") {
					t.Errorf("edge raw_events→validate missing after rename; edges: %v", g.Edges)
				}
			},
		},
		{
			name:       "rename to an existing name is rejected",
			nodeID:     "s3_source",
			body:       map[string]interface{}{"new_id": "validate"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid identifier is rejected",
			nodeID:     "s3_source",
			body:       map[string]interface{}{"new_id": "2bad"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unknown node is 404",
			nodeID:     "nope",
			body:       map[string]interface{}{"new_id": "ok_name"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing new_id is rejected",
			nodeID:     "s3_source",
			body:       map[string]interface{}{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newMux(t)
			dir := setupDir(t, twoNodeTF)
			if m, ok := tt.body.(map[string]interface{}); ok {
				m["dir"] = dir
			}
			rr := doRequest(t, mux, http.MethodPost, "/pipeline/nodes/"+tt.nodeID+"/rename", tt.body)
			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK && tt.checkGraph != nil {
				tt.checkGraph(t, decodeGraph(t, rr))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DELETE /pipeline/edges/{id}
// ---------------------------------------------------------------------------

func TestDeleteEdge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		edgeID     string
		body       interface{}
		wantStatus int
		checkGraph func(t *testing.T, g graph.PipelineGraph)
	}{
		{
			name:       "success remove edge",
			edgeID:     "s3_source->validate",
			body:       map[string]interface{}{}, // dir injected at runtime
			wantStatus: http.StatusOK,
			checkGraph: func(t *testing.T, g graph.PipelineGraph) {
				if hasEdge(g, "s3_source", "validate") {
					t.Error("edge s3_source→validate still present after delete")
				}
			},
		},
		{
			name:       "missing dir",
			edgeID:     "s3_source->validate",
			body:       map[string]interface{}{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "to_node not found",
			edgeID:     "s3_source->ghost",
			body:       map[string]interface{}{}, // dir injected at runtime
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "malformed edge id",
			edgeID:     "no-arrow-here",
			body:       map[string]interface{}{}, // dir injected at runtime
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "malformed body",
			edgeID:     "s3_source->validate",
			body:       "bad",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newMux(t)
			dir := setupDir(t, twoNodeTF)

			if m, ok := tt.body.(map[string]interface{}); ok {
				if _, hasDir := m["dir"]; !hasDir && tt.name != "missing dir" {
					m["dir"] = dir
				}
			}

			rr := doRequest(t, mux, http.MethodDelete, "/pipeline/edges/"+tt.edgeID, tt.body)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK && tt.checkGraph != nil {
				g := decodeGraph(t, rr)
				tt.checkGraph(t, g)
			}
		})
	}
}

// TestAddDeleteEdgeRoundTrip exercises the actual AddEdge→DeleteEdge path:
// connect two nodes via POST /pipeline/edges (which writes the canonical
// `inputs = { alias = … }` map for transforms), then disconnect via DELETE
// /pipeline/edges/{id}. Regression for the bug where DeleteEdge clobbered
// the singular `input` attr and missed the plural `inputs` map produced by
// AddEdge.
func TestAddDeleteEdgeRoundTrip(t *testing.T) {
	t.Parallel()
	const isolatedTF = `module "src" {
  source = "clavesa/source/aws"
  name   = "src"
  bucket = "data"
}

module "validate" {
  source = "clavesa/transform/aws"
  name   = "validate"
  sql    = "SELECT * FROM src"
}
`
	mux := newMux(t)
	dir := setupDir(t, isolatedTF)

	// Connect.
	rr := doRequest(t, mux, http.MethodPost, "/pipeline/edges", map[string]interface{}{
		"dir":         dir,
		"from_node":   "src",
		"from_output": "default",
		"to_node":     "validate",
		"to_input":    "src",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("AddEdge status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}
	if g := decodeGraph(t, rr); !hasEdge(g, "src", "validate") {
		t.Fatalf("edge src→validate not present after AddEdge; edges: %v", g.Edges)
	}

	// Disconnect using the canonical {from}->{to} edge id.
	rr = doRequest(t, mux, http.MethodDelete, "/pipeline/edges/src->validate", map[string]interface{}{
		"dir": dir,
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("DeleteEdge status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}
	if g := decodeGraph(t, rr); hasEdge(g, "src", "validate") {
		t.Fatalf("edge src→validate still present after DeleteEdge; edges: %v", g.Edges)
	}
}

// TestAddEdgeRejectsUnknownFromNode guards the corruption bug where an
// edge from a non-existent node (e.g. the UI's read-only `source:<name>`
// synthetic node) was written straight into `module.<id>.outputs[...]` —
// invalid HCL when the id has a colon, leaving the .tf unparseable.
func TestAddEdgeRejectsUnknownFromNode(t *testing.T) {
	t.Parallel()
	const isolatedTF = `module "validate" {
  source = "clavesa/transform/aws"
  name   = "validate"
  sql    = "SELECT 1"
}
`
	mux := newMux(t)
	dir := setupDir(t, isolatedTF)

	rr := doRequest(t, mux, http.MethodPost, "/pipeline/edges", map[string]interface{}{
		"dir":       dir,
		"from_node": "source:src_trips",
		"to_node":   "validate",
		"to_input":  "src_trips",
	})
	if rr.Code == http.StatusOK {
		t.Fatalf("AddEdge from an unknown node should be rejected, got 200")
	}
}

// ---------------------------------------------------------------------------
// GET /pipeline/validate
// ---------------------------------------------------------------------------

func TestValidatePipeline(t *testing.T) {
	t.Parallel()
	const cyclicTF = `module "a" {
  source = "clavesa/transform/aws"
  name   = "a"
  input  = module.b.outputs["default"]
}

module "b" {
  source = "clavesa/transform/aws"
  name   = "b"
  input  = module.a.outputs["default"]
}
`
	tests := []struct {
		name       string
		content    string
		missingDir bool
		wantStatus int
		wantValid  bool
	}{
		{
			name:       "valid pipeline",
			content:    twoNodeTF,
			wantStatus: http.StatusOK,
			wantValid:  true,
		},
		{
			name:       "cyclic pipeline reports invalid",
			content:    cyclicTF,
			wantStatus: http.StatusOK,
			wantValid:  false,
		},
		{
			name:       "missing dir",
			missingDir: true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newMux(t)

			var target string
			if tt.missingDir {
				target = "/pipeline/validate"
			} else {
				dir := setupDir(t, tt.content)
				target = "/pipeline/validate?dir=" + dir
			}

			rr := doRequest(t, mux, http.MethodGet, target, nil)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK {
				var resp struct {
					Valid  bool     `json:"valid"`
					Errors []string `json:"errors"`
				}
				if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if resp.Valid != tt.wantValid {
					t.Errorf("valid = %v, want %v; errors: %v", resp.Valid, tt.wantValid, resp.Errors)
				}
			}
		})
	}
}
