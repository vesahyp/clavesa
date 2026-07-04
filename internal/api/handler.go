// Package api implements the PIPELINE-API HTTP layer. It exposes nine
// endpoints that serve and mutate Pipeline Graph JSON by delegating reads to
// HCL-PARSER and writes to FILE-OPS-API.
package api

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/fileops"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/hclutil"
	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/lineagetype"
	"github.com/vesahyp/clavesa/internal/pathutil"
	"github.com/vesahyp/clavesa/internal/service"
)

// OrchestrationSyncer can regenerate orchestration.tf after pipeline mutations.
type OrchestrationSyncer interface {
	SyncOrchestration(dir, schedule string) error
}

// LineageEdge is an alias for the canonical leaf-package type so existing
// callers (cli/ui.go, tests) keep compiling without an import switch.
// New code should import internal/lineagetype directly.
type LineageEdge = lineagetype.Edge

// Lineager exposes the per-pipeline lineage derivation. Implemented by
// service.Service; the interface lives here so api stays leaf-y.
type Lineager interface {
	Lineage(dir string) (*lineagetype.Response, error)
}

// NodeAdder is the typed node-creation path used by `POST /pipeline/typed-nodes`.
// Implementations write a fully-formed module block (pipeline_name, bucket,
// catalog, schema, runner_image, current `?ref=`) — everything the CLI's
// `clavesa node add` gets — unlike the legacy raw-block POST
// /pipeline/nodes which takes hand-rolled attributes and was missing every
// one of those fields.
//
// The handler re-parses the directory after the call to return the updated
// graph, so the interface itself doesn't surface the parsed shape — keeps
// internal/service out of the api package's imports.
type NodeAdder interface {
	AddNode(dir, nodeType, name string) error
}

// ExternalTableAttacher is the service-layer entry the UI hits to wire
// a cross-pipeline or external Glue table into a transform's `inputs`
// map (ADR-016 slice 2). The CLI's `clavesa node connect
// --from-table <schema>.<table>` uses the same call.
type ExternalTableAttacher interface {
	AttachExternalTable(dir, ref, toNode, alias string) error
}

// InputDetacher removes a named alias from a transform's inputs regardless
// of which kind (transform→transform edge, registry source, or external
// `<schema>.<table>` reference). Backs the UI's X-button on each input row.
type InputDetacher interface {
	DetachInput(dir, toNode, alias string) error
}

// (SourceFetcher / SourceFromURLResult removed in ADR-017 slice 4 —
// the URL-to-inline-source flow is gone. Workspace source registry
// lives in internal/api/sources.go.)

// Handler holds the dependencies for all PIPELINE-API HTTP handlers.
type Handler struct {
	fo               *fileops.FileOps
	root             string // absolute workspace root; used to resolve relative dir params
	svc              *service.Service
	syncer           OrchestrationSyncer
	lineager         Lineager
	nodeAdder        NodeAdder
	externalAttacher ExternalTableAttacher
	inputDetacher    InputDetacher
}

// New returns a Handler that uses fo for file-level mutations.
// root is the absolute workspace root directory.
func New(fo *fileops.FileOps, root string) *Handler {
	return &Handler{fo: fo, root: root}
}

// WithService attaches the canonical service.Service so handlers that
// previously instantiated service.New(h.root) per request route through
// the wired Service instead — picking up its WithResolver /
// WithTranspiler state (A P1-1 / C P1-1, 2026-05-24 review).
func (h *Handler) WithService(s *service.Service) *Handler {
	h.svc = s
	return h
}

// service returns the wired Service, falling back to a fresh
// service.New(h.root) for tests that construct the Handler without
// WithService. Production code paths always set the canonical Service.
func (h *Handler) service() *service.Service {
	if h.svc != nil {
		return h.svc
	}
	return service.New(h.root)
}

// WithSyncer attaches an OrchestrationSyncer that is called best-effort after
// every pipeline mutation (AddNode, UpdateNode, DeleteNode, AddEdge, DeleteEdge).
func (h *Handler) WithSyncer(s OrchestrationSyncer) *Handler {
	h.syncer = s
	return h
}

// WithLineage wires a Lineager so GET /pipeline/lineage can serve the
// per-pipeline DAG-as-edges shape the UI's TableDetail page consumes.
func (h *Handler) WithLineage(l Lineager) *Handler {
	h.lineager = l
	return h
}

// WithNodeAdder enables `POST /pipeline/typed-nodes`. Without this, the UI
// must fall back to the legacy raw-attribute `POST /pipeline/nodes` —
// which is fine for tests but produces broken transforms for real users
// (TODO line 84: missing pipeline_name/bucket/catalog/schema, pinned to
// `?ref=v0.1.0`).
func (h *Handler) WithNodeAdder(a NodeAdder) *Handler {
	h.nodeAdder = a
	return h
}

// WithExternalTableAttacher enables `POST /pipeline/external-table/attach`,
// the HTTP twin of the CLI `node connect --from-table` command. Without
// this, the UI's cross-pipeline / external-table input picker has no
// backend.
func (h *Handler) WithExternalTableAttacher(a ExternalTableAttacher) *Handler {
	h.externalAttacher = a
	return h
}

// WithInputDetacher enables `POST /pipeline/inputs/detach`, backing the
// editor's X-button on each input row in the transform inspector.
func (h *Handler) WithInputDetacher(d InputDetacher) *Handler {
	h.inputDetacher = d
	return h
}

// syncOrchestration calls the syncer if one is set. Errors are logged
// but not propagated — a sync failure must not block the mutation
// response (the .tf change already landed; orchestration.tf rebuild can
// be re-run by hand or on the next mutation). Pre-2026-05-24 the error
// was silently swallowed with no signal.
func (h *Handler) syncOrchestration(dir string) {
	if h.syncer == nil {
		return
	}
	if err := h.syncer.SyncOrchestration(dir, ""); err != nil {
		fmt.Fprintf(os.Stderr, "warn: orchestration sync failed for %s: %v\n", dir, err)
	}
}

// RegisterRoutes registers all PIPELINE-API routes on mux.
// Requires Go 1.22+ net/http for method-qualified patterns and {id} params.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /pipeline", h.GetPipeline)
	mux.HandleFunc("POST /pipeline/nodes", h.AddNode)
	mux.HandleFunc("POST /pipeline/typed-nodes", h.TypedAddNode)
	mux.HandleFunc("PUT /pipeline/nodes/{id}", h.UpdateNode)
	mux.HandleFunc("POST /pipeline/nodes/{id}/rename", h.RenameNode)
	mux.HandleFunc("DELETE /pipeline/nodes/{id}", h.DeleteNode)
	mux.HandleFunc("POST /pipeline/edges", h.AddEdge)
	mux.HandleFunc("DELETE /pipeline/edges/{id}", h.DeleteEdge)
	mux.HandleFunc("POST /pipeline/external-table/attach", h.AttachExternalTable)
	mux.HandleFunc("POST /pipeline/inputs/detach", h.DetachInput)
	mux.HandleFunc("GET /pipeline/validate", h.ValidatePipeline)
	mux.HandleFunc("GET /pipeline/script", h.GetScript)
	mux.HandleFunc("PUT /pipeline/script", h.PutScript)
	mux.HandleFunc("GET /pipeline/lineage", h.GetLineage)
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

type addNodeRequest struct {
	Dir        string                 `json:"dir"`
	File       string                 `json:"file"`
	BlockName  string                 `json:"block_name"`
	Attributes map[string]interface{} `json:"attributes"`
}

type typedAddNodeRequest struct {
	Dir  string `json:"dir"`
	Type string `json:"type"` // "transform" | "destination" (sources go via /sources)
	Name string `json:"name"` // optional; auto-generated when empty
}

type updateNodeRequest struct {
	Dir        string                 `json:"dir"`
	File       string                 `json:"file"` // optional; auto-detected when empty
	Attributes map[string]interface{} `json:"attributes"`
}

type deleteNodeRequest struct {
	Dir  string `json:"dir"`
	File string `json:"file"` // optional; auto-detected when empty
}

type renameNodeRequest struct {
	Dir   string `json:"dir"`
	NewID string `json:"new_id"`
}

type addEdgeRequest struct {
	Dir        string `json:"dir"`
	FromNode   string `json:"from_node"`
	FromOutput string `json:"from_output"`
	ToNode     string `json:"to_node"`
	ToInput    string `json:"to_input"`
}

type validateResponse struct {
	Valid  bool     `json:"valid"`
	Errors []string `json:"errors"`
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// GET /pipeline?dir=<path>
// Returns the PipelineGraph JSON for the given Terraform directory.
func (h *Handler) GetPipeline(w http.ResponseWriter, r *http.Request) {
	dirParam, ok := httputil.RequireQuery(w, r, "dir")
	if !ok {
		return
	}
	dir := h.resolve(dirParam)
	g, err := hclparser.Parse(dir)
	if err != nil {
		httputil.WriteError(w, parseStatus(err), err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, g)
}

// POST /pipeline/typed-nodes
// Adds a new node by type via the NodeAdder seam (i.e. service.AddNode), which
// threads pipeline_name, bucket, catalog/schema, runner_image and pins
// `?ref=` to the current ModuleVersion. Returns the updated PipelineGraph.
//
// Sources are intentionally not authored here — ADR-017 routes source
// authoring through `/sources` (workspace registry). A request with
// type="source" returns 400 with the same redirect message
// service.AddNode emits.
func (h *Handler) TypedAddNode(w http.ResponseWriter, r *http.Request) {
	if h.nodeAdder == nil {
		httputil.WriteError(w, http.StatusNotImplemented, "typed node-add not wired (server started without NodeAdder)")
		return
	}
	req, ok := httputil.DecodeJSON[typedAddNodeRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir, "type": req.Type}) {
		return
	}
	req.Dir = h.resolve(req.Dir)
	if err := h.nodeAdder.AddNode(req.Dir, req.Type, req.Name); err != nil {
		// service.AddNode emits user-facing redirect messages for misuse
		// (e.g. type=source) — surface verbatim rather than wrapping.
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	// AddNode already calls SyncOrchestration internally; just re-parse and
	// return the new graph.
	h.parseAndRespond(w, req.Dir)
}

// POST /pipeline/node
// Adds a new module block to the specified file, then returns the updated graph.
func (h *Handler) AddNode(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[addNodeRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir, "file": req.File, "block_name": req.BlockName}) {
		return
	}
	req.Dir = h.resolve(req.Dir)
	// Resolve req.File against the workspace root the same way req.Dir is
	// resolved. Without this, any UI session whose cwd differs from the
	// workspace gets a 404 because fileops opens File from cwd.
	// Containment check (A P2-6, C9): the file must end up inside the
	// pipeline dir — a literal `/etc/hosts` request resolves there
	// otherwise and AddBlock would happily write into it.
	filePath := req.File
	if !filepath.IsAbs(filePath) {
		filePath = filepath.Join(req.Dir, filePath)
	}
	if !pathutil.IsWithin(req.Dir, filePath) {
		httputil.WriteError(w, http.StatusBadRequest, "file must be within the pipeline directory")
		return
	}
	attrs := normalizeAttributes(req.Attributes)
	if _, err := h.fo.AddBlock(filePath, "module", req.BlockName, attrs); err != nil {
		httputil.WriteError(w, fileOpsStatus(err), err.Error())
		return
	}
	h.syncOrchestration(req.Dir)
	h.parseAndRespond(w, req.Dir)
}

// PUT /pipeline/node/{id}
// Merges attributes into the named module block, then returns the updated graph.
func (h *Handler) UpdateNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	req, ok := httputil.DecodeJSON[updateNodeRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}
	req.Dir = h.resolve(req.Dir)
	file := req.File
	if file == "" {
		var err error
		file, err = hclutil.FindNodeFile(h.fo, req.Dir, id)
		if err != nil {
			httputil.WriteError(w, http.StatusNotFound, fmt.Sprintf("node %q not found", id))
			return
		}
	}
	// Parse-check the SQL before the write (Slice 3). The UI's editor
	// sends either inline SQL or the `file("<node>.sql")` idiom as a
	// plain string — for a file ref, the editor has already PUT the
	// script, so validate the file's current content; an unreadable ref
	// skips the check (dangling refs surface at preview / run time, as
	// before the check existed). Reject with 400 so the editor can
	// render the parser message inline above the Save button. Transport
	// failures (warm worker dead, docker gone) are logged but don't
	// block the write — the run path will surface a real parse error
	// anyway.
	checkSQL, _ := req.Attributes["sql"].(string)
	if path, isRef := service.ParseFileRef(checkSQL); isRef {
		if data, err := os.ReadFile(filepath.Join(req.Dir, path)); err == nil {
			checkSQL = string(data)
		} else {
			checkSQL = ""
		}
	}
	if checkSQL != "" {
		if err := h.service().ValidateSQL(r.Context(), checkSQL); err != nil {
			var pe *service.ParseError
			if errors.As(err, &pe) {
				httputil.WriteError(w, http.StatusBadRequest, pe.Message)
				return
			}
			fmt.Fprintf(os.Stderr, "warn: SQL parse-check skipped on PUT /pipeline/node: %v\n", err)
		}
	}
	attrs := normalizeAttributes(req.Attributes)
	if _, err := h.fo.UpdateBlock(file, "module."+id, attrs); err != nil {
		httputil.WriteError(w, fileOpsStatus(err), err.Error())
		return
	}
	h.syncOrchestration(req.Dir)
	h.parseAndRespond(w, req.Dir)
}

// POST /pipeline/nodes/{id}/rename
// Renames a node — module block, downstream edge references, and script
// files — via service.RenameNode, the same code path the CLI's `node
// rename` uses (ADR-015). Returns the updated graph.
func (h *Handler) RenameNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	req, ok := httputil.DecodeJSON[renameNodeRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir, "new_id": req.NewID}) {
		return
	}
	dir := h.resolve(req.Dir)
	if _, err := h.service().RenameNode(dir, id, req.NewID); err != nil {
		msg := err.Error()
		switch {
		case strings.Contains(msg, "not found"):
			httputil.WriteError(w, http.StatusNotFound, msg)
		case strings.Contains(msg, "already exists"),
			strings.Contains(msg, "not a valid"),
			strings.Contains(msg, "same as"):
			httputil.WriteError(w, http.StatusBadRequest, msg)
		default:
			httputil.WriteError(w, fileOpsStatus(err), msg)
		}
		return
	}
	// service.RenameNode re-syncs orchestration internally.
	h.parseAndRespond(w, dir)
}

// DELETE /pipeline/node/{id}
// Removes the named module block and all edges referencing it, then returns
// the updated graph.
func (h *Handler) DeleteNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	req, ok := httputil.DecodeJSON[deleteNodeRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir}) {
		return
	}
	req.Dir = h.resolve(req.Dir)
	file := req.File
	if file == "" {
		var err error
		file, err = hclutil.FindNodeFile(h.fo, req.Dir, id)
		if err != nil {
			httputil.WriteError(w, http.StatusNotFound, fmt.Sprintf("node %q not found", id))
			return
		}
	}
	if _, err := h.fo.RemoveBlock(file, "module."+id); err != nil {
		httputil.WriteError(w, fileOpsStatus(err), err.Error())
		return
	}
	if err := hclutil.RemoveEdgesReferencing(h.fo, req.Dir, id); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "failed to clean up edges: "+err.Error())
		return
	}
	h.syncOrchestration(req.Dir)
	h.parseAndRespond(w, req.Dir)
}

// POST /pipeline/edge
// Writes a module reference into the to_node's input attribute, creating an
// edge in the Terraform graph. Returns the updated graph.
//
// Delegates to service.AddEdge — the same code path the CLI's `node
// connect` uses (ADR-015). The handler previously hand-rolled the HCL
// mutation, and its inline version *replaced* the whole `inputs` map
// instead of merging, silently dropping every other edge into the same
// transform; service.AddEdge reconstructs and merges.
func (h *Handler) AddEdge(w http.ResponseWriter, r *http.Request) {
	req, ok := httputil.DecodeJSON[addEdgeRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir, "from_node": req.FromNode, "to_node": req.ToNode}) {
		return
	}
	dir := h.resolve(req.Dir)
	if req.FromOutput == "" {
		req.FromOutput = "default"
	}
	// Default the SQL alias to the from-node id — matches the CLI's
	// `node connect` default and reads naturally in SQL. service.AddEdge's
	// own empty-alias fallback is "default", which is meaningless as a
	// table alias, so resolve it here before delegating.
	toInput := req.ToInput
	if toInput == "" {
		toInput = req.FromNode
	}

	if _, err := h.service().AddEdge(dir, req.FromNode, req.FromOutput, req.ToNode, toInput); err != nil {
		if strings.Contains(err.Error(), "not found") {
			httputil.WriteError(w, http.StatusNotFound, err.Error())
			return
		}
		httputil.WriteError(w, fileOpsStatus(err), err.Error())
		return
	}
	// service.AddEdge re-syncs orchestration internally; no h.syncOrchestration here.
	h.parseAndRespond(w, dir)
}

// POST /pipeline/external-table/attach
// Wires a cross-pipeline / external-Glue-table reference into a
// transform's `inputs` map (ADR-016 slice 2). Body:
//
//	{
//	  "dir":   "<pipeline_dir>",
//	  "ref":   "<schema>.<table>",
//	  "to":    "<transform_node_id>",
//	  "alias": "<sql_alias>"   // optional; defaults to table-name portion
//	}
//
// Returns the updated graph on success (same shape as POST /pipeline/edges).
// Validation: `ref` must match `<schema>.<table>` shape; `to` must be a
// transform node; service layer raises if either fails.
func (h *Handler) AttachExternalTable(w http.ResponseWriter, r *http.Request) {
	if h.externalAttacher == nil {
		httputil.WriteError(w, http.StatusNotImplemented, "external-table attach not configured (server missing service binding)")
		return
	}
	type attachExternalTableRequest struct {
		Dir   string `json:"dir"`
		Ref   string `json:"ref"`
		To    string `json:"to"`
		Alias string `json:"alias"`
	}
	req, ok := httputil.DecodeJSON[attachExternalTableRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir, "ref": req.Ref, "to": req.To}) {
		return
	}
	req.Dir = h.resolve(req.Dir)
	if err := h.externalAttacher.AttachExternalTable(req.Dir, req.Ref, req.To, req.Alias); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.parseAndRespond(w, req.Dir)
}

// POST /pipeline/inputs/detach
// Removes a named alias from a transform's inputs. Handles all three
// attachment kinds (transform→transform edge, registry source, external
// `<schema>.<table>` reference) so the UI X-button doesn't need to
// discriminate. Body:
//
//	{
//	  "dir":   "<pipeline_dir>",
//	  "to":    "<transform_node_id>",
//	  "alias": "<input_key>"
//	}
//
// Returns the updated graph on success.
func (h *Handler) DetachInput(w http.ResponseWriter, r *http.Request) {
	if h.inputDetacher == nil {
		httputil.WriteError(w, http.StatusNotImplemented, "input detach not configured (server missing service binding)")
		return
	}
	type detachInputRequest struct {
		Dir   string `json:"dir"`
		To    string `json:"to"`
		Alias string `json:"alias"`
	}
	req, ok := httputil.DecodeJSON[detachInputRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir, "to": req.To, "alias": req.Alias}) {
		return
	}
	req.Dir = h.resolve(req.Dir)
	if err := h.inputDetacher.DetachInput(req.Dir, req.To, req.Alias); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.parseAndRespond(w, req.Dir)
}

// DELETE /pipeline/edges/{id}
// Removes the edge identified by the path parameter.
// Edge id format: {from_node}->{to_node}
// Expects body: {"dir": "<terraform_directory>"}
//
// Delegates to service.DeleteEdge — the same code path the CLI's `node
// disconnect` uses (ADR-015). The edge id is pre-parsed here only to keep
// the 400 message and the 404's to_node reference identical to the prior
// hand-rolled contract.
func (h *Handler) DeleteEdge(w http.ResponseWriter, r *http.Request) {
	edgeID := r.PathValue("id")
	_, toNode, ok := hclutil.ParseEdgeID(edgeID)
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, "invalid edge id: expected {from_node}->{to_node}")
		return
	}

	type deleteEdgeRequest struct {
		Dir string `json:"dir"`
	}
	body, ok := httputil.DecodeJSON[deleteEdgeRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": body.Dir}) {
		return
	}
	dir := h.resolve(body.Dir)

	if _, err := h.service().DeleteEdge(dir, edgeID); err != nil {
		// FindNodeFile returns a wrapped not-found; surface as 404 to match
		// the prior contract.
		if strings.Contains(err.Error(), "not found") {
			httputil.WriteError(w, http.StatusNotFound, fmt.Sprintf("to_node %q not found", toNode))
			return
		}
		httputil.WriteError(w, fileOpsStatus(err), err.Error())
		return
	}
	// service.DeleteEdge re-syncs orchestration internally; no h.syncOrchestration here.
	h.parseAndRespond(w, dir)
}

// GET /pipeline/validate?dir=<path>
// Runs topology validation on the pipeline and returns { valid, errors }.
func (h *Handler) ValidatePipeline(w http.ResponseWriter, r *http.Request) {
	dirParam, ok := httputil.RequireQuery(w, r, "dir")
	if !ok {
		return
	}
	dir := h.resolve(dirParam)
	g, err := hclparser.Parse(dir)
	if err != nil {
		httputil.WriteError(w, parseStatus(err), err.Error())
		return
	}
	errs := make([]string, 0, len(g.Validation.Errors))
	for _, e := range g.Validation.Errors {
		errs = append(errs, e.Message)
	}
	httputil.WriteJSON(w, http.StatusOK, validateResponse{
		Valid:  len(errs) == 0,
		Errors: errs,
	})
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// resolve resolves dir against the workspace root if it is relative.
func (h *Handler) resolve(dir string) string {
	return pathutil.ResolveDir(h.root, dir)
}

// resolveDir folds resolution + containment: returns (abs, true) when the
// resolved path stays within h.root. On a `../escape` or absolute-outside
// path it writes 400 and returns ("", false) so the caller can early-out.
// Use for endpoints that take untrusted dir input; the bare h.resolve
// pre-dates the containment rule and remains for tests + internal callers.
func (h *Handler) resolveDir(w http.ResponseWriter, dir string) (string, bool) {
	abs := pathutil.ResolveDir(h.root, dir)
	if h.root != "" && !pathutil.IsWithin(h.root, abs) {
		httputil.WriteError(w, http.StatusBadRequest, "dir must be within the workspace root")
		return "", false
	}
	return abs, true
}

// parseAndRespond parses the directory and writes the PipelineGraph as JSON.
func (h *Handler) parseAndRespond(w http.ResponseWriter, dir string) {
	g, err := hclparser.Parse(dir)
	if err != nil {
		httputil.WriteError(w, parseStatus(err), err.Error())
		return
	}
	httputil.WriteJSON(w, http.StatusOK, g)
}

// syntheticParserAttrs are config keys the HCL parser materialises on
// a node's Config to surface attachment intent — they don't correspond
// to real HCL attributes the user wrote, and writing them back to the
// .tf produces malformed Terraform. The UI's `editedConfig` echoes the
// full parser shape on save; strip them here so round-trip Save just
// works.
var syntheticParserAttrs = map[string]bool{
	"source_inputs":   true,
	"external_inputs": true,
}

// normalizeAttributes converts the JSON-decoded attribute map into the typed
// map expected by FILE-OPS-API. In particular it:
//
//	{"__type": "reference", "expression": "..."} → fileops.ModuleReference
//
// recursively, so that nested objects in the attributes map are also handled.
// Synthetic parser keys (see syntheticParserAttrs) are filtered out so a UI
// round-trip — read graph, edit one field, save — can't accidentally write
// parser-internal state back to disk.
func normalizeAttributes(raw map[string]interface{}) map[string]fileops.AttributeValue {
	if raw == nil {
		return nil
	}
	result := make(map[string]fileops.AttributeValue, len(raw))
	for k, v := range raw {
		if syntheticParserAttrs[k] {
			continue
		}
		result[k] = normalizeValue(v)
	}
	return result
}

func normalizeValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return v
	}
	// Detect ModuleReference sentinel.
	if t, ok := m["__type"].(string); ok && t == "reference" {
		if expr, ok := m["expression"].(string); ok {
			return fileops.ModuleReference{Type: "reference", Expression: expr}
		}
	}
	// Recurse into plain objects.
	result := make(map[string]interface{}, len(m))
	for k, mv := range m {
		result[k] = normalizeValue(mv)
	}
	return result
}

// parseStatus maps an hclparser.Parse error to an HTTP status. Missing /
// non-existent pipeline directories surface as 404 so the UI can distinguish
// "bad ?dir=" from a genuine backend failure; everything else is 500.
func parseStatus(err error) int {
	if errors.Is(err, os.ErrNotExist) {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

// fileOpsStatus maps a FILE-OPS error code to an HTTP status code.
func fileOpsStatus(err error) int {
	foe, ok := err.(*fileops.FileOpsError)
	if !ok {
		return http.StatusInternalServerError
	}
	switch foe.Code {
	case fileops.ErrFileNotFound, fileops.ErrDirectoryNotFound, fileops.ErrBlockNotFound:
		return http.StatusNotFound
	case fileops.ErrBlockAlreadyExists:
		return http.StatusConflict
	case fileops.ErrAmbiguousAddress:
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

// GetScript serves GET /pipeline/script?dir=<dir>&path=<relative-path>
// Returns the raw text content of a script file inside the pipeline directory.
func (h *Handler) GetScript(w http.ResponseWriter, r *http.Request) {
	dirParam, ok := httputil.RequireQuery(w, r, "dir")
	if !ok {
		return
	}
	rel, ok := httputil.RequireQuery(w, r, "path")
	if !ok {
		return
	}
	dir := h.resolve(dirParam)
	abs := filepath.Join(dir, rel)
	// Prevent path traversal outside the pipeline directory.
	if !pathutil.IsWithin(dir, abs) {
		httputil.WriteError(w, http.StatusBadRequest, "path must be within the pipeline directory")
		return
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// PutScript serves PUT /pipeline/script — writes text content to a script file.
// Body: {"dir":"...","path":"...","content":"..."}
func (h *Handler) PutScript(w http.ResponseWriter, r *http.Request) {
	type putScriptRequest struct {
		Dir     string `json:"dir"`
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	req, ok := httputil.DecodeJSON[putScriptRequest](w, r)
	if !ok {
		return
	}
	if !httputil.RequireFields(w, map[string]string{"dir": req.Dir, "path": req.Path}) {
		return
	}
	dir := h.resolve(req.Dir)
	abs := filepath.Join(dir, req.Path)
	if !pathutil.IsWithin(dir, abs) {
		httputil.WriteError(w, http.StatusBadRequest, "path must be within the pipeline directory")
		return
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := os.WriteFile(abs, []byte(req.Content), 0o644); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
