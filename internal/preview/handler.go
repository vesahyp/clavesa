package preview

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/httputil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/workspace"
)

const (
	previewDefaultLimit = 50
	previewMaxLimit     = 500
)

// localImageForDir returns the workspace-scoped local Docker image name for the
// given pipeline directory. It loads clavesa.json from the parent directory.
// Falls back to "clavesa//transform-runner" if no manifest is found.
func localImageForDir(dir string) string {
	m, _ := workspace.Load(filepath.Dir(dir))
	if m != nil {
		return runner.LocalImageName(m.Name)
	}
	return runner.LocalImageName("")
}

// NewHandler returns an http.Handler that serves:
//
//	GET /preview/source      — browse items from an S3 source node
//	GET /preview/transform   — execute a transform node's SQL via DuckDB
//	GET /preview/destination — preview rows written to a destination (traces upstream)
//
// resolveDir maps a query-string `dir` (typically a pipeline name like
// "demo") to an absolute filesystem path. The UI sends workspace-relative
// dirs; the CLI sends absolute. Without this, hclparser.Parse and the
// source-registry lookup both fail on the UI path.
func NewHandler(s3Client dataquery.S3Client, hclParserFunc func(dir string) (*graph.PipelineGraph, error), resolveDir func(string) string) http.Handler {
	if resolveDir == nil {
		resolveDir = func(dir string) string { return dir }
	}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /preview/source", func(w http.ResponseWriter, r *http.Request) {
		handleSourcePreview(w, r, s3Client, hclParserFunc, resolveDir)
	})

	mux.HandleFunc("GET /preview/transform", func(w http.ResponseWriter, r *http.Request) {
		handleTransformPreview(w, r, s3Client, hclParserFunc, resolveDir)
	})

	mux.HandleFunc("GET /preview/destination", func(w http.ResponseWriter, r *http.Request) {
		handleDestinationPreview(w, r, s3Client, hclParserFunc, resolveDir)
	})

	return mux
}

// handleSourcePreview serves GET /preview/source?dir=<dir>&node_id=<id>&offset=<n>&limit=<n>
func handleSourcePreview(
	w http.ResponseWriter,
	r *http.Request,
	s3c dataquery.S3Client,
	parseGraph func(string) (*graph.PipelineGraph, error),
	resolveDir func(string) string,
) {
	q := r.URL.Query()
	dir := resolveDir(q.Get("dir"))
	nodeID := q.Get("node_id")
	if dir == "" || nodeID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir and node_id are required")
		return
	}

	offset, limit, ok := parseOffsetLimit(q.Get("offset"), q.Get("limit"))
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, "offset and limit must be non-negative integers")
		return
	}

	g, err := parseGraph(dir)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "parse pipeline: "+err.Error())
		return
	}

	node, err := FindNode(g, nodeID)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	bucket, prefix, format, jsonPath, ok := extractS3Config(node)
	if !ok {
		httputil.WriteError(w, http.StatusBadRequest, "node does not have S3 source configuration (bucket, prefix, format)")
		return
	}

	qr, err := fetchSourceResult(r.Context(), s3c, bucket, prefix, format, jsonPath, previewMaxLimit)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := Convert(qr)
	result.Items = paginate(result.Items, offset, limit)
	// No Served stamp: this is a raw S3 read in Go — no Spark executed,
	// and a badge claiming an engine that didn't run would lie (ADR-024).
	httputil.WriteJSON(w, http.StatusOK, result)
}

const (
	transformDefaultRows = 15
	transformMaxRows     = 50
)

// handleTransformPreview serves GET /preview/transform?dir=<dir>&node_id=<id>&rows=<n>
func handleTransformPreview(
	w http.ResponseWriter,
	r *http.Request,
	s3c dataquery.S3Client,
	parseGraph func(string) (*graph.PipelineGraph, error),
	resolveDir func(string) string,
) {
	q := r.URL.Query()
	dir := resolveDir(q.Get("dir"))
	nodeID := q.Get("node_id")
	if dir == "" || nodeID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir and node_id are required")
		return
	}

	rowCount := transformDefaultRows
	if rowsStr := q.Get("rows"); rowsStr != "" {
		n, err := strconv.Atoi(rowsStr)
		if err != nil || n <= 0 {
			httputil.WriteError(w, http.StatusBadRequest, "rows must be a positive integer")
			return
		}
		if n > transformMaxRows {
			n = transformMaxRows
		}
		rowCount = n
	}

	g, err := parseGraph(dir)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "parse pipeline: "+err.Error())
		return
	}

	node, err := FindNode(g, nodeID)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	lang, _ := node.Config["language"].(string)
	if lang == "" {
		lang = "sql"
	}

	sqlStr, _ := extractSQL(node)
	if sqlOverride := strings.TrimSpace(q.Get("sql")); sqlOverride != "" {
		sqlStr = sqlOverride
	}
	// A `sql = file("…")` attribute resolves to the file's contents, the
	// same as `python = file("…")` below. Skip when the editor passed an
	// inline `?sql=` override (that's already literal SQL).
	if path, ok := resolveFileRef(sqlStr); ok {
		data, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "read sql file: "+err.Error())
			return
		}
		sqlStr = string(data)
	}

	pythonScript, _ := node.Config["python"].(string)
	if path, ok := resolveFileRef(pythonScript); ok {
		data, err := os.ReadFile(filepath.Join(dir, path))
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "read python script: "+err.Error())
			return
		}
		pythonScript = string(data)
	}

	if lang == "sql" && sqlStr == "" {
		httputil.WriteError(w, http.StatusBadRequest, "node does not have SQL configuration")
		return
	}
	if lang == "python" && pythonScript == "" {
		httputil.WriteError(w, http.StatusBadRequest, "node does not have python configuration")
		return
	}

	allRows, err := ResolveInputData(r.Context(), s3c, g, dir, nodeID, rowCount)
	if err != nil {
		httputil.WriteError(w, previewErrorStatus(err), err.Error())
		return
	}

	var primaryAlias string
	var primaryRows []map[string]interface{}
	for alias, rows := range allRows {
		if primaryAlias == "" {
			primaryAlias = alias
			primaryRows = rows
		}
	}
	if len(primaryRows) > rowCount {
		primaryRows = primaryRows[:rowCount]
	}
	for alias, rows := range allRows {
		if rowCount > 0 && len(rows) > rowCount {
			allRows[alias] = rows[:rowCount]
		}
	}

	image, _ := node.Config["runner_image"].(string)
	runSQL, runPy := "", ""
	if lang == "python" {
		runPy = pythonScript
	} else {
		runSQL = sqlStr
	}

	outputsByKey, err := RunPreview(r.Context(), localImageForDir(dir), image, allRows, runSQL, runPy)
	if err != nil {
		httputil.WriteError(w, previewErrorStatus(err), err.Error())
		return
	}

	var outputRows []map[string]interface{}
	for _, rows := range outputsByKey {
		outputRows = append(outputRows, rows...)
	}

	// Pair input rows with output rows by index. Spark runs the transform over
	// the whole batch at once, so output[i] aligns with input[i] when the
	// transform is row-preserving (filter / map). For non-row-preserving
	// transforms (aggregate / join) the alignment is by position only.
	pairs := make([]TransformPair, 0, len(primaryRows))
	for i, row := range primaryRows {
		var out []map[string]interface{}
		if i < len(outputRows) {
			out = []map[string]interface{}{outputRows[i]}
		}
		pairs = append(pairs, TransformPair{Input: row, Output: out})
	}

	displaySQL := sqlStr
	if lang == "python" {
		displaySQL = "(Python transform)"
	}
	httputil.WriteJSON(w, http.StatusOK, &TransformPreviewResult{
		Pairs:  pairs,
		SQL:    displaySQL,
		Served: servedForDir(dir),
	})
}

// handleDestinationPreview serves GET /preview/destination?dir=<dir>&node_id=<id>&rows=<n>
//
// Traces the edge back from the destination to its upstream node, then returns
// what would be written: output rows from a transform branch, or raw items from
// a source. Result is a PreviewResult (same shape as source preview).
func handleDestinationPreview(
	w http.ResponseWriter,
	r *http.Request,
	s3c dataquery.S3Client,
	parseGraph func(string) (*graph.PipelineGraph, error),
	resolveDir func(string) string,
) {
	q := r.URL.Query()
	dir := resolveDir(q.Get("dir"))
	nodeID := q.Get("node_id")
	if dir == "" || nodeID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "dir and node_id are required")
		return
	}

	rowCount := transformDefaultRows
	if rowsStr := q.Get("rows"); rowsStr != "" {
		n, err := strconv.Atoi(rowsStr)
		if err != nil || n <= 0 {
			httputil.WriteError(w, http.StatusBadRequest, "rows must be a positive integer")
			return
		}
		if n > transformMaxRows {
			n = transformMaxRows
		}
		rowCount = n
	}

	g, err := parseGraph(dir)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "parse pipeline: "+err.Error())
		return
	}

	// Find the upstream node that feeds this destination.
	var upstreamID string
	for _, e := range g.Edges {
		if e.ToNode == nodeID {
			upstreamID = e.FromNode
			break
		}
	}
	if upstreamID == "" {
		httputil.WriteError(w, http.StatusBadRequest, "no upstream node connected to this destination")
		return
	}

	upstream, err := FindNode(g, upstreamID)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, err.Error())
		return
	}

	// Source upstream: return raw source data directly.
	if upstream.Type == "source" || upstream.Type == "s3_source" {
		bucket, prefix, format, jsonPath, ok := extractS3Config(upstream)
		if !ok {
			httputil.WriteError(w, http.StatusBadRequest, "upstream source node missing S3 configuration")
			return
		}
		qr, err := fetchSourceResult(r.Context(), s3c, bucket, prefix, format, jsonPath, previewMaxLimit)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, err.Error())
			return
		}
		result := Convert(qr)
		result.Items = paginate(result.Items, 0, rowCount)
		// No Served stamp — raw S3 read, no Spark executed (ADR-024).
		httputil.WriteJSON(w, http.StatusOK, result)
		return
	}

	// Transform upstream: execute the full chain and return output rows.
	items, err := executeTransformChain(r.Context(), s3c, g, dir, upstream, rowCount)
	if err != nil {
		httputil.WriteError(w, previewErrorStatus(err), err.Error())
		return
	}
	if len(items) > rowCount {
		items = items[:rowCount]
	}
	httputil.WriteJSON(w, http.StatusOK, &PreviewResult{Items: items, Total: len(items), Served: servedForDir(dir)})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// servedForDir stamps the engine metadata for a preview response
// (ADR-024). Previews execute through runner Spark; the warehouse label
// follows the workspace warehouse loaded at execution time — the same
// resolution ResolveInputData uses for its catalog reads — so the badge
// reflects what this request actually ran against, not a separately
// fetched workspace state that could flip between requests. `dir` is the
// resolved absolute pipeline dir; its parent is the workspace root.
func servedForDir(dir string) *observability.Served {
	wh := workspace.LoadWarehouse(filepath.Dir(dir))
	return &observability.Served{Engine: "spark", Warehouse: string(wh)}
}

// previewErrorStatus maps an input-resolution / execution error to an HTTP
// status. A cloud warehouse on an undeployed workspace (ADR-024) is a
// user-actionable 409 — the request was fine, the workspace state can't
// satisfy it; everything else stays a 500.
func previewErrorStatus(err error) int {
	if errors.Is(err, workspace.ErrWarehouseUndeployed) {
		return http.StatusConflict
	}
	return http.StatusInternalServerError
}

// extractS3Config pulls bucket, prefix, format, json_path from a node's Config map.
func extractS3Config(node *graph.Node) (bucket, prefix, format, jsonPath string, ok bool) {
	b, hasBucket := node.Config["bucket"].(string)
	if !hasBucket {
		return "", "", "", "", false
	}
	f, hasFormat := node.Config["format"].(string)
	if !hasFormat {
		f = "csv"
	}
	p, _ := node.Config["prefix"].(string)
	jp, _ := node.Config["json_path"].(string)
	return b, p, f, jp, true
}

// extractSQL returns the SQL to execute for a preview. It comes from
// node.PreviewSQL (set by the parser from config["sql"]).
func extractSQL(node *graph.Node) (string, bool) {
	if node.PreviewSQL != "" {
		return node.PreviewSQL, true
	}
	s, ok := node.Config["sql"].(string)
	return s, ok && s != ""
}

// resolveFileRef parses a file("path") HCL expression and returns the inner path.
func resolveFileRef(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "file(") || !strings.HasSuffix(expr, ")") {
		return "", false
	}
	inner := strings.TrimSpace(expr[len("file(") : len(expr)-1])
	inner = strings.Trim(inner, `"`)
	return inner, true
}

// paginate returns items[offset : offset+limit], clamped to slice bounds.
func paginate(items []map[string]interface{}, offset, limit int) []map[string]interface{} {
	if offset >= len(items) {
		return []map[string]interface{}{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

// parseOffsetLimit parses optional offset and limit query parameters.
func parseOffsetLimit(offsetStr, limitStr string) (offset, limit int, ok bool) {
	offset = 0
	limit = previewDefaultLimit

	if offsetStr != "" {
		n, err := strconv.Atoi(offsetStr)
		if err != nil || n < 0 {
			return 0, 0, false
		}
		offset = n
	}

	if limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > previewMaxLimit {
			n = previewMaxLimit
		}
		limit = n
	}

	return offset, limit, true
}
