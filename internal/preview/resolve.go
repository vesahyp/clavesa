package preview

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/sources"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// FindNode finds a node by ID in the pipeline graph.
func FindNode(g *graph.PipelineGraph, nodeID string) (*graph.Node, error) {
	for i := range g.Nodes {
		if g.Nodes[i].ID == nodeID {
			return &g.Nodes[i], nil
		}
	}
	return nil, fmt.Errorf("node %q not found", nodeID)
}

// FindUpstreamSources walks edges backwards from the given nodeID and returns
// all source nodes reachable upstream. A source node is identified by its
// type being "source" (not "transform" or "destination").
func FindUpstreamSources(g *graph.PipelineGraph, nodeID string) ([]graph.Node, error) {
	// Build reverse edge index: toNode -> []fromNode IDs
	upstream := make(map[string][]string)
	for _, e := range g.Edges {
		upstream[e.ToNode] = append(upstream[e.ToNode], e.FromNode)
	}

	// Build node index for O(1) lookup
	nodeIndex := make(map[string]*graph.Node, len(g.Nodes))
	for i := range g.Nodes {
		nodeIndex[g.Nodes[i].ID] = &g.Nodes[i]
	}

	// BFS backwards from nodeID, collecting source nodes
	visited := make(map[string]bool)
	queue := []string{nodeID}
	var sources []graph.Node

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		if visited[curr] {
			continue
		}
		visited[curr] = true

		// Do not include the starting node itself
		if curr != nodeID {
			if n, ok := nodeIndex[curr]; ok {
				if n.Type == "source" {
					sources = append(sources, *n)
				}
			}
		}

		for _, from := range upstream[curr] {
			if !visited[from] {
				queue = append(queue, from)
			}
		}
	}

	return sources, nil
}

// MapSourceAliases returns a map from source node ID to the SQL table alias
// that should be used when registering that source's data for a transform.
//
// For direct connections (source → currentNode), the alias comes from the
// edge's ToInput. For multi-hop chains (source → intermediate → currentNode),
// the alias comes from the edge connecting the intermediate to currentNode,
// since that's what the SQL in currentNode references.
func MapSourceAliases(g *graph.PipelineGraph, nodeID string, sources []graph.Node) map[string]string {
	// Direct parent edges: fromNode → toInput alias
	directParents := make(map[string]string)
	for _, e := range g.Edges {
		if e.ToNode == nodeID {
			alias := e.ToInput
			if alias == "" || alias == "default" {
				alias = e.FromNode
			}
			directParents[e.FromNode] = alias
		}
	}

	// Build forward edge index for walking from source to currentNode
	downstream := make(map[string][]string)
	for _, e := range g.Edges {
		downstream[e.FromNode] = append(downstream[e.FromNode], e.ToNode)
	}

	result := make(map[string]string, len(sources))
	for _, src := range sources {
		// If directly connected, use the direct alias
		if alias, ok := directParents[src.ID]; ok {
			result[src.ID] = alias
			continue
		}
		// Otherwise, BFS forward from source to find which direct parent
		// it reaches currentNode through
		visited := map[string]bool{src.ID: true}
		queue := []string{src.ID}
		for len(queue) > 0 {
			curr := queue[0]
			queue = queue[1:]
			if alias, ok := directParents[curr]; ok {
				result[src.ID] = alias
				break
			}
			for _, next := range downstream[curr] {
				if !visited[next] {
					visited[next] = true
					queue = append(queue, next)
				}
			}
		}
	}
	return result
}

// ResolveInputData returns the data available as inputs to the given node,
// keyed by SQL table alias. For source parents, raw data is fetched via S3.
// For transform parents, the transform is executed first (recursively) so
// that computed columns are available downstream.
func ResolveInputData(ctx context.Context, s3c dataquery.S3Client, g *graph.PipelineGraph, dir, nodeID string, rowCount int) (map[string][]map[string]interface{}, error) {
	type parentEdge struct {
		fromNode string
		alias    string
	}
	var parents []parentEdge
	for _, e := range g.Edges {
		if e.ToNode == nodeID {
			alias := e.ToInput
			if alias == "" || alias == "default" {
				alias = e.FromNode
			}
			parents = append(parents, parentEdge{fromNode: e.FromNode, alias: alias})
		}
	}

	allRows := make(map[string][]map[string]interface{})
	for _, p := range parents {
		parentNode, err := FindNode(g, p.fromNode)
		if err != nil {
			return nil, err
		}

		var rows []map[string]interface{}
		if parentNode.Type == "source" {
			bucket, prefix, format, jsonPath, ok := extractS3Config(parentNode)
			if !ok {
				continue
			}
			qr, err := fetchSourceResult(ctx, s3c, bucket, prefix, format, jsonPath, previewMaxLimit)
			if err != nil {
				return nil, fmt.Errorf("fetch source %s: %w", p.fromNode, err)
			}
			pr := Convert(qr)
			rows = pr.Items
		} else {
			// Transform parent: prefer the already-materialized Iceberg
			// snapshot when fresh — skips the upstream's
			// SQL/PySpark re-execution and the source re-fetch behind
			// it. Falls back to the full chain when the snapshot is
			// missing, stale, or the query fails.
			snapRows, used, err := ResolveUpstreamFromSnapshot(ctx, filepath.Dir(dir), dir, parentNode, rowCount)
			if err != nil {
				return nil, fmt.Errorf("snapshot upstream %s: %w", p.fromNode, err)
			}
			if used {
				rows = snapRows
			} else {
				output, err := executeTransformChain(ctx, s3c, g, dir, parentNode, rowCount)
				if err != nil {
					return nil, fmt.Errorf("chain transform %s: %w", p.fromNode, err)
				}
				rows = output
			}
		}
		allRows[p.alias] = rows
	}

	// ADR-017: transforms can reference workspace-registered sources via
	// `inputs = { alias = "sources.<name>" }` (kind=http legacy sentinel)
	// or a typed `source_inputs[alias] = { spec_name = "..." }` block
	// (kind=s3). Neither shape produces a graph edge — the parser surfaces
	// them under Config["source_inputs"]. `pipeline run` resolves these in
	// the service layer (run.go:buildInputs); preview must mirror that or
	// hand the runner an empty inputs map and fail with
	// TABLE_OR_VIEW_NOT_FOUND. Mirror of internal/service/preview.go.
	var target *graph.Node
	for i := range g.Nodes {
		if g.Nodes[i].ID == nodeID {
			target = &g.Nodes[i]
			break
		}
	}
	if target != nil {
		if srcInputs, ok := target.Config["source_inputs"].(map[string]interface{}); ok && len(srcInputs) > 0 {
			store := sources.New(filepath.Dir(dir))
			for alias, raw := range srcInputs {
				if _, already := allRows[alias]; already {
					continue
				}
				name := ""
				switch v := raw.(type) {
				case string:
					name = strings.TrimPrefix(v, "sources.")
				case map[string]interface{}:
					if sn, ok := v["spec_name"].(string); ok {
						name = sn
					}
				}
				if name == "" {
					return nil, fmt.Errorf("input %q: malformed source_inputs entry %v", alias, raw)
				}
				spec, err := store.Get(name)
				if err != nil {
					return nil, fmt.Errorf("preview input %q references source %q which is not registered: %w", alias, name, err)
				}
				if spec.Credentials != "" {
					return nil, fmt.Errorf("preview input %q: source %q uses credentials; preview does not yet resolve them (works in `pipeline run`)", alias, name)
				}
				rows, err := fetchRegistrySourceRows(ctx, s3c, spec, previewMaxLimit)
				if err != nil {
					return nil, fmt.Errorf("preview input %q (source %q): %w", alias, name, err)
				}
				allRows[alias] = rows
			}
		}
		// ADR-016: cross-pipeline reads — `inputs = { alias = "<schema>.<table>" }`
		// surfaces under Config["external_inputs"]. The referenced table's data
		// already exists in the workspace-shared local Hadoop warehouse, so
		// preview samples it via a query-mode runner rather than re-executing
		// the producing pipeline. Mirror of internal/service/preview.go.
		if extInputs, ok := target.Config["external_inputs"].(map[string]interface{}); ok && len(extInputs) > 0 {
			root := filepath.Dir(dir)
			catalog := ""
			localImage := runner.LocalImageName("")
			if m, _ := workspace.Load(root); m != nil {
				catalog = m.CatalogIdentifier()
				localImage = runner.LocalImageName(m.Name)
			}
			warehouse := workspace.LocalWarehouseDir(root)
			image, _ := target.Config["runner_image"].(string)
			for alias, raw := range extInputs {
				if _, already := allRows[alias]; already {
					continue
				}
				ref, _ := raw.(string)
				tableID, err := identutil.EncodeExternalTableRef(catalog, ref)
				if err != nil {
					return nil, fmt.Errorf("preview input %q: %w", alias, err)
				}
				sql := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableID, previewMaxLimit)
				rows, err := QueryWarehouseTable(ctx, localImage, image, warehouse, sql)
				if err != nil {
					return nil, fmt.Errorf("preview input %q (cross-pipeline table %q): %w", alias, ref, err)
				}
				allRows[alias] = rows
			}
		}
	}
	return allRows, nil
}

// fetchRegistrySourceRows resolves a workspace-registered source to flat
// rows for preview. Mirrors the format dispatch the runner does at real
// execution time, minus credential resolution and partition walking.
func fetchRegistrySourceRows(ctx context.Context, s3c dataquery.S3Client, spec sources.Spec, limit int) ([]map[string]interface{}, error) {
	switch spec.Kind {
	case "http":
		// `host.docker.internal` is the Docker Desktop name for the host
		// from inside a container. Sources that point at a dev-machine
		// service register that hostname so the runner can reach it; on
		// the host itself (where preview runs), it doesn't resolve, so
		// rewrite it to the loopback Docker Desktop forwards it to.
		fetchURL := strings.Replace(spec.URL, "://host.docker.internal", "://127.0.0.1", 1)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fetchURL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", spec.URL, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s: status %d", spec.URL, resp.StatusCode)
		}
		// ParseParquet needs the full body in memory anyway; read once,
		// dispatch on format. csv / json parsers stream fine but normalize
		// the call site.
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read response body: %w", err)
		}
		qr, err := parseByFormat(strings.NewReader(string(body)), spec.Format, "", limit)
		if err != nil {
			return nil, err
		}
		return Convert(qr).Items, nil
	case "s3":
		qr, err := fetchSourceResult(ctx, s3c, spec.Bucket, spec.Prefix, spec.Format, "", limit)
		if err != nil {
			return nil, err
		}
		return Convert(qr).Items, nil
	default:
		return nil, fmt.Errorf("source kind %q not supported in preview", spec.Kind)
	}
}

// executeTransformChain runs a single transform node via the runner container
// and returns its flat output rows. Recursively resolves its own inputs.
func executeTransformChain(ctx context.Context, s3c dataquery.S3Client, g *graph.PipelineGraph, dir string, node *graph.Node, rowCount int) ([]map[string]interface{}, error) {
	lang, _ := node.Config["language"].(string)
	if lang == "" {
		lang = "sql"
	}

	inputData, err := ResolveInputData(ctx, s3c, g, dir, node.ID, rowCount)
	if err != nil {
		return nil, err
	}
	if rowCount > 0 {
		for alias, rows := range inputData {
			if len(rows) > rowCount {
				inputData[alias] = rows[:rowCount]
			}
		}
	}

	var sqlStr, pythonScript string
	if lang == "python" {
		pythonScript, _ = node.Config["python"].(string)
		if path, ok := resolveFileRefPreview(pythonScript); ok {
			data, err := os.ReadFile(filepath.Join(dir, path))
			if err != nil {
				return nil, fmt.Errorf("read python script %s: %w", path, err)
			}
			pythonScript = string(data)
		}
		if pythonScript == "" {
			return nil, fmt.Errorf("node %s has no python configuration", node.ID)
		}
	} else {
		sqlStr, _ = extractSQL(node)
		if path, ok := resolveFileRefPreview(sqlStr); ok {
			data, err := os.ReadFile(filepath.Join(dir, path))
			if err != nil {
				return nil, fmt.Errorf("read sql file %s: %w", path, err)
			}
			sqlStr = string(data)
		}
		if sqlStr == "" {
			return nil, fmt.Errorf("node %s has no SQL configuration", node.ID)
		}
	}

	localImage := runner.LocalImageName("")
	if m, _ := workspace.Load(filepath.Dir(dir)); m != nil {
		localImage = runner.LocalImageName(m.Name)
	}
	image, _ := node.Config["runner_image"].(string)

	outputsByKey, err := RunPreview(ctx, localImage, image, inputData, sqlStr, pythonScript)
	if err != nil {
		return nil, err
	}
	var allOutput []map[string]interface{}
	for _, rows := range outputsByKey {
		allOutput = append(allOutput, rows...)
	}
	return allOutput, nil
}

// resolveFileRefPreview parses a file("path") expression and returns the path.
func resolveFileRefPreview(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "file(") || !strings.HasSuffix(expr, ")") {
		return "", false
	}
	inner := strings.TrimSpace(expr[len("file(") : len(expr)-1])
	inner = strings.Trim(inner, `"`)
	return inner, true
}
