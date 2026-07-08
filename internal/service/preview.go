package service

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/dataquery"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/preview"
	"github.com/vesahyp/clavesa/internal/sources"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// PreviewSource fetches a paginated preview of a source node's data.
func (s *Service) PreviewSource(ctx context.Context, dir, nodeID string, offset, limit int) (*preview.PreviewResult, error) {
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, err
	}
	node, err := preview.FindNode(&g, nodeID)
	if err != nil {
		return nil, err
	}
	bucket, pfx, format, jsonPath, ok := extractS3Config(node)
	if !ok {
		return nil, fmt.Errorf("node %s does not have S3 source configuration", nodeID)
	}
	if !looksLikeLocalPath(bucket) {
		if err := s.ensureS3Client(); err != nil {
			return nil, err
		}
	}
	qr, err := s.fetchSourceData(ctx, bucket, pfx, format, jsonPath, 500)
	if err != nil {
		return nil, err
	}
	result := preview.Convert(qr)
	result.Items = paginateItems(result.Items, offset, limit)
	return result, nil
}

// PreviewTransform executes a transform node via the Clavesa runner
// container and returns input/output pairs.
//
// For multi-hop pipelines (source → transform1 → transform2), intermediate
// transforms are executed first so that computed columns are available to
// downstream SQL.
func (s *Service) PreviewTransform(ctx context.Context, dir, nodeID string, rowCount int) (*preview.TransformPreviewResult, error) {
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, err
	}
	node, err := preview.FindNode(&g, nodeID)
	if err != nil {
		return nil, err
	}

	lang, _ := node.Config["language"].(string)
	if lang == "" {
		lang = "sql"
	}

	var sqlStr, pythonScript string
	if lang == "python" {
		pythonScript, _ = node.Config["python"].(string)
		if path, ok := resolveFileRef(pythonScript); ok {
			data, err := os.ReadFile(filepath.Join(abs, path))
			if err != nil {
				return nil, fmt.Errorf("read python script %s: %w", path, err)
			}
			pythonScript = string(data)
		}
		if pythonScript == "" {
			return nil, fmt.Errorf("node %s has no python configuration", nodeID)
		}
	} else {
		sqlStr = extractSQL(node)
		if path, ok := resolveFileRef(sqlStr); ok {
			data, err := os.ReadFile(filepath.Join(abs, path))
			if err != nil {
				return nil, fmt.Errorf("read sql file %s: %w", path, err)
			}
			sqlStr = string(data)
		}
		if sqlStr == "" {
			return nil, fmt.Errorf("node %s has no SQL configuration", nodeID)
		}
		// Parse-check before dispatch (Slice 3). A bad SELECT * EXCEPT
		// (…) used to fail after the runner container spun up + Spark
		// booted — 30+ seconds of latency for a syntax error. Catch it
		// here so the CLI / UI's "Preview" button returns in tens of
		// milliseconds with the parser's pointer-into-SQL message.
		// Transport failures (warm worker dead) are logged and don't
		// block — preview will then fall through to the runner and
		// surface a real parse error at dispatch time.
		if err := s.ValidateSQL(ctx, sqlStr); err != nil {
			var pe *ParseError
			if errors.As(err, &pe) {
				return nil, fmt.Errorf("SQL parse failed:\n  %s", pe.Message)
			}
			fmt.Fprintf(os.Stderr, "warn: SQL parse-check skipped before preview: %v\n", err)
		}
	}

	allRows, err := s.resolveInputData(ctx, &g, abs, nodeID, rowCount)
	if err != nil {
		return nil, err
	}

	var primaryRows []map[string]interface{}
	for _, rows := range allRows {
		if primaryRows == nil {
			primaryRows = rows
		}
	}
	if rowCount > 0 {
		if len(primaryRows) > rowCount {
			primaryRows = primaryRows[:rowCount]
		}
		for alias, rs := range allRows {
			if len(rs) > rowCount {
				allRows[alias] = rs[:rowCount]
			}
		}
	}

	image, _ := node.Config["runner_image"].(string)
	localTag := workspace.LocalRunnerImageTag(s.workspace)
	if _, err := workspace.Load(s.workspace); err == nil {
		ensured, err := workspace.EnsureLocalRunnerImage(s.workspace)
		if err != nil {
			return nil, fmt.Errorf("ensure runner image: %w", err)
		}
		localTag = ensured
	}

	outputsByKey, err := preview.RunPreview(ctx, localTag, image, allRows, sqlStr, pythonScript)
	if err != nil {
		return nil, err
	}

	var allOutputRows []map[string]interface{}
	for _, rows := range outputsByKey {
		allOutputRows = append(allOutputRows, rows...)
	}
	pairs := make([]preview.TransformPair, 0, len(primaryRows))
	for i, row := range primaryRows {
		var out []map[string]interface{}
		if i < len(allOutputRows) {
			out = []map[string]interface{}{allOutputRows[i]}
		}
		pairs = append(pairs, preview.TransformPair{Input: row, Output: out})
	}

	displaySQL := sqlStr
	if lang == "python" {
		displaySQL = "(Python transform)"
	}
	return &preview.TransformPreviewResult{Pairs: pairs, SQL: displaySQL}, nil
}

// resolveInputData returns the data available as inputs to the given node,
// keyed by the SQL table alias. For source parents, raw S3 data is fetched.
// For transform parents, the transform is executed first (recursively) so
// that computed columns are available downstream.
func (s *Service) resolveInputData(ctx context.Context, g *graph.PipelineGraph, abs, nodeID string, rowCount int) (map[string][]map[string]interface{}, error) {
	// Find direct parent edges.
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

	// Ensure S3 credentials if any upstream source needs them.
	sourceNodes, _ := preview.FindUpstreamSources(g, nodeID)
	for _, src := range sourceNodes {
		if b, ok := src.Config["bucket"].(string); ok && !looksLikeLocalPath(b) {
			if err := s.ensureS3Client(); err != nil {
				return nil, err
			}
			break
		}
	}

	allRows := make(map[string][]map[string]interface{})
	for _, p := range parents {
		parentNode, err := preview.FindNode(g, p.fromNode)
		if err != nil {
			return nil, err
		}

		var rows []map[string]interface{}
		if parentNode.Type == "source" {
			// Source node: fetch raw data.
			bucket, pfx, format, jsonPath, ok := extractS3Config(parentNode)
			if !ok {
				continue
			}
			qr, err := s.fetchSourceData(ctx, bucket, pfx, format, jsonPath, 500)
			if err != nil {
				return nil, fmt.Errorf("fetch source %s: %w", p.fromNode, err)
			}
			pr := preview.Convert(qr)
			rows = pr.Items
		} else {
			// Transform node: re-execute the upstream chain so computed
			// columns are available downstream.
			result, err := s.executeTransform(ctx, g, abs, parentNode, rowCount)
			if err != nil {
				return nil, fmt.Errorf("chain transform %s: %w", p.fromNode, err)
			}
			rows = result
		}
		allRows[p.alias] = rows
	}

	// ADR-017: transforms can also reference workspace-registered sources
	// via `inputs = { alias = "sources.<name>" }` (kind=http legacy
	// sentinel) or a typed `source_inputs[alias] = { spec_name = "..." }`
	// block (kind=s3). Neither shape produces a graph edge — the parser
	// surfaces them under Config["source_inputs"]. `pipeline run` resolves
	// these via internal/sources at execution time (run.go:buildInputs);
	// without the mirror here, preview would hand the runner an empty
	// inputs map and the SQL would fail with TABLE_OR_VIEW_NOT_FOUND.
	var target *graph.Node
	for i := range g.Nodes {
		if g.Nodes[i].ID == nodeID {
			target = &g.Nodes[i]
			break
		}
	}
	if target != nil {
		if srcInputs, ok := target.Config["source_inputs"].(map[string]interface{}); ok && len(srcInputs) > 0 {
			store := sources.New(s.workspace)
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
				res, err := s.fetchRegistrySourceRows(ctx, spec, 500)
				if err != nil {
					return nil, fmt.Errorf("preview input %q (source %q): %w", alias, name, err)
				}
				allRows[alias] = res.Items
			}
		}
		// ADR-016: cross-pipeline reads (`external_inputs`). The referenced
		// table's data already exists in the workspace-shared local Hadoop
		// warehouse — sample it via a query-mode runner rather than
		// re-executing the producing pipeline. Mirror of
		// internal/preview/resolve.go.
		if extInputs, ok := target.Config["external_inputs"].(map[string]interface{}); ok && len(extInputs) > 0 {
			catalog := ""
			localTag := workspace.LocalRunnerImageTag(s.workspace)
			if m, _ := workspace.Load(s.workspace); m != nil {
				catalog = m.CatalogIdentifier()
				ensured, err := workspace.EnsureLocalRunnerImage(s.workspace)
				if err != nil {
					return nil, fmt.Errorf("ensure runner image: %w", err)
				}
				localTag = ensured
			}
			// Cross-pipeline reads sample the workspace's *active*
			// warehouse (ADR-024): local Hadoop dir or the cloud Glue/S3
			// warehouse. Cloud + undeployed is a hard error surfaced to
			// the caller — never a silent local fallback.
			warehouse, err := workspace.WarehouseURI(s.workspace)
			if err != nil {
				return nil, err
			}
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
				sql := fmt.Sprintf("SELECT * FROM %s LIMIT %d", tableID, 500)
				rows, err := preview.QueryWarehouseTable(ctx, localTag, image, warehouse, sql)
				if err != nil {
					return nil, fmt.Errorf("preview input %q (cross-pipeline table %q): %w", alias, ref, err)
				}
				allRows[alias] = rows
			}
		}
	}
	return allRows, nil
}

// PreviewRegistrySource fetches a paginated preview of a workspace-
// registered source's raw data, standalone — without attaching it to a
// pipeline. Reuses the same host-side fetch path that transform-input
// resolution uses, so http and s3 sources both work and the format
// dispatch matches `pipeline run`.
func (s *Service) PreviewRegistrySource(ctx context.Context, name string, offset, limit int) (*preview.PreviewResult, error) {
	spec, err := sources.New(s.workspace).Get(name)
	if err != nil {
		return nil, err
	}
	if spec.Credentials != "" {
		return nil, fmt.Errorf("source %q uses credentials; preview does not yet resolve them (works in `pipeline run`)", name)
	}
	res, err := s.fetchRegistrySourceRows(ctx, spec, 500)
	if err != nil {
		return nil, err
	}
	res.Items = paginateItems(res.Items, offset, limit)
	return res, nil
}

// fetchRegistrySourceRows fetches a workspace-registered source for
// preview. Mirrors the format dispatch the runner does on a real
// `pipeline run`, minus credential resolution and partition walking.
func (s *Service) fetchRegistrySourceRows(ctx context.Context, spec sources.Spec, limit int) (*preview.PreviewResult, error) {
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
		var qr *dataquery.QueryResult
		switch spec.Format {
		case "csv":
			qr, err = parseSvcCSV(resp.Body, limit)
		case "json":
			qr, err = parseSvcJSON(resp.Body, "", limit)
		case "parquet":
			qr, err = dataquery.ParseParquet(resp.Body, limit)
		default:
			return nil, fmt.Errorf("unsupported http source format %q", spec.Format)
		}
		if err != nil {
			return nil, err
		}
		return preview.Convert(qr), nil
	case "s3":
		if !looksLikeLocalPath(spec.Bucket) {
			if err := s.ensureS3Client(); err != nil {
				return nil, err
			}
		}
		qr, err := s.fetchSourceData(ctx, spec.Bucket, spec.Prefix, spec.Format, "", limit)
		if err != nil {
			return nil, err
		}
		return preview.Convert(qr), nil
	default:
		return nil, fmt.Errorf("source kind %q not supported in preview", spec.Kind)
	}
}

// executeTransform runs a single transform node via the runner container and
// returns its flat output rows. Recursively resolves its own inputs.
func (s *Service) executeTransform(ctx context.Context, g *graph.PipelineGraph, abs string, node *graph.Node, rowCount int) ([]map[string]interface{}, error) {
	lang, _ := node.Config["language"].(string)
	if lang == "" {
		lang = "sql"
	}

	inputData, err := s.resolveInputData(ctx, g, abs, node.ID, rowCount)
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
		if path, ok := resolveFileRef(pythonScript); ok {
			data, err := os.ReadFile(filepath.Join(abs, path))
			if err != nil {
				return nil, fmt.Errorf("read python script %s: %w", path, err)
			}
			pythonScript = string(data)
		}
		if pythonScript == "" {
			return nil, fmt.Errorf("node %s has no python configuration", node.ID)
		}
	} else {
		sqlStr = extractSQL(node)
		if path, ok := resolveFileRef(sqlStr); ok {
			data, err := os.ReadFile(filepath.Join(abs, path))
			if err != nil {
				return nil, fmt.Errorf("read sql file %s: %w", path, err)
			}
			sqlStr = string(data)
		}
		if sqlStr == "" {
			return nil, fmt.Errorf("node %s has no SQL configuration", node.ID)
		}
	}

	image, _ := node.Config["runner_image"].(string)
	localTag := workspace.LocalRunnerImageTag(s.workspace)
	if _, err := workspace.Load(s.workspace); err == nil {
		ensured, err := workspace.EnsureLocalRunnerImage(s.workspace)
		if err != nil {
			return nil, fmt.Errorf("ensure runner image: %w", err)
		}
		localTag = ensured
	}

	outputsByKey, err := preview.RunPreview(ctx, localTag, image, inputData, sqlStr, pythonScript)
	if err != nil {
		return nil, err
	}
	var allOutput []map[string]interface{}
	for _, rows := range outputsByKey {
		allOutput = append(allOutput, rows...)
	}
	return allOutput, nil
}

// ---------------------------------------------------------------------------
// Preview helpers
// ---------------------------------------------------------------------------

// looksLikeLocalPath reports whether s is a local filesystem path.
// S3 bucket names cannot start with /, ./, or ../.
func looksLikeLocalPath(s string) bool {
	return strings.HasPrefix(s, "/") || strings.HasPrefix(s, "./") || strings.HasPrefix(s, "../")
}

func (s *Service) fetchSourceData(ctx context.Context, bucket, prefix, format, jsonPath string, limit int) (*dataquery.QueryResult, error) {
	if looksLikeLocalPath(bucket) {
		f, err := os.Open(filepath.Join(bucket, prefix))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		switch format {
		case "csv":
			return parseSvcCSV(f, limit)
		case "json":
			return parseSvcJSON(f, jsonPath, limit)
		case "parquet":
			return dataquery.ParseParquet(f, limit)
		default:
			return nil, fmt.Errorf("preview does not support format %q yet (csv, json, parquet only); tsv sources are read at `clavesa pipeline run`", format)
		}
	}

	// Fetch up to 10 recent files, reading until we have enough rows.
	keys, err := s.findLatestS3Keys(ctx, bucket, prefix, 10)
	if err != nil {
		return nil, err
	}

	var combined *dataquery.QueryResult
	for _, key := range keys {
		getOut, err := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, fmt.Errorf("GetObject %s: %w", key, err)
		}
		var qr *dataquery.QueryResult
		switch format {
		case "csv":
			qr, err = parseSvcCSV(getOut.Body, limit)
		case "json":
			qr, err = parseSvcJSON(getOut.Body, jsonPath, limit)
		case "parquet":
			qr, err = dataquery.ParseParquet(getOut.Body, limit)
		default:
			getOut.Body.Close()
			return nil, fmt.Errorf("preview does not support format %q yet (csv, json, parquet only); tsv sources are read at `clavesa pipeline run`", format)
		}
		getOut.Body.Close()
		if err != nil {
			return nil, err
		}
		if combined == nil {
			combined = qr
		} else {
			combined.Rows = append(combined.Rows, qr.Rows...)
			combined.RowCount = len(combined.Rows)
		}
		if combined.RowCount >= limit {
			combined.Rows = combined.Rows[:limit]
			combined.RowCount = limit
			combined.Truncated = true
			break
		}
	}
	if combined == nil {
		return nil, fmt.Errorf("no data found at s3://%s/%s", bucket, prefix)
	}
	return combined, nil
}

func paginateItems(items []map[string]interface{}, offset, limit int) []map[string]interface{} {
	if offset >= len(items) {
		return []map[string]interface{}{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

func extractS3Config(node *graph.Node) (bucket, prefix, format, jsonPath string, ok bool) {
	b, hasBucket := node.Config["bucket"].(string)
	if !hasBucket || b == "" {
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

func extractSQL(node *graph.Node) string {
	if node.PreviewSQL != "" {
		return node.PreviewSQL
	}
	s, _ := node.Config["sql"].(string)
	return s
}

// resolveFileRef parses a file("path") HCL expression and returns the inner
// path. Returns ("", false) when the expression is not a file() call.
func resolveFileRef(expr string) (string, bool) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "file(") || !strings.HasSuffix(expr, ")") {
		return "", false
	}
	inner := strings.TrimSpace(expr[len("file(") : len(expr)-1])
	inner = strings.Trim(inner, `"`)
	return inner, true
}

// ---------------------------------------------------------------------------
// Format parsers
// ---------------------------------------------------------------------------

func parseSvcCSV(r io.Reader, limit int) (*dataquery.QueryResult, error) {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}
	cols := make([]graph.Column, len(header))
	for i, h := range header {
		cols[i] = graph.Column{Name: h, Type: "string", Nullable: true}
	}
	var rows [][]string
	truncated := false
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV row: %w", err)
		}
		if len(rows) >= limit {
			truncated = true
			break
		}
		rows = append(rows, rec)
	}
	// Per-column type inference. csv.NewReader hands us strings only, but
	// downstream Convert ships these rows to the runner via JSON, where
	// `spark.createDataFrame(rows)` infers Python-side types from each
	// value. Without type hints here every column lands as STRING in the
	// runner; a SQL predicate like `WHERE amount > 0` then fails with
	// CAST_INVALID_INPUT because Spark won't widen STRING to numeric.
	// Mirrors `spark.read.option("inferSchema", "true").csv(...)` in
	// runner/runner.py for the `pipeline run` path.
	//
	// Cost: one extra walk over the parsed rows. Tiny vs the network /
	// disk I/O the CSV already paid. Large-CSV users who want to skip
	// inference can pin an explicit schema on the source spec (TODO —
	// `schema` field on CSV sources not yet implemented; see TODO.md).
	inferCSVColumnTypes(cols, rows)
	return &dataquery.QueryResult{Columns: cols, Rows: rows, RowCount: len(rows), Truncated: truncated}, nil
}

// inferCSVColumnTypes mutates cols[*].Type from "string" to "long" or
// "double" when every non-empty value in that column parses cleanly as
// that type. Empty cells are treated as null and don't disqualify a
// column. A column with no non-empty cells stays "string".
//
// Conservative on purpose — partial successes (some rows parse as int,
// some don't) stay "string" so we never silently drop data. Date
// inference is deliberately not attempted; Spark's date parser is
// format-sensitive and we'd need to thread a format option to do it
// safely. SQL `WHERE amount > 0` over numeric columns is the dominant
// failure mode worth fixing here.
func inferCSVColumnTypes(cols []graph.Column, rows [][]string) {
	for i := range cols {
		hasAny := false
		allInt := true
		allFloat := true
		for _, row := range rows {
			if i >= len(row) {
				continue
			}
			s := strings.TrimSpace(row[i])
			if s == "" {
				continue
			}
			hasAny = true
			if allInt {
				if _, err := strconv.ParseInt(s, 10, 64); err != nil {
					allInt = false
				}
			}
			if allFloat {
				if _, err := strconv.ParseFloat(s, 64); err != nil {
					allFloat = false
				}
			}
			if !allInt && !allFloat {
				break
			}
		}
		if !hasAny {
			continue
		}
		switch {
		case allInt:
			cols[i].Type = "long"
		case allFloat:
			cols[i].Type = "double"
		}
	}
}

func parseSvcJSON(r io.Reader, jsonPath string, limit int) (*dataquery.QueryResult, error) {
	if jsonPath == "" {
		return parseSvcNDJSON(r, limit)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read JSON body: %w", err)
	}
	var root interface{}
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	cur := root
	for _, part := range strings.Split(jsonPath, ".") {
		if part == "" {
			continue
		}
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("json_path %q: expected object at key %q", jsonPath, part)
		}
		cur, ok = m[part]
		if !ok {
			return nil, fmt.Errorf("json_path %q: key %q not found", jsonPath, part)
		}
	}
	arr, ok := cur.([]interface{})
	if !ok {
		return nil, fmt.Errorf("json_path %q: expected array, got %T", jsonPath, cur)
	}
	var rawRows []map[string]interface{}
	colSet := make(map[string]struct{})
	truncated := false
	for _, item := range arr {
		if len(rawRows) >= limit {
			truncated = true
			break
		}
		obj, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for k := range obj {
			colSet[k] = struct{}{}
		}
		rawRows = append(rawRows, obj)
	}
	colNames := make([]string, 0, len(colSet))
	for k := range colSet {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)
	cols := make([]graph.Column, len(colNames))
	for i, n := range colNames {
		cols[i] = graph.Column{Name: n, Type: "string", Nullable: true}
	}
	rows := make([][]string, len(rawRows))
	for i, obj := range rawRows {
		row := make([]string, len(colNames))
		for j, col := range colNames {
			v := obj[col]
			if v == nil {
				row[j] = ""
			} else {
				row[j] = fmt.Sprintf("%v", v)
			}
		}
		rows[i] = row
	}
	return &dataquery.QueryResult{Columns: cols, Rows: rows, RowCount: len(rawRows), Truncated: truncated}, nil
}

func parseSvcNDJSON(r io.Reader, limit int) (*dataquery.QueryResult, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var rawRows []map[string]interface{}
	colSet := make(map[string]struct{})
	truncated := false
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if len(rawRows) >= limit {
			truncated = true
			break
		}
		var obj map[string]interface{}
		if err := json.Unmarshal(line, &obj); err != nil {
			return nil, fmt.Errorf("parse NDJSON line: %w", err)
		}
		for k := range obj {
			colSet[k] = struct{}{}
		}
		rawRows = append(rawRows, obj)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan NDJSON: %w", err)
	}
	colNames := make([]string, 0, len(colSet))
	for k := range colSet {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)
	cols := make([]graph.Column, len(colNames))
	for i, n := range colNames {
		cols[i] = graph.Column{Name: n, Type: "string", Nullable: true}
	}
	rows := make([][]string, len(rawRows))
	for i, obj := range rawRows {
		row := make([]string, len(colNames))
		for j, col := range colNames {
			v := obj[col]
			if v == nil {
				row[j] = ""
			} else {
				row[j] = fmt.Sprintf("%v", v)
			}
		}
		rows[i] = row
	}
	return &dataquery.QueryResult{Columns: cols, Rows: rows, RowCount: len(rawRows), Truncated: truncated}, nil
}

// ---------------------------------------------------------------------------
// S3 latest-object lookup
// ---------------------------------------------------------------------------

// findLatestS3Key returns the key of the most recently modified object under
// prefix. It avoids scanning the entire bucket by learning the key structure
// from one object, then constructing targeted prefixes for recent dates.
//
// Strategy (max 5 API calls):
//  1. List one object to discover the key path structure.
//  2. Identify date-like segments (year=, month=, day=, or YYYY-MM-DD patterns).
//  3. Construct prefixes for today, then progressively earlier dates, and
//     list with each until we find objects.
//  4. Pick the newest by LastModified.
//
// findLatestS3Keys returns up to maxKeys S3 keys sorted newest-first by
// LastModified. It uses date-aware probing to skip to recent data.
func (s *Service) findLatestS3Keys(ctx context.Context, bucket, prefix string, maxKeys int) ([]string, error) {
	// Step 1: learn key structure.
	first, err := s.listS3Page(ctx, bucket, prefix, "")
	if err != nil {
		return nil, err
	}
	if len(first) == 0 {
		return nil, fmt.Errorf("no objects found at s3://%s/%s", bucket, prefix)
	}

	firstKey := aws.ToString(first[0].Key)
	staticPrefix, dateFormat := extractDateStructure(firstKey)

	var objects []s3types.Object
	if staticPrefix != "" && dateFormat != "" {
		now := time.Now().UTC()
		probes := buildDateProbes(staticPrefix, dateFormat, now)

		for _, probe := range probes {
			page, listErr := s.listS3Page(ctx, bucket, probe, "")
			if listErr != nil {
				return nil, listErr
			}
			if len(page) > 0 {
				objects = page
				break
			}
		}
	}

	if len(objects) == 0 {
		objects = first
	}

	// Sort by LastModified descending.
	sort.Slice(objects, func(i, j int) bool {
		ti := objects[i].LastModified
		tj := objects[j].LastModified
		if ti == nil || tj == nil {
			return false
		}
		return ti.After(*tj)
	})

	keys := make([]string, 0, maxKeys)
	for i, obj := range objects {
		if i >= maxKeys {
			break
		}
		keys = append(keys, aws.ToString(obj.Key))
	}
	return keys, nil
}

// extractDateStructure finds date-partitioned segments in an S3 key and returns
// the static prefix before the date and the format string.
// Supports: "year=YYYY/month=MM/day=DD" (Hive-style) and "YYYY/MM/DD" patterns.
func extractDateStructure(key string) (staticPrefix, format string) {
	segments := strings.Split(key, "/")
	for i, seg := range segments {
		if strings.HasPrefix(seg, "year=") {
			return strings.Join(segments[:i], "/") + "/", "hive"
		}
		// Match bare YYYY segment (4 digits, plausible year).
		if len(seg) == 4 && seg >= "2000" && seg <= "2099" {
			return strings.Join(segments[:i], "/") + "/", "bare"
		}
	}
	return "", ""
}

// buildDateProbes generates up to 4 S3 prefix probes for recent dates,
// starting from today and backing off.
func buildDateProbes(staticPrefix, format string, now time.Time) []string {
	var probes []string
	switch format {
	case "hive":
		// Try today, yesterday, 3 days ago, then current month.
		for _, offset := range []int{0, 1, 3} {
			d := now.AddDate(0, 0, -offset)
			probes = append(probes, fmt.Sprintf("%syear=%d/month=%02d/day=%02d/",
				staticPrefix, d.Year(), d.Month(), d.Day()))
		}
		// Broader: current month.
		probes = append(probes, fmt.Sprintf("%syear=%d/month=%02d/",
			staticPrefix, now.Year(), now.Month()))
	case "bare":
		for _, offset := range []int{0, 1, 3} {
			d := now.AddDate(0, 0, -offset)
			probes = append(probes, fmt.Sprintf("%s%d/%02d/%02d/",
				staticPrefix, d.Year(), d.Month(), d.Day()))
		}
		probes = append(probes, fmt.Sprintf("%s%d/%02d/",
			staticPrefix, now.Year(), now.Month()))
	}
	return probes
}

func (s *Service) listS3Page(ctx context.Context, bucket, prefix, startAfter string) ([]s3types.Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1000),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}
	out, err := s.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("ListObjectsV2: %w", err)
	}
	return out.Contents, nil
}
