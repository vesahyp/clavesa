package service

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// LineageEdge is one upstream→downstream relationship in a pipeline DAG,
// derived from the .tf files in the pipeline directory. The data is purely
// a function of the user's HCL — same on cloud and local pipelines (ADR-014
// parity is automatic) — so we recompute on every request rather than
// materializing into a separate table that would drift on hand edits.
type LineageEdge struct {
	// FromNode and ToNode are the unsanitized HCL module names. Pipelines
	// can mix - and _ in node ids; preserve what the user wrote.
	FromNode string `json:"from_node"`
	FromType string `json:"from_type"` // "source" | "transform"
	ToNode   string `json:"to_node"`
	ToType   string `json:"to_type"` // "transform" | "destination"

	// ViaTable is the catalog identifier the downstream node reads —
	// "<database>.<table>" — for transform→transform and transform→
	// destination edges where the upstream writes an Iceberg auto-table.
	// Empty for source→transform edges (sources stream Parquet, not a
	// catalog table). The TableDetail page filters on this exact pair to
	// find downstream consumers of the table being viewed.
	ViaTable string `json:"via_table,omitempty"`

	// FromPipeline / ToPipeline name the producing / consuming pipelines
	// when the edge crosses a pipeline boundary (ADR-016 slice 2).
	// Empty means same pipeline as the one being queried — most edges.
	// The UI uses these to render cross-pipeline edges distinctly and
	// to label them with the other pipeline's name.
	FromPipeline string `json:"from_pipeline,omitempty"`
	ToPipeline   string `json:"to_pipeline,omitempty"`

	// ToTable is the consumer's own output table id —
	// `<consumer_db>.<consumer_node>__default`. Only set when the
	// edge crosses a pipeline boundary (downstream cross-pipeline):
	// intra-pipeline rows derive the consumer table id client-side
	// from the current DB, but cross-pipeline rows need the
	// consumer's pipeline DB which the UI doesn't know without
	// re-fetching. via_table stays the producer's table so the
	// existing `via_table === fullTable` filter keeps matching.
	ToTable string `json:"to_table,omitempty"`
}

// LineageResult is the lineage graph plus the queried pipeline's own
// ADR-016 namespace. Catalog/Schema let the UI label a node's output
// table directly instead of guessing it from a `via_table` — which, for
// a pipeline that only reads cross-pipeline, points at the *upstream*
// pipeline's schema, not this one's.
type LineageResult struct {
	Edges   []LineageEdge `json:"edges"`
	Catalog string        `json:"catalog"`
	Schema  string        `json:"schema"`
}

// Lineage returns the lineage edges for one pipeline directory. Edges are
// ordered deterministically (by FromNode, then ToNode) so the JSON response
// is stable across requests — the UI relies on stability for stable React
// keys.
//
// As of v0.20.1 (ADR-016 slice 2), the response also includes cross-
// pipeline edges: upstreams the queried pipeline reads from other
// pipelines (via `external_inputs` `<schema>.<table>` references), and
// downstreams in other pipelines that read the queried pipeline's
// outputs. Cross-pipeline edges carry `from_pipeline` / `to_pipeline`
// fields so the UI can render them distinctly.
func (s *Service) Lineage(dir string) (*LineageResult, error) {
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, err
	}
	pipelineName := filepath.Base(abs)
	// ADR-016 Glue DB encoding — workspace catalog (always present
	// after Manifest.Load auto-migrates legacy manifests) + pipeline
	// schema (default = sanitize(pipeline_name)). Lineage's `via_table`
	// flows back to the UI's TableDetail page where the user clicks
	// through to a snapshot list keyed on the same DB the runner
	// writes to; mismatched encoding here would mean lineage-derived
	// links 404.
	m, _ := workspace.Load(s.workspace)
	if m == nil {
		// workspace-less call: no via_table or namespace possible
		return &LineageResult{Edges: buildLineage(g, "")}, nil
	}
	thisSchema := resolvePipelineSchema(abs, pipelineName)
	thisDB := identutil.EncodeGlueDatabase(m.CatalogIdentifier(), thisSchema)
	edges := buildLineage(g, thisDB)

	// Cross-pipeline edges (ADR-016 slice 2). Walk every pipeline in
	// the workspace once and use the same scan to feed both directions
	// — UPSTREAM (other pipelines' transforms whose tables this one
	// reads via external_inputs) and DOWNSTREAM (other pipelines'
	// transforms that read this pipeline's tables).
	siblings, _ := s.workspacePipelineScan()
	if len(siblings) > 0 {
		edges = append(edges, crossPipelineEdges(siblings, pipelineName, thisSchema, thisDB, m.CatalogIdentifier(), g)...)
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].FromPipeline != edges[j].FromPipeline {
			return edges[i].FromPipeline < edges[j].FromPipeline
		}
		if edges[i].FromNode != edges[j].FromNode {
			return edges[i].FromNode < edges[j].FromNode
		}
		if edges[i].ToPipeline != edges[j].ToPipeline {
			return edges[i].ToPipeline < edges[j].ToPipeline
		}
		return edges[i].ToNode < edges[j].ToNode
	})
	return &LineageResult{
		Edges:   edges,
		Catalog: m.CatalogIdentifier(),
		Schema:  thisSchema,
	}, nil
}

// scannedPipeline carries one workspace pipeline's identity + parsed
// HCL so cross-pipeline lineage and the producer-lookup helper can
// walk every pipeline once per request without re-parsing.
type scannedPipeline struct {
	name   string
	dir    string
	schema string
	db     string
	graph  graph.PipelineGraph
}

// workspacePipelineScan walks the workspace root one directory deep
// and parses every directory that's a valid clavesa pipeline.
// Returns an empty slice and nil error when the workspace is empty —
// callers degrade gracefully (single-pipeline lineage).
//
// A directory counts as a pipeline if it holds .tf files; node-less
// pipelines (freshly created, no transforms yet) are included so the
// schema-ownership validator sees a schema reserved before any node is
// added. Lineage is unaffected — a node-less sibling contributes no edges.
func (s *Service) workspacePipelineScan() ([]scannedPipeline, error) {
	m, _ := workspace.Load(s.workspace)
	if m == nil {
		return nil, nil
	}
	catalog := m.CatalogIdentifier()
	entries, err := os.ReadDir(s.workspace)
	if err != nil {
		return nil, err
	}
	out := make([]scannedPipeline, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") || strings.HasPrefix(e.Name(), "_") {
			continue
		}
		dir := filepath.Join(s.workspace, e.Name())
		if !hasTFFiles(dir) {
			continue // not a pipeline directory
		}
		g, err := hclparser.Parse(dir)
		if err != nil {
			continue
		}
		schema := resolvePipelineSchema(dir, e.Name())
		out = append(out, scannedPipeline{
			name:   e.Name(),
			dir:    dir,
			schema: schema,
			db:     identutil.EncodeGlueDatabase(catalog, schema),
			graph:  g,
		})
	}
	return out, nil
}

// crossPipelineEdges synthesises the cross-pipeline upstream/downstream
// edges for the queried pipeline. Both directions key off the same
// `external_inputs` config the parser stamps for `<schema>.<table>`
// string references — UPSTREAM walks the queried pipeline's transforms,
// DOWNSTREAM walks sibling pipelines' transforms.
func crossPipelineEdges(siblings []scannedPipeline, thisName, thisSchema, thisDB, catalog string, g graph.PipelineGraph) []LineageEdge {
	out := make([]LineageEdge, 0)

	// Build a lookup of every transform output across the workspace:
	// schema → table-base-name → (pipeline, node, key).
	type producer struct{ pipeline, node, key string }
	bySchemaTable := map[string]map[string]producer{}
	for _, p := range siblings {
		bySchemaTable[p.schema] = map[string]producer{}
		for _, n := range p.graph.Nodes {
			if n.Type != "transform" {
				continue
			}
			// Auto-Iceberg outputs land at `<node>__<key>`. The runner
			// uses "default" when output_definitions is absent; same
			// shape as the intra-pipeline edges above.
			key := "default"
			bySchemaTable[p.schema][identutil.Sanitize(n.ID)+"__"+key] = producer{p.name, n.ID, key}
		}
	}

	// UPSTREAM: external_inputs entries on the queried pipeline's
	// transforms reference tables produced elsewhere.
	for _, n := range g.Nodes {
		if n.Type != "transform" {
			continue
		}
		ext, _ := n.Config["external_inputs"].(map[string]interface{})
		for _, refRaw := range ext {
			refStr, ok := refRaw.(string)
			if !ok {
				continue
			}
			dot := strings.Index(refStr, ".")
			if dot < 0 {
				continue
			}
			refSchema := refStr[:dot]
			refTable := refStr[dot+1:]
			// Self-reference (transform in this pipeline) — not
			// cross-pipeline; the regular module-ref path handles
			// those edges. Skip to avoid double-counting.
			if refSchema == thisSchema {
				continue
			}
			prod, ok := bySchemaTable[refSchema][refTable]
			if !ok {
				// Unresolved — external Glue table or typo. Emit a
				// degraded edge so the UI can render "external"
				// upstream without a producer label.
				out = append(out, LineageEdge{
					FromNode:     refTable,
					FromType:     "transform",
					ToNode:       n.ID,
					ToType:       "transform",
					ViaTable:     identutil.EncodeGlueDatabase(catalog, refSchema) + "." + refTable,
					FromPipeline: "(external)",
				})
				continue
			}
			out = append(out, LineageEdge{
				FromNode:     prod.node,
				FromType:     "transform",
				ToNode:       n.ID,
				ToType:       "transform",
				ViaTable:     identutil.EncodeGlueDatabase(catalog, refSchema) + "." + refTable,
				FromPipeline: prod.pipeline,
			})
		}
	}

	// DOWNSTREAM: walk sibling pipelines, find external_inputs that
	// reference *this* pipeline's outputs.
	for _, sib := range siblings {
		if sib.name == thisName {
			continue
		}
		for _, n := range sib.graph.Nodes {
			if n.Type != "transform" {
				continue
			}
			ext, _ := n.Config["external_inputs"].(map[string]interface{})
			for _, refRaw := range ext {
				refStr, ok := refRaw.(string)
				if !ok {
					continue
				}
				dot := strings.Index(refStr, ".")
				if dot < 0 {
					continue
				}
				refSchema := refStr[:dot]
				if refSchema != thisSchema {
					continue
				}
				refTable := refStr[dot+1:]
				// FromNode = the producer in *this* pipeline.
				// Extract from `<node>__<key>` shape.
				fromNode := refTable
				if i := strings.LastIndex(refTable, "__"); i > 0 {
					fromNode = refTable[:i]
				}
				out = append(out, LineageEdge{
					FromNode:   fromNode,
					FromType:   "transform",
					ToNode:     n.ID,
					ToType:     "transform",
					ViaTable:   thisDB + "." + refTable,
					ToPipeline: sib.name,
					// Consumer's own output table id — the UI uses this
					// instead of deriving from `database` (which is THIS
					// pipeline's DB, wrong for a cross-pipeline jump).
					ToTable: sib.db + "." + identutil.Sanitize(n.ID) + "__default",
				})
			}
		}
	}
	return out
}

// buildLineage extracts the unit-testable core of Lineage so unit tests can
// drive it from a synthetic PipelineGraph without touching the filesystem.
// db is the encoded Glue DB / Iceberg namespace name produced by
// `identutil.EncodeGlueDatabase` from the workspace's catalog and the
// pipeline's schema.
func buildLineage(g graph.PipelineGraph, db string) []LineageEdge {
	nodeByID := make(map[string]graph.Node, len(g.Nodes))
	for _, n := range g.Nodes {
		nodeByID[n.ID] = n
	}

	edges := make([]LineageEdge, 0, len(g.Edges))
	for _, e := range g.Edges {
		from, fromOK := nodeByID[e.FromNode]
		to, toOK := nodeByID[e.ToNode]
		if !fromOK || !toOK {
			continue
		}
		via := ""
		if from.Type == "transform" {
			// All transform outputs land at <db>.<from>__<output_key>.
			// The orchestration emitter hardcodes "default" because graph.Edge
			// doesn't carry the from-output today; mirror that here so the UI
			// can match `via_table` against the table it's viewing.
			via = db + "." + identutil.Sanitize(e.FromNode) + "__default"
		}
		edges = append(edges, LineageEdge{
			FromNode: e.FromNode,
			FromType: from.Type,
			ToNode:   e.ToNode,
			ToType:   to.Type,
			ViaTable: via,
		})
	}

	// ADR-017: registered-source upstreams aren't HCL edges — they sit in
	// transforms' Config under `source_inputs`. Without surfacing them
	// here, TableDetail's Lineage panel renders "No upstream" for any
	// transform whose only upstream is a `sources.<name>` registry
	// reference, which is misleading (it really does have an upstream,
	// just one routed through the workspace source registry instead of
	// an inline module). Stamp one synthetic edge per (transform, alias)
	// so the panel labels the registry entry as the producer.
	for _, n := range g.Nodes {
		if n.Type != "transform" {
			continue
		}
		srcInputs, _ := n.Config["source_inputs"].(map[string]interface{})
		for _, raw := range srcInputs {
			name := sourceInputName(raw)
			if name == "" {
				continue
			}
			edges = append(edges, LineageEdge{
				FromNode: "sources." + name,
				FromType: "source-registry",
				ToNode:   n.ID,
				ToType:   "transform",
			})
		}
	}

	sort.Slice(edges, func(i, j int) bool {
		if edges[i].FromNode != edges[j].FromNode {
			return edges[i].FromNode < edges[j].FromNode
		}
		return edges[i].ToNode < edges[j].ToNode
	})
	return edges
}

// sourceInputName extracts the registered source name from a transform's
// `source_inputs[alias]` entry. Mirrors the orchestration emitter's
// resolution (orchestration.go:122) — string entries are
// `"sources.<name>"` (legacy or http kind=http sentinel); map entries
// carry the name in `spec_name` (v0.22.0+ typed s3 attachments).
func sourceInputName(raw interface{}) string {
	switch v := raw.(type) {
	case string:
		return strings.TrimPrefix(v, "sources.")
	case map[string]interface{}:
		if sn, ok := v["spec_name"].(string); ok {
			return sn
		}
	}
	return ""
}

