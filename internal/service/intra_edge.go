package service

import (
	"strings"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/identutil"
)

// GH #6: string-form intra-pipeline edges.
//
// An intra-pipeline dependency is normally authored as a Terraform
// module-output reference (module.<producer>.outputs["default"]...), which the
// HCL parser turns into a graph.Edge. The DAG builders (topoSort, the
// parents/edges maps in run.go and orchestration.go) then order the consumer
// after its producer.
//
// A user can equally write the same edge as a plain table reference,
//
//	inputs = { x = "<own-schema>.<sibling-table>" }
//
// which the parser cannot distinguish from a genuine cross-pipeline read, so it
// lands in the node's Config["external_inputs"] map with NO graph.Edge. With no
// edge the consumer gets empty parents, the topo sort can't order it after the
// sibling, and on a first run the consumer can execute before the sibling's
// table exists, dead-locking on TABLE_OR_VIEW_NOT_FOUND.
//
// reclassifyIntraPipelineEdges repairs the parsed graph in place: for every
// external_inputs entry that actually points at a sibling node's output table
// in this pipeline's own schema, it synthesises the missing graph.Edge and
// removes the alias from external_inputs. Both DAG builders then treat it
// exactly like a normal module-ref edge:
//   - parents + topoSort order the consumer after the producer;
//   - buildInputs / buildNodeInputsExpr resolve it through the edge machinery;
//   - upstreamProducerPipelines no longer sees it, so no spurious
//     cross-pipeline EventBridge trigger / Lake Formation grant is emitted for
//     what is really a sibling.
//
// A ref is reclassified iff BOTH hold:
//   - its schema equals the pipeline's own schema, AND
//   - its table matches a sibling node's produced table identifier (using the
//     same default-only-bare / "<node>__<key>" naming rule the runner uses in
//     _table_id_for, mirrored by producedTableIndex below).
//
// A string ref to the own schema that does NOT match any sibling-produced
// table (for example an externally-created table that happens to live in the
// same schema) is deliberately left in external_inputs: only a real sibling
// producer creates an edge.
func reclassifyIntraPipelineEdges(g *graph.PipelineGraph, ownSchema string) {
	if ownSchema == "" || len(g.Nodes) == 0 {
		return
	}
	produced := producedTableIndex(g)
	if len(produced) == 0 {
		return
	}
	ownSchemaID := identutil.Sanitize(ownSchema)

	for i := range g.Nodes {
		n := &g.Nodes[i]
		ext, ok := n.Config["external_inputs"].(map[string]interface{})
		if !ok || len(ext) == 0 {
			continue
		}
		for alias, raw := range ext {
			ref, _ := raw.(string)
			producer, ok := intraPipelineProducer(ref, ownSchemaID, n.ID, produced)
			if !ok {
				continue
			}
			// Synthesise the intra-pipeline edge the author omitted. ToInput
			// is the alias so buildInputs/buildNodeInputsExpr key the upstream
			// table under the right SQL alias. FromOutput is left empty
			// (= default) — the consumers (topoSort, the run.go/
			// orchestration.go parents maps) read FromNode/ToNode/ToInput and
			// resolve the producer's default output the same way they do for
			// a parsed module-ref edge, and these synthesized edges are never
			// re-emitted as HCL.
			g.Edges = append(g.Edges, graph.Edge{
				FromNode: producer,
				ToNode:   n.ID,
				ToInput:  alias,
			})
			delete(ext, alias)
		}
		if len(ext) == 0 {
			// Drop the now-empty map so downstream `len(...) > 0` guards and
			// emit paths don't treat the node as having external inputs.
			delete(n.Config, "external_inputs")
		}
	}
}

// producedTableIndex maps each sanitized produced-table identifier to the node
// that writes it, for every transform node in the graph. Keys mirror
// runner.py::_table_id_for and orchestration.go::upstreamProducerPipelines:
// a default-only transform contributes both the bare `<node>` and
// `<node>__default` forms; a multi-output transform contributes `<node>__<key>`
// per output key.
func producedTableIndex(g *graph.PipelineGraph) map[string]string {
	idx := map[string]string{}
	for i := range g.Nodes {
		n := g.Nodes[i]
		if n.Type != "transform" {
			continue
		}
		bare := identutil.Sanitize(n.ID)
		outs, _ := n.Config["output_definitions"].(map[string]interface{})
		if defaultOnlyOutputs(outs) {
			// A default-only transform is reachable by both the bare
			// `<node>` name (what the runner actually writes) and the
			// `<node>__default` form (older authored refs).
			idx[bare] = n.ID
			idx[bare+"__default"] = n.ID
			continue
		}
		for k := range outs {
			idx[bare+"__"+identutil.Sanitize(k)] = n.ID
		}
	}
	return idx
}

// defaultOnlyOutputs reports whether an output_definitions map describes the
// implicit single "default" output: empty/absent, or exactly the one key
// "default". This is the condition under which the runner writes a BARE
// `<node>` table name (no `__default` suffix); see canonicalTableSegment and
// runner/runner.py::_table_id_for.
func defaultOnlyOutputs(outs map[string]interface{}) bool {
	if len(outs) == 0 {
		return true
	}
	if len(outs) == 1 {
		_, ok := outs["default"]
		return ok
	}
	return false
}

// canonicalTableSegment returns the table-name segment for a node output,
// encoding the single bare/suffixed rule shared by every canonical-name path
// (intra-edge wiring, backfill staging/diff/promote). It mirrors
// runner/runner.py::_table_id_for exactly:
//   - bare <nodeID> when the output key is "default" AND output_definitions is
//     empty/absent or is exactly the single key ["default"];
//   - <nodeID>__<key> otherwise.
//
// nodeID is passed through verbatim; the suffix decision is the only thing this
// helper owns. (producedTableIndex sanitizes its own keys separately because it
// indexes by sanitized identifier.)
func canonicalTableSegment(nodeID string, outputDefs map[string]interface{}, key string) string {
	if key == "default" && defaultOnlyOutputs(outputDefs) {
		return nodeID
	}
	return nodeID + "__" + key
}

// intraPipelineProducer returns the producing node and true when ref is a
// string-form "<schema>.<table>" reference resolving to a sibling node in this
// same pipeline. ownSchemaID is the pipeline's sanitized schema identifier;
// produced is the index from producedTableIndex. consumer is the node holding
// the ref and is never matched against itself.
func intraPipelineProducer(ref, ownSchemaID, consumer string, produced map[string]string) (string, bool) {
	if ownSchemaID == "" || ref == "" {
		return "", false
	}
	// Only the two-part "<schema>.<table>" form is an external table ref;
	// anything else (module refs, paths, three-level names) is out of scope.
	parts := strings.Split(ref, ".")
	if len(parts) != 2 {
		return "", false
	}
	schema := identutil.Sanitize(parts[0])
	table := identutil.Sanitize(parts[1])
	if schema != ownSchemaID {
		return "", false
	}
	producer, ok := produced[table]
	if !ok || producer == consumer {
		return "", false
	}
	return producer, true
}
