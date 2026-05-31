package hclparser

import (
	"fmt"

	"github.com/vesahyp/clavesa/internal/graph"
)

// Validate runs all graph-level checks on g and returns the resulting messages.
// Topology checks always run; schema checks only when both sides have schemas.
func Validate(g graph.PipelineGraph) []graph.ValidationMessage {
	var msgs []graph.ValidationMessage

	nodeByID := make(map[string]graph.Node, len(g.Nodes))
	for _, n := range g.Nodes {
		nodeByID[n.ID] = n
	}

	// UNKNOWN_MODULE_SOURCE
	for _, n := range g.Nodes {
		if !IsRecognisedModuleSource(n.ModuleSource) {
			msgs = append(msgs, graph.ValidationMessage{
				Code:    graph.CodeUnknownModuleSource,
				Message: fmt.Sprintf("node %q has unrecognised module source %q", n.ID, n.ModuleSource),
				Nodes:   []string{n.ID},
			})
		}
	}

	// DANGLING_REFERENCE
	for _, e := range g.Edges {
		if _, ok := nodeByID[e.FromNode]; !ok {
			msgs = append(msgs, graph.ValidationMessage{
				Code:    graph.CodeDanglingReference,
				Message: fmt.Sprintf("edge references unknown source node %q", e.FromNode),
				Edges:   []graph.ValidationEdgeRef{{From: e.FromNode, To: e.ToNode}},
			})
			continue
		}
		if _, ok := nodeByID[e.ToNode]; !ok {
			msgs = append(msgs, graph.ValidationMessage{
				Code:    graph.CodeDanglingReference,
				Message: fmt.Sprintf("edge references unknown destination node %q", e.ToNode),
				Edges:   []graph.ValidationEdgeRef{{From: e.FromNode, To: e.ToNode}},
			})
		}
	}

	// DISCONNECTED_NODE — a node wired to nothing in the data flow. A
	// node-to-node edge counts, and so does an external input: a transform
	// that reads a workspace source (source_inputs, ADR-017) or a
	// cross-pipeline / external table (external_inputs, ADR-016 slice 2)
	// is fed real data even though that reference is not an edge between
	// two nodes in THIS pipeline's graph. Without the external-input check,
	// every terminal star-schema dim that reads `<schema>.<table>` from an
	// upstream pipeline gets falsely flagged.
	connected := make(map[string]bool)
	for _, e := range g.Edges {
		connected[e.FromNode] = true
		connected[e.ToNode] = true
	}
	for _, n := range g.Nodes {
		if connected[n.ID] || hasExternalInputs(n) {
			continue
		}
		msgs = append(msgs, graph.ValidationMessage{
			Code:    graph.CodeDisconnectedNode,
			Message: fmt.Sprintf("node %q has no edges", n.ID),
			Nodes:   []string{n.ID},
		})
	}

	// CYCLE_DETECTED — DFS
	if cycles := detectCycles(g.Nodes, g.Edges); len(cycles) > 0 {
		msgs = append(msgs, graph.ValidationMessage{
			Code:    graph.CodeCycleDetected,
			Message: "pipeline graph contains a cycle",
			Nodes:   cycles,
		})
	}

	return msgs
}

// hasExternalInputs reports whether a node reads from a workspace source
// (source_inputs, ADR-017) or a cross-pipeline / external table
// (external_inputs, ADR-016 slice 2). The parser stows these references in
// Config under those synthetic keys because they are not node-to-node
// edges. A node fed only by them is connected to the data flow, so it must
// not be flagged DISCONNECTED_NODE.
func hasExternalInputs(n graph.Node) bool {
	for _, key := range []string{"source_inputs", "external_inputs"} {
		if m, ok := n.Config[key].(map[string]interface{}); ok && len(m) > 0 {
			return true
		}
	}
	return false
}

// detectCycles returns the set of node IDs involved in cycles using iterative DFS.
func detectCycles(nodes []graph.Node, edges []graph.Edge) []string {
	adj := make(map[string][]string)
	for _, e := range edges {
		adj[e.FromNode] = append(adj[e.FromNode], e.ToNode)
	}

	const (
		unvisited = 0
		inStack   = 1
		done      = 2
	)
	state := make(map[string]int)
	var cycleNodes []string

	var dfs func(id string) bool
	dfs = func(id string) bool {
		state[id] = inStack
		for _, neighbor := range adj[id] {
			if state[neighbor] == inStack {
				cycleNodes = append(cycleNodes, neighbor)
				return true
			}
			if state[neighbor] == unvisited {
				if dfs(neighbor) {
					if state[id] == inStack {
						cycleNodes = append(cycleNodes, id)
					}
					return true
				}
			}
		}
		state[id] = done
		return false
	}

	for _, n := range nodes {
		if state[n.ID] == unvisited {
			dfs(n.ID)
		}
	}

	return cycleNodes
}
