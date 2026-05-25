// Package aslgen builds the Step Functions ASL state-machine shape from a
// pipeline DAG. Returns a structure suitable for handoff to the HCL emitter
// (see internal/orchestration/tfgen). No HCL or Terraform knowledge here —
// just graph → state-machine shape (Task, Parallel, Next/End wiring).
//
// Lives outside the historical HCL-side ASL builder (modules/orchestration/
// aws/main.tf:1-247) because HCL can't recurse: nested fanouts and
// multi-hop branches were structurally unrepresentable, leaving downstream
// nodes orphaned at the top level and failing AWS's MISSING_TRANSITION_TARGET
// validator. See plan: ~/.claude/plans/agent-reported-a-bug-eager-riddle.md.
package aslgen

import (
	"fmt"
	"sort"
)

// StateType discriminates between the two ASL state kinds the builder emits.
// Choice / Map / Wait aren't needed yet; transforms are always Task and
// fanouts are always Parallel.
type StateType int

const (
	Task StateType = iota
	Parallel
)

// State is one entry in the state machine's States map. Discriminated by Type.
//   - Task: Next OR End must be set (Next empty + End false is malformed).
//   - Parallel: Branches non-nil; Next or End set on the Parallel itself.
type State struct {
	Name     string // map key in the State machine States map
	Type     StateType
	Next     string // empty if End is true
	End      bool
	Branches []Branch // populated only when Type == Parallel
}

// Branch is one branch inside a Parallel state. States are returned in
// alphabetical-by-name order so the emitter can produce byte-stable output.
type Branch struct {
	StartAt string
	States  []State
}

// StateMachine is the full graph-shape output. States are in dependency-
// preserving order (entry first, then topological).
type StateMachine struct {
	StartAt string
	States  []State
}

// Build turns a list of node IDs + a list of edges into a StateMachine.
// nodes contains every node ID in the DAG (transform nodes only —
// source/destination nodes don't get Task states). edges is from→to pairs.
//
// Errors when:
//   - the DAG has zero or multiple root nodes (no predecessors).
//   - a cycle is detected.
//   - a fanin node has predecessors that don't all descend from a single
//     common fanout ancestor (Parallel-unrepresentable topology).
func Build(nodes []string, edges []Edge) (StateMachine, error) {
	g := buildGraph(nodes, edges)

	roots := g.roots()
	if len(roots) == 0 {
		return StateMachine{}, fmt.Errorf("aslgen: no entry node (every node has at least one predecessor — cycle?)")
	}
	if len(roots) > 1 {
		return StateMachine{}, fmt.Errorf("aslgen: multiple entry nodes %v (single-root DAGs only)", roots)
	}

	states, err := g.walkChain(roots[0], map[string]bool{})
	if err != nil {
		return StateMachine{}, err
	}
	return StateMachine{StartAt: roots[0], States: states}, nil
}

// Edge is the minimal from→to pair aslgen needs. Mirrors graph.Edge but
// keeps this package free of the heavier graph dependency for easier testing.
type Edge struct{ From, To string }

// ---------------------------------------------------------------------------
// Internal graph adjacency
// ---------------------------------------------------------------------------

type adjGraph struct {
	nodes []string // sorted, unique
	succ  map[string][]string
	pred  map[string][]string
}

func buildGraph(nodes []string, edges []Edge) *adjGraph {
	g := &adjGraph{
		succ: map[string][]string{},
		pred: map[string][]string{},
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		if !seen[n] {
			seen[n] = true
			g.nodes = append(g.nodes, n)
		}
	}
	sort.Strings(g.nodes)
	for _, e := range edges {
		g.succ[e.From] = append(g.succ[e.From], e.To)
		g.pred[e.To] = append(g.pred[e.To], e.From)
	}
	// Sort successors/predecessors so output is deterministic.
	for k := range g.succ {
		sort.Strings(g.succ[k])
	}
	for k := range g.pred {
		sort.Strings(g.pred[k])
	}
	return g
}

func (g *adjGraph) roots() []string {
	var out []string
	for _, n := range g.nodes {
		if len(g.pred[n]) == 0 {
			out = append(out, n)
		}
	}
	return out
}

// walkChain walks forward from start, building State entries, stopping
// when it hits a node in stopAt (the parent walkChain owns that node) or
// runs off the end of the graph (terminal leaf). On a fanout, recurses into
// each branch with stopAt augmented by the convergence node (if any).
//
// Algorithm sketched in the plan file under "The ASL builder algorithm".
func (g *adjGraph) walkChain(start string, stopAt map[string]bool) ([]State, error) {
	var states []State
	cur := start
	visited := map[string]bool{}
	for {
		if stopAt[cur] {
			return states, nil
		}
		if visited[cur] {
			return nil, fmt.Errorf("aslgen: cycle through %q", cur)
		}
		visited[cur] = true

		succs := g.succ[cur]
		switch len(succs) {
		case 0:
			states = append(states, State{Name: cur, Type: Task, End: true})
			return states, nil
		case 1:
			next := succs[0]
			if stopAt[next] {
				// End-of-branch: the next hop is owned by a parent
				// walkChain. Close this branch with End=true.
				states = append(states, State{Name: cur, Type: Task, End: true})
				return states, nil
			}
			states = append(states, State{Name: cur, Type: Task, Next: next})
			cur = next
		default:
			// Fanout. Find the (closest) fanin convergence reachable from
			// any branch. If found, the Parallel transitions to it; if
			// the convergence is owned by an outer Parallel (in stopAt),
			// this Parallel ends and the outer walkChain picks it up.
			conv := g.findConvergence(cur)
			branchStops := copySet(stopAt)
			if conv != "" {
				branchStops[conv] = true
			}

			branches := make([]Branch, 0, len(succs))
			for _, s := range succs {
				branchStates, err := g.walkChain(s, branchStops)
				if err != nil {
					return nil, err
				}
				branches = append(branches, Branch{StartAt: s, States: branchStates})
			}
			// Stable branch order by StartAt — same shape on every emit.
			sort.Slice(branches, func(i, j int) bool {
				return branches[i].StartAt < branches[j].StartAt
			})

			parallelName := cur + "_Branches"
			parallel := State{Name: parallelName, Type: Parallel, Branches: branches}

			// Fanout node itself runs as a Task before the fanout.
			states = append(states, State{Name: cur, Type: Task, Next: parallelName})

			if conv == "" || stopAt[conv] {
				parallel.End = true
				states = append(states, parallel)
				return states, nil
			}
			parallel.Next = conv
			states = append(states, parallel)
			cur = conv
		}
	}
}

// findConvergence picks the fan-in node closest to fanout — measured by
// BFS distance from fanout — that's reachable from at least one of
// fanout's direct successors. Matches v1.1.4 HCL semantics: a fan-in is a
// convergence even if not all branches reach it (branches that don't
// converge just terminate inside the Parallel via End=true; the Parallel's
// Next runs once with whatever upstream branches produced).
//
// Returns "" when no such fan-in exists; the caller closes the Parallel
// with End=true.
func (g *adjGraph) findConvergence(fanout string) string {
	// BFS from fanout, find first node whose predecessor count > 1 and
	// is not the fanout itself.
	type qe struct {
		node  string
		depth int
	}
	visited := map[string]bool{fanout: true}
	queue := []qe{{fanout, 0}}
	for len(queue) > 0 {
		head := queue[0]
		queue = queue[1:]
		if head.node != fanout && len(g.pred[head.node]) > 1 {
			return head.node
		}
		for _, s := range g.succ[head.node] {
			if !visited[s] {
				visited[s] = true
				queue = append(queue, qe{s, head.depth + 1})
			}
		}
	}
	return ""
}

func copySet(s map[string]bool) map[string]bool {
	out := make(map[string]bool, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}
