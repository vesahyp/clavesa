package aslgen

import (
	"fmt"
	"strings"
	"testing"
)

func TestBuild_Linear(t *testing.T) {
	t.Parallel()
	sm, err := Build([]string{"a", "b", "c"}, []Edge{{"a", "b"}, {"b", "c"}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if sm.StartAt != "a" {
		t.Fatalf("StartAt = %q, want a", sm.StartAt)
	}
	want := []State{
		{Name: "a", Type: Task, Next: "b"},
		{Name: "b", Type: Task, Next: "c"},
		{Name: "c", Type: Task, End: true},
	}
	assertStates(t, sm.States, want)
	assertReachable(t, sm)
}

func TestBuild_SingleFanout_NoConvergence(t *testing.T) {
	t.Parallel()
	// a → {b, c}, both terminal
	sm, err := Build([]string{"a", "b", "c"}, []Edge{{"a", "b"}, {"a", "c"}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	want := []State{
		{Name: "a", Type: Task, Next: "a_Branches"},
		{Name: "a_Branches", Type: Parallel, End: true, Branches: []Branch{
			{StartAt: "b", States: []State{{Name: "b", Type: Task, End: true}}},
			{StartAt: "c", States: []State{{Name: "c", Type: Task, End: true}}},
		}},
	}
	assertStates(t, sm.States, want)
	assertReachable(t, sm)
}

func TestBuild_FanoutFanin(t *testing.T) {
	t.Parallel()
	// a → {b, c} → d → e
	sm, err := Build(
		[]string{"a", "b", "c", "d", "e"},
		[]Edge{{"a", "b"}, {"a", "c"}, {"b", "d"}, {"c", "d"}, {"d", "e"}},
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	want := []State{
		{Name: "a", Type: Task, Next: "a_Branches"},
		{Name: "a_Branches", Type: Parallel, Next: "d", Branches: []Branch{
			{StartAt: "b", States: []State{{Name: "b", Type: Task, End: true}}},
			{StartAt: "c", States: []State{{Name: "c", Type: Task, End: true}}},
		}},
		{Name: "d", Type: Task, Next: "e"},
		{Name: "e", Type: Task, End: true},
	}
	assertStates(t, sm.States, want)
	assertReachable(t, sm)
}

func TestBuild_MultiHopBranch(t *testing.T) {
	t.Parallel()
	// a → {b → c, d}; no convergence
	sm, err := Build(
		[]string{"a", "b", "c", "d"},
		[]Edge{{"a", "b"}, {"a", "d"}, {"b", "c"}},
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	want := []State{
		{Name: "a", Type: Task, Next: "a_Branches"},
		{Name: "a_Branches", Type: Parallel, End: true, Branches: []Branch{
			{StartAt: "b", States: []State{
				{Name: "b", Type: Task, Next: "c"},
				{Name: "c", Type: Task, End: true},
			}},
			{StartAt: "d", States: []State{{Name: "d", Type: Task, End: true}}},
		}},
	}
	assertStates(t, sm.States, want)
	assertReachable(t, sm)
}

func TestBuild_NestedFanout(t *testing.T) {
	t.Parallel()
	// a → b → {c → d, e}; c→d is the multi-hop sub-branch (this is the
	// fact_events → {dim_session → sessions_daily, top_ctas} shape).
	sm, err := Build(
		[]string{"a", "b", "c", "d", "e"},
		[]Edge{{"a", "b"}, {"b", "c"}, {"b", "e"}, {"c", "d"}},
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// b is a fanout. Expect top-level: a, b, b_Branches.
	// Inside b_Branches: branch c→d, branch e.
	want := []State{
		{Name: "a", Type: Task, Next: "b"},
		{Name: "b", Type: Task, Next: "b_Branches"},
		{Name: "b_Branches", Type: Parallel, End: true, Branches: []Branch{
			{StartAt: "c", States: []State{
				{Name: "c", Type: Task, Next: "d"},
				{Name: "d", Type: Task, End: true},
			}},
			{StartAt: "e", States: []State{{Name: "e", Type: Task, End: true}}},
		}},
	}
	assertStates(t, sm.States, want)
	assertReachable(t, sm)
}

func TestBuild_FanoutInsideBranch(t *testing.T) {
	t.Parallel()
	// Outer fanout a→{b, c}; inner fanout b→{d, e} (no convergence inside).
	// Validates that a Parallel can appear as a child state inside an
	// outer Parallel's branch.
	sm, err := Build(
		[]string{"a", "b", "c", "d", "e"},
		[]Edge{{"a", "b"}, {"a", "c"}, {"b", "d"}, {"b", "e"}},
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	want := []State{
		{Name: "a", Type: Task, Next: "a_Branches"},
		{Name: "a_Branches", Type: Parallel, End: true, Branches: []Branch{
			{StartAt: "b", States: []State{
				{Name: "b", Type: Task, Next: "b_Branches"},
				{Name: "b_Branches", Type: Parallel, End: true, Branches: []Branch{
					{StartAt: "d", States: []State{{Name: "d", Type: Task, End: true}}},
					{StartAt: "e", States: []State{{Name: "e", Type: Task, End: true}}},
				}},
			}},
			{StartAt: "c", States: []State{{Name: "c", Type: Task, End: true}}},
		}},
	}
	assertStates(t, sm.States, want)
	assertReachable(t, sm)
}

// TestBuild_CloudfrontAnalytics is the exact shape from the failing v1.1.4
// pipeline at /Users/vesa/Repositories/monorepo/analytics/cloudfront-
// analytics/cloudfront-model/orchestration.tf. Both AWS validator errors
// (fact_events_Branches + sessions_daily unreachable) reproduce here against
// the v1.1.4 HCL builder; this test pins the fix.
func TestBuild_CloudfrontAnalytics(t *testing.T) {
	t.Parallel()
	nodes := []string{
		"bronze", "enriched",
		"fact_events", "fact_page_views", "user_identity_map",
		"dim_campaign", "dim_device", "dim_geography", "dim_page",
		"dim_referer", "dim_session", "dim_status", "dim_time",
		"dim_user", "dim_website",
		"fact_page_views_resolved", "insights_website_daily",
		"sessions_daily", "top_ctas", "top_pages",
	}
	edges := []Edge{
		{"bronze", "enriched"},
		{"enriched", "fact_page_views"}, {"enriched", "fact_events"},
		{"enriched", "user_identity_map"},
		{"enriched", "dim_time"}, {"enriched", "dim_website"},
		{"enriched", "dim_page"}, {"enriched", "dim_status"},
		{"enriched", "dim_device"}, {"enriched", "dim_referer"},
		{"enriched", "dim_geography"}, {"enriched", "dim_campaign"},
		{"enriched", "dim_user"},
		{"fact_events", "dim_session"}, {"fact_events", "top_ctas"},
		{"user_identity_map", "fact_page_views_resolved"},
		{"fact_page_views", "fact_page_views_resolved"},
		{"fact_page_views_resolved", "insights_website_daily"},
		{"dim_session", "sessions_daily"},
		{"insights_website_daily", "top_pages"},
	}
	sm, err := Build(nodes, edges)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assertReachable(t, sm)

	// Pin the structural pieces that the v1.1.4 bug breaks:
	//   1. fact_events_Branches must exist INSIDE the enriched_Branches
	//      Parallel (under the fact_events branch), not at top level.
	//   2. sessions_daily must appear INSIDE the dim_session branch of
	//      fact_events_Branches, not as an orphan top-level state.
	topLevelNames := map[string]bool{}
	for _, s := range sm.States {
		topLevelNames[s.Name] = true
	}
	if topLevelNames["fact_events_Branches"] {
		t.Errorf("fact_events_Branches should NOT be a top-level state (it's the nested-fanout case)")
	}
	if topLevelNames["sessions_daily"] {
		t.Errorf("sessions_daily should NOT be a top-level state (it lives inside a Parallel branch)")
	}
	if !topLevelNames["fact_page_views_resolved"] {
		t.Errorf("fact_page_views_resolved must be a top-level state (it's the convergence of enriched_Branches)")
	}

	// Walk into enriched_Branches → fact_events branch → fact_events_Branches.
	var enrichedBranches *State
	for i := range sm.States {
		if sm.States[i].Name == "enriched_Branches" {
			enrichedBranches = &sm.States[i]
			break
		}
	}
	if enrichedBranches == nil || enrichedBranches.Type != Parallel {
		t.Fatalf("missing enriched_Branches Parallel state")
	}
	if enrichedBranches.Next != "fact_page_views_resolved" {
		t.Errorf("enriched_Branches.Next = %q, want fact_page_views_resolved", enrichedBranches.Next)
	}
	var factBranch *Branch
	for i := range enrichedBranches.Branches {
		if enrichedBranches.Branches[i].StartAt == "fact_events" {
			factBranch = &enrichedBranches.Branches[i]
		}
	}
	if factBranch == nil {
		t.Fatalf("missing fact_events branch under enriched_Branches")
	}
	branchNames := map[string]bool{}
	for _, s := range factBranch.States {
		branchNames[s.Name] = true
	}
	if !branchNames["fact_events_Branches"] {
		t.Errorf("fact_events branch must contain fact_events_Branches (nested Parallel)")
	}

	// Drill one more level: fact_events_Branches.dim_session branch must
	// contain dim_session AND sessions_daily.
	var innerParallel *State
	for i := range factBranch.States {
		if factBranch.States[i].Name == "fact_events_Branches" {
			innerParallel = &factBranch.States[i]
		}
	}
	if innerParallel == nil || innerParallel.Type != Parallel {
		t.Fatalf("fact_events_Branches not a Parallel inside fact_events branch")
	}
	var dimSessionBranch *Branch
	for i := range innerParallel.Branches {
		if innerParallel.Branches[i].StartAt == "dim_session" {
			dimSessionBranch = &innerParallel.Branches[i]
		}
	}
	if dimSessionBranch == nil {
		t.Fatalf("missing dim_session branch under fact_events_Branches")
	}
	dsNames := map[string]bool{}
	for _, s := range dimSessionBranch.States {
		dsNames[s.Name] = true
	}
	if !dsNames["dim_session"] || !dsNames["sessions_daily"] {
		t.Errorf("dim_session branch must contain BOTH dim_session and sessions_daily; got %v", dsNames)
	}
}

func TestBuild_MultipleRootsError(t *testing.T) {
	t.Parallel()
	_, err := Build([]string{"a", "b"}, []Edge{})
	if err == nil {
		t.Fatal("expected error for multiple roots, got nil")
	}
}

func TestBuild_CycleError(t *testing.T) {
	t.Parallel()
	_, err := Build([]string{"a", "b"}, []Edge{{"a", "b"}, {"b", "a"}})
	if err == nil {
		t.Fatal("expected error for cycle, got nil")
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// assertReachable mirrors AWS Step Functions' MISSING_TRANSITION_TARGET
// validator: every state in States must be either StartAt or referenced by
// another state's Next or a Branch's StartAt (transitively, recursively
// into Parallel branches).
func assertReachable(t *testing.T, sm StateMachine) {
	t.Helper()
	reachable := map[string]bool{}
	declared := collectStateNames(sm.States, "")
	var walk func(states []State, startAt string, ctx string)
	walk = func(states []State, startAt string, ctx string) {
		reachable[ctx+startAt] = true
		// Find startAt and follow Next chain; recurse into branches.
		cur := startAt
		for cur != "" {
			var s *State
			for i := range states {
				if states[i].Name == cur {
					s = &states[i]
					break
				}
			}
			if s == nil {
				return
			}
			reachable[ctx+s.Name] = true
			if s.Type == Parallel {
				for _, b := range s.Branches {
					walk(b.States, b.StartAt, ctx+s.Name+"/")
				}
			}
			cur = s.Next
		}
	}
	walk(sm.States, sm.StartAt, "")
	for name := range declared {
		if !reachable[name] {
			t.Errorf("state %q declared but unreachable", name)
		}
	}
}

func collectStateNames(states []State, ctx string) map[string]bool {
	out := map[string]bool{}
	for _, s := range states {
		out[ctx+s.Name] = true
		if s.Type == Parallel {
			for _, b := range s.Branches {
				for k, v := range collectStateNames(b.States, ctx+s.Name+"/") {
					out[k] = v
				}
			}
		}
	}
	return out
}

func assertStates(t *testing.T, got, want []State) {
	t.Helper()
	gs := dumpStates(got, "")
	ws := dumpStates(want, "")
	if gs != ws {
		t.Errorf("state mismatch:\n--- want ---\n%s\n--- got ---\n%s", ws, gs)
	}
}

func dumpStates(states []State, indent string) string {
	var b strings.Builder
	for _, s := range states {
		fmt.Fprintf(&b, "%s%s: ", indent, s.Name)
		if s.Type == Task {
			if s.End {
				b.WriteString("Task End\n")
			} else {
				fmt.Fprintf(&b, "Task → %s\n", s.Next)
			}
		} else {
			next := "End"
			if !s.End {
				next = "→ " + s.Next
			}
			fmt.Fprintf(&b, "Parallel %s\n", next)
			for _, br := range s.Branches {
				fmt.Fprintf(&b, "%s  branch StartAt=%s:\n", indent, br.StartAt)
				b.WriteString(dumpStates(br.States, indent+"    "))
			}
		}
	}
	return b.String()
}
