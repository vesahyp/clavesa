package service

import "testing"

// TestCanonicalTableSegment pins the shared bare/suffixed rule that every
// canonical-name path (intra-edge wiring, backfill staging/diff/promote)
// routes through. It must mirror runner/runner.py::_table_id_for exactly.
//
// This is the root cause of issue #9: a default-only transform writes a BARE
// `<node>` Delta table, but the backfill sidecar reconstructed `<node>__default`,
// so every follow-up backfill diff/promote/discard/list failed with
// run_id-not-found. Both canonicalTargetFor and canonicalFromLambdaEnv compose
// their `<db>.<segment>` result from this helper, so proving "bare" for the
// default-only case and "suffixed" for the multi-output case covers the fix on
// both backfill resolution paths.
func TestCanonicalTableSegment(t *testing.T) {
	cases := []struct {
		name string
		defs map[string]interface{}
		key  string
		want string
	}{
		// Default-only transform → BARE <node> (the #9 fix).
		{"nil defs, default key -> bare", nil, "default", "trips"},
		{"empty defs, default key -> bare", map[string]interface{}{}, "default", "trips"},
		{"single default -> bare", map[string]interface{}{"default": struct{}{}}, "default", "trips"},
		// Multi-output transform → <node>__<key>.
		{"multi-output, default key -> suffixed", map[string]interface{}{"default": struct{}{}, "errors": struct{}{}}, "default", "trips__default"},
		{"multi-output, non-default key -> suffixed", map[string]interface{}{"default": struct{}{}, "errors": struct{}{}}, "errors", "trips__errors"},
		{"single non-default -> suffixed", map[string]interface{}{"errors": struct{}{}}, "errors", "trips__errors"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := canonicalTableSegment("trips", c.defs, c.key); got != c.want {
				t.Errorf("canonicalTableSegment(%v, %q) = %q, want %q", c.defs, c.key, got, c.want)
			}
		})
	}
}

// TestDefaultOnlyOutputs covers the predicate canonicalTableSegment and
// producedTableIndex both use to decide bare-vs-suffixed.
func TestDefaultOnlyOutputs(t *testing.T) {
	cases := []struct {
		name string
		defs map[string]interface{}
		want bool
	}{
		{"nil", nil, true},
		{"empty", map[string]interface{}{}, true},
		{"single default", map[string]interface{}{"default": struct{}{}}, true},
		{"single non-default", map[string]interface{}{"errors": struct{}{}}, false},
		{"two outputs incl default", map[string]interface{}{"default": struct{}{}, "errors": struct{}{}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := defaultOnlyOutputs(c.defs); got != c.want {
				t.Errorf("defaultOnlyOutputs(%v) = %v, want %v", c.defs, got, c.want)
			}
		})
	}
}
