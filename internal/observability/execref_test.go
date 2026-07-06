package observability_test

import (
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

// TestExecRefRoundTrip pins the one canonical encoding every execution
// endpoint exchanges (GH #78): FormatExecRef and SplitExecRef must be exact
// inverses for every (dir, runID) class.
func TestExecRefRoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		dir, runID string
	}{
		{"relative dir + run", "demo", "run-abc"},
		{"absolute dir + run", "/abs/path/demo", "feedface"},
		{"dir with colon + run", "/mnt/c:/weird/demo", "run-1"},
		{"dir only (latest run)", "workdir/pipelineA", ""},
		{"bare cloud-local run id", "", "local-1746612000-abcd"},
		{"bare local run id", "", "run-9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref := observability.FormatExecRef(tc.dir, tc.runID)
			dir, run := observability.SplitExecRef(ref)
			if dir != tc.dir || run != tc.runID {
				t.Errorf("round trip (%q,%q) → %q → (%q,%q)", tc.dir, tc.runID, ref, dir, run)
			}
		})
	}
}

// TestExecRefARNPassthrough pins the cloud dispatch contract: a runID that
// is already a full SFN execution ARN must pass through unchanged so the
// cloud provider can parse it. The bug this guards: prefixing an ARN with a
// dir shifted the colon-split in StateMachineNameFromExecutionARN, so cloud
// live progress never surfaced.
func TestExecRefARNPassthrough(t *testing.T) {
	const arn = "arn:aws:states:eu-north-1:699166197771:execution:clavesa-bigagg:bcf294d6-dc5f-413f-a2f0-a103aefb22ff"
	if got := observability.FormatExecRef("bigagg", arn); got != arn {
		t.Errorf("ARN runID must pass through unchanged; got %q", got)
	}
	if observability.StateMachineNameFromExecutionARN(observability.FormatExecRef("bigagg", arn)) != "clavesa-bigagg" {
		t.Errorf("formatted ARN ref must still parse to its state machine name")
	}
	if dir, run := observability.SplitExecRef(arn); dir != "" || run != arn {
		t.Errorf("SplitExecRef(ARN) = (%q, %q), want (\"\", the ARN)", dir, run)
	}
}

// TestExecRefCanonicalShape pins the wire bytes: /pipeline/status has minted
// `local:<dir>#<runID>` since the sheet-era UI shipped, so bookmarked run
// URLs carry that exact shape.
func TestExecRefCanonicalShape(t *testing.T) {
	if got := observability.FormatExecRef("/ws/demo", "run-1"); got != "local:/ws/demo#run-1" {
		t.Errorf("canonical shape = %q, want local:/ws/demo#run-1", got)
	}
}

// TestSplitExecRefLegacyComposite — the pre-GH-78 states/logs encoding
// ("<dir>:<runID>", last-colon split) must still decode so refs minted by an
// older binary keep working.
func TestSplitExecRefLegacyComposite(t *testing.T) {
	cases := []struct {
		ref, wantDir, wantRun string
	}{
		{"demo:run-abc", "demo", "run-abc"},
		{"/abs/dir/demo:feedface", "/abs/dir/demo", "feedface"},
		{"/ws/demo:local-1746612000-abcd", "/ws/demo", "local-1746612000-abcd"},
	}
	for _, tc := range cases {
		dir, run := observability.SplitExecRef(tc.ref)
		if dir != tc.wantDir || run != tc.wantRun {
			t.Errorf("SplitExecRef(%q) = (%q, %q), want (%q, %q)", tc.ref, dir, run, tc.wantDir, tc.wantRun)
		}
	}
}

// TestSplitExecRefEmpty — the zero value decodes to the zero pair.
func TestSplitExecRefEmpty(t *testing.T) {
	if dir, run := observability.SplitExecRef(""); dir != "" || run != "" {
		t.Errorf("SplitExecRef(\"\") = (%q, %q), want empty pair", dir, run)
	}
}
