package workspace

import (
	"strings"
	"testing"
)

func TestPipelineAWSName(t *testing.T) {
	legacy := &Manifest{Name: "acme"}
	ws := &Manifest{Name: "acme", AWSNames: AWSNamesWorkspace}
	if got := PipelineAWSName(nil, "orders"); got != "clavesa-orders" {
		t.Errorf("nil manifest = %q", got)
	}
	if got := PipelineAWSName(legacy, "orders"); got != "clavesa-orders" {
		t.Errorf("legacy = %q", got)
	}
	// Legacy names are never cut, however long.
	long := strings.Repeat("p", 80)
	if got := PipelineAWSName(legacy, long); got != "clavesa-"+long {
		t.Errorf("legacy long = %q", got)
	}
	if got := PipelineAWSName(ws, "orders"); got != "clavesa-acme-orders" {
		t.Errorf("workspace = %q", got)
	}
	// Exactly 40 characters is kept as is.
	p40 := strings.Repeat("p", 40-len("clavesa-acme-"))
	if got := PipelineAWSName(ws, p40); len(got) != 40 || got != "clavesa-acme-"+p40 {
		t.Errorf("40-char base = %q", got)
	}
	// Longer: 31 chars + "-" + 8 hex = 40, stable, and distinct per input.
	a := PipelineAWSName(ws, p40+"a")
	b := PipelineAWSName(ws, p40+"b")
	if len(a) != 40 || a[31] != '-' || a == b {
		t.Errorf("truncated names: %q %q", a, b)
	}
	if a != PipelineAWSName(ws, p40+"a") {
		t.Errorf("truncation is not stable")
	}
	if !strings.HasPrefix(a, "clavesa-acme-") {
		t.Errorf("truncated name lost its prefix: %q", a)
	}
}

func TestInitWritesWorkspaceAWSNames(t *testing.T) {
	root := t.TempDir()
	if err := Init(root, "acme", "aws", "", "v0.0.0", nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	m, err := Load(root)
	if err != nil || m == nil {
		t.Fatalf("Load: %v", err)
	}
	if m.AWSNames != AWSNamesWorkspace {
		t.Errorf("AWSNames = %q, want workspace", m.AWSNames)
	}
}
