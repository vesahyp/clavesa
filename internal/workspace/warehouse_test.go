package workspace

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadWarehouse(t *testing.T) {
	tests := []struct {
		name    string
		content string // "" = don't write the file at all
		want    Warehouse
	}{
		{"absent file → local", "", WarehouseLocal},
		{"explicit local → local", `{"warehouse":"local"}`, WarehouseLocal},
		{"explicit cloud → cloud", `{"warehouse":"cloud"}`, WarehouseCloud},
		{"legacy mode key local → local", `{"mode":"local"}`, WarehouseLocal},
		{"legacy mode key cloud → cloud", `{"mode":"cloud"}`, WarehouseCloud},
		{"warehouse wins over mode", `{"warehouse":"cloud","mode":"local"}`, WarehouseCloud},
		{"unknown value → local", `{"warehouse":"banana"}`, WarehouseLocal},
		{"unknown legacy value → local", `{"mode":"banana"}`, WarehouseLocal},
		{"malformed json → local", `{not json`, WarehouseLocal},
		{"empty object → local", `{}`, WarehouseLocal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			if tt.content != "" {
				if err := os.MkdirAll(filepath.Join(root, ".clavesa"), 0o755); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				if err := os.WriteFile(EnvironmentFilePath(root), []byte(tt.content), 0o644); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			if got := LoadWarehouse(root); got != tt.want {
				t.Errorf("LoadWarehouse = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseWarehouse(t *testing.T) {
	tests := []struct {
		in     string
		want   Warehouse
		wantOK bool
	}{
		{"local", WarehouseLocal, true},
		{"cloud", WarehouseCloud, true},
		{"", WarehouseLocal, false},
		{"Local", WarehouseLocal, false},
		{"banana", WarehouseLocal, false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, ok := ParseWarehouse(tt.in)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("ParseWarehouse(%q) = (%q, %v), want (%q, %v)", tt.in, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestWriteWarehouse(t *testing.T) {
	root := t.TempDir() // .clavesa/ does not exist yet

	if err := WriteWarehouse(root, WarehouseCloud); err != nil {
		t.Fatalf("WriteWarehouse: %v", err)
	}
	if got := LoadWarehouse(root); got != WarehouseCloud {
		t.Errorf("round-trip cloud: got %q", got)
	}

	if err := WriteWarehouse(root, WarehouseLocal); err != nil {
		t.Fatalf("WriteWarehouse: %v", err)
	}
	if got := LoadWarehouse(root); got != WarehouseLocal {
		t.Errorf("round-trip local: got %q", got)
	}

	// An unrecognized value is coerced to local.
	if err := WriteWarehouse(root, Warehouse("banana")); err != nil {
		t.Fatalf("WriteWarehouse: %v", err)
	}
	if got := LoadWarehouse(root); got != WarehouseLocal {
		t.Errorf("coerce unknown: got %q", got)
	}
}

// TestWarehouseURILocal — a local-warehouse workspace (default, no
// environment.json needed) resolves to the shared local Hadoop-catalog dir.
func TestWarehouseURILocal(t *testing.T) {
	root := t.TempDir()
	got, err := WarehouseURI(root)
	if err != nil {
		t.Fatalf("WarehouseURI: %v", err)
	}
	if got != LocalWarehouseDir(root) {
		t.Errorf("WarehouseURI = %q, want %q", got, LocalWarehouseDir(root))
	}
}

// TestWarehouseURICloud — a cloud-warehouse workspace with a deployed
// shell resolves to the workspace warehouse prefix on the pipeline bucket.
func TestWarehouseURICloud(t *testing.T) {
	root := t.TempDir()
	if err := WriteWarehouse(root, WarehouseCloud); err != nil {
		t.Fatalf("WriteWarehouse: %v", err)
	}
	tfstate := `{
  "version": 4,
  "outputs": {
    "pipeline_bucket": { "value": "clavesa-demo-12345", "type": "string" }
  }
}`
	if err := os.WriteFile(filepath.Join(root, "terraform.tfstate"), []byte(tfstate), 0o644); err != nil {
		t.Fatalf("write tfstate: %v", err)
	}
	got, err := WarehouseURI(root)
	if err != nil {
		t.Fatalf("WarehouseURI: %v", err)
	}
	if want := "s3://clavesa-demo-12345/_workspace/_warehouse/"; got != want {
		t.Errorf("WarehouseURI = %q, want %q", got, want)
	}
}

// TestWarehouseURICloudUndeployed — cloud warehouse with no tfstate is a
// hard, actionable error (never a silent fallback to local).
func TestWarehouseURICloudUndeployed(t *testing.T) {
	root := t.TempDir()
	if err := WriteWarehouse(root, WarehouseCloud); err != nil {
		t.Fatalf("WriteWarehouse: %v", err)
	}
	got, err := WarehouseURI(root)
	if err == nil {
		t.Fatalf("WarehouseURI = %q, want error", got)
	}
	if !errors.Is(err, ErrWarehouseUndeployed) {
		t.Errorf("error %v does not wrap ErrWarehouseUndeployed", err)
	}
	if !strings.Contains(err.Error(), "clavesa workspace deploy") {
		t.Errorf("error %v lacks the actionable hint", err)
	}
}

// TestWriteWarehouseDualKeys — the file carries both the "warehouse" key
// and the legacy "mode" key with the same value, so a pre-ADR-024 binary
// (which reads only "mode") still honours the setting.
func TestWriteWarehouseDualKeys(t *testing.T) {
	root := t.TempDir()
	if err := WriteWarehouse(root, WarehouseCloud); err != nil {
		t.Fatalf("WriteWarehouse: %v", err)
	}
	data, err := os.ReadFile(EnvironmentFilePath(root))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc map[string]string
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if doc["warehouse"] != "cloud" || doc["mode"] != "cloud" {
		t.Errorf("dual keys = %v, want warehouse=cloud and mode=cloud", doc)
	}
}
