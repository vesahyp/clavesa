package workspace_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

func TestInitAndLoad(t *testing.T) {
	dir := t.TempDir()

	if err := workspace.Init(dir, "my-test", "aws", "", "v0.5.0"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	m, err := workspace.Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m == nil {
		t.Fatal("Load returned nil, expected manifest")
	}
	if m.Name != "my-test" {
		t.Errorf("Name = %q, want %q", m.Name, "my-test")
	}
	if m.Cloud != "aws" {
		t.Errorf("Cloud = %q, want %q", m.Cloud, "aws")
	}
	if m.Version != 1 {
		t.Errorf("Version = %d, want 1", m.Version)
	}
	// Default catalog persisted: clavesa_<sanitize(name)> with my-test → my_test.
	if m.Catalog != "clavesa_my_test" {
		t.Errorf("Catalog = %q, want %q", m.Catalog, "clavesa_my_test")
	}
	if got := m.CatalogIdentifier(); got != "clavesa_my_test" {
		t.Errorf("CatalogIdentifier() = %q, want %q", got, "clavesa_my_test")
	}

	// main.tf exists
	if _, err := os.Stat(filepath.Join(dir, "main.tf")); err != nil {
		t.Errorf("main.tf: %v", err)
	}
	// variables.tf exists
	if _, err := os.Stat(filepath.Join(dir, "variables.tf")); err != nil {
		t.Errorf("variables.tf: %v", err)
	}
	// outputs.tf exists
	if _, err := os.Stat(filepath.Join(dir, "outputs.tf")); err != nil {
		t.Errorf("outputs.tf: %v", err)
	}
}

func TestInitCreatesMissingDir(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "not-yet-created")

	if err := workspace.Init(target, "fresh-ws", "aws", "", "v0.5.0"); err != nil {
		t.Fatalf("Init against non-existent dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(target, "clavesa.json")); err != nil {
		t.Errorf("clavesa.json under created dir: %v", err)
	}
}

func TestLoadMissing(t *testing.T) {
	dir := t.TempDir()

	m, err := workspace.Load(dir)
	if err != nil {
		t.Fatalf("Load on empty dir: %v", err)
	}
	if m != nil {
		t.Errorf("Load returned non-nil manifest for directory without clavesa.json")
	}
}

func TestInitDefaultCloud(t *testing.T) {
	dir := t.TempDir()

	if err := workspace.Init(dir, "test-ws", "", "", "v0.5.0"); err != nil {
		t.Fatalf("Init with empty cloud: %v", err)
	}

	m, err := workspace.Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m.Cloud != "aws" {
		t.Errorf("Cloud = %q, want \"aws\" (default)", m.Cloud)
	}
}

func TestLoadAutoMigratesLegacyManifest(t *testing.T) {
	// A manifest written before v0.18.0 has no `catalog` field. Load
	// auto-populates it with DefaultCatalog(name) and rewrites the file
	// so subsequent reads (and the rest of the codebase, which now
	// requires a non-empty catalog) work.
	dir := t.TempDir()
	legacy := []byte(`{"name":"old-ws","cloud":"aws","version":1}` + "\n")
	if err := os.WriteFile(filepath.Join(dir, "clavesa.json"), legacy, 0o644); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}

	m, err := workspace.Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m == nil {
		t.Fatal("Load returned nil")
	}
	if m.Catalog != "clavesa_old_ws" {
		t.Errorf("Catalog = %q, want %q (auto-migrated default)", m.Catalog, "clavesa_old_ws")
	}
	if got := m.CatalogIdentifier(); got != "clavesa_old_ws" {
		t.Errorf("CatalogIdentifier() = %q, want %q", got, "clavesa_old_ws")
	}

	// Verify the manifest on disk now carries the field.
	data, err := os.ReadFile(filepath.Join(dir, "clavesa.json"))
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if !strings.Contains(string(data), `"catalog": "clavesa_old_ws"`) {
		t.Errorf("clavesa.json was not auto-migrated to add catalog field:\n%s", data)
	}
}

func TestDefaultCatalog(t *testing.T) {
	// DefaultCatalog is the helper Init uses to compute the catalog
	// identifier for NEW workspaces. Legacy workspaces never use it
	// (CatalogIdentifier() returns "" instead of synthesizing this).
	cases := []struct {
		name, want string
	}{
		{"demo", "clavesa_demo"},
		{"demo-ws", "clavesa_demo_ws"},
		{"cloudfront-analytics", "clavesa_cloudfront_analytics"},
	}
	for _, c := range cases {
		if got := workspace.DefaultCatalog(c.name); got != c.want {
			t.Errorf("DefaultCatalog(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestInitWithExplicitCatalog(t *testing.T) {
	dir := t.TempDir()
	if err := workspace.Init(dir, "demo-ws", "aws", "clavesa", "v0.5.0"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	m, err := workspace.Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Explicit override (the legacy "clavesa" catalog) is persisted as-is,
	// not normalized via DefaultCatalog. The sanitization rule is for
	// derived defaults; an explicit value is the user's choice.
	if m.Catalog != "clavesa" {
		t.Errorf("Catalog = %q, want %q (explicit override)", m.Catalog, "clavesa")
	}
	if got := m.CatalogIdentifier(); got != "clavesa" {
		t.Errorf("CatalogIdentifier() = %q, want %q", got, "clavesa")
	}
}

func TestInitManifestContents(t *testing.T) {
	dir := t.TempDir()

	if err := workspace.Init(dir, "my-analytics", "aws", "", "v0.5.0"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Verify module version appears in main.tf
	data, err := os.ReadFile(filepath.Join(dir, "main.tf"))
	if err != nil {
		t.Fatalf("read main.tf: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "v0.5.0") {
		t.Errorf("main.tf does not contain module version v0.5.0:\n%s", content)
	}
	if !strings.Contains(content, "workspace_name") {
		t.Errorf("main.tf does not reference workspace_name:\n%s", content)
	}
}
