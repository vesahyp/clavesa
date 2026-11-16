package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// A workspace whose Terraform still has null_resource.push_runner pins and
// pushes its own runner; the deploy flow must not push the binary's runner
// over its :latest (monorepo web-traffic, 2026-11-16).
func TestTerraformPushesRunner(t *testing.T) {
	cases := []struct {
		name, tf string
		want     bool
	}{
		{"old layout", "resource \"null_resource\" \"push_runner\" {\n  triggers = { runner_version = var.runner_version }\n}\n", true},
		{"indented", "  resource \"null_resource\"   \"push_runner\" {\n}\n", true},
		{"current layout", "resource \"aws_ecr_repository\" \"runner\" {\n  name = \"clavesa-x/transform-runner\"\n}\n", false},
		{"only a comment", "# the old null_resource.push_runner is gone\n", false},
		{"other null_resource", "resource \"null_resource\" \"push_runner_extra\" {\n}\n", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(c.tf), 0o644); err != nil {
				t.Fatal(err)
			}
			file, got := terraformPushesRunner(dir)
			if got != c.want {
				t.Fatalf("terraformPushesRunner = %v, want %v", got, c.want)
			}
			if got && file != "main.tf" {
				t.Fatalf("file = %q, want main.tf", file)
			}
		})
	}
	if _, got := terraformPushesRunner(t.TempDir()); got {
		t.Fatal("an empty dir reported a push_runner")
	}
}
