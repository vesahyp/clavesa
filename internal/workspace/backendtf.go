package workspace

import (
	"fmt"
	"os"
	"path/filepath"
)

// backendTFHeader is prepended to every clavesa-generated backend.tf
// (ADR-025, "Emit: one clavesa-owned backend.tf"). It is regenerated from
// clavesa.json's `backend` field on every init/upgrade, so a hand edit
// here is lost on the next run — change the manifest instead.
const backendTFHeader = `# clavesa-managed Terraform backend, generated from clavesa.json's
# "backend" field (ADR-025). Do not edit by hand: clavesa overwrites this
# file on every init/upgrade to match the manifest.

`

// RenderWorkspaceBackendTF renders the workspace root's backend.tf: the
// workspace stack's own "s3" backend, keyed by
// Backend.WorkspaceStateKey(m.Name) (ADR-025 "State keys"). Callers must
// only call this when m.Backend is non-nil.
func RenderWorkspaceBackendTF(m *Manifest) string {
	b := m.Backend
	return backendTFHeader + fmt.Sprintf(`terraform {
  backend "s3" {
    bucket       = %q
    key          = %q
    region       = %q
    encrypt      = true
    use_lockfile = true
  }
}
`, b.Bucket, b.WorkspaceStateKey(m.Name), b.Region)
}

// WriteWorkspaceBackendTF renders and writes backend.tf into the workspace
// root at root. Callers must only call this when m.Backend is non-nil.
func WriteWorkspaceBackendTF(root string, m *Manifest) error {
	return os.WriteFile(filepath.Join(root, "backend.tf"), []byte(RenderWorkspaceBackendTF(m)), 0o644)
}

// RenderPipelineBackendTF renders one pipeline's backend.tf: the
// pipeline's own "s3" backend (keyed by
// Backend.PipelineStateKey(m.Name, pipelineDir)) plus the
// `data "terraform_remote_state" "workspace"` block that reads the
// workspace stack's remote state over the same "s3" backend (ADR-025
// "Emit: one clavesa-owned backend.tf"). pipelineDir may be an absolute
// path or a bare directory name — only its base name feeds the state
// key (PipelineStateKey already does that Base() internally). Callers
// must only call this when m.Backend is non-nil.
func RenderPipelineBackendTF(m *Manifest, pipelineDir string) string {
	b := m.Backend
	return backendTFHeader + fmt.Sprintf(`terraform {
  backend "s3" {
    bucket       = %q
    key          = %q
    region       = %q
    encrypt      = true
    use_lockfile = true
  }
}

data "terraform_remote_state" "workspace" {
  backend = "s3"
  config = {
    bucket = %q
    key    = %q
    region = %q
  }
}
`, b.Bucket, b.PipelineStateKey(m.Name, pipelineDir), b.Region,
		b.Bucket, b.WorkspaceStateKey(m.Name), b.Region)
}

// WritePipelineBackendTF renders and writes backend.tf into the pipeline
// directory pipelineDir. Callers must only call this when m.Backend is
// non-nil.
func WritePipelineBackendTF(pipelineDir string, m *Manifest) error {
	return os.WriteFile(filepath.Join(pipelineDir, "backend.tf"), []byte(RenderPipelineBackendTF(m, pipelineDir)), 0o644)
}

// RefreshBackendTF rewrites an existing backend.tf in stackDir from the
// manifest, and does nothing when the stack has none. This is what every
// regen path calls (ADR-025, "Regen keeps the backend"): a stack gets its
// first backend.tf from create or migrate-state, never from regen. A
// stack whose manifest names a backend but which has not been migrated
// still carries `backend "local" {}` or a local terraform_remote_state
// block in main.tf, and writing backend.tf beside it would give terraform
// two backends. stackDir equal to root refreshes the workspace file,
// anything else a pipeline file.
func RefreshBackendTF(root, stackDir string, m *Manifest) error {
	if m == nil || m.Backend == nil {
		return nil
	}
	if _, err := os.Stat(filepath.Join(stackDir, "backend.tf")); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if stackKeyIsWorkspace(root, stackDir) {
		return WriteWorkspaceBackendTF(stackDir, m)
	}
	return WritePipelineBackendTF(stackDir, m)
}

// stackKeyIsWorkspace reports whether stackDir is the workspace root,
// compared as cleaned absolute paths.
func stackKeyIsWorkspace(root, stackDir string) bool {
	a, errA := filepath.Abs(root)
	b, errB := filepath.Abs(stackDir)
	return errA == nil && errB == nil && a == b
}
