// Package workspace manages Clavesa workspace metadata and initialization.
// A workspace is a directory containing an clavesa.json manifest and
// shared Terraform infrastructure (S3 bucket, ECR repository for the runner
// image) owned by all pipelines in that workspace.
package workspace

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/modules"
	"github.com/vesahyp/clavesa/internal/runner"
)

const manifestFile = "clavesa.json"
const manifestVersion = 1

// warehouseMigratedMarker sits beside the shared warehouse once the
// one-shot per-pipeline → workspace-shared relocation has completed. Its
// presence short-circuits migrateLocalWarehouses on every later Load.
const warehouseMigratedMarker = ".warehouse-migrated"

// LocalWarehouseDir returns the workspace-shared Iceberg Hadoop-catalog
// warehouse path. One warehouse per workspace holds every local pipeline's
// tables under separate `<catalog>__<schema>` namespaces, mirroring the
// cloud model (one S3 bucket per workspace, Glue DB per schema). This is
// what makes cross-pipeline reads work on `compute = "local"` — the
// consumer's Spark catalog sees the producer's tables because they live
// in the same warehouse.
func LocalWarehouseDir(workspaceRoot string) string {
	return filepath.Join(workspaceRoot, ".clavesa", "warehouse")
}

// Manifest is the on-disk workspace metadata stored in clavesa.json.
type Manifest struct {
	Name    string `json:"name"`
	Cloud   string `json:"cloud"`
	Version int    `json:"version"`
	// Catalog is the workspace's three-level-namespace catalog identifier
	// (ADR-016). Always present after Load (auto-migrated from legacy
	// manifests without it). Init writes this field for every new
	// workspace; CatalogIdentifier returns it as-is.
	Catalog string `json:"catalog"`
	// SystemCatalog is the workspace-owned catalog that holds observability
	// tables — `runs`, `node_runs`, `tables` under the `pipelines` schema,
	// future `query.history` / `billing.run_costs` / `access.audit` under
	// their respective schemas. Mirrors Databricks Unity Catalog's
	// account-level `system` catalog. Always present after Load
	// (auto-derived from Catalog for legacy manifests). Multi-writer:
	// every pipeline in the workspace appends to its `pipelines` schema,
	// distinguished by the `pipeline` column — the slice 4 schema
	// ownership validator exempts this catalog.
	SystemCatalog string `json:"system_catalog"`
}

// CatalogIdentifier returns the workspace's catalog name. Always
// non-empty after Load: legacy workspaces (pre-v0.18.0 clavesa.json
// without a `catalog` field) get auto-populated with the default and
// the manifest is rewritten to disk on first read — see Load.
func (m *Manifest) CatalogIdentifier() string {
	return m.Catalog
}

// SystemCatalogIdentifier returns the workspace's system-catalog name.
// Always non-empty after Load — auto-derived from CatalogIdentifier()
// for legacy manifests, persisted on first read.
func (m *Manifest) SystemCatalogIdentifier() string {
	return m.SystemCatalog
}

// DefaultCatalog computes the default workspace catalog identifier for
// a workspace name. Init persists this on first creation; Load
// auto-populates it for legacy manifests that predate the field.
func DefaultCatalog(workspaceName string) string {
	return "clavesa_" + identutil.Sanitize(workspaceName)
}

// DefaultSystemCatalog returns the default system-catalog identifier
// derived from a user-catalog identifier. Append `_system` so the
// pairing is visible from either name alone — `clavesa_demo_ws` ↔
// `clavesa_demo_ws_system`.
func DefaultSystemCatalog(catalog string) string {
	return catalog + "_system"
}

// Init initializes a new workspace in root, writing clavesa.json,
// creating _workspace/ with Terraform files, extracting the runner source,
// and building the local Docker image for preview.
//
// catalog is the three-level-namespace catalog identifier (ADR-016).
// Empty falls back to DefaultCatalog(name); pass an explicit value when
// the user wants display name = identifier (e.g., `--catalog clavesa`
// for the legacy default, or a custom org-prefix scheme).
func Init(root, name, cloud, catalog, moduleVersion string) error {
	if cloud == "" {
		cloud = "aws"
	}
	if catalog == "" {
		catalog = DefaultCatalog(name)
	}
	systemCatalog := DefaultSystemCatalog(catalog)

	// `init` is the workspace-creation command — making the user mkdir the
	// directory first turns every cookbook into "and don't forget mkdir -p".
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("create workspace dir: %w", err)
	}

	// Write clavesa.json
	m := Manifest{Name: name, Cloud: cloud, Version: manifestVersion, Catalog: catalog, SystemCatalog: systemCatalog}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(root, manifestFile), append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write clavesa.json: %w", err)
	}

	// Write main.tf
	mainTF := fmt.Sprintf(`terraform {
  required_providers {
    aws  = { source = "hashicorp/aws" }
    null = { source = "hashicorp/null" }
  }
  backend "local" {}
}

provider "aws" {
  # Workspace-wide default tags. Every AWS resource created by every
  # module under this workspace gets these — Cost Explorer can then
  # group spend by clavesa:workspace once the keys are activated as
  # cost-allocation tags in Billing. Per-resource tags merge on top.
  default_tags {
    tags = {
      "clavesa:workspace"  = var.workspace_name
      "clavesa:managed-by" = "clavesa"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

module "workspace" {
  source         = ".clavesa/modules/%s/workspace/aws"
  workspace_name = var.workspace_name
  system_catalog = var.system_catalog
}

resource "aws_ecr_repository" "runner" {
  name         = "clavesa-${var.workspace_name}/transform-runner"
  force_delete = true
  tags         = { "clavesa:workspace" = var.workspace_name }
}

resource "null_resource" "push_runner" {
  triggers = { runner_version = var.runner_version }

  provisioner "local-exec" {
    command = <<-EOT
      aws ecr get-login-password --region ${data.aws_region.current.region} | \
        docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.region}.amazonaws.com
      docker tag %s:${var.runner_version} ${aws_ecr_repository.runner.repository_url}:${var.runner_version}
      docker tag %s:${var.runner_version} ${aws_ecr_repository.runner.repository_url}:latest
      docker push ${aws_ecr_repository.runner.repository_url}:${var.runner_version}
      docker push ${aws_ecr_repository.runner.repository_url}:latest
    EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = "docker rmi %s:${self.triggers.runner_version} %s:latest 2>/dev/null || true"
  }
}
`, moduleVersion, runner.LocalImageName(name), runner.LocalImageName(name), runner.LocalImageName(name), runner.LocalImageName(name))
	if err := os.WriteFile(filepath.Join(root, "main.tf"), []byte(mainTF), 0o644); err != nil {
		return fmt.Errorf("write main.tf: %w", err)
	}

	// Write variables.tf
	variablesTF := fmt.Sprintf(`variable "workspace_name" {
  description = "Unique name for this workspace. Used as prefix for shared AWS resources."
  default     = %q
}

variable "runner_version" {
  description = "Transform runner image version tag (must be built locally before apply)."
  default     = %q
}

variable "system_catalog" {
  description = "Workspace-owned observability catalog (ADR-016). Hosts runs/node_runs/tables under the 'pipelines' schema."
  default     = %q
}
`, name, moduleVersion, systemCatalog)
	if err := os.WriteFile(filepath.Join(root, "variables.tf"), []byte(variablesTF), 0o644); err != nil {
		return fmt.Errorf("write variables.tf: %w", err)
	}

	// Write outputs.tf
	outputsTF := `output "pipeline_bucket" {
  description = "S3 bucket name shared by all pipelines in this workspace."
  value       = module.workspace.pipeline_bucket
}

output "runner_image" {
  description = "ECR URI for the transform runner image (use as runner_image in pipeline modules)."
  value       = "${aws_ecr_repository.runner.repository_url}:latest"
}

output "system_catalog" {
  description = "Workspace-owned observability catalog (ADR-016). Pipelines read this via remote_state to point runs/node_runs/tables writes at the workspace's system Glue DB."
  value       = module.workspace.system_catalog
}
`
	if err := os.WriteFile(filepath.Join(root, "outputs.tf"), []byte(outputsTF), 0o644); err != nil {
		return fmt.Errorf("write outputs.tf: %w", err)
	}

	// clavesa's workspace init contributes three .gitignore entries —
	// the rest of the file is the user's. (1) ADR-017 slice 2:
	// file:-backed credential payloads live next to the registry as
	// plaintext and must never be committed (the credential JSON spec
	// itself is fine to commit; only the .secret payload is ignored).
	// (2) the per-developer environment-mode file. (3) the
	// per-developer AWS-profile selection.
	// Each line is appended only if absent, so re-running init — or
	// running it on a workspace created by an older clavesa — stays
	// idempotent and never duplicates a line.
	gitignoreEntries := []struct{ marker, snippet string }{
		{".clavesa/credentials/*.secret", "# clavesa: never commit file:-backend credential payloads\n.clavesa/credentials/*.secret\n"},
		{".clavesa/environment.json", "# clavesa: per-developer environment mode (local/cloud lens)\n.clavesa/environment.json\n"},
		{".clavesa/aws-profile.json", "# clavesa: per-developer AWS profile selection\n.clavesa/aws-profile.json\n"},
	}
	gitignorePath := filepath.Join(root, ".gitignore")
	existing, _ := os.ReadFile(gitignorePath)
	merged := string(existing)
	changed := false
	for _, e := range gitignoreEntries {
		if strings.Contains(merged, e.marker) {
			continue
		}
		if merged != "" && !strings.HasSuffix(merged, "\n") {
			merged += "\n"
		}
		merged += e.snippet
		changed = true
	}
	if changed {
		if err := os.WriteFile(gitignorePath, []byte(merged), 0o644); err != nil {
			return fmt.Errorf("write .gitignore: %w", err)
		}
	}

	// Extract runner source to runner/
	runnerDir := filepath.Join(root, "runner")
	if err := extractRunnerFiles(runnerDir); err != nil {
		return fmt.Errorf("extract runner source: %w", err)
	}

	// Extract embedded Terraform modules to .clavesa/modules/<version>/
	// so the generated workspace + pipeline .tf files resolve their `source`
	// references locally — `terraform init` makes no network call.
	if err := modules.Extract(root, moduleVersion); err != nil {
		return fmt.Errorf("extract modules: %w", err)
	}

	// Build the local Docker image for preview — skip only when the cached
	// image was built from byte-identical embedded runner source (matched via
	// the clavesa.runner_sha label). Without the label match, edits to
	// runner.py would silently continue serving stale images.
	localTag := runner.LocalImageName(name)
	imageTag := localTag + ":" + moduleVersion
	wantSHA, shaErr := runner.EmbeddedSHA()
	if shaErr != nil {
		return fmt.Errorf("compute embedded runner sha: %w", shaErr)
	}
	if !imageMatchesSHA(imageTag, wantSHA) {
		// Try to retag from any image whose runner_sha label already matches
		// — the typical case is the user's own previously-built fresh image
		// at `clavesa/transform-runner:latest` (from `make build-runner`)
		// or this same workspace's `:latest` from a prior init. Old/unlabeled
		// images are rejected so stale code never gets re-tagged forward.
		retagSources := []string{
			"clavesa/transform-runner:" + moduleVersion,
			"clavesa/transform-runner:latest",
			localTag + ":latest",
		}
		retagged := false
		for _, src := range retagSources {
			if !imageMatchesSHA(src, wantSHA) {
				continue
			}
			if err := exec.Command("docker", "tag", src, imageTag).Run(); err != nil {
				continue
			}
			_ = exec.Command("docker", "tag", src, localTag+":latest").Run()
			retagged = true
			break
		}
		if !retagged {
			cmd := exec.Command("docker", "build",
				"--label", runner.SHALabel+"="+wantSHA,
				"--build-arg", "CLAVESA_MODULE_VERSION="+moduleVersion,
				"-t", imageTag, "-t", localTag+":latest", runnerDir)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("build runner image: %w", err)
			}
		}
	}

	return nil
}

// VerifyRunnerImage checks that the local runner image for `workspaceName`
// at `version` carries the same `clavesa.runner_sha` label as the
// runner files embedded in this binary. Used as a deploy preflight: the
// workspace's `null_resource.push_runner` provisioner retags whatever's
// at the local tag and pushes to ECR, so a stale or missing image
// silently lands in production unless the caller refuses to invoke
// terraform.
func VerifyRunnerImage(workspaceName, version string) error {
	wantSHA, err := runner.EmbeddedSHA()
	if err != nil {
		return fmt.Errorf("compute embedded runner sha: %w", err)
	}
	ref := runner.LocalImageName(workspaceName) + ":" + version
	if !imageMatchesSHA(ref, wantSHA) {
		return fmt.Errorf("local runner image %s does not match the embedded runner SHA %s — re-run `clavesa workspace init` to rebuild before deploying", ref, wantSHA)
	}
	return nil
}

// imageMatchesSHA returns true iff `ref` exists locally AND carries the
// expected clavesa.runner_sha label. Missing image, missing label, or
// label mismatch all return false — the caller falls through to either the
// next retag candidate or a fresh build.
func imageMatchesSHA(ref, wantSHA string) bool {
	out, err := exec.Command("docker", "image", "inspect",
		"--format", "{{ index .Config.Labels \""+runner.SHALabel+"\" }}", ref).Output()
	if err != nil {
		return false
	}
	got := string(out)
	// Strip trailing newline; docker inspect always appends one.
	if len(got) > 0 && got[len(got)-1] == '\n' {
		got = got[:len(got)-1]
	}
	return got == wantSHA
}

// extractRunnerFiles copies the embedded runner source to dest.
func extractRunnerFiles(dest string) error {
	return fs.WalkDir(runner.FS, "files", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel("files", path)
		target := filepath.Join(dest, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		src, err := runner.FS.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.Create(target)
		if err != nil {
			return err
		}
		defer dst.Close()
		// Preserve executable bit for entrypoint.sh
		if filepath.Base(target) == "entrypoint.sh" {
			if err := dst.Chmod(0o755); err != nil {
				return err
			}
		}
		_, err = io.Copy(dst, src)
		return err
	})
}

// Load reads the workspace manifest from root. Returns nil, nil if
// clavesa.json does not exist (backward-compatible: directory is a
// legacy workspace without metadata).
//
// Auto-migrates pre-v0.18.0 manifests that lack a `catalog` field by
// populating it with DefaultCatalog(name) and rewriting the file. The
// only production user (cloudfront-analytics) was migrated manually
// before this code shipped; the auto-migration is a safety net for any
// straggler. Failure to write back doesn't block Load — the in-memory
// value is still useful, and the caller will encounter the missing
// field again on the next run.
func Load(root string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(root, manifestFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read clavesa.json: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse clavesa.json: %w", err)
	}
	dirty := false
	if m.Catalog == "" {
		m.Catalog = DefaultCatalog(m.Name)
		dirty = true
	}
	if m.SystemCatalog == "" {
		m.SystemCatalog = DefaultSystemCatalog(m.Catalog)
		dirty = true
	}
	if dirty {
		// Best-effort persist. Errors here log to stderr and let the
		// caller proceed with the in-memory default.
		if rewritten, mErr := json.MarshalIndent(m, "", "  "); mErr == nil {
			if wErr := os.WriteFile(filepath.Join(root, manifestFile), append(rewritten, '\n'), 0o644); wErr != nil {
				fmt.Fprintf(os.Stderr, "workspace: auto-migrate clavesa.json failed: %v\n", wErr)
			}
		}
	}
	migrateLocalWarehouses(root)
	return &m, nil
}

// migrateLocalWarehouses relocates pre-existing per-pipeline Iceberg
// warehouses (`<pipelineDir>/.clavesa/warehouse/`) into the single
// workspace-shared warehouse (`LocalWarehouseDir`). Before the shared
// warehouse landed, each local pipeline kept its own; cross-pipeline reads
// couldn't see sibling tables. This one-shot move makes existing workspaces
// pick up the new layout without losing user data.
//
// Best-effort, idempotent, never blocks Load — same contract as the
// clavesa.json auto-migration above. Errors log to stderr.
//
//   - User-schema namespaces (`<catalog>__<schema>`) are unique per pipeline
//     and move cleanly.
//   - The system-catalog namespace (`<system_catalog>__pipelines`, holding
//     node_runs/runs/tables) exists in every pipeline's warehouse. Iceberg
//     tables can't be merged on disk, so the first one wins: a target that
//     already exists is left alone. Other pipelines' pre-migration run
//     history is dropped — observability telemetry rebuilds on the next run.
func migrateLocalWarehouses(root string) {
	marker := filepath.Join(root, ".clavesa", warehouseMigratedMarker)
	if _, err := os.Stat(marker); err == nil {
		return // already migrated
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	shared := LocalWarehouseDir(root)
	clean := true
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		oldWh := filepath.Join(root, e.Name(), ".clavesa", "warehouse")
		nsDirs, err := os.ReadDir(oldWh)
		if err != nil {
			continue // pipeline never ran locally, or no warehouse — skip
		}
		for _, ns := range nsDirs {
			if !ns.IsDir() {
				continue
			}
			src := filepath.Join(oldWh, ns.Name())
			dst := filepath.Join(shared, ns.Name())
			if _, err := os.Stat(dst); err == nil {
				// Target exists: a sibling pipeline already supplied this
				// namespace (the system-catalog keep-first case). Leave the
				// stale copy in place — harmless under gitignored .clavesa/.
				continue
			}
			if err := os.MkdirAll(shared, 0o755); err != nil {
				fmt.Fprintf(os.Stderr, "workspace: warehouse migration: %v\n", err)
				clean = false
				continue
			}
			if err := os.Rename(src, dst); err != nil {
				fmt.Fprintf(os.Stderr, "workspace: warehouse migration: relocate %s: %v\n", src, err)
				clean = false
			}
		}
	}
	if clean {
		if err := os.MkdirAll(filepath.Dir(marker), 0o755); err == nil {
			_ = os.WriteFile(marker, []byte("local Iceberg warehouse relocated to the workspace-shared path\n"), 0o644)
		}
	}
}
