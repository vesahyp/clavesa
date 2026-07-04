// Package workspace manages Clavesa workspace metadata and initialization.
// A workspace is a directory containing an clavesa.json manifest and
// shared Terraform infrastructure (S3 bucket, ECR repository for the runner
// image) owned by all pipelines in that workspace.
package workspace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/modules"
	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/runnerreqs"
	"github.com/vesahyp/clavesa/internal/version"
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
    aws = { source = "hashicorp/aws" }
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

module "workspace" {
  # Terraform 1.x rejects bare module paths without a leading "./"
  # prefix as "ambiguous registry / local" — v1.1.6 fix. Older
  # workspaces still parse the bare form via the embedded-form
  # heuristic in hclparser, and clavesa workspace upgrade rewrites it
  # on next run.
  source         = "./.clavesa/modules/%s/workspace/aws"
  workspace_name = var.workspace_name
  system_catalog = var.system_catalog
}

resource "aws_ecr_repository" "runner" {
  name         = "clavesa-${var.workspace_name}/transform-runner"
  force_delete = true
  tags         = { "clavesa:workspace" = var.workspace_name }
}
`, moduleVersion)
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
	// (2) the per-developer warehouse file (environment.json). (3) the
	// per-developer AWS-profile selection.
	// Each line is appended only if absent, so re-running init — or
	// running it on a workspace created by an older clavesa — stays
	// idempotent and never duplicates a line.
	gitignoreEntries := []struct{ marker, snippet string }{
		{".clavesa/credentials/*.secret", "# clavesa: never commit file:-backend credential payloads\n.clavesa/credentials/*.secret\n"},
		{".clavesa/environment.json", "# clavesa: per-developer warehouse selection (local/cloud)\n.clavesa/environment.json\n"},
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

	// Seed a commented runner-requirements template so users discover the
	// extension point. Only when absent — never clobber an existing file.
	if _, err := os.Stat(runnerreqs.Path(root)); os.IsNotExist(err) {
		template := `# Extra Python pip dependencies for your transform UDFs — one per line.
# Installed into the runner image on the next build (pipeline run / workspace deploy).
# Example:
#   pyasn>=1.6
#   crawlerdetect>=0.3
`
		if err := runnerreqs.Write(root, template); err != nil {
			return fmt.Errorf("seed runner requirements: %w", err)
		}
	}

	// Extract embedded Terraform modules to .clavesa/modules/<version>/
	// so the generated workspace + pipeline .tf files resolve their `source`
	// references locally — `terraform init` makes no network call.
	if err := modules.Extract(root, moduleVersion); err != nil {
		return fmt.Errorf("extract modules: %w", err)
	}

	// Scaffold the opt-in `_maintenance` pipeline (GH #53) — a scheduled
	// transform that OPTIMIZE/VACUUMs the workspace system tables so their
	// Delta `_delta_log` stays bounded. Written to disk, not deployed; the
	// user opts in by deploying it (or deletes the directory). Needs the
	// extracted modules above for its `source` reference to resolve.
	if err := scaffoldMaintenancePipeline(root, catalog, systemCatalog, moduleVersion); err != nil {
		return fmt.Errorf("scaffold maintenance pipeline: %w", err)
	}

	// Build the local Docker image so the workspace has the image its first
	// `pipeline run` will use.
	if _, err := ensureLocalRunnerImageAt(root, moduleVersion); err != nil {
		return err
	}

	return nil
}

// workspaceModuleSourceRE matches the workspace's own `module "workspace"`
// source line. Two historical forms exist:
//
//  1. Bare local path:     `source = ".clavesa/modules/vX/workspace/aws"`
//     (v1.1.5 and earlier; Terraform 1.x rejects this).
//  2. Prefixed local path: `source = "./.clavesa/modules/vX/workspace/aws"`
//     (v1.1.6+; the valid form).
//
// Upgrade rewrites both to the prefixed form at the target version.
// Group 1 = `source = "`. Group 2 = optional `./` prefix in the existing
// line. Group 3 = current version (vX.Y.Z). Group 4 = trailing
// `/workspace/aws"`.
var workspaceModuleSourceRE = regexp.MustCompile(
	`(source\s*=\s*")(\.?/?)\.clavesa/modules/(v[^/"]+)(/workspace/aws")`)

// runnerVersionDefaultRE matches the `default = "vX.Y.Z"` line inside
// variables.tf's `runner_version` variable block, capturing the leading
// `default     = "` prefix (group 1) and the trailing closing quote
// (group 2) so Upgrade can swap only the version literal. The block init
// writes is:
//
//	variable "runner_version" {
//	  description = "..."
//	  default     = "v2.2.2"
//	}
//
// The pattern keys off the preceding `variable "runner_version"` block so
// it never touches a stray `default = "v..."` belonging to some other
// variable. The `(?s)` flag lets `.*?` span the description line.
var runnerVersionDefaultRE = regexp.MustCompile(
	`(?s)(variable\s+"runner_version"\s*\{.*?default\s*=\s*")v[^"]*(")`)

// Upgrade refreshes a workspace's Terraform-side state to a target module
// version. Pure file operations — no Docker, no network.
//
// Mechanics:
//   - Re-extracts the embedded modules tree to .clavesa/modules/<targetVersion>/
//     (idempotent — short-circuits when the SHA stamp already matches).
//   - Rewrites `module "workspace" { source = ... }` in main.tf to point at
//     the target version with the required `./` prefix.
//   - Bumps `runner_version`'s default in variables.tf to targetVersion so
//     a post-upgrade `terraform apply` pushes the new runner image (GH #8).
//     Skipped silently when variables.tf is absent.
//
// Doesn't touch clavesa.json or the user-owned parts of main.tf — those
// are the user's content from the original `init`.
// Doesn't touch the local Docker runner image — that's a separate step,
// called explicitly by the CLI wrapper via EnsureLocalRunnerImage so a
// CI / pure-Go test can exercise the TF rewrite without Docker.
//
// Returns the previous version (empty when main.tf carries no
// recognised source line) and the count of files actually rewritten
// (main.tf and/or variables.tf; zero on a full no-op).
func Upgrade(root, targetVersion string) (prevVersion string, rewritten int, err error) {
	if _, statErr := os.Stat(filepath.Join(root, manifestFile)); statErr != nil {
		return "", 0, fmt.Errorf("%s is not a clavesa workspace (no clavesa.json)", root)
	}

	// Step 1: extract modules at targetVersion. Idempotent — skips work
	// if the SHA stamp matches the embedded tree.
	if err := modules.Extract(root, targetVersion); err != nil {
		return "", 0, fmt.Errorf("extract modules: %w", err)
	}

	// Step 2: rewrite the workspace module source line. Doesn't touch
	// the rest of main.tf — the file may carry user edits (provider
	// blocks, extra resources) we don't want to clobber.
	mainPath := filepath.Join(root, "main.tf")
	data, err := os.ReadFile(mainPath)
	if err != nil {
		return "", 0, fmt.Errorf("read main.tf: %w", err)
	}
	if m := workspaceModuleSourceRE.FindSubmatch(data); m != nil {
		prevVersion = string(m[3])
	}
	newSource := []byte(fmt.Sprintf(`${1}./.clavesa/modules/%s/workspace/aws"`, targetVersion))
	updated := workspaceModuleSourceRE.ReplaceAll(data, newSource)
	if !bytes.Equal(updated, data) {
		if err := os.WriteFile(mainPath, updated, 0o644); err != nil {
			return prevVersion, rewritten, fmt.Errorf("write main.tf: %w", err)
		}
		rewritten++
	}

	// Step 3: bump runner_version's default in variables.tf so a
	// post-upgrade deploy pushes the new runner image (GH #8). A
	// workspace with no variables.tf (or one without the variable) is
	// left untouched — the rewrite is best-effort, never a hard error.
	varsPath := filepath.Join(root, "variables.tf")
	if varsData, varsErr := os.ReadFile(varsPath); varsErr == nil {
		newDefault := []byte("${1}" + targetVersion + "${2}")
		updatedVars := runnerVersionDefaultRE.ReplaceAll(varsData, newDefault)
		if !bytes.Equal(updatedVars, varsData) {
			if err := os.WriteFile(varsPath, updatedVars, 0o644); err != nil {
				return prevVersion, rewritten, fmt.Errorf("write variables.tf: %w", err)
			}
			rewritten++
		}
	}

	return prevVersion, rewritten, nil
}

// LocalRunnerImageTag returns the workspace-scoped runner image `:latest`
// tag for the workspace at root, resolved fresh from clavesa.json on each
// call (cheap — one small file read — and lazy so a workspace created after
// `clavesa ui` started is picked up without a restart). Falls back to the
// empty-workspace-name image when no manifest is readable. Use this instead
// of composing `runner.LocalImageName(...)+":latest"` inline; it does NOT
// build the image — callers that need a guaranteed-fresh image use
// EnsureLocalRunnerImage below.
func LocalRunnerImageTag(root string) string {
	name := ""
	if m, _ := Load(root); m != nil {
		name = m.Name
	}
	return runner.LocalImageName(name) + ":latest"
}

// EnsureLocalRunnerImage guarantees the workspace at `root` has a current
// local Docker runner image — built from the runner source embedded in this
// binary — and returns the `:latest` tag callers pass to `docker run`.
//
// It does not try to guess whether a rebuild is needed: it re-extracts the
// embedded runner source and calls `docker build` every time, tagging both
// `:latest` and `:<version>`. Docker's layer cache is the staleness check —
// an unchanged source tree is a full cache hit (seconds; the Dockerfile's
// expensive Java/pip/JAR layers sit above the version arg), and a CLI upgrade
// carrying new runner code rebuilds exactly the changed layers. This replaces
// a hand-rolled SHA-label/image-ID cache that kept growing gaps docker
// already handles correctly.
//
// The version tagged comes from `internal/version.Module`. `Init` injects its
// own caller-supplied version via the unexported `ensureLocalRunnerImageAt`
// helper — tests rely on injecting a specific version there.
func EnsureLocalRunnerImage(root string) (string, error) {
	return ensureLocalRunnerImageAt(root, version.Module)
}

func ensureLocalRunnerImageAt(root, moduleVersion string) (string, error) {
	m, err := Load(root)
	if err != nil {
		return "", fmt.Errorf("load workspace at %s: %w — run `clavesa workspace init` first", root, err)
	}
	localTag := runner.LocalImageName(m.Name)
	latest := localTag + ":latest"
	versioned := localTag + ":" + moduleVersion

	// Re-extract the embedded runner source so the build context matches this
	// binary, then let docker's layer cache decide what work is needed.
	// Tagging both `:latest` and `:<version>` on every build guarantees the
	// version tag the deploy preflight and the ECR push provisioner depend on
	// always exists and is always current.
	runnerDir := filepath.Join(root, "runner")
	if err := extractRunnerFiles(runnerDir); err != nil {
		return "", fmt.Errorf("extract runner source: %w", err)
	}
	// Stage the user's extra Python deps into the build context. The runner
	// Dockerfile has a `COPY extra-requirements.txt` layer, so the file must
	// always exist; extractRunnerFiles just wrote the embedded empty default.
	// Overwrite it with the workspace's runner-requirements.txt content (empty
	// when absent — best-effort, a read error degrades to the empty default
	// rather than blocking the build).
	extraReqs, _ := runnerreqs.Read(root)
	if err := os.WriteFile(filepath.Join(runnerDir, "extra-requirements.txt"), []byte(extraReqs), 0o644); err != nil {
		return "", fmt.Errorf("stage runner requirements: %w", err)
	}
	cmd := exec.Command("docker", "build",
		"--build-arg", "CLAVESA_MODULE_VERSION="+moduleVersion,
		"-t", versioned, "-t", latest, runnerDir)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build runner image: %w", err)
	}
	return latest, nil
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
