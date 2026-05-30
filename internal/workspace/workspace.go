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

	// Build (or retag) the local Docker image for preview so the workspace
	// has the image its first `pipeline run` will use.
	if _, _, err := ensureLocalRunnerImageAt(root, moduleVersion); err != nil {
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

// EnsureLocalRunnerImage guarantees the workspace at `root` has a local
// Docker runner image whose `clavesa.runner_sha` label matches the
// runner files embedded in this binary (or, if a fresher
// `clavesa/transform-runner:<version>` / `:latest` exists in the dev
// tree, the SHA of that), then returns the `:latest` tag callers should
// pass to `docker run`.
//
// On match: returns instantly (one `docker image inspect`, ~5ms). Most
// invocations land here.
//
// On mismatch: emits a one-line progress message on stderr, then either
// retags from a candidate image whose label already matches (cheap,
// <1s) or falls back to `docker build` from the embedded runner files
// (1-3 min on a cold Docker cache, typical first-run-after-brew-upgrade
// case). Subsequent calls are back on the fast path.
//
// This is the entry every local docker-spawning code path should use
// instead of composing `runner.LocalImageName(...)+":latest"` directly,
// so that a CLI upgrade carrying new runner code doesn't silently keep
// serving the pre-upgrade runner image (the regression that bit users
// going from v1.0.x to v1.1.4).
//
// The version this binary tags new images with comes from
// `internal/version.Module`. The `Init` flow uses its own
// caller-supplied version via the unexported `ensureLocalRunnerImageAt`
// helper — tests rely on injecting a specific version there.
func EnsureLocalRunnerImage(root string) (string, error) {
	tag, _, err := ensureLocalRunnerImageAt(root, version.Module)
	return tag, err
}

// EnsureLocalRunnerImageStatus is the variant that also reports whether
// the call actually performed work (retag or rebuild). `workspace
// upgrade` uses this so its "runner refreshed" log line only fires when
// something actually changed — silent reuse of an already-current image
// doesn't print a refresh claim it can't back up.
func EnsureLocalRunnerImageStatus(root string) (tag string, refreshed bool, err error) {
	return ensureLocalRunnerImageAt(root, version.Module)
}

func ensureLocalRunnerImageAt(root, moduleVersion string) (string, bool, error) {
	m, err := Load(root)
	if err != nil {
		return "", false, fmt.Errorf("load workspace at %s: %w — run `clavesa workspace init` first", root, err)
	}
	localTag := runner.LocalImageName(m.Name)
	latest := localTag + ":latest"
	versioned := localTag + ":" + moduleVersion

	wantSHA, err := runner.EmbeddedSHA()
	if err != nil {
		return "", false, fmt.Errorf("compute embedded runner sha: %w", err)
	}

	// Dev-tree sources, in priority order. `make build-runner` produces
	// these; brew-installed users typically have none of them.
	devSources := []string{
		"clavesa/transform-runner:" + moduleVersion,
		"clavesa/transform-runner:latest",
	}

	workspaceID, workspaceExists := imageID(latest)
	workspaceSHA, workspaceHasLabel := imageRunnerSHA(latest)

	// No-op fast path. Two equivalent signals that workspace `:latest`
	// is already current: either it carries the binary's embedded
	// runner SHA label, or it points at the same image content as a
	// dev-tree `clavesa/transform-runner:*` tag (which we'd otherwise
	// retag from in the next step). Either signal suffices.
	if workspaceExists && workspaceHasLabel && workspaceSHA == wantSHA {
		// Belt-and-braces: also re-check against dev sources. If the
		// label says we're current but a dev image with newer content
		// exists, fall through to retag — the label was stamped by an
		// earlier in-binary build that the dev image has since
		// superseded. This is the load-bearing case for an iterating
		// runner developer.
		stale := false
		for _, src := range devSources {
			if srcID, ok := imageID(src); ok && srcID != workspaceID {
				stale = true
				break
			}
		}
		if !stale {
			return latest, false, nil
		}
	} else if workspaceExists {
		// No label, or label mismatch. If the workspace already points
		// at the same content as a dev source, no retag would
		// accomplish anything — accept it.
		for _, src := range devSources {
			if srcID, ok := imageID(src); ok && srcID == workspaceID {
				return latest, false, nil
			}
		}
	}

	// Retag from a dev source if one exists and its content differs
	// from the workspace's `:latest`. This is the "user just ran `make
	// build-runner`" branch. We stamp the `clavesa.runner_sha` label
	// during retag so the deploy preflight (VerifyRunnerImage) sees a
	// labelled workspace image even when `make build-runner` didn't
	// stamp the dev source itself.
	for _, src := range devSources {
		srcID, ok := imageID(src)
		if !ok {
			continue
		}
		if workspaceExists && srcID == workspaceID {
			continue
		}
		fmt.Fprintf(os.Stderr, "[clavesa] workspace runner image stale; retagging from %s…\n", src)
		if err := stampedRetag(src, wantSHA, versioned, latest); err != nil {
			return "", false, fmt.Errorf("retag %s -> %s: %w", src, latest, err)
		}
		return latest, true, nil
	}

	// Workspace's own prior `:<version>` build (from a previous init at
	// this CLI version) is the last cheap fallback — useful when the
	// dev tree isn't present (brew-installed user upgrading the
	// binary).
	if vSHA, ok := imageRunnerSHA(versioned); ok && vSHA == wantSHA {
		if workspaceExists {
			if vID, ok := imageID(versioned); ok && vID == workspaceID {
				return latest, false, nil
			}
		}
		fmt.Fprintf(os.Stderr, "[clavesa] workspace runner image stale; retagging from %s…\n", versioned)
		if err := stampedRetag(versioned, wantSHA, versioned, latest); err != nil {
			return "", false, fmt.Errorf("retag %s -> %s: %w", versioned, latest, err)
		}
		return latest, true, nil
	}

	// Full rebuild — slow. Re-extract embedded runner files first so a
	// CLI carrying updated runner code writes them to <root>/runner/
	// before `docker build` reads from there. Init already does this
	// for the first-init path; the re-extract here covers the
	// upgrade-after-init case where the on-disk copy is from an older
	// CLI version.
	fmt.Fprintln(os.Stderr, "[clavesa] workspace runner image stale; rebuilding from embedded files — first run after a CLI upgrade can take 1–3 min on a cold Docker cache…")
	runnerDir := filepath.Join(root, "runner")
	if err := extractRunnerFiles(runnerDir); err != nil {
		return "", false, fmt.Errorf("extract runner source: %w", err)
	}
	cmd := exec.Command("docker", "build",
		"--label", runner.SHALabel+"="+wantSHA,
		"--build-arg", "CLAVESA_MODULE_VERSION="+moduleVersion,
		"-t", versioned, "-t", latest, runnerDir)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", false, fmt.Errorf("build runner image: %w", err)
	}
	fmt.Fprintln(os.Stderr, "[clavesa] runner image ready.")
	return latest, true, nil
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

// stampedRetag re-tags `src` to `dstVersioned` and `dstLatest` while
// stamping the `clavesa.runner_sha=wantSHA` label so the deploy
// preflight (VerifyRunnerImage) can recognise the retagged image as
// current. `make build-runner` intentionally doesn't stamp the label
// (so dev-cycle content changes are caught by image-ID compare), so we
// stamp it here at workspace-retag time via a tiny `docker build` with
// a stdin Dockerfile (`FROM <src>` + `LABEL`) — no layer rebuild, just
// a metadata stamp.
func stampedRetag(src, wantSHA, dstVersioned, dstLatest string) error {
	dockerfile := "FROM " + src + "\nLABEL " + runner.SHALabel + "=" + wantSHA + "\n"
	cmd := exec.Command("docker", "build",
		"-t", dstVersioned, "-t", dstLatest, "-")
	cmd.Stdin = strings.NewReader(dockerfile)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("stamp runner-sha label on %s: %w (%s)", src, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// imageMatchesSHA returns true iff `ref` exists locally AND carries the
// expected clavesa.runner_sha label. Missing image, missing label, or
// label mismatch all return false — the caller falls through to either the
// next retag candidate or a fresh build.
func imageMatchesSHA(ref, wantSHA string) bool {
	got, ok := imageRunnerSHA(ref)
	return ok && got == wantSHA
}

// imageID returns the content-addressed Docker image ID (`sha256:…`)
// for `ref`. Returns ("", false) when the image doesn't exist. Used to
// detect "user just rebuilt the dev runner image" without relying on
// any label being present — `make build-runner` doesn't stamp the
// clavesa.runner_sha label, so we compare image IDs directly to spot
// content changes between the dev image and the workspace's retagged
// copy.
func imageID(ref string) (string, bool) {
	out, err := exec.Command("docker", "image", "inspect",
		"--format", "{{ .Id }}", ref).Output()
	if err != nil {
		return "", false
	}
	got := strings.TrimRight(string(out), "\n")
	if got == "" {
		return "", false
	}
	return got, true
}

// imageRunnerSHA reads the clavesa.runner_sha label off `ref`. Returns
// (sha, true) when the image exists and carries the label, ("", false)
// otherwise. Used for the "is the workspace's `:latest` already at the
// binary's embedded SHA?" check on the no-op fast path.
func imageRunnerSHA(ref string) (string, bool) {
	out, err := exec.Command("docker", "image", "inspect",
		"--format", "{{ index .Config.Labels \""+runner.SHALabel+"\" }}", ref).Output()
	if err != nil {
		return "", false
	}
	got := string(out)
	if len(got) > 0 && got[len(got)-1] == '\n' {
		got = got[:len(got)-1]
	}
	if got == "" {
		// Image exists but has no clavesa.runner_sha label. Treat as
		// "unknown SHA" — same effect as missing image: no equality
		// claim can be made.
		return "", false
	}
	return got, true
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
