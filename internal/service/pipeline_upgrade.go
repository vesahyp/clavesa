package service

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/vesahyp/clavesa/internal/modules"
)

// githubSourceRE captures the repo path and current ref from a GitHub
// module source — read-only, used by extractCurrentRef to detect a
// pipeline's module version. The matching `rewriteModuleRefs` helper
// was deleted (C P3-6, 2026-05-24); GitHub-ref pipelines now flow
// through the embedded-modules path post-v0.30.0.
var githubSourceRE = regexp.MustCompile(`github\.com/([^/]+/[^/]+)//[^?]+\?ref=([^\s"]+)`)

// githubModuleSourceLineRE matches a github-form `source = "..."` attribute
// across a pipeline .tf, capturing the module relative path (e.g.
// "transform/aws") so it can be re-emitted in the embedded form.
// Group 1 = leading prefix up to and including `source = "`. Group 2 =
// the moduleRel. Group 3 = the trailing closing quote.
var githubModuleSourceLineRE = regexp.MustCompile(`(source\s*=\s*")github\.com/[^"]+//modules/([^"?]+)(?:\?ref=[^"]+)?(")`)

// embeddedModuleVersionLineRE matches an already-embedded `source = "..."`
// attribute and captures the moduleRel suffix and the version, so a
// `pipeline upgrade` to a newer ModuleVersion only swaps the version.
// Group 1 = `source = "<prefix>/.<metadir>/modules/`. Group 2 = the
// current version (e.g. "v0.29.0", with the leading `v`). Group 3 =
// `/<moduleRel>"`.
var embeddedModuleVersionLineRE = regexp.MustCompile(`(source\s*=\s*"(?:\.\./)+\.[A-Za-z0-9_-]+/modules/)(v[^/"]+)(/[^"]+")`)

// ModuleVersionInfo reports the current and (optionally) latest module
// versions a pipeline references. CurrentRef is empty when no GitHub
// module sources are present.
type ModuleVersionInfo struct {
	CurrentRef string `json:"current_ref"`
	LatestRef  string `json:"latest_ref"`
	RepoURL    string `json:"repo_url"`
	// CLIVersion is the embedded ModuleVersion of the running binary —
	// the exact ref a `workspace upgrade` / `pipeline upgrade` will
	// target. The UI reads this for its "upgrade to X" chip. Distinct
	// from LatestRef, which for legacy github-form pipelines can report a
	// newer upstream tag the binary itself doesn't yet ship.
	CLIVersion string `json:"cli_version"`
}

// PipelineModuleVersion inspects a pipeline's .tf files and returns the
// current module version (whichever source form the pipeline uses) and
// the latest version the binary can deliver. Since v0.30.0 the binary's
// embedded ModuleVersion is the authoritative latest — no network call
// for embedded-form pipelines. Legacy github-form pipelines still get a
// network `git ls-remote` so the UI can report a separate upstream tag
// when the binary itself is behind. Errors from the network probe are
// reported via RepoURL = "" + the upstream-equals-self LatestRef so the
// UI degrades gracefully on disconnected hosts.
func (s *Service) PipelineModuleVersion(dir string) (*ModuleVersionInfo, error) {
	abs := s.resolveDir(dir)
	tfFiles, err := filepath.Glob(filepath.Join(abs, "*.tf"))
	if err != nil {
		return nil, err
	}
	if len(tfFiles) == 0 {
		return nil, fmt.Errorf("no .tf files found in %s", dir)
	}
	currentRef := detectCurrentModuleVersion(tfFiles)
	// Embedded-form pipelines: binary's ModuleVersion is the only ref
	// that matters; no network probe.
	repoURL, _, ghErr := detectGitHubSources(tfFiles)
	if ghErr != nil {
		return &ModuleVersionInfo{
			CurrentRef: currentRef,
			LatestRef:  ModuleVersion,
			CLIVersion: ModuleVersion,
		}, nil
	}
	// Legacy github pipeline — probe upstream so the UI can show "newer
	// tag available" vs. just "your binary already has v0.30.0".
	latest, err := latestGitTag(repoURL)
	if err != nil {
		return &ModuleVersionInfo{
			CurrentRef: currentRef,
			LatestRef:  ModuleVersion,
			RepoURL:    repoURL,
			CLIVersion: ModuleVersion,
		}, nil
	}
	return &ModuleVersionInfo{
		CurrentRef: currentRef,
		LatestRef:  latest,
		RepoURL:    repoURL,
		CLIVersion: ModuleVersion,
	}, nil
}

// localComputeRE matches a standalone `compute = "local"` attribute line
// (compute is the cloud deploy target; "local" is no longer a value).
// Anchored to a whole line so it never touches a substring; tolerates a
// CRLF line ending.
var localComputeRE = regexp.MustCompile(`(?m)^[ \t]*compute[ \t]*=[ \t]*"local"[ \t]*\r?\n`)

// incrementalInputsRE matches a standalone `incremental_inputs = [...]`
// attribute line inside a transform module block. The transform module
// dropped this variable; the orchestration emitter still reads it from
// the parsed graph for the incremental-read descriptor, but `terraform
// validate` rejects passing it as a module argument. `pipeline upgrade`
// strips the line so deploys stop failing — does NOT touch the
// incremental behaviour itself (the emitter falls back to full reads).
var incrementalInputsRE = regexp.MustCompile(`(?m)^[ \t]*incremental_inputs[ \t]*=[ \t]*\[[^\]\n]*\][ \t]*\r?\n`)

// runnerImageRE matches a standalone `runner_image = <expr>` attribute
// line inside a transform module block. v2.2.0 collapsed the
// per-transform Lambda; v2.2.1 dropped `var.runner_image` from the
// transform module. A v2.1.x pipeline carrying `runner_image =
// data.terraform_remote_state.workspace.outputs.runner_image` now fails
// `terraform validate` with "Unsupported argument: runner_image".
// `pipeline upgrade` strips the line; the pipeline Lambda emitted by
// orchestration tfgen still threads the URI from workspace remote state
// directly, so deploys keep working.
var runnerImageRE = regexp.MustCompile(`(?m)^[ \t]*runner_image[ \t]*=[ \t]*[^\r\n]+\r?\n`)

// UpgradePipeline rewrites every Clavesa module `source = "..."`
// in a pipeline's .tf files to the embedded-modules form at targetRef
// (defaults to the binary's ModuleVersion when empty), strips the
// legacy `compute = "local"` attribute (a one-shot migration), and
// re-syncs orchestration.tf when present.
//
// Three source forms are accepted on input:
//
//  1. Legacy github: `github.com/vesahyp/clavesa//modules/X/aws?ref=vY`
//     (rewritten to embedded form at finalRef)
//  2. Already-embedded: `../.clavesa/modules/vY/X/aws`
//     (version bumped to finalRef)
//  3. Local fixture / dev: `../../modules/X/aws` (left alone — these are
//     only used by tests against the live dev tree)
//
// Mirrors the CLI's `clavesa pipeline upgrade <dir>` flow — the CLI
// command is a thin wrapper around this so both surfaces emit
// byte-identical .tf. `migrated` counts the compute lines stripped.
//
// No network call. The binary's embedded modules are authoritative for
// the version; `latestGitTag` (a network call) is only exercised by
// `PipelineModuleVersion`, used by the UI to display "an update is
// available" hints for legacy v0.x pipelines.
func (s *Service) UpgradePipeline(dir, targetRef string) (currentRef, finalRef string, updated, migrated int, err error) {
	abs := s.resolveDir(dir)
	tfFiles, err := filepath.Glob(filepath.Join(abs, "*.tf"))
	if err != nil {
		return "", "", 0, 0, err
	}
	if len(tfFiles) == 0 {
		return "", "", 0, 0, fmt.Errorf("no .tf files found in %s", dir)
	}
	currentRef = detectCurrentModuleVersion(tfFiles)
	finalRef = targetRef
	if finalRef == "" {
		finalRef = ModuleVersion
	}

	// One-shot migration: strip `compute = "local"`. Runs regardless of
	// whether the ref changes — a pipeline already on the latest module
	// version can still carry the legacy value, and `pipeline upgrade`
	// is where it gets cleaned up.
	for _, f := range tfFiles {
		n, merr := stripLocalCompute(f)
		if merr != nil {
			return currentRef, finalRef, updated, migrated, merr
		}
		migrated += n
	}

	// One-shot migration: strip `incremental_inputs = [...]`. The
	// transform module dropped the variable (last seen at v0.19.0); a
	// pipeline carrying it now fails `terraform validate` with
	// "Unsupported argument: incremental_inputs". The orchestration
	// emitter no longer reads it either — the snapshot-bounded read
	// descriptor moved to a different authoring shape (TODO line item).
	for _, f := range tfFiles {
		n, merr := stripIncrementalInputs(f)
		if merr != nil {
			return currentRef, finalRef, updated, migrated, merr
		}
		migrated += n
	}

	// One-shot migration: strip `runner_image = ...`. v2.2.0 collapsed
	// the per-transform Lambda; v2.2.1 dropped the variable. Existing
	// pipelines on v2.1.x have `runner_image =
	// data.terraform_remote_state.workspace.outputs.runner_image` in
	// every transform module block — `pipeline upgrade` clears it so
	// the pipeline validates against the new module.
	for _, f := range tfFiles {
		n, merr := stripRunnerImage(f)
		if merr != nil {
			return currentRef, finalRef, updated, migrated, merr
		}
		migrated += n
	}

	// Rewrite every clavesa module source to the embedded form at
	// finalRef. Runs unconditionally — a pipeline already on finalRef
	// with embedded sources will see zero substitutions, which is the
	// expected no-op outcome.
	for _, f := range tfFiles {
		n, rerr := s.rewriteAllModuleSources(f, abs, finalRef)
		if rerr != nil {
			return currentRef, finalRef, updated, migrated, rerr
		}
		updated += n
	}

	// Sync orchestration.tf — the emitter's output shape can change
	// between versions. Run whenever we touched source lines OR migrated
	// a compute attribute, since the orchestration emit also encodes the
	// rewritten compute structure. Unconditional: pipelines whose
	// directory never had an orchestration.tf (older `pipeline create`
	// flows, or hand-authored directories) get one generated, instead of
	// the silent skip that left them deploy-broken (GH #3).
	if updated > 0 || migrated > 0 {
		if syncErr := s.SyncOrchestration(dir, ""); syncErr != nil {
			return currentRef, finalRef, updated, migrated, fmt.Errorf("sync orchestration.tf: %w", syncErr)
		}
	}
	return currentRef, finalRef, updated, migrated, nil
}

// detectCurrentModuleVersion inspects tf files for a Clavesa module
// source reference and returns the version embedded in it. Tries the
// embedded form first (v0.30.0+), then github form (legacy v0.x), then
// returns "" if neither is found. Informational only — UpgradePipeline
// always targets the binary's ModuleVersion.
func detectCurrentModuleVersion(files []string) string {
	embeddedVerRE := regexp.MustCompile(`/\.[A-Za-z0-9_-]+/modules/(v[^/"]+)/`)
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		if m := embeddedVerRE.FindSubmatch(data); m != nil {
			return string(m[1])
		}
		if m := githubSourceRE.FindSubmatch(data); m != nil {
			return string(m[2])
		}
	}
	return ""
}

// rewriteAllModuleSources rewrites every clavesa-module `source = "..."`
// attribute in path to the embedded form at finalRef. Counts how many
// substitutions occurred. Three patterns are handled:
//
//  1. github form  → rewritten to embedded form at finalRef
//  2. embedded form with stale version → version field bumped to finalRef
//  3. embedded form already at finalRef → no-op (zero substitutions)
func (s *Service) rewriteAllModuleSources(path, pipelineAbs, finalRef string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	count := 0

	// Pass 1: github → embedded.
	data = githubModuleSourceLineRE.ReplaceAllFunc(data, func(match []byte) []byte {
		m := githubModuleSourceLineRE.FindSubmatch(match)
		if m == nil {
			return match
		}
		moduleRel := string(m[2])
		newSrc, err := s.relativeModuleSource(pipelineAbs, moduleRel, finalRef)
		if err != nil {
			return match
		}
		count++
		return []byte(string(m[1]) + newSrc + string(m[3]))
	})

	// Pass 2: embedded with stale version → embedded at finalRef.
	data = embeddedModuleVersionLineRE.ReplaceAllFunc(data, func(match []byte) []byte {
		m := embeddedModuleVersionLineRE.FindSubmatch(match)
		if m == nil {
			return match
		}
		if string(m[2]) == finalRef {
			return match // already at target
		}
		count++
		return []byte(string(m[1]) + finalRef + string(m[3]))
	})

	if count == 0 {
		return 0, nil
	}
	return count, os.WriteFile(path, data, 0o644)
}

// relativeModuleSource is the rewriter-local equivalent of Service.ModuleSource
// that lets UpgradePipeline target an arbitrary version (not just the
// binary's current ModuleVersion).
func (s *Service) relativeModuleSource(pipelineDir, moduleRel, ver string) (string, error) {
	return modules.RelativeSource(pipelineDir, s.workspace, ver, moduleRel)
}

// detectGitHubSources scans tf files for the first GitHub module source
// and returns the bare repo URL (suitable for `git ls-remote`) and the
// current ?ref= value.
func detectGitHubSources(files []string) (repoURL, currentRef string, err error) {
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return "", "", err
		}
		if m := githubSourceRE.FindSubmatch(data); m != nil {
			return "https://github.com/" + string(m[1]) + ".git", string(m[2]), nil
		}
	}
	return "", "", fmt.Errorf("no GitHub module sources found in pipeline")
}

// latestGitTag queries the remote repo for the highest semver tag.
func latestGitTag(repoURL string) (string, error) {
	out, err := exec.Command("git", "ls-remote", "--tags", "--sort=-v:refname", repoURL, "v*").Output()
	if err != nil {
		return "", fmt.Errorf("git ls-remote: %w", err)
	}
	for _, line := range bytes.Split(out, []byte("\n")) {
		if bytes.HasSuffix(line, []byte("^{}")) {
			continue
		}
		parts := bytes.Fields(line)
		if len(parts) < 2 {
			continue
		}
		ref := string(parts[1])
		if tag, ok := strings.CutPrefix(ref, "refs/tags/"); ok {
			return tag, nil
		}
	}
	return "", fmt.Errorf("no version tags found on %s", repoURL)
}

// stripLocalCompute removes every `compute = "local"` line from a single
// tf file and returns the number of lines removed. A file with no match
// is left untouched.
func stripLocalCompute(path string) (int, error) {
	return stripLines(path, localComputeRE)
}

// stripIncrementalInputs removes every `incremental_inputs = [...]`
// line from a single tf file. The transform module no longer declares
// this variable; carrying it makes deploys fail at validate-time.
func stripIncrementalInputs(path string) (int, error) {
	return stripLines(path, incrementalInputsRE)
}

// stripRunnerImage removes every `runner_image = ...` line from a
// single tf file. v2.2.1 dropped `var.runner_image` from the transform
// module; carrying the attribute on a v2.1.x pipeline makes upgraded
// deploys fail with "Unsupported argument".
func stripRunnerImage(path string) (int, error) {
	return stripLines(path, runnerImageRE)
}

func stripLines(path string, re *regexp.Regexp) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	matches := re.FindAll(data, -1)
	if len(matches) == 0 {
		return 0, nil
	}
	out := re.ReplaceAll(data, nil)
	if err := os.WriteFile(path, out, 0o644); err != nil {
		return 0, err
	}
	return len(matches), nil
}
