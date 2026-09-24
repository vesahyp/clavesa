package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// SetBackend writes b into clavesa.json's `backend` field (ADR-025,
// "Manifest field") and nothing else: no backend.tf is written and no
// state moves. That's MigrateState's job, run separately so a developer
// can review the manifest change before touching any Terraform state.
//
// b == nil clears the backend, but only when no stack in the workspace
// has a backend.tf yet — once any stack has been migrated, moving back
// to local state is a state operation this method deliberately doesn't
// perform, so it refuses rather than leaving the manifest and the
// on-disk stacks disagreeing about where state lives.
func (s *Service) SetBackend(b *workspace.Backend) error {
	m, err := workspace.Load(s.workspace)
	if err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("%s is not a clavesa workspace (no clavesa.json)", s.workspace)
	}

	if b != nil {
		if err := b.Validate(); err != nil {
			return err
		}
	} else if m.Backend != nil {
		stacks, err := discoverStacks(s.workspace)
		if err != nil {
			return fmt.Errorf("discover stacks: %w", err)
		}
		for _, dir := range stacks {
			if _, statErr := os.Stat(filepath.Join(dir, "backend.tf")); statErr == nil {
				return fmt.Errorf("cannot clear backend: %s already has a backend.tf — moving a migrated stack back to local state is not supported", relStackDir(s.workspace, dir))
			}
		}
	}

	m.Backend = b
	return workspace.SaveManifest(s.workspace, m)
}

// MigrateStackStatus is the per-stack outcome MigrateState reports
// (ADR-025, "Migration, not recreate").
type MigrateStackStatus string

const (
	MigrateStatusMigrated        MigrateStackStatus = "migrated"
	MigrateStatusAlreadyMigrated MigrateStackStatus = "already-migrated"
	MigrateStatusSkippedNoState  MigrateStackStatus = "skipped-no-state"
	MigrateStatusFailed          MigrateStackStatus = "failed"
)

// Plan summary values MigrateStackResult.Plan carries after the
// post-migration plan pass (ADR-025 step "f").
const (
	PlanNoChanges = "no changes"
	PlanChanges   = "changes"
	PlanError     = "error"
)

// MigrateStackResult is one row of MigrateState's report.
type MigrateStackResult struct {
	// Dir is workspace-relative ("." for the workspace root itself).
	Dir string `json:"dir"`
	// Key is the S3 key this stack's state lives at (or will, once
	// deployed, for a skipped-no-state stack).
	Key    string             `json:"key"`
	Status MigrateStackStatus `json:"status"`
	// Plan is set after the post-migration plan pass: PlanNoChanges,
	// PlanChanges or PlanError. Empty for a stack the run never reached
	// (it stopped on an earlier failure).
	Plan string `json:"plan,omitempty"`
	// hasRemoteState is true when the stack's state is in S3 after this
	// run, which is what decides whether the post-migration plan runs.
	hasRemoteState bool
	// Err carries the failure detail for Status == failed, and any
	// non-fatal follow-up problem (e.g. local cleanup after a
	// migration that otherwise succeeded) for any other status.
	Err string `json:"err,omitempty"`
}

// MigrateResult is MigrateState's full report.
type MigrateResult struct {
	Stacks []MigrateStackResult `json:"stacks"`
}

// MigrateStateOptions carries MigrateState's optional progress sinks.
type MigrateStateOptions struct {
	// Out/Err receive terraform init/plan's own output as it runs, for
	// callers that want to stream progress (the CLI wrapper, slice 5).
	// Nil discards it — unit tests don't need to wire anything.
	Out, Err io.Writer
}

func (o MigrateStateOptions) stdout() io.Writer {
	if o.Out != nil {
		return o.Out
	}
	return io.Discard
}

func (o MigrateStateOptions) stderr() io.Writer {
	if o.Err != nil {
		return o.Err
	}
	return io.Discard
}

// MigrateState moves a workspace from local Terraform state to the
// remote S3 backend named in clavesa.json (ADR-025, "Migration, not
// recreate"). It never destroys or recreates anything: every stack's
// state is carried over with `terraform init -migrate-state
// -force-copy`, and the pre-migration local state is kept on disk
// (renamed, not deleted) so a botched run can be recovered by hand.
//
// A failure on any one stack stops the whole run — later stacks are
// left untouched — and returns a non-nil error alongside the partial
// MigrateResult built so far. This is deliberate, not a shortcut: a
// stack already marked "migrated" or "already-migrated" is
// self-evident on the next run (backend.tf exists, no non-empty local
// state), so re-running MigrateState after fixing the problem resumes
// exactly where it stopped instead of redoing completed work.
func (s *Service) MigrateState(ctx context.Context, opts MigrateStateOptions) (MigrateResult, error) {
	// Stacks is never nil, so JSON callers always get an array, even
	// when a precondition fails before any stack is reached.
	result := MigrateResult{Stacks: []MigrateStackResult{}}

	m, err := workspace.Load(s.workspace)
	if err != nil {
		return result, err
	}
	if m == nil {
		return result, fmt.Errorf("%s is not a clavesa workspace (no clavesa.json)", s.workspace)
	}
	if m.Backend == nil {
		return result, fmt.Errorf("clavesa.json has no backend configured — run `clavesa workspace set-backend` first")
	}
	if err := m.Backend.Validate(); err != nil {
		return result, fmt.Errorf("clavesa.json: %w", err)
	}

	if err := s.checkTerraformVersion(ctx); err != nil {
		return result, err
	}
	if err := s.checkStateBucket(ctx, m.Backend.Bucket, m.Backend.Region); err != nil {
		return result, fmt.Errorf("state bucket precondition failed: %w", err)
	}

	stacks, err := discoverStacks(s.workspace)
	if err != nil {
		return result, fmt.Errorf("discover stacks: %w", err)
	}

	for _, dir := range stacks {
		row := MigrateStackResult{
			Dir: relStackDir(s.workspace, dir),
			Key: stackStateKey(m, s.workspace, dir),
		}
		isWS := isWorkspaceStack(s.workspace, dir)

		alreadyMigrated, err := stackAlreadyMigrated(dir)
		if err != nil {
			row.Status = MigrateStatusFailed
			row.Err = err.Error()
			result.Stacks = append(result.Stacks, row)
			return result, fmt.Errorf("stack %s: %w", row.Dir, err)
		}
		if alreadyMigrated {
			// backend.tf alone does not prove the state moved: an earlier
			// run can stop between init and the object check. Only a
			// non-empty remote object, or a stack that never had state,
			// counts as migrated. A local copy with no remote object is
			// the one case a deploy would read as "recreate everything".
			exists, size, err := s.stateObjectStatus(ctx, m.Backend.Bucket, m.Backend.Region, row.Key)
			if err != nil {
				row.Status = MigrateStatusFailed
				row.Err = fmt.Sprintf("check existing state object: %v", err)
				result.Stacks = append(result.Stacks, row)
				return result, fmt.Errorf("stack %s: %s", row.Dir, row.Err)
			}
			if !exists || size == 0 {
				if local := nonEmptyLocalStateCopy(dir); local != "" {
					row.Status = MigrateStatusFailed
					row.Err = fmt.Sprintf("backend.tf is present but s3://%s/%s holds no state, and %s does: restore it as terraform.tfstate, remove backend.tf, and run migrate-state again", m.Backend.Bucket, row.Key, local)
					result.Stacks = append(result.Stacks, row)
					return result, fmt.Errorf("stack %s: %s", row.Dir, row.Err)
				}
			}
			row.Status = MigrateStatusAlreadyMigrated
			row.hasRemoteState = exists && size > 0
			result.Stacks = append(result.Stacks, row)
			continue
		}

		exists, _, err := s.stateObjectStatus(ctx, m.Backend.Bucket, m.Backend.Region, row.Key)
		if err != nil {
			row.Status = MigrateStatusFailed
			row.Err = fmt.Sprintf("check existing state object: %v", err)
			result.Stacks = append(result.Stacks, row)
			return result, fmt.Errorf("stack %s: %s", row.Dir, row.Err)
		}
		if exists {
			row.Status = MigrateStatusFailed
			row.Err = fmt.Sprintf("refusing to migrate: state already exists at s3://%s/%s", m.Backend.Bucket, row.Key)
			result.Stacks = append(result.Stacks, row)
			return result, fmt.Errorf("stack %s: %s", row.Dir, row.Err)
		}

		hasState, err := stackHasNonEmptyLocalState(dir)
		if err != nil {
			row.Status = MigrateStatusFailed
			row.Err = err.Error()
			result.Stacks = append(result.Stacks, row)
			return result, fmt.Errorf("stack %s: %w", row.Dir, err)
		}

		origMain, writeErr := migrateStackFiles(dir, m, isWS)
		if writeErr != nil {
			row.Status = MigrateStatusFailed
			row.Err = writeErr.Error()
			result.Stacks = append(result.Stacks, row)
			return result, fmt.Errorf("stack %s: %w", row.Dir, writeErr)
		}

		if !hasState {
			// Never deployed: backend.tf is written and main.tf is
			// stripped so the stack is remote-backed for its first
			// deploy, but there's no state to move — no init.
			row.Status = MigrateStatusSkippedNoState
			result.Stacks = append(result.Stacks, row)
			continue
		}

		exitCode, runErr := s.runTerraform(ctx, dir, opts.stdout(), opts.stderr(), "init", "-migrate-state", "-force-copy", "-input=false")
		if runErr != nil || exitCode != 0 {
			msg := fmt.Sprintf("terraform init -migrate-state exited %d", exitCode)
			if runErr != nil {
				msg = fmt.Sprintf("terraform init -migrate-state: %v", runErr)
			}
			if rbErr := rollbackStackFiles(dir, origMain); rbErr != nil {
				msg += fmt.Sprintf("; rollback also failed: %v", rbErr)
			}
			if rsErr := restoreLocalStateFromBackup(dir); rsErr != nil {
				msg += fmt.Sprintf("; restoring local state also failed: %v", rsErr)
			}
			row.Status = MigrateStatusFailed
			row.Err = msg
			result.Stacks = append(result.Stacks, row)
			return result, fmt.Errorf("stack %s: %s", row.Dir, msg)
		}

		// Confirm the object actually landed, non-empty, before touching
		// any local file — never trust an exit code alone for a
		// state-destructive cleanup step.
		exists, size, err := s.stateObjectStatus(ctx, m.Backend.Bucket, m.Backend.Region, row.Key)
		if err != nil || !exists || size == 0 {
			row.Err = "terraform init reported success but no non-empty state object was found at the target key; stack rolled back to local state"
			if rbErr := rollbackStackFiles(dir, origMain); rbErr != nil {
				row.Err += fmt.Sprintf("; rollback also failed: %v", rbErr)
			}
			if rsErr := restoreLocalStateFromBackup(dir); rsErr != nil {
				row.Err += fmt.Sprintf("; restoring local state also failed: %v", rsErr)
			}
			row.Status = MigrateStatusFailed
			result.Stacks = append(result.Stacks, row)
			return result, fmt.Errorf("stack %s: %s", row.Dir, row.Err)
		}

		row.Status = MigrateStatusMigrated
		row.hasRemoteState = true
		if err := finalizeLocalState(dir); err != nil {
			// Remote state is confirmed good; this is a local-cleanup
			// nicety, not a reason to fail the migration or roll back.
			row.Err = fmt.Sprintf("migrated, but local cleanup failed: %v", err)
		}
		result.Stacks = append(result.Stacks, row)
	}

	// ADR-025 step "f": plan every stack the run touched, never apply.
	// stacks and result.Stacks are 1:1 here — the loop above only
	// reaches this point after processing every stack without a
	// mid-run failure (any failure returns early, above).
	for i, dir := range stacks {
		// A stack with no state has no .terraform either; its first
		// deploy initializes it. Planning it here only reports an error.
		if !result.Stacks[i].hasRemoteState {
			continue
		}
		// A plain init first: after `workspace upgrade` the module sources
		// point at a version directory terraform has not installed yet, and
		// plan fails with "Module not installed". The backend is already
		// S3 here, so this init moves nothing.
		if code, err := s.runTerraform(ctx, dir, opts.stdout(), opts.stderr(), "init", "-input=false"); err != nil || code != 0 {
			result.Stacks[i].Plan = PlanError
			msg := fmt.Sprintf("terraform init before plan exited %d", code)
			if err != nil {
				msg = fmt.Sprintf("terraform init before plan: %v", err)
			}
			result.Stacks[i].Err = appendErr(result.Stacks[i].Err, msg)
			continue
		}
		exitCode, err := s.runTerraform(ctx, dir, opts.stdout(), opts.stderr(), "plan", "-detailed-exitcode", "-input=false", "-lock=false")
		switch {
		case err != nil:
			result.Stacks[i].Plan = PlanError
			result.Stacks[i].Err = appendErr(result.Stacks[i].Err, err.Error())
		case exitCode == 0:
			result.Stacks[i].Plan = PlanNoChanges
		case exitCode == 2:
			result.Stacks[i].Plan = PlanChanges
		default:
			result.Stacks[i].Plan = PlanError
			result.Stacks[i].Err = appendErr(result.Stacks[i].Err, fmt.Sprintf("terraform plan exited %d", exitCode))
		}
	}

	return result, nil
}

func appendErr(existing, add string) string {
	if existing == "" {
		return add
	}
	return existing + "; " + add
}

// ---------------------------------------------------------------------------
// Stack discovery (ADR-025 "Migration, not recreate", step "b")
// ---------------------------------------------------------------------------

// discoverStacks returns every Terraform stack in the workspace, in
// migration order: the workspace root first, then every pipeline stack
// sorted by directory name. A pipeline stack is a direct subdirectory
// that either looks like an ordinary pipeline (IsPipelineDir — has .tf
// files, no clavesa.json) or is an underscore-prefixed directory (the
// normal pipeline scan skips these) that carries real Terraform state:
// a terraform.tfstate file, a backend.tf (already migrated, so a resumed
// run must still find it to report "already-migrated"), or a main.tf
// still carrying the pre-migration `terraform_remote_state "workspace"`
// block. That covers a deployed `_maintenance` (GH #53), which is opted
// out of the ordinary pipeline scan but has state that must move like
// any other stack once it's been deployed. Dot-directories (.clavesa,
// .git, ...) are always skipped.
func discoverStacks(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read workspace dir: %w", err)
	}
	var pipelineDirs []string
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		dir := filepath.Join(root, e.Name())
		if strings.HasPrefix(e.Name(), "_") {
			if stackHasRealState(dir) {
				pipelineDirs = append(pipelineDirs, dir)
			}
			continue
		}
		if IsPipelineDir(dir) {
			pipelineDirs = append(pipelineDirs, dir)
		}
	}
	sort.Strings(pipelineDirs)
	return append([]string{root}, pipelineDirs...), nil
}

// stackHasRealState reports whether an underscore-prefixed directory —
// normally excluded from the pipeline scan — has actual Terraform state
// migrate-state must account for.
func stackHasRealState(dir string) bool {
	if _, err := os.Stat(filepath.Join(dir, "terraform.tfstate")); err == nil {
		return true
	}
	if _, err := os.Stat(filepath.Join(dir, "backend.tf")); err == nil {
		return true
	}
	data, err := os.ReadFile(filepath.Join(dir, "main.tf"))
	if err != nil {
		return false
	}
	return bytes.Contains(data, remoteStateBlockMarker)
}

// relStackDir returns dir relative to workspaceRoot, or "." for the
// workspace root itself.
func relStackDir(workspaceRoot, dir string) string {
	rel, err := filepath.Rel(workspaceRoot, dir)
	if err != nil {
		return dir
	}
	return rel
}

// isWorkspaceStack reports whether dir is the workspace root itself,
// compared as cleaned absolute paths (mirrors workspace.stackKeyIsWorkspace,
// unexported in that package).
func isWorkspaceStack(workspaceRoot, dir string) bool {
	a, errA := filepath.Abs(workspaceRoot)
	b, errB := filepath.Abs(dir)
	return errA == nil && errB == nil && a == b
}

// stackStateKey returns the S3 key a stack's state lives (or will live)
// at (ADR-025 "State keys").
func stackStateKey(m *workspace.Manifest, workspaceRoot, dir string) string {
	if isWorkspaceStack(workspaceRoot, dir) {
		return m.Backend.WorkspaceStateKey(m.Name)
	}
	return m.Backend.PipelineStateKey(m.Name, filepath.Base(dir))
}

// ---------------------------------------------------------------------------
// Per-stack state inspection
// ---------------------------------------------------------------------------

// stackHasNonEmptyLocalState reports whether dir has a non-empty
// terraform.tfstate — "deployed" in the sense that matters for
// migration: an absent or 0-byte file means nothing to carry over.
func stackHasNonEmptyLocalState(dir string) (bool, error) {
	info, err := os.Stat(filepath.Join(dir, "terraform.tfstate"))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat terraform.tfstate: %w", err)
	}
	return info.Size() > 0, nil
}

// stackAlreadyMigrated reports whether dir has already gone through
// MigrateState: a backend.tf exists and there's no non-empty local
// state left to move. This is what makes a failed run resumable — a
// stack that got this far on a previous attempt is recorded and
// skipped rather than reprocessed.
func stackAlreadyMigrated(dir string) (bool, error) {
	if _, err := os.Stat(filepath.Join(dir, "backend.tf")); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat backend.tf: %w", err)
	}
	hasState, err := stackHasNonEmptyLocalState(dir)
	if err != nil {
		return false, err
	}
	return !hasState, nil
}

// ---------------------------------------------------------------------------
// main.tf rewriting (ADR-025 step "c")
// ---------------------------------------------------------------------------

// migrateStackFiles writes the stack's backend.tf from the manifest and
// strips its local wiring from main.tf: the workspace's
// `backend "local" {}` line, or a pipeline's whole
// `data "terraform_remote_state" "workspace" { ... }` block. It returns
// the original main.tf bytes so the caller can restore them
// (rollbackStackFiles) if the subsequent terraform init fails.
//
// Writing backend.tf and stripping main.tf happens for every
// not-yet-migrated stack, whether or not it has local state to move —
// a never-deployed stack (2e) gets exactly the same file treatment,
// just no init afterward.
func migrateStackFiles(dir string, m *workspace.Manifest, isWorkspace bool) (origMain []byte, err error) {
	mainPath := filepath.Join(dir, "main.tf")
	origMain, err = os.ReadFile(mainPath)
	if err != nil {
		return nil, fmt.Errorf("read main.tf: %w", err)
	}

	var stripped []byte
	if isWorkspace {
		stripped = stripWorkspaceLocalBackend(origMain)
	} else {
		stripped, _ = stripLocalRemoteStateBlock(origMain)
	}
	if err := os.WriteFile(mainPath, stripped, 0o644); err != nil {
		return nil, fmt.Errorf("write main.tf: %w", err)
	}

	var backendErr error
	if isWorkspace {
		backendErr = workspace.WriteWorkspaceBackendTF(dir, m)
	} else {
		backendErr = workspace.WritePipelineBackendTF(dir, m)
	}
	if backendErr != nil {
		_ = os.WriteFile(mainPath, origMain, 0o644) // best-effort restore
		return nil, fmt.Errorf("write backend.tf: %w", backendErr)
	}
	return origMain, nil
}

// rollbackStackFiles undoes migrateStackFiles after a failed
// `terraform init -migrate-state`: main.tf goes back to its
// pre-migration content and the just-written backend.tf is removed, so
// the stack is left exactly as it was before this run touched it.
func rollbackStackFiles(dir string, origMain []byte) error {
	var errs []string
	if err := os.WriteFile(filepath.Join(dir, "main.tf"), origMain, 0o644); err != nil {
		errs = append(errs, fmt.Sprintf("restore main.tf: %v", err))
	}
	if err := os.Remove(filepath.Join(dir, "backend.tf")); err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Sprintf("remove backend.tf: %v", err))
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// workspaceLocalBackendLineRE matches the single
// `  backend "local" {}` line WorkspaceMainTF writes inside the
// `terraform { ... }` block. This is the only shape Init has ever
// emitted (unlike the pipeline remote-state block below, it has no
// older-version variants to match).
var workspaceLocalBackendLineRE = regexp.MustCompile(`(?m)^[ \t]*backend[ \t]+"local"[ \t]*\{\}[ \t]*\n`)

func stripWorkspaceLocalBackend(src []byte) []byte {
	return workspaceLocalBackendLineRE.ReplaceAll(src, nil)
}

// remoteStateBlockMarker opens every version of the pipeline's local
// remote-state read, whatever the internal formatting.
var remoteStateBlockMarker = []byte(`data "terraform_remote_state" "workspace"`)

// multiBlankLineRE collapses the blank-line gap a block removal can
// leave behind, so the rewritten main.tf doesn't accumulate blank runs
// over repeated (or partially-failed-and-retried) migrations.
var multiBlankLineRE = regexp.MustCompile(`\n{3,}`)

// stripLocalRemoteStateBlock removes a pipeline main.tf's
// `data "terraform_remote_state" "workspace" { ... }` block — but only
// the local-backend shape (`backend = "local"` somewhere inside it); an
// s3-backend remote-state block, which shouldn't exist in main.tf at
// this point but is left alone defensively, never matches.
//
// The block is found by brace-counting from its opening `{`, not by a
// fixed-format regex: older clavesa versions wrote it with different
// internal formatting — CreatePipeline's current shape is
//
//	data "terraform_remote_state" "workspace" {
//	  backend = "local"
//	  config  = { path = "${path.module}/../terraform.tfstate" }
//	}
//
// but the pre-v1.1.x layout (when the workspace lived in a `_workspace/`
// subdirectory) wrote the same block with a different config value:
//
//	data "terraform_remote_state" "workspace" {
//	  backend = "local"
//	  config  = { path = "${path.module}/../_workspace/terraform.tfstate" }
//	}
//
// A regex anchored on the exact path string would miss the second shape
// (or any other hand-edited variant); brace-counting only cares that
// the block is well-formed HCL, which both shapes are. Returns the
// input unchanged (and false) if no local-backend block is found.
func stripLocalRemoteStateBlock(src []byte) ([]byte, bool) {
	idx := bytes.Index(src, remoteStateBlockMarker)
	if idx == -1 {
		return src, false
	}
	open := bytes.IndexByte(src[idx:], '{')
	if open == -1 {
		return src, false
	}
	open += idx

	depth := 0
	end := -1
	for i := open; i < len(src); i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				end = i + 1
			}
		}
		if end != -1 {
			break
		}
	}
	if end == -1 {
		return src, false // unbalanced braces — leave untouched
	}

	block := src[idx:end]
	if !bytes.Contains(block, []byte(`"local"`)) {
		return src, false // an s3 block, or something else entirely
	}

	out := make([]byte, 0, len(src)-(end-idx))
	out = append(out, src[:idx]...)
	out = append(out, src[end:]...)
	return multiBlankLineRE.ReplaceAll(out, []byte("\n\n")), true
}

// ---------------------------------------------------------------------------
// Post-init local cleanup (ADR-025 step "d")
// ---------------------------------------------------------------------------

// finalizeLocalState cleans up after a successful
// `terraform init -migrate-state`, once MigrateState has confirmed a
// non-empty S3 object at the stack's key. What Terraform leaves behind
// depends on how the old backend was declared:
//
//   - Implicit local backend (pipelines: no backend block): a 0-byte
//     terraform.tfstate, with the real state in terraform.tfstate.backup.
//     The backup becomes terraform.tfstate.pre-migrate and the 0-byte
//     file is removed.
//   - Explicit `backend "local" {}` (the workspace root): terraform.tfstate
//     is left as it was, still full. It becomes
//     terraform.tfstate.pre-migrate, and any older terraform.tfstate.backup
//     is left alone. Without this rename the deploy guard would refuse the
//     stack forever for still having local state.
//
// Either way the old state is kept on disk under one well-known name,
// never deleted, so a botched migration can be recovered by hand.
func finalizeLocalState(dir string) error {
	statePath := filepath.Join(dir, "terraform.tfstate")
	backupPath := filepath.Join(dir, "terraform.tfstate.backup")
	preMigratePath := filepath.Join(dir, "terraform.tfstate.pre-migrate")

	info, err := os.Stat(statePath)
	switch {
	case err == nil && info.Size() > 0:
		if err := os.Rename(statePath, preMigratePath); err != nil {
			return fmt.Errorf("rename terraform.tfstate: %w", err)
		}
		return nil
	case err != nil && !os.IsNotExist(err):
		return fmt.Errorf("stat terraform.tfstate: %w", err)
	}

	if _, err := os.Stat(backupPath); err == nil {
		if err := os.Rename(backupPath, preMigratePath); err != nil {
			return fmt.Errorf("rename terraform.tfstate.backup: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat terraform.tfstate.backup: %w", err)
	}

	if err := os.Remove(statePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove 0-byte terraform.tfstate: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Production seam implementations — AWS + terraform. Swapped out in
// tests via the Service fields above (checkStateBucket, stateObjectStatus,
// runTerraform), same pattern as workspace.SetStateGetterForTest.
// ---------------------------------------------------------------------------

// checkStateBucketS3 is the production ADR-025 state-bucket precondition
// check: the bucket exists (implied by GetBucketVersioning succeeding),
// has versioning Enabled, and has a default encryption configuration.
func checkStateBucketS3(ctx context.Context, bucket, region string) error {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)

	verOut, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return fmt.Errorf("check bucket %s versioning: %w", bucket, err)
	}
	if verOut.Status != types.BucketVersioningStatusEnabled {
		return fmt.Errorf("state bucket %s does not have versioning enabled (status=%q)", bucket, verOut.Status)
	}

	encOut, err := client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: aws.String(bucket)})
	if err != nil {
		return fmt.Errorf("check bucket %s encryption: %w", bucket, err)
	}
	if encOut.ServerSideEncryptionConfiguration == nil || len(encOut.ServerSideEncryptionConfiguration.Rules) == 0 {
		return fmt.Errorf("state bucket %s has no default encryption configured", bucket)
	}
	return nil
}

// stateObjectStatusS3 is the production S3 HeadObject check backing the
// "refuse to overwrite" and "confirm the migration landed" checks.
func stateObjectStatusS3(ctx context.Context, bucket, region, key string) (bool, int64, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return false, 0, fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFoundS3(err) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("head s3://%s/%s: %w", bucket, key, err)
	}
	return true, aws.ToInt64(out.ContentLength), nil
}

// isNotFoundS3 matches the SDK v2 not-found shape the same way
// workspace.isNoSuchKey does: a typed error code first, falling back to
// a message match for the untyped-404 case HeadObject returns.
func isNotFoundS3(err error) bool {
	var nf interface{ ErrorCode() string }
	if errors.As(err, &nf) {
		switch nf.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	return strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "NoSuchKey")
}

// runTerraformExec is the production terraform runner: a plain
// `terraform <args...>` in dir, inheriting the parent process's
// environment (AWS_PROFILE / AWS_REGION / ~/.aws), same as deployFlow.tf
// in internal/cli/deploy.go. exitCode is terraform's own exit status;
// err is non-nil only when the binary itself couldn't be started.
func runTerraformExec(ctx context.Context, dir string, stdout, stderr io.Writer, args ...string) (int, error) {
	c := exec.CommandContext(ctx, "terraform", args...)
	c.Dir = dir
	c.Stdout = stdout
	c.Stderr = stderr
	err := c.Run()
	if err == nil {
		return 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), nil
	}
	return -1, err
}

// restoreLocalStateFromBackup undoes the local half of a failed
// `terraform init -migrate-state`: when terraform.tfstate is missing or
// 0 bytes and terraform.tfstate.backup is not, the backup is copied back
// (copied, not renamed, so the backup survives too). Nothing happens when
// terraform.tfstate still has content.
func restoreLocalStateFromBackup(dir string) error {
	statePath := filepath.Join(dir, "terraform.tfstate")
	if info, err := os.Stat(statePath); err == nil && info.Size() > 0 {
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat terraform.tfstate: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "terraform.tfstate.backup"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read terraform.tfstate.backup: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	return os.WriteFile(statePath, data, 0o644)
}

// nonEmptyLocalStateCopy returns the name of a non-empty local state copy
// left in dir (terraform.tfstate.backup or terraform.tfstate.pre-migrate),
// or "" when there is none.
func nonEmptyLocalStateCopy(dir string) string {
	for _, name := range []string{"terraform.tfstate.backup", "terraform.tfstate.pre-migrate"} {
		if info, err := os.Stat(filepath.Join(dir, name)); err == nil && info.Size() > 0 {
			return name
		}
	}
	return ""
}
