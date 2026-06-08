package service

import (
	"fmt"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// PipelineUpgradeResult is one row of a workspace-wide upgrade: the
// per-pipeline outcome of the embedded UpgradePipeline call. Err is
// non-empty when that pipeline failed to upgrade; the rest of the row is
// still populated with whatever progress was made, and the other
// pipelines in the same UpgradeWorkspace call still ran (continue-on-error).
type PipelineUpgradeResult struct {
	Name       string `json:"name"`
	Dir        string `json:"dir"` // workspace-relative
	CurrentRef string `json:"current_ref"`
	TargetRef  string `json:"target_ref"`
	Updated    int    `json:"updated"`
	Migrated   int    `json:"migrated"`
	// Err is the upgrade failure message for this pipeline, "" on success.
	Err string `json:"err,omitempty"`
}

// WorkspaceUpgradeResult is the combined outcome of upgrading the
// workspace shell plus (optionally) every pipeline in it.
type WorkspaceUpgradeResult struct {
	PrevVersion   string `json:"prev_version"`
	TargetVersion string `json:"target_version"`
	// WorkspaceRewritten counts the workspace-shell files rewritten
	// (main.tf module source and/or variables.tf runner_version default).
	WorkspaceRewritten int `json:"workspace_rewritten"`
	// Pipelines is one row per pipeline when includePipelines was true,
	// nil for a shell-only upgrade.
	Pipelines []PipelineUpgradeResult `json:"pipelines"`
}

// UpgradeWorkspace upgrades the workspace shell to target (the binary's
// ModuleVersion when target is empty) and, when includePipelines is true,
// every pipeline in the workspace too.
//
// The shell upgrade (workspace.Upgrade) is fatal — a failure there aborts
// the whole operation and returns the error. Per-pipeline upgrades are
// continue-on-error: a failing pipeline records its error in that row's
// Err and the loop moves on, so one broken pipeline doesn't block the
// rest. The method itself returns a nil error in that case; callers
// inspect each row's Err.
//
// Pure-TF / Docker-free: the runner image build
// (workspace.EnsureLocalRunnerImage) is the caller's job (the CLI
// and the HTTP handler both invoke it), so this stays exercisable in
// pure-Go tests.
func (s *Service) UpgradeWorkspace(target string, includePipelines bool) (WorkspaceUpgradeResult, error) {
	if target == "" {
		target = ModuleVersion
	}

	prev, rewritten, err := workspace.Upgrade(s.workspace, target)
	if err != nil {
		return WorkspaceUpgradeResult{}, fmt.Errorf("workspace upgrade: %w", err)
	}

	result := WorkspaceUpgradeResult{
		PrevVersion:        prev,
		TargetVersion:      target,
		WorkspaceRewritten: rewritten,
	}

	if !includePipelines {
		return result, nil
	}

	pipelines, err := s.ListPipelines()
	if err != nil {
		return result, fmt.Errorf("list pipelines: %w", err)
	}

	result.Pipelines = make([]PipelineUpgradeResult, 0, len(pipelines))
	for _, info := range pipelines {
		row := PipelineUpgradeResult{Name: info.Name, Dir: info.Dir, TargetRef: target}
		// info.Dir is workspace-relative; UpgradePipeline calls
		// resolveDir internally, so pass it straight through.
		current, finalRef, updated, migrated, upErr := s.UpgradePipeline(info.Dir, target)
		row.CurrentRef = current
		if finalRef != "" {
			row.TargetRef = finalRef
		}
		row.Updated = updated
		row.Migrated = migrated
		if upErr != nil {
			row.Err = upErr.Error()
		}
		result.Pipelines = append(result.Pipelines, row)
	}

	return result, nil
}
