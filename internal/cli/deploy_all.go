package cli

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// newDeployCmd is the top-level `clavesa deploy` — apply the workspace infra
// and then every pipeline in the workspace, in one command. The workspace
// goes first because each pipeline's main.tf reads
// data.terraform_remote_state.workspace from ../terraform.tfstate; a pipeline
// can't plan against a workspace that hasn't been applied.
func newDeployCmd() *cobra.Command {
	var autoApprove bool
	var planOnly bool
	cmd := &cobra.Command{
		Use:   "deploy",
		Short: "Apply the workspace infra and every pipeline in one pass",
		Long: `Deploy the whole workspace: apply the workspace terraform first, then
every pipeline in it.

The workspace is applied before any pipeline because each pipeline reads
data.terraform_remote_state.workspace from ../terraform.tfstate, so a
pipeline can't plan against a workspace that hasn't been applied. If the
workspace apply fails the run aborts before touching any pipeline.

Each pipeline first regenerates its orchestration.tf from this binary's
emitter (orchestration.tf is a generated file; manual edits don't survive),
then runs its own init → plan → apply lifecycle. A pipeline failure is
reported and the run continues to the next pipeline; the command exits
non-zero if any pipeline failed.

Re-running is a cheap no-op. Terraform's plan and the ECR image digest
decide what actually changes; there's no hand-rolled staleness check, so a
no-change workspace and pipelines just re-plan and apply nothing.

Use --yes to skip the per-target confirmation prompts (for CI / scripted use).
Use --plan-only to stop after plan without applying (same as` + " `clavesa plan`" + `).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return deployAll(cmd, autoApprove, planOnly)
		},
	}
	cmd.Flags().BoolVarP(&autoApprove, "yes", "y", false, "skip the interactive 'Apply this plan?' confirmation")
	cmd.Flags().BoolVar(&planOnly, "plan-only", false, "stop after terraform plan (don't apply)")
	return cmd
}

// newPlanCmd is the top-level `clavesa plan` — the plan-only sibling of
// `clavesa deploy`. It plans the workspace infra and every pipeline without
// applying anything. No flags: plan never applies, so there's nothing to
// confirm.
func newPlanCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "plan",
		Short: "Plan the workspace infra and every pipeline (no apply)",
		Long: `Plan the whole workspace: terraform plan the workspace and every pipeline,
without applying anything. Equivalent to` + " `clavesa deploy --plan-only`" + `.

If the workspace hasn't been deployed yet (no ../terraform.tfstate), the
per-pipeline plans can't resolve data.terraform_remote_state.workspace, so
they're skipped with a note rather than reported as failures. The workspace
plan still runs and is the useful output in that case. Run` + " `clavesa deploy`" + `
once to lay down the workspace state, then` + " `clavesa plan`" + ` shows pipeline
diffs too.

Re-running is a cheap no-op: terraform's plan decides what would change;
there's no hand-rolled staleness check.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return deployAll(cmd, false, true)
		},
	}
}

// deployAll drives the deployFlow for the workspace and then every pipeline.
// Shared by `clavesa deploy` and `clavesa plan` (planOnly=true). The workspace
// runs first and aborts on error (pipelines depend on its state); pipelines
// run continue-on-error so one bad pipeline doesn't hide the rest, and the
// command exits non-zero if any pipeline failed.
func deployAll(cmd *cobra.Command, autoApprove, planOnly bool) error {
	root, err := resolveWorkspace(cmd)
	if err != nil {
		return err
	}
	svc, _, err := newService(cmd)
	if err != nil {
		return err
	}

	label := "deploy (workspace + all pipelines)"
	if planOnly {
		label = "plan (workspace + all pipelines)"
	}
	printTargetContext(label, root, "")

	// Workspace first — pipelines read its remote state. Abort on failure;
	// pipeline plans/applies would only error against missing state.
	fmt.Fprintf(cmd.OutOrStdout(), "\n=== Workspace ===\n")
	if err := (deployFlow{
		WorkspaceRoot:    root,
		TfDir:            root,
		BuildRunnerImage: true,
		AutoApprove:      autoApprove,
		PlanOnly:         planOnly,
	}).Run(); err != nil {
		return fmt.Errorf("workspace: %w", err)
	}

	pipelines, err := svc.ListPipelines()
	if err != nil {
		return fmt.Errorf("list pipelines: %w", err)
	}

	// On a plan against a never-deployed workspace the pipeline plans can't
	// resolve ../terraform.tfstate, so don't run them — report a skip instead.
	// (Detected once: an empty pipeline_bucket means no usable tfstate.)
	skipPipelines := planOnly && workspace.PipelineBucket(root) == ""

	var failures []string
	ok := 0
	for _, info := range pipelines {
		// info.Dir is workspace-relative; join with root for the absolute
		// TfDir terraform runs in.
		absDir := filepath.Join(root, info.Dir)
		if skipPipelines {
			fmt.Fprintf(cmd.OutOrStdout(), "\nPipeline %s: skipped (workspace not deployed yet; run `clavesa deploy` first)\n", info.Name)
			continue
		}
		fmt.Fprintf(cmd.OutOrStdout(), "\n=== Pipeline %s ===\n", info.Name)
		// Regenerate orchestration.tf from this binary's emitter before
		// terraform — `clavesa deploy` means "make everything current",
		// and that includes the orchestration shape, not just the apply.
		// Runs in plan-only mode too so `clavesa plan` previews exactly
		// what `clavesa deploy` would apply.
		if err := svc.SyncOrchestration(absDir, ""); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Pipeline %s failed: orchestration sync: %v\n", info.Name, err)
			failures = append(failures, fmt.Sprintf("%s: orchestration sync: %v", info.Name, err))
			continue
		}
		if err := (deployFlow{
			WorkspaceRoot:    root,
			TfDir:            absDir,
			BuildRunnerImage: false,
			AutoApprove:      autoApprove,
			PlanOnly:         planOnly,
		}).Run(); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Pipeline %s failed: %v\n", info.Name, err)
			failures = append(failures, fmt.Sprintf("%s: %v", info.Name, err))
			continue
		}
		ok++
	}

	verb := "deployed"
	if planOnly {
		verb = "planned"
	}
	fmt.Fprintf(cmd.OutOrStdout(), "\n=== Summary ===\n")
	if skipPipelines {
		fmt.Fprintf(cmd.OutOrStdout(), "Workspace %s. %d pipeline(s) skipped (workspace not deployed yet).\n", verb, len(pipelines))
		return nil
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Workspace %s. %d/%d pipeline(s) %s OK.\n", verb, ok, len(pipelines), verb)
	if len(failures) > 0 {
		fmt.Fprintf(cmd.ErrOrStderr(), "%d pipeline(s) failed:\n", len(failures))
		for _, f := range failures {
			fmt.Fprintf(cmd.ErrOrStderr(), "  - %s\n", f)
		}
		return fmt.Errorf("%d pipeline(s) failed", len(failures))
	}
	return nil
}
