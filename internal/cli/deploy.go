package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// deployFlow is the substantive `terraform init -upgrade → plan → apply`
// sequence shared by `clavesa workspace deploy` and `clavesa
// pipeline deploy`. Replaces the previous one-line `terraform apply`
// shells so destructive surprises stay reviewable behind a saved plan
// and preflight failures abort before any AWS state changes.
type deployFlow struct {
	// WorkspaceRoot is the workspace dir (the one with clavesa.json).
	// Always set; the preflight refuses to run if clavesa.json isn't
	// present here.
	WorkspaceRoot string
	// TfDir is where terraform actually runs — equal to WorkspaceRoot
	// for `workspace deploy`, equal to a pipeline subdirectory for
	// `pipeline deploy`.
	TfDir string
	// VerifyRunnerImage gates `workspace deploy` only — the workspace's
	// `null_resource.push_runner` retags + pushes the local image, so a
	// stale image silently lands in production. Pipeline deploys don't
	// push images (they pin the Lambda to an ECR digest), so skip there.
	VerifyRunnerImage bool
	// AutoApprove skips the interactive "Apply this plan? [y/N]" prompt.
	// For scripted / CI use; the prompt is the default safety.
	AutoApprove bool
	// PlanOnly stops after `terraform plan -out=tfplan` without applying.
	// Useful for "what would change" inspection without destructive risk.
	PlanOnly bool
	// In/Out/Err override stdin/stdout/stderr — leave nil to use os.*
	// (tests inject buffers).
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

// planFilename is the saved-plan file written by `plan` and consumed by
// `apply`. Cleared on success / failure so a stale plan can never get
// applied against a different state.
const planFilename = "tfplan"

func (d deployFlow) stdout() io.Writer {
	if d.Out != nil {
		return d.Out
	}
	return os.Stdout
}

func (d deployFlow) stderr() io.Writer {
	if d.Err != nil {
		return d.Err
	}
	return os.Stderr
}

func (d deployFlow) stdin() io.Reader {
	if d.In != nil {
		return d.In
	}
	return os.Stdin
}

// Run executes the deploy flow. Returns a non-nil error if any step
// fails — caller should print the error and exit non-zero.
func (d deployFlow) Run() error {
	if err := d.preflight(); err != nil {
		return err
	}

	planPath := filepath.Join(d.TfDir, planFilename)
	// Best-effort: remove any stale plan from a previous interrupted run.
	// A stale plan from a different state would be rejected by terraform
	// apply, but cleaning up keeps the directory tidy.
	_ = os.Remove(planPath)

	if err := d.tf("init", "-upgrade", "-input=false"); err != nil {
		return fmt.Errorf("terraform init: %w", err)
	}

	if err := d.tf("plan", "-out="+planFilename, "-input=false"); err != nil {
		return fmt.Errorf("terraform plan: %w", err)
	}

	if d.PlanOnly {
		fmt.Fprintf(d.stdout(), "\nPlan saved to %s. Skipping apply (--plan-only).\n", planPath)
		return nil
	}

	if !d.AutoApprove {
		if err := d.confirm(); err != nil {
			// Cleanup the plan on cancel so it doesn't accidentally get
			// applied later from a stale state.
			_ = os.Remove(planPath)
			return err
		}
	}

	if err := d.tf("apply", "-input=false", planFilename); err != nil {
		// Keep the plan around on apply failure — the user may want to
		// inspect it or retry against the same plan.
		return fmt.Errorf("terraform apply: %w", err)
	}

	// Success: remove the saved plan. A plan that already applied is
	// strictly worse than no plan — terraform apply against the same
	// file again would either no-op or error.
	_ = os.Remove(planPath)
	return nil
}

func (d deployFlow) preflight() error {
	manifestPath := filepath.Join(d.WorkspaceRoot, "clavesa.json")
	if _, err := os.Stat(manifestPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("not a clavesa workspace: %s (no clavesa.json — run `clavesa workspace init` first)", d.WorkspaceRoot)
		}
		return fmt.Errorf("read clavesa.json: %w", err)
	}

	if err := d.verifyAWSCredentials(); err != nil {
		return err
	}

	if !d.VerifyRunnerImage {
		return nil
	}

	m, err := workspace.Load(d.WorkspaceRoot)
	if err != nil {
		return fmt.Errorf("load manifest for runner-image check: %w", err)
	}
	if m == nil {
		// Stat said the file exists; Load returned nil only when the
		// file is missing. Treat the same as the stat branch.
		return fmt.Errorf("clavesa.json could not be read at %s", d.WorkspaceRoot)
	}
	return workspace.VerifyRunnerImage(m.Name, service.ModuleVersion)
}

// verifyAWSCredentials runs sts:GetCallerIdentity against the default
// credential chain so a missing/expired profile fails fast — before
// `terraform init -upgrade` writes `.terraform/` and provider plugin
// caches. The provider would surface the same error eventually, but
// after a state-mutating step; recoverable, but the .terraform/ then
// needs cleaning before retry.
func (d deployFlow) verifyAWSCredentials() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w (check AWS_PROFILE / AWS_REGION / ~/.aws/credentials)", err)
	}
	client := sts.NewFromConfig(cfg)
	out, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("AWS credentials check failed: %w (check AWS_PROFILE / AWS_REGION / ~/.aws/credentials)", err)
	}
	fmt.Fprintf(d.stdout(), "→ AWS account %s as %s\n", derefStr(out.Account), derefStr(out.Arn))
	return nil
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (d deployFlow) tf(args ...string) error {
	c := exec.Command("terraform", args...)
	c.Dir = d.TfDir
	c.Stdout = d.stdout()
	c.Stderr = d.stderr()
	c.Stdin = d.stdin()
	return c.Run()
}

func (d deployFlow) confirm() error {
	fmt.Fprintf(d.stdout(), "\nApply this plan? Type 'yes' to confirm (anything else cancels): ")
	r := bufio.NewReader(d.stdin())
	line, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("read confirmation: %w", err)
	}
	if strings.TrimSpace(line) != "yes" {
		return fmt.Errorf("deploy cancelled")
	}
	return nil
}
