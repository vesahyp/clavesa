package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/version"
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
	// BuildRunnerImage gates `workspace deploy` only — it owns the runner
	// image lifecycle. The local image is rebuilt in preflight (docker
	// decides what work is needed), and after the apply that creates the ECR
	// repository the image is pushed to ECR unconditionally (see
	// pushRunnerImage). Pipeline deploys don't build or push images (they pin
	// the Lambda to an ECR digest), so both steps skip there.
	BuildRunnerImage bool
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

	// Workspace deploy owns the runner-image lifecycle. The image was rebuilt
	// in preflight; now (with the ECR repository created by the apply above)
	// push it. The push is unconditional — no staleness gate — so a rebuild
	// under an unchanged module version still reaches ECR. ECR's
	// content-addressed layers dedup, so an unchanged image re-pushes only
	// metadata. This replaces the version-gated null_resource.push_runner,
	// which skipped the push whenever runner_version was unchanged.
	if d.BuildRunnerImage {
		if err := d.pushRunnerImage(); err != nil {
			return err
		}
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

	if !d.BuildRunnerImage {
		return nil
	}

	// Build the local runner image (docker's layer cache decides what work is
	// needed) so a current `:<version>` tag exists for the post-apply push.
	// Cheap when nothing changed.
	if _, err := workspace.EnsureLocalRunnerImage(d.WorkspaceRoot); err != nil {
		return fmt.Errorf("build runner image before deploy: %w", err)
	}
	return nil
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

// pushRunnerImage tags the freshly-built local runner image into the
// workspace ECR repository and pushes it — unconditionally. The local image
// is rebuilt on every deploy (preflight), so the push mirrors that: always
// push, let ECR's content-addressed layers decide what actually uploads (an
// unchanged image re-pushes only metadata). Must run after the apply that
// creates aws_ecr_repository.runner. Called for workspace deploy only.
func (d deployFlow) pushRunnerImage() error {
	fmt.Fprintf(d.stdout(), "\n→ Pushing runner image to ECR…\n")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	m, err := workspace.Load(d.WorkspaceRoot)
	if err != nil {
		return fmt.Errorf("push runner image: load workspace: %w", err)
	}
	if m == nil {
		return fmt.Errorf("push runner image: %s is not a clavesa workspace", d.WorkspaceRoot)
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("push runner image: load AWS config: %w", err)
	}
	region := cfg.Region
	if region == "" {
		return fmt.Errorf("push runner image: no AWS region configured (set AWS_REGION or a profile region)")
	}
	ident, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("push runner image: resolve AWS account: %w", err)
	}

	registry := fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", derefStr(ident.Account), region)
	// Mirrors aws_ecr_repository.runner.name in the workspace main.tf.
	repoURL := fmt.Sprintf("%s/clavesa-%s/transform-runner", registry, m.Name)
	// The build (EnsureLocalRunnerImage) always tags the local image with the
	// binary's version.Module, so that tag is guaranteed present here.
	localImage := runner.LocalImageName(m.Name) + ":" + version.Module

	if err := dockerLoginECR(ctx, cfg, registry, d.stderr()); err != nil {
		return fmt.Errorf("push runner image: %w", err)
	}

	// pipeline deploy pins the Lambda to the :latest digest, so :latest must
	// always carry the current image; also push the versioned tag for refs.
	for _, tag := range []string{version.Module, "latest"} {
		if err := d.docker(ctx, "tag", localImage, repoURL+":"+tag); err != nil {
			return fmt.Errorf("push runner image: docker tag %s: %w", tag, err)
		}
	}
	for _, tag := range []string{version.Module, "latest"} {
		if err := d.docker(ctx, "push", repoURL+":"+tag); err != nil {
			return fmt.Errorf("push runner image: docker push %s: %w", tag, err)
		}
	}
	return nil
}

func (d deployFlow) docker(ctx context.Context, args ...string) error {
	c := exec.CommandContext(ctx, "docker", args...)
	c.Stdout = d.stderr()
	c.Stderr = d.stderr()
	return c.Run()
}

// dockerLoginECR fetches an ECR authorization token via the AWS SDK and runs
// `docker login --password-stdin`, so the deploy flow needs only the AWS SDK
// credential chain — not the `aws` CLI the old null_resource.push_runner
// provisioner shelled out to.
func dockerLoginECR(ctx context.Context, cfg aws.Config, registry string, errOut io.Writer) error {
	out, err := ecr.NewFromConfig(cfg).GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{})
	if err != nil {
		return fmt.Errorf("ECR authorization: %w", err)
	}
	if len(out.AuthorizationData) == 0 || out.AuthorizationData[0].AuthorizationToken == nil {
		return fmt.Errorf("ECR authorization: empty token")
	}
	decoded, err := base64.StdEncoding.DecodeString(*out.AuthorizationData[0].AuthorizationToken)
	if err != nil {
		return fmt.Errorf("decode ECR token: %w", err)
	}
	creds := strings.SplitN(string(decoded), ":", 2)
	if len(creds) != 2 {
		return fmt.Errorf("malformed ECR token")
	}
	c := exec.CommandContext(ctx, "docker", "login", "--username", "AWS", "--password-stdin", registry)
	c.Stdin = strings.NewReader(creds[1])
	c.Stdout = errOut
	c.Stderr = errOut
	if err := c.Run(); err != nil {
		return fmt.Errorf("docker login to %s: %w", registry, err)
	}
	return nil
}

func (d deployFlow) tf(args ...string) error {
	c := exec.Command("terraform", args...)
	c.Dir = d.TfDir
	c.Stdout = d.stdout()
	c.Stderr = d.stderr()
	c.Stdin = d.stdin()
	return c.Run()
}

// tfInit runs `terraform init -input=false` in dir before a raw `terraform
// <cmd>` shell-out (plan / destroy from outside the deployFlow). Idempotent
// when `.terraform/` is current; on a fresh checkout or after `pipeline
// upgrade` rewrites module versions it pulls the providers so the next
// command doesn't fail with "Initialization required".
func tfInit(dir string, stdout, stderr io.Writer) error {
	c := exec.Command("terraform", "init", "-input=false")
	c.Dir = dir
	c.Stdout = stdout
	c.Stderr = stderr
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
