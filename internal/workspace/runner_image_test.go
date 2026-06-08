package workspace

import (
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/runnerreqs"
)

// dockerAvailable reports whether a working Docker daemon is reachable.
// Mirrors the make test-runner gating: the build-path tests are skipped
// when Docker isn't present rather than failing the whole suite.
func dockerAvailable() bool {
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	return exec.Command("docker", "info").Run() == nil
}

func imageTagExists(ref string) bool {
	return exec.Command("docker", "image", "inspect", ref).Run() == nil
}

// TestEnsureLocalRunnerImageRestoresVersionTag reproduces the v2.7.4 deploy
// blocker: a version bump that ships no runner-file changes left the
// `:<version>` tag uncreated because the old SHA-label fast path returned
// `:latest` as current without ever minting the version alias. The deploy
// preflight and the ECR push provisioner both require that version tag.
//
// With the unconditional-build design, ensureLocalRunnerImageAt rebuilds
// (cache hit) and re-tags both `:latest` and `:<version>` every call, so a
// missing version tag is always restored.
func TestEnsureLocalRunnerImageRestoresVersionTag(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("docker not available; skipping runner-image build test")
	}

	const ver = "v0.5.0"
	dir := t.TempDir()
	name := "test-runner-img"
	localTag := runner.LocalImageName(name)
	versioned := localTag + ":" + ver
	latest := localTag + ":latest"

	// Init builds the image, tagging both :latest and :<version>.
	if err := Init(dir, name, "aws", "", ver); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rmi", "-f", versioned, latest).Run()
	})

	if !imageTagExists(versioned) {
		t.Fatalf("expected %s to exist after Init", versioned)
	}
	if !imageTagExists(latest) {
		t.Fatalf("expected %s to exist after Init", latest)
	}

	// Reproduce the bug state: drop the version tag, keep :latest current.
	if out, err := exec.Command("docker", "rmi", versioned).CombinedOutput(); err != nil {
		t.Fatalf("docker rmi %s: %v (%s)", versioned, err, out)
	}
	if imageTagExists(versioned) {
		t.Fatalf("%s should be gone after rmi", versioned)
	}
	if !imageTagExists(latest) {
		t.Fatalf("%s should still exist after rmi of the version tag", latest)
	}

	// The fix: ensureLocalRunnerImageAt rebuilds (cache hit) and restores the
	// version tag. A second call must also succeed (idempotent cache-hit path).
	for i := 0; i < 2; i++ {
		tag, err := ensureLocalRunnerImageAt(dir, ver)
		if err != nil {
			t.Fatalf("ensureLocalRunnerImageAt (call %d): %v", i, err)
		}
		if tag != latest {
			t.Fatalf("ensureLocalRunnerImageAt returned %q, want %q", tag, latest)
		}
		if !imageTagExists(versioned) {
			t.Fatalf("%s not restored after ensureLocalRunnerImageAt (call %d)", versioned, i)
		}
	}
}

// TestRunnerRequirementsReachImage proves the runner-extensibility mechanism
// end to end: a package listed in the workspace's
// .clavesa/runner-requirements.txt is pip-installed into the built image (and
// thus importable in transform UDFs), while the empty/comment-only default
// builds cleanly and installs nothing extra.
func TestRunnerRequirementsReachImage(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("docker not available; skipping runner-image deps test")
	}

	const ver = "v0.5.0"
	dir := t.TempDir()
	name := "test-runner-deps"
	localTag := runner.LocalImageName(name)
	versioned := localTag + ":" + ver
	latest := localTag + ":latest"

	// Init builds the image with only the seeded comment-only requirements
	// (no extra packages). A pure-stdlib import that is NOT in the base image
	// is the negative control.
	if err := Init(dir, name, "aws", "", ver); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rmi", "-f", versioned, latest).Run()
	})

	importOK := func(pkg string) bool {
		return exec.Command("docker", "run", "--rm", "--entrypoint", "python",
			latest, "-c", "import "+pkg).Run() == nil
	}

	// `cowsay` is a tiny dependency-free pure-Python package not pulled in by
	// the base image's requirements (pyspark/pandas/boto3 + their closure).
	// Before opting in, importing it must fail.
	if importOK("cowsay") {
		t.Fatalf("base image unexpectedly already has `cowsay`; pick a different probe package")
	}

	// Opt in via the workspace requirements file, rebuild, and confirm the
	// package is now importable inside the image.
	if err := runnerreqs.Write(dir, "cowsay\n"); err != nil {
		t.Fatalf("write runner requirements: %v", err)
	}
	if _, err := ensureLocalRunnerImageAt(dir, ver); err != nil {
		t.Fatalf("rebuild with extra dep: %v", err)
	}
	if !importOK("cowsay") {
		t.Fatalf("`cowsay` not importable after adding it to %s", filepath.Base(runnerreqs.Path(dir)))
	}
}
