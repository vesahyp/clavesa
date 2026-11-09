package workspace

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// RunnerArchitectureTFFile is the clavesa-owned workspace file that
// records which Lambda architecture the pushed runner image needs. Written
// by `workspace deploy` before every apply, from the image it is about to
// push; pipeline stacks read the output through their workspace remote
// state.
const RunnerArchitectureTFFile = "runner_architecture.tf"

// LambdaArchitecture maps a Docker image architecture to Lambda's name.
func LambdaArchitecture(dockerArch string) (string, error) {
	switch dockerArch {
	case "amd64":
		return "x86_64", nil
	case "arm64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("runner image architecture %q is not one Lambda runs (amd64 or arm64)", dockerArch)
	}
}

// LocalImageArchitecture returns the Lambda architecture of a local image.
func LocalImageArchitecture(tag string) (string, error) {
	out, err := exec.Command("docker", "image", "inspect", tag, "--format", "{{.Architecture}}").Output()
	if err != nil {
		return "", fmt.Errorf("inspect runner image %s: %w", tag, err)
	}
	return LambdaArchitecture(strings.TrimSpace(string(out)))
}

// RenderRunnerArchitectureTF renders runner_architecture.tf.
func RenderRunnerArchitectureTF(lambdaArch string) string {
	return fmt.Sprintf(`# clavesa-managed: the Lambda architecture of the runner image this
# workspace pushed. Rewritten by every `+"`clavesa workspace deploy`"+` from the
# image it builds; do not edit by hand.
output "runner_architecture" {
  description = "Lambda architecture of the pushed runner image (x86_64 or arm64)."
  value       = %q
}
`, lambdaArch)
}

// WriteRunnerArchitectureTF writes runner_architecture.tf into root.
func WriteRunnerArchitectureTF(root, lambdaArch string) error {
	return os.WriteFile(filepath.Join(root, RunnerArchitectureTFFile), []byte(RenderRunnerArchitectureTF(lambdaArch)), 0o644)
}
