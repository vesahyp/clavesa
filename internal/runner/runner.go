// Package runner embeds the Clavesa transform runner source files so the
// CLI can extract them at workspace init time without requiring the user to
// have the source tree present.
package runner

import (
	"embed"
)

// FS contains Dockerfile, runner.py, requirements.txt, and entrypoint.sh.
//
//go:embed files/*
var FS embed.FS

// LocalImageName returns the local Docker image name for a workspace's
// transform runner, namespaced to avoid collisions between workspaces.
func LocalImageName(workspaceName string) string {
	return "clavesa/" + workspaceName + "/transform-runner"
}
