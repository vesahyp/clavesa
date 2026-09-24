package workspace

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// minTerraformVersion is the floor for a remote-backed workspace (ADR-025,
// "Locking"): S3-native locking (`use_lockfile`) needs Terraform 1.10 or
// newer. Local-backend workspaces keep today's unrestricted floor —
// nothing here is called unless a backend is configured.
var minTerraformVersion = [3]int{1, 10, 0}

// tfVersionLineRE matches the first line of `terraform version` output,
// e.g. "Terraform v1.14.5". Pre-release/build suffixes (a rare local dev
// build) fall through to the "garbage" branch of CheckTerraformVersion —
// intentional, since a floor check on a non-release binary isn't
// meaningful.
var tfVersionLineRE = regexp.MustCompile(`^Terraform v(\d+)\.(\d+)\.(\d+)\s*$`)

// CheckTerraformVersion parses the first line of `terraform version`
// output and returns an error if it is below minTerraformVersion, or if
// the line can't be parsed as a plain release version. Pure — no exec —
// so it's unit-testable without a terraform binary on PATH.
func CheckTerraformVersion(versionOutput string) error {
	firstLine, _, _ := strings.Cut(versionOutput, "\n")
	firstLine = strings.TrimSpace(firstLine)
	m := tfVersionLineRE.FindStringSubmatch(firstLine)
	if m == nil {
		return fmt.Errorf("could not parse terraform version from %q", firstLine)
	}
	major, _ := strconv.Atoi(m[1])
	minor, _ := strconv.Atoi(m[2])
	patch, _ := strconv.Atoi(m[3])
	got := [3]int{major, minor, patch}
	if versionLess(got, minTerraformVersion) {
		return fmt.Errorf("terraform %d.%d.%d is below the required %d.%d.%d for a remote backend (ADR-025)",
			major, minor, patch, minTerraformVersion[0], minTerraformVersion[1], minTerraformVersion[2])
	}
	return nil
}

// versionLess reports whether a is a lower [major, minor, patch] than b.
func versionLess(a, b [3]int) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// TerraformVersionOK runs `terraform version` and checks the result
// against minTerraformVersion (ADR-025, "Locking"). Thin wrapper around
// CheckTerraformVersion — callers that already have output in hand
// (tests, or output captured for another reason) should call that
// directly instead.
func TerraformVersionOK(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "terraform", "version").Output()
	if err != nil {
		return fmt.Errorf("run terraform version: %w", err)
	}
	return CheckTerraformVersion(string(out))
}
