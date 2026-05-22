package workspace

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Mode is the workspace environment lens. It decides whether clavesa
// authors and operates against the local runner +
// Hadoop catalog, or against the deployed cloud pipeline. It is a
// per-developer, gitignored preference — distinct from a transform's
// `compute` attr, which is the deploy target. Default is ModeLocal.
type Mode string

const (
	ModeLocal Mode = "local"
	ModeCloud Mode = "cloud"
)

// ParseMode converts a string to a Mode, reporting whether it was a
// recognized value. Used by the CLI / HTTP surfaces that set the mode,
// so an unknown value is a clear error rather than a silent coercion.
func ParseMode(s string) (Mode, bool) {
	switch Mode(s) {
	case ModeLocal:
		return ModeLocal, true
	case ModeCloud:
		return ModeCloud, true
	}
	return ModeLocal, false
}

// environmentFile is the per-workspace mode file under .clavesa/.
// Gitignored — see the workspace init .gitignore snippet.
const environmentFile = "environment.json"

type environmentDoc struct {
	Mode string `json:"mode"`
}

// EnvironmentFilePath returns the path of the environment-mode file for
// the workspace rooted at root.
func EnvironmentFilePath(root string) string {
	return filepath.Join(root, ".clavesa", environmentFile)
}

// LoadEnvironmentMode reports the workspace environment mode. An absent
// file, an unreadable file, malformed JSON, or an unrecognized value all
// resolve to ModeLocal — local is the safe default and the only mode
// that needs no deployed cloud infrastructure. Pure read: it never
// writes the file back, so it is safe to call on every dispatch.
func LoadEnvironmentMode(root string) Mode {
	data, err := os.ReadFile(EnvironmentFilePath(root))
	if err != nil {
		return ModeLocal
	}
	var doc environmentDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return ModeLocal
	}
	if Mode(doc.Mode) == ModeCloud {
		return ModeCloud
	}
	return ModeLocal
}

// WriteEnvironmentMode persists the workspace environment mode, creating
// .clavesa/ if needed. An unrecognized mode is coerced to ModeLocal.
// Written by `workspace use --env` and the HTTP env-mode endpoint;
// dispatch paths only read.
func WriteEnvironmentMode(root string, m Mode) error {
	if m != ModeCloud {
		m = ModeLocal
	}
	dir := filepath.Join(root, ".clavesa")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(environmentDoc{Mode: string(m)}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, environmentFile), append(data, '\n'), 0o644)
}
