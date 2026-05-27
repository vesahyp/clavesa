// Package sidecar embeds the Python source for the poller Lambda the
// orchestration TF emitter wires in: poller.py (SQS → SFN trigger).
//
// Previously this also embedded runs_writer/index.py (a separate
// zip-Lambda). ADR-018 swapped runs_writer to the runner image, so its
// handler now lives in runner.py and no per-pipeline Python ships here
// for it. The empty `_clavesa_sidecar/` directory still exists for
// the poller and any future sidecar Lambdas.
//
// tfgen-emitted orchestration.tf references these files via
// `${path.module}/_clavesa_sidecar/...`, so the service layer must
// materialise this directory into every pipeline dir before running
// `terraform apply`. See Materialise below.
package sidecar

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/vesahyp/clavesa/internal/orchestration/tfgen"
)

//go:embed poller.py
var files embed.FS

// Materialise copies every embedded sidecar file into
// <pipelineDir>/<sidecarDir>/..., creating directories as needed and
// overwriting on every call so emitter runs are idempotent. The sidecar
// directory name is the one tfgen-emitted .tf points at.
func Materialise(pipelineDir string) error {
	base := filepath.Join(pipelineDir, tfgen.SidecarDirName())
	if err := os.MkdirAll(base, 0o755); err != nil {
		return fmt.Errorf("sidecar: mkdir %s: %w", base, err)
	}
	return fs.WalkDir(files, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == "." {
				return nil
			}
			return os.MkdirAll(filepath.Join(base, path), 0o755)
		}
		data, err := files.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(base, path), data, 0o644)
	})
}
