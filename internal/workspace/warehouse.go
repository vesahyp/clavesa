package workspace

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Warehouse is where all workspace state lives (ADR-024): catalog +
// data (local Hadoop dir vs Glue + S3), observability provider, serving
// read engine, watermarks and source cursors. It decides whether clavesa
// authors and operates against the local runner + Hadoop catalog, or
// against the deployed cloud pipeline. It is a per-developer, gitignored
// preference — distinct from compute (which machine executes heavy
// work; per-action, defaults to the warehouse) and from a transform's
// `compute` attr, which is the cloud deploy target. Default is
// WarehouseLocal.
type Warehouse string

const (
	WarehouseLocal Warehouse = "local"
	WarehouseCloud Warehouse = "cloud"
)

// ParseWarehouse converts a string to a Warehouse, reporting whether it
// was a recognized value. Used by the CLI / HTTP surfaces that set the
// warehouse, so an unknown value is a clear error rather than a silent
// coercion.
func ParseWarehouse(s string) (Warehouse, bool) {
	switch Warehouse(s) {
	case WarehouseLocal:
		return WarehouseLocal, true
	case WarehouseCloud:
		return WarehouseCloud, true
	}
	return WarehouseLocal, false
}

// environmentFile is the per-workspace warehouse file under .clavesa/.
// The filename predates the warehouse rename (ADR-024) and stays
// "environment.json" for disk compatibility with older binaries.
// Gitignored — see the workspace init .gitignore snippet.
const environmentFile = "environment.json"

// environmentDoc is the on-disk shape. Warehouse is the current key;
// Mode is the pre-ADR-024 legacy key. Reads prefer Warehouse and fall
// back to Mode; writes fill both with the same value so older binaries
// keep reading the file.
type environmentDoc struct {
	Warehouse string `json:"warehouse,omitempty"`
	Mode      string `json:"mode,omitempty"`
}

// EnvironmentFilePath returns the path of the warehouse file for the
// workspace rooted at root. The name reflects the on-disk filename
// (environment.json), kept for compatibility.
func EnvironmentFilePath(root string) string {
	return filepath.Join(root, ".clavesa", environmentFile)
}

// LoadWarehouse reports the workspace warehouse. An absent file, an
// unreadable file, malformed JSON, or an unrecognized value all resolve
// to WarehouseLocal — local is the safe default and the only warehouse
// that needs no deployed cloud infrastructure. The legacy "mode" key is
// honoured when "warehouse" is absent. Pure read: it never writes the
// file back, so it is safe to call on every dispatch.
func LoadWarehouse(root string) Warehouse {
	data, err := os.ReadFile(EnvironmentFilePath(root))
	if err != nil {
		return WarehouseLocal
	}
	var doc environmentDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return WarehouseLocal
	}
	v := doc.Warehouse
	if v == "" {
		v = doc.Mode
	}
	if Warehouse(v) == WarehouseCloud {
		return WarehouseCloud
	}
	return WarehouseLocal
}

// ErrWarehouseUndeployed reports a cloud-warehouse workspace whose shell
// hasn't been deployed: there is no pipeline bucket to point Spark at.
// Callers test with errors.Is to map it to a user-actionable failure
// (HTTP 409, CLI error) rather than a generic 500.
var ErrWarehouseUndeployed = errors.New(`workspace warehouse is "cloud" but the workspace shell isn't deployed (no pipeline bucket in terraform.tfstate)`)

// WarehouseURI returns the warehouse location the interactive Spark
// surfaces (preview, notebooks, the warm query worker) should target,
// following the workspace warehouse (ADR-024):
//
//   - WarehouseLocal → the workspace-shared Hadoop-catalog directory
//     (LocalWarehouseDir).
//   - WarehouseCloud → `s3://<pipeline-bucket>/_workspace/_warehouse/`.
//     The runner keys Glue Hive federation + S3SingleDriverLogStore on
//     the `s3://` prefix, so the same string flips the container into
//     cloud-catalog mode.
//
// A cloud warehouse with no deployed shell (PipelineBucket empty) is an
// error wrapping ErrWarehouseUndeployed — the caller surfaces it; there
// is no silent fallback to local.
func WarehouseURI(root string) (string, error) {
	if LoadWarehouse(root) == WarehouseCloud {
		bucket := PipelineBucket(root)
		if bucket == "" {
			return "", fmt.Errorf("%w — run `clavesa workspace deploy` to create it, or switch back with `clavesa workspace use --warehouse local`", ErrWarehouseUndeployed)
		}
		return "s3://" + bucket + "/_workspace/_warehouse/", nil
	}
	return LocalWarehouseDir(root), nil
}

// WriteWarehouse persists the workspace warehouse, creating .clavesa/
// if needed. An unrecognized value is coerced to WarehouseLocal. Both
// the "warehouse" key and the legacy "mode" key are written with the
// same value so older binaries still read the file. Written by
// `workspace use --warehouse` and the HTTP environment endpoint;
// dispatch paths only read.
func WriteWarehouse(root string, wh Warehouse) error {
	if wh != WarehouseCloud {
		wh = WarehouseLocal
	}
	dir := filepath.Join(root, ".clavesa")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(environmentDoc{Warehouse: string(wh), Mode: string(wh)}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, environmentFile), append(data, '\n'), 0o644)
}
