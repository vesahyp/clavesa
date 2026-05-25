package preview

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/vesahyp/clavesa/internal/version"
)

// RunPreview executes a transform inside the Clavesa runner container and
// returns a map of output key → rows. Exactly one of sql or python should be
// non-empty.
//
// `localTag` is the full `<repo>:<tag>` reference for the workspace's
// local runner image; used when `image` is a Terraform interpolation
// (cloud authoring) instead of a concrete reference. Callers should
// pass `workspace.EnsureLocalRunnerImage(root, ModuleVersion)`'s
// return value so a stale local image gets refreshed automatically
// after a CLI upgrade.
//
// Calls dispatch through a package-level var so tests can swap in a stub via
// SetRunnerForTest, avoiding a Docker dependency in unit tests.
func RunPreview(ctx context.Context, localTag, image string, inputs map[string][]map[string]interface{}, sql, python string) (map[string][]map[string]interface{}, error) {
	return previewRunner(ctx, localTag, image, inputs, sql, python)
}

// runPreviewDocker is the production runner. Builds a `docker run` command
// against the Clavesa runner image with inputs/sql/python forwarded as env
// vars. The container must write a JSON object {"key": [...rows...]} to stdout.
func runPreviewDocker(ctx context.Context, localTag, image string, inputs map[string][]map[string]interface{}, sql, python string) (map[string][]map[string]interface{}, error) {
	if image == "" || strings.Contains(image, "${") || strings.HasPrefix(image, "data.") || strings.HasPrefix(image, "var.") {
		image = localTag
	}

	args := []string{"run", "--rm", "-e", "CLAVESA_PREVIEW=1"}
	// Override the baked-in CLAVESA_MODULE_VERSION ENV — the cache-retag
	// path in workspace.EnsureLocalRunnerImage can rebrand an image built
	// at a different version (same runner SHA, different build-arg), and
	// any tag-mismatched row would surface as wrong on the run-detail
	// triage strip. See service.runTransform for the same override.
	args = append(args, "-e", "CLAVESA_MODULE_VERSION="+version.Module)

	for alias, rows := range inputs {
		jsonBytes, err := json.Marshal(rows)
		if err != nil {
			return nil, fmt.Errorf("marshal input %q: %w", alias, err)
		}
		envKey := "CLAVESA_PREVIEW_INPUT_" + strings.ToUpper(strings.ReplaceAll(alias, "-", "_"))
		args = append(args, "-e", envKey+"="+string(jsonBytes))
	}

	if sql != "" {
		args = append(args, "-e", "CLAVESA_SQL="+sql)
	}
	if python != "" {
		args = append(args, "-e", "CLAVESA_PYTHON_SCRIPT="+python)
	}

	args = append(args, image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("container preview: %w\n%s", err, msg)
		}
		return nil, fmt.Errorf("container preview: %w", err)
	}

	var result map[string][]map[string]interface{}
	if err := json.Unmarshal(out, &result); err != nil {
		return nil, fmt.Errorf("parse container output: %w", err)
	}
	return result, nil
}

// QueryWarehouseTable runs the runner in query mode (CLAVESA_QUERY=1)
// against a local Hadoop-catalog warehouse and returns `sql`'s result rows
// as column-keyed maps. Preview uses it to sample a cross-pipeline
// (`external_inputs`) table — the table's data already exists on disk, so
// preview reads it rather than re-executing the producing pipeline.
//
// Dispatches through a package var so tests can stub out Docker.
//
// `localTag` is the full `<repo>:<tag>` reference for the workspace's
// local runner image — see RunPreview for the same convention.
func QueryWarehouseTable(ctx context.Context, localTag, image, warehouse, sql string) ([]map[string]interface{}, error) {
	return warehouseQueryRunner(ctx, localTag, image, warehouse, sql)
}

func queryWarehouseDocker(ctx context.Context, localTag, image, warehouse, sql string) ([]map[string]interface{}, error) {
	if image == "" || strings.Contains(image, "${") || strings.HasPrefix(image, "data.") || strings.HasPrefix(image, "var.") {
		image = localTag
	}
	args := []string{
		"run", "--rm", "-i",
		"-e", "CLAVESA_QUERY=1",
		"-e", "CLAVESA_WAREHOUSE=" + warehouse,
		// Override the baked-in version — see runPreviewDocker for rationale.
		"-e", "CLAVESA_MODULE_VERSION=" + version.Module,
		"-v", warehouse + ":" + warehouse,
		image,
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = strings.NewReader(sql)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("warehouse query: %w\n%s", err, msg)
		}
		return nil, fmt.Errorf("warehouse query: %w", err)
	}
	var res struct {
		Columns []string        `json:"columns"`
		Rows    [][]interface{} `json:"rows"`
		Error   string          `json:"error"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, fmt.Errorf("parse warehouse query output: %w", err)
	}
	if res.Error != "" {
		return nil, fmt.Errorf("warehouse query: %s", res.Error)
	}
	rows := make([]map[string]interface{}, 0, len(res.Rows))
	for _, r := range res.Rows {
		m := make(map[string]interface{}, len(res.Columns))
		for i, col := range res.Columns {
			if i < len(r) {
				m[col] = r[i]
			}
		}
		rows = append(rows, m)
	}
	return rows, nil
}

// warehouseQueryRunner is the active warehouse-query runner; tests swap it.
var warehouseQueryRunner = queryWarehouseDocker

// SetWarehouseQueryRunnerForTest swaps the warehouse-query runner and returns
// a restore function. Use only from tests.
func SetWarehouseQueryRunnerForTest(fn func(ctx context.Context, localTag, image, warehouse, sql string) ([]map[string]interface{}, error)) func() {
	prev := warehouseQueryRunner
	warehouseQueryRunner = fn
	return func() { warehouseQueryRunner = prev }
}

// previewRunner is the active runner. Tests swap this for an in-process stub.
var previewRunner = runPreviewDocker

// SetRunnerForTest swaps the active runner and returns a function that
// restores the previous value. Use only from tests.
func SetRunnerForTest(fn func(ctx context.Context, localTag, image string, inputs map[string][]map[string]interface{}, sql, python string) (map[string][]map[string]interface{}, error)) func() {
	prev := previewRunner
	previewRunner = fn
	return func() { previewRunner = prev }
}
