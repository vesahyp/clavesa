package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/vesahyp/clavesa/internal/runner"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// QueryRunner shells out to the runner image to execute a single Spark SQL
// statement against a local Hadoop catalog warehouse. Default implementation
// uses `docker run`; tests inject a stub via WithQueryRunner so the suite
// stays Docker-free.
type QueryRunner interface {
	Run(ctx context.Context, warehouse, sql string) (*QueryRunnerResult, error)
}

// QueryRunnerResult mirrors the JSON shape CLAVESA_QUERY=1 emits.
// ColumnTypes is the Spark DataType.simpleString() per column (e.g.
// "string", "bigint", "timestamp"); empty when the runner is older than
// the column-types-in-query-mode change (the slice degrades to empty
// strings in SampleTableColumn.Type, which matches the prior behavior).
type QueryRunnerResult struct {
	Columns     []string        `json:"columns"`
	ColumnTypes []string        `json:"column_types"`
	Rows        [][]interface{} `json:"rows"`
}

// WithQueryRunner overrides the docker-shell-out implementation. Used in
// tests; production wiring uses NewLocalProvider's default runner.
func (p *LocalProvider) WithQueryRunner(qr QueryRunner) *LocalProvider {
	p.query = qr
	return p
}

// runQueryFor runs sql against the workspace-shared local warehouse. The
// warehouse holds every local pipeline's tables under separate
// `<catalog>__<schema>` namespaces (ADR-016), so the query resolves
// cross-pipeline references regardless of which pipeline `pipelineRef`
// names — the argument is kept for the empty-ref guard and caller symmetry.
func (p *LocalProvider) runQueryFor(ctx context.Context, pipelineRef, sql string) (*QueryRunnerResult, error) {
	if pipelineRef == "" {
		return nil, fmt.Errorf("local provider: pipeline reference required")
	}
	warehouse := workspace.LocalWarehouseDir(p.workspaceRoot)

	qr := p.query
	if qr == nil {
		qr = newDockerQueryRunner(p.workspaceRoot)
	}
	return qr.Run(ctx, warehouse, sql)
}

// dockerQueryRunner is the production QueryRunner — invokes the workspace's
// runner image with CLAVESA_QUERY=1, reads JSON from stdout.
type dockerQueryRunner struct {
	image string
	// workspaceRoot is kept so Run can join the shared-metastore docker
	// network. A one-shot CLI query is a hive→Derby client too; pointing
	// it at the metastore server (when one is up) lets it run alongside a
	// concurrent `pipeline run` without colliding on the embedded lock.
	workspaceRoot string
	// workspaceName is the metastore image-resolution key for
	// EnsureMetastore. Empty falls back to the empty-name image, same as
	// the `image` field's fallback above.
	workspaceName string
}

func newDockerQueryRunner(workspaceRoot string) *dockerQueryRunner {
	image := runner.LocalImageName("") + ":latest"
	name := ""
	if m, _ := workspace.Load(workspaceRoot); m != nil {
		image = runner.LocalImageName(m.Name) + ":latest"
		name = m.Name
	}
	return &dockerQueryRunner{image: image, workspaceRoot: workspaceRoot, workspaceName: name}
}

func (d *dockerQueryRunner) Run(ctx context.Context, warehouse, sql string) (*QueryRunnerResult, error) {
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		return nil, fmt.Errorf("create warehouse dir: %w", err)
	}

	args := []string{"run", "--rm", "-i"}
	args = append(args, "-e", "CLAVESA_QUERY=1")
	args = append(args, "-e", "CLAVESA_WAREHOUSE="+warehouse)
	// Shared local Derby metastore — see local_query_warm.go for the full
	// rationale. Best-effort: on Ensure failure log and fall back to the
	// container's embedded Derby (safe, since no server is then serving).
	if d.workspaceRoot != "" {
		if addr, err := EnsureMetastore(ctx, d.workspaceRoot, d.workspaceName); err != nil {
			fmt.Fprintf(os.Stderr, "clavesa: query falling back to embedded metastore (shared metastore unavailable): %v\n", err)
		} else {
			args = append(args, "--network", metastoreNetworkName())
			args = append(args, "-e", "CLAVESA_METASTORE_ADDR="+addr)
		}
	}
	args = append(args, "-v", warehouse+":"+warehouse)
	args = append(args, d.image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	// Runner emits exactly one JSON document on stdout (success or
	// {"error":...}). We parse stdout regardless of exit status — the runner
	// exits 1 on query failure and emits the error envelope on stdout, so
	// dropping stdout when Run errors loses the most useful context.
	out := bytes.TrimSpace(stdout.Bytes())

	if len(out) > 0 {
		var errEnv struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(out, &errEnv); err == nil && errEnv.Error != "" {
			return nil, fmt.Errorf("query runner: %s", trimSparkStackTrace(errEnv.Error))
		}

		var res QueryRunnerResult
		if err := json.Unmarshal(out, &res); err == nil {
			return &res, nil
		}
		// stdout had something we couldn't parse — fall through to surface
		// it alongside the docker exit error below.
	}

	if runErr != nil {
		return nil, fmt.Errorf("docker run query: %w\nstdout: %s\nstderr: %s",
			runErr, stdout.String(), stderr.String())
	}
	return nil, fmt.Errorf("query runner returned no parseable output\nstdout: %s\nstderr: %s",
		stdout.String(), stderr.String())
}

// columnIndex returns a name → index map for fast row-to-struct projection.
func columnIndex(columns []string) map[string]int {
	out := make(map[string]int, len(columns))
	for i, c := range columns {
		out[c] = i
	}
	return out
}

// stringValue coerces an arbitrary JSON-decoded value into the string column
// type our row structs expect. nil → empty; everything else fmt.Sprint'd so
// timestamps / numerics serialize predictably.
func stringValue(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// int64Pointer attempts to read v as an int64. Returns nil for missing values
// so the row JSON omits the column ("memory_mb" etc. are nullable).
func int64Pointer(v interface{}) *int64 {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case float64:
		n := int64(t)
		return &n
	case int64:
		return &t
	case int:
		n := int64(t)
		return &n
	case string:
		var n int64
		if _, err := fmt.Sscanf(t, "%d", &n); err == nil {
			return &n
		}
	}
	return nil
}

// float64Pointer attempts to read v as a float64. Returns nil for missing
// values so the row JSON omits the column.
func float64Pointer(v interface{}) *float64 {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case float64:
		return &t
	case int64:
		f := float64(t)
		return &f
	case int:
		f := float64(t)
		return &f
	case string:
		var f float64
		if _, err := fmt.Sscanf(t, "%g", &f); err == nil {
			return &f
		}
	}
	return nil
}

// boolPointer attempts to read v as a bool, returning nil for missing values.
func boolPointer(v interface{}) *bool {
	if v == nil {
		return nil
	}
	if b, ok := v.(bool); ok {
		return &b
	}
	return nil
}

// rowAt returns the value at column name from row, or nil when the column
// is absent from the result set (defensive — Iceberg schema evolution could
// add or drop columns under us between runner and provider).
func rowAt(row []interface{}, idx map[string]int, col string) interface{} {
	i, ok := idx[col]
	if !ok || i >= len(row) {
		return nil
	}
	return row[i]
}
