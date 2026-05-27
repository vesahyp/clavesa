package service

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/pathutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// ListPipelines scans the workspace for pipeline directories.
func (s *Service) ListPipelines() ([]PipelineInfo, error) {
	return scanPipelines(s.workspace)
}

// CreatePipeline creates a new pipeline directory with boilerplate Terraform.
//
// schema is the ADR-016 pipeline schema identifier (middle level of
// <catalog>.<schema>.<table>). Empty falls back to sanitize(pipeline_name);
// pass an explicit value to share a schema across pipelines (e.g.,
// `marketing` for several marketing-domain pipelines) — but only one
// pipeline is allowed to write into any given schema, so a non-default
// value here implies coordinated naming across the workspace.
func (s *Service) CreatePipeline(name, schema string) (string, error) {
	rel := filepath.Clean(strings.TrimPrefix(name, "/"))
	abs := filepath.Join(s.workspace, rel)

	if !pathutil.IsWithin(s.workspace, abs) {
		return "", fmt.Errorf("invalid pipeline name: path escapes workspace")
	}

	pipelineName := filepath.Base(rel)
	if schema == "" {
		schema = identutil.Sanitize(pipelineName)
	}
	// ADR-016 §5: one pipeline per schema. Reject before MkdirAll so a
	// rejected create leaves no empty directory behind.
	if err := s.validateSchemaOwnership(pipelineName, schema); err != nil {
		return "", err
	}

	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", fmt.Errorf("create directory: %w", err)
	}

	ws, _ := workspace.Load(s.workspace) // nil on legacy workspaces

	var mainTF string
	if ws != nil {
		mainTF = `# clavesa pipeline
terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

data "terraform_remote_state" "workspace" {
  backend = "local"
  config  = { path = "${path.module}/../terraform.tfstate" }
}
`
	} else {
		mainTF = fmt.Sprintf(`# clavesa pipeline
terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

resource "aws_s3_bucket" "pipeline_bucket" {
  bucket        = "clavesa-%s"
  force_destroy = true
}
`, pipelineName)
	}

	variablesTF := fmt.Sprintf(`variable "pipeline_name" {
  description = "Human-readable name for this pipeline"
  default     = %q
}

variable "schema" {
  description = "Pipeline schema identifier (ADR-016 middle level). Default is the sanitized pipeline name; override to share a schema across pipelines (only one pipeline may write into any given schema)."
  type        = string
  default     = %q
}

variable "trigger_schedule" {
  description = "Optional EventBridge schedule expression to run the pipeline on a fixed interval, e.g. \"rate(1 day)\" or \"cron(0 2 * * ? *)\". Null to disable."
  type        = string
  default     = null
}

variable "trigger_batch_window" {
  description = "How often the poller checks source queues for new data, e.g. \"rate(1 minute)\" or \"rate(15 minutes)\". Only has effect when the pipeline has S3-event-driven sources (trigger_queue_arns non-empty in orchestration). Set to null to disable the poller; the source queues will then accumulate messages with nothing to drain them."
  type        = string
  default     = "rate(1 minute)"
}
`, pipelineName, schema)

	mainPath := filepath.Join(abs, "main.tf")
	if _, err := os.Stat(mainPath); os.IsNotExist(err) {
		if err := os.WriteFile(mainPath, []byte(mainTF), 0o644); err != nil {
			return "", fmt.Errorf("write main.tf: %w", err)
		}
	}
	varsPath := filepath.Join(abs, "variables.tf")
	if _, err := os.Stat(varsPath); os.IsNotExist(err) {
		if err := os.WriteFile(varsPath, []byte(variablesTF), 0o644); err != nil {
			return "", fmt.Errorf("write variables.tf: %w", err)
		}
	}

	// .gitignore — keep local run artifacts (warehouse, run state, logs) and
	// terraform state out of version control. Pipelines are .tf source; the
	// rest is regenerated on demand.
	gitignorePath := filepath.Join(abs, ".gitignore")
	if _, err := os.Stat(gitignorePath); os.IsNotExist(err) {
		gitignore := `# Local run artifacts (Iceberg warehouse, run progress channel, captured logs)
.clavesa/

# Terraform state
.terraform/
.terraform.lock.hcl
terraform.tfstate
terraform.tfstate.backup
`
		_ = os.WriteFile(gitignorePath, []byte(gitignore), 0o644)
	}

	// Generate initial orchestration.tf (no nodes/edges yet, manual trigger only).
	_ = s.SyncOrchestration(rel, "")

	// First pipeline in this workspace? Seed a dashboard for it so /dashboards
	// renders working content out of the box. Best-effort — if the seed fails
	// the pipeline is still good. Subsequent pipelines don't get auto-seeded
	// (the user has dashboards now and may not want noise).
	systemCatalog := ""
	if ws != nil {
		systemCatalog = ws.SystemCatalogIdentifier()
	}
	_ = seedDashboardForPipeline(s.workspace, pipelineName, systemCatalog)

	return rel, nil
}

// pipelineRunsDashboardJSON is the per-pipeline seed dashboard, in the
// datasets shape: named, reusable SQL queries the widgets bind to.
// Placeholders:
//
//	%[1]s — original pipeline name (the dataset dir, and the pipeline=
//	        filter on every query — runs/node_runs are workspace-wide as
//	        of v0.20.0 so the dashboard must scope explicitly).
//	%[2]s — Glue-encoded namespace for the workspace system catalog
//	        (`<system_catalog>__pipelines`, ADR-016). Both runs and
//	        node_runs live here, distinguished by the pipeline column.
const pipelineRunsDashboardJSON = `{
  "title": "Pipeline runs — %[1]s",
  "datasets": [
    { "name": "failures_24h", "dir": "%[1]s", "sql": "SELECT COUNT(*) AS n FROM %[2]s.runs WHERE pipeline = '%[1]s' AND status = 'FAILED' AND started_at > current_timestamp - INTERVAL 1 DAY" },
    { "name": "runs_total", "dir": "%[1]s", "sql": "SELECT COUNT(*) AS n FROM %[2]s.runs WHERE pipeline = '%[1]s'" },
    { "name": "duration", "dir": "%[1]s", "sql": "SELECT x, y FROM (SELECT started_at AS x, duration_ms AS y FROM %[2]s.runs WHERE pipeline = '%[1]s' ORDER BY started_at DESC LIMIT 50) t ORDER BY x ASC" },
    { "name": "failures_by_node", "dir": "%[1]s", "sql": "SELECT node AS x, COUNT(*) AS y FROM %[2]s.node_runs WHERE pipeline = '%[1]s' AND status = 'failed' GROUP BY node ORDER BY y DESC" },
    { "name": "recent_runs", "dir": "%[1]s", "sql": "SELECT run_id, status, trigger, duration_ms FROM %[2]s.runs WHERE pipeline = '%[1]s' ORDER BY started_at DESC LIMIT 10" }
  ],
  "widgets": [
    { "id": "failures-24h", "type": "big_number", "title": "Failures (24h)", "dataset": "failures_24h", "value_field": "n", "layout": { "x": 0, "y": 0, "w": 3, "h": 2 } },
    { "id": "runs-total", "type": "big_number", "title": "Runs (total)", "dataset": "runs_total", "value_field": "n", "layout": { "x": 3, "y": 0, "w": 3, "h": 2 } },
    { "id": "duration-trend", "type": "line", "title": "Run duration", "dataset": "duration", "x_field": "x", "y_field": "y", "layout": { "x": 6, "y": 0, "w": 6, "h": 4 } },
    { "id": "failures-by-node", "type": "bar", "title": "Failures by node", "dataset": "failures_by_node", "x_field": "x", "y_field": "y", "layout": { "x": 0, "y": 2, "w": 6, "h": 4 } },
    { "id": "recent-runs", "type": "table", "title": "Recent runs", "dataset": "recent_runs", "layout": { "x": 6, "y": 4, "w": 6, "h": 4 } }
  ]
}
`

// seedDashboardForPipeline writes a "Pipeline runs — <name>" dashboard
// into the workspace's dashboards dir, but only when the dir is empty
// (no dashboards yet). The first-pipeline-create gets a working
// dashboard; subsequent creates leave the user's dashboard collection
// alone.
//
// Idempotent on the per-file level too — if a dashboard with the same
// slug already exists we don't overwrite it.
func seedDashboardForPipeline(workspaceRoot, pipelineName, systemCatalog string) error {
	dir := filepath.Join(workspaceRoot, ".clavesa", "dashboards")
	// Only seed when the workspace has no dashboards. Any existing file
	// (even an unrelated one the user wrote themselves) is a signal to
	// stay out of their way.
	if entries, err := os.ReadDir(dir); err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
				return nil
			}
		}
	}
	// A `.imported` sibling means this workspace has already migrated its
	// dashboards into the system table — it has dashboards, just not as
	// files. Don't re-seed.
	if _, err := os.Stat(dir + ".imported"); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create dashboards dir: %w", err)
	}
	slug := "pipeline-runs-" + sanitizeSlug(pipelineName)
	path := filepath.Join(dir, slug+".json")
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	// Workspace system observability DB (ADR-016 v0.20.0): runs and
	// node_runs both live in `<system_catalog>__pipelines`, multi-writer.
	// Empty systemCatalog (no manifest) falls back to today's per-pipeline
	// shape only as a defensive guard — fresh `pipeline create` always
	// runs inside a workspace.
	namespace := identutil.EncodeGlueDatabase(systemCatalog, "pipelines")
	if systemCatalog == "" {
		namespace = "clavesa_" + identutil.Sanitize(pipelineName)
	}
	body := fmt.Sprintf(pipelineRunsDashboardJSON, pipelineName, namespace)
	return os.WriteFile(path, []byte(body), 0o644)
}

// sanitizeSlug produces a filesystem-safe dashboard slug from a pipeline
// name. Same character class the dashboards handler validates against
// (lowercase, digits, dash, underscore). Pipeline names already match
// this set in practice, but be defensive.
func sanitizeSlug(name string) string {
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c+('a'-'A'))
		case c >= '0' && c <= '9', c == '-', c == '_':
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return "pipeline"
	}
	return string(out)
}

// DeletePipeline permanently removes the pipeline directory.
func (s *Service) DeletePipeline(dir string) error {
	rel := filepath.Clean(strings.TrimPrefix(dir, "/"))
	abs := filepath.Join(s.workspace, rel)
	if filepath.IsAbs(dir) {
		abs = filepath.Clean(dir)
	}
	if !pathutil.IsWithin(s.workspace, abs) {
		return fmt.Errorf("invalid pipeline dir: path escapes workspace")
	}
	return os.RemoveAll(abs)
}

// GetPipeline parses and returns the pipeline graph for dir.
func (s *Service) GetPipeline(dir string) (PipelineGraph, error) {
	return hclparser.Parse(s.resolveDir(dir))
}

// ---------------------------------------------------------------------------
// Workspace scan helpers
// ---------------------------------------------------------------------------

func scanPipelines(root string) ([]PipelineInfo, error) {
	var results []PipelineInfo
	// Don't list the workspace root itself as a pipeline — it has .tf files
	// (main.tf, outputs.tf, etc.) but is workspace infrastructure, not a pipeline.
	isWorkspaceRoot := false
	if m, _ := workspace.Load(root); m != nil {
		isWorkspaceRoot = true
	}
	candidates := []string{}
	if !isWorkspaceRoot {
		candidates = append(candidates, root)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") || strings.HasPrefix(e.Name(), "_") {
			continue
		}
		sub := filepath.Join(root, e.Name())
		candidates = append(candidates, sub)
		subEntries, err := os.ReadDir(sub)
		if err != nil {
			continue
		}
		for _, se := range subEntries {
			if se.IsDir() && !strings.HasPrefix(se.Name(), ".") && !strings.HasPrefix(se.Name(), "_") {
				candidates = append(candidates, filepath.Join(sub, se.Name()))
			}
		}
	}
	for _, dir := range candidates {
		if !hasTFFiles(dir) {
			continue
		}
		g, err := hclparser.Parse(dir)
		if err != nil {
			// Surface parse errors on dirs that look like pipelines (embedded
			// clavesa module reference) — they're bugs, not "not a pipeline".
			// Pure non-pipeline dirs (no clavesa marker) get skipped silently.
			if hasClavesaModules(dir) {
				fmt.Fprintf(os.Stderr, "pipeline list: parse failed in %s: %v\n", dir, err)
			}
			continue
		}
		if len(g.Nodes) == 0 && !hasClavesaModules(dir) {
			continue
		}
		rel, relErr := filepath.Rel(root, dir)
		if relErr != nil {
			rel = dir
		}
		name := filepath.Base(dir)
		if rel == "." {
			name = filepath.Base(root)
		}
		nodeCount := 0
		cloud := ""
		compute := ""
		sourceSet := map[string]struct{}{}
		if err == nil {
			nodeCount = len(g.Nodes)
			for _, n := range g.Nodes {
				if strings.Contains(n.ModuleSource, "/aws/") {
					cloud = "aws"
				}
				if n.Type != "transform" {
					continue
				}
				// Registered-source references (ADR-017) live in
				// source_inputs — both the http "sources.<name>"
				// sentinel and the typed s3 {spec_name=...} form.
				if srcInputs, ok := n.Config["source_inputs"].(map[string]interface{}); ok {
					for _, raw := range srcInputs {
						if sn := sourceInputName(raw); sn != "" {
							sourceSet[sn] = struct{}{}
						}
					}
				}
				c, _ := n.Config["compute"].(string)
				if c == "local" {
					compute = "local"
				} else if compute == "" && c != "" {
					compute = c
				}
			}
			if compute == "" && nodeCount > 0 {
				compute = "lambda" // module default per modules/transform/aws/variables.tf
			}
		}
		sources := make([]string, 0, len(sourceSet))
		for sn := range sourceSet {
			sources = append(sources, sn)
		}
		sort.Strings(sources)
		results = append(results, PipelineInfo{
			Name: name, Dir: rel, NodeCount: nodeCount, Cloud: cloud, Compute: compute,
			Schema:  resolvePipelineSchema(dir, name),
			Sources: sources,
		})
	}
	return results, nil
}

func hasTFFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tf") {
			return true
		}
	}
	return false
}

// IsPipelineDir reports whether dir looks like a pipeline directory: it
// contains .tf files and is not a workspace root (no clavesa.json). This
// is the loose bar scanPipelines treats as a pipeline candidate — a malformed
// pipeline still passes here; the downstream hclparser.Parse / terraform call
// produces the precise error. Used by the CLI to infer the pipeline from the
// current directory when the <pipeline-dir> argument is omitted.
func IsPipelineDir(dir string) bool {
	if !hasTFFiles(dir) {
		return false
	}
	if _, err := os.Stat(filepath.Join(dir, "clavesa.json")); err == nil {
		return false // workspace root, not a pipeline
	}
	return true
}

// hasClavesaModules reports whether dir has any .tf file referencing a
// clavesa module source — either the embedded form (`.clavesa/modules/`,
// post-v0.30.0) or the legacy GitHub `?ref=` form (`clavesa//modules/`).
// Used only as a fallback when hclparser.Parse fails or returns zero
// nodes; the bare-substring match for "clavesa" was a false-positive
// magnet (any `bucket = "clavesa-..."` in a non-pipeline dir tripped it).
func hasClavesaModules(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".tf") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		s := string(data)
		if strings.Contains(s, ".clavesa/modules/") || strings.Contains(s, "clavesa//modules/") {
			return true
		}
	}
	return false
}
