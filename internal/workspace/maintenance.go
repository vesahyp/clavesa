package workspace

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vesahyp/clavesa/internal/modules"
)

// MaintenancePipelineDir is the workspace-relative directory of the opt-in
// system-table maintenance pipeline scaffolded by Init.
const MaintenancePipelineDir = "_maintenance"

// maintenanceTransformPy is the PySpark body of the maintenance pipeline's
// single transform. It OPTIMIZEs and VACUUMs the four workspace bookkeeping
// tables under the system catalog so their Delta `_delta_log` and small-file
// count stay bounded (GH #53), then returns no outputs. Only the
// workspace-owned system catalog is touched, so the pipeline's default runner
// IAM (which already has S3 + Glue write to `_system/pipelines`) is enough; no
// cross-pipeline access is needed.
const maintenanceTransformPy = `"""clavesa system-table maintenance (GH #53).

OPTIMIZE + VACUUM the four workspace bookkeeping tables under the system
catalog (node_runs, runs, tables, column_stats) so their Delta transaction
log and small-file count stay bounded. clavesa writes these tables once per
node per run, so without periodic compaction their _delta_log dirs grow to
thousands of commit files and dominate S3 LIST cost.

Runs as a normal scheduled transform (Lambda in the cloud, local docker
otherwise) and produces no output tables: it returns an empty dict. Only the
workspace-owned system catalog is touched, so the pipeline's default runner
IAM is sufficient and no other pipeline's data is read or rewritten.
"""

import os
import sys

_SYSTEM_TABLES = ["node_runs", "runs", "tables", "column_stats"]

_PROPS = {
    "delta.logRetentionDuration": "interval 24 hours",
    "delta.deletedFileRetentionDuration": "interval 24 hours",
    "delta.checkpointInterval": "10",
}

# VACUUM retention window. The system tables set a 24h
# deletedFileRetentionDuration, so 24h reclaims promptly while staying far
# above the longest concurrent transaction: several pipelines write these
# tables (multi-writer by design), so the window must exceed any in-flight
# append, and a seconds-long append clears 24h with enormous margin.
_VACUUM_RETAIN_HOURS = 24


def _system_db():
    sys_cat = os.environ.get("CLAVESA_SYSTEM_CATALOG") or ""
    if not sys_cat:
        cat = os.environ.get("CLAVESA_CATALOG", "")
        sys_cat = cat + "_system" if cat else ""
    return sys_cat.replace("-", "_") + "__pipelines"


def transform(spark, inputs):
    # Permit VACUUM with a sub-7-day retention, scoped to this session only —
    # the global runner config keeps the safety check on for every other run.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    db = _system_db()
    if not db or db.startswith("__"):
        print("[clavesa] _maintenance: no system catalog resolved; nothing to do", file=sys.stderr)
        return {}

    props = ", ".join("'%s' = '%s'" % (k, v) for k, v in _PROPS.items())
    for name in _SYSTEM_TABLES:
        table_id = "%s.%s" % (db, name)
        if not spark.catalog.tableExists(table_id):
            print("[clavesa] _maintenance: %s not created yet, skipping" % table_id, file=sys.stderr)
            continue
        try:
            # SET TBLPROPERTIES also covers system tables created before the
            # #53 retention props shipped (the runner sets them only on first
            # create); idempotent on already-configured tables.
            spark.sql("ALTER TABLE %s SET TBLPROPERTIES (%s)" % (table_id, props))
            spark.sql("OPTIMIZE %s" % table_id)
            spark.sql("VACUUM %s RETAIN %d HOURS" % (table_id, _VACUUM_RETAIN_HOURS))
            print("[clavesa] _maintenance: compacted %s" % table_id, file=sys.stderr)
        except Exception as exc:  # noqa: BLE001
            # Best-effort per table — one failure must not abort the rest.
            print("[clavesa] _maintenance: %s failed: %r" % (table_id, exc), file=sys.stderr)

    return {}
`

// scaffoldMaintenancePipeline writes the opt-in `_maintenance` pipeline: a
// single PySpark transform that OPTIMIZEs and VACUUMs the workspace system
// bookkeeping tables so their Delta `_delta_log` stays bounded (GH #53).
// Compaction is a scheduled, observable pipeline rather than hidden on the
// per-node write path. The pipeline is written to disk but never auto-deployed
// — the user opts in with `clavesa pipeline deploy _maintenance` (a daily
// schedule), or deletes the directory. Idempotent: each file is written only
// when absent, so re-running init preserves a user's edits.
func scaffoldMaintenancePipeline(root, catalog, systemCatalog, moduleVersion string) error {
	dir := filepath.Join(root, MaintenancePipelineDir)
	if err := os.MkdirAll(filepath.Join(dir, "transforms"), 0o755); err != nil {
		return fmt.Errorf("create _maintenance dir: %w", err)
	}

	moduleSrc, err := modules.RelativeSource(dir, root, moduleVersion, "transform/aws")
	if err != nil {
		return fmt.Errorf("resolve _maintenance module source: %w", err)
	}

	mainTF := fmt.Sprintf(`# clavesa maintenance pipeline (opt-in, GH #53).
#
# OPTIMIZEs and VACUUMs the workspace system bookkeeping tables under
# _system/pipelines (node_runs, runs, tables, column_stats) so their Delta
# transaction log and small-file count stay bounded. Compaction is a
# scheduled, observable pipeline instead of hidden on the per-node write path.
#
# This pipeline is scaffolded but NOT deployed. Enable it with
# 'clavesa pipeline deploy _maintenance' (it runs on the daily schedule
# below), or delete this directory to opt out. The runner already has S3 +
# Glue write to the workspace-owned system catalog, so no extra IAM is needed;
# it touches only the system tables, never another pipeline's outputs.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

data "terraform_remote_state" "workspace" {
  backend = "local"
  config  = { path = "${path.module}/../terraform.tfstate" }
}

module "compact" {
  source         = %q
  pipeline_name  = var.pipeline_name
  name           = "compact"
  bucket         = data.terraform_remote_state.workspace.outputs.pipeline_bucket
  catalog        = %q
  schema         = var.schema
  system_catalog = %q

  language           = "python"
  python             = file("transforms/compact.py")
  inputs             = {}
  output_definitions = {}
}
`, moduleSrc, catalog, systemCatalog)

	variablesTF := `variable "pipeline_name" {
  description = "Human-readable name for this pipeline."
  default     = "_maintenance"
}

variable "schema" {
  description = "Pipeline schema identifier (ADR-016). Unused for writes — the maintenance transform produces no user outputs — but required by the transform module."
  type        = string
  default     = "_maintenance"
}

variable "trigger_schedule" {
  description = "EventBridge schedule for the maintenance run. Daily by default; OPTIMIZE/VACUUM of the system tables is cheap. Null to disable the schedule."
  type        = string
  default     = "cron(0 3 * * ? *)"
}
`

	gitignore := `# Local run artifacts and terraform state
.clavesa/
.terraform/
.terraform.lock.hcl
terraform.tfstate
terraform.tfstate.backup
tfplan
`

	files := []struct {
		path, content string
	}{
		{filepath.Join(dir, "main.tf"), mainTF},
		{filepath.Join(dir, "variables.tf"), variablesTF},
		{filepath.Join(dir, ".gitignore"), gitignore},
		{filepath.Join(dir, "transforms", "compact.py"), maintenanceTransformPy},
	}
	for _, f := range files {
		if _, err := os.Stat(f.path); err == nil {
			continue // never clobber an existing file (preserves user edits)
		}
		if err := os.WriteFile(f.path, []byte(f.content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", filepath.Base(f.path), err)
		}
	}
	return nil
}
