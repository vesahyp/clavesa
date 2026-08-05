# CLI reference

Generated from `clavesa --help`. Do not edit by hand — run `make docs-cli` to regenerate (a Go test fails if this directory drifts from the CLI).

Visual ETL for Terraform pipelines

## Commands

| Command | Description |
| --- | --- |
| [`clavesa credential`](clavesa_credential.md) | Manage workspace credentials (registry) |
| &nbsp;&nbsp;[`clavesa credential delete`](clavesa_credential_delete.md) | Delete a registered credential |
| &nbsp;&nbsp;[`clavesa credential list`](clavesa_credential_list.md) | List registered credentials |
| &nbsp;&nbsp;[`clavesa credential register`](clavesa_credential_register.md) | Register a new credential |
| &nbsp;&nbsp;[`clavesa credential show`](clavesa_credential_show.md) | Show a credential's spec (never the secret material) |
| [`clavesa dashboards`](clavesa_dashboards.md) | List, inspect, render, and author workspace dashboards |
| &nbsp;&nbsp;[`clavesa dashboards apply`](clavesa_dashboards_apply.md) | Create or replace a dashboard from a JSON spec file |
| &nbsp;&nbsp;[`clavesa dashboards delete`](clavesa_dashboards_delete.md) | Delete a dashboard |
| &nbsp;&nbsp;[`clavesa dashboards list`](clavesa_dashboards_list.md) | List dashboards |
| &nbsp;&nbsp;[`clavesa dashboards render`](clavesa_dashboards_render.md) | Execute every widget's dataset and print the results |
| &nbsp;&nbsp;[`clavesa dashboards show`](clavesa_dashboards_show.md) | Show a dashboard's datasets and widgets |
| [`clavesa deploy`](clavesa_deploy.md) | Apply the workspace infra and every pipeline in one pass |
| [`clavesa node`](clavesa_node.md) | Manage pipeline nodes and edges |
| &nbsp;&nbsp;[`clavesa node add`](clavesa_node_add.md) | Add a node to a pipeline |
| &nbsp;&nbsp;[`clavesa node connect`](clavesa_node_connect.md) | Connect a node, source, or external table to a transform's input |
| &nbsp;&nbsp;[`clavesa node disable`](clavesa_node_disable.md) | Disable a node — skip it in runs without deleting it |
| &nbsp;&nbsp;[`clavesa node disconnect`](clavesa_node_disconnect.md) | Remove an edge between nodes |
| &nbsp;&nbsp;[`clavesa node edit`](clavesa_node_edit.md) | Edit node configuration |
| &nbsp;&nbsp;[`clavesa node enable`](clavesa_node_enable.md) | Re-enable a previously disabled node |
| &nbsp;&nbsp;[`clavesa node list`](clavesa_node_list.md) | List nodes in a pipeline |
| &nbsp;&nbsp;[`clavesa node preview`](clavesa_node_preview.md) | Preview data flowing through a node |
| &nbsp;&nbsp;[`clavesa node remove`](clavesa_node_remove.md) | Remove a node from a pipeline |
| &nbsp;&nbsp;[`clavesa node rename`](clavesa_node_rename.md) | Rename a node |
| &nbsp;&nbsp;[`clavesa node show`](clavesa_node_show.md) | Show node details |
| [`clavesa notebook`](clavesa_notebook.md) | Multi-cell SQL + PySpark notebooks (Jupyter .ipynb) |
| &nbsp;&nbsp;[`clavesa notebook clear-outputs`](clavesa_notebook_clear-outputs.md) | Clear all cell outputs (matches `jupyter nbconvert --clear-output`) |
| &nbsp;&nbsp;[`clavesa notebook create`](clavesa_notebook_create.md) | Create an empty notebook |
| &nbsp;&nbsp;[`clavesa notebook delete`](clavesa_notebook_delete.md) | Delete a notebook (also stops its REPL if running) |
| &nbsp;&nbsp;[`clavesa notebook graduate`](clavesa_notebook_graduate.md) | Promote a notebook cell into a transform node |
| &nbsp;&nbsp;[`clavesa notebook list`](clavesa_notebook_list.md) | List notebooks in the workspace |
| &nbsp;&nbsp;[`clavesa notebook run`](clavesa_notebook_run.md) | Run every cell (or one cell with --cell) and persist outputs |
| &nbsp;&nbsp;[`clavesa notebook session`](clavesa_notebook_session.md) | Manage notebook REPL sessions |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa notebook session stop`](clavesa_notebook_session_stop.md) | Stop the REPL subprocess for a notebook (loses Python globals) |
| &nbsp;&nbsp;[`clavesa notebook show`](clavesa_notebook_show.md) | Print the notebook (cell sources, no outputs) |
| [`clavesa pipeline`](clavesa_pipeline.md) | Manage pipelines |
| &nbsp;&nbsp;[`clavesa pipeline backfill`](clavesa_pipeline_backfill.md) | Replay a transform over a historical partition window |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa pipeline backfill diff`](clavesa_pipeline_backfill_diff.md) | Compare a staging table against its canonical target |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa pipeline backfill discard`](clavesa_pipeline_backfill_discard.md) | Drop a staging table without promoting |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa pipeline backfill list`](clavesa_pipeline_backfill_list.md) | List open (un-promoted/un-discarded) backfill staging tables |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa pipeline backfill promote`](clavesa_pipeline_backfill_promote.md) | Merge a staging table into its canonical target, then drop staging |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa pipeline backfill stage`](clavesa_pipeline_backfill_stage.md) | Stage a backfill into a parallel Delta table |
| &nbsp;&nbsp;[`clavesa pipeline cost`](clavesa_pipeline_cost.md) | Report cost per billion records — clavesa's north-star metric |
| &nbsp;&nbsp;[`clavesa pipeline create`](clavesa_pipeline_create.md) | Create a new pipeline |
| &nbsp;&nbsp;[`clavesa pipeline delete`](clavesa_pipeline_delete.md) | Delete a pipeline |
| &nbsp;&nbsp;[`clavesa pipeline deploy`](clavesa_pipeline_deploy.md) | terraform init -upgrade → plan -out=tfplan → apply tfplan for a pipeline |
| &nbsp;&nbsp;[`clavesa pipeline destroy`](clavesa_pipeline_destroy.md) | terraform destroy on a pipeline (sweeping runtime-created Glue tables first) |
| &nbsp;&nbsp;[`clavesa pipeline lineage`](clavesa_pipeline_lineage.md) | Show the data-lineage graph for a pipeline |
| &nbsp;&nbsp;[`clavesa pipeline list`](clavesa_pipeline_list.md) | List pipelines in the workspace |
| &nbsp;&nbsp;[`clavesa pipeline logs`](clavesa_pipeline_logs.md) | Print the captured log for a run |
| &nbsp;&nbsp;[`clavesa pipeline optimize`](clavesa_pipeline_optimize.md) | Compact, re-cluster, and vacuum a pipeline's Delta output tables |
| &nbsp;&nbsp;[`clavesa pipeline orchestration`](clavesa_pipeline_orchestration.md) | Manage pipeline orchestration |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa pipeline orchestration sync`](clavesa_pipeline_orchestration_sync.md) | Generate orchestration.tf for a pipeline |
| &nbsp;&nbsp;[`clavesa pipeline plan`](clavesa_pipeline_plan.md) | Run terraform plan on a pipeline |
| &nbsp;&nbsp;[`clavesa pipeline reset`](clavesa_pipeline_reset.md) | Drop a pipeline's output tables and watermarks so the next run rebuilds from scratch |
| &nbsp;&nbsp;[`clavesa pipeline rightsize`](clavesa_pipeline_rightsize.md) | Recommend per-node Lambda memory from recent run history |
| &nbsp;&nbsp;[`clavesa pipeline run`](clavesa_pipeline_run.md) | Execute the pipeline (local: runner container; cloud: SFN StartExecution) |
| &nbsp;&nbsp;[`clavesa pipeline runs`](clavesa_pipeline_runs.md) | List recent runs of a pipeline |
| &nbsp;&nbsp;[`clavesa pipeline show`](clavesa_pipeline_show.md) | Show pipeline details |
| &nbsp;&nbsp;[`clavesa pipeline status`](clavesa_pipeline_status.md) | Show per-node status for the latest (or a given) run |
| &nbsp;&nbsp;[`clavesa pipeline upgrade`](clavesa_pipeline_upgrade.md) | Upgrade module versions in a pipeline |
| [`clavesa plan`](clavesa_plan.md) | Plan the workspace infra and every pipeline (no apply) |
| [`clavesa query`](clavesa_query.md) | Run an ad-hoc SQL query against the workspace catalog |
| [`clavesa runner`](clavesa_runner.md) | Manage the PySpark runner image (Python deps, etc.) |
| &nbsp;&nbsp;[`clavesa runner requirements`](clavesa_runner_requirements.md) | Manage extra Python packages baked into the runner image |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa runner requirements add`](clavesa_runner_requirements_add.md) | Add a runner requirement (pip spec) |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa runner requirements import`](clavesa_runner_requirements_import.md) | Replace all runner requirements with the contents of a file |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa runner requirements list`](clavesa_runner_requirements_list.md) | List the extra runner requirements |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa runner requirements remove`](clavesa_runner_requirements_remove.md) | Remove a runner requirement |
| &nbsp;&nbsp;&nbsp;&nbsp;[`clavesa runner requirements show`](clavesa_runner_requirements_show.md) | Show the raw requirements file (exactly what gets installed) |
| [`clavesa source`](clavesa_source.md) | Manage workspace input sources (registry) |
| &nbsp;&nbsp;[`clavesa source attach`](clavesa_source_attach.md) | Attach a registered source to a transform input |
| &nbsp;&nbsp;[`clavesa source delete`](clavesa_source_delete.md) | Delete a registered source |
| &nbsp;&nbsp;[`clavesa source detach`](clavesa_source_detach.md) | Detach a named input from a transform |
| &nbsp;&nbsp;[`clavesa source edit`](clavesa_source_edit.md) | Edit a registered source |
| &nbsp;&nbsp;[`clavesa source list`](clavesa_source_list.md) | List registered sources |
| &nbsp;&nbsp;[`clavesa source preview`](clavesa_source_preview.md) | Preview a registered source's data |
| &nbsp;&nbsp;[`clavesa source register`](clavesa_source_register.md) | Register a new source in the workspace registry |
| &nbsp;&nbsp;[`clavesa source show`](clavesa_source_show.md) | Show a source's spec |
| [`clavesa sql`](clavesa_sql.md) | SparkSQL tooling (parse-check, lint) |
| &nbsp;&nbsp;[`clavesa sql lint`](clavesa_sql_lint.md) | Parse-check a SparkSQL file; exits non-zero on parse failure |
| [`clavesa ui`](clavesa_ui.md) | Start the visual editor in your browser |
| [`clavesa version`](clavesa_version.md) | Print the clavesa version |
| [`clavesa workspace`](clavesa_workspace.md) | Manage workspaces |
| &nbsp;&nbsp;[`clavesa workspace deploy`](clavesa_workspace_deploy.md) | terraform init -upgrade → plan -out=tfplan → apply tfplan, with preflight |
| &nbsp;&nbsp;[`clavesa workspace destroy`](clavesa_workspace_destroy.md) | terraform destroy on the workspace (sweeping system-catalog Glue tables first) |
| &nbsp;&nbsp;[`clavesa workspace init`](clavesa_workspace_init.md) | Initialize a new workspace |
| &nbsp;&nbsp;[`clavesa workspace plan`](clavesa_workspace_plan.md) | Run terraform plan on the workspace |
| &nbsp;&nbsp;[`clavesa workspace tables`](clavesa_workspace_tables.md) | List Delta tables in the workspace catalog |
| &nbsp;&nbsp;[`clavesa workspace upgrade`](clavesa_workspace_upgrade.md) | Upgrade the workspace shell and every pipeline to the binary's module version |
| &nbsp;&nbsp;[`clavesa workspace use`](clavesa_workspace_use.md) | Switch the current workspace, or set its warehouse / AWS profile |

See also [`clavesa`](clavesa.md) for the root command and global flags.
