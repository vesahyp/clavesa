package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/workspace"
)

func newPipelineCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Manage pipelines",
		Long: `Manage pipelines (list, show, create, delete, upgrade, plan, deploy, destroy).

A pipeline is a subdirectory containing .tf files that define sources,
transforms, and destinations.

Most pipeline commands take the pipeline directory as the first argument.
Omit it to use the current directory once you have cd'd into the pipeline.

Examples:
  clavesa pipeline list
  clavesa pipeline create my-pipeline
  clavesa pipeline show my-pipeline
  clavesa pipeline show                  # from inside the pipeline dir
  clavesa pipeline upgrade my-pipeline
  clavesa pipeline delete my-pipeline --force`,
		RunE: requireSubcommand(),
	}

	cmd.AddCommand(
		newPipelineListCmd(),
		newPipelineShowCmd(),
		newPipelineLineageCmd(),
		newPipelineCreateCmd(),
		newPipelineDeleteCmd(),
		newPipelineUpgradeCmd(),
		newPipelinePlanCmd(),
		newPipelineDeployCmd(),
		newPipelineDestroyCmd(),
		newPipelineResetCmd(),
		newPipelineRunCmd(),
		newPipelineStatusCmd(),
		newPipelineRightsizeCmd(),
		newPipelineCostCmd(),
		newPipelineOptimizeCmd(),
		newOrchestrationCmd(),
		newPipelineBackfillCmd(),
	)

	return cmd
}

func newPipelineListCmd() *cobra.Command {
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List pipelines in the workspace",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			pipelines, err := svc.ListPipelines()
			if err != nil {
				return fmt.Errorf("list pipelines: %w", err)
			}
			if pipelines == nil {
				pipelines = []service.PipelineInfo{}
			}
			if jsonOut {
				return printJSON(os.Stdout, pipelines)
			}
			rows := make([][]string, len(pipelines))
			for i, p := range pipelines {
				rows[i] = []string{p.Name, strconv.Itoa(p.NodeCount), p.Dir}
			}
			printTable(os.Stdout, []string{"NAME", "NODES", "DIR"}, rows)
			return nil
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")

	return cmd
}

func newPipelineShowCmd() *cobra.Command {
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "show [pipeline-dir]",
		Short: "Show pipeline details",
		Long:  "Show pipeline details.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			g, err := svc.GetPipeline(dir)
			if err != nil {
				return fmt.Errorf("get pipeline: %w", err)
			}
			if jsonOut {
				return printJSON(os.Stdout, g)
			}
			fmt.Printf("Directory:  %s\n", g.Pipeline.Directory)
			fmt.Printf("Nodes:      %d\n", len(g.Nodes))
			fmt.Printf("Edges:      %d\n", len(g.Edges))
			if len(g.Nodes) > 0 {
				fmt.Println()
				rows := make([][]string, len(g.Nodes))
				for i, n := range g.Nodes {
					rows[i] = []string{n.ID, n.Type, n.ModuleSource}
				}
				printTable(os.Stdout, []string{"ID", "TYPE", "MODULE"}, rows)
			}
			if len(g.Edges) > 0 {
				fmt.Println()
				fmt.Println("Edges:")
				for _, e := range g.Edges {
					fmt.Printf("  %s->%s\n", e.FromNode, e.ToNode)
				}
			}
			if len(g.Validation.Errors) > 0 {
				fmt.Println()
				fmt.Println("Validation errors:")
				for _, ve := range g.Validation.Errors {
					fmt.Printf("  [%s] %s\n", ve.Code, ve.Message)
				}
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")

	return cmd
}

// newPipelineLineageCmd surfaces the data-lineage graph the UI's
// TableDetail panel renders — source/transform/destination edges plus
// the catalog table each edge flows through, including cross-pipeline
// reads. CLI/UI parity (ADR-015): the UI had this since the catalog
// rebuild; the CLI did not.
func newPipelineLineageCmd() *cobra.Command {
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "lineage [pipeline-dir]",
		Short: "Show the data-lineage graph for a pipeline",
		Long:  "Show the data-lineage graph for a pipeline.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			res, err := svc.Lineage(dir)
			if err != nil {
				return fmt.Errorf("lineage: %w", err)
			}
			edges := []service.LineageEdge{}
			if res != nil {
				edges = res.Edges
			}
			if jsonOut {
				return printJSON(os.Stdout, edges)
			}
			if len(edges) == 0 {
				fmt.Println("No lineage edges — the pipeline has no wired nodes yet.")
				return nil
			}
			rows := make([][]string, len(edges))
			for i, e := range edges {
				from := e.FromNode
				if e.FromPipeline != "" {
					from = e.FromPipeline + "/" + from
				}
				to := e.ToNode
				if e.ToPipeline != "" {
					to = e.ToPipeline + "/" + to
				}
				rows[i] = []string{from, e.FromType, to, e.ToType, e.ViaTable}
			}
			printTable(os.Stdout, []string{"FROM", "FROM_TYPE", "TO", "TO_TYPE", "VIA_TABLE"}, rows)
			return nil
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")

	return cmd
}

func newPipelineCreateCmd() *cobra.Command {
	var schema string
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new pipeline",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			rel, err := svc.CreatePipeline(name, schema)
			if err != nil {
				return fmt.Errorf("create pipeline: %w", err)
			}
			fmt.Printf("Created pipeline: %s\n", rel)
			fmt.Println()
			fmt.Println("Next steps:")
			fmt.Println("  clavesa source register <name> --from <url>            # workspace-level source")
			fmt.Printf("  clavesa node add %s --type transform --name <name>\n", rel)
			fmt.Printf("  clavesa source attach %s <source> --to <transform>     # wire it up\n", rel)
			fmt.Printf("  clavesa pipeline run %s\n", rel)
			fmt.Println("  clavesa ui                                             # or author in the editor")
			return nil
		},
	}
	cmd.Flags().StringVar(&schema, "schema", "", "ADR-016 schema identifier (default: sanitized pipeline name)")
	return cmd
}

func newPipelineDeleteCmd() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "delete [pipeline-dir]",
		Short: "Delete a pipeline",
		Long:  "Delete a pipeline.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !force {
				return fmt.Errorf("pipeline delete requires --force (permanently removes the directory)")
			}
			dir, _, ws, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.DeletePipeline(dir); err != nil {
				return fmt.Errorf("delete pipeline: %w", err)
			}
			fmt.Printf("Deleted pipeline: %s\n", displayDir(ws, dir))
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "required: permanently delete the pipeline directory")

	return cmd
}

func newPipelineUpgradeCmd() *cobra.Command {
	var version string

	cmd := &cobra.Command{
		Use:   "upgrade [pipeline-dir]",
		Short: "Upgrade module versions in a pipeline",
		Long:  "Upgrade module versions in a pipeline.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			_, orchStatErr := os.Stat(filepath.Join(dir, "orchestration.tf"))
			orchExistedBefore := orchStatErr == nil
			currentRef, targetRef, updated, migrated, err := svc.UpgradePipeline(dir, version)
			if err != nil {
				return err
			}
			if updated == 0 && migrated == 0 && currentRef == targetRef {
				fmt.Printf("Already at %s\n", targetRef)
				return nil
			}
			if updated > 0 {
				fmt.Printf("Upgraded %d module source(s): %s → %s\n", updated, currentRef, targetRef)
				if orchExistedBefore {
					fmt.Println("Re-synced orchestration.tf to match the new emitter shape.")
				} else {
					fmt.Println("Generated missing orchestration.tf.")
				}
			}
			if migrated > 0 {
				fmt.Printf("Migrated %d transform(s): removed the legacy compute = \"local\" attribute.\n", migrated)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "target version tag (default: this CLI's module version)")

	return cmd
}

func newPipelinePlanCmd() *cobra.Command {
	return newPipelineTerraformCmd("plan", "Run terraform plan on a pipeline")
}

func newPipelineDeployCmd() *cobra.Command {
	var autoApprove bool
	var planOnly bool
	cmd := &cobra.Command{
		Use:   "deploy <pipeline-dir>",
		Short: "terraform init -upgrade → plan -out=tfplan → apply tfplan for a pipeline",
		Long: `Run the substantive deploy lifecycle against the pipeline's terraform.

Preflight checks clavesa.json at the workspace root (the pipeline's
parent). Saves the plan to <pipeline>/tfplan and pauses for a 'yes'
confirmation before applying; the plan is cleaned up on success.

The runner image isn't re-checked here — pipeline deploys pin the Lambda
to an ECR digest and don't push, so a stale local image isn't a risk.
That preflight runs as part of` + " `workspace deploy`" + `.

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, ws, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			printTargetContext("deploy "+filepath.Base(dir), ws, "")
			// ADR-016 §5 preflight: refuse to deploy a pipeline whose
			// schema is owned by another pipeline. Deploy doesn't re-emit
			// orchestration.tf, so this is the last gate before terraform.
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.ValidateSchemaOwnership(dir); err != nil {
				return err
			}
			if _, statErr := os.Stat(filepath.Join(dir, "orchestration.tf")); os.IsNotExist(statErr) {
				return fmt.Errorf("orchestration.tf missing in %s — run `clavesa pipeline upgrade %s` (regenerates it) or `clavesa pipeline orchestration sync %s`",
					displayDir(ws, dir), filepath.Base(dir), filepath.Base(dir))
			}
			return deployFlow{
				WorkspaceRoot:    ws,
				TfDir:            dir,
				BuildRunnerImage: false,
				AutoApprove:      autoApprove,
				PlanOnly:         planOnly,
			}.Run()
		},
	}
	cmd.Flags().BoolVarP(&autoApprove, "yes", "y", false, "skip the interactive 'Apply this plan?' confirmation")
	cmd.Flags().BoolVar(&planOnly, "plan-only", false, "stop after terraform plan (don't apply)")
	return cmd
}

func newPipelineDestroyCmd() *cobra.Command {
	var skipSweep bool
	var glueDB string
	var autoApprove bool
	cmd := &cobra.Command{
		Use:   "destroy <pipeline-dir>",
		Short: "terraform destroy on a pipeline (sweeping runtime-created Glue tables first)",
		Long: `Run terraform destroy after deleting Glue tables that the runner created
at execution time. Without the sweep, terraform destroy refuses on
` + "`aws_glue_catalog_database.pipeline`" + ` with "database is not empty"
because runner-created Iceberg tables aren't in terraform state.

The sweep targets the pipeline's own Glue DB (default:
<workspace_catalog>__sanitize(<pipeline_name>)). Pass --glue-db <name>
if the pipeline's var.schema was overridden from its default.

Workspace system-DB row cleanup (runs / node_runs / tables rows where
pipeline = <this pipeline>) is not done here — those rows live inside
shared Iceberg tables and need an Athena DELETE through the workspace
workgroup. They stay around after destroy as historical context.

--skip-sweep skips the sweep step (faster when you know the DB is already
empty); the sweep itself asks for explicit 'yes' confirmation before
deleting anything.
Use --yes to skip both the sweep confirmation and the terraform destroy
prompt (for CI / scripted use). The pipeline name + workspace path are
still echoed to stderr before any AWS calls.

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, ws, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			pipelineName := filepath.Base(dir)
			printTargetContext("destroy "+pipelineName, ws, "")

			if !skipSweep {
				var sweepIn io.Reader = os.Stdin
				if autoApprove {
					sweepIn = autoYesReader()
				}
				if err := sweepPipelineGlueTables(cmd.Context(), ws, pipelineName, glueDB, os.Stdout, sweepIn); err != nil {
					return err
				}
			}

			if err := tfInit(dir, os.Stdout, os.Stderr); err != nil {
				return fmt.Errorf("terraform init: %w", err)
			}
			args2 := []string{"destroy"}
			if autoApprove {
				args2 = append(args2, "-auto-approve")
			}
			c := exec.Command("terraform", args2...)
			c.Dir = dir
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			c.Stdin = os.Stdin
			return c.Run()
		},
	}
	cmd.Flags().BoolVar(&skipSweep, "skip-sweep", false, "skip the Glue-table sweep preflight")
	cmd.Flags().StringVar(&glueDB, "glue-db", "", "explicit Glue DB to sweep (default: <catalog>__sanitize(<pipeline>))")
	cmd.Flags().BoolVarP(&autoApprove, "yes", "y", false, "skip the interactive confirmation prompt")
	return cmd
}

func newPipelineResetCmd() *cobra.Command {
	var node string
	var includeWatermarks bool
	var autoApprove bool
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "reset [pipeline-dir]",
		Short: "Drop a pipeline's output tables and watermarks so the next run rebuilds from scratch",
		Long: `Drop the canonical output tables of a pipeline's transforms — and by
default the CDF watermarks feeding them — so the next run rebuilds
everything from source. Reset is a data operation: it never touches the
deployed Lambda / Step Functions / IAM stack — tearing that down is
` + "`pipeline destroy`" + `.

--include-watermarks defaults to true on purpose: for a CDF consumer,
dropping the table but keeping the watermark leaves the table empty on
the next run, because CDF reads only the not-yet-consumed range and
that range is empty after a drop. Clearing the watermark replays
upstream history from version 0, which is exactly the rebuild a reset
is for. Pass --include-watermarks=false to keep the cursors and drop
data only (rare).

In local mode this deletes warehouse table directories and watermark
files; in cloud mode it deletes the S3 warehouse prefixes, the Glue
catalog entries, and the S3 watermark objects.

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if jsonOut && !autoApprove {
				return fmt.Errorf("--json implies non-interactive use; pass --yes to confirm")
			}
			dir, _, ws, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			req := service.PipelineResetRequest{
				Dir:               dir,
				Node:              node,
				IncludeWatermarks: includeWatermarks,
			}
			plan, err := svc.PipelineResetPlan(cmd.Context(), req)
			if err != nil {
				return err
			}
			if len(plan.TablesDropped) == 0 && len(plan.WatermarksCleared) == 0 {
				if jsonOut {
					return printJSON(os.Stdout, plan)
				}
				fmt.Println("(nothing to reset)")
				return nil
			}
			if !jsonOut {
				printTargetContext("reset "+filepath.Base(dir), ws, workspace.Mode(plan.Mode))
				fmt.Printf("\nWill drop %d output table(s):\n", len(plan.TablesDropped))
				for _, t := range plan.TablesDropped {
					fmt.Printf("  - %s  (%s)\n", t.Table, t.Location)
				}
				if len(plan.WatermarksCleared) > 0 {
					fmt.Printf("\nWill clear %d watermark(s):\n", len(plan.WatermarksCleared))
					for _, w := range plan.WatermarksCleared {
						fmt.Printf("  - %s ← %s  (%s)\n", w.Consumer, w.Alias, w.Path)
					}
				}
				if !autoApprove {
					fmt.Printf("\nType 'yes' to drop these %d table(s) and %d watermark(s) (infra stays): ",
						len(plan.TablesDropped), len(plan.WatermarksCleared))
					r := bufio.NewReader(os.Stdin)
					line, _ := r.ReadString('\n')
					if strings.TrimSpace(line) != "yes" {
						return fmt.Errorf("pipeline reset cancelled")
					}
				}
			}
			res, err := svc.PipelineReset(cmd.Context(), req)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, res)
			}
			// The receipt lists only what actually existed and was deleted;
			// echo planned-but-absent entries too so the gap between the
			// confirmation list and the receipt is never a mystery.
			droppedTables := map[string]bool{}
			for _, t := range res.TablesDropped {
				droppedTables[t.Table] = true
			}
			for _, t := range plan.TablesDropped {
				if droppedTables[t.Table] {
					fmt.Printf("  ✓ dropped %s\n", t.Table)
				} else {
					fmt.Printf("  - %s (did not exist; skipped)\n", t.Table)
				}
			}
			clearedWMs := map[string]bool{}
			for _, w := range res.WatermarksCleared {
				clearedWMs[w.Consumer+"__"+w.Alias] = true
			}
			for _, w := range plan.WatermarksCleared {
				name := w.Consumer + "__" + w.Alias
				if clearedWMs[name] {
					fmt.Printf("  ✓ cleared watermark %s\n", name)
				} else {
					fmt.Printf("  - %s (did not exist; skipped)\n", name)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&node, "node", "", "reset only this node's output (default: all transform nodes)")
	cmd.Flags().BoolVar(&includeWatermarks, "include-watermarks", true, "clear CDF watermark state so the next run replays upstream history from version 0")
	cmd.Flags().BoolVarP(&autoApprove, "yes", "y", false, "skip the interactive confirmation prompt")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit machine-readable output (requires --yes)")
	return cmd
}

// newPipelineTerraformCmd builds a `pipeline <subcommand>` that resolves the
// pipeline arg against the active workspace (matching `pipeline run` /
// `pipeline upgrade` / orchestration sync) and shells out to terraform there.
func newPipelineTerraformCmd(tfSubcmd, short string) *cobra.Command {
	return &cobra.Command{
		Use:   tfSubcmd + " [pipeline-dir]",
		Short: short,
		Long:  short + ".\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			if err := tfInit(dir, os.Stdout, os.Stderr); err != nil {
				return fmt.Errorf("terraform init: %w", err)
			}
			c := exec.Command("terraform", tfSubcmd)
			c.Dir = dir
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			return c.Run()
		},
	}
}

func newPipelineRunCmd() *cobra.Command {
	var jsonOut bool
	var wait bool
	var envOverride string
	var force bool
	var forceNodes []string
	cmd := &cobra.Command{
		Use:   "run <pipeline-dir>",
		Short: "Execute the pipeline (local: runner container; cloud: SFN StartExecution)",
		Long: `Dispatches by the workspace environment mode:

  - mode = local  →  walks the DAG and invokes the runner container for
                     each transform; outputs land in a fresh temp
                     workdir.
  - mode = cloud  →  finds the deployed Step Functions state machine
                     (clavesa-<pipeline_name>) and calls
                     StartExecution. Pass --wait to block until the
                     execution terminates.

The mode defaults to "local" and is set with ` + "`clavesa workspace use --env`" + `.
Pass --env local|cloud to override it for this run only.

Local filesystem sources only on the local path — S3 sources need cloud
dispatch.

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, ws, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			g, err := hclparser.Parse(dir)
			if err != nil {
				return fmt.Errorf("parse pipeline at %s: %w", displayDir(ws, dir), err)
			}

			mode := workspace.LoadEnvironmentMode(ws)
			if envOverride != "" {
				m, ok := workspace.ParseMode(envOverride)
				if !ok {
					return fmt.Errorf(`--env must be "local" or "cloud", got %q`, envOverride)
				}
				mode = m
			}

			// --force-node implies a scoped --force; defend in depth.
			effectiveForce := force || len(forceNodes) > 0
			if effectiveForce {
				warnForceAppendWithoutMergeKeys(&g, forceNodes)
			}

			if !jsonOut {
				printTargetContext("run "+filepath.Base(dir), ws, mode)
			}
			if mode == workspace.ModeLocal {
				return runLocalPipeline(cmd, dir, jsonOut, effectiveForce, forceNodes)
			}
			return runCloudPipeline(cmd, dir, jsonOut, wait, effectiveForce, forceNodes)
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit machine-readable output")
	cmd.Flags().StringVar(&envOverride, "env", "", "override the workspace environment mode for this run: local | cloud")
	cmd.Flags().BoolVar(&wait, "wait", false, "(cloud only) block until the SFN execution terminates")
	cmd.Flags().BoolVar(&force, "force", false, "Bypass incremental-skip checks for this run; the runner reads the full source range. Watermarks still advance on success.")
	cmd.Flags().StringSliceVar(&forceNodes, "force-node", nil, "Bypass incremental-skip for the named node only. Repeatable. Implies --force scoped to that node.")
	return cmd
}

// warnForceAppendWithoutMergeKeys prints one stderr line per node in the
// force set that has an append-mode output without merge_keys. A forced
// run re-reads the full source range; an append+no-keys output writes
// duplicates. Don't refuse — the operator may want exactly this; just
// surface the risk so it can't surprise them.
func warnForceAppendWithoutMergeKeys(g *graph.PipelineGraph, forceNodes []string) {
	scope := map[string]bool{}
	for _, n := range forceNodes {
		scope[n] = true
	}
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if n.Type != "transform" {
			continue
		}
		if len(scope) > 0 && !scope[n.ID] {
			continue
		}
		defs, _ := n.Config["output_definitions"].(map[string]interface{})
		var risky []string
		for key, raw := range defs {
			def, _ := raw.(map[string]interface{})
			if def == nil {
				continue
			}
			mode, _ := def["mode"].(string)
			if mode != "append" {
				continue
			}
			hasKeys := false
			if mk, ok := def["merge_keys"].([]interface{}); ok && len(mk) > 0 {
				hasKeys = true
			}
			if !hasKeys {
				risky = append(risky, key)
			}
		}
		if len(risky) > 0 {
			fmt.Fprintf(os.Stderr,
				"warning: --force on node %q has append outputs without merge_keys (%q); a re-read of the full source range may write duplicates. Consider mode=replace or mode=merge for keyed outputs.\n",
				n.ID, strings.Join(risky, ", "),
			)
		}
	}
}

// runLocalPipeline preserves the pre-dispatch behavior — DAG walk + runner
// container per transform — for compute=local pipelines. Returns the same
// shape (table rows + workdir) the command always has.
func runLocalPipeline(cmd *cobra.Command, dir string, jsonOut, force bool, forceNodes []string) error {
	svc, _, err := newService(cmd)
	if err != nil {
		return err
	}
	result, err := svc.RunPipelineWithOpts(cmd.Context(), dir, service.RunOpts{
		Force:      force,
		ForceNodes: forceNodes,
	})
	if err != nil {
		if result != nil && jsonOut {
			_ = printJSON(os.Stdout, result)
		}
		return err
	}
	if jsonOut {
		return printJSON(os.Stdout, result)
	}
	fmt.Printf("Workdir: %s\n", result.Workdir)
	rows := make([][]string, len(result.Nodes))
	for i, n := range result.Nodes {
		rows[i] = []string{n.NodeID, n.Type, n.Status, n.Output}
	}
	printTable(os.Stdout, []string{"NODE", "TYPE", "STATUS", "OUTPUT"}, rows)
	for _, n := range result.Nodes {
		if n.Note != "" {
			fmt.Printf("  %s: %s\n", n.NodeID, n.Note)
		}
	}
	return nil
}

// runCloudPipeline is the CLI cloud-run shell. The actual work — looking up
// the state machine by name, building the execution input payload, and
// calling StartExecution — lives in service.RunPipelineCloud so the HTTP
// handler at POST /api/pipeline/run can share the same code path
// (ADR-015). What stays here is CLI-only concerns: text/JSON output
// formatting and `--wait` polling of DescribeExecution until status
// leaves RUNNING.
//
// Polls once every 5s; cap is the cobra context (which honors Ctrl-C).
func runCloudPipeline(cmd *cobra.Command, abs string, jsonOut, wait, force bool, forceNodes []string) error {
	ctx := cmd.Context()
	svc, _, err := newService(cmd)
	if err != nil {
		return err
	}
	execARN, err := svc.RunPipelineCloud(ctx, abs, service.RunOpts{
		Force:      force,
		ForceNodes: forceNodes,
	})
	if err != nil {
		return err
	}
	pipelineName := filepath.Base(abs)
	stateMachineName := "clavesa-" + pipelineName

	if !wait {
		if jsonOut {
			return printJSON(os.Stdout, map[string]string{
				"execution_arn": execARN,
				"state_machine": stateMachineName,
			})
		}
		fmt.Printf("Started execution: %s\n", execARN)
		fmt.Printf("(use --wait to block until terminal, or check the dashboard)\n")
		return nil
	}

	// --wait: build a thin SFN client just for DescribeExecution polling.
	// Service is intentionally not the right home for the polling loop
	// (it's a CLI-shell concern; the UI uses the existing
	// /pipeline/execution/states channel instead).
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	client := sfn.NewFromConfig(cfg)
	if !jsonOut {
		fmt.Printf("Started execution: %s\n", execARN)
		fmt.Printf("Waiting for terminal status…\n")
	}
	final, err := waitForExecution(ctx, client, execARN)
	if err != nil {
		return err
	}
	if jsonOut {
		return printJSON(os.Stdout, map[string]string{
			"execution_arn": execARN,
			"status":        string(final.Status),
			"started_at":    aws.ToTime(final.StartDate).Format(time.RFC3339),
			"stopped_at":    aws.ToTime(final.StopDate).Format(time.RFC3339),
		})
	}
	fmt.Printf("Status: %s\n", final.Status)
	if final.Status != sfntypes.ExecutionStatusSucceeded {
		// Surface SFN's "cause" text if present — typically the failed
		// step's name + a short error.
		if final.Cause != nil && *final.Cause != "" {
			fmt.Printf("Cause: %s\n", *final.Cause)
		}
		return fmt.Errorf("execution did not succeed")
	}
	return nil
}

// waitForExecution polls every 5s until the execution leaves RUNNING.
// Honors ctx cancellation (Ctrl-C).
func waitForExecution(ctx context.Context, client *sfn.Client, execARN string) (*sfn.DescribeExecutionOutput, error) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		out, err := client.DescribeExecution(ctx, &sfn.DescribeExecutionInput{
			ExecutionArn: aws.String(execARN),
		})
		if err != nil {
			return nil, fmt.Errorf("DescribeExecution: %w", err)
		}
		if out.Status != sfntypes.ExecutionStatusRunning {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func newOrchestrationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "orchestration",
		Short: "Manage pipeline orchestration",
		RunE:  requireSubcommand(),
	}

	cmd.AddCommand(newOrchestrationSyncCmd())

	return cmd
}

func newOrchestrationSyncCmd() *cobra.Command {
	var schedule string

	cmd := &cobra.Command{
		Use:   "sync [pipeline-dir]",
		Short: "Generate orchestration.tf for a pipeline",
		Long:  "Generate orchestration.tf for a pipeline.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.SyncOrchestration(dir, schedule); err != nil {
				return fmt.Errorf("sync orchestration: %w", err)
			}
			if schedule != "" {
				fmt.Printf("Wrote orchestration.tf (schedule: %s)\n", schedule)
			} else {
				fmt.Println("Wrote orchestration.tf (manual trigger)")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&schedule, "schedule", "", `EventBridge schedule expression, e.g. "rate(1 hour)"`)

	return cmd
}
