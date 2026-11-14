package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/bundle"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/workspace"
)

func newBundleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bundle",
		Short: "Deploy Databricks bundle (DAB) jobs to clavesa (ADR-031)",
		Long: `Read a Databricks Asset Bundle (databricks.yml) and run its jobs on clavesa,
in your own AWS account, from the same bundle that deploys them to
Databricks. A clavesa target is an ordinary bundle target that sets the
bundle variable "clavesa" (type: complex); see docs/decisions/031-dab-bundles-on-clavesa.md.

Every command takes --bundle (default: the current folder) and -t/--target
(default: the bundle's default target).`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(newBundlePlanCmd(), newBundleRunCmd(), newBundleDeployCmd(), newBundleCompareCmd(), newBundleSwitchCmd(), newBundleListCmd(), newBundleDestroyCmd())
	return cmd
}

// bundleFlags adds the flags every bundle command shares.
func bundleFlags(cmd *cobra.Command, o *service.BundleOptions, vars *[]string) {
	cmd.Flags().StringVar(&o.Dir, "bundle", ".", "the bundle folder (the one with databricks.yml)")
	cmd.Flags().StringVarP(&o.Target, "target", "t", "", "the bundle target (default: the target with default: true)")
	cmd.Flags().StringArrayVar(vars, "var", nil, "set a bundle variable, name=value (repeatable); wins over BUNDLE_VAR_<name> and the target")
}

func parseBundleVars(vars []string) (map[string]string, error) {
	out := map[string]string{}
	for _, v := range vars {
		k, val, ok := strings.Cut(v, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("--var %q: use name=value", v)
		}
		out[k] = val
	}
	return out, nil
}

func newBundlePlanCmd() *cobra.Command {
	var o service.BundleOptions
	var vars []string
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show, task by task, where each job of a bundle target runs and what has to change",
		Long: `Resolve the bundle for the target the way the Databricks CLI does (includes,
variables, target overrides, presets) and report every task of every job:
its type, where it runs (clavesa or databricks), and ok, warn or refuse
with the reason. A refuse makes the plan exit 1. It needs no Databricks
login and makes no network call, so it runs in CI.

  clavesa bundle plan -t aws
  clavesa bundle plan --bundle ./my-bundle -t aws --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if o.Vars, err = parseBundleVars(vars); err != nil {
				return err
			}
			r, err := service.PlanBundle(cmd.Context(), o)
			if err != nil {
				return err
			}
			if jsonOut {
				if err := printJSON(os.Stdout, r); err != nil {
					return err
				}
			} else {
				fmt.Fprint(os.Stdout, r.Text())
			}
			if !r.Ready {
				return fmt.Errorf("bundle plan: %d refuse", r.Refuse)
			}
			return nil
		},
	}
	bundleFlags(cmd, &o, &vars)
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

// useBundleWorkspace points --workspace at the clavesa.workspace setting of
// the target when the flag was not given.
func useBundleWorkspace(cmd *cobra.Command, r *bundle.Report) error {
	f := cmd.Flag("workspace")
	if f == nil || f.Changed || r.Settings() == nil || r.Settings().Workspace == "" {
		return nil
	}
	ws := r.Settings().Workspace
	if strings.HasPrefix(ws, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		ws = filepath.Join(home, ws[2:])
	}
	return cmd.Flags().Set("workspace", ws)
}

func newBundleRunCmd() *cobra.Command {
	var o service.BundleOptions
	var vars, params []string
	var job, compute string
	var jsonOut, wait bool
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run one job of a bundle target on clavesa",
		Long: `Plan the bundle target, generate the job's clavesa pipeline into the
workspace (bundles/<bundle>_<job>/, read-only: the bundle is the source) and
run it. The plan must be ready (clavesa bundle plan says Ready: yes).

Job parameters take their defaults from the bundle; --param overrides one.
Databricks secrets the code reads come from the shell for a local run:
export CLAVESA_DBX_SECRET__<scope>__<key>.

  clavesa bundle run -t aws-local --job daily_sales --compute local
  clavesa bundle run -t aws-local --job daily_sales --param run_date=2026-11-12
  clavesa bundle run -t aws --job daily_sales --wait     # the deployed job`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if o.Vars, err = parseBundleVars(vars); err != nil {
				return err
			}
			p, err := parseBundleVars(params)
			if err != nil {
				return fmt.Errorf("--param: %w", err)
			}
			if compute != "" && compute != "local" && compute != "cloud" {
				return fmt.Errorf(`--compute must be "local" or "cloud", got %q`, compute)
			}
			r, err := service.PlanBundle(cmd.Context(), o)
			if err != nil {
				return err
			}
			if job == "" {
				if len(r.Jobs) != 1 {
					var keys []string
					for _, j := range r.Jobs {
						keys = append(keys, j.Key)
					}
					return fmt.Errorf("the bundle has several jobs: name one with --job (%s)", strings.Join(keys, ", "))
				}
				job = r.Jobs[0].Key
			}
			if err := useBundleWorkspace(cmd, r); err != nil {
				return err
			}
			svc, ws, err := newService(cmd)
			if err != nil {
				return err
			}
			warehouse := workspace.LoadWarehouse(ws)
			if warehouse != workspace.WarehouseLocal {
				if compute == "local" {
					return fmt.Errorf("bundle run --compute local needs a workspace with the local warehouse; this one is on the cloud warehouse: run without --compute local to start the deployed job")
				}
				// The deployed job (clavesa bundle deploy): start its state
				// machine with the run parameters.
				dir := filepath.Join(ws, service.BundlePipelineDir(r.Bundle, job))
				if !service.IsBundlePipeline(dir) {
					return fmt.Errorf("job %s is not deployed in this workspace: run clavesa bundle deploy -t %s first", job, r.Target)
				}
				return runCloudPipeline(cmd, dir, jsonOut, wait, service.RunOpts{BundleParams: p, Trigger: "manual"})
			}
			if compute == "cloud" {
				return fmt.Errorf("--compute cloud needs a workspace on the cloud warehouse with the job deployed (clavesa bundle deploy)")
			}
			gen, res, err := svc.RunBundleJob(cmd.Context(), o, job, p)
			if gen != nil && gen.Report != nil && !gen.Report.Ready {
				fmt.Fprint(os.Stdout, gen.Report.Text())
			}
			if err != nil {
				if res != nil && jsonOut {
					_ = printJSON(os.Stdout, res)
				}
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, res)
			}
			fmt.Printf("Pipeline: %s (%s)\n", gen.Name, gen.Dir)
			if res.RunID != "" {
				fmt.Printf("Run: %s\n", res.RunID)
			}
			rows := make([][]string, len(res.Nodes))
			for i, n := range res.Nodes {
				rows[i] = []string{n.NodeID, gen.JobFile.Tasks[n.NodeID].Type, n.Status}
			}
			printTable(os.Stdout, []string{"TASK", "TYPE", "STATUS"}, rows)
			for _, n := range res.Nodes {
				if n.Note != "" {
					fmt.Printf("  %s: %s\n", n.NodeID, n.Note)
				}
			}
			return nil
		},
	}
	bundleFlags(cmd, &o, &vars)
	cmd.Flags().StringVar(&job, "job", "", "the job to run (its key under resources.jobs); may be left out when the bundle has one job")
	cmd.Flags().StringArrayVar(&params, "param", nil, "a job parameter, name=value (repeatable)")
	cmd.Flags().StringVar(&compute, "compute", "", "local: the docker runner on the local warehouse; cloud (the default on a cloud warehouse): the deployed job")
	cmd.Flags().BoolVar(&wait, "wait", false, "with the deployed job: block until the run ends")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newBundleDeployCmd() *cobra.Command {
	var o service.BundleOptions
	var vars, jobs []string
	var autoApprove, planOnly, emitOnly bool
	cmd := &cobra.Command{
		Use:   "deploy",
		Short: "Deploy the jobs of a bundle target to AWS as clavesa pipelines",
		Long: `Plan the bundle target (it must be ready), generate one clavesa pipeline
per job into the workspace, and deploy each with Terraform the way
clavesa pipeline deploy does: the runner (Lambda, or Fargate sized from the
job's clusters), the Step Functions state machine, the bundle's files in
S3, the schedule (EventBridge Scheduler, with the job's time zone),
periodic and file_arrival triggers, an SNS topic per notification kind
with the job's email addresses, a CloudWatch alarm for a
RUN_DURATION_SECONDS health rule, and the runner's access to the job's
Glue databases and Databricks secrets (Secrets Manager
clavesa/<workspace>/dbx/<scope>/<key>).

The workspace must be deployed (clavesa workspace deploy). When the job
adds runner requirements, deploy the workspace again so the runner image
has them.

  clavesa bundle deploy -t aws --plan-only
  clavesa bundle deploy -t aws --yes`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if o.Vars, err = parseBundleVars(vars); err != nil {
				return err
			}
			r, err := service.PlanBundle(cmd.Context(), o)
			if err != nil {
				return err
			}
			if !r.Ready {
				fmt.Fprint(os.Stdout, r.Text())
				return fmt.Errorf("bundle plan: %d refuse", r.Refuse)
			}
			if err := useBundleWorkspace(cmd, r); err != nil {
				return err
			}
			svc, ws, err := newService(cmd)
			if err != nil {
				return err
			}
			if len(jobs) == 0 {
				for _, j := range r.Jobs {
					jobs = append(jobs, j.Key)
				}
			}
			for _, job := range jobs {
				gen, err := svc.GenerateBundleJob(cmd.Context(), o, job)
				if err != nil {
					return err
				}
				fmt.Printf("Job %s -> pipeline %s (%s)\n", job, gen.Name, gen.Dir)
				if len(gen.RequirementsAdded) > 0 {
					fmt.Printf("Added runner requirements %s: run clavesa workspace deploy so the runner image has them.\n", strings.Join(gen.RequirementsAdded, ", "))
				}
				if err := svc.SyncOrchestration(gen.Dir, ""); err != nil {
					return err
				}
				if emitOnly {
					fmt.Printf("Wrote %s/orchestration.tf (no terraform run).\n", gen.Dir)
					continue
				}
				if err := syncDatabricksAccess(cmd, svc, gen.Dir, os.Stdout); err != nil {
					return err
				}
				flow, err := newDeployFlow(cmd, ws, gen.Dir)
				if err != nil {
					return err
				}
				flow.AutoApprove = autoApprove
				flow.PlanOnly = planOnly
				if err := flow.Run(); err != nil {
					return fmt.Errorf("deploy %s: %w", gen.Name, err)
				}
			}
			return nil
		},
	}
	bundleFlags(cmd, &o, &vars)
	cmd.Flags().StringArrayVar(&jobs, "job", nil, "deploy only this job (repeatable; default: every job of the target)")
	cmd.Flags().BoolVarP(&autoApprove, "yes", "y", false, "skip the interactive 'Apply this plan?' confirmation")
	cmd.Flags().BoolVar(&planOnly, "plan-only", false, "stop after terraform plan (don't apply)")
	cmd.Flags().BoolVar(&emitOnly, "emit-only", false, "write the pipelines and their Terraform, run no terraform (for review)")
	return cmd
}

func newBundleCompareCmd() *cobra.Command {
	var o service.BundleOptions
	var vars []string
	var job string
	var days int
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare the tables a job writes on Databricks with clavesa's shadow tables (PASS or FAIL)",
		Long: `For every table the job writes, compare the Databricks job's table with the
table clavesa writes next to it in shadow mode (<table>_clavesa): rows and
the sum of each column in clavesa.compare.<table>.sums, per value of
clavesa.compare.<table>.partition, and the columns. The job passes when
every table does. Exit status 1 on FAIL, so a daily cron job can alert.

The target must have catalog: unity and shadow mode, and the job must be
deployed (clavesa bundle deploy): the deployed runner reads both tables
(a managed one through vended credentials), so it costs no DBUs.

  clavesa bundle compare -t aws --job daily_sales --days 7`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if o.Vars, err = parseBundleVars(vars); err != nil {
				return err
			}
			r, err := service.PlanBundle(cmd.Context(), o)
			if err != nil {
				return err
			}
			if job == "" && len(r.Jobs) == 1 {
				job = r.Jobs[0].Key
			}
			if job == "" {
				return fmt.Errorf("name the job with --job")
			}
			if err := useBundleWorkspace(cmd, r); err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			res, err := svc.CompareBundleJob(cmd.Context(), o, job, days)
			if err != nil {
				return err
			}
			if jsonOut {
				if err := printJSON(os.Stdout, res); err != nil {
					return err
				}
			} else {
				printBundleCompare(os.Stdout, res)
			}
			if !res.Pass {
				return fmt.Errorf("comparison FAILED for job %s", job)
			}
			return nil
		},
	}
	bundleFlags(cmd, &o, &vars)
	cmd.Flags().StringVar(&job, "job", "", "the job (may be left out when the bundle has one job)")
	cmd.Flags().IntVar(&days, "days", 0, "compare only the last N days of each table's partition column (0: all)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func printBundleCompare(w io.Writer, r *service.BundleCompareResult) {
	for _, t := range r.Tables {
		switch {
		case t.Skipped != "":
			fmt.Fprintf(w, "SKIP  %s: %s\n", t.Table, t.Skipped)
		case t.Error != "":
			fmt.Fprintf(w, "FAIL  %s: %s\n", t.Table, t.Error)
		default:
			status := "PASS"
			if !t.Result.Pass {
				status = "FAIL"
			}
			fmt.Fprintf(w, "%s  %s vs %s (%d partitions)\n", status, t.Table, t.Shadow, len(t.Result.Partitions))
			if !t.Result.Pass {
				printDatabricksCompare(w, *t.Result)
			}
		}
	}
	status := "PASS"
	if !r.Pass {
		status = "FAIL"
	}
	fmt.Fprintf(w, "\nJob %s (pipeline %s): %s\n", r.Job, r.Pipeline, status)
}

func newBundleSwitchCmd() *cobra.Command {
	var o service.BundleOptions
	var vars []string
	var job, dbxTarget string
	cmd := &cobra.Command{
		Use:   "switch",
		Short: "Print the steps and the exact SQL that switch a job's tables from Databricks to clavesa (runs nothing)",
		Long: `Print, table by table and in order, the switch of a job from its Databricks
target to its clavesa target: the history copy, pausing the Databricks job,
the name swap for managed tables, the grants, and the bundle change
(shadow: false, replaces_job) with the deploy. Clavesa runs nothing in
Databricks; a person who can run SQL there does those steps.

  clavesa bundle switch -t aws --job daily_sales --databricks-target prod`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if o.Vars, err = parseBundleVars(vars); err != nil {
				return err
			}
			if job == "" {
				return fmt.Errorf("name the job with --job")
			}
			text, err := service.BundleSwitchSteps(cmd.Context(), o, job, dbxTarget)
			if err != nil {
				return err
			}
			fmt.Fprint(os.Stdout, text)
			return nil
		},
	}
	bundleFlags(cmd, &o, &vars)
	cmd.Flags().StringVar(&job, "job", "", "the job")
	cmd.Flags().StringVar(&dbxTarget, "databricks-target", "", "the bundle target that deploys the job to Databricks (named in the steps)")
	return cmd
}

// bundleBridge adapts the service to the /api/bundle handler (the same
// calls as clavesa bundle plan and bundle list, ADR-015).
type bundleBridge struct {
	svc  *service.Service
	root string
}

func (b bundleBridge) PlanBundle(ctx context.Context, dir, target string) (any, error) {
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(b.root, dir)
	}
	return service.PlanBundle(ctx, service.BundleOptions{Dir: dir, Target: target})
}

func (b bundleBridge) BundlePipelines() (any, error) { return b.svc.BundlePipelines() }

func newBundleListCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the workspace's pipelines generated from bundle jobs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			list, err := svc.BundlePipelines()
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, list)
			}
			rows := make([][]string, len(list))
			for i, p := range list {
				cat := p.Catalog
				if p.Shadow {
					cat += " (shadow)"
				}
				rows[i] = []string{p.Pipeline, p.Bundle, p.Target, p.Job, cat, fmt.Sprint(p.Tasks)}
			}
			printTable(os.Stdout, []string{"PIPELINE", "BUNDLE", "TARGET", "JOB", "CATALOG", "TASKS"}, rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newBundleDestroyCmd() *cobra.Command {
	var o service.BundleOptions
	var vars, jobs []string
	var autoApprove bool
	cmd := &cobra.Command{
		Use:   "destroy",
		Short: "Remove a bundle target's jobs from AWS (terraform destroy of their pipelines)",
		Long: `For each job of the target (or the --job ones): terraform destroy of its
clavesa pipeline, after the runner-created Glue tables of the pipeline
are swept, as clavesa pipeline destroy does. The tables the job's code
wrote in its own Glue databases, and the shadow tables in Unity Catalog,
stay: they are data, not infrastructure; the command lists the shadow
tables with the SQL that drops them. The job on Databricks is not
touched (databricks bundle destroy -t <target> does that).

  clavesa bundle destroy -t aws --job daily_sales --yes`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if o.Vars, err = parseBundleVars(vars); err != nil {
				return err
			}
			r, err := service.PlanBundle(cmd.Context(), o)
			if err != nil {
				return err
			}
			if err := useBundleWorkspace(cmd, r); err != nil {
				return err
			}
			_, ws, err := newService(cmd)
			if err != nil {
				return err
			}
			if len(jobs) == 0 {
				for _, j := range r.Jobs {
					jobs = append(jobs, j.Key)
				}
			}
			for _, job := range jobs {
				dir := filepath.Join(ws, service.BundlePipelineDir(r.Bundle, job))
				if !service.IsBundlePipeline(dir) {
					fmt.Printf("Job %s: no pipeline %s in this workspace, nothing to destroy.\n", job, filepath.Base(dir))
					continue
				}
				if err := destroyPipeline(cmd, dir, ws, false, "", autoApprove); err != nil {
					return fmt.Errorf("destroy %s: %w", filepath.Base(dir), err)
				}
				if r.Shadow {
					if jp := findJobPlan(r, job); jp != nil {
						for _, tp := range jp.Tasks {
							for _, w := range tp.Writes {
								fmt.Printf("Unity Catalog shadow table kept: %s_clavesa (DROP TABLE %s_clavesa; to remove it)\n", w, w)
							}
						}
					}
				}
			}
			return nil
		},
	}
	bundleFlags(cmd, &o, &vars)
	cmd.Flags().StringArrayVar(&jobs, "job", nil, "destroy only this job (repeatable; default: every job of the target)")
	cmd.Flags().BoolVarP(&autoApprove, "yes", "y", false, "skip the confirmations")
	return cmd
}

func findJobPlan(r *bundle.Report, job string) *bundle.JobPlan {
	for i := range r.Jobs {
		if r.Jobs[i].Key == job {
			return &r.Jobs[i]
		}
	}
	return nil
}
