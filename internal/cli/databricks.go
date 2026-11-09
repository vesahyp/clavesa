package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/service"
)

// newWorkspaceConnectCmd is the parent of `clavesa workspace connect
// <target>`. Databricks is the only target (ADR-027).
func newWorkspaceConnectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect",
		Short: "Connect the workspace to an outside catalog",
		RunE:  requireSubcommand(),
	}
	cmd.AddCommand(newWorkspaceConnectDatabricksCmd())
	return cmd
}

func newWorkspaceDisconnectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disconnect",
		Short: "Remove a connection to an outside catalog",
		RunE:  requireSubcommand(),
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "databricks",
		Short: "Remove the Databricks connection from clavesa.json",
		Long: `Remove the Databricks connection from clavesa.json.

Tables already registered in Unity Catalog stay there. With a catalog
made by --create-catalog, the catalog, its schemas, the storage
credential, the external location and the IAM role behind them are taken
out of clavesa's Terraform state first: they stay as they are, and
clavesa no longer manages them. Every pipeline's orchestration.tf is
re-emitted without Unity Catalog; deploy the workspace and the pipelines
to apply that.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			released, err := svc.DisconnectDatabricks(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Println("Removed the Databricks connection.")
			if len(released) > 0 {
				fmt.Println("These stay in Databricks and AWS; clavesa no longer manages them:")
				for _, a := range released {
					fmt.Println("  " + a)
				}
			}
			fmt.Println("Next: clavesa deploy (workspace), then clavesa pipeline deploy <name> for each pipeline.")
			return nil
		},
	})
	return cmd
}

func newWorkspaceConnectDatabricksCmd() *cobra.Command {
	var in service.DatabricksConnectInput
	var publishTables, pushLineage, jsonOut bool
	cmd := &cobra.Command{
		Use:   "databricks",
		Short: "Write this workspace's tables into a Databricks Unity Catalog",
		Long: `Write this workspace's tables into Databricks Unity Catalog, so clavesa
can replace steps of existing Databricks pipelines (ADR-027).

  clavesa credential register dbx-sp --kind databricks-oauth \
    --client-id <application id> --secret env:DATABRICKS_CLIENT_SECRET
  clavesa workspace connect databricks \
    --host https://dbc-1234abcd-5678.cloud.databricks.com \
    --catalog analytics --credential dbx-sp

Each pipeline output is the table <catalog>.<schema>.<table>, where the
schema is the pipeline's unity_catalog_schema (default: its clavesa
schema) and the table is the output's clavesa table name. When that
table already exists as an external table, clavesa writes into it in
place; a managed table is refused. A table that does not exist yet is
registered as a new external table in clavesa's bucket (needs
--external-location, or --create-catalog).

By default --catalog is an existing catalog, and clavesa creates no
catalog and no schema. --create-catalog is for a Databricks workspace
with no real catalog: the next workspace deploy creates the catalog, its
storage on the workspace bucket and the pipelines' schemas.

The command checks every step a Databricks admin must do first and prints
the exact fix for each one that is missing. It records the connection in
clavesa.json only when no check fails. It never changes anything in
Databricks: the next deploy does that.

--publish-tables=false and --push-lineage=false turn off the table
registration or the lineage push. Both APIs are Public Preview.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if in.Host == "" || in.Catalog == "" || in.Credential == "" {
				return fmt.Errorf("--host, --catalog and --credential are required")
			}
			in.PublishTables = &publishTables
			in.PushLineage = &pushLineage
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			st, err := svc.ConnectDatabricks(cmd.Context(), in)
			if err != nil && !errors.Is(err, service.ErrDatabricksChecksFailed) {
				return err
			}
			if jsonOut {
				if perr := printJSON(os.Stdout, st); perr != nil {
					return perr
				}
			} else {
				printDatabricksChecks(os.Stdout, st.Checks)
			}
			if err != nil {
				return fmt.Errorf("not connected: a Databricks admin must do the steps above first, then run this command again")
			}
			if !jsonOut {
				fmt.Printf("\nConnected: tables are written as %s.<pipeline schema>.<table> on %s.\n", st.Connection.Catalog, st.Connection.Host)
				if st.Connection.CreateCatalog {
					fmt.Printf("The next workspace deploy creates catalog %s and its storage.\n", st.Connection.Catalog)
				}
				fmt.Println("Next: clavesa workspace deploy")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&in.Host, "host", "", "Databricks workspace URL (https://dbc-....cloud.databricks.com)")
	cmd.Flags().StringVar(&in.Catalog, "catalog", "", "Unity Catalog catalog to publish into")
	cmd.Flags().StringVar(&in.Credential, "credential", "", "name of a kind=databricks-oauth credential")
	cmd.Flags().StringVar(&in.ExternalLocation, "external-location", "", "admin-created external location over the workspace bucket, for new tables")
	cmd.Flags().BoolVar(&in.CreateCatalog, "create-catalog", false, "create --catalog, its storage and the pipelines' schemas at the next deploy (for a workspace with no real catalog)")
	cmd.Flags().BoolVar(&publishTables, "publish-tables", true, "register each output table in Unity Catalog")
	cmd.Flags().BoolVar(&pushLineage, "push-lineage", true, "push pipeline lineage into Unity Catalog")
	cmd.Flags().StringSliceVar(&in.Readers, "reader", nil, "Databricks user, group or service principal that reads the tables (from a SQL warehouse); gets SELECT on every table clavesa registers. Repeat or comma-separate")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

// newWorkspaceDatabricksCmd shows the recorded connection; --check runs
// the checks again.
func newWorkspaceDatabricksCmd() *cobra.Command {
	var check, jsonOut bool
	cmd := &cobra.Command{
		Use:   "databricks",
		Short: "Show the workspace's Databricks connection",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			var st service.DatabricksStatus
			if check {
				st, err = svc.CheckDatabricks(cmd.Context())
			} else {
				st, err = svc.GetDatabricks()
			}
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, st)
			}
			if !st.Connected {
				fmt.Println("not connected")
				return nil
			}
			c := st.Connection
			fmt.Printf("Host:              %s\n", c.Host)
			if c.CreateCatalog {
				fmt.Printf("Catalog:           %s (created by clavesa)\n", c.Catalog)
			} else {
				fmt.Printf("Catalog:           %s (existing)\n", c.Catalog)
			}
			fmt.Printf("Credential:        %s (client %s)\n", c.Credential, st.ClientID)
			switch {
			case c.ExternalLocation != "":
				fmt.Printf("External location: %s\n", c.ExternalLocation)
			case c.CreateCatalog:
				fmt.Println("External location: created by clavesa at deploy")
			default:
				fmt.Println("External location: none (existing external tables only)")
			}
			fmt.Printf("Publish tables:    %t\n", c.PublishTables)
			fmt.Printf("Push lineage:      %t\n", c.PushLineage)
			if len(c.Readers) > 0 {
				fmt.Printf("Readers:           %s\n", strings.Join(c.Readers, ", "))
			} else {
				fmt.Println("Readers:           none")
			}
			if check {
				fmt.Println()
				printDatabricksChecks(os.Stdout, st.Checks)
				if !databricks.AllPass(st.Checks) {
					return fmt.Errorf("a check failed")
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&check, "check", false, "run the admin checks against Databricks again")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.AddCommand(newWorkspaceDatabricksTablesCmd())
	cmd.AddCommand(newWorkspaceDatabricksPreflightCmd())
	return cmd
}

// newWorkspaceDatabricksPreflightCmd checks, read-only, every right and
// setting clavesa needs to replace one Databricks step, before anything
// is set up. It needs no clavesa workspace.
func newWorkspaceDatabricksPreflightCmd() *cobra.Command {
	var in service.PreflightInput
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "preflight",
		Short: "Check, read-only, what a Databricks and an AWS admin must grant before clavesa can replace a step",
		Long: `Check every right and setting clavesa needs to replace one step of a
Databricks pipeline, and print the exact grant for each one missing. It
only reads: nothing in Databricks or AWS changes. It needs no clavesa
workspace, so it can run before anything is set up.

Sign in as a person (a Databricks CLI profile) and name the service
principal clavesa will run as with --principal, or sign in as that
service principal (--client-id, --secret env:VAR). A person who is not an
admin may not be able to read another principal's rights; those rows say
"??" and the grants are listed anyway. AWS checks use your AWS login
(the account clavesa deploys into); --skip-aws leaves them out.

  clavesa workspace databricks preflight \
    --host https://dbc-1234abcd-5678.cloud.databricks.com --profile work \
    --principal <service principal application id> \
    --table main.sales.daily_events --shadow-location s3://team-bucket/clavesa/daily_events/ \
    --job 123456789 --input main.raw.events --reader analysts

Exit status 0 when nothing is missing, 1 otherwise.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cwd, _ := os.Getwd()
			res, err := service.New(cwd).Preflight(cmd.Context(), in)
			if err != nil {
				return err
			}
			if jsonOut {
				if err := printJSON(os.Stdout, res); err != nil {
					return err
				}
			} else {
				printDatabricksChecks(os.Stdout, res.Checks)
				printPreflightGrants(os.Stdout, res.Checks)
			}
			if !res.Ready {
				cmd.SilenceUsage = true
				return fmt.Errorf("not ready: an admin must make the changes above first")
			}
			if !jsonOut {
				fmt.Println("\nReady: nothing is missing.")
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&in.Host, "host", "", "Databricks workspace URL (https://dbc-....cloud.databricks.com)")
	f.StringVar(&in.Profile, "profile", "", "Databricks CLI profile to sign in with (a person's login)")
	f.StringVar(&in.ClientID, "client-id", "", "sign in as this service principal (application ID) instead")
	f.StringVar(&in.Secret, "secret", "", "the service principal's OAuth secret reference: env:VAR, file:<path> or a Secrets Manager ARN")
	f.StringVar(&in.Principal, "principal", "", "the service principal clavesa will run as (application ID); default: whoever signs in")
	f.StringVar(&in.Table, "table", "", "the table the replaced Databricks step writes, <catalog>.<schema>.<table>")
	f.StringVar(&in.Shadow, "shadow", "", "the shadow table clavesa writes next to it (default <table>_clavesa)")
	f.StringVar(&in.ShadowLocation, "shadow-location", "", "s3:// folder for the shadow table in the team's bucket")
	f.StringSliceVar(&in.Inputs, "input", nil, "Unity Catalog tables the step reads; repeat or comma-separate")
	f.Int64Var(&in.Job, "job", 0, "the Databricks job that runs the step today")
	f.Int64SliceVar(&in.StartJobs, "start-job", nil, "Databricks jobs clavesa starts after each successful run")
	f.StringSliceVar(&in.Readers, "reader", nil, "users or groups that read the table from SQL warehouses")
	f.BoolVar(&in.SkipAWS, "skip-aws", false, "leave the AWS checks out")
	f.StringVar(&in.Compute, "compute", "fargate", "where the step will run: fargate (checks its network and vCPU quota) or lambda")
	f.IntVar(&in.FargateCPU, "fargate-cpu", 16, "vCPUs of one Fargate task (the quota must allow it)")
	f.StringSliceVar(&in.Subnets, "subnet", nil, "subnets the Fargate task will run in (default: the default VPC's)")
	f.BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

// printPreflightGrants gathers every fix into one list an admin can work
// through: the SQL grants first, then the rest.
func printPreflightGrants(w io.Writer, checks []databricks.Check) {
	var sql, other []string
	for _, c := range checks {
		if c.Fix == "" || c.Status == databricks.StatusPass {
			continue
		}
		for _, line := range strings.Split(c.Fix, "\n") {
			if strings.HasPrefix(line, "GRANT ") || strings.HasPrefix(line, "CREATE ") {
				sql = append(sql, line)
			} else {
				other = append(other, "- "+line)
			}
		}
	}
	if len(sql) > 0 {
		fmt.Fprintln(w, "\nThe Databricks admin runs (SQL editor, as a metastore or catalog admin):")
		for _, s := range sql {
			fmt.Fprintln(w, "  "+s)
		}
	}
	if len(other) > 0 {
		fmt.Fprintln(w, "\nAlso:")
		for _, s := range other {
			fmt.Fprintln(w, s)
		}
	}
}

// newWorkspaceDatabricksTablesCmd lists what Unity Catalog holds in each
// pipeline's schema (ADR-027 slice 7). The UI twin is the table list on
// /unity-catalog.
func newWorkspaceDatabricksTablesCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "tables",
		Short: "List each pipeline's tables in Unity Catalog, with Catalog Explorer links",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			rows, err := svc.DatabricksTables(cmd.Context())
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, rows)
			}
			for _, p := range rows {
				fmt.Printf("%s  →  %s\n", p.Pipeline, p.Schema)
				if p.SchemaError != "" {
					fmt.Printf("    %s\n", p.SchemaError)
				}
				for _, t := range p.Tables {
					by := "not written by this pipeline"
					if t.ByClavesa {
						by = "written by clavesa"
					}
					fmt.Printf("    %-40s %-8s %s\n      %s\n", t.FullName, t.Type, by, t.URL)
				}
				if len(p.Jobs) > 0 {
					fmt.Printf("    starts Databricks jobs: %v\n", p.Jobs)
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

// newPipelineDatabricksJobsCmd shows or sets the Databricks jobs a
// pipeline starts after every successful run (ADR-027 section 6, A).
func newPipelineDatabricksJobsCmd() *cobra.Command {
	var set []int64
	var clear, jsonOut bool
	cmd := &cobra.Command{
		Use:   "databricks-jobs <pipeline>",
		Short: "Show or set the Databricks jobs started after every successful run",
		Long: `Show or set the Databricks jobs this pipeline starts after every
successful run (ADR-027, option A). Each job starts once, after the whole
run succeeded; the run page links to the Databricks run.

  clavesa pipeline databricks-jobs orders --set 263121218945262
  clavesa pipeline databricks-jobs orders --clear

Takes effect at the next ` + "`clavesa pipeline deploy`" + `. The service principal
needs CAN MANAGE RUN on each job.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if clear && len(set) > 0 {
				return fmt.Errorf("--set and --clear cannot be combined")
			}
			if clear || len(set) > 0 {
				if err := svc.SetDatabricksJobs(args[0], set); err != nil {
					return err
				}
			}
			jobs, err := svc.GetDatabricksJobs(args[0])
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, map[string]any{"pipeline": args[0], "jobs": jobs})
			}
			if len(jobs) == 0 {
				fmt.Println("no Databricks jobs")
			} else {
				fmt.Printf("Databricks jobs: %v\n", jobs)
			}
			if clear || len(set) > 0 {
				fmt.Printf("Next: clavesa pipeline deploy %s\n", args[0])
			}
			return nil
		},
	}
	cmd.Flags().Int64SliceVar(&set, "set", nil, "job IDs, comma-separated (replaces the list)")
	cmd.Flags().BoolVar(&clear, "clear", false, "start no jobs")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

// newPipelineDatabricksRunCmd shows what one run did in Databricks.
func newPipelineDatabricksRunCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "databricks-run <pipeline> <run-id>",
		Short: "Show the Unity Catalog tables a run wrote and the Databricks jobs it started",
		Long: `Show the Unity Catalog tables a run wrote and the Databricks jobs it
started, with links. <run-id> is the run's execution ARN, as ` + "`pipeline run`" + `
prints it, or its short ID as ` + "`pipeline runs`" + ` lists it.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			run, err := svc.GetDatabricksRun(cmd.Context(), args[0], args[1])
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, run)
			}
			printDatabricksRun(os.Stdout, run)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newPipelineDatabricksCompareCmd() *cobra.Command {
	var in service.DatabricksCompareInput
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "databricks-compare <pipeline>",
		Short: "Compare the replaced Databricks step's table with clavesa's shadow table, per partition",
		Long: `Compare the table the Databricks step writes (--existing) with the table
clavesa writes next to it in shadow mode (--shadow): rows and the sum of
each --sum column, per --partition value, and the columns. It passes only
when both tables have the same partitions and every number is equal.
Exit status 0 is pass, 1 is fail, so a daily cron job can alert on it.

The check runs in the local runner container against the cloud, with the
deployed pipeline's Databricks connection: Spark reads both Delta tables
directly (a managed table through credentials Unity Catalog vends), so it
costs no DBUs. Sums are exact (each value is cast to decimal(38,6)).

  clavesa pipeline databricks-compare events \
    --existing analytics.sales.daily_events --shadow analytics.sales.daily_events_clavesa \
    --partition event_date --sum amount,quantity \
    --where "event_date >= date_sub(current_date(), 7)"

See docs/cookbook/replace-a-databricks-step.md.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			res, err := svc.CompareDatabricksTables(cmd.Context(), args[0], in)
			if err != nil {
				return err
			}
			if jsonOut {
				if err := printJSON(os.Stdout, res); err != nil {
					return err
				}
			} else {
				printDatabricksCompare(os.Stdout, res)
			}
			if !res.Pass {
				cmd.SilenceUsage = true
				return fmt.Errorf("comparison FAILED: %s and %s differ", res.Existing, res.Shadow)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&in.Existing, "existing", "", "the table the Databricks step writes, <catalog>.<schema>.<table>")
	cmd.Flags().StringVar(&in.Shadow, "shadow", "", "the table clavesa writes next to it, <catalog>.<schema>.<table>")
	cmd.Flags().StringVar(&in.Partition, "partition", "", "the column both tables are partitioned by, e.g. event_date")
	cmd.Flags().StringSliceVar(&in.Sums, "sum", nil, "key columns to sum per partition (numeric); repeat or comma-separate")
	cmd.Flags().StringVar(&in.Where, "where", "", "Spark SQL predicate applied to both tables, e.g. \"event_date >= date_sub(current_date(), 7)\"")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func printDatabricksCompare(w io.Writer, r service.DatabricksCompareResult) {
	fmt.Fprintf(w, "existing: %s\nshadow:   %s\n", r.Existing, r.Shadow)
	if r.Where != "" {
		fmt.Fprintf(w, "where:    %s\n", r.Where)
	}
	fmt.Fprintln(w)
	num := func(p *int64) string {
		if p == nil {
			return "-"
		}
		return fmt.Sprint(*p)
	}
	for _, p := range r.Partitions {
		fmt.Fprintf(w, "%s=%s  rows %s / %s", r.Partition, p.Partition, num(p.Rows[0]), num(p.Rows[1]))
		keys := make([]string, 0, len(p.Sums))
		for k := range p.Sums {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "  sum(%s) %v / %v", k, orDash(p.Sums[k][0]), orDash(p.Sums[k][1]))
		}
		fmt.Fprintf(w, "  %s\n", p.Status)
	}
	if len(r.Partitions) == 0 {
		fmt.Fprintln(w, "no partitions in either table (check --where)")
	}
	for _, d := range r.SchemaDiff {
		fmt.Fprintf(w, "column differs: %s\n", d)
	}
	if r.Pass {
		fmt.Fprintf(w, "\nPASS: %d partitions match\n", len(r.Partitions))
	} else {
		fmt.Fprintln(w, "\nFAIL")
	}
}

func orDash(v any) any {
	if v == nil {
		return "-"
	}
	return v
}

func printDatabricksRun(w io.Writer, run service.DatabricksRun) {
	if !run.Found {
		fmt.Fprintln(w, "This run wrote nothing to Unity Catalog and started no Databricks job.")
		return
	}
	for _, t := range run.Tables {
		fmt.Fprintf(w, "Unity Catalog: %s %s  %s\n", t.FullName, t.Action, t.URL)
		if t.Lineage != "" && t.Lineage != "pushed" {
			fmt.Fprintf(w, "  lineage %s\n", t.Lineage)
		}
	}
	for _, j := range run.Jobs {
		if j.Error != "" {
			fmt.Fprintf(w, "Databricks job %d: not started: %s\n", j.JobID, j.Error)
			continue
		}
		fmt.Fprintf(w, "Databricks job %d: run %d  %s\n", j.JobID, j.RunID, j.RunPageURL)
	}
}

func printDatabricksChecks(w io.Writer, checks []databricks.Check) {
	for _, c := range checks {
		mark := map[string]string{
			databricks.StatusPass:    "ok  ",
			databricks.StatusFail:    "FAIL",
			databricks.StatusUnknown: "??  ",
			databricks.StatusSkipped: "skip",
		}[c.Status]
		fmt.Fprintf(w, "%s  %s", mark, c.Title)
		if c.Detail != "" {
			fmt.Fprintf(w, ": %s", c.Detail)
		}
		fmt.Fprintln(w)
		if c.Fix != "" {
			fmt.Fprintf(w, "      fix: %s\n", strings.ReplaceAll(c.Fix, "\n", "\n           "))
		}
	}
}

// syncDatabricksAccess writes the pipeline's team-storage grants
// (databricks_access.auto.tfvars) before its terraform runs, and prints
// what Unity Catalog said. A no-op without a Databricks connection.
func syncDatabricksAccess(cmd *cobra.Command, svc *service.Service, dir string, w io.Writer) error {
	acc, err := svc.SyncDatabricksAccess(cmd.Context(), dir)
	if err != nil {
		return fmt.Errorf("unity catalog lookup for the runner's storage grants: %w", err)
	}
	for _, n := range acc.Notes {
		fmt.Fprintf(w, "Unity Catalog: %s\n", n)
	}
	return nil
}
