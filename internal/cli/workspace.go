package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/api"
	"github.com/vesahyp/clavesa/internal/observability"
	tuiservice "github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/workspace"
)

func newWorkspaceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workspace",
		Short: "Manage workspaces",
		Long: `Manage workspaces (init, plan, deploy, destroy).

A workspace is a directory that contains one or more pipeline subdirectories.

Examples:
  clavesa workspace init my-project
  clavesa workspace init my-project --cloud aws
  clavesa workspace plan
  clavesa workspace deploy`,
		RunE: requireSubcommand(),
	}

	cmd.AddCommand(
		newWorkspaceInitCmd(),
		newWorkspaceUpgradeCmd(),
		newWorkspaceUseCmd(),
		newWorkspaceTablesCmd(),
		newWorkspacePlanCmd(),
		newWorkspaceDeployCmd(),
		newWorkspaceDestroyCmd(),
	)

	return cmd
}

func newWorkspaceUpgradeCmd() *cobra.Command {
	var shellOnly bool
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade the workspace shell and every pipeline to the binary's module version",
		Long: `Upgrade the workspace shell AND every pipeline in it to match the
running ` + "`clavesa`" + ` binary's ModuleVersion, then rebuild the local
runner image. One shot — no need to walk pipelines by hand.

Mechanics:
  - Re-extracts the embedded Terraform modules to .clavesa/modules/<version>/
    (idempotent; skips when the SHA stamp already matches).
  - Rewrites the workspace's ` + "`module \"workspace\"`" + ` source line to
    the new version with the leading "./" prefix Terraform 1.x requires.
  - Bumps ` + "`runner_version`" + `'s default in variables.tf so the next
    deploy pushes the matching runner image.
  - Upgrades every pipeline (rewrites module sources, strips deprecated
    module arguments, re-syncs orchestration.tf). Continue-on-error: a
    pipeline that fails is reported and the rest still run.
  - Rebuilds the local Docker runner image from the embedded runner
    sources, tagging both :latest and :<version>. The build runs every
    time; docker's layer cache makes a no-change rebuild a fast cache hit.

Pass --shell-only to upgrade just the workspace shell and skip the
pipeline walk — the pre-one-shot behaviour.

Does NOT touch clavesa.json or the rest of main.tf — your provider
blocks and any extra resources are preserved.

Run this after upgrading ` + "`clavesa`" + ` itself (` + "`brew upgrade clavesa`" + ` or
swapping the binary).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			target := tuiservice.ModuleVersion
			res, err := svc.UpgradeWorkspace(target, !shellOnly)
			if err != nil {
				return fmt.Errorf("workspace upgrade: %w", err)
			}
			// Image build is a separate step — UpgradeWorkspace is
			// pure-TF so pure-Go tests can exercise it without Docker.
			// Surface the image error but don't abort: the TF rewrites are
			// already on disk and useful on their own. The build is
			// unconditional; docker's layer cache makes a no-change rebuild
			// a fast cache hit.
			runnerNote := "runner image built"
			if _, imgErr := workspace.EnsureLocalRunnerImage(root); imgErr != nil {
				fmt.Fprintf(os.Stderr, "warning: build local runner image: %v\n", imgErr)
				runnerNote = "runner image build FAILED (see warning above)"
			}
			prev := res.PrevVersion
			switch {
			case prev == "":
				fmt.Printf("Workspace at %s: module source line not found in main.tf; modules refreshed, %s.\n", root, runnerNote)
			case prev == target && res.WorkspaceRewritten == 0:
				fmt.Printf("Workspace at %s: already on %s; modules refreshed, %s.\n", root, target, runnerNote)
			default:
				fmt.Printf("Upgraded workspace at %s: %s -> %s (%s)\n", root, prev, target, runnerNote)
				if res.WorkspaceRewritten > 0 {
					fmt.Printf("  shell: rewrote %d file(s) (main.tf / variables.tf)\n", res.WorkspaceRewritten)
				}
			}
			if shellOnly {
				fmt.Println("\nNext: run `clavesa pipeline upgrade <pipeline>` on each pipeline in this workspace.")
				return nil
			}
			// Per-pipeline roll-up. Empty when the workspace has no
			// pipelines yet.
			if len(res.Pipelines) == 0 {
				fmt.Println("\nNo pipelines to upgrade.")
				return nil
			}
			fmt.Printf("\nPipelines (%d):\n", len(res.Pipelines))
			failures := 0
			for _, p := range res.Pipelines {
				if p.Err != "" {
					failures++
					fmt.Printf("  %s: FAILED %s\n", p.Name, p.Err)
					continue
				}
				cur := p.CurrentRef
				if cur == "" {
					cur = "(none)"
				}
				fmt.Printf("  %s: %s -> %s (%d sources, %d migrated)\n",
					p.Name, cur, p.TargetRef, p.Updated, p.Migrated)
			}
			if failures > 0 {
				return fmt.Errorf("%d pipeline(s) failed to upgrade", failures)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&shellOnly, "shell-only", false, "upgrade only the workspace shell; skip the per-pipeline walk")
	return cmd
}

func newWorkspaceInitCmd() *cobra.Command {
	var cloud string
	var catalog string

	cmd := &cobra.Command{
		Use:   "init <name>",
		Short: "Initialize a new workspace",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			// workspace init uses the simpler --workspace-or-cwd resolution
			// rather than the env-var/state-file chain — a remembered
			// workspace from a previous session shouldn't redirect a
			// brand-new init into someone else's directory. When no
			// --workspace is given, scaffold into ./<name>/ rather than
			// the current directory — `init` is creation, not in-place.
			root, err := resolveWorkspaceForInit(cmd, name)
			if err != nil {
				return err
			}
			if _, statErr := os.Stat(filepath.Join(root, "clavesa.json")); statErr == nil {
				return fmt.Errorf("workspace init: %s is already a clavesa workspace (clavesa.json exists)", root)
			}
			if err := workspace.Init(root, name, cloud, catalog, tuiservice.ModuleVersion); err != nil {
				return fmt.Errorf("workspace init: %w", err)
			}
			// Remember the new workspace so subsequent commands don't need
			// --workspace. Best-effort: a state-file write failure does
			// not abort the init.
			stateNote := ""
			if err := writeWorkspaceStateFile(root); err != nil {
				stateNote = fmt.Sprintf("  (could not record current workspace: %v)\n", err)
			}
			fmt.Printf("Initialized workspace %q at %s\n", name, root)
			if stateNote != "" {
				fmt.Print(stateNote)
			}
			fmt.Println()
			fmt.Println("Next steps:")
			fmt.Println("  clavesa pipeline create <name>          # add a pipeline")
			fmt.Println("  clavesa ui                              # open the visual editor")
			fmt.Println()
			fmt.Println("To pin this workspace in your shell:")
			fmt.Printf("  export CLAVESA_WORKSPACE=%s\n", root)
			fmt.Println()
			fmt.Println("To deploy to AWS later:  clavesa workspace deploy --workspace " + root)
			return nil
		},
	}

	cmd.Flags().StringVar(&cloud, "cloud", "aws", "cloud provider (aws)")
	cmd.Flags().StringVar(&catalog, "catalog", "", "three-level-namespace catalog identifier (default: clavesa_<sanitize(name)>)")

	return cmd
}

// resolveWorkspaceForInit only consults --workspace and cwd. Skips the env-
// var / state-file chain so a previously-remembered workspace doesn't
// redirect a fresh `workspace init` into a stale parent directory.
//
// When --workspace is omitted, the workspace dir defaults to ./<name>/
// in the current directory — `init` is a creation step, so scaffolding
// in-place over whatever else lives in cwd is the surprising option.
// An explicit --workspace path is used verbatim.
func resolveWorkspaceForInit(cmd *cobra.Command, name string) (string, error) {
	if w, _ := cmd.Flags().GetString("workspace"); w != "" {
		return filepath.Abs(w)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(cwd, name), nil
}

func newWorkspaceUseCmd() *cobra.Command {
	var warehouseFlag string
	var env string
	var profile string
	profileSet := false
	cmd := &cobra.Command{
		Use:   "use [path]",
		Short: "Switch the current workspace, or set its warehouse / AWS profile",
		Long: `With <path>, record it as the current workspace — subsequent commands
without --workspace and without $CLAVESA_WORKSPACE resolve to it. The
selection is stored in $XDG_CONFIG_HOME/clavesa/current-workspace
(default: ~/.config/clavesa/current-workspace).

With --warehouse, set where all workspace state lives:

  - local: author and run against the local runner + Hadoop catalog.
  - cloud: operate the deployed pipeline (Step Functions, Glue, Athena).

The warehouse is stored per-workspace in .clavesa/environment.json
(gitignored) and defaults to "local". It drives local-vs-cloud dispatch
for pipeline runs and the observability surfaces. (--env is the
deprecated alias.)

With --profile, set the AWS profile the workspace operates as — the
profile ` + "`clavesa ui`" + ` resolves AWS credentials from, and forwards
into the runner for S3-source reads. Stored in
.clavesa/aws-profile.json (gitignored). Pass an empty value
(--profile "") to clear the override and fall back to the ambient
AWS_PROFILE / default credential chain. The profile must exist in
~/.aws; a running ` + "`clavesa ui`" + ` server picks the change up on its
next start.

Run with no arguments to print the current workspace, warehouse, and AWS profile.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var root string
			if len(args) == 1 {
				abs, err := filepath.Abs(args[0])
				if err != nil {
					return fmt.Errorf("resolve path: %w", err)
				}
				if _, err := os.Stat(filepath.Join(abs, "clavesa.json")); err != nil {
					return fmt.Errorf("%s does not look like a clavesa workspace (no clavesa.json)", abs)
				}
				if err := writeWorkspaceStateFile(abs); err != nil {
					return fmt.Errorf("write state file: %w", err)
				}
				root = abs
			} else {
				r, err := resolveWorkspace(cmd)
				if err != nil {
					return err
				}
				if _, err := os.Stat(filepath.Join(r, "clavesa.json")); err != nil {
					return fmt.Errorf("%s does not look like a clavesa workspace (no clavesa.json)", r)
				}
				root = r
			}
			// --warehouse wins when both it and the deprecated --env
			// alias are set.
			raw := warehouseFlag
			if raw == "" {
				raw = env
			}
			if raw != "" {
				wh, ok := workspace.ParseWarehouse(raw)
				if !ok {
					return fmt.Errorf(`--warehouse must be "local" or "cloud", got %q`, raw)
				}
				if err := workspace.WriteWarehouse(root, wh); err != nil {
					return fmt.Errorf("write warehouse: %w", err)
				}
			}
			if profileSet {
				if profile != "" {
					avail := workspace.ListAWSProfiles()
					if !slices.Contains(avail, profile) {
						return fmt.Errorf("AWS profile %q not found in ~/.aws (available: %s)",
							profile, strings.Join(avail, ", "))
					}
				}
				if err := workspace.WriteAWSProfile(root, profile); err != nil {
					return fmt.Errorf("write AWS profile: %w", err)
				}
			}
			fmt.Printf("Current workspace: %s\n", root)
			fmt.Printf("Warehouse:         %s\n", workspace.LoadWarehouse(root))
			if p := workspace.LoadAWSProfile(root); p != "" {
				fmt.Printf("AWS profile:       %s\n", p)
			} else {
				fmt.Printf("AWS profile:       (ambient / default credential chain)\n")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&warehouseFlag, "warehouse", "", `set the workspace warehouse: "local" or "cloud"`)
	cmd.Flags().StringVar(&env, "env", "", `deprecated alias for --warehouse`)
	cmd.Flags().MarkHidden("env")
	cmd.Flags().MarkDeprecated("env", "use --warehouse")
	cmd.Flags().StringVar(&profile, "profile", "", `set the AWS profile (must exist in ~/.aws); "" clears the override`)
	cmd.PreRun = func(cmd *cobra.Command, _ []string) {
		profileSet = cmd.Flags().Changed("profile")
	}
	return cmd
}

// newWorkspaceTablesCmd lists every Iceberg table the workspace catalog
// owns — the CLI counterpart of the Catalog page (ADR-015 parity). Uses
// the same api.CatalogHandler.Tables core the UI's GET /workspace/tables
// route does, so both surfaces report an identical list. Cloud (Glue)
// tables need AWS; local-pipeline tables come from the workspace walk
// and surface even without credentials.
func newWorkspaceTablesCmd() *cobra.Command {
	var jsonOut bool
	var catalogFilter, schemaFilter string

	cmd := &cobra.Command{
		Use:   "tables",
		Short: "List Iceberg tables in the workspace catalog",
		Long: `List every Iceberg table the workspace catalog owns.

Filter to one catalog or schema (ADR-016 three-level namespace) — the
CLI twin of the Catalog page's ?catalog=&schema= view:

  clavesa workspace tables --schema taxis
  clavesa workspace tables --catalog clavesa_demo_ws --json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()

			// Glue client is best-effort: a workspace with only
			// compute=local pipelines (or no AWS creds) still has tables
			// worth listing. Mirrors the UI handler's nil-Glue degradation.
			// The S3 client (built from the same config) lets the cloud half
			// read each Delta table's real schema from its `_delta_log/` —
			// Glue only carries a stub `col array<string>` for Delta tables.
			var glueClient api.GlueClient
			var s3Client api.S3API
			if cfg, cfgErr := config.LoadDefaultConfig(ctx); cfgErr == nil {
				glueClient = glue.NewFromConfig(cfg)
				s3Client = s3.NewFromConfig(cfg)
			}

			catalogHandler := api.NewCatalogHandler(glueClient).WithWorkspace(root)
			if s3Client != nil {
				catalogHandler = catalogHandler.WithS3(s3Client)
			}
			resp := catalogHandler.Tables(ctx)
			// ADR-016: a Glue DB name encodes <catalog>__<schema>.
			// --catalog / --schema filter on those pieces.
			if catalogFilter != "" || schemaFilter != "" {
				kept := resp.Tables[:0]
				for _, t := range resp.Tables {
					cat, sch := t.Database, ""
					if i := strings.Index(t.Database, "__"); i >= 0 {
						cat, sch = t.Database[:i], t.Database[i+2:]
					}
					if catalogFilter != "" && cat != catalogFilter {
						continue
					}
					if schemaFilter != "" && sch != schemaFilter {
						continue
					}
					kept = append(kept, t)
				}
				resp.Tables = kept
			}
			if len(resp.Tables) == 0 {
				if jsonOut {
					return printJSON(os.Stdout, catalogResponseWithMetrics{
						Tables:       []catalogTableWithMetrics{},
						AWSAvailable: resp.AWSAvailable,
					})
				}
				if catalogFilter != "" || schemaFilter != "" {
					fmt.Println("No tables match the filter.")
				} else {
					fmt.Println("No tables yet — run a pipeline to produce one.")
				}
				if !resp.AWSAvailable {
					fmt.Println("(AWS not configured — cloud Glue tables, if any, are not shown.)")
				}
				return nil
			}

			// Enrich each table with the observability layer's per-table
			// file count and byte size (ADR-014: same figures the UI's
			// tables-state surfaces). Best-effort — a workspace with no
			// warehouse or no creds simply renders "—" for the metrics
			// rather than failing the listing. One provider call per
			// distinct owning pipeline; matched back to catalog rows by
			// table name.
			enriched := enrichTablesWithMetrics(cmd, ctx, resp.Tables)

			if jsonOut {
				return printJSON(os.Stdout, catalogResponseWithMetrics{
					Tables:       enriched,
					AWSAvailable: resp.AWSAvailable,
				})
			}
			rows := make([][]string, len(enriched))
			for i, t := range enriched {
				rows[i] = []string{
					t.Database, t.Name,
					strconv.Itoa(len(t.Columns)),
					fileCountCell(t.FileCount),
					avgSizeCell(t.FileCount, t.TotalBytes),
					t.OwningPipeline,
				}
			}
			printTable(os.Stdout, []string{"DATABASE", "TABLE", "COLUMNS", "FILES", "AVG SIZE", "PIPELINE"}, rows)
			if !resp.AWSAvailable {
				fmt.Println("\n(AWS not configured — only local-pipeline tables shown.)")
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().StringVar(&catalogFilter, "catalog", "", "show only tables in this catalog")
	cmd.Flags().StringVar(&schemaFilter, "schema", "", "show only tables in this schema")

	return cmd
}

// tablesStateLimit caps how many per-table rows the observability layer
// returns per owning pipeline when enriching the catalog listing. Well
// above any realistic single-pipeline output-table count.
const tablesStateLimit = 500

// catalogTableWithMetrics wraps api.CatalogTable with the observability
// layer's per-table file count and byte size. It's a CLI-local view struct
// (not a change to api.CatalogTable) so the enrichment stays contained to
// the CLI and does not touch the UI's /workspace/tables wire shape or its
// Zod boundary. The embedded CatalogTable flattens into the JSON at the top
// level, so `file_count` / `total_bytes` sit alongside the existing fields.
type catalogTableWithMetrics struct {
	api.CatalogTable
	FileCount  *int64 `json:"file_count,omitempty"`
	TotalBytes *int64 `json:"total_bytes,omitempty"`
}

// catalogResponseWithMetrics mirrors api.CatalogResponse for the enriched
// `--json` output.
type catalogResponseWithMetrics struct {
	Tables       []catalogTableWithMetrics `json:"tables"`
	AWSAvailable bool                      `json:"aws_available"`
}

// enrichTablesWithMetrics attaches per-table file-count / byte-size metrics
// (from the observability layer) onto each catalog row. It groups the
// tables by owning pipeline, calls TablesStateForDir once per distinct
// pipeline, and matches the returned rows back by table name. Best-effort:
// a failure to build the service or fetch a pipeline's tables leaves the
// affected rows with nil metrics (rendered "—") rather than failing.
func enrichTablesWithMetrics(cmd *cobra.Command, ctx context.Context, tables []api.CatalogTable) []catalogTableWithMetrics {
	byDir := map[string][]observability.TableInfo{}
	if svc, _, err := newService(cmd); err == nil {
		// dir -> owning pipeline (the sanitized schema form the runner
		// writes into the tables `pipeline` column).
		seen := map[string]string{}
		for _, t := range tables {
			if t.Dir == "" || t.OwningPipeline == "" {
				continue
			}
			seen[t.Dir] = t.OwningPipeline
		}
		for dir, pipeline := range seen {
			res, err := svc.TablesStateForDir(ctx, pipeline, dir, tablesStateLimit)
			if err != nil || res == nil {
				continue
			}
			byDir[dir] = res.Rows
		}
	}
	return mergeTableMetrics(tables, byDir)
}

// mergeTableMetrics is the pure join between catalog rows and the
// observability tables-state rows (keyed by pipeline dir). Match is by
// table name, falling back to `<node>__<output_key>`. Rows with no owning
// pipeline dir, or whose dir the fetch couldn't answer for, keep nil
// metrics. Extracted for unit testing without a live provider.
func mergeTableMetrics(tables []api.CatalogTable, byDir map[string][]observability.TableInfo) []catalogTableWithMetrics {
	idx := make(map[string]map[string]observability.TableInfo, len(byDir))
	for dir, rows := range byDir {
		m := make(map[string]observability.TableInfo, len(rows)*2)
		for _, ti := range rows {
			if ti.TableName != "" {
				m[ti.TableName] = ti
			}
			if ti.Node != "" {
				m[ti.Node+"__"+ti.OutputKey] = ti
			}
		}
		idx[dir] = m
	}
	out := make([]catalogTableWithMetrics, len(tables))
	for i, t := range tables {
		out[i] = catalogTableWithMetrics{CatalogTable: t}
		m, ok := idx[t.Dir]
		if !ok {
			continue
		}
		ti, ok := m[t.Name]
		if !ok {
			ti, ok = m[t.OwningNode+"__"+t.OutputKey]
		}
		if !ok {
			continue
		}
		out[i].FileCount = ti.FileCount
		out[i].TotalBytes = ti.TotalBytes
	}
	return out
}

// fileCountCell renders a nullable file count, or an em-dash when unknown.
func fileCountCell(v *int64) string {
	if v == nil {
		return "—"
	}
	return strconv.FormatInt(*v, 10)
}

// avgSizeCell renders total_bytes / file_count as a human-readable size, or
// an em-dash when either input is missing or the file count is zero.
func avgSizeCell(fileCount, totalBytes *int64) string {
	if fileCount == nil || totalBytes == nil || *fileCount == 0 {
		return "—"
	}
	return formatBytes(*totalBytes / *fileCount)
}

// formatBytes renders a byte count in binary units (B/KB/MB/GB/…) with one
// decimal place above the byte range.
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

func newWorkspacePlanCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "plan",
		Short: "Run terraform plan on the workspace",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			if err := tfInit(root, os.Stdout, os.Stderr); err != nil {
				return fmt.Errorf("terraform init: %w", err)
			}
			c := exec.Command("terraform", "plan")
			c.Dir = root
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			return c.Run()
		},
	}
}

func newWorkspaceDeployCmd() *cobra.Command {
	var autoApprove bool
	var planOnly bool
	cmd := &cobra.Command{
		Use:   "deploy",
		Short: "terraform init -upgrade → plan -out=tfplan → apply tfplan, with preflight",
		Long: `Run the substantive deploy lifecycle against the workspace's terraform.

Preflight refuses to invoke terraform unless clavesa.json is present and
the local runner image's clavesa.runner_sha label matches the embedded
runner files (catches the stale-image-pushed-silently case). The flow saves
the plan to ./tfplan and pauses for a 'yes' confirmation before applying;
the plan is cleaned up on success.

Use --yes to skip the confirmation prompt (for CI / scripted use).
Use --plan-only to stop after plan without applying.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			printTargetContext("workspace deploy", root, "")
			return deployFlow{
				WorkspaceRoot:    root,
				TfDir:            root,
				BuildRunnerImage: true,
				AutoApprove:      autoApprove,
				PlanOnly:         planOnly,
			}.Run()
		},
	}
	cmd.Flags().BoolVarP(&autoApprove, "yes", "y", false, "skip the interactive 'Apply this plan?' confirmation")
	cmd.Flags().BoolVar(&planOnly, "plan-only", false, "stop after terraform plan (don't apply)")
	return cmd
}

func newWorkspaceDestroyCmd() *cobra.Command {
	var skipSweep bool
	var autoApprove bool
	cmd := &cobra.Command{
		Use:   "destroy",
		Short: "terraform destroy on the workspace (sweeping system-catalog Glue tables first)",
		Long: `Run terraform destroy after deleting Glue tables that the runner + the
runs_writer Lambda created at runtime against the workspace-wide system
catalog. The system DB holds runs / node_runs / tables — workspace-shared
Iceberg tables, multi-writer across every pipeline in the workspace
(ADR-016 v0.20.0). They aren't in terraform state, so without the sweep,
` + "`aws_glue_catalog_database.system_pipelines`" + ` refuses to destroy
with "database is not empty".

Tear down individual pipelines first via ` + "`clavesa pipeline destroy`" + `
— this command does not chain into per-pipeline destroys. The sweep
targets the system DB only (` + "`<system_catalog>__pipelines`" + ` per ADR-016).

Workspace destroy also pre-empties the versioned workspace S3 bucket
and drains the Athena workgroup with RecursiveDeleteOption=true so
terraform destroy doesn't 409 on bucket / workgroup state.

--skip-sweep bypasses the sweep step; the sweep itself asks for explicit
'yes' confirmation before deleting anything.
Use --yes to skip both the sweep confirmation and the terraform destroy
prompt (for CI / scripted use). The workspace name + path are still
echoed to stderr before any AWS calls.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			root, err := resolveWorkspace(cmd)
			if err != nil {
				return err
			}
			printTargetContext("workspace destroy", root, "")
			if !skipSweep {
				var sweepIn io.Reader = os.Stdin
				if autoApprove {
					sweepIn = autoYesReader()
				}
				if err := sweepWorkspaceSystemGlueTables(cmd.Context(), root, os.Stdout, sweepIn); err != nil {
					return err
				}
			}

			// Pre-flight: empty the versioned workspace bucket and drain
			// the Athena workgroup. Both block `terraform destroy` if
			// they still carry runtime-written state. The workspace
			// module names them deterministically — see
			// `modules/workspace/aws/main.tf` (bucket: `<name>-clavesa`,
			// workgroup: `clavesa-<name>`).
			m, mErr := workspace.Load(root)
			if mErr != nil {
				return fmt.Errorf("load manifest: %w", mErr)
			}
			if m == nil {
				return fmt.Errorf("not a clavesa workspace at %s (no clavesa.json)", root)
			}
			bucketName := m.Name + "-clavesa"
			workgroupName := "clavesa-" + m.Name
			if err := emptyVersionedBucket(cmd.Context(), bucketName, os.Stderr); err != nil {
				return err
			}
			if err := drainAthenaWorkgroup(cmd.Context(), workgroupName, os.Stderr); err != nil {
				return err
			}

			args2 := []string{"destroy"}
			if autoApprove {
				args2 = append(args2, "-auto-approve")
			}
			c := exec.Command("terraform", args2...)
			c.Dir = root
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			c.Stdin = os.Stdin
			return c.Run()
		},
	}
	cmd.Flags().BoolVar(&skipSweep, "skip-sweep", false, "skip the Glue-table sweep preflight")
	cmd.Flags().BoolVarP(&autoApprove, "yes", "y", false, "skip the interactive confirmation prompt")
	return cmd
}
