package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/notebooks"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// newNotebookCmd implements the Slice 1 `clavesa notebook` noun: workspace-
// level multi-cell SQL + PySpark notebooks stored as .ipynb (Jupyter
// nbformat 4.5) for native GitHub rendering and JupyterLab interop. Cells
// run against the warm-Spark Connect container with per-notebook session
// isolation (Slice 0 prep'd this); persistent Python globals across cells
// match the Databricks notebook feel.
//
// CLI/UI parity per ADR-015: every UI capability has a CLI verb here.
func newNotebookCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "notebook",
		Short: "Multi-cell SQL + PySpark notebooks (Jupyter .ipynb)",
		Long: `Manage workspace notebooks — Databricks-style multi-cell SQL + PySpark.

Notebooks live as .ipynb files under <workspace>/notebooks/, so GitHub
renders them natively and JupyterLab can open them offline. Cells run
against the warm Spark Connect server with per-notebook SparkSession
isolation; Python globals and SQL temp views persist across cells in
the same notebook.

Examples:
  clavesa notebook create exploration
  clavesa notebook list
  clavesa notebook show exploration
  clavesa notebook run exploration
  clavesa notebook run exploration --cell <id> --json
  clavesa notebook session stop exploration
  clavesa notebook clear-outputs exploration   # git-friendly commit prep
  clavesa notebook delete exploration`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(
		newNotebookCreateCmd(),
		newNotebookListCmd(),
		newNotebookShowCmd(),
		newNotebookDeleteCmd(),
		newNotebookRunCmd(),
		newNotebookSessionCmd(),
		newNotebookClearOutputsCmd(),
		newNotebookGraduateCmd(),
	)
	return cmd
}

func newNotebookGraduateCmd() *cobra.Command {
	var pipeline, transformName string
	cmd := &cobra.Command{
		Use:   "graduate <notebook> --cell <id> --to <pipeline> --as <transform>",
		Short: "Promote a notebook cell into a transform node",
		Long: `Turn an explored notebook cell into a pipeline transform.

Writes the cell source to <pipeline>/transforms/<transform>.{sql,py}
(stripping the leading %%magic) and registers a new transform node in
the pipeline's main.tf. The cell's language (SQL vs Python) determines
the file extension and the node's language attribute.

The graduated transform has no inputs wired — attach sources or connect
upstream nodes via the editor afterward.

Examples:
  clavesa notebook graduate exploration --cell c2py --to demo --as enrich_orders
  clavesa notebook graduate scratch    --cell 1a2b  --to demo --as revenue`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cellID, _ := cmd.Flags().GetString("cell")
			if cellID == "" {
				return fmt.Errorf("--cell <id> is required")
			}
			if pipeline == "" {
				return fmt.Errorf("--to <pipeline> is required")
			}
			if transformName == "" {
				return fmt.Errorf("--as <transform> is required")
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if _, err := svc.GraduateCell(args[0], cellID, pipeline, transformName); err != nil {
				return err
			}
			fmt.Printf("Graduated %s/%s → %s/transforms/%s\n",
				args[0], cellID, pipeline, transformName)
			return nil
		},
	}
	cmd.Flags().String("cell", "", "Cell ID to graduate")
	cmd.Flags().StringVar(&pipeline, "to", "", "Target pipeline directory (must exist)")
	cmd.Flags().StringVar(&transformName, "as", "", "Transform node name to create")
	return cmd
}

func newNotebookCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create <name>",
		Short: "Create an empty notebook",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			nb, err := svc.CreateNotebook(args[0])
			if err != nil {
				return err
			}
			fmt.Printf("Created notebook %s\n", nb.Name)
			return nil
		},
	}
}

func newNotebookListCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List notebooks in the workspace",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			list, err := svc.ListNotebooks()
			if err != nil {
				return err
			}
			if asJSON {
				return json.NewEncoder(os.Stdout).Encode(list)
			}
			if len(list) == 0 {
				fmt.Println("No notebooks. Create one with `clavesa notebook create <name>`.")
				return nil
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tCELLS\tUPDATED")
			for _, n := range list {
				fmt.Fprintf(tw, "%s\t%d\t%s\n", n.Name, n.CellCount, n.ModTime)
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit JSON")
	return cmd
}

func newNotebookShowCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "show <name>",
		Short: "Print the notebook (cell sources, no outputs)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			nb, err := svc.GetNotebook(args[0])
			if err != nil {
				return err
			}
			if asJSON {
				return json.NewEncoder(os.Stdout).Encode(nb)
			}
			fmt.Printf("notebook %s · %d cells\n\n", nb.Name, len(nb.Cells))
			for i, c := range nb.Cells {
				kind := c.CellType
				if c.CellType == notebooks.CellTypeCode {
					lang := detectLanguageDisplay(strings.Join(c.Source, ""))
					kind = "code/" + lang
				}
				status := "—"
				if c.Metadata.Clavesa != nil && c.Metadata.Clavesa.LastStatus != "" {
					status = c.Metadata.Clavesa.LastStatus
				}
				fmt.Printf("# Cell %d · %s · id=%s · last=%s\n", i+1, kind, c.ID, status)
				fmt.Println(strings.Join(c.Source, ""))
				fmt.Println()
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit nbformat JSON (full .ipynb)")
	return cmd
}

func newNotebookDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a notebook (also stops its REPL if running)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.DeleteNotebook(args[0]); err != nil {
				return err
			}
			fmt.Printf("Deleted notebook %s\n", args[0])
			return nil
		},
	}
}

func newNotebookRunCmd() *cobra.Command {
	var (
		cellID string
		asJSON bool
	)
	cmd := &cobra.Command{
		Use:   "run <name>",
		Short: "Run every cell (or one cell with --cell) and persist outputs",
		Args:  cobra.ExactArgs(1),
		Long: `Run cells through the warm Spark Connect container.

If --cell <id> is given, only that cell runs. Otherwise every code cell
runs sequentially in notebook order, in the SAME REPL subprocess —
SparkSession + Python globals persist across cells, matching what the UI
gives you.

This spawns its own warm worker container if one isn't already running,
which adds ~30s of Spark cold start on first invocation. Subsequent runs
in the same workspace reuse the running container.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			svc, wsRoot, err := newService(cmd)
			if err != nil {
				return err
			}
			nb, err := svc.GetNotebook(name)
			if err != nil {
				return err
			}

			// Spin up a warm container for the duration of this CLI run.
			// Stays alive across cells (one cold start, not N). Stops on
			// process exit via defer.
			warm := observability.NewPersistentQueryRunner(wsRoot)
			defer warm.Close()
			ctx := cmd.Context()
			warm.Warmup(ctx, defaultWarehouseDir(wsRoot))

			nbRunner := observability.NewNotebookSessionRunner(warm)
			defer nbRunner.Close()
			svc = svc.WithNotebookRunner(nbRunner)

			targets := make([]string, 0, len(nb.Cells))
			if cellID != "" {
				targets = append(targets, cellID)
			} else {
				for _, c := range nb.Cells {
					if c.CellType == notebooks.CellTypeCode {
						targets = append(targets, c.ID)
					}
				}
			}

			results := make([]map[string]any, 0, len(targets))
			for _, id := range targets {
				res, err := svc.RunCell(ctx, name, id)
				if err != nil {
					results = append(results, map[string]any{
						"cell_id": id, "error": err.Error(),
					})
					if !asJSON {
						fmt.Printf("cell %s · ERROR: %s\n", id, err)
					}
					continue
				}
				results = append(results, map[string]any{
					"cell_id": id, "result": res.Result,
				})
				if !asJSON {
					fmt.Printf("cell %s · %s · %dms\n", id, res.Result.Status, res.Result.DurationMS)
					if res.Result.Status == "error" && res.Result.Error != nil {
						fmt.Printf("  %s: %s\n", res.Result.Error.EName, res.Result.Error.EValue)
					}
				}
			}
			if asJSON {
				return json.NewEncoder(os.Stdout).Encode(results)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cellID, "cell", "", "Run only this cell ID (default: all code cells)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit CellResult[] JSON")
	return cmd
}

func newNotebookSessionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "session",
		Short: "Manage notebook REPL sessions",
		RunE:  requireSubcommand(),
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "stop <name>",
			Short: "Stop the REPL subprocess for a notebook (loses Python globals)",
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				name := args[0]
				svc, wsRoot, err := newService(cmd)
				if err != nil {
					return err
				}
				// We need the same notebookSessionRunner that the running
				// `clavesa ui` has so its in-memory REPL map gets cleared.
				// CLI processes don't share memory with the UI, so the best
				// we can do is DELETE on the warm worker's HTTP supervisor
				// directly — the UI's runner will discover the REPL is gone
				// on next access.
				warm := observability.NewPersistentQueryRunner(wsRoot)
				defer warm.Close()
				nbRunner := observability.NewNotebookSessionRunner(warm)
				defer nbRunner.Close()
				if err := svc.WithNotebookRunner(nbRunner).StopNotebookSession(cmd.Context(), name); err != nil {
					return err
				}
				fmt.Printf("Stopped notebook session %s (if running)\n", name)
				return nil
			},
		},
	)
	return cmd
}

func newNotebookClearOutputsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clear-outputs <name>",
		Short: "Clear all cell outputs (matches `jupyter nbconvert --clear-output`)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			nb, err := svc.ClearOutputs(args[0])
			if err != nil {
				return err
			}
			fmt.Printf("Cleared outputs from %s (%d cells)\n", nb.Name, len(nb.Cells))
			return nil
		},
	}
}

// detectLanguageDisplay mirrors service.detectCellLanguage but returns a
// user-readable label ("sql" / "python") for the show command. Keeping it
// separate keeps the CLI free of internal/service helpers — the service
// version is private.
func detectLanguageDisplay(source string) string {
	trimmed := strings.TrimLeft(source, "\n\r ")
	if strings.HasPrefix(trimmed, "%%sql") {
		return "sql"
	}
	return "python"
}

// defaultWarehouseDir is the path the warm worker uses for the workspace's
// Iceberg warehouse — wraps workspace.LocalWarehouseDir for the CLI's
// `notebook run` so it can prime the warmup at the right path.
func defaultWarehouseDir(wsRoot string) string {
	return workspace.LocalWarehouseDir(wsRoot)
}
