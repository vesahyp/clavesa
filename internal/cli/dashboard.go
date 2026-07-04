package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

// newDashboardsCmd implements `clavesa dashboards` — the CLI half of
// the dashboards surface (ADR-015 parity with the UI). Dashboards are
// file-backed JSON specs under <workspace>/.clavesa/dashboards/<slug>.json
// (ADR-021); these commands read and write them through the same service
// layer the HTTP handler uses.
func newDashboardsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dashboards",
		Short: "List, inspect, render, and author workspace dashboards",
		Long: `Manage workspace dashboards — saved SQL widgets over the catalog.

Dashboards are stored as JSON specs under the workspace's
` + "`.clavesa/dashboards/`" + ` directory (one file per dashboard), shared with
everyone who has the workspace checked out.

Examples:
  clavesa dashboards list
  clavesa dashboards show pipeline-runs-demo
  clavesa dashboards render pipeline-runs-demo --json
  clavesa dashboards apply revenue.json
  clavesa dashboards delete revenue`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(
		newDashboardsListCmd(),
		newDashboardsShowCmd(),
		newDashboardsRenderCmd(),
		newDashboardsApplyCmd(),
		newDashboardsDeleteCmd(),
	)
	return cmd
}

func newDashboardsListCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List dashboards",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			list, err := svc.ListDashboards(cmd.Context())
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, list)
			}
			if len(list) == 0 {
				fmt.Println("No dashboards yet.")
				return nil
			}
			rows := make([][]string, len(list))
			for i, d := range list {
				rows[i] = []string{d.Slug, d.Title}
			}
			printTable(os.Stdout, []string{"SLUG", "TITLE"}, rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newDashboardsShowCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "show <slug>",
		Short: "Show a dashboard's datasets and widgets",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			d, err := svc.GetDashboard(cmd.Context(), args[0])
			if err != nil {
				return dashboardNotFoundHint(err, args[0])
			}
			if jsonOut {
				return printJSON(os.Stdout, d)
			}
			fmt.Printf("%s  (%s)\n", d.Title, d.Slug)
			if len(d.Datasets) > 0 {
				fmt.Println("\nDatasets:")
				rows := make([][]string, len(d.Datasets))
				for i, ds := range d.Datasets {
					rows[i] = []string{ds.Name, ds.Dir, oneLine(ds.SQL)}
				}
				printTable(os.Stdout, []string{"NAME", "DIR", "SQL"}, rows)
			}
			if len(d.Widgets) > 0 {
				fmt.Println("\nWidgets:")
				rows := make([][]string, len(d.Widgets))
				for i, w := range d.Widgets {
					rows[i] = []string{w.ID, w.Type, w.Title, w.Dataset}
				}
				printTable(os.Stdout, []string{"ID", "TYPE", "TITLE", "DATASET"}, rows)
			}
			if len(d.Controls) > 0 {
				fmt.Println("\nControls:")
				rows := make([][]string, len(d.Controls))
				for i, c := range d.Controls {
					detail := c.Default
					if c.Type == "select" && c.SQL != "" {
						detail = fmt.Sprintf("sql @ %s", c.Dir)
					} else if c.Type == "select" && len(c.Options) > 0 && detail == "" {
						detail = fmt.Sprintf("%d option(s)", len(c.Options))
					}
					rows[i] = []string{c.Name, c.Type, c.Label, detail}
				}
				printTable(os.Stdout, []string{"NAME", "TYPE", "LABEL", "DEFAULT/SOURCE"}, rows)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newDashboardsRenderCmd() *cobra.Command {
	var jsonOut bool
	var paramFlags []string
	cmd := &cobra.Command{
		Use:   "render <slug>",
		Short: "Execute every widget's dataset and print the results",
		Long: `Execute a dashboard — runs each widget's bound dataset SQL and
prints the results. Datasets shared by multiple widgets execute once.

Useful for cron / CI smoke tests: a non-zero exit means at least one
widget's query failed.

Pass dashboard control values with --param key=value (repeatable). Keys
not provided fall back to each control's declared default — a
time_range with default "last_30d" expands to {start, end} at "now".
For a time_range control named "tr", the two keys are "tr.start" and
"tr.end"; for a select control, the key is the control name.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			params, err := parseParamFlags(paramFlags)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			render, err := svc.RenderDashboard(cmd.Context(), args[0], params)
			if err != nil {
				return dashboardNotFoundHint(err, args[0])
			}
			if jsonOut {
				if err := printJSON(os.Stdout, render); err != nil {
					return err
				}
			} else {
				fmt.Printf("%s  (%s)\n", render.Title, render.Slug)
				for _, w := range render.Widgets {
					fmt.Printf("\n• %s  [%s]\n", w.Title, w.Type)
					if w.Error != "" {
						fmt.Printf("  error: %s\n", w.Error)
						continue
					}
					printTable(os.Stdout, w.Columns, w.Rows)
				}
			}
			// Non-zero exit when any widget errored — the CI smoke-test signal.
			for _, w := range render.Widgets {
				if w.Error != "" {
					return fmt.Errorf("%d widget(s) failed to render", countErrors(render))
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().StringArrayVar(&paramFlags, "param", nil, "control value as key=value (repeatable; e.g. --param tr.start=2026-01-01)")
	return cmd
}

// parseParamFlags collects --param key=value flags into a map. An empty
// value is allowed (`--param region=`); a flag without `=` is rejected
// rather than silently treated as `key=""`.
func parseParamFlags(flags []string) (map[string]string, error) {
	out := map[string]string{}
	for _, f := range flags {
		i := strings.Index(f, "=")
		if i < 0 {
			return nil, fmt.Errorf("--param %q: expected key=value", f)
		}
		key := strings.TrimSpace(f[:i])
		if key == "" {
			return nil, fmt.Errorf("--param %q: key is empty", f)
		}
		out[key] = f[i+1:]
	}
	return out, nil
}

func newDashboardsApplyCmd() *cobra.Command {
	var slug string
	cmd := &cobra.Command{
		Use:   "apply <file.json>",
		Short: "Create or replace a dashboard from a JSON spec file",
		Long: `Create or replace a dashboard from a JSON file. The file is the
datasets-shaped spec (a title, datasets, widgets); the legacy
per-widget-SQL shape is accepted and migrated automatically.

The slug defaults to the file's base name; override with --slug.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read %s: %w", args[0], err)
			}
			if slug == "" {
				slug = strings.TrimSuffix(filepath.Base(args[0]), ".json")
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			d, err := svc.ApplyDashboardFile(cmd.Context(), slug, data)
			if err != nil {
				return err
			}
			fmt.Printf("Applied dashboard %s (%d dataset(s), %d widget(s))\n",
				d.Slug, len(d.Datasets), len(d.Widgets))
			return nil
		},
	}
	cmd.Flags().StringVar(&slug, "slug", "", "dashboard slug (default: file base name)")
	return cmd
}

func newDashboardsDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <slug>",
		Short: "Delete a dashboard",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.DeleteDashboard(cmd.Context(), args[0]); err != nil {
				return dashboardNotFoundHint(err, args[0])
			}
			fmt.Printf("Deleted dashboard %s\n", args[0])
			return nil
		},
	}
	return cmd
}

// dashboardNotFoundHint turns the service's wrapped os.ErrNotExist into a
// terse "no such dashboard" message instead of leaking the wrapped text.
func dashboardNotFoundHint(err error, slug string) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("no dashboard named %q (try `clavesa dashboards list`)", slug)
	}
	return err
}

// oneLine collapses whitespace so multi-line widget SQL fits one table cell.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func countErrors(r service.DashboardRender) int {
	n := 0
	for _, w := range r.Widgets {
		if w.Error != "" {
			n++
		}
	}
	return n
}
