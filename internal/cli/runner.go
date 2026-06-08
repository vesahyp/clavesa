package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// applyReminder is printed after any mutating runner-requirements command so
// users know the change is staged, not live: it bakes into the runner image
// at the next build.
const applyReminder = "Applies on the next runner build (clavesa pipeline run locally, or clavesa workspace deploy for cloud)."

// newRunnerCmd is the `clavesa runner` noun group: workspace-level runner
// extensibility. Today it holds `requirements` (extra Python deps for UDFs);
// later it extends to jars/build.
func newRunnerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "runner",
		Short: "Manage the PySpark runner image (Python deps, etc.)",
		Long: `Manage the workspace-level PySpark runner image.

The runner is the container that executes transforms (local, Lambda,
Fargate, EMR Serverless). "requirements" declares extra Python packages
(e.g. for PySpark UDFs) that get pip-installed into the image at build
time.

Examples:
  clavesa runner requirements list
  clavesa runner requirements add "pyasn>=1.6"
  clavesa runner requirements remove pyasn
  clavesa runner requirements import requirements.txt
  clavesa runner requirements show`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(newRunnerRequirementsCmd())
	return cmd
}

// newRunnerRequirementsCmd is the `clavesa runner requirements` subcommand
// group: a thin CLI over the workspace runner-requirements file.
func newRunnerRequirementsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "requirements",
		Short: "Manage extra Python packages baked into the runner image",
		Long: `Manage the extra Python packages installed into the runner image.

These are pip-installed at runner build time, on top of the baseline
PySpark + Delta + AWS stack. Use them for transform UDF dependencies.`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(
		newRunnerRequirementsListCmd(),
		newRunnerRequirementsAddCmd(),
		newRunnerRequirementsRemoveCmd(),
		newRunnerRequirementsImportCmd(),
		newRunnerRequirementsShowCmd(),
	)
	return cmd
}

func newRunnerRequirementsListCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the extra runner requirements",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			lines, err := svc.ListRunnerRequirements()
			if err != nil {
				return fmt.Errorf("list runner requirements: %w", err)
			}
			if jsonOut {
				if lines == nil {
					lines = []string{}
				}
				return printJSON(os.Stdout, lines)
			}
			if len(lines) == 0 {
				fmt.Println("No extra requirements.")
				return nil
			}
			rows := make([][]string, len(lines))
			for i, l := range lines {
				rows[i] = []string{l}
			}
			printTable(os.Stdout, []string{"REQUIREMENT"}, rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newRunnerRequirementsAddCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add <spec>",
		Short: "Add a runner requirement (pip spec)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec := args[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			added, err := svc.AddRunnerRequirement(spec)
			if err != nil {
				return fmt.Errorf("add runner requirement: %w", err)
			}
			if !added {
				fmt.Printf("%s already present (package already required)\n", spec)
				return nil
			}
			fmt.Printf("Added %s\n", spec)
			fmt.Println(applyReminder)
			return nil
		},
	}
	return cmd
}

func newRunnerRequirementsRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove <spec>",
		Short: "Remove a runner requirement",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec := args[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			removed, err := svc.RemoveRunnerRequirement(spec)
			if err != nil {
				return fmt.Errorf("remove runner requirement: %w", err)
			}
			if !removed {
				fmt.Printf("no requirement matching %s\n", spec)
				return nil
			}
			fmt.Printf("Removed %s\n", spec)
			fmt.Println(applyReminder)
			return nil
		},
	}
	return cmd
}

func newRunnerRequirementsImportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import <file>",
		Short: "Replace all runner requirements with the contents of a file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			content, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("read %s: %w", path, err)
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.SetRunnerRequirements(string(content)); err != nil {
				return fmt.Errorf("import runner requirements: %w", err)
			}
			lines, err := svc.ListRunnerRequirements()
			if err != nil {
				return fmt.Errorf("list runner requirements: %w", err)
			}
			fmt.Printf("Imported: %d requirement(s) now set\n", len(lines))
			fmt.Println(applyReminder)
			return nil
		},
	}
	return cmd
}

func newRunnerRequirementsShowCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show",
		Short: "Show the raw requirements file (exactly what gets installed)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			content, err := svc.RunnerRequirements()
			if err != nil {
				return fmt.Errorf("read runner requirements: %w", err)
			}
			if content == "" {
				fmt.Println("(no requirements file — use `clavesa runner requirements add <spec>`)")
				return nil
			}
			fmt.Print(content)
			return nil
		},
	}
	return cmd
}
