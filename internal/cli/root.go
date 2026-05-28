// Package cli implements the command-line interface for Clavesa pipeline
// operations using Cobra.
package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

// newRootCmd builds a fresh command tree. Using a factory (not a package-level
// var) so parallel tests each get their own tree.
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "clavesa",
		Short: "Visual ETL for Terraform pipelines",
		Long: `Clavesa — visual ETL for Terraform pipelines

Pipelines are Terraform. The visual UI reads and writes .tf files directly.

Quick start:
  clavesa workspace init my-project        # scaffold a workspace
  clavesa pipeline create my-pipeline      # add a pipeline
  clavesa source register trips --from https://example.com/data.parquet
  clavesa node add my-pipeline --type transform
  clavesa source attach my-pipeline trips --to transform1 --as trips
  clavesa node edit my-pipeline transform1 --set sql="SELECT * FROM trips"
  clavesa node preview my-pipeline transform1
  clavesa ui                               # open the visual editor`,
		Version:           service.ModuleVersion,
		SilenceUsage:      true,
		SilenceErrors:     true,
	}
	root.SetVersionTemplate("{{.Version}}\n")

	root.PersistentFlags().String("workspace", "", "workspace root directory (default: current directory)")

	root.CompletionOptions.DisableDefaultCmd = true

	root.AddCommand(
		newUICmd(),
		newWorkspaceCmd(),
		newPipelineCmd(),
		newNodeCmd(),
		newSourceCmd(),
		newCredentialCmd(),
		newDashboardsCmd(),
		newNotebookCmd(),
		newQueryCmd(),
		newVersionCmd(),
	)

	return root
}

// requireSubcommand returns a RunE that prints help and returns an error,
// used for parent commands that should not be invoked without a subcommand.
func requireSubcommand() func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		_ = cmd.Help()
		return fmt.Errorf("missing subcommand")
	}
}

// Execute runs the CLI with os.Args.
func Execute() error {
	cmd := newRootCmd()
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "clavesa: %v\n", err)
		os.Exit(1)
	}
	return nil
}

// Run dispatches CLI commands from the given args slice. Kept for backward
// compatibility with unit tests that call Run([]string{...}).
func Run(args []string) error {
	cmd := newRootCmd()
	cmd.SetArgs(args)
	return cmd.Execute()
}
