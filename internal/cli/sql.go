package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

// newSQLCmd implements `clavesa sql` — parser tooling for SparkSQL.
// Slice 3 ships one subcommand (`lint`); future slices may add format,
// explain, etc.
func newSQLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql",
		Short: "SparkSQL tooling (parse-check, lint)",
		Long: `SparkSQL tooling. Subcommands work against the workspace's
warm Spark worker (the same JVM that powers the Catalog UI's
ad-hoc query runner), so a parse-check is a single in-JVM call
without paying the Spark cold-start cost.`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(newSQLLintCmd())
	return cmd
}

func newSQLLintCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lint <file>",
		Short: "Parse-check a SparkSQL file; exits non-zero on parse failure",
		Long: `Parse-check a SparkSQL file. Exits 0 with no output on success,
non-zero with the parser's pointer-into-SQL hint on stderr on
failure. Useful in pre-commit hooks and CI to catch SQL typos
before they land in a transform's .tf and cost a Spark cold start
to surface.

Examples:
  clavesa sql lint transforms/enrich.sql
  find . -name '*.sql' -exec clavesa sql lint {} \;`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read %s: %w", args[0], err)
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.ValidateSQL(cmd.Context(), string(data)); err != nil {
				var pe *service.ParseError
				if errors.As(err, &pe) {
					fmt.Fprintf(os.Stderr, "%s: SQL parse failed\n  %s\n", args[0], pe.Message)
					os.Exit(1)
				}
				return fmt.Errorf("SQL validation: %w", err)
			}
			return nil
		},
	}
	return cmd
}
