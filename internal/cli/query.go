package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// newQueryCmd implements `clavesa query` — workspace-level ad-hoc SQL
// against the local Hadoop catalog, the CLI peer of the UI's /query page
// per ADR-015. SQL-only by design (PySpark scratchpad lives in
// `clavesa notebook` per TODO bucket 10's v1 scope).
func newQueryCmd() *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "query [SQL]",
		Short: "Run an ad-hoc SparkSQL query against the workspace catalog",
		Long: `Run a free-form SparkSQL query through the warm Spark Connect
container. The query runs against the workspace's local Hive metastore
catalog where Delta tables resolve as <workspace>__<schema>.<table>
(ADR-018; the v1.x `+"`clavesa.`"+` Iceberg-catalog prefix is gone).

Reads SQL from the first positional arg, or from STDIN when none given.

Examples:
  clavesa query "SHOW DATABASES"
  clavesa query "SELECT * FROM clavesa_demo__demo.trips__default LIMIT 5"
  echo "SELECT count(*) FROM clavesa_demo__demo.trips__default" | clavesa query --json`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sql := ""
			if len(args) == 1 {
				sql = args[0]
			} else {
				bytes, err := io.ReadAll(os.Stdin)
				if err != nil {
					return fmt.Errorf("read stdin: %w", err)
				}
				sql = string(bytes)
			}
			sql = strings.TrimSpace(sql)
			if sql == "" {
				return fmt.Errorf("no SQL provided (pass as argument or pipe via stdin)")
			}

			_, wsRoot, err := newService(cmd)
			if err != nil {
				return err
			}
			warm := observability.NewPersistentQueryRunner(wsRoot)
			defer warm.Close()
			warmupCtx := cmd.Context()
			// Eagerly trigger spawn so the first query doesn't see the
			// 503-shaped "warm worker not ready" error from the catalog
			// path; the warmup is synchronous since the user is waiting.
			wh := workspace.LocalWarehouseDir(wsRoot)
			warm.Warmup(warmupCtx, wh)
			res, err := warm.Run(cmd.Context(), wh, sql)
			if err != nil {
				return err
			}

			if asJSON {
				return json.NewEncoder(os.Stdout).Encode(res)
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, strings.Join(res.Columns, "\t"))
			for _, row := range res.Rows {
				cells := make([]string, len(row))
				for i, c := range row {
					if c == nil {
						cells[i] = ""
					} else {
						cells[i] = fmt.Sprintf("%v", c)
					}
				}
				fmt.Fprintln(tw, strings.Join(cells, "\t"))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit columns + rows JSON")
	return cmd
}
