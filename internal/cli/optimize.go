package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

// newPipelineOptimizeCmd implements `clavesa pipeline optimize [dir]` — the
// CLI half of the Delta maintenance surface (ADR-015 parity with the UI
// table-maintenance action). It compacts a pipeline's canonical Delta output
// tables via the runner's control-plane operations; --recluster migrates
// pre-clustering tables to liquid clustering, --vacuum prunes tombstoned
// files. Works against local and cloud pipelines (ADR-014) — the provider is
// picked by workspace mode inside the service.
func newPipelineOptimizeCmd() *cobra.Command {
	var (
		jsonOut     bool
		node        string
		recluster   bool
		vacuum      bool
		retainHours int
	)
	cmd := &cobra.Command{
		Use:   "optimize [pipeline-dir]",
		Short: "Compact, re-cluster, and vacuum a pipeline's Delta output tables",
		Long: `Run Delta table maintenance over a pipeline's output tables by
invoking the runner's control-plane operations.

By default every transform output table is OPTIMIZEd (compacted). With
--recluster, tables that declare cluster_by (or merge_keys on a merge-mode
output) get ALTER TABLE CLUSTER BY (keys) before the OPTIMIZE — the migration
path for tables created before liquid clustering. With --vacuum, each table is
also VACUUMed past the retention window.

A single-table failure is reported in the results; the sweep continues to the
remaining tables. Exit is non-zero if any table errored.

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newDashboardService(cmd)
			if err != nil {
				return err
			}
			results, err := svc.OptimizeTable(cmd.Context(), service.OptimizeRequest{
				Dir:         dir,
				Node:        node,
				Recluster:   recluster,
				Vacuum:      vacuum,
				RetainHours: retainHours,
			})
			if err != nil {
				return err
			}
			if jsonOut {
				if err := printJSON(os.Stdout, results); err != nil {
					return err
				}
			} else {
				if len(results) == 0 {
					fmt.Println("No transform output tables to optimize.")
					return nil
				}
				table := make([][]string, len(results))
				for i, r := range results {
					op := r.Operation
					if r.Vacuumed {
						op += " + vacuum"
					}
					detail := r.Status
					if r.Error != "" {
						detail = r.Error
					}
					table[i] = []string{r.Node, r.Table, op, r.Status, detail}
				}
				printTable(os.Stdout, []string{"NODE", "TABLE", "OPERATION", "STATUS", "DETAIL"}, table)
			}
			for _, r := range results {
				if r.Status != "ok" {
					return fmt.Errorf("one or more tables failed to optimize")
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().StringVar(&node, "node", "", "optimize only this node's output table(s) (default: all)")
	cmd.Flags().BoolVar(&recluster, "recluster", false, "re-cluster (ALTER TABLE CLUSTER BY merge/cluster keys + OPTIMIZE); migrates pre-clustering tables")
	cmd.Flags().BoolVar(&vacuum, "vacuum", false, "also VACUUM after optimize")
	cmd.Flags().IntVar(&retainHours, "retain-hours", 168, "VACUUM retention window in hours")
	return cmd
}
