package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// newPipelineRightsizeCmd implements `clavesa pipeline rightsize [dir]` — the
// CLI half of the rightsizing surface (ADR-015 parity with the run-detail
// node drawer). Recommend-only: it reads the pipeline's recent runner
// invocations and prints a per-node memory recommendation; it never mutates
// the pipeline. Works against local and cloud pipelines (ADR-014) — the
// provider is picked by workspace mode.
func newPipelineRightsizeCmd() *cobra.Command {
	var jsonOut bool
	var lastN int
	cmd := &cobra.Command{
		Use:   "rightsize [pipeline-dir]",
		Short: "Recommend per-node Lambda memory from recent run history",
		Long: `Recommend a Lambda memory allocation per node, computed from the
p95 of the node's recent peak RSS and how often it spilled.

Recommend-only: this prints advice; it does not re-deploy or edit the
pipeline. Nodes with no allocated memory on record (local runs) or no
Spark memory metrics yet show confidence "n/a".

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
			pipeline := filepath.Base(dir)
			rows, err := svc.Rightsize(cmd.Context(), pipeline, lastN)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, rows)
			}
			if len(rows) == 0 {
				fmt.Println("No runs with memory metrics yet — run the pipeline a few times first.")
				return nil
			}
			table := make([][]string, len(rows))
			for i, r := range rows {
				table[i] = []string{
					r.Node,
					mbCell(r.CurrentMB),
					mbCell(r.RecommendedMB),
					mbCell(r.P95PeakRSSMB),
					fmt.Sprintf("%.0f%%", r.SpillRate*100),
					r.Confidence,
					r.Reason,
				}
			}
			printTable(os.Stdout, []string{"NODE", "CURRENT", "RECOMMENDED", "P95 PEAK", "SPILL%", "CONFIDENCE", "REASON"}, table)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().IntVar(&lastN, "last", 50, "number of recent runs to consider")
	return cmd
}

// mbCell renders a nullable megabyte value as "<n> MB" or an em-dash.
func mbCell(v *int64) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("%d MB", *v)
}
