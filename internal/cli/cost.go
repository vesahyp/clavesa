package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// newPipelineCostCmd implements `clavesa pipeline cost --dir <dir> [--json]`
// — the CLI surface for clavesa's north-star metric, cost per billion
// records processed. It reads the pipeline's recent runner invocations
// (node_runs) and prices the billed compute, dispatching local/cloud by
// workspace mode (ADR-014). Report-only: it never mutates the pipeline.
func newPipelineCostCmd() *cobra.Command {
	var jsonOut bool
	var dirFlag string
	var lastN int
	cmd := &cobra.Command{
		Use:   "cost [pipeline-dir]",
		Short: "Report cost per billion records — clavesa's north-star metric",
		Long: `Report clavesa's north-star metric: cost per billion records processed.

Reads the pipeline's recent runner invocations, sums the records processed
and the billed compute, and prints the blended cost-per-billion alongside
sustained throughput. All-local pipelines have zero compute cost; the
throughput half of the metric is still reported.

Report-only: this prints the metric; it does not re-deploy or edit the
pipeline.

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// --dir is an alias for the positional; it stands in when no
			// positional was given so `--dir <pipelineDir>` resolves the
			// same way as `cost <pipelineDir>`.
			if dirFlag != "" && len(args) == 0 {
				args = []string{dirFlag}
			}
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			pipeline := filepath.Base(dir)
			cost, err := svc.PipelineCostForDir(cmd.Context(), pipeline, lastN)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, cost)
			}
			if cost.TotalCostUSD == 0 {
				fmt.Printf("Cost per billion records: $0.00 compute (local) (%s records/sec)\n",
					formatRate(cost.RecordsPerSec))
			} else {
				fmt.Printf("Cost per billion records: %s (%s records/sec)\n",
					formatUSD(cost.CostPerBillion), formatRate(cost.RecordsPerSec))
			}
			if cost.PriceBasis != "" {
				fmt.Printf("Price basis: %s\n", cost.PriceBasis)
			}
			if len(cost.PerNode) == 0 {
				fmt.Println("No runs with cost metrics yet — run the pipeline a few times first.")
				return nil
			}
			table := make([][]string, len(cost.PerNode))
			for i, n := range cost.PerNode {
				table[i] = []string{
					n.Node,
					n.ComputeTarget,
					formatCount(n.Records),
					formatUSD(n.CostUSD),
					formatUSD(n.CostPerBillion),
					formatRate(n.RecordsPerSec),
				}
			}
			printTable(os.Stdout, []string{"NODE", "TARGET", "RECORDS", "COST", "$/BILLION", "REC/S"}, table)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().StringVar(&dirFlag, "dir", "", "pipeline directory (alternative to the positional argument)")
	cmd.Flags().IntVar(&lastN, "last", 50, "number of recent runs to consider")
	return cmd
}

// formatUSD renders a dollar amount with readable precision: small values
// (sub-dollar) get more decimal places so a fraction of a cent is visible,
// large values round to cents.
func formatUSD(v float64) string {
	switch {
	case v == 0:
		return "$0.00"
	case v < 0.01:
		return fmt.Sprintf("$%.6f", v)
	case v < 1:
		return fmt.Sprintf("$%.4f", v)
	default:
		return fmt.Sprintf("$%.2f", v)
	}
}

// formatRate renders a records/sec throughput with thousands separators.
func formatRate(v float64) string {
	return formatCount(int64(v + 0.5))
}

// formatCount renders an integer with thousands separators.
func formatCount(n int64) string {
	s := fmt.Sprintf("%d", n)
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
