package cli

import (
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/observability"
)

// newPipelineStatusCmd implements `clavesa pipeline status [dir]` — the CLI
// half of the live in-flight progress surface (ADR-015 parity with the
// run-detail DAG's per-node progress bar). It reads the latest run's
// execution states and prints per-node status, with task progress for any
// node still RUNNING. Works against local and cloud pipelines (ADR-014) —
// the provider is picked by workspace mode.
func newPipelineStatusCmd() *cobra.Command {
	var jsonOut bool
	var run string
	cmd := &cobra.Command{
		Use:   "status [pipeline-dir]",
		Short: "Show per-node status for the latest (or a given) run",
		Long: `Print the per-node execution state for the latest run of a pipeline.

Nodes still RUNNING show live Spark task progress ("124/300 tasks") as the
runner reports it; finished nodes show their terminal status. Pass --run to
inspect a specific run id instead of the latest.

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
			res, err := svc.ExecutionStates(cmd.Context(), dir, run)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, res)
			}
			if len(res.States) == 0 {
				fmt.Println("No runs yet — run the pipeline first.")
				return nil
			}
			nodes := make([]string, 0, len(res.States))
			for n := range res.States {
				nodes = append(nodes, n)
			}
			sort.Strings(nodes)
			table := make([][]string, len(nodes))
			for i, n := range nodes {
				st := res.States[n]
				table[i] = []string{n, st.Status, progressCell(st)}
			}
			printTable(os.Stdout, []string{"NODE", "STATUS", "PROGRESS"}, table)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().StringVar(&run, "run", "", "run id to inspect (default: latest)")
	return cmd
}

// progressCell renders a node's in-flight task progress, e.g. "124/300 tasks"
// (with a "· N failed" suffix when failures are present). Only RUNNING nodes
// with a reported task total carry progress; everything else is an em-dash.
func progressCell(st observability.StateStatus) string {
	if st.Status != "RUNNING" || st.TasksTotal == nil || *st.TasksTotal <= 0 {
		return "—"
	}
	completed := int64(0)
	if st.TasksCompleted != nil {
		completed = *st.TasksCompleted
	}
	cell := fmt.Sprintf("%d/%d tasks", completed, *st.TasksTotal)
	if st.TasksFailed != nil && *st.TasksFailed > 0 {
		cell += fmt.Sprintf(" · %d failed", *st.TasksFailed)
	}
	return cell
}
