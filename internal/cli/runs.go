package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// newPipelineRunsCmd implements `clavesa pipeline runs [dir]` — the CLI half
// of run history (ADR-015 parity with the dashboard's Runs tab). It reads
// the pipeline's recent executions and prints one row per run, newest first.
// Works against local and cloud pipelines (ADR-014) — the provider is picked
// by workspace mode.
func newPipelineRunsCmd() *cobra.Command {
	var jsonOut bool
	var limit int
	var status string
	cmd := &cobra.Command{
		Use:   "runs [pipeline-dir]",
		Short: "List recent runs of a pipeline",
		Long: `List recent executions of a pipeline, newest first.

Each row shows the run id, overall status, trigger, start time, duration,
and (for a failed run) the failed step and a truncated error message. Pass
--status to filter to one status; use --json for the full untruncated
error text.

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			statusFilter := strings.ToUpper(strings.TrimSpace(status))
			switch statusFilter {
			case "", "RUNNING", "SUCCEEDED", "FAILED":
			default:
				return fmt.Errorf("invalid --status %q: must be one of RUNNING, SUCCEEDED, FAILED", status)
			}
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			res, err := svc.Runs(cmd.Context(), dir, limit)
			if err != nil {
				return err
			}
			if statusFilter != "" {
				n := 0
				for _, r := range res.Rows {
					if strings.EqualFold(r.Status, statusFilter) {
						res.Rows[n] = r
						n++
					}
				}
				res.Rows = res.Rows[:n]
			}
			if jsonOut {
				return printJSON(os.Stdout, res)
			}
			if len(res.Rows) == 0 {
				if statusFilter != "" {
					fmt.Printf("No %s runs in the last %d.\n", statusFilter, limit)
				} else {
					fmt.Println("No runs yet — run the pipeline first.")
				}
				return nil
			}
			table := make([][]string, len(res.Rows))
			for i, r := range res.Rows {
				trigger := r.Trigger
				if trigger == "" {
					trigger = "—"
				}
				failedStep := r.FailedStep
				if failedStep == "" {
					failedStep = "—"
				}
				errCell := "—"
				if r.ErrorMsg != "" {
					// 120 bytes: Spark analysis errors open with a long class
				// prefix ("[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column,
				// variable, or function parameter with name ..."), and a
				// tighter cap cuts the row before the identifier that
				// actually explains the failure.
				errCell = truncateForTable(r.ErrorMsg, 120)
				}
				table[i] = []string{
					r.RunID,
					r.Status,
					trigger,
					formatLocalTimestamp(r.StartedAt),
					humanDurationMs(r.DurationMs),
					failedStep,
					errCell,
				}
			}
			printTable(os.Stdout, []string{"RUN", "STATUS", "TRIGGER", "STARTED", "DURATION", "FAILED STEP", "ERROR"}, table)
			if res.Truncated {
				fmt.Printf("Showing %d most recent runs; pass --limit for more.\n", len(res.Rows))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().IntVar(&limit, "limit", 20, "maximum number of runs to show")
	cmd.Flags().StringVar(&status, "status", "", "filter by status: RUNNING, SUCCEEDED, or FAILED")
	return cmd
}

// newPipelineLogsCmd implements `clavesa pipeline logs [dir]` — the CLI half
// of the run-detail log panel (ADR-015 parity). It reads the captured log
// for one run (latest by default) and prints it to stdout. Works against
// local and cloud pipelines (ADR-014) — the provider is picked by workspace
// mode: cloud reads CloudWatch, local reads the run's bundle log.
func newPipelineLogsCmd() *cobra.Command {
	var jsonOut bool
	var run string
	var node string
	var tail int
	cmd := &cobra.Command{
		Use:   "logs [pipeline-dir]",
		Short: "Print the captured log for a run",
		Long: `Print the captured log for one run of a pipeline.

Defaults to the latest run. On cloud pipelines --node selects which step's
CloudWatch log stream to read; on local pipelines the log is one bundle per
run covering every node, so --node has no effect there.

` + pipelineDirHelp,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			res, err := svc.ExecutionLogs(cmd.Context(), dir, run, node, tail)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, res)
			}
			if len(res.Events) == 0 {
				fmt.Println("No captured log for this run.")
				return nil
			}
			fmt.Printf("Log: %s\n", res.LogGroup)
			for _, ev := range res.Events {
				fmt.Printf("%s  %s\n", formatLogTimestamp(ev.Timestamp), ev.Message)
			}
			if res.Truncated {
				fmt.Printf("(truncated; full log: %s)\n", res.LogGroup)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().StringVar(&run, "run", "", "run id to inspect (default: latest)")
	cmd.Flags().StringVar(&node, "node", "", "node/step name; selects the CloudWatch stream on cloud runs, ignored on local runs")
	cmd.Flags().IntVar(&tail, "tail", 2000, "maximum log lines; local runs return the last N")
	return cmd
}

// humanDurationMs renders a millisecond duration as a compact string, e.g.
// "1m32s". Returns the table placeholder for a nil duration (run still in
// flight, or the field wasn't populated).
func humanDurationMs(ms *int64) string {
	if ms == nil {
		return "—"
	}
	return (time.Duration(*ms) * time.Millisecond).Round(time.Second).String()
}

// formatLocalTimestamp parses an RFC3339 timestamp and renders it in local
// time for display. Falls back to the raw string on parse failure or empty
// input so a malformed value is still visible rather than swallowed.
func formatLocalTimestamp(s string) string {
	if s == "" {
		return "—"
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

// formatLogTimestamp is formatLocalTimestamp's compact counterpart for log
// lines: time-of-day only, since a single run's log rarely spans a day
// boundary and the wider table columns already carry the date.
func formatLogTimestamp(s string) string {
	if s == "" {
		return "—"
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	return t.Local().Format("15:04:05.000")
}

// truncateForTable flattens s to a single line (error messages carry
// embedded newlines and stack-trace indentation that would break the table
// row) and shortens it to at most max bytes, appending an ellipsis when it
// does. Byte-based, not rune-based: table alignment in printTable counts
// bytes, and error messages are effectively always ASCII.
func truncateForTable(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}
