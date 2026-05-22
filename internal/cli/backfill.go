package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/vesahyp/clavesa/internal/service"
)

// newPipelineBackfillCmd is the `clavesa pipeline backfill ...` group.
// Default shape is stage → review → promote — the runner writes to a
// parallel staging table so the user can diff before merging into the
// canonical target.
func newPipelineBackfillCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "Replay a transform over a historical partition window",
		Long: `Backfill a transform's output over a historical partition window.

Default shape — stage → review → promote — gives you a parallel
Iceberg staging table to inspect before anything lands in the canonical
target:

  clavesa pipeline backfill stage <dir> --node <n> --from <c> --to <c>
  clavesa pipeline backfill diff <dir> <run_id>
  clavesa pipeline backfill promote <dir> <run_id>     # or discard

Every subcommand takes the pipeline directory as the first argument;
omit it to use the current directory once you have cd'd into the
pipeline.

The --direct flag on stage skips staging and writes to the canonical
target — for cases where you know the output is keyed (merge mode) and
want to skip the round-trip.`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(
		newBackfillStageCmd(),
		newBackfillListCmd(),
		newBackfillDiffCmd(),
		newBackfillPromoteCmd(),
		newBackfillDiscardCmd(),
	)
	return cmd
}

func newBackfillStageCmd() *cobra.Command {
	var nodeID, fromStr, toStr string
	var direct, jsonOut bool
	cmd := &cobra.Command{
		Use:   "stage [pipeline-dir]",
		Short: "Stage a backfill into a parallel Iceberg table",
		Long:  "Stage a backfill into a parallel Iceberg table.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, ws, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			req := service.BackfillStageRequest{
				Dir:    dir,
				Node:   nodeID,
				From:   splitCursorArg(fromStr),
				To:     splitCursorArg(toStr),
				Direct: direct,
			}
			run, err := svc.BackfillStage(cmd.Context(), req)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, run)
			}
			fmt.Printf("Backfill staged.\n")
			fmt.Printf("  run_id:         %s\n", run.RunID)
			fmt.Printf("  staging table:  %s\n", run.TargetTable)
			fmt.Printf("  canonical:      %s\n", run.CanonicalTable)
			fmt.Printf("  window:         [%s, %s]\n", joinCursorArg(run.From), joinCursorArg(run.To))
			if run.Direct {
				fmt.Printf("  --direct:       wrote straight to canonical target (no staging)\n")
				return nil
			}
			fmt.Printf("\nNext steps:\n")
			rel := displayDir(ws, dir)
			fmt.Printf("  clavesa pipeline backfill diff %s %s\n", rel, run.RunID)
			fmt.Printf("  clavesa pipeline backfill promote %s %s\n", rel, run.RunID)
			fmt.Printf("  clavesa pipeline backfill discard %s %s\n", rel, run.RunID)
			return nil
		},
	}
	cmd.Flags().StringVar(&nodeID, "node", "", "transform node to backfill (required)")
	cmd.Flags().StringVar(&fromStr, "from", "", "partition cursor lower bound, slash-separated (e.g. 2026/04/26/00) (required)")
	cmd.Flags().StringVar(&toStr, "to", "", "partition cursor upper bound, slash-separated (e.g. 2026/04/27/00) (required)")
	cmd.Flags().BoolVar(&direct, "direct", false, "skip staging — write straight to the canonical target (escape hatch; non-merge outputs need this carefully)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit machine-readable output")
	_ = cmd.MarkFlagRequired("node")
	_ = cmd.MarkFlagRequired("from")
	_ = cmd.MarkFlagRequired("to")
	return cmd
}

func newBackfillListCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "list [pipeline-dir]",
		Short: "List open (un-promoted/un-discarded) backfill staging tables",
		Long:  "List open (un-promoted/un-discarded) backfill staging tables.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			runs, err := svc.BackfillList(cmd.Context(), dir)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, runs)
			}
			if len(runs) == 0 {
				fmt.Println("(no open backfills)")
				return nil
			}
			rows := make([][]string, len(runs))
			for i, r := range runs {
				rows[i] = []string{r.RunID, r.Node, joinCursorArg(r.From) + " → " + joinCursorArg(r.To), r.TargetTable}
			}
			printTable(os.Stdout, []string{"RUN_ID", "NODE", "WINDOW", "STAGING"}, rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit machine-readable output")
	return cmd
}

func newBackfillDiffCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "diff [pipeline-dir] <run_id>",
		Short: "Compare a staging table against its canonical target",
		Long:  "Compare a staging table against its canonical target.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, _, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			d, err := svc.BackfillDiff(cmd.Context(), dir, rest[0])
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, d)
			}
			fmt.Printf("Backfill diff %s\n", d.RunID)
			fmt.Printf("  staging:        %s (%d rows)\n", d.StagingTable, d.StagingRows)
			if d.CanonicalRows < 0 {
				fmt.Printf("  canonical:      %s (does not exist — first backfill creates target)\n", d.CanonicalTable)
			} else {
				fmt.Printf("  canonical:      %s (%d rows)\n", d.CanonicalTable, d.CanonicalRows)
			}
			fmt.Printf("  schema match:   %v\n", d.SchemaMatches)
			if d.SchemaDiff != "" {
				fmt.Printf("  schema diff:\n%s\n", indent(d.SchemaDiff, "    "))
			}
			fmt.Printf("  output mode:    %s\n", d.OutputMode)
			if len(d.MergeKeys) > 0 {
				fmt.Printf("  merge keys:     %s\n", strings.Join(d.MergeKeys, ", "))
				fmt.Printf("  matching keys:  %d (would UPDATE on promote)\n", d.MatchingKeyRows)
				fmt.Printf("  new keys:       %d (would INSERT on promote)\n", d.NewKeyRows)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "emit machine-readable output")
	return cmd
}

func newBackfillPromoteCmd() *cobra.Command {
	var forceDedup string
	var allowDuplicates bool
	cmd := &cobra.Command{
		Use:   "promote [pipeline-dir] <run_id>",
		Short: "Merge a staging table into its canonical target, then drop staging",
		Long:  "Merge a staging table into its canonical target, then drop staging.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, _, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			runID := rest[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			err = svc.BackfillPromote(cmd.Context(), dir, runID, service.BackfillPromoteOpts{
				ForceDedup:      forceDedup,
				AllowDuplicates: allowDuplicates,
			})
			if err != nil {
				return err
			}
			fmt.Printf("Promoted %s into canonical target. Staging table dropped.\n", runID)
			return nil
		},
	}
	cmd.Flags().StringVar(&forceDedup, "force-dedup", "", "(append outputs) column to MERGE on so duplicates are dropped; column must uniquely identify a row")
	cmd.Flags().BoolVar(&allowDuplicates, "allow-duplicates", false, "(append outputs) accept duplicates — plain INSERT INTO without dedup")
	return cmd
}

func newBackfillDiscardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "discard [pipeline-dir] <run_id>",
		Short: "Drop a staging table without promoting",
		Long:  "Drop a staging table without promoting.\n\n" + pipelineDirHelp,
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, rest, _, err := resolvePipelineDir(cmd, args, 1)
			if err != nil {
				return err
			}
			runID := rest[0]
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.BackfillDiscard(cmd.Context(), dir, runID); err != nil {
				return err
			}
			fmt.Printf("Discarded %s.\n", runID)
			return nil
		},
	}
	return cmd
}

func splitCursorArg(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

func joinCursorArg(parts []string) string { return strings.Join(parts, "/") }

func indent(s, prefix string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString(prefix)
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}
