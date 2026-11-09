package cli

import (
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/runtiming"
	"github.com/vesahyp/clavesa/internal/service"
)

// newPipelineTimingsCmd implements `clavesa pipeline timings [dir]`: the
// run-timing history of one pipeline, or with --all of every pipeline on
// this machine. The service read (Service.RunTimings / AllRunTimings) is
// the same one GET /api/data/run-timings serves (ADR-015).
func newPipelineTimingsCmd() *cobra.Command {
	var (
		jsonOut  bool
		all      bool
		runs     int
		sinceArg string
	)
	cmd := &cobra.Command{
		Use:   "timings [pipeline-dir]",
		Short: "Show where the time of recent runs went, phase by phase",
		Long: `Show the timing history of recent runs, one row per run, oldest first.

Every local ` + "`pipeline run`" + ` writes a timing record: wall-clock time
per run-level phase (measured by the CLI) plus the runner's per-node phases.
The record lands in two places:

  <workspace>/.clavesa/warehouse/_progress/<run-id>/_timing.json
  <history dir>/<started>_<workspace>_<pipeline>_<run8>.json

The history dir collects the records of every workspace on this machine:
$CLAVESA_RUN_HISTORY_DIR, else $XDG_STATE_HOME/clavesa/runs, else
~/.local/state/clavesa/runs. --all reads it, so it needs no workspace and
ignores [pipeline-dir].

On a cloud warehouse there are no CLI-side phases; each run is assembled
from the node_runs table, so only the node columns carry time.

Columns (times are wall clock):
  TOTAL      start of the command to the end of the run
  STARTUP    startup + prepare + lock + image + container + boot
  METASTORE  bringing up or reusing the shared metastore
  SPARK      Spark session and catalog setup, summed over nodes
  READS      resolving sources plus every node's input reads
  TRANSFORM  the transform itself, summed over nodes
  WRITES     table writes, summed over nodes
  STATS      column statistics, summed over nodes
  OTHER      TOTAL minus the columns above
A "-" means the runner image predates per-node timing; OTHER then holds
all node time.

--json prints {"generated_at","history_dir","runs":[...]} with every record
in full: run_id, workspace, workspace_dir, pipeline, status, trigger,
compute, started_at, ended_at, total_ms, phases (startup, prepare, lock,
metastore, image, inputs, container, boot, bundle, nodes, record) and
nodes[] (node, status, started_ms, ended_ms, duration_ms, output_rows,
timing). history_dir is "" for a single pipeline.

` + pipelineDirHelp,
		Example: `  clavesa pipeline timings my-pipeline
  clavesa pipeline timings my-pipeline --runs 50 --since 7d
  clavesa pipeline timings --all --since 90d
  clavesa pipeline timings my-pipeline --json`,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			since, err := runtiming.ParseSince(sinceArg)
			if err != nil {
				return err
			}
			q := service.RunTimingsQuery{Limit: runs, Since: since}
			out := cmd.OutOrStdout()
			if all {
				h, err := service.AllRunTimings(q)
				if err != nil {
					return err
				}
				if jsonOut {
					return printJSON(out, h)
				}
				printTimingsTable(out, h.Runs, true)
				return nil
			}
			dir, _, _, err := resolvePipelineDir(cmd, args, 0)
			if err != nil {
				return err
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			recs, err := svc.RunTimings(cmd.Context(), dir, q)
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(out, runtiming.History{
					GeneratedAt: time.Now().Format(time.RFC3339),
					Runs:        recs,
				})
			}
			printTimingsTable(out, recs, false)
			return nil
		},
	}
	cmd.Flags().IntVar(&runs, "runs", 20, "number of most recent runs to show (0 = all)")
	cmd.Flags().StringVar(&sinceArg, "since", "", "only runs that started within this window: a Go duration (24h) or days (7d)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	cmd.Flags().BoolVar(&all, "all", false, "every pipeline in every workspace on this machine, from the history dir")
	return cmd
}

// printTimingsTable prints one row per run, oldest first. withSource adds
// the WORKSPACE and PIPELINE columns (--all).
func printTimingsTable(w io.Writer, recs []runtiming.Record, withSource bool) {
	if len(recs) == 0 {
		_, _ = io.WriteString(w, "No timing records yet: run a pipeline first.\n")
		return
	}
	headers := []string{"STARTED"}
	if withSource {
		headers = append(headers, "WORKSPACE", "PIPELINE")
	}
	headers = append(headers, "STATUS", "TOTAL", "STARTUP", "METASTORE", "SPARK", "READS", "TRANSFORM", "WRITES", "STATS", "OTHER")
	rows := make([][]string, 0, len(recs))
	for _, rec := range recs {
		s := runtiming.SeriesOf(rec)
		row := []string{fmtStarted(rec.StartedAt)}
		if withSource {
			row = append(row, rec.Workspace, rec.Pipeline)
		}
		node := func(ms int64) string {
			if !s.NodeTiming {
				return "-"
			}
			return fmtMs(ms)
		}
		row = append(row, rec.Status, fmtMs(s.TotalMs), fmtMs(s.StartupMs), fmtMs(s.MetastoreMs),
			node(s.SparkMs), node(s.ReadsMs), node(s.TransformMs), node(s.WritesMs), node(s.StatsMs),
			fmtMs(s.OtherMs))
		rows = append(rows, row)
	}
	printTable(w, headers, rows)
}

// fmtStarted renders an RFC 3339 start time as local "2006-01-02 15:04:05";
// the raw string when it does not parse.
func fmtStarted(s string) string {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		if s == "" {
			return "-"
		}
		return s
	}
	return t.Local().Format("2006-01-02 15:04:05")
}
