package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/vesahyp/clavesa/internal/runtiming"
)

// printRunTiming prints the end-of-run phase summary: one `Time` line with
// the run-level phases, then a per-node table from the runner's timing.
// Cells the runner did not report (old runner image) print as "-".
func printRunTiming(w io.Writer, rec *runtiming.Record) {
	p := rec.Phases
	var spark int64
	sparkKnown := false
	for _, n := range rec.Nodes {
		if t := n.ParsedTiming(); t != nil {
			if v, ok := t.Phases["spark_session"]; ok {
				spark += v
				sparkKnown = true
			}
		}
	}
	parts := []string{"Time " + fmtMs(rec.TotalMs)}
	add := func(label string, ms int64) { parts = append(parts, label+" "+fmtMs(ms)) }
	add("startup", p.Startup)
	add("prepare", p.Prepare)
	add("metastore", p.Metastore)
	add("image", p.Image)
	if p.Inputs >= 100 {
		add("inputs", p.Inputs)
	}
	add("container", p.Container)
	add("boot", p.Boot)
	if sparkKnown {
		add("spark", spark)
	} else {
		parts = append(parts, "spark -")
	}
	add("nodes", p.Nodes)
	add("record", p.Record)
	fmt.Fprintln(w, strings.Join(parts, "  "))

	if len(rec.Nodes) == 0 {
		return
	}
	rows := make([][]string, 0, len(rec.Nodes))
	for _, n := range rec.Nodes {
		row := []string{n.Node, "-", "-", "-", "-", "-", "-", "-", "-", "-"}
		if n.DurationMs > 0 {
			row[1] = fmtMs(n.DurationMs)
		}
		if t := n.ParsedTiming(); t != nil {
			for i, phase := range []string{"inputs", "transform", "write", "stats"} {
				if v, ok := t.Phases[phase]; ok && v > 0 {
					row[2+i] = fmtMs(v)
				}
			}
			if sp, ok := t.Spark["write"]; ok {
				row[6] = fmt.Sprintf("%d", sp.NumStages)
				row[7] = fmt.Sprintf("%d", sp.NumTasks)
				row[8] = fmtBytes(sp.InputBytes)
				row[9] = fmtBytes(sp.ShuffleReadBytes + sp.ShuffleWriteBytes)
			}
		}
		rows = append(rows, row)
	}
	printTable(w, []string{"NODE", "TOTAL", "INPUTS", "TRANSFORM", "WRITE", "STATS", "STAGES", "TASKS", "INPUT", "SHUFFLE"}, rows)
}

// fmtMs renders a duration: "0.3s" under a minute, "3m 25s" under an hour,
// "1h 02m" above.
func fmtMs(ms int64) string {
	if ms < 60_000 {
		return fmt.Sprintf("%.1fs", float64(ms)/1000)
	}
	secs := ms / 1000
	if secs < 3600 {
		return fmt.Sprintf("%dm %02ds", secs/60, secs%60)
	}
	return fmt.Sprintf("%dh %02dm", secs/3600, (secs%3600)/60)
}

// fmtBytes renders a byte count in binary units ("1.2 GB", "40 MB").
func fmtBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	v := float64(b) / unit
	i := 0
	for v >= unit && i < len(units)-1 {
		v /= unit
		i++
	}
	if v < 10 {
		return fmt.Sprintf("%.1f %s", v, units[i])
	}
	return fmt.Sprintf("%.0f %s", v, units[i])
}
