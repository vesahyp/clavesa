package runtiming

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// History is the read-back shape of `clavesa pipeline timings --json` and
// GET /api/data/run-timings: a list of run records, oldest first.
// HistoryDir is the cross-workspace history directory the records came
// from, or "" when they came from one pipeline's workspace.
type History struct {
	GeneratedAt string   `json:"generated_at"`
	HistoryDir  string   `json:"history_dir"`
	Runs        []Record `json:"runs"`
}

// ParseSince parses a look-back window: a Go duration ("24h", "90m") or a
// whole number of days ("7d", "90d"). Empty means no bound (0).
func ParseSince(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid since %q: want a duration like 24h or a day count like 7d", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("invalid since %q: want a duration like 24h or a day count like 7d", s)
	}
	return d, nil
}

// Window keeps the records (sorted oldest first) that started within
// `since` of now (0 = no bound), then the most recent `limit` of those
// (0 = all). The result stays oldest first.
func Window(recs []Record, limit int, since time.Duration, now time.Time) []Record {
	out := recs
	if since > 0 {
		cutoff := now.Add(-since)
		kept := make([]Record, 0, len(recs))
		for _, r := range recs {
			t, err := time.Parse(time.RFC3339, r.StartedAt)
			if err != nil || t.Before(cutoff) {
				continue
			}
			kept = append(kept, r)
		}
		out = kept
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	if out == nil {
		out = []Record{}
	}
	return out
}

// Series is one run's time split into the buckets the CLI table and the UI
// chart show. All values are milliseconds.
//
//   - Startup:   startup + prepare + lock + image + container + boot
//   - Metastore: the metastore phase
//   - Spark:     Σ node spark_session + catalog
//   - Reads:     phases.inputs + Σ node inputs
//   - Transform: Σ node transform
//   - Writes:    Σ node write
//   - Stats:     Σ node stats
//   - Other:     Total minus everything above, never below 0
//
// When no node carries timing (an old runner image), NodeTiming is false,
// the five node buckets are 0 and Other is Total - Startup - Metastore: the
// whole node time is unattributed.
type Series struct {
	TotalMs     int64 `json:"total_ms"`
	StartupMs   int64 `json:"startup_ms"`
	MetastoreMs int64 `json:"metastore_ms"`
	SparkMs     int64 `json:"spark_ms"`
	ReadsMs     int64 `json:"reads_ms"`
	TransformMs int64 `json:"transform_ms"`
	WritesMs    int64 `json:"writes_ms"`
	StatsMs     int64 `json:"stats_ms"`
	OtherMs     int64 `json:"other_ms"`
	NodeTiming  bool  `json:"node_timing"`
}

// SeriesOf maps rec onto the Series buckets.
func SeriesOf(rec Record) Series {
	p := rec.Phases
	s := Series{
		TotalMs:     rec.TotalMs,
		StartupMs:   p.Startup + p.Prepare + p.Lock + p.Image + p.Container + p.Boot,
		MetastoreMs: p.Metastore,
	}
	for _, n := range rec.Nodes {
		t := n.ParsedTiming()
		if t == nil {
			continue
		}
		s.NodeTiming = true
		s.SparkMs += t.Phases["spark_session"] + t.Phases["catalog"]
		s.ReadsMs += t.Phases["inputs"]
		s.TransformMs += t.Phases["transform"]
		s.WritesMs += t.Phases["write"]
		s.StatsMs += t.Phases["stats"]
	}
	if s.NodeTiming {
		s.ReadsMs += p.Inputs
	}
	s.OtherMs = s.TotalMs - s.StartupMs - s.MetastoreMs - s.SparkMs - s.ReadsMs - s.TransformMs - s.WritesMs - s.StatsMs
	if s.OtherMs < 0 {
		s.OtherMs = 0
	}
	return s
}
