package runtiming

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseSince(t *testing.T) {
	cases := map[string]time.Duration{
		"":    0,
		"7d":  7 * 24 * time.Hour,
		"90d": 90 * 24 * time.Hour,
		"24h": 24 * time.Hour,
		"90m": 90 * time.Minute,
	}
	for in, want := range cases {
		got, err := ParseSince(in)
		if err != nil || got != want {
			t.Errorf("ParseSince(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"7x", "d", "-3d", "-1h", "1.5d", "week"} {
		if _, err := ParseSince(bad); err == nil {
			t.Errorf("ParseSince(%q) accepted bad input", bad)
		}
	}
}

func TestWindow(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	at := func(h int) string { return now.Add(-time.Duration(h) * time.Hour).Format(time.RFC3339) }
	recs := []Record{
		{RunID: "a", StartedAt: at(72)},
		{RunID: "b", StartedAt: at(30)},
		{RunID: "c", StartedAt: at(5)},
		{RunID: "d", StartedAt: at(1)},
	}
	ids := func(rs []Record) string {
		s := ""
		for _, r := range rs {
			s += r.RunID
		}
		return s
	}
	if got := ids(Window(recs, 0, 0, now)); got != "abcd" {
		t.Errorf("no bounds = %q", got)
	}
	if got := ids(Window(recs, 2, 0, now)); got != "cd" {
		t.Errorf("limit 2 = %q, want the two most recent, oldest first", got)
	}
	if got := ids(Window(recs, 0, 48*time.Hour, now)); got != "bcd" {
		t.Errorf("since 48h = %q", got)
	}
	if got := ids(Window(recs, 1, 48*time.Hour, now)); got != "d" {
		t.Errorf("since 48h limit 1 = %q", got)
	}
	if got := Window(nil, 5, time.Hour, now); got == nil || len(got) != 0 {
		t.Errorf("empty input should give an empty, non-nil slice: %#v", got)
	}
}

func TestSeriesWithNodeTiming(t *testing.T) {
	rec := Record{
		TotalMs: 10_000,
		Phases:  Phases{Startup: 100, Prepare: 200, Lock: 10, Metastore: 500, Image: 40, Inputs: 300, Container: 150, Boot: 1000},
		Nodes: []Node{
			{Node: "a", Timing: json.RawMessage(`{"phases":{"spark_session":400,"catalog":100,"inputs":700,"transform":200,"write":2000,"stats":300,"table_state":50}}`)},
			{Node: "b", Timing: json.RawMessage(`{"phases":{"catalog":50,"inputs":100,"transform":100,"write":1000,"stats":0}}`)},
			{Node: "old", Timing: json.RawMessage(`null`)},
		},
	}
	s := SeriesOf(rec)
	want := Series{
		TotalMs: 10_000, StartupMs: 1500, MetastoreMs: 500,
		SparkMs: 550, ReadsMs: 1100, TransformMs: 300, WritesMs: 3000, StatsMs: 300,
		NodeTiming: true,
	}
	want.OtherMs = 10_000 - 1500 - 500 - 550 - 1100 - 300 - 3000 - 300
	if s != want {
		t.Errorf("SeriesOf =\n %+v\nwant\n %+v", s, want)
	}
}

func TestSeriesWithoutNodeTiming(t *testing.T) {
	rec := Record{
		TotalMs: 5_000,
		Phases:  Phases{Startup: 100, Prepare: 100, Metastore: 800, Inputs: 400, Boot: 300, Nodes: 2500},
		Nodes:   []Node{{Node: "a"}, {Node: "b", Timing: json.RawMessage(`null`)}},
	}
	s := SeriesOf(rec)
	if s.NodeTiming {
		t.Fatal("NodeTiming should be false when no node has timing")
	}
	if s.SparkMs != 0 || s.ReadsMs != 0 || s.TransformMs != 0 || s.WritesMs != 0 || s.StatsMs != 0 {
		t.Errorf("node buckets should be 0: %+v", s)
	}
	if s.StartupMs != 500 || s.MetastoreMs != 800 || s.OtherMs != 5_000-500-800 {
		t.Errorf("SeriesOf = %+v; want startup 500, metastore 800, other 3700", s)
	}
}

func TestSeriesOtherNeverNegative(t *testing.T) {
	rec := Record{
		TotalMs: 100,
		Phases:  Phases{Startup: 500},
		Nodes:   []Node{{Node: "a", Timing: json.RawMessage(`{"phases":{"write":900}}`)}},
	}
	if s := SeriesOf(rec); s.OtherMs != 0 {
		t.Errorf("OtherMs = %d, want 0", s.OtherMs)
	}
}
