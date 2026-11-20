package service

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"
)

type periodFixture struct {
	Presets   []string          `json:"presets"`
	Fallback  string            `json:"fallback"`
	Canonical map[string]string `json:"canonical"`
	Spans     map[string]int64  `json:"spans_seconds"`
	Defaults  []struct {
		Name    string   `json:"name"`
		Control string   `json:"control"`
		SQLs    []string `json:"sqls"`
		Want    string   `json:"want"`
	} `json:"defaults"`
}

// TestPeriodFixture checks the dashboard periods against
// testdata/time_range_defaults.json; make ui-time-range-check checks the
// UI against the same file.
func TestPeriodFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/time_range_defaults.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx periodFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(PeriodPresets, fx.Presets) {
		t.Errorf("PeriodPresets %v, fixture %v", PeriodPresets, fx.Presets)
	}
	if FallbackPeriod != fx.Fallback {
		t.Errorf("FallbackPeriod %q, fixture %q", FallbackPeriod, fx.Fallback)
	}
	for in, want := range fx.Canonical {
		if got := CanonicalPeriod(in); got != want {
			t.Errorf("CanonicalPeriod(%q) = %q, want %q", in, got, want)
		}
	}
	now := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	for in, secs := range fx.Spans {
		start, end := ResolveTimeRange(in, now)
		s, _ := time.Parse(time.RFC3339, start)
		e, _ := time.Parse(time.RFC3339, end)
		if got := e.Sub(s); got != time.Duration(secs)*time.Second {
			t.Errorf("ResolveTimeRange(%q) spans %v, want %ds", in, got, secs)
		}
	}
	for _, c := range fx.Defaults {
		if got := DefaultPeriod(c.Control, c.SQLs); got != c.Want {
			t.Errorf("%s: DefaultPeriod = %q, want %q", c.Name, got, c.Want)
		}
	}
}

// A control with no default resolves through DefaultPeriod; one with a
// default keeps it.
func TestResolveControlDefaultsFromGrain(t *testing.T) {
	now := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	ds := []DashboardDataset{{Name: "h", SQL: "SELECT event_hour, n FROM t_hourly WHERE event_hour >= {{p.start}}"}}
	span := func(def string) time.Duration {
		out := map[string]string{}
		resolveControlDefaults([]DashboardControl{{Name: "p", Type: "time_range", Default: def}}, ds, out, now)
		s, _ := time.Parse(time.RFC3339, out["p.start"])
		e, _ := time.Parse(time.RFC3339, out["p.end"])
		return e.Sub(s)
	}
	if got := span(""); got != 24*time.Hour {
		t.Errorf("no default on hourly data: %v, want 24h", got)
	}
	if got := span("last_30d"); got != 30*24*time.Hour {
		t.Errorf("declared default: %v, want 30 days", got)
	}
	if got := span("now-5m"); got != time.Hour {
		t.Errorf("retired now-5m: %v, want 1h", got)
	}
}
