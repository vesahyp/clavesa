package service

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// dayWindowsFixture is testdata/day_windows_0010.json. The UI reads the
// same file (ui/scripts/day-windows-check.mjs), so both sides are held
// to one answer.
type dayWindowsFixture struct {
	Now      string       `json:"now"`
	TimeZone string       `json:"time_zone"`
	Runs     []SummaryRun `json:"runs"`
	Want     struct {
		D14           int    `json:"d14"`
		D30           int    `json:"d30"`
		FirstDay14    string `json:"first_day_14"`
		FirstDay30    string `json:"first_day_30"`
		LastDay       string `json:"last_day"`
		LastDayFailed int    `json:"last_day_failed"`
	} `json:"want"`
}

// TestDayWindowsAt0010 counts the fixture's runs the way the workspace
// summary does (`clavesa workspace summary`), at 00:10 Helsinki time,
// with the zone taken from the one setting ($CLAVESA_TIME_ZONE).
func TestDayWindowsAt0010(t *testing.T) {
	raw, err := os.ReadFile("testdata/day_windows_0010.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx dayWindowsFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	t.Setenv(workspace.TimeZoneEnv, fx.TimeZone)
	name, loc := workspace.TimeZone(t.TempDir())
	if name != fx.TimeZone {
		t.Fatalf("zone %q, want %q", name, fx.TimeZone)
	}
	now, err := time.Parse(time.RFC3339, fx.Now)
	if err != nil {
		t.Fatal(err)
	}
	now = now.In(loc)

	sum := func(days []SummaryDayRuns) int {
		n := 0
		for _, d := range days {
			n += d.Succeeded + d.Failed
		}
		return n
	}
	d14 := runsPerDay(fx.Runs, now, summaryStripDays)
	d30 := runsPerDay(fx.Runs, now, summaryWindowDays)
	if got := sum(d14); got != fx.Want.D14 {
		t.Errorf("14-day runs %d, want %d", got, fx.Want.D14)
	}
	if got := sum(d30); got != fx.Want.D30 {
		t.Errorf("30-day runs %d, want %d", got, fx.Want.D30)
	}
	if d14[0].Date != fx.Want.FirstDay14 || d30[0].Date != fx.Want.FirstDay30 {
		t.Errorf("first days %s and %s, want %s and %s", d14[0].Date, d30[0].Date, fx.Want.FirstDay14, fx.Want.FirstDay30)
	}
	last := d14[len(d14)-1]
	if last.Date != fx.Want.LastDay || last.Failed != fx.Want.LastDayFailed {
		t.Errorf("last day %+v, want %s with %d failed", last, fx.Want.LastDay, fx.Want.LastDayFailed)
	}
	if got := stripStart(now).Format("2006-01-02"); got != fx.Want.FirstDay14 {
		t.Errorf("failure window starts %s, want %s", got, fx.Want.FirstDay14)
	}
}

// TestTimeZoneSetting: $CLAVESA_TIME_ZONE wins, then clavesa.json
// "time_zone"; a name that does not load is skipped.
func TestTimeZoneSetting(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/clavesa.json", []byte(`{"name":"x","time_zone":"America/New_York"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(workspace.TimeZoneEnv, "")
	if name, _ := workspace.TimeZone(dir); name != "America/New_York" {
		t.Errorf("workspace zone %q, want America/New_York", name)
	}
	t.Setenv(workspace.TimeZoneEnv, "Europe/Helsinki")
	if name, _ := workspace.TimeZone(dir); name != "Europe/Helsinki" {
		t.Errorf("env zone %q, want Europe/Helsinki", name)
	}
	t.Setenv(workspace.TimeZoneEnv, "Not/AZone")
	if name, _ := workspace.TimeZone(dir); name != "America/New_York" {
		t.Errorf("bad env zone gives %q, want the workspace's America/New_York", name)
	}
}
