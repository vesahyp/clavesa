package service

import (
	"fmt"
	"time"
	// The summary's times are Helsinki time on every machine, also in the
	// hosted Lambda (no zoneinfo there): the embedded zone database.
	_ "time/tzdata"
)

// summaryZone is the zone every time in a status reason is written in:
// Helsinki, the zone the hosted UI shows (ui/src/lib/clock.ts), so the
// CLI, a summary written by the hosted Lambda and the UI say the same.
var summaryZone = func() *time.Location {
	loc, err := time.LoadLocation("Europe/Helsinki")
	if err != nil {
		return time.UTC
	}
	return loc
}()

// reasonTime is "9.11 07:16", Helsinki time.
func reasonTime(ms int64) string {
	return time.UnixMilli(ms).In(summaryZone).Format("2.1 15:04")
}

// everyText is how often a pipeline should run, in words: "daily",
// "hourly", "every 6 h", "every 15 min", "every 2 days".
func everyText(seconds int64) string {
	switch {
	case seconds == 86400:
		return "daily"
	case seconds == 3600:
		return "hourly"
	case seconds%86400 == 0:
		return fmt.Sprintf("every %d days", seconds/86400)
	case seconds%3600 == 0:
		return fmt.Sprintf("every %d h", seconds/3600)
	case seconds%60 == 0:
		return fmt.Sprintf("every %d min", seconds/60)
	default:
		return fmt.Sprintf("every %d s", seconds)
	}
}

// overdueReason says that a pipeline has not run inside its expected
// interval, with when it last ran: "no run since 9.11 07:16, expected
// daily", or "never run, expected daily". The time is absolute, so a
// stored summary that is read days later still says the truth.
func overdueReason(lastRun *SummaryRun, intervalSeconds int64) string {
	if lastRun == nil || isoMillis(lastRun.StartedAt) == 0 {
		return "never run, expected " + everyText(intervalSeconds)
	}
	return fmt.Sprintf("no run since %s, expected %s", reasonTime(isoMillis(lastRun.StartedAt)), everyText(intervalSeconds))
}

// pausedNote is the note of a pipeline whose schedule is paused:
// "schedule paused since 9.11 09:21", or "schedule paused" when the time
// it was paused is not known (CloudTrail keeps 90 days, and the reader
// may not be allowed to look).
func pausedNote(since string) string {
	if t, err := time.Parse(time.RFC3339, since); err == nil {
		return "schedule paused since " + reasonTime(t.UnixMilli())
	}
	return "schedule paused"
}

// recentFailures applies the failed-run rule: a pipeline with a failed
// run in the card's strip window (summaryStripDays) is never "ok". It
// returns the reason, "7 failed runs in 14 days, last 15.11 11:08", or ""
// when there is none to add. A single failure that is the last run is
// already "last run failed" and adds nothing.
func recentFailures(runs []SummaryRun, now time.Time, lastFailed bool) string {
	from := stripStart(now)
	n := 0
	var newest int64
	for _, r := range runs {
		if r.Result != "failed" {
			continue
		}
		ms := isoMillis(r.StartedAt)
		if ms == 0 || time.UnixMilli(ms).Before(from) {
			continue
		}
		n++
		if ms > newest {
			newest = ms
		}
	}
	if n == 0 || (n == 1 && lastFailed) {
		return ""
	}
	runsWord := "runs"
	if n == 1 {
		runsWord = "run"
	}
	return fmt.Sprintf("%d failed %s in %d days, last %s", n, runsWord, summaryStripDays, reasonTime(newest))
}

// stripStart is the start of the first day of the strip window, the
// same UTC days runsPerDay counts.
func stripStart(now time.Time) time.Time {
	d := now.UTC().AddDate(0, 0, -(summaryStripDays - 1))
	return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
}
