package service

import (
	"fmt"
	"time"
)

// reasonTime is "9.11 07:16" in the summary's zone (workspace.TimeZone:
// the hosted config, the workspace or the machine), the zone the UI
// shows too, so the CLI, the hosted Lambda and the UI say the same.
func reasonTime(ms int64, loc *time.Location) string {
	return time.UnixMilli(ms).In(loc).Format("2.1 15:04")
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
func overdueReason(lastRun *SummaryRun, intervalSeconds int64, loc *time.Location) string {
	if lastRun == nil || isoMillis(lastRun.StartedAt) == 0 {
		return "never run, expected " + everyText(intervalSeconds)
	}
	return fmt.Sprintf("no run since %s, expected %s", reasonTime(isoMillis(lastRun.StartedAt), loc), everyText(intervalSeconds))
}

// pausedNote is the note of a pipeline whose schedule or poller is
// paused. It has no time: when a rule was paused is only in CloudTrail,
// which the reader role may not read (Vesa, 2026-11-17).
const pausedNote = "schedule paused"

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
	return fmt.Sprintf("%d failed %s in %d days, last %s", n, runsWord, summaryStripDays, reasonTime(newest, now.Location()))
}

// stripStart is the start of the first day of the strip window, the
// same days runsPerDay counts, in now's zone.
func stripStart(now time.Time) time.Time {
	d := now.AddDate(0, 0, -(summaryStripDays - 1))
	return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, now.Location())
}
