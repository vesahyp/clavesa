package bundle

import (
	"fmt"
	"strings"
)

// AWSCron converts a Databricks (Quartz) cron expression,
// "sec min hour day-of-month month day-of-week [year]", to an EventBridge
// Scheduler expression "cron(min hour day-of-month month day-of-week
// year)" (ADR-031 section 7). Both use 1-7 = SUN-SAT and the names, and
// both want ? in one of the two day fields. A seconds field other than 0
// cannot be kept and is an error.
func AWSCron(quartz string) (string, error) {
	f := strings.Fields(quartz)
	if len(f) != 6 && len(f) != 7 {
		return "", fmt.Errorf("quartz_cron_expression %q: want 6 or 7 fields (sec min hour day month weekday [year])", quartz)
	}
	if f[0] != "0" && f[0] != "00" {
		return "", fmt.Errorf("quartz_cron_expression %q: the seconds field is %s; AWS schedules start on the minute, so it must be 0", quartz, f[0])
	}
	year := "*"
	if len(f) == 7 {
		year = f[6]
	}
	dom, dow := f[3], f[5]
	switch {
	case dom == "?" && dow == "?":
		return "", fmt.Errorf("quartz_cron_expression %q: day-of-month and day-of-week are both ?", quartz)
	case dom != "?" && dow != "?":
		// Quartz refuses this too; AWS needs one ?. "*" in one of them is
		// the common case: that one becomes ?.
		switch {
		case dow == "*":
			dow = "?"
		case dom == "*":
			dom = "?"
		default:
			return "", fmt.Errorf("quartz_cron_expression %q: set ? in day-of-month or day-of-week", quartz)
		}
	}
	return fmt.Sprintf("cron(%s %s %s %s %s %s)", f[1], f[2], dom, f[4], dow, year), nil
}

// AWSRate converts trigger.periodic to an EventBridge rate expression.
func AWSRate(interval int, unit string) (string, error) {
	if interval < 1 {
		return "", fmt.Errorf("trigger.periodic interval %d: must be 1 or more", interval)
	}
	var n int
	var u string
	switch strings.ToUpper(unit) {
	case "HOURS":
		n, u = interval, "hour"
	case "DAYS":
		n, u = interval, "day"
	case "WEEKS":
		n, u = interval*7, "day"
	default:
		return "", fmt.Errorf("trigger.periodic unit %q: want HOURS, DAYS or WEEKS", unit)
	}
	if n != 1 {
		u += "s"
	}
	return fmt.Sprintf("rate(%d %s)", n, u), nil
}
