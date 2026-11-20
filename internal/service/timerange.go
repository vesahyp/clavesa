package service

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Time-range parser shared by the dashboard `time_range` control's
// default value and the per-render relative expression on the URL.
//
// **Canonical form: `now-<n><unit>`** (`now-1h`, `now-30m`, `now-2w`).
// Mirrors Grafana's de-facto syntax — the goal is not to invent a new
// format, see `feedback_prefer_standards_over_custom_formats`. The TS
// side at `ui/src/lib/timeRange.ts` parses the same shape.
//
// **Back-compat aliases** for the legacy preset keys (`last_24h`,
// `last_7d`, `last_30d`, `last_90d`) so saved dashboards keep working
// without a migration.
//
// **Periods (Vesa, 2026-11-19: "Last 5min? Smallest should be 1h or
// something. Lets have sensible dashboard defaults").** The picker offers
// PeriodPresets: 1 hour, 24 hours, 7, 30 and 90 days. A dashboard or link
// that names a preset since removed (5, 15 or 30 minutes, 3 or 12 hours,
// 3 days) gets the nearest one left, and any other window under an hour
// is read as an hour, with no error. A control with no default takes one
// from the grain of the SQL that uses it (DefaultPeriod); empty or
// invalid input resolves to FallbackPeriod, 7 days.
//
// testdata/time_range_defaults.json holds the cases; TestPeriodFixture
// checks them here and `make ui-time-range-check` checks the same file
// against ui/src/lib/timeRange.ts.

var relativeRE = regexp.MustCompile(`^now-(\d+)([mhdw])$`)

// PeriodPresets are the periods the dashboard picker offers, shortest
// first.
var PeriodPresets = []string{"now-1h", "now-24h", "now-7d", "now-30d", "now-90d"}

// FallbackPeriod is the window for an empty or invalid expression, and
// for a control whose SQL says nothing about its grain.
const FallbackPeriod = "now-7d"

// retiredPreset maps each preset the picker no longer offers to the
// nearest one left.
var retiredPreset = map[string]string{
	"now-5m":  "now-1h",
	"now-15m": "now-1h",
	"now-30m": "now-1h",
	"now-3h":  "now-1h",
	"now-12h": "now-24h",
	"now-3d":  "now-7d",
}

// CanonicalPeriod folds a legacy key, a retired preset or a window under
// an hour into the expression it now means, so the picker names the
// window that loads; anything else is returned trimmed.
func CanonicalPeriod(expr string) string {
	e := strings.TrimSpace(expr)
	if alias, ok := legacyAlias[e]; ok {
		e = alias
	}
	if next, ok := retiredPreset[e]; ok {
		e = next
	}
	if m := minutesRE.FindStringSubmatch(e); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 && n < 60 {
			e = "now-1h"
		}
	}
	return e
}

var minutesRE = regexp.MustCompile(`^now-(\d+)m$`)

// The grain of a dataset is read only from its time column: an item of
// the outer SELECT list or GROUP BY (ui/src/lib/timeRange.ts has the same
// rules). Table names, WHERE filters (current_date, a date cast of a
// bound) and hour(ts) or month(ts) breakdowns say nothing about it.
var (
	itemFine = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^date_trunc\s*\(\s*'(hour|minute|second)'`),
		regexp.MustCompile(`(?i)^(\w+\.)?(\w+_(hour|minute)|(hour|minute)_ts)$`),
	}
	itemCoarse = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^date_trunc\s*\(\s*'(week|month|quarter|year)'`),
		regexp.MustCompile(`(?i)^(\w+\.)?\w+_(week|month)$`),
	}
	itemDaily = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^date_trunc\s*\(\s*'day'`),
		regexp.MustCompile(`(?is)^(try_)?cast\s*\(.*\s+as\s+date\s*\)$`),
		regexp.MustCompile(`(?i)^(to_)?date\s*\(`),
		regexp.MustCompile(`(?i)^(\w+\.)?(date|day|dt)$`),
		regexp.MustCompile(`(?i)^(\w+\.)?\w+_date$`),
	}
	sqlKeyword = regexp.MustCompile(`(?i)^(select|from|where|group\s+by|having|order\s+by|limit|union|window)\b`)
	itemAlias  = regexp.MustCompile("(?i)\\s+as\\s+[\"`]?\\w+[\"`]?$")
	wordSpace  = regexp.MustCompile(`\s+`)
)

func isWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// timeItems returns the items of the outer SELECT list and GROUP BY,
// aliases removed. Only the first outer SELECT counts; a CTE's sit inside
// brackets.
func timeItems(sql string) []string {
	var items []string
	depth := 0
	var quote byte
	var cur strings.Builder
	part := ""
	seenSelect := false
	flush := func() {
		e := strings.TrimSpace(itemAlias.ReplaceAllString(strings.TrimSpace(cur.String()), ""))
		if part != "" && e != "" {
			items = append(items, e)
		}
		cur.Reset()
	}
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if quote != 0 {
			cur.WriteByte(ch)
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
			cur.WriteByte(ch)
			continue
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && isWordByte(ch) && (i == 0 || !isWordByte(sql[i-1])) {
			if m := sqlKeyword.FindStringSubmatch(sql[i:]); m != nil {
				flush()
				k := strings.ToLower(wordSpace.ReplaceAllString(m[1], " "))
				switch {
				case k == "select" && !seenSelect:
					part = "select"
				case k == "group by":
					part = "group"
				default:
					part = ""
				}
				if k == "select" {
					seenSelect = true
				}
				i += len(m[0]) - 1
				continue
			}
		}
		if depth == 0 && ch == ',' {
			flush()
			continue
		}
		if depth >= 0 {
			cur.WriteByte(ch)
		}
	}
	flush()
	return items
}

func grainOf(sql string) string {
	has := func(res []*regexp.Regexp) bool {
		for _, e := range timeItems(sql) {
			if e == "current_date" {
				continue
			}
			for _, r := range res {
				if r.MatchString(e) {
					return true
				}
			}
		}
		return false
	}
	switch {
	case has(itemFine):
		return "now-24h"
	case has(itemCoarse):
		return "now-90d"
	case has(itemDaily):
		return "now-7d"
	}
	return ""
}

// DefaultPeriod is the period a time_range control named `control` shows
// when the dashboard sets none: 24 hours for data by the hour or finer,
// 7 days for daily data, 90 days for weekly or monthly data. It reads the
// time column (timeItems) of the datasets that use the control (all of
// them when none does);
// with several grains the longest period wins, so no chart shows a
// single bar. Nothing to go by gives FallbackPeriod.
func DefaultPeriod(control string, sqls []string) string {
	using := []string{}
	for _, q := range sqls {
		if strings.Contains(q, "{{"+control+".start}}") || strings.Contains(q, "{{"+control+".end}}") {
			using = append(using, q)
		}
	}
	if len(using) == 0 {
		using = sqls
	}
	best := time.Duration(0)
	pick := ""
	for _, q := range using {
		p := grainOf(q)
		if p == "" {
			continue
		}
		if d, _ := ParseRelative(p); d > best {
			best, pick = d, p
		}
	}
	if pick == "" {
		return FallbackPeriod
	}
	return pick
}

// legacyAlias maps the four pre-Slice-E preset keys to their canonical
// `now-<n><unit>` form. Older saved dashboards persisted Default as
// `last_24h` etc.; we accept those on read but never emit them.
var legacyAlias = map[string]string{
	"last_24h": "now-24h",
	"last_7d":  "now-7d",
	"last_30d": "now-30d",
	"last_90d": "now-90d",
}

// ParseRelative returns the lookback duration encoded by a `now-<n><unit>`
// expression (or a legacy alias or a retired preset, see
// CanonicalPeriod). Empty input is treated as FallbackPeriod. A window
// under an hour is an hour. Anything else returns an error so a typo
// fails loud rather than silently defaulting.
func ParseRelative(expr string) (time.Duration, error) {
	expr = CanonicalPeriod(expr)
	if expr == "" {
		expr = FallbackPeriod
	}
	m := relativeRE.FindStringSubmatch(expr)
	if m == nil {
		return 0, fmt.Errorf("invalid time-range expression %q (want `now-<n><unit>` with unit m|h|d|w)", expr)
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid time-range expression %q (n must be a positive integer)", expr)
	}
	var unit time.Duration
	switch m[2] {
	case "m":
		unit = time.Minute
	case "h":
		unit = time.Hour
	case "d":
		unit = 24 * time.Hour
	case "w":
		unit = 7 * 24 * time.Hour
	default:
		// regex already gates this; defensive only.
		return 0, fmt.Errorf("invalid time-range unit %q", m[2])
	}
	d := time.Duration(n) * unit
	if d < time.Hour {
		d = time.Hour
	}
	return d, nil
}

// ResolveTimeRange turns a canonical `now-<n><unit>` expression (or a
// legacy preset key) into a {start, end} pair of ISO 8601 / RFC 3339
// timestamps at `now`. Invalid input falls back to FallbackPeriod so a
// freshly-added control without a typed default Just Works.
//
// This mirrors `ui/src/lib/timeRange.ts: resolveTimeRange` — same
// inputs, same outputs to the second. Parity is asserted in
// `dashboard_test.go`.
func ResolveTimeRange(expr string, now time.Time) (start, end string) {
	d, err := ParseRelative(expr)
	if err != nil {
		// Defensive fallback. The viewer should never hit this — the
		// authoring path validates the default before save (or will, once
		// validateDraft lands in Slice D); URL `?…rel=` values come from
		// the picker which only emits canonical form.
		d = 7 * 24 * time.Hour
	}
	end = now.UTC().Format(time.RFC3339)
	start = now.UTC().Add(-d).Format(time.RFC3339)
	return start, end
}
