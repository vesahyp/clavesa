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
// without a migration. Empty input resolves to `now-30d`, the same
// default the old `resolveTimePreset` used to fall back to.

var relativeRE = regexp.MustCompile(`^now-(\d+)([mhdw])$`)

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
// expression (or one of the legacy aliases). Empty input is treated as
// the default `now-30d`. Anything else returns an error so a typo fails
// loud rather than silently defaulting.
func ParseRelative(expr string) (time.Duration, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		expr = "now-30d"
	}
	if alias, ok := legacyAlias[expr]; ok {
		expr = alias
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
	return time.Duration(n) * unit, nil
}

// ResolveTimeRange turns a canonical `now-<n><unit>` expression (or a
// legacy preset key) into a {start, end} pair of ISO 8601 / RFC 3339
// timestamps at `now`. Invalid input falls back to `now-30d` so a
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
		d = 30 * 24 * time.Hour
	}
	end = now.UTC().Format(time.RFC3339)
	start = now.UTC().Add(-d).Format(time.RFC3339)
	return start, end
}
