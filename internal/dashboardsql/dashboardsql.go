// Package dashboardsql carries the `{{name}}` placeholder substitution
// + parameter-value safety regex shared by the dashboard query path.
// Before C13 (2026-05-24), api/dashboards.go and service/dashboard.go
// each kept their own copy with a comment "keep both in sync" — exactly
// the drift hazard the leaf package eliminates.
package dashboardsql

import (
	"fmt"
	"regexp"
)

// PlaceholderRE matches `{{name}}` tokens in dataset SQL. Dotted names
// (`{{range.start}}`) and hyphens (control names share that character
// class) are allowed in the capture.
var PlaceholderRE = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_.\-]*)\s*\}\}`)

// SafeParamValueRE restricts substituted values to a safe character
// class. Quotes and semicolons are rejected so a value can never close
// the surrounding string literal or chain a statement. ISO timestamps,
// identifiers, hyphens, and slashes cover the real cases.
var SafeParamValueRE = regexp.MustCompile(`^[A-Za-z0-9 _.:+\-T/]*$`)

// ExpandPlaceholders substitutes `{{name}}` tokens in sql with quoted
// values from params. Missing keys are an error — a typo in a dataset's
// SQL should fail loud rather than silently produce an empty WHERE.
// Substituted values are inserted as single-quoted SQL string literals;
// authors do NOT wrap placeholders in quotes themselves.
func ExpandPlaceholders(sql string, params map[string]string) (string, error) {
	var firstErr error
	expanded := PlaceholderRE.ReplaceAllStringFunc(sql, func(match string) string {
		if firstErr != nil {
			return match
		}
		m := PlaceholderRE.FindStringSubmatch(match)
		name := m[1]
		val, ok := params[name]
		if !ok {
			firstErr = fmt.Errorf("dataset SQL references {{%s}} but no control or param sets it", name)
			return match
		}
		if !SafeParamValueRE.MatchString(val) {
			firstErr = fmt.Errorf("param %q value %q contains characters outside [A-Za-z0-9 _.:+\\-T/]", name, val)
			return match
		}
		// Single-quote the literal. The charset already excludes ' and \
		// so no escaping is needed and both Spark and Athena read the
		// result identically.
		return "'" + val + "'"
	})
	if firstErr != nil {
		return "", firstErr
	}
	return expanded, nil
}
