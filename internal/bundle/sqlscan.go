package bundle

import (
	"regexp"
	"strings"
)

// SQL text as the plan reads it: comments and string literals blanked out
// (newlines kept, so offsets and line numbers stay right), then split into
// statements. This is a scanner, not a full parser: it finds the table
// names after the keywords that name a table, and the constructs on the
// Databricks-only list. The runner resolves names with sqlglot at run time
// (ADR-031 section 6); the plan's job is to know the tables before the
// first run.

// sqlText is one SQL source: the original and the blanked copy.
type sqlText struct {
	Orig  string
	Clean string // comments and string literals replaced by spaces
	// Strings are the string literals, by offset in Orig (for the
	// read_files('s3://...') style checks that need their value).
}

// blankSQL replaces comments and the contents of string literals with
// spaces. Quotes stay, so `'x'` becomes `' '`; backquoted identifiers stay.
func blankSQL(s string) string {
	b := []byte(s)
	out := make([]byte, len(b))
	copy(out, b)
	blank := func(i int) {
		if out[i] != '\n' {
			out[i] = ' '
		}
	}
	for i := 0; i < len(b); i++ {
		switch {
		case b[i] == '-' && i+1 < len(b) && b[i+1] == '-':
			for ; i < len(b) && b[i] != '\n'; i++ {
				blank(i)
			}
		case b[i] == '/' && i+1 < len(b) && b[i+1] == '*':
			for ; i < len(b); i++ {
				if b[i] == '*' && i+1 < len(b) && b[i+1] == '/' {
					blank(i)
					blank(i + 1)
					i++
					break
				}
				blank(i)
			}
		case b[i] == '\'' || b[i] == '"':
			q := b[i]
			for i++; i < len(b); i++ {
				if b[i] == '\\' && i+1 < len(b) {
					blank(i)
					i++
					blank(i)
					continue
				}
				if b[i] == q {
					if i+1 < len(b) && b[i+1] == q { // '' escape
						blank(i)
						i++
						blank(i)
						continue
					}
					break
				}
				blank(i)
			}
		case b[i] == '`':
			for i++; i < len(b) && b[i] != '`'; i++ {
			}
		}
	}
	return string(out)
}

// lineAt is the 1-based line of offset off in s.
func lineAt(s string, off int) int {
	return strings.Count(s[:off], "\n") + 1
}

const ident = "(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*|\\{\\{[^}]+\\}\\}|\\$\\{[^}]+\\}|:[A-Za-z_][A-Za-z0-9_]*)"
const tableName = ident + "(?:\\s*\\.\\s*" + ident + "){0,2}"

var (
	writeRes = []*regexp.Regexp{
		regexp.MustCompile(`(?is)\bINSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bMERGE\s+INTO\s+(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bREPLACE\s+TABLE\s+(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bUPDATE\s+(` + tableName + `)\s+SET\b`),
		regexp.MustCompile(`(?is)\bDELETE\s+FROM\s+(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bTRUNCATE\s+TABLE\s+(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bALTER\s+TABLE\s+(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bOPTIMIZE\s+(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(` + tableName + `)`),
	}
	readRes = []*regexp.Regexp{
		regexp.MustCompile(`(?is)\b(?:FROM|JOIN)\s+(` + tableName + `)`),
		regexp.MustCompile(`(?is)\bMERGE\s+INTO\s+` + tableName + `(?:\s+(?:AS\s+)?\w+)?\s+USING\s+(` + tableName + `)`),
	}
	tempViewRe = regexp.MustCompile(`(?is)\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?TEMP(?:ORARY)?\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(` + ident + `)`)
	cteRe      = regexp.MustCompile(`(?is)(?:\bWITH|,)\s*(` + ident + `)\s+AS\s*\(`)
	useCatRe   = regexp.MustCompile(`(?is)\bUSE\s+CATALOG\s+(` + ident + `)`)
	useSchRe   = regexp.MustCompile(`(?is)\bUSE\s+(?:SCHEMA\s+|DATABASE\s+)?(` + ident + `(?:\s*\.\s*` + ident + `)?)\s*(?:;|$)`)
)

// TableRef is a table a task reads or writes, as written and resolved.
type TableRef struct {
	Name string `json:"name"` // catalog.schema.table
	File string `json:"file,omitempty"`
	Line int    `json:"line,omitempty"`
}

// sqlTables finds the tables a SQL text reads and writes. cat and sch are
// the current catalog and schema (USE statements in the text change them).
// It returns resolved three-level names; a name it cannot resolve to three
// levels (no current catalog) is returned as written, for the caller to
// report. A FROM over a subquery, a CTE or a temp view is not a table.
var identLiteralRe = regexp.MustCompile(`(?i)\bIDENTIFIER\s*\(\s*'([^'\\]*)'\s*\)`)

// unwrapIdentifier turns IDENTIFIER('a.b.c') with a literal string into
// the name a.b.c, so the scanner sees it (also USE CATALOG IDENTIFIER('x')).
func unwrapIdentifier(text string) string {
	return identLiteralRe.ReplaceAllStringFunc(text, func(m string) string {
		inner := identLiteralRe.FindStringSubmatch(m)[1]
		parts := strings.Split(inner, ".")
		for i, p := range parts {
			parts[i] = "`" + p + "`"
		}
		return strings.Join(parts, ".")
	})
}

func sqlTables(text, cat, sch string, views ...map[string]bool) (reads, writes []TableRef, unresolved []TableRef) {
	text = unwrapIdentifier(text)
	clean := blankSQL(text)
	local := map[string]bool{}
	for _, v := range views {
		for k := range v {
			local[k] = true
		}
	}
	for _, m := range tempViewRe.FindAllStringSubmatch(clean, -1) {
		local[strings.ToLower(unquote(m[1]))] = true
	}
	for _, m := range cteRe.FindAllStringSubmatch(clean, -1) {
		local[strings.ToLower(unquote(m[1]))] = true
	}
	// Statements are scanned in order so USE applies to what follows.
	off := 0
	for _, stmt := range splitStatements(clean) {
		if m := useCatRe.FindStringSubmatch(stmt); m != nil {
			cat = unquote(m[1])
		} else if m := useSchRe.FindStringSubmatch(stmt); m != nil && !strings.Contains(strings.ToUpper(stmt), "USING") {
			parts := splitName(m[1])
			if len(parts) == 2 {
				cat, sch = parts[0], parts[1]
			} else {
				sch = parts[0]
			}
		}
		add := func(res []*regexp.Regexp, into *[]TableRef) {
			for _, re := range res {
				for _, m := range re.FindAllStringSubmatchIndex(stmt, -1) {
					raw := stmt[m[2]:m[3]]
					if strings.HasPrefix(raw, "(") || strings.HasPrefix(raw, ":") {
						continue
					}
					parts := splitName(raw)
					if len(parts) == 1 && local[strings.ToLower(parts[0])] {
						continue
					}
					if isTableFunction(stmt, m[3]) || notATableFrom(stmt, m[0]) {
						continue
					}
					ref := TableRef{Line: lineAt(clean, off+m[2])}
					switch len(parts) {
					case 3:
						ref.Name = strings.Join(parts, ".")
					case 2:
						if cat == "" {
							ref.Name = raw
							unresolved = append(unresolved, ref)
							continue
						}
						ref.Name = cat + "." + parts[0] + "." + parts[1]
					case 1:
						if cat == "" || sch == "" {
							ref.Name = raw
							unresolved = append(unresolved, ref)
							continue
						}
						ref.Name = cat + "." + sch + "." + parts[0]
					}
					*into = append(*into, ref)
				}
			}
		}
		add(writeRes, &writes)
		add(readRes, &reads)
		off += len(stmt) + 1
	}
	return dedupe(reads), dedupe(writes), unresolved
}

// isTableFunction is true when the name at end is followed by "(": a table
// function such as read_files(...) or range(...), not a table.
func isTableFunction(stmt string, end int) bool {
	rest := strings.TrimLeft(stmt[end:], " \t\r\n")
	return strings.HasPrefix(rest, "(")
}

var (
	distinctBeforeRe = regexp.MustCompile(`(?i)\bDISTINCT\s*$`)
	funcBeforeRe     = regexp.MustCompile(`(?i)\b(EXTRACT|TRIM|SUBSTRING|POSITION|OVERLAY)\s*$`)
)

// notATableFrom is true for a FROM that is not a table clause:
// "IS [NOT] DISTINCT FROM x", and FROM inside extract(... FROM x),
// trim(... FROM x), substring(x FROM n) and the like.
func notATableFrom(stmt string, at int) bool {
	before := stmt[:at]
	if distinctBeforeRe.MatchString(before) {
		return true
	}
	depth := 0
	for i := len(before) - 1; i >= 0; i-- {
		switch before[i] {
		case ')':
			depth++
		case '(':
			if depth == 0 {
				return funcBeforeRe.MatchString(before[:i])
			}
			depth--
		}
	}
	return false
}

// splitStatements splits blanked SQL at semicolons, keeping lengths so the
// caller can track offsets (each statement plus its ';').
func splitStatements(clean string) []string {
	return strings.Split(clean, ";")
}

func splitName(raw string) []string {
	var parts []string
	for _, p := range strings.Split(raw, ".") {
		parts = append(parts, unquote(strings.TrimSpace(p)))
	}
	return parts
}

func unquote(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`")
}

func dedupe(refs []TableRef) []TableRef {
	seen := map[string]bool{}
	var out []TableRef
	for _, r := range refs {
		k := strings.ToLower(r.Name)
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, r)
	}
	return out
}

// SQLStatements splits SQL text into statements the way the runner runs a
// sql_task file: at semicolons outside comments and string literals, with
// empty statements dropped.
func SQLStatements(text string) []string {
	clean := blankSQL(text)
	var out []string
	start := 0
	for i := 0; i <= len(clean); i++ {
		if i == len(clean) || clean[i] == ';' {
			stmt := strings.TrimSpace(text[start:i])
			if strings.TrimSpace(clean[start:i]) != "" {
				out = append(out, stmt)
			}
			start = i + 1
		}
	}
	return out
}
