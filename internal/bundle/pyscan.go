package bundle

import (
	"regexp"
	"strings"
)

// pyString is one Python string literal.
type pyString struct {
	Start, End int    // offsets of the whole literal in the source, quotes included
	Prefix     string // lower-cased: "", "f", "r", "rb", ...
	Value      string // the text between the quotes (escapes not processed)
}

// scanPython blanks the comments of Python source (newlines kept) and
// returns its string literals.
func scanPython(src string) (clean string, strs []pyString) {
	b := []byte(src)
	out := make([]byte, len(b))
	copy(out, b)
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c == '#' {
			for ; i < len(b) && b[i] != '\n'; i++ {
				out[i] = ' '
			}
			continue
		}
		if c != '\'' && c != '"' {
			continue
		}
		// String prefix: letters right before the quote.
		ps := i
		for ps > 0 && strings.ContainsRune("rRbBfFuU", rune(b[ps-1])) {
			ps--
		}
		if ps > 0 && (isIdentByte(b[ps-1])) {
			ps = i // part of a name, e.g. `bar"` is not a prefix
		}
		prefix := strings.ToLower(string(b[ps:i]))
		triple := i+2 < len(b) && b[i+1] == c && b[i+2] == c
		q := 1
		if triple {
			q = 3
		}
		start := i + q
		j := start
		for j < len(b) {
			if b[j] == '\\' && !strings.Contains(prefix, "r") {
				j += 2
				continue
			}
			if b[j] == c && (!triple || (j+2 < len(b) && b[j+1] == c && b[j+2] == c)) {
				break
			}
			if !triple && b[j] == '\n' {
				break
			}
			j++
		}
		end := j + q
		if end > len(b) {
			end = len(b)
		}
		val := ""
		if j <= len(b) && start <= j {
			val = string(b[start:min(j, len(b))])
		}
		strs = append(strs, pyString{Start: ps, End: end, Prefix: prefix, Value: val})
		i = end - 1
	}
	return string(out), strs
}

func isIdentByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// literalArg returns the plain string literal that is the first argument of
// the call whose "(" is at open, and whether it is one. An f-string with
// placeholders, a variable or an expression is not plain.
func literalArg(src string, strs []pyString, open int) (pyString, bool) {
	i := open + 1
	for i < len(src) && (src[i] == ' ' || src[i] == '\n' || src[i] == '\t' || src[i] == '\r') {
		i++
	}
	// keyword form: name="x"
	if m := kwArgRe.FindStringIndex(src[i:]); m != nil && m[0] == 0 {
		i += m[1]
	}
	var out pyString
	found := false
	for idx := 0; idx < len(strs); idx++ {
		s := strs[idx]
		if s.Start != i {
			continue
		}
		if strings.Contains(s.Prefix, "f") && strings.Contains(s.Value, "{") {
			return s, false
		}
		if !found {
			out, found = s, true
		} else {
			out.Value += s.Value // implicit concatenation: "a " "b"
			out.End = s.End
		}
		// The next literal right after this one (whitespace, newlines or a
		// backslash continuation between them) is part of the same string.
		j := s.End
		for j < len(src) && strings.ContainsRune(" \t\r\n\\", rune(src[j])) {
			j++
		}
		if j < len(src) && j != s.End {
			i = j
			continue
		}
		break
	}
	if !found {
		return pyString{}, false
	}
	rest := strings.TrimLeft(src[out.End:], " \t\r\n")
	if strings.HasPrefix(rest, ")") || strings.HasPrefix(rest, ",") {
		return out, true
	}
	return out, false
}

var kwArgRe = regexp.MustCompile(`^[A-Za-z_]\w*\s*=\s*`)

// pyCall is one call the plan looks at.
type pyCall struct {
	Kind string // "read", "write", "sql", "secret"
	Open int    // offset of "("
	Line int
}

var pyCallRes = []struct {
	kind string
	re   *regexp.Regexp
}{
	{"read", regexp.MustCompile(`\bspark\s*\.\s*(?:read\s*\.\s*)?table\s*\(`)},
	{"read", regexp.MustCompile(`\.\s*readStream\s*\.\s*table\s*\(`)},
	{"write", regexp.MustCompile(`\.\s*saveAsTable\s*\(`)},
	{"write", regexp.MustCompile(`\.\s*insertInto\s*\(`)},
	{"write", regexp.MustCompile(`\.\s*toTable\s*\(`)},
	{"deltatable", regexp.MustCompile(`\bDeltaTable\s*\.\s*forName\s*\(`)},
	{"sql", regexp.MustCompile(`\bspark\s*\.\s*sql\s*\(`)},
	{"secret", regexp.MustCompile(`\bdbutils\s*\.\s*secrets\s*\.\s*get(?:Bytes)?\s*\(`)},
}

// pyCalls finds the calls of pyCallRes in comment-blanked source, skipping
// matches inside string literals.
func pyCalls(clean string, strs []pyString) []pyCall {
	var out []pyCall
	inString := func(off int) bool {
		for _, s := range strs {
			if off >= s.Start && off < s.End {
				return true
			}
		}
		return false
	}
	for _, c := range pyCallRes {
		for _, m := range c.re.FindAllStringIndex(clean, -1) {
			if inString(m[0]) {
				continue
			}
			out = append(out, pyCall{Kind: c.kind, Open: m[1] - 1, Line: lineAt(clean, m[0])})
		}
	}
	return out
}

// secondArg finds the literal second argument of a call (DeltaTable.forName(spark, "t")
// or dbutils.secrets.get("scope", "key")).
func secondArg(src string, strs []pyString, open int) (pyString, bool) {
	depth := 0
	for i := open + 1; i < len(src); i++ {
		inStr := false
		for _, s := range strs {
			if i >= s.Start && i < s.End {
				i = s.End - 1
				inStr = true
				break
			}
		}
		if inStr {
			continue
		}
		switch src[i] {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth == 0 {
				return pyString{}, false
			}
			depth--
		case ',':
			if depth == 0 {
				return literalArg(src, strs, i)
			}
		}
	}
	return pyString{}, false
}
