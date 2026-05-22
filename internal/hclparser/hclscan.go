// Package hclparser implements a minimal HCL scanner for the subset of HCL
// used in Clavesa .tf files. It does not use github.com/hashicorp/hcl/v2
// directly, instead using a hand-written tokeniser that covers exactly the
// constructs present in Clavesa module blocks.
package hclparser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// tokenKind is the class of a lexical token.
type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokString
	tokNumber
	tokBool    // true / false
	tokNull    // null
	tokEquals  // =
	tokLBrace  // {
	tokRBrace  // }
	tokLBrack  // [
	tokRBrack  // ]
	tokLParen  // (
	tokRParen  // )
	tokComma   // ,
	tokDot     // .
	tokNewline // \n (significant in HCL)
)

// token is a single lexical element.
type token struct {
	kind  tokenKind
	value string // raw text for ident / string (unquoted for strings)
	line  int
}

// lexer tokenises an HCL source string.
type lexer struct {
	src  []rune
	pos  int
	line int
	toks []token
}

func lex(src string) ([]token, error) {
	l := &lexer{src: []rune(src), line: 1}
	if err := l.run(); err != nil {
		return nil, err
	}
	return l.toks, nil
}

func (l *lexer) peek() (rune, bool) {
	if l.pos >= len(l.src) {
		return 0, false
	}
	return l.src[l.pos], true
}

func (l *lexer) next() (rune, bool) {
	if l.pos >= len(l.src) {
		return 0, false
	}
	r := l.src[l.pos]
	l.pos++
	if r == '\n' {
		l.line++
	}
	return r, true
}

func (l *lexer) run() error {
	for {
		// Skip whitespace (except newlines, which are significant).
		for {
			r, ok := l.peek()
			if !ok {
				break
			}
			if r == '\n' {
				break
			}
			if unicode.IsSpace(r) {
				l.next()
				continue
			}
			break
		}

		r, ok := l.peek()
		if !ok {
			l.toks = append(l.toks, token{kind: tokEOF, line: l.line})
			return nil
		}

		line := l.line

		switch {
		case r == '\n':
			l.next()
			l.toks = append(l.toks, token{kind: tokNewline, line: line})

		case r == '#' || (r == '/' && l.peekAt(1) == '/'):
			// Line comment — consume until newline.
			for {
				c, ok2 := l.peek()
				if !ok2 || c == '\n' {
					break
				}
				l.next()
			}

		case r == '/' && l.peekAt(1) == '*':
			// Block comment.
			l.next() // /
			l.next() // *
			for {
				c, ok2 := l.next()
				if !ok2 {
					return fmt.Errorf("unterminated block comment")
				}
				if c == '*' {
					if d, _ := l.peek(); d == '/' {
						l.next()
						break
					}
				}
			}

		case r == '<' && l.peekAt(1) == '<':
			s, err := l.lexHeredoc()
			if err != nil {
				return err
			}
			l.toks = append(l.toks, token{kind: tokString, value: s, line: line})

		case r == '"':
			s, err := l.lexString()
			if err != nil {
				return err
			}
			l.toks = append(l.toks, token{kind: tokString, value: s, line: line})

		case r == '=':
			l.next()
			l.toks = append(l.toks, token{kind: tokEquals, line: line})

		case r == '{':
			l.next()
			l.toks = append(l.toks, token{kind: tokLBrace, line: line})

		case r == '}':
			l.next()
			l.toks = append(l.toks, token{kind: tokRBrace, line: line})

		case r == '[':
			l.next()
			l.toks = append(l.toks, token{kind: tokLBrack, line: line})

		case r == ']':
			l.next()
			l.toks = append(l.toks, token{kind: tokRBrack, line: line})

		case r == '(':
			l.next()
			l.toks = append(l.toks, token{kind: tokLParen, line: line})

		case r == ')':
			l.next()
			l.toks = append(l.toks, token{kind: tokRParen, line: line})

		case r == ',':
			l.next()
			l.toks = append(l.toks, token{kind: tokComma, line: line})

		case r == '.':
			l.next()
			l.toks = append(l.toks, token{kind: tokDot, line: line})

		case unicode.IsDigit(r) || r == '-':
			n := l.lexNumber()
			l.toks = append(l.toks, token{kind: tokNumber, value: n, line: line})

		case isIdentStart(r):
			word := l.lexIdent()
			// If the identifier is immediately followed by '(', consume the
			// entire function call (including its arguments) as a single ident
			// token so callers receive the full expression, e.g. file("path").
			if next, ok := l.peek(); ok && next == '(' {
				word = word + l.lexCallArgs()
			}
			switch word {
			case "true", "false":
				l.toks = append(l.toks, token{kind: tokBool, value: word, line: line})
			case "null":
				l.toks = append(l.toks, token{kind: tokNull, value: word, line: line})
			default:
				l.toks = append(l.toks, token{kind: tokIdent, value: word, line: line})
			}

		default:
			// Skip unknown characters (e.g. > < ! etc. in constraint expressions).
			l.next()
		}
	}
}

func (l *lexer) peekAt(offset int) rune {
	pos := l.pos + offset
	if pos >= len(l.src) {
		return 0
	}
	return l.src[pos]
}

// lexHeredoc parses a <<EOF or <<-EOF heredoc.
// <<-EOF strips the indentation of the closing marker from every content line.
// <<EOF preserves all leading whitespace.
// Called after the first '<' has been peeked but not consumed.
func (l *lexer) lexHeredoc() (string, error) {
	l.next() // first <
	l.next() // second <

	// Optional - for indented heredocs (<<-).
	stripped := false
	if r, ok := l.peek(); ok && r == '-' {
		l.next()
		stripped = true
	}

	// Read terminator name up to end of line.
	var termBuf strings.Builder
	for {
		r, ok := l.peek()
		if !ok || r == '\n' {
			break
		}
		l.next()
		termBuf.WriteRune(r)
	}
	terminator := strings.TrimSpace(termBuf.String())
	if terminator == "" {
		return "", fmt.Errorf("heredoc missing terminator name")
	}

	// Consume the newline after <<[-]TERM.
	if r, ok := l.peek(); ok && r == '\n' {
		l.next()
	}

	// Collect raw lines until the terminator line.
	var lines []string
	for {
		var lineBuf strings.Builder
		hitEOF := false
		for {
			r, ok := l.peek()
			if !ok {
				hitEOF = true
				break
			}
			if r == '\n' {
				l.next()
				break
			}
			l.next()
			lineBuf.WriteRune(r)
		}
		lineStr := lineBuf.String()
		if strings.TrimSpace(lineStr) == terminator {
			// For <<-, determine strip prefix from the terminator line indentation.
			if stripped {
				strip := leadingWhitespace(lineStr)
				var out strings.Builder
				for _, ln := range lines {
					out.WriteString(strings.TrimPrefix(ln, strip))
					out.WriteRune('\n')
				}
				return out.String(), nil
			}
			break
		}
		if hitEOF {
			return "", fmt.Errorf("unterminated heredoc, expected %q", terminator)
		}
		lines = append(lines, lineStr)
	}

	// <<EOF — preserve whitespace as-is.
	var content strings.Builder
	for _, ln := range lines {
		content.WriteString(ln)
		content.WriteRune('\n')
	}
	return content.String(), nil
}

// leadingWhitespace returns the leading spaces/tabs of s.
func leadingWhitespace(s string) string {
	for i, r := range s {
		if r != ' ' && r != '\t' {
			return s[:i]
		}
	}
	return s
}

func (l *lexer) lexString() (string, error) {
	l.next() // opening "
	var sb strings.Builder
	for {
		r, ok := l.next()
		if !ok {
			return "", fmt.Errorf("unterminated string")
		}
		if r == '"' {
			break
		}
		if r == '\\' {
			esc, ok2 := l.next()
			if !ok2 {
				return "", fmt.Errorf("unterminated escape")
			}
			switch esc {
			case 'n':
				sb.WriteRune('\n')
			case 't':
				sb.WriteRune('\t')
			case '"':
				sb.WriteRune('"')
			case '\\':
				sb.WriteRune('\\')
			default:
				sb.WriteRune('\\')
				sb.WriteRune(esc)
			}
			continue
		}
		sb.WriteRune(r)
	}
	return sb.String(), nil
}

func (l *lexer) lexNumber() string {
	var sb strings.Builder
	for {
		r, ok := l.peek()
		if !ok {
			break
		}
		if unicode.IsDigit(r) || r == '.' || r == '-' || r == '+' || r == 'e' || r == 'E' {
			sb.WriteRune(r)
			l.next()
		} else {
			break
		}
	}
	return sb.String()
}

func (l *lexer) lexIdent() string {
	var sb strings.Builder
	for {
		r, ok := l.peek()
		if !ok {
			break
		}
		if isIdentContinue(r) {
			sb.WriteRune(r)
			l.next()
		} else {
			break
		}
	}
	return sb.String()
}

func isIdentStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isIdentContinue(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-'
}

// lexCallArgs consumes a balanced parenthesised argument list starting with '('
// and returns it as a string, e.g. `("transforms/filter.py")`.
// Handles nested parens and double-quoted strings inside arguments.
func (l *lexer) lexCallArgs() string {
	var sb strings.Builder
	depth := 0
	for {
		r, ok := l.peek()
		if !ok {
			break
		}
		l.next()
		sb.WriteRune(r)
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return sb.String()
			}
		case '"':
			// Consume the quoted string so inner parens don't confuse the counter.
			for {
				c, ok := l.peek()
				if !ok {
					break
				}
				l.next()
				sb.WriteRune(c)
				if c == '\\' {
					// Consume escaped char.
					if ec, ok := l.peek(); ok {
						l.next()
						sb.WriteRune(ec)
					}
					continue
				}
				if c == '"' {
					break
				}
			}
		}
	}
	return sb.String()
}

// Ensure utf8 import is used (used indirectly by rune conversions via strings).
var _ = utf8.RuneLen
