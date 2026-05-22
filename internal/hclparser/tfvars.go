package hclparser

import (
	"os"
	"path/filepath"
)

// parseTFVars reads terraform.tfvars and *.auto.tfvars from dir and returns
// a map of variable name → string value. Missing files are silently skipped.
func parseTFVars(dir string) (map[string]string, error) {
	vars := make(map[string]string)

	files, _ := filepath.Glob(filepath.Join(dir, "*.auto.tfvars"))
	main := filepath.Join(dir, "terraform.tfvars")
	if _, err := os.Stat(main); err == nil {
		files = append(files, main)
	}

	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		if err := parseTFVarsContent(string(data), vars); err != nil {
			return nil, err
		}
	}
	return vars, nil
}

// parseTFVarsContent parses key = "value" pairs from tfvars content using
// the existing lex() tokenizer and merges them into vars.
func parseTFVarsContent(src string, vars map[string]string) error {
	toks, err := lex(src)
	if err != nil {
		return err
	}
	p := &parser{toks: toks}
	for {
		t := p.peek()
		if t.kind == tokEOF {
			break
		}
		if t.kind != tokIdent && t.kind != tokString {
			p.next()
			continue
		}
		key := p.next().value
		if p.peek().kind != tokEquals {
			continue
		}
		p.next() // consume =
		val, err := p.parseValue()
		if err != nil || val == nil {
			continue
		}
		if s, ok := val.(string); ok {
			vars[key] = s
		}
	}
	return nil
}
