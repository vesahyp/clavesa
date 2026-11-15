package service

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// GetVars reads variable values from terraform.tfvars in dir.
func (s *Service) GetVars(dir string) (map[string]string, error) {
	abs := s.resolveDir(dir)
	vars, err := readTFVars(abs)
	if err != nil {
		return nil, err
	}
	decls, err := readVariableDecls(abs)
	if err == nil {
		for k, def := range decls {
			if _, exists := vars[k]; !exists {
				vars[k] = def
			}
		}
	}
	return vars, nil
}

// PutVars writes key/value pairs to terraform.tfvars in the pipeline directory.
func (s *Service) PutVars(dir string, vars map[string]string) error {
	abs := s.resolveDir(dir)
	existing, _ := readTFVars(abs)
	if existing == nil {
		existing = make(map[string]string)
	}
	for k, v := range vars {
		existing[k] = v
	}
	return writeTFVars(filepath.Join(abs, "terraform.tfvars"), existing)
}

func readTFVars(dir string) (map[string]string, error) {
	result := make(map[string]string)
	for _, name := range []string{"terraform.tfvars", "terraform.auto.tfvars"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			key := strings.TrimSpace(k)
			result[key] = tfvarValue(strings.TrimSpace(v))
		}
	}
	return result, nil
}

// tfvarValue reads the value side of a `key = value` tfvars line: the
// text inside the quotes of a quoted string (a trailing `# comment` or
// `// comment` dropped), or the bare value with such a comment dropped.
func tfvarValue(val string) string {
	if strings.HasPrefix(val, `"`) {
		for i := 1; i < len(val); i++ {
			switch val[i] {
			case '\\':
				i++
			case '"':
				return val[1:i]
			}
		}
		return val
	}
	for _, marker := range []string{" #", " //"} {
		if i := strings.Index(val, marker); i >= 0 {
			val = strings.TrimSpace(val[:i])
		}
	}
	return val
}

func writeTFVars(path string, vars map[string]string) error {
	var sb strings.Builder
	for k, v := range vars {
		sb.WriteString(fmt.Sprintf("%s = %q\n", k, v))
	}
	return os.WriteFile(path, []byte(sb.String()), 0o644)
}

func readVariableDecls(dir string) (map[string]string, error) {
	data, err := os.ReadFile(filepath.Join(dir, "variables.tf"))
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	var currentVar string
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "variable ") {
			parts := strings.Fields(trimmed)
			if len(parts) >= 2 {
				currentVar = strings.Trim(parts[1], `"`)
				result[currentVar] = ""
			}
			continue
		}
		if currentVar != "" && strings.HasPrefix(trimmed, "default") {
			_, val, ok := strings.Cut(trimmed, "=")
			if ok {
				v := strings.TrimSpace(val)
				if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
					v = v[1 : len(v)-1]
				}
				result[currentVar] = v
			}
		}
		if trimmed == "}" {
			currentVar = ""
		}
	}
	return result, nil
}
