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
			val := strings.TrimSpace(v)
			if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
				val = val[1 : len(val)-1]
			}
			result[key] = val
		}
	}
	return result, nil
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
