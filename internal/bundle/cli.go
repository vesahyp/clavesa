package bundle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// DatabricksCLI is the Databricks CLI binary; tests point it elsewhere.
var DatabricksCLI = "databricks"

// ValidateJSON runs `databricks bundle validate -o json -t <target>` in root
// and returns what it prints. The CLI exits 1 when it has no working login
// but still prints the configuration it resolved before it needed one; the
// JSON is returned with the error then, so the caller decides.
func ValidateJSON(ctx context.Context, root, target string, vars map[string]string, env []string) ([]byte, error) {
	args := []string{"bundle", "validate", "-o", "json"}
	if target != "" {
		args = append(args, "-t", target)
	}
	for k, v := range vars {
		args = append(args, "--var", k+"="+v)
	}
	cmd := exec.CommandContext(ctx, DatabricksCLI, args...)
	cmd.Dir = root
	if env != nil {
		cmd.Env = env
	}
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	if err != nil {
		msg := strings.TrimSpace(errb.String())
		if i := strings.LastIndex(msg, "Error: "); i >= 0 {
			msg = msg[i:]
		}
		return out.Bytes(), fmt.Errorf("databricks bundle validate: %v: %s", err, msg)
	}
	return out.Bytes(), nil
}

// ResolveWithCLI resolves a bundle through the Databricks CLI with the
// user's own login. Clavesa needs it for bundles with a python: section,
// whose jobs exist only after the Python code ran.
func ResolveWithCLI(ctx context.Context, root string, opts Options) (*Bundle, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	out, err := ValidateJSON(ctx, abs, opts.Target, opts.Vars, nil)
	if err != nil {
		return nil, fmt.Errorf("this bundle has a python: section, so clavesa reads it through the Databricks CLI, which needs a working login (databricks auth login): %w", err)
	}
	return FromValidateJSON(abs, out)
}

// FromValidateJSON turns the output of `databricks bundle validate -o json`
// into a Bundle, with workspace paths made bundle-root-relative again (the
// CLI rewrites them to /Workspace/.../files/...).
func FromValidateJSON(root string, data []byte) (*Bundle, error) {
	var tree map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&tree); err != nil {
		return nil, fmt.Errorf("databricks bundle validate printed no JSON: %w", err)
	}
	tree = numbersToGo(tree).(map[string]any)
	bundleSec, _ := tree["bundle"].(map[string]any)
	ws, _ := tree["workspace"].(map[string]any)
	b := &Bundle{
		Root:      root,
		Jobs:      map[string]map[string]any{},
		Pipelines: map[string]map[string]any{},
		JobFiles:  map[string]string{},
		Variables: map[string]any{},
		Workspace: ws,
	}
	b.Name, _ = bundleSec["name"].(string)
	b.Target, _ = bundleSec["target"].(string)
	b.Mode, _ = bundleSec["mode"].(string)
	b.Host, _ = ws["host"].(string)
	b.Presets, _ = tree["presets"].(map[string]any)
	b.Artifacts, _ = tree["artifacts"].(map[string]any)
	_, b.Python = tree["python"]
	if vars, ok := tree["variables"].(map[string]any); ok {
		for n, v := range vars {
			vm, _ := v.(map[string]any)
			b.Variables[n] = vm["value"]
			if n == "clavesa" {
				b.HasClavesaVar = true
				b.Clavesa, _ = vm["value"].(map[string]any)
			}
		}
	}
	filePath, _ := ws["file_path"].(string)
	res, _ := tree["resources"].(map[string]any)
	if jobs, ok := res["jobs"].(map[string]any); ok {
		for k, j := range jobs {
			jm, _ := j.(map[string]any)
			for _, drop := range []string{"deployment", "edit_mode", "format"} {
				delete(jm, drop)
			}
			tasks, _ := jm["tasks"].([]any)
			for _, t := range tasks {
				if tm, ok := t.(map[string]any); ok {
					unworkspaceTask(root, filePath, tm)
				}
			}
			b.Jobs[k] = jm
		}
	}
	if pls, ok := res["pipelines"].(map[string]any); ok {
		for k, p := range pls {
			pm, _ := p.(map[string]any)
			delete(pm, "deployment")
			libs, _ := pm["libraries"].([]any)
			for _, l := range libs {
				lm, _ := l.(map[string]any)
				for _, kind := range []string{"notebook", "file"} {
					if sub, ok := lm[kind].(map[string]any); ok {
						if s, ok := sub["path"].(string); ok {
							sub["path"] = stripWorkspace(filePath, s)
						}
					}
				}
			}
			b.Pipelines[k] = pm
		}
	}
	return b, nil
}

func unworkspaceTask(root, filePath string, t map[string]any) {
	fix := func(m map[string]any, key string) {
		if s, ok := m[key].(string); ok {
			m[key] = stripWorkspace(filePath, s)
		}
	}
	if nt, ok := t["notebook_task"].(map[string]any); ok {
		if s, ok := nt["notebook_path"].(string); ok && strings.HasPrefix(s, filePath+"/") {
			nt["notebook_path"] = notebookFile(root, stripWorkspace(filePath, s))
		}
	}
	if pt, ok := t["spark_python_task"].(map[string]any); ok {
		fix(pt, "python_file")
	}
	if st, ok := t["sql_task"].(map[string]any); ok {
		if f, ok := st["file"].(map[string]any); ok {
			fix(f, "path")
		}
	}
	libs, _ := t["libraries"].([]any)
	for _, l := range libs {
		if lm, ok := l.(map[string]any); ok {
			for _, k := range []string{"whl", "jar", "requirements"} {
				fix(lm, k)
			}
		}
	}
	if fe, ok := t["for_each_task"].(map[string]any); ok {
		if inner, ok := fe["task"].(map[string]any); ok {
			unworkspaceTask(root, filePath, inner)
		}
	}
}

func stripWorkspace(filePath, p string) string {
	if filePath != "" && strings.HasPrefix(p, filePath+"/") {
		return strings.TrimPrefix(p, filePath+"/")
	}
	return p
}

// NotebookExtensions are the notebook source file extensions, in the order
// the CLI tries them.
var NotebookExtensions = []string{".py", ".ipynb", ".sql", ".scala", ".r", ".R"}

// notebookFile puts back the file extension the CLI strips from a
// notebook path.
func notebookFile(root, rel string) string {
	if filepath.Ext(rel) != "" {
		if _, err := os.Stat(filepath.Join(root, rel)); err == nil {
			return rel
		}
	}
	for _, ext := range NotebookExtensions {
		if _, err := os.Stat(filepath.Join(root, rel+ext)); err == nil {
			return rel + ext
		}
	}
	return rel
}

func numbersToGo(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			t[k] = numbersToGo(val)
		}
		return t
	case []any:
		for i, val := range t {
			t[i] = numbersToGo(val)
		}
		return t
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return int(n)
		}
		f, _ := t.Float64()
		return f
	default:
		return v
	}
}

// DeployedJobID returns the ID of a job that `databricks bundle deploy -t
// <target>` deployed, from `databricks bundle summary -o json` run with the
// team's own Databricks login (ADR-031 section 8: the Databricks segments
// of a mixed job run in that job).
var DeployedJobID = func(ctx context.Context, root, target string, vars map[string]string, job string) (int64, error) {
	args := []string{"bundle", "summary", "-o", "json"}
	if target != "" {
		args = append(args, "-t", target)
	}
	for k, v := range vars {
		args = append(args, "--var", k+"="+v)
	}
	cmd := exec.CommandContext(ctx, DatabricksCLI, args...)
	cmd.Dir = root
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if i := strings.LastIndex(msg, "Error: "); i >= 0 {
			msg = msg[i:]
		}
		return 0, fmt.Errorf("databricks bundle summary -t %s (needs a Databricks login and the job deployed with databricks bundle deploy -t %s): %v: %s", target, target, err, msg)
	}
	var sum struct {
		Resources struct {
			Jobs map[string]struct {
				ID any `json:"id"`
			} `json:"jobs"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(out.Bytes(), &sum); err != nil {
		return 0, fmt.Errorf("databricks bundle summary printed no JSON: %w", err)
	}
	j, ok := sum.Resources.Jobs[job]
	if !ok || j.ID == nil || fmt.Sprint(j.ID) == "" {
		return 0, fmt.Errorf("job %s is not deployed to Databricks for target %s: run databricks bundle deploy -t %s first", job, target, target)
	}
	var id int64
	if _, err := fmt.Sscan(fmt.Sprint(j.ID), &id); err != nil {
		return 0, fmt.Errorf("job %s: deployed job id %v is not a number", job, j.ID)
	}
	return id, nil
}
