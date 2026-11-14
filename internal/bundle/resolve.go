package bundle

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Options select what Resolve resolves.
type Options struct {
	// Target is the bundle target; empty picks the target with default: true,
	// or the only target.
	Target string
	// Vars are --var name=value overrides. They win over BUNDLE_VAR_* and
	// the target's values.
	Vars map[string]string
	// Env looks up environment variables (BUNDLE_VAR_*). Nil means os.LookupEnv.
	Env func(string) (string, bool)
	// CurrentUser is the Databricks user name ${workspace.current_user.*}
	// resolves to. Empty leaves those references (and the workspace paths
	// built from them) unresolved: clavesa has no Databricks login.
	CurrentUser string
	// FilesPath is what ${workspace.file_path} resolves to: the folder that
	// holds the bundle's files when the job runs. Clavesa sets it to
	// FilesPlaceholder, which the runner replaces with the real folder.
	FilesPath string
}

// FilesPlaceholder stands for ${workspace.file_path} in a job generated
// for clavesa; the runner replaces it with the folder of the bundle's
// files in that run (runner/bundle_task.py FILES_PLACEHOLDER).
const FilesPlaceholder = "/__clavesa_bundle_files__"

// Bundle is a resolved bundle for one target.
type Bundle struct {
	Root   string // absolute bundle folder
	Name   string
	Target string
	Mode   string // "", "development" or "production"
	Host   string // the target's workspace host; "" when it has none

	// Variables holds each declared variable's resolved value.
	Variables map[string]any
	// Clavesa is the value of the `clavesa` variable (empty for a target
	// that deploys to Databricks only).
	Clavesa map[string]any
	// HasClavesaVar is true when the bundle declares the `clavesa` variable.
	HasClavesaVar bool
	// Python is true when the bundle has a python: section (jobs defined in
	// code; the YAML is then not the whole job).
	Python bool

	Presets   map[string]any
	Workspace map[string]any
	Artifacts map[string]any

	// Jobs and Pipelines are the resolved resources as plain JSON-shaped
	// maps, keyed by resource key. JobFiles names the file each job was
	// defined in (relative to Root).
	Jobs      map[string]map[string]any
	Pipelines map[string]map[string]any
	JobFiles  map[string]string
}

// TargetNames lists a bundle's targets without resolving it.
func TargetNames(root string) ([]string, error) {
	files, err := loadFiles(root)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, f := range files {
		if t, ok := f.Tree["targets"].(map[string]any); ok {
			for n := range t {
				names = append(names, n)
			}
		}
	}
	sort.Strings(names)
	return names, nil
}

// Resolve reads the bundle in root and resolves it for one target.
func Resolve(root string, opts Options) (*Bundle, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	files, err := loadFiles(abs)
	if err != nil {
		return nil, err
	}

	// 1. Make relative paths bundle-root-relative, per file, then merge
	//    every file into one tree.
	tree := map[string]any{}
	jobFiles := map[string]string{}
	for _, f := range files {
		dir := path.Dir(f.Rel)
		rebasePaths(f.Tree, dir)
		if targets, ok := f.Tree["targets"].(map[string]any); ok {
			for _, t := range targets {
				if tm, ok := t.(map[string]any); ok {
					rebasePaths(tm, dir)
				}
			}
		}
		if res, ok := f.Tree["resources"].(map[string]any); ok {
			for kind, rs := range res {
				m, _ := rs.(map[string]any)
				for key := range m {
					if prevKind, ok := tree["resources"].(map[string]any); ok {
						if pm, ok := prevKind[kind].(map[string]any); ok {
							if _, dup := pm[key]; dup {
								return nil, fmt.Errorf("%s: resource %s.%s is defined twice", f.Rel, kind, key)
							}
						}
					}
					if kind == "jobs" {
						jobFiles[key] = f.Rel
					}
				}
			}
		}
		for k, v := range f.Tree {
			if k == "include" {
				continue
			}
			tree[k] = mergeValue(tree[k], v, k)
		}
	}

	// 2. Pick the target and apply its overrides.
	targets, _ := tree["targets"].(map[string]any)
	name, err := pickTarget(targets, opts.Target)
	if err != nil {
		return nil, err
	}
	target, _ := targets[name].(map[string]any)
	delete(tree, "targets")
	for k, v := range target {
		switch k {
		case "variables", "default", "mode":
			continue
		case "compute_id", "cluster_id", "git":
			continue
		default:
			tree[k] = mergeValue(tree[k], v, k)
		}
	}
	mode, _ := target["mode"].(string)

	bundleSec, _ := tree["bundle"].(map[string]any)
	if bundleSec == nil {
		bundleSec = map[string]any{}
		tree["bundle"] = bundleSec
	}
	bundleName, _ := bundleSec["name"].(string)
	if bundleName == "" {
		return nil, fmt.Errorf("databricks.yml: bundle.name is required")
	}
	bundleSec["target"] = name
	bundleSec["environment"] = name
	if mode != "" {
		bundleSec["mode"] = mode
	}

	// The workspace paths the CLI derives from root_path.
	ws, _ := tree["workspace"].(map[string]any)
	if ws == nil {
		ws = map[string]any{}
		tree["workspace"] = ws
	}
	if opts.CurrentUser != "" {
		short := opts.CurrentUser
		if i := strings.Index(short, "@"); i >= 0 {
			short = short[:i]
		}
		ws["current_user"] = map[string]any{"userName": opts.CurrentUser, "short_name": short}
		if _, ok := ws["root_path"]; !ok {
			ws["root_path"] = "/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}"
		}
	}
	if _, ok := ws["root_path"]; ok {
		for k, sub := range map[string]string{"file_path": "files", "artifact_path": "artifacts", "state_path": "state", "resource_path": "resources"} {
			if _, set := ws[k]; !set {
				ws[k] = "${workspace.root_path}/" + sub
			}
		}
	}
	if opts.FilesPath != "" {
		ws["file_path"] = opts.FilesPath
	}

	// 3. Variables: --var > BUNDLE_VAR_* > target value > default.
	vars, hasClavesa, err := resolveVariables(tree, target, opts)
	if err != nil {
		return nil, err
	}

	// 4. ${...} substitutions, to a fixed point.
	ctx := map[string]any{
		"bundle":    bundleSec,
		"workspace": tree["workspace"],
		"var":       vars,
		"resources": tree["resources"],
	}
	for i := 0; i < 10; i++ {
		changed := false
		for k, v := range vars {
			nv, c := substitute(v, ctx)
			vars[k] = nv
			changed = changed || c
		}
		for _, k := range []string{"bundle", "workspace", "resources", "artifacts", "presets", "sync"} {
			if v, ok := tree[k]; ok {
				nv, c := substitute(v, ctx)
				tree[k] = nv
				changed = changed || c
			}
		}
		ctx["bundle"] = tree["bundle"]
		ctx["workspace"] = tree["workspace"]
		ctx["resources"] = tree["resources"]
		if !changed {
			break
		}
	}

	b := &Bundle{
		Root:          abs,
		Name:          bundleName,
		Target:        name,
		Mode:          mode,
		Variables:     vars,
		HasClavesaVar: hasClavesa,
		JobFiles:      jobFiles,
		Jobs:          map[string]map[string]any{},
		Pipelines:     map[string]map[string]any{},
	}
	_, b.Python = tree["python"]
	b.Presets, _ = tree["presets"].(map[string]any)
	b.Workspace, _ = tree["workspace"].(map[string]any)
	b.Artifacts, _ = tree["artifacts"].(map[string]any)
	if b.Workspace != nil {
		b.Host, _ = b.Workspace["host"].(string)
	}
	if c, ok := vars["clavesa"].(map[string]any); ok {
		b.Clavesa = c
	}
	if res, ok := tree["resources"].(map[string]any); ok {
		if jobs, ok := res["jobs"].(map[string]any); ok {
			for k, j := range jobs {
				if jm, ok := j.(map[string]any); ok {
					b.Jobs[k] = jm
				}
			}
		}
		if pls, ok := res["pipelines"].(map[string]any); ok {
			for k, p := range pls {
				if pm, ok := p.(map[string]any); ok {
					b.Pipelines[k] = pm
				}
			}
		}
	}

	// 5. Presets and the CLI's job defaults.
	applyPresets(b)
	return b, nil
}

func pickTarget(targets map[string]any, want string) (string, error) {
	if len(targets) == 0 {
		return "", fmt.Errorf("databricks.yml has no targets")
	}
	if want != "" {
		if _, ok := targets[want]; !ok {
			return "", fmt.Errorf("no target %q in the bundle (targets: %s)", want, strings.Join(sortedKeys(targets), ", "))
		}
		return want, nil
	}
	var defaults []string
	for n, t := range targets {
		if tm, ok := t.(map[string]any); ok {
			if d, _ := tm["default"].(bool); d {
				defaults = append(defaults, n)
			}
		}
	}
	switch {
	case len(defaults) == 1:
		return defaults[0], nil
	case len(defaults) > 1:
		sort.Strings(defaults)
		return "", fmt.Errorf("more than one target has default: true (%s)", strings.Join(defaults, ", "))
	case len(targets) == 1:
		for n := range targets {
			return n, nil
		}
	}
	return "", fmt.Errorf("the bundle has several targets and none is the default: use -t (targets: %s)", strings.Join(sortedKeys(targets), ", "))
}

func resolveVariables(tree, target map[string]any, opts Options) (map[string]any, bool, error) {
	decls, _ := tree["variables"].(map[string]any)
	tvals, _ := target["variables"].(map[string]any)
	env := opts.Env
	if env == nil {
		env = os.LookupEnv
	}
	for n := range tvals {
		if _, ok := decls[n]; !ok {
			return nil, false, fmt.Errorf("target %v sets variable %q, which the bundle does not declare", target["name"], n)
		}
	}
	for n := range opts.Vars {
		if _, ok := decls[n]; !ok {
			return nil, false, fmt.Errorf("--var %s: the bundle declares no such variable", n)
		}
	}
	out := map[string]any{}
	var missing []string
	for n, d := range decls {
		dm, _ := d.(map[string]any)
		complex := dm["type"] == "complex"
		if v, ok := opts.Vars[n]; ok {
			if complex {
				return nil, false, fmt.Errorf("--var %s: a complex variable cannot be set on the command line", n)
			}
			out[n] = v
			continue
		}
		if v, ok := env("BUNDLE_VAR_" + n); ok {
			if complex {
				var parsed any
				if err := json.Unmarshal([]byte(v), &parsed); err != nil {
					return nil, false, fmt.Errorf("BUNDLE_VAR_%s: not JSON: %w", n, err)
				}
				out[n] = parsed
			} else {
				out[n] = v
			}
			continue
		}
		if tv, ok := tvals[n]; ok {
			// The long form {default: x} also works in a target.
			if tm, ok := tv.(map[string]any); ok && !complex {
				if dv, ok := tm["default"]; ok {
					out[n] = dv
					continue
				}
				if _, ok := tm["lookup"]; ok {
					return nil, false, fmt.Errorf("variable %q uses lookup, which needs a Databricks login: give it a value in the target", n)
				}
			}
			out[n] = tv
			continue
		}
		if dv, ok := dm["default"]; ok {
			out[n] = dv
			continue
		}
		if _, ok := dm["lookup"]; ok {
			return nil, false, fmt.Errorf("variable %q uses lookup, which needs a Databricks login: give it a value in the target", n)
		}
		missing = append(missing, n)
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, false, fmt.Errorf("no value for variable %s: set it in the target, with --var or with BUNDLE_VAR_", strings.Join(missing, ", "))
	}
	_, hasClavesa := decls["clavesa"]
	return out, hasClavesa, nil
}

// keyedLists are the sequences the CLI merges element by element on a key
// when a target overrides them; every other sequence is replaced.
var keyedLists = map[string]string{
	"tasks":        "task_key",
	"job_clusters": "job_cluster_key",
	"environments": "environment_key",
	"parameters":   "name",
}

// mergeValue merges src over dst. field is the key the values sit under.
func mergeValue(dst, src any, field string) any {
	if dst == nil {
		return deepCopy(src)
	}
	switch s := src.(type) {
	case map[string]any:
		d, ok := dst.(map[string]any)
		if !ok {
			return deepCopy(src)
		}
		for k, v := range s {
			d[k] = mergeValue(d[k], v, k)
		}
		return d
	case []any:
		key, keyed := keyedLists[field]
		d, ok := dst.([]any)
		if !ok {
			return deepCopy(src)
		}
		if !keyed || !allMaps(s) || !allMaps(d) {
			// The CLI appends a sequence that has no merge key.
			return append(d, deepCopy(src).([]any)...)
		}
		idx := map[string]int{}
		for i, e := range d {
			if em, ok := e.(map[string]any); ok {
				if kv, ok := em[key].(string); ok {
					idx[kv] = i
				}
			}
		}
		for _, e := range s {
			em, ok := e.(map[string]any)
			kv, _ := em[key].(string)
			if i, found := idx[kv]; ok && found {
				d[i] = mergeValue(d[i], em, "")
				continue
			}
			d = append(d, deepCopy(e))
		}
		return d
	default:
		return src
	}
}

func allMaps(l []any) bool {
	for _, e := range l {
		if _, ok := e.(map[string]any); !ok {
			return false
		}
	}
	return true
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = deepCopy(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = deepCopy(val)
		}
		return out
	default:
		return v
	}
}

var refRe = regexp.MustCompile(`\$\{([A-Za-z0-9_.\-\[\]]+)\}`)

// substitute replaces ${...} references it can resolve. A string that is
// exactly one reference takes the referenced value with its type (so a
// complex variable stays a map); references inside a longer string are
// replaced by their text. References it cannot resolve (resources ids,
// workspace.current_user without a login) are left as they are.
func substitute(v any, ctx map[string]any) (any, bool) {
	switch t := v.(type) {
	case map[string]any:
		changed := false
		for k, val := range t {
			nv, c := substitute(val, ctx)
			t[k] = nv
			changed = changed || c
		}
		return t, changed
	case []any:
		changed := false
		for i, val := range t {
			nv, c := substitute(val, ctx)
			t[i] = nv
			changed = changed || c
		}
		return t, changed
	case string:
		m := refRe.FindAllStringSubmatchIndex(t, -1)
		if len(m) == 0 {
			return t, false
		}
		if len(m) == 1 && m[0][0] == 0 && m[0][1] == len(t) {
			if val, ok := lookupRef(t[m[0][2]:m[0][3]], ctx); ok {
				return deepCopy(val), true
			}
			return t, false
		}
		changed := false
		out := refRe.ReplaceAllStringFunc(t, func(ref string) string {
			val, ok := lookupRef(ref[2:len(ref)-1], ctx)
			if !ok {
				return ref
			}
			switch x := val.(type) {
			case string:
				changed = true
				return x
			case int, int64, float64, bool:
				changed = true
				return fmt.Sprint(x)
			}
			return ref
		})
		return out, changed
	default:
		return v, false
	}
}

func lookupRef(ref string, ctx map[string]any) (any, bool) {
	parts := strings.Split(ref, ".")
	if len(parts) < 2 {
		return nil, false
	}
	// Resource ids exist only after a Databricks deploy.
	if parts[0] == "resources" && parts[len(parts)-1] == "id" {
		return nil, false
	}
	var cur any = ctx
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			// list index, e.g. tasks[0]
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	if s, ok := cur.(string); ok && refRe.MatchString(s) {
		// Not resolved yet; a later pass may resolve it.
		return nil, false
	}
	return cur, true
}

// rebasePaths makes the relative local paths in a file's resources and
// artifacts relative to the bundle root. dir is the file's folder relative
// to the root.
func rebasePaths(tree map[string]any, dir string) {
	rebase := func(m map[string]any, key string) {
		if s, ok := m[key].(string); ok && isLocalRelative(s) {
			m[key] = path.Clean(path.Join(dir, s))
		}
	}
	if arts, ok := tree["artifacts"].(map[string]any); ok {
		for _, a := range arts {
			if am, ok := a.(map[string]any); ok {
				rebase(am, "path")
			}
		}
	}
	res, _ := tree["resources"].(map[string]any)
	if jobs, ok := res["jobs"].(map[string]any); ok {
		for _, j := range jobs {
			jm, _ := j.(map[string]any)
			tasks, _ := jm["tasks"].([]any)
			for _, t := range tasks {
				if tm, ok := t.(map[string]any); ok {
					rebaseTask(tm, rebase)
				}
			}
			envs, _ := jm["environments"].([]any)
			for _, e := range envs {
				em, _ := e.(map[string]any)
				spec, _ := em["spec"].(map[string]any)
				deps, _ := spec["dependencies"].([]any)
				for i, d := range deps {
					if s, ok := d.(string); ok && (strings.HasPrefix(s, "./") || strings.HasPrefix(s, "../")) {
						deps[i] = path.Clean(path.Join(dir, s))
					}
				}
			}
		}
	}
	if pls, ok := res["pipelines"].(map[string]any); ok {
		for _, p := range pls {
			pm, _ := p.(map[string]any)
			rebase(pm, "root_path")
			libs, _ := pm["libraries"].([]any)
			for _, l := range libs {
				lm, _ := l.(map[string]any)
				for _, k := range []string{"notebook", "file"} {
					if sub, ok := lm[k].(map[string]any); ok {
						rebase(sub, "path")
					}
				}
			}
		}
	}
}

func rebaseTask(t map[string]any, rebase func(map[string]any, string)) {
	notGit := func(m map[string]any) bool { s, _ := m["source"].(string); return s != "GIT" }
	if nt, ok := t["notebook_task"].(map[string]any); ok && notGit(nt) {
		rebase(nt, "notebook_path")
	}
	if pt, ok := t["spark_python_task"].(map[string]any); ok && notGit(pt) {
		rebase(pt, "python_file")
	}
	if st, ok := t["sql_task"].(map[string]any); ok {
		if f, ok := st["file"].(map[string]any); ok && notGit(f) {
			rebase(f, "path")
		}
	}
	if dt, ok := t["dbt_task"].(map[string]any); ok && notGit(dt) {
		rebase(dt, "project_directory")
	}
	libs, _ := t["libraries"].([]any)
	for _, l := range libs {
		if lm, ok := l.(map[string]any); ok {
			for _, k := range []string{"whl", "jar", "requirements"} {
				rebase(lm, k)
			}
		}
	}
	if fe, ok := t["for_each_task"].(map[string]any); ok {
		if inner, ok := fe["task"].(map[string]any); ok {
			rebaseTask(inner, rebase)
		}
	}
}

func isLocalRelative(p string) bool {
	return p != "" && !strings.HasPrefix(p, "/") && !strings.Contains(p, "://") &&
		!strings.HasPrefix(p, "${") && !strings.HasPrefix(p, "dbfs:") && !strings.HasPrefix(p, "~")
}

// applyPresets applies the target's presets and the defaults the CLI gives
// every job: max_concurrent_runs 1, queue enabled, and UNPAUSED for a
// schedule or trigger that names no pause status.
func applyPresets(b *Bundle) {
	p := b.Presets
	pause, _ := p["trigger_pause_status"].(string)
	prefix, _ := p["name_prefix"].(string)
	tags, _ := p["tags"].(map[string]any)
	for _, j := range b.Jobs {
		stringFields(j)
		if prefix != "" {
			if n, ok := j["name"].(string); ok {
				j["name"] = prefix + n
			}
		}
		if len(tags) > 0 {
			jt, _ := j["tags"].(map[string]any)
			if jt == nil {
				jt = map[string]any{}
			}
			for k, v := range tags {
				if _, set := jt[k]; !set {
					jt[k] = v
				}
			}
			j["tags"] = jt
		}
		for _, k := range []string{"schedule", "trigger", "continuous"} {
			sec, ok := j[k].(map[string]any)
			if !ok {
				continue
			}
			if _, set := sec["pause_status"]; !set {
				if pause != "" {
					sec["pause_status"] = pause
				} else {
					sec["pause_status"] = "UNPAUSED"
				}
			}
		}
		if _, set := j["max_concurrent_runs"]; !set {
			if n, ok := p["jobs_max_concurrent_runs"]; ok {
				j["max_concurrent_runs"] = n
			} else {
				j["max_concurrent_runs"] = 1
			}
		}
		if _, set := j["queue"]; !set {
			j["queue"] = map[string]any{"enabled": true}
		}
	}
	for _, pl := range b.Pipelines {
		if prefix != "" {
			if n, ok := pl["name"].(string); ok {
				pl["name"] = prefix + n
			}
		}
	}
}

// stringFields turns numbers and bools into text in the job fields the
// schema types as strings, the way the CLI does (a YAML `default: 2` is
// "2").
func stringFields(j map[string]any) {
	str := func(v any) any {
		switch t := v.(type) {
		case int, int64, bool:
			return fmt.Sprint(t)
		case float64:
			return strconv.FormatFloat(t, 'f', -1, 64)
		}
		return v
	}
	strMap := func(m map[string]any, key string) {
		if sub, ok := m[key].(map[string]any); ok {
			for k, v := range sub {
				sub[k] = str(v)
			}
		}
	}
	strList := func(m map[string]any, key string) {
		if l, ok := m[key].([]any); ok {
			for i, v := range l {
				l[i] = str(v)
			}
		}
	}
	params, _ := j["parameters"].([]any)
	for _, p := range params {
		if pm, ok := p.(map[string]any); ok {
			pm["default"] = str(pm["default"])
		}
	}
	var task func(t map[string]any)
	task = func(t map[string]any) {
		if nt, ok := t["notebook_task"].(map[string]any); ok {
			strMap(nt, "base_parameters")
		}
		if pt, ok := t["spark_python_task"].(map[string]any); ok {
			strList(pt, "parameters")
		}
		if wt, ok := t["python_wheel_task"].(map[string]any); ok {
			strList(wt, "parameters")
			strMap(wt, "named_parameters")
		}
		if st, ok := t["sql_task"].(map[string]any); ok {
			strMap(st, "parameters")
		}
		if rt, ok := t["run_job_task"].(map[string]any); ok {
			strMap(rt, "job_parameters")
		}
		if ct, ok := t["condition_task"].(map[string]any); ok {
			ct["left"], ct["right"] = str(ct["left"]), str(ct["right"])
		}
		if fe, ok := t["for_each_task"].(map[string]any); ok {
			if inner, ok := fe["task"].(map[string]any); ok {
				task(inner)
			}
		}
	}
	tasks, _ := j["tasks"].([]any)
	for _, t := range tasks {
		if tm, ok := t.(map[string]any); ok {
			task(tm)
		}
	}
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// intOf reads a YAML/JSON number as an int.
func intOf(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case string:
		n, err := strconv.Atoi(t)
		return n, err == nil
	}
	return 0, false
}
