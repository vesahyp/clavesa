package bundle

import (
	_ "embed"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// The plan detectors of ADR-031 section 10: they read the code each task
// points at and report what cannot run outside Databricks, the tables each
// task reads and writes, the secrets and the libraries.

//go:embed databricks_only.txt
var databricksOnlyFile string

type dbxOnly struct {
	Kind    string // function | syntax
	Pattern string
	Fix     string
	Example string
	re      *regexp.Regexp
}

var databricksOnly = parseDatabricksOnly(databricksOnlyFile)

func parseDatabricksOnly(s string) []dbxOnly {
	var out []dbxOnly
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, " | ")
		if len(f) != 4 {
			panic("databricks_only.txt: want 4 fields: " + line)
		}
		d := dbxOnly{Kind: f[0], Pattern: f[1], Fix: f[2], Example: f[3]}
		switch d.Kind {
		case "function":
			name := regexp.QuoteMeta(strings.TrimSuffix(d.Pattern, "*"))
			if strings.HasSuffix(d.Pattern, "*") {
				name += `\w*`
			}
			d.re = regexp.MustCompile(`(?i)(?:^|[^\w.])(` + name + `)\s*\(`)
		case "syntax":
			d.re = regexp.MustCompile(`(?is)` + d.Pattern)
		default:
			panic("databricks_only.txt: unknown kind " + d.Kind)
		}
		out = append(out, d)
	}
	return out
}

// sqlFindings checks SQL text against the Databricks-only list. line0 is
// the file line of the text's first line.
func sqlFindings(text, file string, line0 int) []Finding {
	clean := blankSQL(text)
	var out []Finding
	for _, d := range databricksOnly {
		for _, m := range d.re.FindAllStringIndex(clean, -1) {
			what := strings.Join(strings.Fields(clean[m[0]:m[1]]), " ")
			what = strings.TrimLeft(what, " ,(=")
			what = strings.TrimSuffix(what, "(")
			out = append(out, Finding{Severity: Refuse, File: file, Line: line0 + lineAt(clean, m[0]) - 1,
				Message: fmt.Sprintf("%s: %s", strings.TrimSpace(what), d.Fix)})
		}
	}
	return out
}

// pythonConstructs are the Python constructs the plan refuses.
var pythonConstructs = []struct {
	re      *regexp.Regexp
	strings bool // look inside string literals too
	msg     string
}{
	{regexp.MustCompile(`(?m)^\s*(?:import\s+dlt\b|from\s+dlt\s+import\b)`), false, "import dlt: Lakeflow pipeline code runs only in Databricks"},
	{regexp.MustCompile(`(?m)^\s*(?:import\s+pyspark\.pipelines\b|from\s+pyspark\.pipelines\b|from\s+pyspark\s+import\s+pipelines\b)`), false, "pyspark.pipelines: Lakeflow pipeline code runs only in Databricks"},
	{regexp.MustCompile(`cloudFiles`), true, "Auto Loader (cloudFiles): use a clavesa s3 source on the task, or spark.read over the prefix"},
	{regexp.MustCompile(`\bdbutils\s*\.\s*notebook\s*\.\s*run\s*\(`), false, "dbutils.notebook.run: rewrite as %run (same code) or as a run_job_task (separate job)"},
	{regexp.MustCompile(`\bdbutils\s*\.\s*library\s*\.`), false, "dbutils.library: move the package to the task's libraries"},
	{regexp.MustCompile(`\bdbfs:/`), true, "dbfs: path: DBFS exists only in Databricks; use an s3:// path or a volume"},
	{regexp.MustCompile(`(?:^|[^\w])/dbfs/`), true, "/dbfs/ path: DBFS exists only in Databricks; use an s3:// path or a volume"},
	{regexp.MustCompile(`(?m)^\s*(?:from\s+databricks\.connect\b|import\s+databricks\.connect\b)|\bDatabricksSession\b`), false, "Databricks Connect: on clavesa the task runs in the runner's Spark; use SparkSession.builder.getOrCreate()"},
}

// taskScan is what the detectors found in one task's code.
type taskScan struct {
	reads, writes []TableRef
	unseen        []Finding       // table calls with names built at run time
	secrets       []string        // scope/key
	views         map[string]bool // temp views the code creates
	findings      []Finding
	pypi          []string
}

// scanPythonCode runs the Python detectors on one Python text.
var tempViewPyRe = regexp.MustCompile(`\.\s*(?:createOrReplaceTempView|createTempView|createOrReplaceGlobalTempView|createGlobalTempView|registerTempTable)\s*\(`)

func scanPythonCode(src, file string, line0 int, cat, sch string, ts *taskScan) {
	clean, strs := scanPython(src)
	if ts.views == nil {
		ts.views = map[string]bool{}
	}
	for _, m := range tempViewPyRe.FindAllStringIndex(clean, -1) {
		if s, ok := literalArg(src, strs, m[1]-1); ok {
			ts.views[strings.ToLower(s.Value)] = true
		}
	}
	line := func(off int) int { return line0 + lineAt(src, off) - 1 }
	for _, c := range pythonConstructs {
		text := clean
		if !c.strings {
			text = blankStrings(clean, strs)
		}
		for _, m := range c.re.FindAllStringIndex(text, -1) {
			ts.findings = append(ts.findings, Finding{Severity: Refuse, File: file, Line: line(m[0]), Message: c.msg})
		}
	}
	for _, call := range pyCalls(clean, strs) {
		switch call.Kind {
		case "read", "write":
			s, ok := literalArg(src, strs, call.Open)
			if !ok {
				ts.unseen = append(ts.unseen, Finding{File: file, Line: line(call.Open),
					Message: "a table name built at run time"})
				continue
			}
			ref, ok := resolveName(s.Value, cat, sch)
			ref.File, ref.Line = file, line(call.Open)
			if !ok {
				ts.unseen = append(ts.unseen, Finding{File: file, Line: ref.Line, Message: "table " + s.Value + " has no catalog (use catalog.schema.table)"})
				continue
			}
			if call.Kind == "read" {
				ts.reads = append(ts.reads, ref)
			} else {
				ts.writes = append(ts.writes, ref)
			}
		case "deltatable":
			s, ok := secondArg(src, strs, call.Open)
			if !ok {
				ts.unseen = append(ts.unseen, Finding{File: file, Line: line(call.Open), Message: "a table name built at run time"})
				continue
			}
			ref, ok := resolveName(s.Value, cat, sch)
			ref.File, ref.Line = file, line(call.Open)
			if ok {
				// DeltaTable.forName is used to MERGE or UPDATE: a write.
				ts.writes = append(ts.writes, ref)
			}
		case "sql":
			s, ok := literalArg(src, strs, call.Open)
			if !ok {
				ts.unseen = append(ts.unseen, Finding{File: file, Line: line(call.Open), Message: "spark.sql with SQL built at run time"})
				continue
			}
			l0 := line(s.Start)
			ts.findings = append(ts.findings, sqlFindings(s.Value, file, l0)...)
			r, w, un := sqlTables(s.Value, cat, sch, ts.views)
			addRefs(&ts.reads, r, file, l0)
			addRefs(&ts.writes, w, file, l0)
			for _, u := range un {
				ts.unseen = append(ts.unseen, Finding{File: file, Line: l0 + u.Line - 1, Message: "table " + u.Name + " has no catalog (use catalog.schema.table)"})
			}
		case "secret":
			scope, ok1 := literalArg(src, strs, call.Open)
			key, ok2 := secondArg(src, strs, call.Open)
			if !ok1 || !ok2 {
				ts.findings = append(ts.findings, Finding{Severity: Warn, File: file, Line: line(call.Open),
					Message: "dbutils.secrets.get with a name built at run time: the plan cannot list the secret"})
				continue
			}
			ts.secrets = append(ts.secrets, scope.Value+"/"+key.Value)
		}
	}
}

func blankStrings(clean string, strs []pyString) string {
	b := []byte(clean)
	for _, s := range strs {
		for i := s.Start; i < s.End && i < len(b); i++ {
			if b[i] != '\n' {
				b[i] = ' '
			}
		}
	}
	return string(b)
}

func addRefs(dst *[]TableRef, refs []TableRef, file string, line0 int) {
	for _, r := range refs {
		r.File, r.Line = file, line0+r.Line-1
		*dst = append(*dst, r)
	}
}

// resolveName makes a table name three-level with the current catalog and
// schema.
func resolveName(name, cat, sch string) (TableRef, bool) {
	parts := splitName(name)
	switch {
	case len(parts) == 3:
		return TableRef{Name: strings.Join(parts, ".")}, true
	case len(parts) == 2 && cat != "":
		return TableRef{Name: cat + "." + parts[0] + "." + parts[1]}, true
	case len(parts) == 1 && cat != "" && sch != "":
		return TableRef{Name: cat + "." + sch + "." + parts[0]}, true
	}
	return TableRef{Name: name}, false
}

var mustacheRe = regexp.MustCompile(`\{\{\s*([A-Za-z_][\w.]*)\s*\}\}`)

// substituteMustache applies the legacy {{name}} parameter syntax of a SQL
// file with the task parameters whose values are known at plan time (no
// {{...}} reference of their own), the way the runner does at run time.
func substituteMustache(src string, params StringMap) string {
	return mustacheRe.ReplaceAllStringFunc(src, func(m string) string {
		name := mustacheRe.FindStringSubmatch(m)[1]
		if v, ok := params[name]; ok && !strings.Contains(v, "{{") {
			// Databricks puts the value in as a quoted string literal.
			return "'" + strings.NewReplacer(`\`, `\\`, "'", `\'`).Replace(v) + "'"
		}
		return m
	})
}

// scanSQLFile runs the SQL detectors on a .sql file.
func scanSQLFile(src, file, cat, sch string, ts *taskScan) {
	ts.findings = append(ts.findings, sqlFindings(src, file, 1)...)
	r, w, un := sqlTables(src, cat, sch, ts.views)
	addRefs(&ts.reads, r, file, 1)
	addRefs(&ts.writes, w, file, 1)
	for _, u := range un {
		ts.unseen = append(ts.unseen, Finding{File: file, Line: u.Line, Message: "table " + u.Name + " has no catalog (use catalog.schema.table)"})
	}
}

// scanNotebook runs the detectors on a notebook and the notebooks it %runs.
func scanNotebook(root, rel string, ts *taskScan, seen map[string]bool) {
	if seen[rel] {
		ts.findings = append(ts.findings, Finding{Severity: Refuse, File: rel, Message: "%run cycle"})
		return
	}
	seen[rel] = true
	nb, err := ParseNotebook(root, rel)
	if err != nil {
		ts.findings = append(ts.findings, Finding{Severity: Refuse, File: rel, Message: err.Error()})
		return
	}
	if nb.Language != "python" && nb.Language != "sql" {
		ts.findings = append(ts.findings, Finding{Severity: Refuse, File: rel, Message: nb.Language + " notebooks are not supported: clavesa runs Python and SQL"})
		return
	}
	cat, sch := "", ""
	for _, c := range nb.Cells {
		switch c.Lang {
		case "md":
		case "python":
			scanPythonCode(c.Body, rel, c.Line, cat, sch, ts)
		case "sql":
			ts.findings = append(ts.findings, sqlFindings(c.Body, rel, c.Line)...)
			// USE in a cell carries over to the next cells.
			for _, st := range SQLStatements(c.Body) {
				if m := useCatRe.FindStringSubmatch(blankSQL(st)); m != nil {
					cat = unquote(m[1])
				}
			}
			if ts.views == nil {
				ts.views = map[string]bool{}
			}
			for _, m := range tempViewRe.FindAllStringSubmatch(blankSQL(c.Body), -1) {
				ts.views[strings.ToLower(unquote(m[1]))] = true
			}
			r, w, un := sqlTables(c.Body, cat, sch, ts.views)
			addRefs(&ts.reads, r, rel, c.Line)
			addRefs(&ts.writes, w, rel, c.Line)
			for _, u := range un {
				ts.unseen = append(ts.unseen, Finding{File: rel, Line: c.Line + u.Line - 1, Message: "table " + u.Name + " has no catalog (use catalog.schema.table)"})
			}
		case "scala", "r":
			ts.findings = append(ts.findings, Finding{Severity: Refuse, File: rel, Line: c.MagicLine,
				Message: "%" + c.Magic + ": clavesa runs Python and SQL cells only"})
		case "magic":
			switch c.Magic {
			case "run":
				target, err := RunTarget(root, rel, c.Args)
				if err != nil {
					ts.findings = append(ts.findings, Finding{Severity: Refuse, File: rel, Line: c.MagicLine, Message: err.Error()})
					continue
				}
				scanNotebook(root, target, ts, seen)
			case "pip", "conda":
				fix := "move the package to the task's libraries"
				if pk := pipPackages(c.Args); len(pk) > 0 {
					fix = "move " + strings.Join(pk, ", ") + " to the task's libraries (pypi)"
				}
				ts.findings = append(ts.findings, Finding{Severity: Refuse, File: rel, Line: c.MagicLine,
					Message: "%" + c.Magic + " " + c.Args + ": " + fix})
			case "fs":
				// served by dbutils.fs (ADR-031 section 5)
			default:
				ts.findings = append(ts.findings, Finding{Severity: Refuse, File: rel, Line: c.MagicLine,
					Message: "%" + c.Magic + ": this magic runs only in Databricks"})
			}
		}
	}
}

// pipPackages returns the requirement specs of a %pip install line.
func pipPackages(args string) []string {
	f := strings.Fields(args)
	if len(f) == 0 || f[0] != "install" {
		return nil
	}
	var out []string
	for _, a := range f[1:] {
		if strings.HasPrefix(a, "-") {
			continue
		}
		out = append(out, a)
	}
	return out
}

// scanFile reads a bundle file for a detector, or reports it missing.
func readBundleFile(root, rel string) (string, *Finding) {
	b, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		return "", &Finding{Severity: Refuse, File: rel, Message: "the file is not in the bundle"}
	}
	return string(b), nil
}

// wheelSources finds the Python files of the package a wheel task runs:
// the artifact folder that holds the package.
func wheelSources(b *Bundle, pkg string) []string {
	var dirs []string
	for _, a := range sortedKeys(b.Artifacts) {
		am, _ := b.Artifacts[a].(map[string]any)
		p, _ := am["path"].(string)
		if p == "" {
			continue
		}
		for _, cand := range []string{path.Join(p, pkg), path.Join(p, "src", pkg)} {
			if st, err := os.Stat(filepath.Join(b.Root, cand)); err == nil && st.IsDir() {
				dirs = append(dirs, cand)
			}
		}
	}
	var files []string
	for _, d := range dirs {
		_ = filepath.WalkDir(filepath.Join(b.Root, d), func(p string, e os.DirEntry, err error) error {
			if err == nil && !e.IsDir() && strings.HasSuffix(p, ".py") {
				rel, _ := filepath.Rel(b.Root, p)
				files = append(files, filepath.ToSlash(rel))
			}
			return nil
		})
	}
	sort.Strings(files)
	return files
}

// scanTask runs the code detectors for one task.
func scanTask(b *Bundle, t *Task) *taskScan {
	ts := &taskScan{}
	switch t.Type {
	case "notebook_task":
		scanNotebook(b.Root, t.NotebookTask.NotebookPath, ts, map[string]bool{})
	case "spark_python_task":
		if src, f := readBundleFile(b.Root, t.SparkPythonTask.PythonFile); f != nil {
			ts.findings = append(ts.findings, *f)
		} else {
			scanPythonCode(src, t.SparkPythonTask.PythonFile, 1, "", "", ts)
		}
	case "sql_task":
		if t.SQLTask.File != nil {
			if src, f := readBundleFile(b.Root, t.SQLTask.File.Path); f != nil {
				ts.findings = append(ts.findings, *f)
			} else {
				scanSQLFile(substituteMustache(src, t.SQLTask.Parameters), t.SQLTask.File.Path, "", "", ts)
			}
		}
	case "python_wheel_task":
		files := wheelSources(b, t.PythonWheelTask.PackageName)
		if len(files) == 0 {
			hasWhl := false
			for _, l := range t.Libraries {
				hasWhl = hasWhl || l.Whl != ""
			}
			if !hasWhl {
				ts.findings = append(ts.findings, Finding{Severity: Refuse,
					Message: "package " + t.PythonWheelTask.PackageName + ": no artifact builds it and no whl library names it"})
			} else {
				ts.findings = append(ts.findings, Finding{Severity: Note,
					Message: "the wheel's source is not in the bundle, so the plan cannot check its code; list its tables in clavesa.tables"})
			}
		}
		for _, f := range files {
			src, _ := readBundleFile(b.Root, f)
			scanPythonCode(src, f, 1, "", "", ts)
		}
	case "for_each_task":
		if t.ForEachTask.Task != nil {
			return scanTask(b, t.ForEachTask.Task)
		}
	}
	ts.sortFindings()
	return ts
}

func (ts *taskScan) sortFindings() {
	sort.SliceStable(ts.findings, func(i, j int) bool {
		if ts.findings[i].File != ts.findings[j].File {
			return ts.findings[i].File < ts.findings[j].File
		}
		return ts.findings[i].Line < ts.findings[j].Line
	})
}
