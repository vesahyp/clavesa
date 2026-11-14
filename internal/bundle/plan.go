package bundle

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Severity of a plan finding. Refuse makes the plan exit 1; Note is printed
// but not counted.
type Severity string

const (
	OK     Severity = "ok"
	Note   Severity = "note"
	Warn   Severity = "warn"
	Refuse Severity = "refuse"
)

func (s Severity) rank() int {
	switch s {
	case Note:
		return 1
	case Warn:
		return 2
	case Refuse:
		return 3
	}
	return 0
}

// Finding is one line of the plan report.
type Finding struct {
	Severity Severity `json:"severity"`
	Task     string   `json:"task,omitempty"`
	File     string   `json:"file,omitempty"`
	Line     int      `json:"line,omitempty"`
	Message  string   `json:"message"`
}

func (f Finding) where() string {
	if f.File == "" {
		return ""
	}
	if f.Line > 0 {
		return fmt.Sprintf("%s:%d  ", f.File, f.Line)
	}
	return f.File + "  "
}

// TaskPlan is where one task runs and what the plan found about it.
type TaskPlan struct {
	Key      string    `json:"task_key"`
	Type     string    `json:"type"`
	Place    string    `json:"place"` // clavesa | databricks
	Status   Severity  `json:"status"`
	Disabled bool      `json:"disabled,omitempty"`
	Reads    []string  `json:"reads,omitempty"`
	Writes   []string  `json:"writes,omitempty"`
	Findings []Finding `json:"findings,omitempty"`
}

// JobPlan is the plan of one job.
type JobPlan struct {
	Key      string     `json:"job"`
	Pipeline string     `json:"pipeline"` // the generated clavesa pipeline: <bundle>_<job>
	Schedule string     `json:"schedule,omitempty"`
	Trigger  string     `json:"trigger,omitempty"`
	Segments int        `json:"segments"`
	Compute  *Size      `json:"compute,omitempty"`
	Tasks    []TaskPlan `json:"tasks"`
	Findings []Finding  `json:"findings,omitempty"`
}

// Report is the result of `clavesa bundle plan`.
type Report struct {
	Bundle   string    `json:"bundle"`
	Target   string    `json:"target"`
	Catalog  string    `json:"catalog"`
	Shadow   bool      `json:"shadow"`
	Jobs     []JobPlan `json:"jobs"`
	Findings []Finding `json:"findings,omitempty"`
	// Libraries are the pypi requirements the runner image needs.
	Libraries []string `json:"libraries,omitempty"`
	// Secrets are the Databricks secrets the code reads.
	Secrets []SecretUse `json:"secrets,omitempty"`
	Refuse  int         `json:"refuse"`
	Warn    int         `json:"warn"`
	Ready   bool        `json:"ready"`

	settings *Settings
	jobs     []*Job
}

// Settings returns the parsed clavesa variable the report was planned with.
func (r *Report) Settings() *Settings { return r.settings }

// Job returns the typed job by key.
func (r *Report) Job(key string) *Job {
	for _, j := range r.jobs {
		if j.Key == key {
			return j
		}
	}
	return nil
}

// IsClavesaTarget reports whether the target deploys to clavesa: it sets
// the clavesa variable to something non-empty.
func (b *Bundle) IsClavesaTarget() bool { return b.HasClavesaVar && len(b.Clavesa) > 0 }

// ErrNotClavesaTarget explains why a target cannot be planned.
func notClavesaTarget(b *Bundle) error {
	if !b.HasClavesaVar {
		return fmt.Errorf(`bundle %s declares no clavesa variable. Add it to databricks.yml and set it in a target:

variables:
  clavesa:
    description: Settings for clavesa deploys (ignored by Databricks)
    type: complex
    default: {}

targets:
  aws:
    variables:
      clavesa:
        catalog: glue      # or unity`, b.Name)
	}
	return fmt.Errorf("target %s sets no clavesa settings, so it deploys to Databricks only: set the clavesa variable in the target, or pick a clavesa target with -t", b.Target)
}

// outOfScope are the task types clavesa does not run (ADR-031 section 5),
// with the reason the plan prints.
var outOfScope = map[string]string{
	"pipeline_task":             "Lakeflow pipeline: its engine exists only in Databricks",
	"spark_jar_task":            "JVM code: needs EMR Serverless, not built yet",
	"spark_submit_task":         "JVM code: needs EMR Serverless, not built yet",
	"dbt_task":                  "dbt task: not supported in v1",
	"dbt_cloud_task":            "dbt Cloud task: Databricks only",
	"dbt_platform_task":         "dbt platform task: Databricks only",
	"clean_rooms_notebook_task": "clean rooms notebook: Databricks only",
	"dashboard_task":            "dashboard task: Databricks only",
	"alert_task":                "alert task: Databricks only",
	"power_bi_task":             "Power BI task: Databricks only",
	"gen_ai_compute_task":       "GenAI compute task: Databricks only",
	"ai_function_task":          "AI function task: Databricks only",
	"ai_runtime_task":           "AI runtime task: Databricks only",
	"python_operator_task":      "Python operator task: Databricks only",
}

// ShortType is the task type the report prints: notebook_task -> notebook.
func ShortType(t string) string {
	switch t {
	case "spark_python_task":
		return "python"
	case "python_wheel_task":
		return "wheel"
	case "":
		return "none"
	}
	return strings.TrimSuffix(t, "_task")
}

var resourceJobRef = regexp.MustCompile(`^\$\{resources\.jobs\.([A-Za-z0-9_\-]+)\.id\}$`)

// Plan checks a resolved clavesa target and reports, task by task, where
// each task runs and what stops it. It makes no network call.
func Plan(b *Bundle) (*Report, error) {
	if !b.IsClavesaTarget() {
		return nil, notClavesaTarget(b)
	}
	jobs, err := b.TypedJobs()
	if err != nil {
		return nil, err
	}
	settings, fs := parseSettings(b, jobs)
	r := &Report{Bundle: b.Name, Target: b.Target, Catalog: settings.Catalog, Shadow: settings.Shadow,
		Findings: fs, settings: settings, jobs: jobs}
	if b.Mode == "development" {
		r.Findings = append(r.Findings, Finding{Severity: Refuse,
			Message: "target is mode: development, which renames jobs after the Databricks user: use a target without it for clavesa"})
	}
	if len(jobs) == 0 {
		r.Findings = append(r.Findings, Finding{Severity: Refuse, Message: "the bundle has no jobs"})
	}

	for _, j := range jobs {
		jp := JobPlan{Key: j.Key, Pipeline: PipelineName(b.Name, b.Target, j.Key)}
		planJobLevel(b, j, &jp)
		for i := range j.Tasks {
			t := &j.Tasks[i]
			tp := planTask(b, settings, jobs, j, t)
			jp.Tasks = append(jp.Tasks, tp)
		}
		sort.SliceStable(jp.Tasks, func(a, c int) bool { return taskOrder(j, jp.Tasks[a].Key) < taskOrder(j, jp.Tasks[c].Key) })
		jp.Segments = countSegments(j, jp.Tasks)
		r.Jobs = append(r.Jobs, jp)
	}
	runDetectors(b, r)
	return r, nil
}

// PipelineName is the clavesa pipeline a bundle job becomes in a target:
// <bundle>_<target>_<job>. The target is in the name so two clavesa targets
// of one bundle (shadow and mixed, say) are two pipelines in one workspace.
func PipelineName(bundle, target, job string) string {
	return bundle + "_" + nameSafe(target) + "_" + job
}

var nameUnsafeRe = regexp.MustCompile(`[^A-Za-z0-9_]+`)

// nameSafe makes a target name fit a pipeline name: aws-local -> aws_local.
func nameSafe(s string) string { return strings.ToLower(nameUnsafeRe.ReplaceAllString(s, "_")) }

func (r *Report) tally() {
	r.Refuse, r.Warn = 0, 0
	count := func(fs []Finding) {
		for _, f := range fs {
			switch f.Severity {
			case Refuse:
				r.Refuse++
			case Warn:
				r.Warn++
			}
		}
	}
	count(r.Findings)
	for ji := range r.Jobs {
		jp := &r.Jobs[ji]
		count(jp.Findings)
		for ti := range jp.Tasks {
			tp := &jp.Tasks[ti]
			count(tp.Findings)
			tp.Status = OK
			for _, f := range tp.Findings {
				if f.Severity.rank() > tp.Status.rank() && f.Severity != Note {
					tp.Status = f.Severity
				}
			}
		}
	}
	r.Ready = r.Refuse == 0
}

// AddFinding adds a finding to a task (or to the job when task is "") and
// recounts. The detectors of section 10 report through it.
func (r *Report) AddFinding(job string, f Finding) {
	for ji := range r.Jobs {
		jp := &r.Jobs[ji]
		if jp.Key != job {
			continue
		}
		if f.Task == "" {
			jp.Findings = append(jp.Findings, f)
		} else {
			for ti := range jp.Tasks {
				if jp.Tasks[ti].Key == f.Task {
					jp.Tasks[ti].Findings = append(jp.Tasks[ti].Findings, f)
				}
			}
		}
	}
	r.tally()
}

func planJobLevel(b *Bundle, j *Job, jp *JobPlan) {
	add := func(s Severity, msg string) {
		jp.Findings = append(jp.Findings, Finding{Severity: s, Message: msg})
	}
	if j.Schedule != nil {
		jp.Schedule = strings.TrimSpace(j.Schedule.QuartzCron + " " + j.Schedule.TimezoneID)
		if j.Schedule.PauseStatus == "PAUSED" {
			jp.Schedule += " (paused)"
		}
		if _, err := AWSCron(j.Schedule.QuartzCron); err != nil {
			add(Refuse, err.Error())
		}
	}
	if per, ok := j.Trigger["periodic"].(map[string]any); ok {
		n, _ := intOf(per["interval"])
		unit, _ := per["unit"].(string)
		if _, err := AWSRate(n, unit); err != nil {
			add(Refuse, err.Error())
		}
	}
	if fa, ok := j.Trigger["file_arrival"].(map[string]any); ok {
		if u, _ := fa["url"].(string); !strings.HasPrefix(u, "s3://") {
			add(Refuse, fmt.Sprintf("trigger.file_arrival url %q: clavesa watches s3:// locations only (volumes are not built yet)", u))
		}
	}
	if tu, ok := j.Trigger["table_update"]; ok && tu != nil {
		add(Refuse, "trigger.table_update is not built yet: start the job from the producing pipeline instead")
	}
	if j.Continuous != nil {
		add(Refuse, "continuous jobs are not supported in v1: use a schedule or a trigger")
	}
	if j.Trigger != nil {
		for _, k := range sortedKeys(j.Trigger) {
			switch k {
			case "pause_status":
			case "file_arrival", "table_update", "periodic", "table":
				jp.Trigger = k
			default:
				add(Refuse, fmt.Sprintf("trigger %s is not supported in v1", k))
			}
		}
		if ps, _ := j.Trigger["pause_status"].(string); ps == "PAUSED" && jp.Trigger != "" {
			jp.Trigger += " (paused)"
		}
	}
	if j.MaxConcurrentRuns > 1 {
		add(Warn, fmt.Sprintf("max_concurrent_runs %d: clavesa runs one run of a pipeline at a time", j.MaxConcurrentRuns))
	}
	if j.GitSource != nil {
		add(Refuse, "git_source: clavesa runs the files in the bundle; remove git_source for a clavesa target")
	}
	if j.RunAs != nil {
		add(Note, "run_as is Databricks only: on clavesa the job runs as the pipeline's runner role")
	}
}

func planTask(b *Bundle, s *Settings, jobs []*Job, j *Job, t *Task) TaskPlan {
	tp := TaskPlan{Key: t.TaskKey, Type: ShortType(t.Type), Place: "clavesa", Disabled: t.Disabled}
	add := func(sev Severity, msg string) {
		tp.Findings = append(tp.Findings, Finding{Severity: sev, Task: t.TaskKey, Message: msg})
	}
	if place, ok := lookupTask(s.Place, j.Key, t.TaskKey); ok && place == "databricks" {
		tp.Place = "databricks"
	}

	if t.Disabled {
		add(Note, "disabled: not run")
	}
	if t.MaxRetries != nil && (*t.MaxRetries < 0 || *t.MaxRetries > 10) {
		add(Warn, fmt.Sprintf("max_retries %d: clavesa retries a task at most 10 times", *t.MaxRetries))
	}
	switch {
	case t.Type == "":
		add(Refuse, "the task has no task type")
	case outOfScope[t.Type] != "":
		reason := outOfScope[t.Type]
		if t.Disabled {
			break
		}
		if b.Host != "" {
			tp.Place = "databricks"
			add(Note, reason+": stays on Databricks")
		} else {
			add(Refuse, reason+": needs a target with a Databricks host, or disabled: true in this target")
		}
	default:
		checkMappedTask(b, jobs, j, t, add)
	}
	if tp.Place == "databricks" && outOfScope[t.Type] == "" {
		add(Note, "placed on Databricks by clavesa.place")
	}
	return tp
}

// checkMappedTask checks a task type clavesa runs (ADR-031 section 5).
func checkMappedTask(b *Bundle, jobs []*Job, j *Job, t *Task, add func(Severity, string)) {
	switch t.Type {
	case "notebook_task":
		if t.NotebookTask.Source == "GIT" {
			add(Refuse, "source: GIT: clavesa runs the files in the bundle; drop source: GIT")
		}
	case "spark_python_task":
		needsCompute(t, add)
		if t.SparkPythonTask.Source == "GIT" {
			add(Refuse, "source: GIT: clavesa runs the files in the bundle; drop source: GIT")
		}
	case "sql_task":
		st := t.SQLTask
		switch {
		case st.Query != nil:
			add(Refuse, "sql_task query: a saved query has no file in the bundle; move the SQL into a .sql file and use file")
		case st.Dashboard != nil:
			add(Refuse, "sql_task dashboard: move the SQL into a .sql file and use file")
		case st.Alert != nil:
			add(Refuse, "sql_task alert: move the SQL into a .sql file and use file")
		case st.File == nil:
			add(Refuse, "sql_task without file")
		case st.File.Source == "GIT":
			add(Refuse, "source: GIT: clavesa runs the files in the bundle; drop source: GIT")
		}
	case "python_wheel_task":
		needsCompute(t, add)
	case "run_job_task":
		ref := fmt.Sprint(t.RunJobTask.JobID)
		if m := resourceJobRef.FindStringSubmatch(ref); m != nil {
			found := false
			for _, o := range jobs {
				if o.Key == m[1] {
					found = true
				}
			}
			if !found {
				add(Refuse, fmt.Sprintf("run_job_task names job %s, which is not in the bundle", m[1]))
			} else {
				add(Note, fmt.Sprintf("starts pipeline %s and waits", PipelineName(b.Name, b.Target, m[1])))
			}
		} else if b.Host != "" {
			add(Note, fmt.Sprintf("runs Databricks job %s (run-now) and waits", ref))
		} else {
			add(Refuse, fmt.Sprintf("run_job_task on Databricks job %s needs a target with a Databricks host", ref))
		}
	case "for_each_task":
		fe := t.ForEachTask
		if fe.Task == nil {
			add(Refuse, "for_each_task without task")
			return
		}
		if fe.Concurrency > 1 {
			add(Warn, fmt.Sprintf("for_each_task concurrency %d: v1 runs the items one after the other", fe.Concurrency))
		}
		if reason := outOfScope[fe.Task.Type]; reason != "" {
			add(Refuse, "for_each_task runs a "+ShortType(fe.Task.Type)+" task: "+reason)
			return
		}
		checkMappedTask(b, jobs, j, fe.Task, add)
	}
}

// taskOrder is the task's position in a topological order of the job
// (ties in the bundle's order), so the report reads top to bottom.
func taskOrder(j *Job, key string) int {
	order := TopoOrder(j)
	for i, k := range order {
		if k == key {
			return i
		}
	}
	return len(order)
}

// TopoOrder returns the job's task keys in dependency order; ties keep the
// order of the bundle.
func TopoOrder(j *Job) []string {
	indeg := map[string]int{}
	children := map[string][]string{}
	pos := map[string]int{}
	for i, t := range j.Tasks {
		pos[t.TaskKey] = i
		indeg[t.TaskKey] += 0
		for _, d := range t.DependsOn {
			indeg[t.TaskKey]++
			children[d.TaskKey] = append(children[d.TaskKey], t.TaskKey)
		}
	}
	var ready, out []string
	for _, t := range j.Tasks {
		if indeg[t.TaskKey] == 0 {
			ready = append(ready, t.TaskKey)
		}
	}
	for len(ready) > 0 {
		sort.SliceStable(ready, func(a, c int) bool { return pos[ready[a]] < pos[ready[c]] })
		k := ready[0]
		ready = ready[1:]
		out = append(out, k)
		for _, c := range children[k] {
			indeg[c]--
			if indeg[c] == 0 {
				ready = append(ready, c)
			}
		}
	}
	return out
}

// Segment is a run of tasks in one place (ADR-031 section 8): tasks
// joined by a dependency and placed alike.
type Segment struct {
	ID    int      `json:"id"`
	Place string   `json:"place"`
	Tasks []string `json:"tasks"`
}

// Segments splits a job's enabled tasks into segments, in an order where
// every segment comes after the segments it depends on. Tasks keep the
// job's dependency order inside a segment.
func Segments(j *Job, tasks []TaskPlan) []Segment {
	place := map[string]string{}
	for _, tp := range tasks {
		if !tp.Disabled {
			place[tp.Key] = tp.Place
		}
	}
	parent := map[string]string{}
	var find func(string) string
	find = func(k string) string {
		if parent[k] == "" || parent[k] == k {
			parent[k] = k
			return k
		}
		parent[k] = find(parent[k])
		return parent[k]
	}
	for _, t := range j.Tasks {
		if _, ok := place[t.TaskKey]; !ok {
			continue
		}
		find(t.TaskKey)
		for _, d := range t.DependsOn {
			if p, ok := place[d.TaskKey]; ok && p == place[t.TaskKey] {
				parent[find(t.TaskKey)] = find(d.TaskKey)
			}
		}
	}
	// Order the components by the dependency order of their tasks: a
	// component starts when its first task in topological order can.
	order := TopoOrder(j)
	idx := map[string]int{}
	var segs []Segment
	for _, k := range order {
		if _, ok := place[k]; !ok {
			continue
		}
		root := find(k)
		i, ok := idx[root]
		if !ok {
			i = len(segs)
			idx[root] = i
			segs = append(segs, Segment{ID: i, Place: place[k]})
		}
		segs[i].Tasks = append(segs[i].Tasks, k)
	}
	// A segment must come after every segment it depends on; reorder
	// stably until that holds (segments form a DAG).
	segOf := map[string]int{}
	for i, s := range segs {
		for _, k := range s.Tasks {
			segOf[k] = i
		}
	}
	deps := map[int]map[int]bool{}
	for _, t := range j.Tasks {
		a, ok := segOf[t.TaskKey]
		if !ok {
			continue
		}
		for _, d := range t.DependsOn {
			if b, ok := segOf[d.TaskKey]; ok && b != a {
				if deps[a] == nil {
					deps[a] = map[int]bool{}
				}
				deps[a][b] = true
			}
		}
	}
	var out []Segment
	done := map[int]bool{}
	for len(out) < len(segs) {
		progressed := false
		for i, s := range segs {
			if done[i] {
				continue
			}
			ready := true
			for d := range deps[i] {
				if !done[d] {
					ready = false
				}
			}
			if ready {
				done[i] = true
				s.ID = len(out)
				out = append(out, s)
				progressed = true
				break
			}
		}
		if !progressed {
			break // a cycle cannot happen in a valid job; keep what we have
		}
	}
	return out
}

func countSegments(j *Job, tasks []TaskPlan) int {
	return len(Segments(j, tasks))
}

// Text renders the report the way `clavesa bundle plan` prints it.
func (r *Report) Text() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "bundle %s, target %s (catalog: %s", r.Bundle, r.Target, r.Catalog)
	if r.Shadow {
		sb.WriteString(", shadow")
	}
	sb.WriteString(")\n")
	for _, f := range r.Findings {
		fmt.Fprintf(&sb, "  %-6s %s%s\n", f.Severity, f.where(), f.Message)
	}
	for _, jp := range r.Jobs {
		var head []string
		if jp.Schedule != "" {
			head = append(head, "schedule: "+jp.Schedule)
		}
		if jp.Trigger != "" {
			head = append(head, "trigger: "+jp.Trigger)
		}
		seg := "segments"
		if jp.Segments == 1 {
			seg = "segment"
		}
		head = append(head, fmt.Sprintf("%d %s", jp.Segments, seg))
		if jp.Compute != nil {
			head = append(head, jp.Compute.String())
		}
		fmt.Fprintf(&sb, "\n%s  (%s)\n", jp.Key, strings.Join(head, ", "))
		for _, f := range jp.Findings {
			fmt.Fprintf(&sb, "  %-13s %-10s %-11s %-6s %s%s\n", "(job)", "", "", f.Severity, f.where(), f.Message)
		}
		for _, tp := range jp.Tasks {
			if len(tp.Findings) == 0 {
				fmt.Fprintf(&sb, "  %-13s %-10s %-11s %s\n", tp.Key, tp.Type, tp.Place, tp.Status)
				continue
			}
			for i, f := range tp.Findings {
				status := string(f.Severity)
				if f.Severity == Note {
					status = string(tp.Status)
				}
				if i == 0 {
					fmt.Fprintf(&sb, "  %-13s %-10s %-11s %-6s %s%s\n", tp.Key, tp.Type, tp.Place, status, f.where(), f.Message)
				} else {
					fmt.Fprintf(&sb, "  %-13s %-10s %-11s %-6s %s%s\n", "", "", "", status, f.where(), f.Message)
				}
			}
		}
	}
	for _, jp := range r.Jobs {
		var lines []string
		for _, tp := range jp.Tasks {
			if len(tp.Reads)+len(tp.Writes) == 0 {
				continue
			}
			l := fmt.Sprintf("  %-13s", tp.Key)
			if len(tp.Writes) > 0 {
				l += " writes " + strings.Join(tp.Writes, ", ")
			}
			if len(tp.Reads) > 0 {
				if len(tp.Writes) > 0 {
					l += ";"
				}
				l += " reads " + strings.Join(tp.Reads, ", ")
			}
			lines = append(lines, l)
		}
		if len(lines) > 0 {
			fmt.Fprintf(&sb, "\nTables of %s:\n%s\n", jp.Key, strings.Join(lines, "\n"))
		}
	}
	if len(r.Libraries) > 0 {
		fmt.Fprintf(&sb, "\nLibraries (runner requirements): %s\n", strings.Join(r.Libraries, ", "))
	}
	if len(r.Secrets) > 0 {
		sb.WriteString("\nSecrets (put each in AWS Secrets Manager once; clavesa never copies them):\n")
		for _, s := range r.Secrets {
			fmt.Fprintf(&sb, "  %-24s -> %s  (%s)\n", s.Name, s.SecretsManager, strings.Join(s.Tasks, ", "))
		}
	}
	ready := "yes"
	if !r.Ready {
		ready = "no"
	}
	fmt.Fprintf(&sb, "\nReady: %s.  %d refuse, %d warn.\n", ready, r.Refuse, r.Warn)
	return sb.String()
}

// needsCompute refuses a Python task with no cluster and no environment,
// as databricks bundle validate does.
func needsCompute(t *Task, add func(Severity, string)) {
	if t.JobClusterKey == "" && t.EnvironmentKey == "" && t.ExistingClusterID == "" && t.NewCluster == nil {
		add(Refuse, "the task needs a cluster or an environment (job_cluster_key, environment_key, existing_cluster_id or new_cluster), as Databricks requires")
	}
}
