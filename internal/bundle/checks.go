package bundle

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/vesahyp/clavesa/internal/pricing"
)

// RunnerSparkVersion is the Spark version of the clavesa runner.
const RunnerSparkVersion = "4.0.2"

// dbrSpark maps a Databricks Runtime major.minor to its Spark major.minor.
var dbrSpark = map[string]string{
	"13.3": "3.4", "14.3": "3.5", "15.4": "3.5", "16.0": "3.5", "16.1": "3.5", "16.2": "3.5",
	"16.3": "3.5", "16.4": "3.5", "17.0": "4.0", "17.1": "4.0", "17.2": "4.0", "17.3": "4.0",
}

var dbrRe = regexp.MustCompile(`^(\d+\.\d+)\.`)

// Size is a compute size.
type Size struct {
	Kind     string  `json:"kind"` // lambda | fargate
	VCPU     float64 `json:"vcpu"`
	MemoryGB float64 `json:"memory_gb"`
}

func (s Size) String() string {
	if s.Kind == "lambda" {
		return fmt.Sprintf("Lambda %d MB", int(s.MemoryGB*1024+0.5))
	}
	return fmt.Sprintf("Fargate %g vCPU / %g GB", s.VCPU, s.MemoryGB)
}

// ServerlessSize is the Fargate size of a task on serverless compute or a
// SQL warehouse: no cluster says how big, so the small size that fits most
// jobs; clavesa.compute overrides it.
var ServerlessSize = Size{Kind: "fargate", VCPU: 4, MemoryGB: 16}

// LambdaMaxMemoryGB is the most memory a Lambda function has.
const LambdaMaxMemoryGB = 10

// LambdaDefaultMemoryGB is the runner Lambda's memory when the job asks
// for none: 3008 MB, the per-function quota of a new AWS account.
const LambdaDefaultMemoryGB = 3008.0 / 1024

// runDetectors runs every detector of section 10 over the planned bundle and
// adds the findings to the report.
func runDetectors(b *Bundle, r *Report) {
	s := r.settings
	type writer struct{ job, task string }
	writers := map[string][]writer{}
	secrets := map[string][]string{}
	pypi := map[string]map[string][]string{} // package -> spec -> tasks

	for ji := range r.Jobs {
		jp := &r.Jobs[ji]
		j := r.Job(jp.Key)
		var jobSize Size
		allLambda, sumTimeouts, allTimeouts := true, 0, true
		for ti := range jp.Tasks {
			tp := &jp.Tasks[ti]
			t := taskByKey(j, tp.Key)
			if t == nil || tp.Disabled || tp.Place != "clavesa" {
				continue
			}
			add := func(f Finding) {
				f.Task = tp.Key
				tp.Findings = append(tp.Findings, f)
			}

			// Code: Databricks-only SQL and Python, tables, secrets.
			ts := scanTask(b, t)
			for _, f := range ts.findings {
				add(f)
			}
			listed, hasList := lookupTask(s.Tables, j.Key, t.TaskKey)
			if len(ts.unseen) > 0 && !hasList {
				for _, u := range ts.unseen {
					u.Severity = Refuse
					u.Message += ": list the task's tables in clavesa.tables." + t.TaskKey + " (reads, writes)"
					add(u)
				}
			}
			tp.Reads = names(ts.reads)
			tp.Writes = names(ts.writes)
			if hasList {
				tp.Reads = mergeNames(tp.Reads, listed.Reads)
				tp.Writes = mergeNames(tp.Writes, listed.Writes)
			}
			for _, w := range tp.Writes {
				writers[strings.ToLower(w)] = append(writers[strings.ToLower(w)], writer{j.Key, t.TaskKey})
			}
			for _, sec := range ts.secrets {
				secrets[sec] = appendUnique(secrets[sec], j.Key+"."+t.TaskKey)
			}

			// {{...}} references.
			for _, f := range checkRefs(j, t) {
				add(f)
			}

			// Compute.
			size, fs := taskSize(j, t)
			for _, f := range fs {
				add(f)
			}
			if cs, ok := lookupTask(s.Compute, j.Key, t.TaskKey); ok {
				size = Size{Kind: cs.Kind, VCPU: cs.CPU, MemoryGB: cs.MemoryGB}
				if cs.Kind == "fargate" {
					if cpu, mem, ok := pricing.FargateSize(cs.CPU, cs.MemoryGB); ok {
						size.VCPU, size.MemoryGB = cpu, mem
					} else {
						add(Finding{Severity: Refuse, Message: fmt.Sprintf("clavesa.compute %g vCPU / %g GB is above the Fargate maximum (16 vCPU, 120 GB)", cs.CPU, cs.MemoryGB)})
					}
				}
				if cs.Kind == "lambda" && cs.MemoryGB > LambdaMaxMemoryGB {
					add(Finding{Severity: Refuse, Message: fmt.Sprintf("clavesa.compute lambda with %g GB: Lambda has at most 10 GB", cs.MemoryGB)})
				}
			} else {
				allLambda = false
			}
			if size.VCPU > jobSize.VCPU || (size.VCPU == jobSize.VCPU && size.MemoryGB > jobSize.MemoryGB) {
				jobSize.VCPU, jobSize.MemoryGB = size.VCPU, size.MemoryGB
			}
			if t.TimeoutSeconds == 0 {
				allTimeouts = false
			}
			sumTimeouts += t.TimeoutSeconds

			// Libraries.
			for _, lib := range t.Libraries {
				switch {
				case lib.Pypi != nil:
					addPypi(pypi, lib.Pypi.Package, j.Key+"."+t.TaskKey)
				case lib.Requirements != "":
					src, f := readBundleFile(b.Root, lib.Requirements)
					if f != nil {
						add(*f)
						continue
					}
					for _, line := range strings.Split(src, "\n") {
						line = strings.TrimSpace(line)
						if line != "" && !strings.HasPrefix(line, "#") && !strings.HasPrefix(line, "-") {
							addPypi(pypi, line, j.Key+"."+t.TaskKey)
						}
					}
				case lib.Whl != "":
				case lib.Jar != "", lib.Maven != nil:
					add(Finding{Severity: Refuse, Message: "jar and maven libraries are not supported in v1 (JVM code needs EMR Serverless)"})
				default:
					add(Finding{Severity: Refuse, Message: "egg and cran libraries are not supported"})
				}
			}
			if t.EnvironmentKey != "" {
				for _, e := range j.Environments {
					if e.EnvironmentKey == t.EnvironmentKey {
						for _, d := range e.Spec.Dependencies {
							if strings.HasPrefix(d, "./") || strings.HasPrefix(d, "../") || strings.HasSuffix(d, ".whl") || strings.HasPrefix(d, "/") {
								continue
							}
							addPypi(pypi, d, j.Key+"."+t.TaskKey)
						}
					}
				}
			}
		}

		// The job's size: its largest task; Lambda only when every task
		// says so, or when the timeouts add up to 15 minutes and it fits.
		if allLambda {
			// Every task says lambda: the largest memory asked for, else
			// the runner Lambda's default.
			jobSize.Kind = "lambda"
			if jobSize.MemoryGB == 0 {
				jobSize.MemoryGB = LambdaDefaultMemoryGB
			}
		} else {
			if jobSize.VCPU == 0 {
				jobSize = ServerlessSize
			}
			jobSize.Kind = "fargate"
			if allTimeouts && sumTimeouts > 0 && sumTimeouts <= 900 && jobSize.MemoryGB <= LambdaMaxMemoryGB {
				jobSize.Kind = "lambda"
			}
		}
		if jobSize.Kind == "lambda" && jobSize.MemoryGB > LambdaMaxMemoryGB {
			jobSize.MemoryGB = LambdaMaxMemoryGB
		}
		jp.Compute = &jobSize

		// A mixed job (ADR-031 section 8): data crosses the boundary as
		// tables only, and the clavesa segments run on Lambda.
		place := map[string]string{}
		for _, tp := range jp.Tasks {
			if !tp.Disabled {
				place[tp.Key] = tp.Place
			}
		}
		mixed := false
		for _, p := range place {
			if p == "databricks" {
				mixed = true
			}
		}
		if mixed {
			for ti := range jp.Tasks {
				tp := &jp.Tasks[ti]
				t := taskByKey(j, tp.Key)
				if t == nil || tp.Disabled {
					continue
				}
				for _, m := range taskValueRefRe.FindAllStringSubmatch(rawText(t.Raw), -1) {
					if p, ok := place[m[1]]; ok && p != tp.Place {
						tp.Findings = append(tp.Findings, Finding{Severity: Refuse, Task: tp.Key,
							Message: fmt.Sprintf("{{tasks.%s.values...}} crosses from %s to %s: in a mixed job data crosses as tables only", m[1], p, tp.Place)})
					}
				}
			}
			if jobSize.Kind == "fargate" {
				jobSize.Kind = "lambda"
				if jobSize.MemoryGB > LambdaMaxMemoryGB {
					jobSize.MemoryGB = LambdaMaxMemoryGB
				}
				jp.Findings = append(jp.Findings, Finding{Severity: Warn,
					Message: "mixed job: its clavesa segments run on Lambda (10 GB, 15 minutes per segment); Fargate segments are not built yet"})
			}
		}
	}

	// No table has two writers across pipelines (ADR-031 section 6).
	for _, table := range sortedMapKeys(writers) {
		ws := writers[table]
		jobs := map[string]bool{}
		for _, w := range ws {
			jobs[w.job] = true
		}
		if len(jobs) > 1 {
			var who []string
			for _, w := range ws {
				who = append(who, w.job+"."+w.task)
			}
			r.Findings = append(r.Findings, Finding{Severity: Refuse,
				Message: fmt.Sprintf("table %s is written by %s: one table has one writing job", table, strings.Join(who, " and "))})
		}
	}

	// Libraries go into the workspace's runner requirements, so two
	// different pins of one package cannot both hold.
	for _, pkg := range sortedMapKeys(pypi) {
		specs := pypi[pkg]
		if len(specs) > 1 {
			var parts []string
			for _, spec := range sortedMapKeys(specs) {
				parts = append(parts, fmt.Sprintf("%s (%s)", spec, strings.Join(specs[spec], ", ")))
			}
			r.Findings = append(r.Findings, Finding{Severity: Refuse,
				Message: fmt.Sprintf("package %s is pinned differently: %s; one runner image holds one version", pkg, strings.Join(parts, " and "))})
		}
	}
	for _, pkg := range sortedMapKeys(pypi) {
		for _, spec := range sortedMapKeys(pypi[pkg]) {
			r.Libraries = appendUnique(r.Libraries, spec)
		}
	}
	for _, sec := range sortedMapKeys(secrets) {
		r.Secrets = append(r.Secrets, SecretUse{Name: sec, Tasks: secrets[sec],
			SecretsManager: "clavesa/<workspace>/dbx/" + sec})
	}
	r.tally()
}

// SecretUse is one Databricks secret a bundle reads, and where clavesa
// reads it from.
type SecretUse struct {
	Name           string   `json:"name"` // scope/key
	Tasks          []string `json:"tasks"`
	SecretsManager string   `json:"secrets_manager"`
}

func taskByKey(j *Job, key string) *Task {
	for i := range j.Tasks {
		if j.Tasks[i].TaskKey == key {
			return &j.Tasks[i]
		}
	}
	return nil
}

// taskSize is a task's compute size from its cluster (ADR-031 section 7),
// with the cluster findings: Photon, spark.databricks.* conf and the
// Spark version.
func taskSize(j *Job, t *Task) (Size, []Finding) {
	var fs []Finding
	var c *Cluster
	switch {
	case t.NewCluster != nil:
		c = t.NewCluster
	case t.JobClusterKey != "":
		for _, jc := range j.JobClusters {
			if jc.JobClusterKey == t.JobClusterKey {
				c = jc.NewCluster
			}
		}
		if c == nil {
			fs = append(fs, Finding{Severity: Refuse, Message: "job_cluster_key " + t.JobClusterKey + " is not in the job's job_clusters"})
			return ServerlessSize, fs
		}
	case t.ExistingClusterID != "":
		fs = append(fs, Finding{Severity: Warn, Message: "existing_cluster_id: an all-purpose cluster has no size in the bundle; the task gets " + ServerlessSize.String() + " unless clavesa.compute sets one"})
		return ServerlessSize, fs
	default:
		return ServerlessSize, nil
	}
	if c.RuntimeEngine == "PHOTON" || strings.Contains(strings.ToLower(c.SparkVersion), "photon") {
		fs = append(fs, Finding{Severity: Warn, Message: "runtime_engine PHOTON: runs without Photon, with the same results"})
	}
	for _, k := range sortedStringKeys(c.SparkConf) {
		if strings.HasPrefix(k, "spark.databricks.") {
			fs = append(fs, Finding{Severity: Warn, Message: "spark_conf " + k + " is Databricks only: dropped"})
		}
	}
	if m := dbrRe.FindStringSubmatch(c.SparkVersion); m != nil {
		spark, known := dbrSpark[m[1]]
		runner := RunnerSparkVersion[:strings.LastIndex(RunnerSparkVersion, ".")]
		if !known {
			fs = append(fs, Finding{Severity: Note, Message: "runtime " + m[1] + ": its Spark version is not in clavesa's table; clavesa runs Spark " + RunnerSparkVersion})
		} else if spark != runner {
			fs = append(fs, Finding{Severity: Warn, Message: fmt.Sprintf("runtime %s runs Spark %s; clavesa runs Spark %s: code that depends on the difference behaves differently", m[1], spark, RunnerSparkVersion)})
		}
	}
	node, ok := pricing.LookupNodeType(c.NodeTypeID)
	if !ok {
		fs = append(fs, Finding{Severity: Warn, Message: "node type " + c.NodeTypeID + " is not in clavesa's table: the task gets " + ServerlessSize.String() + " unless clavesa.compute sets one"})
		return ServerlessSize, fs
	}
	driver := node
	if c.DriverNodeID != "" {
		if d, ok := pricing.LookupNodeType(c.DriverNodeID); ok {
			driver = d
		}
	}
	workers := 0
	if c.NumWorkers != nil {
		workers = *c.NumWorkers
	}
	if c.Autoscale != nil {
		workers = c.Autoscale.MaxWorkers
	}
	cpu := driver.VCPU + float64(workers)*node.VCPU
	mem := driver.MemoryGB + float64(workers)*node.MemoryGB
	fc, fm, ok := pricing.FargateSize(cpu, mem)
	if !ok {
		fs = append(fs, Finding{Severity: Refuse, Message: fmt.Sprintf("the cluster is %g vCPU / %g GB, above the Fargate maximum (16 vCPU, 120 GB): it needs EMR Serverless, not built yet; set a smaller size in clavesa.compute if the data allows", cpu, mem)})
		return Size{Kind: "fargate", VCPU: pricing.FargateMaxVCPU, MemoryGB: pricing.FargateMaxMemoryGB}, fs
	}
	return Size{Kind: "fargate", VCPU: fc, MemoryGB: fm}, fs
}

var refRunRe = regexp.MustCompile(`\{\{\s*([^}]*?)\s*\}\}`)

var taskValueRefRe = regexp.MustCompile(`\{\{\s*tasks\.([A-Za-z0-9_\-]+)\.values\.`)

func rawText(m map[string]any) string {
	b, _ := json.Marshal(m)
	return string(b)
}

var jobRefs = map[string]bool{
	"job.id": true, "job.name": true, "job.run_id": true, "job.repair_count": true,
	"job.trigger.type": true, "task.name": true, "task.run_id": true, "task.execution_count": true,
	"job.trigger.file_arrival.location": true,
}

var timeRefRe = regexp.MustCompile(`^job\.(?:start_time|trigger\.time)\.(?:iso_date|iso_datetime|timestamp_ms|year|month|day|hour|minute|second|is_weekday|is_weekend)$`)

// checkRefs checks every {{...}} reference in a task against the list
// the handler resolves (ADR-031 section 7).
func checkRefs(j *Job, t *Task) []Finding {
	var fs []Finding
	params := map[string]bool{}
	for _, p := range j.Parameters {
		params[p.Name] = true
	}
	inForEach := false
	var walk func(v any, inner bool)
	walk = func(v any, inner bool) {
		switch x := v.(type) {
		case map[string]any:
			for _, k := range sortedKeys(x) {
				if k == "task" && x["inputs"] != nil {
					walk(x[k], true)
					continue
				}
				walk(x[k], inner)
			}
		case []any:
			for _, e := range x {
				walk(e, inner)
			}
		case string:
			for _, m := range refRunRe.FindAllStringSubmatch(x, -1) {
				if msg := refProblem(m[1], j, params, inner || inForEach); msg != "" {
					fs = append(fs, Finding{Severity: Refuse, Message: "{{" + m[1] + "}}: " + msg})
				}
			}
		}
	}
	walk(t.Raw, false)
	return fs
}

func refProblem(ref string, j *Job, params map[string]bool, inForEach bool) string {
	switch {
	case jobRefs[ref], timeRefRe.MatchString(ref):
		return ""
	case strings.HasPrefix(ref, "job.parameters."):
		if !params[strings.TrimPrefix(ref, "job.parameters.")] {
			return "the job has no parameter " + strings.TrimPrefix(ref, "job.parameters.")
		}
		return ""
	case strings.HasPrefix(ref, "secrets/"):
		return ""
	case ref == "input" || strings.HasPrefix(ref, "input."):
		if !inForEach {
			return "{{input}} works only inside a for_each_task"
		}
		return ""
	case strings.HasPrefix(ref, "tasks."):
		parts := strings.SplitN(ref, ".", 4)
		if len(parts) < 3 {
			return "not a task reference"
		}
		if taskByKey(j, parts[1]) == nil {
			return "the job has no task " + parts[1]
		}
		switch parts[2] {
		case "values":
			if len(parts) < 4 {
				return "name the value: tasks.<task>.values.<name>"
			}
			return ""
		case "result_state", "run_id", "error_code", "execution_count":
			return ""
		case "output":
			return "SQL task output is not carried in v1: write the result to a table, or set a task value"
		}
		return "not resolved by clavesa (supported: values, result_state, run_id, error_code, execution_count)"
	case strings.HasPrefix(ref, "workspace."):
		return "workspace values exist only in Databricks"
	case strings.ContainsAny(ref, "+-*/ "):
		return "date arithmetic in references is not supported in v1: compute the date in the task"
	}
	return "not a reference clavesa resolves (supported: job.*, task.*, tasks.<task>.values|result_state|run_id, input)"
}

func addPypi(m map[string]map[string][]string, spec, task string) {
	spec = strings.TrimSpace(spec)
	name := strings.ToLower(regexp.MustCompile(`^[A-Za-z0-9_.\-]+`).FindString(spec))
	name = strings.NewReplacer("_", "-", ".", "-").Replace(name)
	if name == "" {
		return
	}
	if m[name] == nil {
		m[name] = map[string][]string{}
	}
	m[name][spec] = appendUnique(m[name][spec], task)
	// A bare name next to a pinned one is the same requirement.
	if len(m[name]) > 1 {
		if _, bare := m[name][name]; bare {
			delete(m[name], name)
		}
		for s := range m[name] {
			if strings.EqualFold(s, name) {
				delete(m[name], s)
			}
		}
	}
}

func names(refs []TableRef) []string {
	var out []string
	for _, r := range refs {
		out = appendUnique(out, r.Name)
	}
	return out
}

func mergeNames(a, b []string) []string {
	for _, n := range b {
		a = appendUnique(a, n)
	}
	return a
}

func appendUnique(l []string, s string) []string {
	for _, e := range l {
		if strings.EqualFold(e, s) {
			return l
		}
	}
	return append(l, s)
}

func sortedMapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedStringKeys(m map[string]string) []string { return sortedMapKeys(m) }

var _ = strconv.Itoa
var _ = os.Stat
var _ = filepath.Join
