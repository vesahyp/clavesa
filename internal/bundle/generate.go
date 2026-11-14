package bundle

import (
	"fmt"
	"sort"
	"strings"
)

// JobFileName is the file in a generated bundle pipeline that holds the
// resolved job: the runner reads every task from it. Its presence marks the
// pipeline as generated from a bundle (read-only: change databricks.yml).
const JobFileName = "bundle-job.json"

// FilesDirName is the folder of a generated bundle pipeline that holds a
// copy of the bundle's files (code, SQL, notebooks, data).
const FilesDirName = "bundle"

// JobFile is the content of bundle-job.json.
type JobFile struct {
	Bundle string `json:"bundle"`
	Target string `json:"target"`
	Job    string `json:"job"`
	// Source is the bundle file the job is defined in, relative to the
	// bundle root (where a change to the job goes).
	Source     string            `json:"source,omitempty"`
	Name       string            `json:"name"`
	Catalog    string            `json:"catalog"` // glue | unity
	Catalogs   map[string]string `json:"catalogs,omitempty"`
	Shadow     bool              `json:"shadow"`
	Parameters []JobParameter    `json:"parameters,omitempty"`
	// Order is the tasks in dependency order.
	Order []string            `json:"order"`
	Tasks map[string]*JobTask `json:"tasks"`
	// Compute is the job's size from the plan.
	Compute *Size `json:"compute,omitempty"`
	// Libraries are the job's pypi requirements.
	Libraries []string `json:"libraries,omitempty"`
	// The job's schedule, trigger, notifications and health rules, for the
	// AWS deploy (ADR-031 section 7).
	Schedule    *Schedule      `json:"schedule,omitempty"`
	Trigger     map[string]any `json:"trigger,omitempty"`
	EmailNotify *EmailNotify   `json:"email_notifications,omitempty"`
	Health      *Health        `json:"health,omitempty"`
	TimeoutSec  int            `json:"timeout_seconds,omitempty"`
	// Writes are every table the job's tasks write: in shadow mode they
	// all take the shadow name, also when a later task reads them.
	Writes []string `json:"writes,omitempty"`
	// ReplacesJob is the paused Databricks job clavesa replaces after the
	// switch (clavesa.replaces_job): a run refuses to write while that
	// job is unpaused or running.
	ReplacesJob int64 `json:"replaces_job,omitempty"`
	// Segments are the runs of tasks in one place, in run order (ADR-031
	// section 8); more than one only for a mixed job.
	Segments []Segment `json:"segments,omitempty"`
	// DatabricksJobID is the job deployed by `databricks bundle deploy`
	// for this target, which runs the Databricks segments (run-now with
	// only). Set by clavesa when the job is mixed.
	DatabricksJobID int64 `json:"databricks_job_id,omitempty"`
	// Disabled are the tasks with disabled: true.
	Disabled []string `json:"disabled,omitempty"`
}

// Mixed reports whether some task runs on Databricks.
func (jf *JobFile) Mixed() bool {
	for _, s := range jf.Segments {
		if s.Place == "databricks" {
			return true
		}
	}
	return false
}

// JobTask is one task of JobFile.
type JobTask struct {
	Key      string   `json:"task_key"`
	Type     string   `json:"type"`
	Place    string   `json:"place"`
	Disabled bool     `json:"disabled,omitempty"`
	Parents  []string `json:"parents,omitempty"`
	Reads    []string `json:"reads,omitempty"`
	Writes   []string `json:"writes,omitempty"`
	Script   string   `json:"script,omitempty"` // a notebook translated to a script
	// Wheels are the wheel files the task installs, relative to the bundle
	// root (built from the bundle's artifacts).
	Wheels []string `json:"wheels,omitempty"`
	// EnvDeps are the dependencies of the task's environment.
	EnvDeps []string `json:"env_deps,omitempty"`
	// Job is the job a run_job_task starts, when it is a job of the same
	// bundle (run in the same runner on a local run).
	Job *JobFile `json:"job,omitempty"`
	// Segment is the task's segment in JobFile.Segments.
	Segment int            `json:"segment"`
	Task    map[string]any `json:"task"`
}

// BuildJobFile builds the job file of one job from a ready plan.
func BuildJobFile(b *Bundle, r *Report, jobKey string) (*JobFile, error) {
	j := r.Job(jobKey)
	if j == nil {
		var keys []string
		for _, jp := range r.Jobs {
			keys = append(keys, jp.Key)
		}
		return nil, fmt.Errorf("no job %q in the bundle (jobs: %s)", jobKey, strings.Join(keys, ", "))
	}
	var jp *JobPlan
	for i := range r.Jobs {
		if r.Jobs[i].Key == jobKey {
			jp = &r.Jobs[i]
		}
	}
	s := r.Settings()
	name := j.Name
	if name == "" {
		name = jobKey
	}
	jf := &JobFile{
		Bundle: b.Name, Target: b.Target, Job: jobKey, Name: name, Source: b.JobFiles[jobKey],
		Catalog: s.Catalog, Catalogs: s.Catalogs, Shadow: s.Shadow,
		Parameters: j.Parameters, Order: TopoOrder(j), Tasks: map[string]*JobTask{},
		Compute: jp.Compute, Libraries: r.Libraries,
		Schedule: j.Schedule, Trigger: j.Trigger, EmailNotify: j.EmailNotify, Health: j.Health,
		TimeoutSec: j.TimeoutSeconds,
	}
	for _, tp := range jp.Tasks {
		t := taskByKey(j, tp.Key)
		var parents []string
		for _, d := range t.DependsOn {
			parents = append(parents, d.TaskKey)
		}
		jt := &JobTask{
			Key: tp.Key, Type: tp.Type, Place: tp.Place, Disabled: tp.Disabled,
			Parents: parents, Reads: tp.Reads, Writes: tp.Writes, Task: t.Raw,
		}
		if t.EnvironmentKey != "" {
			for _, e := range j.Environments {
				if e.EnvironmentKey == t.EnvironmentKey {
					jt.EnvDeps = e.Spec.Dependencies
				}
			}
		}
		if t.RunJobTask != nil && !tp.Disabled {
			if m := resourceJobRef.FindStringSubmatch(fmt.Sprint(t.RunJobTask.JobID)); m != nil && m[1] != jobKey {
				child, err := BuildJobFile(b, r, m[1])
				if err != nil {
					return nil, fmt.Errorf("task %s: %w", tp.Key, err)
				}
				jt.Job = child
			}
		}
		jf.Tasks[tp.Key] = jt
		if !tp.Disabled && tp.Place == "clavesa" {
			for _, w := range tp.Writes {
				jf.Writes = appendUnique(jf.Writes, w)
			}
		}
	}
	jf.ReplacesJob = s.ReplacesJob
	jf.Segments = Segments(j, jp.Tasks)
	for _, seg := range jf.Segments {
		for _, k := range seg.Tasks {
			jf.Tasks[k].Segment = seg.ID
		}
	}
	for _, tp := range jp.Tasks {
		if tp.Disabled {
			jf.Disabled = append(jf.Disabled, tp.Key)
		}
	}
	return jf, nil
}

// MainTFMarker starts the generated part of a bundle pipeline's main.tf.
const MainTFMarker = "# --- bundle tasks: generated from databricks.yml by clavesa (ADR-031); do not edit ---"

// TaskModules renders one module block per task. moduleSource is the
// source of the task module (modules/task/aws) seen from the pipeline.
func TaskModules(jf *JobFile, moduleSource string) string {
	var sb strings.Builder
	sb.WriteString(MainTFMarker + "\n")
	keys := append([]string{}, jf.Order...)
	for k := range jf.Tasks {
		if !contains(keys, k) {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		t := jf.Tasks[k]
		parents := make([]string, 0, len(t.Parents))
		for _, p := range t.Parents {
			parents = append(parents, fmt.Sprintf("%q", p))
		}
		sort.Strings(parents)
		fmt.Fprintf(&sb, "\nmodule %q {\n  source    = %q\n  task_key  = %q\n  task_type = %q\n  place     = %q\n  parents   = [%s]\n  enabled   = %t\n}\n",
			k, moduleSource, k, t.Type, t.Place, strings.Join(parents, ", "), !t.Disabled)
	}
	return sb.String()
}
