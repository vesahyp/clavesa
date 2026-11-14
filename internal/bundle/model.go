package bundle

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// Job is the typed view of a resolved job: the fields clavesa acts on.
// Fields clavesa does not model stay in the raw map (Bundle.Jobs).
type Job struct {
	Key               string            `json:"-"`
	Name              string            `json:"name,omitempty"`
	Tasks             []Task            `json:"tasks,omitempty"`
	JobClusters       []JobCluster      `json:"job_clusters,omitempty"`
	Environments      []Environment     `json:"environments,omitempty"`
	Parameters        []JobParameter    `json:"parameters,omitempty"`
	Schedule          *Schedule         `json:"schedule,omitempty"`
	Trigger           map[string]any    `json:"trigger,omitempty"`
	Continuous        map[string]any    `json:"continuous,omitempty"`
	TimeoutSeconds    int               `json:"timeout_seconds,omitempty"`
	MaxConcurrentRuns int               `json:"max_concurrent_runs,omitempty"`
	Queue             map[string]any    `json:"queue,omitempty"`
	Health            *Health           `json:"health,omitempty"`
	EmailNotify       *EmailNotify      `json:"email_notifications,omitempty"`
	WebhookNotify     map[string]any    `json:"webhook_notifications,omitempty"`
	NotifySettings    map[string]any    `json:"notification_settings,omitempty"`
	Tags              map[string]string `json:"tags,omitempty"`
	GitSource         map[string]any    `json:"git_source,omitempty"`
	RunAs             map[string]any    `json:"run_as,omitempty"`
	Extra             map[string]any    `json:"-"`
}

// Task is one task of a job. Exactly one of the *Task fields is set.
type Task struct {
	TaskKey                string         `json:"task_key"`
	DependsOn              []Dependency   `json:"depends_on,omitempty"`
	RunIf                  string         `json:"run_if,omitempty"`
	MaxRetries             *int           `json:"max_retries,omitempty"`
	MinRetryIntervalMillis int            `json:"min_retry_interval_millis,omitempty"`
	RetryOnTimeout         bool           `json:"retry_on_timeout,omitempty"`
	TimeoutSeconds         int            `json:"timeout_seconds,omitempty"`
	JobClusterKey          string         `json:"job_cluster_key,omitempty"`
	ExistingClusterID      string         `json:"existing_cluster_id,omitempty"`
	NewCluster             *Cluster       `json:"new_cluster,omitempty"`
	EnvironmentKey         string         `json:"environment_key,omitempty"`
	Libraries              []Library      `json:"libraries,omitempty"`
	Disabled               bool           `json:"disabled,omitempty"`
	Description            string         `json:"description,omitempty"`
	EmailNotify            map[string]any `json:"email_notifications,omitempty"`
	WebhookNotify          map[string]any `json:"webhook_notifications,omitempty"`

	NotebookTask    *NotebookTask    `json:"notebook_task,omitempty"`
	SparkPythonTask *SparkPythonTask `json:"spark_python_task,omitempty"`
	PythonWheelTask *PythonWheelTask `json:"python_wheel_task,omitempty"`
	SQLTask         *SQLTask         `json:"sql_task,omitempty"`
	ConditionTask   *ConditionTask   `json:"condition_task,omitempty"`
	ForEachTask     *ForEachTask     `json:"for_each_task,omitempty"`
	RunJobTask      *RunJobTask      `json:"run_job_task,omitempty"`
	PipelineTask    map[string]any   `json:"pipeline_task,omitempty"`
	SparkJarTask    map[string]any   `json:"spark_jar_task,omitempty"`
	SparkSubmitTask map[string]any   `json:"spark_submit_task,omitempty"`
	DbtTask         map[string]any   `json:"dbt_task,omitempty"`

	// Type is the task type key (for example "notebook_task"), set by
	// decodeJob from the raw map, so the task types clavesa does not model
	// are named too.
	Type string `json:"-"`
	// Raw is the task's resolved map.
	Raw map[string]any `json:"-"`
}

type Dependency struct {
	TaskKey string `json:"task_key"`
	Outcome string `json:"outcome,omitempty"`
}

type JobCluster struct {
	JobClusterKey string   `json:"job_cluster_key"`
	NewCluster    *Cluster `json:"new_cluster,omitempty"`
}

type Cluster struct {
	SparkVersion  string            `json:"spark_version,omitempty"`
	NodeTypeID    string            `json:"node_type_id,omitempty"`
	DriverNodeID  string            `json:"driver_node_type_id,omitempty"`
	NumWorkers    *int              `json:"num_workers,omitempty"`
	Autoscale     *Autoscale        `json:"autoscale,omitempty"`
	RuntimeEngine string            `json:"runtime_engine,omitempty"`
	SparkConf     map[string]string `json:"spark_conf,omitempty"`
	SparkEnvVars  map[string]string `json:"spark_env_vars,omitempty"`
}

type Autoscale struct {
	MinWorkers int `json:"min_workers"`
	MaxWorkers int `json:"max_workers"`
}

type Environment struct {
	EnvironmentKey string `json:"environment_key"`
	Spec           struct {
		Client       string   `json:"client,omitempty"`
		Dependencies []string `json:"dependencies,omitempty"`
	} `json:"spec"`
}

type JobParameter struct {
	Name    string `json:"name"`
	Default Scalar `json:"default"`
}

type Schedule struct {
	QuartzCron  string `json:"quartz_cron_expression"`
	TimezoneID  string `json:"timezone_id"`
	PauseStatus string `json:"pause_status,omitempty"`
}

type Health struct {
	Rules []struct {
		Metric string  `json:"metric"`
		Op     string  `json:"op"`
		Value  float64 `json:"value"`
	} `json:"rules"`
}

type EmailNotify struct {
	OnStart               []string `json:"on_start,omitempty"`
	OnSuccess             []string `json:"on_success,omitempty"`
	OnFailure             []string `json:"on_failure,omitempty"`
	OnDurationWarning     []string `json:"on_duration_warning_threshold_exceeded,omitempty"`
	NoAlertForSkippedRuns bool     `json:"no_alert_for_skipped_runs,omitempty"`
}

type Library struct {
	Whl          string         `json:"whl,omitempty"`
	Jar          string         `json:"jar,omitempty"`
	Requirements string         `json:"requirements,omitempty"`
	Pypi         *PypiLib       `json:"pypi,omitempty"`
	Maven        map[string]any `json:"maven,omitempty"`
	Cran         map[string]any `json:"cran,omitempty"`
	Egg          string         `json:"egg,omitempty"`
}

type PypiLib struct {
	Package string `json:"package"`
	Repo    string `json:"repo,omitempty"`
}

type NotebookTask struct {
	NotebookPath   string    `json:"notebook_path"`
	BaseParameters StringMap `json:"base_parameters,omitempty"`
	Source         string    `json:"source,omitempty"`
	WarehouseID    string    `json:"warehouse_id,omitempty"`
}

type SparkPythonTask struct {
	PythonFile string     `json:"python_file"`
	Parameters StringList `json:"parameters,omitempty"`
	Source     string     `json:"source,omitempty"`
}

type PythonWheelTask struct {
	PackageName     string    `json:"package_name"`
	EntryPoint      string    `json:"entry_point"`
	Parameters      []string  `json:"parameters,omitempty"`
	NamedParameters StringMap `json:"named_parameters,omitempty"`
}

type SQLTask struct {
	File        *SQLFile       `json:"file,omitempty"`
	Query       map[string]any `json:"query,omitempty"`
	Dashboard   map[string]any `json:"dashboard,omitempty"`
	Alert       map[string]any `json:"alert,omitempty"`
	WarehouseID string         `json:"warehouse_id,omitempty"`
	Parameters  StringMap      `json:"parameters,omitempty"`
}

type SQLFile struct {
	Path   string `json:"path"`
	Source string `json:"source,omitempty"`
}

type ConditionTask struct {
	Op    string `json:"op"`
	Left  string `json:"left"`
	Right string `json:"right"`
}

type ForEachTask struct {
	Inputs      string `json:"inputs"`
	Concurrency int    `json:"concurrency,omitempty"`
	Task        *Task  `json:"task"`
}

type RunJobTask struct {
	JobID         any       `json:"job_id"`
	JobParameters StringMap `json:"job_parameters,omitempty"`
}

// Scalar is a YAML scalar read as text: the CLI accepts numbers and bools
// where the API wants strings.
type Scalar string

func (s *Scalar) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch t := v.(type) {
	case nil:
		*s = ""
	case string:
		*s = Scalar(t)
	case float64:
		*s = Scalar(strconv.FormatFloat(t, 'f', -1, 64))
	default:
		*s = Scalar(fmt.Sprint(t))
	}
	return nil
}

// StringMap is a map of scalars read as text.
type StringMap map[string]string

func (m *StringMap) UnmarshalJSON(b []byte) error {
	var raw map[string]Scalar
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*m = StringMap{}
	for k, v := range raw {
		(*m)[k] = string(v)
	}
	return nil
}

// StringList is a list of scalars read as text.
type StringList []string

func (l *StringList) UnmarshalJSON(b []byte) error {
	var raw []Scalar
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*l = make(StringList, len(raw))
	for i, v := range raw {
		(*l)[i] = string(v)
	}
	return nil
}

// TaskTypes are every task type key of the DAB schema (ADR-031, Context).
var TaskTypes = []string{
	"notebook_task", "spark_python_task", "python_wheel_task", "spark_jar_task",
	"spark_submit_task", "sql_task", "dbt_task", "dbt_cloud_task", "dbt_platform_task",
	"pipeline_task", "run_job_task", "condition_task", "for_each_task",
	"clean_rooms_notebook_task", "dashboard_task", "alert_task", "power_bi_task",
	"gen_ai_compute_task", "ai_function_task", "ai_runtime_task", "python_operator_task",
}

// TypedJobs decodes every job of the bundle, sorted by key.
func (b *Bundle) TypedJobs() ([]*Job, error) {
	keys := make([]string, 0, len(b.Jobs))
	for k := range b.Jobs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*Job, 0, len(keys))
	for _, k := range keys {
		j, err := decodeJob(k, b.Jobs[k])
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

func decodeJob(key string, raw map[string]any) (*Job, error) {
	buf, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	j := &Job{Key: key, Extra: raw}
	if err := json.Unmarshal(buf, j); err != nil {
		return nil, fmt.Errorf("job %s: %w", key, err)
	}
	rawTasks, _ := raw["tasks"].([]any)
	for i := range j.Tasks {
		if i < len(rawTasks) {
			rt, _ := rawTasks[i].(map[string]any)
			setTaskType(&j.Tasks[i], rt)
		}
	}
	return j, nil
}

func setTaskType(t *Task, raw map[string]any) {
	t.Raw = raw
	for _, tt := range TaskTypes {
		if _, ok := raw[tt]; ok {
			t.Type = tt
			break
		}
	}
	if t.ForEachTask != nil && t.ForEachTask.Task != nil {
		fe, _ := raw["for_each_task"].(map[string]any)
		inner, _ := fe["task"].(map[string]any)
		setTaskType(t.ForEachTask.Task, inner)
	}
}
