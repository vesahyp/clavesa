package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/bundle"
	"github.com/vesahyp/clavesa/internal/modules"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// TaskModuleRel is the module of a bundle task node (ADR-031).
const TaskModuleRel = "task/aws"

// BundleOptions select the bundle folder, target and variable overrides of
// a `clavesa bundle` command (ADR-031 section 11).
type BundleOptions struct {
	Dir    string
	Target string
	Vars   map[string]string
}

// ResolveBundle reads a Databricks bundle for one target. A bundle with a
// python: section is read through the Databricks CLI with the user's own
// login (ADR-031 section 2); every other bundle is read by clavesa alone.
func ResolveBundle(ctx context.Context, o BundleOptions) (*bundle.Bundle, error) {
	opts := bundle.Options{Target: o.Target, Vars: o.Vars}
	b, err := bundle.Resolve(o.Dir, opts)
	if err != nil {
		return nil, err
	}
	if b.Python {
		return bundle.ResolveWithCLI(ctx, o.Dir, opts)
	}
	return b, nil
}

// PlanBundle resolves a bundle target and plans it: where each task runs and
// what stops it. It makes no network call (unless the bundle has a python:
// section, see ResolveBundle).
func PlanBundle(ctx context.Context, o BundleOptions) (*bundle.Report, error) {
	b, err := ResolveBundle(ctx, o)
	if err != nil {
		return nil, err
	}
	return bundle.Plan(b)
}

// BundlePipelineDir is the workspace-relative folder of the pipeline a
// bundle job becomes: <bundle>_<job>, at the top of the workspace like
// every pipeline (the run path takes the workspace root to be the
// pipeline folder's parent). bundle-job.json in it marks it as generated.
func BundlePipelineDir(bundleName, job string) string {
	return bundle.PipelineName(bundleName, job)
}

// IsBundlePipeline reports whether a pipeline folder was generated from a
// bundle (ADR-031): it is then read-only, and the bundle is the source.
func IsBundlePipeline(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, bundle.JobFileName))
	return err == nil
}

// GeneratedBundleJob is what GenerateBundleJob wrote.
type GeneratedBundleJob struct {
	Dir     string // absolute pipeline folder
	Name    string // pipeline name
	JobFile *bundle.JobFile
	Report  *bundle.Report
	// RequirementsAdded are the runner requirements this generation added
	// to the workspace (the runner image must be rebuilt and pushed).
	RequirementsAdded []string
}

// GenerateBundleJob plans the bundle target and (re)writes the pipeline of
// one job into the workspace: main.tf with one task node per task,
// bundle-job.json, and a copy of the bundle's files. The plan must be
// ready; the report comes back either way so the caller can print it.
func (s *Service) GenerateBundleJob(ctx context.Context, o BundleOptions, job string) (*GeneratedBundleJob, error) {
	b, err := resolveForRun(ctx, o)
	if err != nil {
		return nil, err
	}
	r, err := bundle.Plan(b)
	if err != nil {
		return nil, err
	}
	out := &GeneratedBundleJob{Report: r}
	if m, _ := workspace.Load(s.workspace); m == nil {
		return out, fmt.Errorf("%s is not a clavesa workspace: run clavesa workspace init <name> --workspace %s first, or point --workspace (or clavesa.workspace in the target) at one", s.workspace, s.workspace)
	}
	if !r.Ready {
		return out, fmt.Errorf("the plan for target %s is not ready (%d refuse): fix what clavesa bundle plan reports first", b.Target, r.Refuse)
	}
	jf, err := bundle.BuildJobFile(b, r, job)
	if err != nil {
		return out, err
	}
	out.JobFile = jf
	if jf.Mixed() {
		id, err := bundle.DeployedJobID(ctx, b.Root, b.Target, o.Vars, job)
		if err != nil {
			return out, err
		}
		jf.DatabricksJobID = id
	}
	rel := BundlePipelineDir(b.Name, job)
	abs := filepath.Join(s.workspace, rel)
	out.Dir, out.Name = abs, filepath.Base(rel)
	// The task module (modules/task/aws) came with ADR-031: a workspace
	// whose modules were extracted before it has no task/ folder.
	// Extract is idempotent and re-extracts when the tree differs.
	if err := modules.Extract(s.workspace, ModuleVersion); err != nil {
		return out, fmt.Errorf("extract the clavesa modules into the workspace: %w", err)
	}
	if _, err := os.Stat(filepath.Join(abs, "main.tf")); os.IsNotExist(err) {
		if _, err := s.CreatePipeline(rel, ""); err != nil {
			return out, fmt.Errorf("create pipeline %s: %w", rel, err)
		}
	}

	// main.tf: keep the scaffold CreatePipeline wrote, replace the
	// generated task blocks.
	mainPath := filepath.Join(abs, "main.tf")
	cur, err := os.ReadFile(mainPath)
	if err != nil {
		return out, err
	}
	head := string(cur)
	if i := strings.Index(head, bundle.MainTFMarker); i >= 0 {
		head = head[:i]
	}
	head = strings.TrimRight(head, "\n") + "\n\n"
	src := s.ModuleSource(abs, TaskModuleRel)
	if err := os.WriteFile(mainPath, []byte(head+bundle.TaskModules(jf, src)), 0o644); err != nil {
		return out, err
	}

	// The bundle's files, copied fresh.
	files := filepath.Join(abs, bundle.FilesDirName)
	if err := os.RemoveAll(files); err != nil {
		return out, err
	}
	if err := copyBundleFiles(b.Root, files); err != nil {
		return out, fmt.Errorf("copy the bundle's files: %w", err)
	}
	if err := buildArtifacts(ctx, b, files); err != nil {
		return out, err
	}
	if err := prepareJobTasks(files, jf); err != nil {
		return out, err
	}
	// The job's pypi libraries go into the workspace's runner requirements
	// (ADR-031 section 7); the next run's image build picks them up.
	added, err := s.addBundleRequirements(jf.Libraries)
	if err != nil {
		return out, err
	}
	out.RequirementsAdded = added
	data, _ := json.MarshalIndent(jf, "", "  ")
	if err := os.WriteFile(filepath.Join(abs, bundle.JobFileName), append(data, '\n'), 0o644); err != nil {
		return out, err
	}
	return out, nil
}

// resolveForRun resolves a bundle for a clavesa run: ${workspace.file_path}
// becomes the placeholder the runner replaces with the run's files folder.
func resolveForRun(ctx context.Context, o BundleOptions) (*bundle.Bundle, error) {
	opts := bundle.Options{Target: o.Target, Vars: o.Vars, FilesPath: bundle.FilesPlaceholder}
	b, err := bundle.Resolve(o.Dir, opts)
	if err != nil {
		return nil, err
	}
	if b.Python {
		return bundle.ResolveWithCLI(ctx, o.Dir, opts)
	}
	return b, nil
}

// skipCopy are the bundle folders that are never part of a run.
var skipCopy = map[string]bool{".git": true, ".databricks": true, ".venv": true, "node_modules": true, "__pycache__": true, ".clavesa": true}

func copyBundleFiles(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() && skipCopy[d.Name()] && rel != "." {
			return filepath.SkipDir
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
}

// RunBundleJob generates the job's pipeline and runs it on the local
// warehouse. params are --param values for the job parameters.
func (s *Service) RunBundleJob(ctx context.Context, o BundleOptions, job string, params map[string]string) (*GeneratedBundleJob, *RunResult, error) {
	gen, err := s.GenerateBundleJob(ctx, o, job)
	if err != nil {
		return gen, nil, err
	}
	declared := map[string]bool{}
	for _, p := range gen.JobFile.Parameters {
		declared[p.Name] = true
	}
	for k := range params {
		if !declared[k] {
			return gen, nil, fmt.Errorf("--param %s: job %s has no parameter %s", k, job, k)
		}
	}
	res, err := s.RunPipelineWithOpts(ctx, gen.Dir, RunOpts{Trigger: "manual", BundleParams: params})
	return gen, res, err
}

func bundleJobPath(pipelineDir string) string {
	return filepath.Join(pipelineDir, bundle.JobFileName)
}

// bundleRunParts is the bundle part of a pipeline run event.
type bundleRunParts struct {
	job    map[string]any
	tasks  map[string]any
	mounts []dockerMount
}

// bundleRunEvent builds the _bundle_job part of the runner event for a
// pipeline generated from a bundle, and copies the bundle's files into the
// run's workdir. It returns nil for any other pipeline.
func bundleRunEvent(pipelineDir, workdir, runID string, entries []bundleTransformConfig) (*bundleRunParts, error) {
	var params map[string]string
	isBundle := false
	for _, e := range entries {
		if e.Language == "task" {
			isBundle = true
			params = e.BundleParams
		}
	}
	if !isBundle {
		return nil, nil
	}
	raw, err := os.ReadFile(bundleJobPath(pipelineDir))
	if err != nil {
		return nil, fmt.Errorf("bundle pipeline without %s: run clavesa bundle run again: %w", bundle.JobFileName, err)
	}
	var jf bundle.JobFile
	if err := json.Unmarshal(raw, &jf); err != nil {
		return nil, fmt.Errorf("%s: %w", bundle.JobFileName, err)
	}
	merged := map[string]string{}
	for _, p := range jf.Parameters {
		merged[p.Name] = string(p.Default)
	}
	for k, v := range params {
		merged[k] = v
	}
	files := filepath.Join(workdir, bundle.FilesDirName)
	if err := copyBundleFiles(filepath.Join(pipelineDir, bundle.FilesDirName), files); err != nil {
		return nil, fmt.Errorf("copy the bundle's files into the run: %w", err)
	}
	var jobMap map[string]any
	_ = json.Unmarshal(raw, &jobMap)
	delete(jobMap, "tasks")
	jobMap["params"] = merged
	jobMap["bundle_root"] = files
	jobMap["run_id"] = runID
	tasks := map[string]any{}
	for k, t := range jf.Tasks {
		b, _ := json.Marshal(t)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		tasks[k] = m
	}
	return &bundleRunParts{job: jobMap, tasks: tasks}, nil
}

// bundleSecretEnv forwards the shell's CLAVESA_DBX_SECRET__* variables
// (Databricks secrets for a local bundle run) into the runner.
func bundleSecretEnv() []string {
	var out []string
	for _, kv := range os.Environ() {
		// DATABRICKS_HOST / DATABRICKS_TOKEN: the Databricks segments of a
		// mixed job on a local run (ADR-031 section 8).
		if strings.HasPrefix(kv, "CLAVESA_DBX_SECRET__") || strings.HasPrefix(kv, "DATABRICKS_HOST=") || strings.HasPrefix(kv, "DATABRICKS_TOKEN=") {
			out = append(out, kv)
		}
	}
	return out
}

// refuseBundleEdit refuses an edit of a pipeline generated from a bundle:
// the bundle is the only source (ADR-031 section 3).
func (s *Service) refuseBundleEdit(dir string) error {
	abs := s.resolveDir(dir)
	raw, err := os.ReadFile(bundleJobPath(abs))
	if err != nil {
		return nil
	}
	var jf bundle.JobFile
	_ = json.Unmarshal(raw, &jf)
	where := "databricks.yml"
	if jf.Source != "" {
		where = jf.Source
	}
	return fmt.Errorf("pipeline %s is generated from job %s of the Databricks bundle %s: change the job in %s, then run clavesa bundle run (or deploy) again",
		filepath.Base(abs), jf.Job, jf.Bundle, where)
}

// prepareJobTasks translates the job's notebooks into scripts and resolves
// the wheels each task installs, in the copy of the bundle's files; the
// same for the jobs its run_job_tasks start.
func prepareJobTasks(files string, jf *bundle.JobFile) error {
	for _, k := range jf.Order {
		t := jf.Tasks[k]
		if t == nil || t.Place != "clavesa" || t.Disabled {
			continue
		}
		if nt, ok := t.Task["notebook_task"].(map[string]any); ok {
			// Notebooks are translated here, once, so a run never parses a
			// notebook (ADR-031 section 5).
			nbPath, _ := nt["notebook_path"].(string)
			script, err := bundle.TranslateNotebook(files, nbPath)
			if err != nil {
				return fmt.Errorf("task %s: %w", k, err)
			}
			t.Script = bundle.ScriptPath(nbPath)
			dst := filepath.Join(files, filepath.FromSlash(t.Script))
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(dst, []byte(script), 0o644); err != nil {
				return err
			}
		}
		wheels, err := taskWheels(files, t, t.EnvDeps)
		if err != nil {
			return err
		}
		t.Wheels = wheels
		if t.Job != nil {
			if err := prepareJobTasks(files, t.Job); err != nil {
				return err
			}
		}
	}
	return nil
}

// addBundleRequirements adds the bundle's pypi libraries to the workspace's
// runner requirements. A package the workspace already pins to another
// version is an error: one runner image holds one version.
func (s *Service) addBundleRequirements(specs []string) ([]string, error) {
	have, err := s.ListRunnerRequirements()
	if err != nil {
		return nil, err
	}
	var added []string
	pinned := map[string]string{}
	for _, l := range have {
		pinned[reqName(l)] = l
	}
	for _, spec := range specs {
		if cur, ok := pinned[reqName(spec)]; ok {
			if !strings.EqualFold(strings.ReplaceAll(cur, " ", ""), strings.ReplaceAll(spec, " ", "")) {
				return nil, fmt.Errorf("the bundle needs %s, but the workspace's runner requirements have %s: one runner image holds one version (clavesa runner requirements)", spec, cur)
			}
			continue
		}
		if _, err := s.AddRunnerRequirement(spec); err != nil {
			return nil, err
		}
		added = append(added, spec)
	}
	return added, nil
}

func reqName(spec string) string {
	n := strings.ToLower(strings.TrimSpace(spec))
	if i := strings.IndexAny(n, "=<>!~[; "); i >= 0 {
		n = n[:i]
	}
	return strings.NewReplacer("_", "-", ".", "-").Replace(n)
}

// BundlePipelineInfo is one pipeline generated from a bundle job.
type BundlePipelineInfo struct {
	Pipeline string `json:"pipeline"`
	Bundle   string `json:"bundle"`
	Target   string `json:"target"`
	Job      string `json:"job"`
	Catalog  string `json:"catalog"`
	Shadow   bool   `json:"shadow"`
	Tasks    int    `json:"tasks"`
	Source   string `json:"source,omitempty"`
}

// BundlePipelines lists the workspace's pipelines generated from bundle
// jobs (ADR-031; `clavesa bundle list` and the /bundles page).
func (s *Service) BundlePipelines() ([]BundlePipelineInfo, error) {
	entries, err := os.ReadDir(s.workspace)
	if err != nil {
		return nil, err
	}
	out := []BundlePipelineInfo{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.workspace, e.Name(), bundle.JobFileName))
		if err != nil {
			continue
		}
		var jf bundle.JobFile
		if json.Unmarshal(raw, &jf) != nil {
			continue
		}
		out = append(out, BundlePipelineInfo{Pipeline: e.Name(), Bundle: jf.Bundle, Target: jf.Target, Job: jf.Job,
			Catalog: jf.Catalog, Shadow: jf.Shadow, Tasks: len(jf.Tasks), Source: jf.Source})
	}
	return out, nil
}
