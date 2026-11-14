package service

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/vesahyp/clavesa/internal/bundle"
	"github.com/vesahyp/clavesa/internal/orchestration/tfgen"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// BundleTFVarsFile holds the Fargate size of a bundle pipeline (ADR-031
// section 7); terraform loads *.auto.tfvars by itself.
const BundleTFVarsFile = "bundle.auto.tfvars"

var s3BucketRe = regexp.MustCompile(`s3a?://([a-z0-9][a-z0-9.\-]{1,61}[a-z0-9])`)

// bundleOrchestration builds the orchestration of a bundle pipeline: one
// runner entry per task (the runner reads the task from the uploaded
// bundle-job.json), the compute, and the AWS side of the job's schedule,
// triggers and notifications.
func (s *Service) bundleOrchestration(abs, bucketExpr string, ws *workspace.Manifest) ([]tfgen.TransformConfig, *tfgen.BundleConfig, string, error) {
	raw, err := os.ReadFile(bundleJobPath(abs))
	if err != nil {
		return nil, nil, "", err
	}
	var jf bundle.JobFile
	if err := json.Unmarshal(raw, &jf); err != nil {
		return nil, nil, "", fmt.Errorf("%s: %w", bundle.JobFileName, err)
	}
	var out []tfgen.TransformConfig
	logic := fmt.Sprintf(`"s3://${%s}/${var.pipeline_name}/_bundle/bundle-job.json"`, bucketExpr)
	dbs := map[string]bool{}
	for _, k := range jf.Order {
		t := jf.Tasks[k]
		if t == nil || t.Disabled {
			continue
		}
		if t.Place == "databricks" {
			continue // a Databricks segment (ADR-031 section 8)
		}
		var parents []string
		for _, p := range t.Parents {
			if pt := jf.Tasks[p]; pt != nil && !pt.Disabled {
				parents = append(parents, p)
			}
		}
		sort.Strings(parents)
		out = append(out, tfgen.TransformConfig{
			NodeID: k, Language: "task", LogicS3URI: logic,
			InputsExpr: "{}", OutputsExpr: "{}", Parents: parents,
		})
		// Both modes keep the job's own tables in Glue databases
		// <catalog>__<schema> (catalog: unity publishes them after the task).
		if jf.Catalog == "glue" || jf.Catalog == "unity" {
			for _, name := range append(append([]string{}, t.Reads...), t.Writes...) {
				parts := strings.SplitN(name, ".", 3)
				if len(parts) == 3 {
					cat := parts[0]
					if m, ok := jf.Catalogs[cat]; ok {
						cat = m
					}
					dbs[strings.ToLower(cat+"__"+parts[1])] = true
				}
			}
		}
	}

	bc := &tfgen.BundleConfig{FilesDir: bundle.FilesDirName, JobFile: bundle.JobFileName, Notify: map[string][]string{}}
	for db := range dbs {
		bc.GlueDatabases = append(bc.GlueDatabases, db)
	}
	sort.Strings(bc.GlueDatabases)
	if ws != nil && ws.Name != "" {
		bc.SecretsPrefix = "clavesa/" + ws.Name + "/dbx/"
	}
	if jf.Schedule != nil && jf.Schedule.QuartzCron != "" {
		expr, err := bundle.AWSCron(jf.Schedule.QuartzCron)
		if err != nil {
			return nil, nil, "", err
		}
		bc.ScheduleExpression, bc.ScheduleTimezone = expr, jf.Schedule.TimezoneID
		bc.SchedulePaused = jf.Schedule.PauseStatus == "PAUSED"
	}
	if jf.Trigger != nil {
		paused, _ := jf.Trigger["pause_status"].(string)
		if per, ok := jf.Trigger["periodic"].(map[string]any); ok {
			n, _ := per["interval"].(float64)
			unit, _ := per["unit"].(string)
			expr, err := bundle.AWSRate(int(n), unit)
			if err != nil {
				return nil, nil, "", err
			}
			bc.ScheduleExpression, bc.SchedulePaused = expr, paused == "PAUSED"
		}
		if fa, ok := jf.Trigger["file_arrival"].(map[string]any); ok {
			u, _ := fa["url"].(string)
			if !strings.HasPrefix(u, "s3://") {
				return nil, nil, "", fmt.Errorf("trigger.file_arrival url %q: clavesa watches s3:// locations only", u)
			}
			rest := strings.TrimPrefix(u, "s3://")
			bucket, prefix, _ := strings.Cut(rest, "/")
			bc.FileArrivalBucket, bc.FileArrivalPrefix, bc.FileArrivalPaused = bucket, prefix, paused == "PAUSED"
		}
	}
	if en := jf.EmailNotify; en != nil {
		bc.Notify["start"], bc.Notify["success"], bc.Notify["failure"] = en.OnStart, en.OnSuccess, en.OnFailure
	}
	if h := jf.Health; h != nil {
		for _, r := range h.Rules {
			if r.Metric == "RUN_DURATION_SECONDS" {
				bc.HealthMaxSeconds = r.Value
			}
		}
	}

	// External buckets the job's code and parameters name.
	buckets := map[string]bool{}
	scan := func(text string) {
		for _, m := range s3BucketRe.FindAllStringSubmatch(text, -1) {
			buckets[m[1]] = true
		}
	}
	scan(string(raw))
	_ = filepath.WalkDir(filepath.Join(abs, bundle.FilesDirName), func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		switch strings.ToLower(filepath.Ext(p)) {
		case ".sql", ".py", ".ipynb", ".yml", ".yaml", ".json", ".txt":
			if b, err := os.ReadFile(p); err == nil && len(b) < 4<<20 {
				scan(string(b))
			}
		}
		return nil
	})
	for b := range buckets {
		bc.ReadBuckets = append(bc.ReadBuckets, b)
	}
	sort.Strings(bc.ReadBuckets)

	if jf.Mixed() {
		if jf.DatabricksJobID == 0 {
			return nil, nil, "", fmt.Errorf("mixed job %s without the deployed Databricks job: run clavesa bundle deploy again", jf.Job)
		}
		byNode := map[string]tfgen.TransformConfig{}
		for _, t := range out {
			byNode[t.NodeID] = t
		}
		for _, seg := range jf.Segments {
			ps := tfgen.PipelineSegment{Place: seg.Place, Tasks: seg.Tasks}
			if seg.Place == "clavesa" {
				for _, k := range seg.Tasks {
					ps.Transforms = append(ps.Transforms, byNode[k])
				}
			}
			bc.Segments = append(bc.Segments, ps)
		}
		bc.DatabricksJobID = jf.DatabricksJobID
		bc.JobURI = logic
	}
	compute := "lambda"
	if jf.Compute != nil && jf.Compute.Kind == "lambda" {
		if mb := int(jf.Compute.MemoryGB*1024 + 0.5); mb != 3008 && mb >= 128 {
			bc.LambdaMemoryMB = mb
		}
	}
	if jf.Compute != nil && jf.Compute.Kind == "fargate" && !jf.Mixed() {
		compute = tfgen.ComputeFargate
		vars := fmt.Sprintf("# Generated by clavesa from the bundle job's clusters (ADR-031); do not edit.\nfargate_cpu       = %g\nfargate_memory_gb = %g\n", jf.Compute.VCPU, jf.Compute.MemoryGB)
		if err := os.WriteFile(filepath.Join(abs, BundleTFVarsFile), []byte(vars), 0o644); err != nil {
			return nil, nil, "", err
		}
	} else {
		_ = os.Remove(filepath.Join(abs, BundleTFVarsFile))
	}
	return out, bc, compute, nil
}
