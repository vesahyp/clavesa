package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// DatabricksJobsFile is the clavesa-owned tfvars file in a pipeline dir
// holding the downstream Databricks job IDs (ADR-027 section 6, option
// A). Separate from terraform.tfvars, whose generic writer quotes every
// value and so cannot hold a list.
const DatabricksJobsFile = "databricks.auto.tfvars"

var jobsLineRE = regexp.MustCompile(`(?m)^\s*databricks_jobs\s*=\s*\[([^\]]*)\]`)

// GetDatabricksJobs returns the job IDs a pipeline starts after a
// successful run. No file means none.
func (s *Service) GetDatabricksJobs(dir string) ([]int64, error) {
	abs, err := s.pipelineDir(dir)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(abs, DatabricksJobsFile))
	if errors.Is(err, os.ErrNotExist) {
		return []int64{}, nil
	}
	if err != nil {
		return nil, err
	}
	m := jobsLineRE.FindSubmatch(data)
	if m == nil {
		return []int64{}, nil
	}
	out := []int64{}
	for _, part := range strings.Split(string(m[1]), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %q is not a job ID", DatabricksJobsFile, part)
		}
		out = append(out, id)
	}
	return out, nil
}

// SetDatabricksJobs writes the pipeline's downstream job IDs. An empty
// list removes the file. Takes effect at the next `pipeline deploy`
// (the runner reads them from its Lambda environment).
func (s *Service) SetDatabricksJobs(dir string, jobs []int64) error {
	abs, err := s.pipelineDir(dir)
	if err != nil {
		return err
	}
	path := filepath.Join(abs, DatabricksJobsFile)
	if len(jobs) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	parts := make([]string, len(jobs))
	for i, j := range jobs {
		if j <= 0 {
			return fmt.Errorf("job ID %d: must be a positive number", j)
		}
		parts[i] = strconv.FormatInt(j, 10)
	}
	body := "# clavesa-managed (ADR-027): Databricks jobs started after every successful run.\n" +
		"# Written by `clavesa pipeline databricks-jobs` and the Unity Catalog page.\n" +
		"databricks_jobs = [" + strings.Join(parts, ", ") + "]\n"
	return os.WriteFile(path, []byte(body), 0o644)
}

// pipelineDir resolves a pipeline dir (name or path) inside the workspace.
func (s *Service) pipelineDir(dir string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("pipeline is required")
	}
	abs := dir
	if !filepath.IsAbs(abs) {
		abs = filepath.Join(s.workspace, dir)
	}
	if _, err := os.Stat(filepath.Join(abs, "main.tf")); err != nil {
		return "", fmt.Errorf("%q is not a pipeline in this workspace", dir)
	}
	return abs, nil
}

// DatabricksPipelineTables is one pipeline's view of Unity Catalog: its
// schema there, the tables in it, and its downstream jobs.
type DatabricksPipelineTables struct {
	Pipeline    string            `json:"pipeline"`
	Dir         string            `json:"dir"`
	Schema      string            `json:"schema"`     // <catalog>.<schema>
	SchemaURL   string            `json:"schema_url"` // Catalog Explorer
	SchemaError string            `json:"schema_error,omitempty"`
	Tables      []DatabricksTable `json:"tables"`
	Jobs        []int64           `json:"jobs"`
}

// DatabricksTable is one table in a pipeline's Unity Catalog schema.
type DatabricksTable struct {
	FullName  string `json:"full_name"`
	Type      string `json:"type"` // EXTERNAL / MANAGED
	Location  string `json:"location"`
	Owner     string `json:"owner"`
	ByClavesa bool   `json:"by_clavesa"` // registered by this pipeline
	URL       string `json:"url"`
}

// DatabricksTables lists, per pipeline, what Unity Catalog holds in the
// pipeline's schema (ADR-027 slice 7). It reads Databricks directly, so
// it shows what Databricks users see, not what clavesa intended.
func (s *Service) DatabricksTables(ctx context.Context) ([]DatabricksPipelineTables, error) {
	m, err := s.loadManifest()
	if err != nil {
		return nil, err
	}
	if m.Databricks == nil {
		return nil, fmt.Errorf("this workspace has no Databricks connection (run `clavesa workspace connect databricks`)")
	}
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return nil, err
	}
	secret, err := s.resolveSecret(ctx, cred.Secret)
	if err != nil {
		return nil, fmt.Errorf("credential %q: %w", cred.Name, err)
	}
	client := newDatabricksLister(m.Databricks.Host, cred.ClientID, secret)
	pipelines, err := s.ListPipelines()
	if err != nil {
		return nil, err
	}
	out := []DatabricksPipelineTables{}
	for _, p := range pipelines {
		if strings.HasPrefix(p.Name, "_") {
			continue // _maintenance and other internal stacks
		}
		schema := identutil.Sanitize(p.Schema)
		if schema == "" {
			schema = identutil.Sanitize(p.Name)
		}
		row := DatabricksPipelineTables{
			Pipeline:  p.Name,
			Dir:       p.Dir,
			Schema:    m.Databricks.Catalog + "." + schema,
			SchemaURL: explorerURL(m.Databricks.Host, m.Databricks.Catalog, schema),
			Tables:    []DatabricksTable{},
		}
		if jobs, err := s.GetDatabricksJobs(p.Dir); err == nil {
			row.Jobs = jobs
		}
		seen := map[string]bool{}
		// byPipeline: an output names the table, so this pipeline writes
		// it. Unity Catalog drops the REST-set clavesa.pipeline property
		// once Databricks writes a table, so the property alone is not
		// enough.
		add := func(t databricks.Table, byPipeline bool) {
			if seen[t.FullName] {
				return
			}
			seen[t.FullName] = true
			row.Tables = append(row.Tables, DatabricksTable{
				FullName:  t.FullName,
				Type:      t.TableType,
				Location:  t.StorageLocation,
				Owner:     t.Owner,
				ByClavesa: byPipeline || t.Properties["clavesa.pipeline"] == p.Name,
				URL:       explorerURL(m.Databricks.Host, strings.Split(t.FullName, ".")...),
			})
		}
		// Outputs that name their Unity Catalog table (a shadow table, or
		// the team's table after the switch) usually live outside the
		// pipeline's schema, so the schema listing alone misses them.
		explicit := explicitUnityCatalogTables(s.resolveDir(p.Dir), m.Databricks.Catalog, schema)
		for _, name := range explicit {
			t, err := client.GetTable(ctx, name)
			if err != nil {
				if !databricks.IsNotFound(err) {
					row.SchemaError = err.Error()
				}
				continue
			}
			add(t, true)
		}
		tables, err := client.ListTables(ctx, m.Databricks.Catalog, schema)
		if err != nil {
			switch {
			case databricks.IsNotFound(err) && len(explicit) > 0:
				// Every output names its own table; the pipeline's
				// schema is not used.
			case databricks.IsNotFound(err):
				row.SchemaError = "schema does not exist in Unity Catalog yet (deploy the pipeline)"
			default:
				row.SchemaError = err.Error()
			}
		}
		for _, t := range tables {
			add(t, false)
		}
		sort.Slice(row.Tables, func(i, j int) bool { return row.Tables[i].FullName < row.Tables[j].FullName })
		out = append(out, row)
	}
	return out, nil
}

func explorerURL(host string, parts ...string) string {
	esc := make([]string, len(parts))
	for i, p := range parts {
		esc[i] = url.PathEscape(p)
	}
	return host + "/explore/data/" + strings.Join(esc, "/")
}

// tableLister is the part of the Databricks client the listing uses.
type tableLister interface {
	ListTables(ctx context.Context, catalog, schema string) ([]databricks.Table, error)
	GetTable(ctx context.Context, fullName string) (databricks.Table, error)
}

// explicitUnityCatalogTables returns the full names of the tables a
// pipeline's enabled outputs name with unity_catalog_table, in node order.
// A pipeline that cannot be parsed has none.
func explicitUnityCatalogTables(dir, catalog, schema string) []string {
	g, err := hclparser.Parse(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, n := range g.Nodes {
		if n.Type != "transform" || !nodeEnabled(n) {
			continue
		}
		for _, key := range outputKeyList(n) {
			if ucTable := outputStringField(n, key, "unity_catalog_table"); ucTable != "" {
				out = append(out, ucFullName(catalog, schema, ucTable, ""))
			}
		}
	}
	return out
}

// newDatabricksLister builds the listing client; tests replace it.
var newDatabricksLister = func(host, clientID, secret string) tableLister {
	return databricks.NewClient(host, clientID, secret)
}

// DatabricksRun is what one clavesa run did in Databricks (ADR-027): the
// tables it registered or wrote, and the jobs it started. The runner
// writes it as `_progress/<run>/_databricks.json` in the workspace bucket.
// Found is false when the run published nothing.
type DatabricksRun struct {
	Found  bool                 `json:"found"`
	Tables []DatabricksRunTable `json:"tables"`
	Jobs   []DatabricksRunJob   `json:"jobs"`
	Status string               `json:"status,omitempty"`
}

// DatabricksRunTable is one Unity Catalog table a run wrote.
type DatabricksRunTable struct {
	Output   string `json:"output,omitempty"`
	FullName string `json:"full_name"`
	Action   string `json:"action"`
	URL      string `json:"url"`
	Lineage  string `json:"lineage,omitempty"`
}

// DatabricksRunJob is one Databricks job a run started (or failed to).
type DatabricksRunJob struct {
	JobID      int64  `json:"job_id"`
	RunID      int64  `json:"run_id,omitempty"`
	RunPageURL string `json:"run_page_url,omitempty"`
	Error      string `json:"error,omitempty"`
}

// GetDatabricksRun reads the run's `_databricks.json`. runID is the run's
// progress key, the Step Functions execution ARN for a cloud run, or the
// execution's short ID together with its pipeline; the ARN is then built
// from the AWS region and account (the run page has only the short ID
// until the run row loads).
func (s *Service) GetDatabricksRun(ctx context.Context, pipeline, runID string) (DatabricksRun, error) {
	out := DatabricksRun{Tables: []DatabricksRunTable{}, Jobs: []DatabricksRunJob{}}
	m, err := s.loadManifest()
	if err != nil {
		return out, err
	}
	if m.Databricks == nil || runID == "" {
		return out, nil
	}
	if !strings.HasPrefix(runID, "arn:") && pipeline != "" {
		arn, err := executionARN(ctx, workspace.PipelineAWSName(m, filepath.Base(pipeline)), runID)
		if err != nil {
			return out, err
		}
		runID = arn
	}
	data, err := readDatabricksRunObject(ctx, m.Name+"-clavesa", "_progress/"+runID+"/_databricks.json")
	if err != nil || data == nil {
		return out, err
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, fmt.Errorf("parse _databricks.json: %w", err)
	}
	out.Found = true
	if out.Tables == nil {
		out.Tables = []DatabricksRunTable{}
	}
	if out.Jobs == nil {
		out.Jobs = []DatabricksRunJob{}
	}
	return out, nil
}

// executionARN builds a pipeline run's Step Functions execution ARN from
// its short ID. A variable so tests can replace it.
var executionARN = func(ctx context.Context, awsName, id string) (string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}
	who, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("resolve run %s: %w", id, err)
	}
	return fmt.Sprintf("arn:aws:states:%s:%s:execution:%s:%s", cfg.Region, *who.Account, awsName, id), nil
}

// readDatabricksRunObject fetches one S3 object; nil, nil when absent.
// A variable so tests can replace it.
var readDatabricksRunObject = func(ctx context.Context, bucket, key string) ([]byte, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	obj, err := s3.NewFromConfig(cfg).GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) || strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "StatusCode: 404") {
			return nil, nil
		}
		return nil, err
	}
	defer obj.Body.Close()
	return io.ReadAll(obj.Body)
}
