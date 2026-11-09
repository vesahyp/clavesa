package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

// DatabricksCompareInput is the shadow-mode check (docs/cookbook/
// replace-a-databricks-step.md): the table the Databricks step writes
// (Existing) against the table clavesa writes next to it (Shadow), per
// partition. `clavesa pipeline databricks-compare` and POST
// /pipeline/databricks-compare decode into it (ADR-015).
type DatabricksCompareInput struct {
	Existing  string   `json:"existing"`
	Shadow    string   `json:"shadow"`
	Partition string   `json:"partition"`
	Sums      []string `json:"sums"`
	// Where limits both tables to the same rows, a Spark SQL predicate,
	// e.g. "event_date >= date_sub(current_date(), 7)".
	Where string `json:"where,omitempty"`
}

// DatabricksComparePartition is one partition's numbers: each pair is
// [existing, shadow].
type DatabricksComparePartition struct {
	Partition string            `json:"partition"`
	Status    string            `json:"status"` // match / differs: ... / missing in ...
	Rows      [2]*int64         `json:"rows"`
	Sums      map[string][2]any `json:"sums"`
}

// DatabricksCompareResult is the verdict. Pass is true only when both
// tables have the same partitions, every count and sum is equal, and the
// columns match.
type DatabricksCompareResult struct {
	Pass       bool                         `json:"pass"`
	Existing   string                       `json:"existing"`
	Shadow     string                       `json:"shadow"`
	Partition  string                       `json:"partition"`
	Where      string                       `json:"where,omitempty"`
	Partitions []DatabricksComparePartition `json:"partitions"`
	SchemaDiff []string                     `json:"schema_diff"`
}

// CompareDatabricksTables runs the comparison in the workspace's local
// runner container against the cloud, with the deployed pipeline's
// Databricks connection (ADR-024's cloud-local seam): Spark reads both
// Delta tables directly, a managed one with credentials Unity Catalog
// vends, so the check costs no DBUs. The pipeline must be deployed.
func (s *Service) CompareDatabricksTables(ctx context.Context, pipeline string, in DatabricksCompareInput) (DatabricksCompareResult, error) {
	var out DatabricksCompareResult
	in.Existing, in.Shadow, in.Partition = strings.TrimSpace(in.Existing), strings.TrimSpace(in.Shadow), strings.TrimSpace(in.Partition)
	for _, f := range []struct{ name, v string }{{"existing", in.Existing}, {"shadow", in.Shadow}} {
		if strings.Count(f.v, ".") != 2 {
			return out, fmt.Errorf("%s table %q: give the full name <catalog>.<schema>.<table>", f.name, f.v)
		}
	}
	if in.Existing == in.Shadow {
		return out, fmt.Errorf("existing and shadow are the same table %q", in.Existing)
	}
	if in.Partition == "" {
		return out, fmt.Errorf("partition column is required (the column both tables are partitioned by, e.g. event_date)")
	}
	sums := []string{}
	for _, c := range in.Sums {
		if c = strings.TrimSpace(c); c != "" {
			sums = append(sums, c)
		}
	}
	m, err := s.loadManifest()
	if err != nil {
		return out, err
	}
	if m.Databricks == nil {
		return out, fmt.Errorf("this workspace has no Databricks connection (run `clavesa workspace connect databricks`)")
	}
	name := filepath.Base(pipeline)
	if _, err := s.pipelineDir(pipeline); err != nil {
		return out, err
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return out, fmt.Errorf("load AWS config: %w", err)
	}
	env, err := lambdaRunnerEnv(ctx, lambda.NewFromConfig(cfg), pipelineRunnerLambdaName(s.awsName(name)))
	if err != nil {
		return out, fmt.Errorf("pipeline %s is not deployed (the comparison uses its runner's Databricks settings): %w", name, err)
	}
	event := map[string]any{
		"_operation": "uc_compare",
		"existing":   in.Existing,
		"shadow":     in.Shadow,
		"partition":  in.Partition,
		"sums":       sums,
		"where":      in.Where,
	}
	res, err := s.cloudLocalDispatch(ctx, env, nil, event, nil)
	if err != nil {
		return out, err
	}
	return parseCompareResponse(res.RawResponse)
}

// parseCompareResponse turns the runner's uc_compare response into the
// result. The runner sends each number as [existing, shadow]; sums are
// strings (exact decimals).
func parseCompareResponse(raw []byte) (DatabricksCompareResult, error) {
	var out DatabricksCompareResult
	var resp struct {
		Status     string           `json:"status"`
		Message    string           `json:"message"`
		Pass       bool             `json:"pass"`
		Existing   string           `json:"existing"`
		Shadow     string           `json:"shadow"`
		Partition  string           `json:"partition"`
		Where      string           `json:"where"`
		Partitions []map[string]any `json:"partitions"`
		SchemaDiff []string         `json:"schema_diff"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return out, fmt.Errorf("parse comparison: %w", err)
	}
	if resp.Status != "ok" {
		return out, fmt.Errorf("comparison failed: %s %s", resp.Status, resp.Message)
	}
	out = DatabricksCompareResult{Pass: resp.Pass, Existing: resp.Existing, Shadow: resp.Shadow,
		Partition: resp.Partition, Where: resp.Where, SchemaDiff: resp.SchemaDiff,
		Partitions: []DatabricksComparePartition{}}
	if out.SchemaDiff == nil {
		out.SchemaDiff = []string{}
	}
	for _, p := range resp.Partitions {
		row := DatabricksComparePartition{Sums: map[string][2]any{}}
		row.Partition, _ = p["partition"].(string)
		row.Status, _ = p["status"].(string)
		for k, v := range p {
			pair, ok := v.([]any)
			if !ok || len(pair) != 2 {
				continue
			}
			if k == "rows" {
				for i := 0; i < 2; i++ {
					if f, ok := pair[i].(float64); ok {
						n := int64(f)
						row.Rows[i] = &n
					}
				}
			} else if strings.HasPrefix(k, "sum_") {
				row.Sums[strings.TrimPrefix(k, "sum_")] = [2]any{pair[0], pair[1]}
			}
		}
		out.Partitions = append(out.Partitions, row)
	}
	return out, nil
}
