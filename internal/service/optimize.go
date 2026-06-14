package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// OptimizeRequest describes a maintenance sweep over a pipeline's Delta
// output tables. With no Node it targets every transform output; Recluster
// migrates a table to (or re-applies) liquid clustering before compacting;
// Vacuum additionally prunes tombstoned files past the retention window.
type OptimizeRequest struct {
	Dir         string // pipeline dir, relative to workspace
	Node        string // optional: limit to one node; empty = all transform nodes
	Recluster   bool   // ALTER TABLE CLUSTER BY (keys) + OPTIMIZE, else plain OPTIMIZE
	Vacuum      bool   // also VACUUM after the optimize/cluster step
	RetainHours int    // VACUUM retention window; defaults to 168 when 0 and Vacuum set
}

// OptimizeTableResult is the per-table outcome of one sweep. Status is "ok"
// or "failed"; on failure Error carries the runner / dispatch message and the
// sweep continues to the next table rather than aborting.
type OptimizeTableResult struct {
	Table     string `json:"table"`
	Node      string `json:"node"`
	OutputKey string `json:"output_key"`
	Operation string `json:"operation"` // "optimize" | "cluster_alter"
	Vacuumed  bool   `json:"vacuumed"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// defaultVacuumRetainHours is Delta's safe default retention window (7 days).
// Vacuuming below this risks deleting files a concurrent reader still needs;
// the runner enforces nothing, so we keep the conservative default here.
const defaultVacuumRetainHours = 168

// OptimizeTable compacts (and optionally re-clusters / vacuums) a pipeline's
// canonical Delta output tables by dispatching the runner's control-plane
// `optimize` / `cluster_alter` / `vacuum` operations. Mirrors BackfillPromote's
// local-vs-cloud dispatch: local runs the runner image directly via
// runOperation; cloud invokes the pipeline's single runner Lambda. Each table
// is handled independently — a single-table failure is recorded and the sweep
// continues. Results come back sorted by table id for determinism.
func (s *Service) OptimizeTable(ctx context.Context, req OptimizeRequest) ([]OptimizeTableResult, error) {
	abs := s.resolveDir(req.Dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}

	pipelineName := filepath.Base(abs)

	// Resolve catalog / system catalog / schema the same way prepareRun does,
	// so the table ids and the system-`tables` snapshot record match the
	// names the pipeline run path writes (local and cloud alike — the
	// workspace manifest is local in both modes).
	catalog := "clavesa"
	systemCatalog := ""
	if m, _ := workspace.Load(s.workspace); m != nil {
		catalog = m.CatalogIdentifier()
		systemCatalog = m.SystemCatalogIdentifier()
	}
	schema := resolvePipelineSchema(abs, pipelineName)

	// Build the target list: every (or just req.Node's) transform node, each
	// declared output key.
	type target struct {
		node      *graph.Node
		outputKey string
		tableID   string
		operation string
		clusterBy []string
	}
	var targets []target
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if n.Type != "transform" {
			continue
		}
		if req.Node != "" && n.ID != req.Node {
			continue
		}
		defs, _ := n.Config["output_definitions"].(map[string]interface{})
		for _, key := range outputKeyList(*n) {
			// autoDeltaTableID gives the <db>.<node> base; canonicalTableSegment
			// owns the bare-vs-`__<key>` suffix decision (matches the runner's
			// _table_id_for). Rebuild the id from the db prefix + segment.
			base := autoDeltaTableID(catalog, schema, n.ID)
			db := base[:strings.LastIndex(base, ".")]
			tableID := fmt.Sprintf("%s.%s", db, canonicalTableSegment(n.ID, defs, key))

			op := "optimize"
			var clusterBy []string
			if req.Recluster {
				clusterBy = outputClusterBy(*n, key)
				if len(clusterBy) == 0 {
					// Fall back to merge keys when the output is merge-mode but
					// has no explicit cluster_by — those are the natural
					// clustering columns for the table.
					if outputMode(*n, key) == "merge" {
						clusterBy = outputMergeKeys(*n, key)
					}
				}
				if len(clusterBy) > 0 {
					op = "cluster_alter"
					if len(clusterBy) > 4 {
						clusterBy = clusterBy[:4] // Delta's CLUSTER BY column limit
					}
				}
			}
			targets = append(targets, target{
				node: n, outputKey: key, tableID: tableID,
				operation: op, clusterBy: clusterBy,
			})
		}
	}
	if req.Node != "" && len(targets) == 0 {
		return nil, fmt.Errorf("optimize: node %q not found or not a transform in %s", req.Node, req.Dir)
	}

	sort.Slice(targets, func(i, j int) bool { return targets[i].tableID < targets[j].tableID })

	retain := req.RetainHours
	if retain == 0 {
		retain = defaultVacuumRetainHours
	}

	// One dispatcher closure per mode keeps the per-table loop identical.
	var dispatch func(ctx context.Context, op map[string]any) error
	if workspace.LoadWarehouse(s.workspace) == workspace.WarehouseLocal {
		dispatch = func(ctx context.Context, op map[string]any) error {
			_, err := s.runOperation(ctx, op)
			return err
		}
	} else {
		cfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("load AWS config: %w", err)
		}
		lc := lambda.NewFromConfig(cfg)
		functionName := pipelineRunnerLambdaName(pipelineName)
		dispatch = func(ctx context.Context, op map[string]any) error {
			body, mErr := json.Marshal(op)
			if mErr != nil {
				return mErr
			}
			out, iErr := lc.Invoke(ctx, &lambda.InvokeInput{
				FunctionName: aws.String(functionName),
				Payload:      body,
			})
			if iErr != nil {
				return fmt.Errorf("invoke %q: %w", functionName, iErr)
			}
			if out.FunctionError != nil && *out.FunctionError != "" {
				return fmt.Errorf("Lambda %q returned error: %s", functionName, string(out.Payload))
			}
			return nil
		}
	}

	results := make([]OptimizeTableResult, 0, len(targets))
	for _, t := range targets {
		res := OptimizeTableResult{
			Table:     t.tableID,
			Node:      t.node.ID,
			OutputKey: t.outputKey,
			Operation: t.operation,
			Status:    "ok",
		}
		// REC lets the runner refresh the system `tables` snapshot after the
		// out-of-band rewrite; populated from the same catalog/schema/pipeline
		// values the run path uses. run_id is freshly generated per op (32-char
		// hex, same shape as newRunID / the runner's uuid4().hex).
		record := map[string]any{
			"catalog":        catalog,
			"system_catalog": systemCatalog,
			"schema":         schema,
			"pipeline":       pipelineName,
			"node":           t.node.ID,
			"output_key":     t.outputKey,
			"run_id":         newRunID(),
		}

		var op map[string]any
		switch t.operation {
		case "cluster_alter":
			op = map[string]any{
				"_operation": "cluster_alter",
				"table":      t.tableID,
				"cluster_by": t.clusterBy,
				"record":     record,
			}
		default:
			op = map[string]any{
				"_operation": "optimize",
				"table":      t.tableID,
				"record":     record,
			}
		}
		if err := dispatch(ctx, op); err != nil {
			res.Status = "failed"
			res.Error = err.Error()
			results = append(results, res)
			continue
		}

		if req.Vacuum {
			vac := map[string]any{
				"_operation":   "vacuum",
				"table":        t.tableID,
				"retain_hours": retain,
				"record": map[string]any{
					"catalog":        catalog,
					"system_catalog": systemCatalog,
					"schema":         schema,
					"pipeline":       pipelineName,
					"node":           t.node.ID,
					"output_key":     t.outputKey,
					"run_id":         newRunID(),
				},
			}
			if err := dispatch(ctx, vac); err != nil {
				res.Status = "failed"
				res.Error = fmt.Sprintf("optimize ok but vacuum failed: %v", err)
				results = append(results, res)
				continue
			}
			res.Vacuumed = true
		}
		results = append(results, res)
	}
	return results, nil
}
