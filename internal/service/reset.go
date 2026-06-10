package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/observability"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// PipelineResetRequest selects what `pipeline reset` drops: the canonical
// output tables of one transform (Node set) or every transform (Node
// empty), optionally plus the consumer-side CDF watermarks so incremental
// inputs replay from the start. Deployed infra (Lambda / SFN / IAM) is
// never touched — reset is a data operation, not a destroy.
type PipelineResetRequest struct {
	Dir               string // pipeline dir, workspace-relative (same convention as backfill)
	Node              string // "" = all transform nodes
	IncludeWatermarks bool
}

// ResetTarget is one canonical output table slated for (or confirmed)
// deletion.
type ResetTarget struct {
	Node      string `json:"node"`
	OutputKey string `json:"output_key"`
	Table     string `json:"table"` // <glueDB>.<segment>
	GlueDB    string `json:"glue_db"`
	Location  string `json:"location"` // local dir path OR s3:// prefix
}

// WatermarkTarget is one consumer-side CDF watermark slated for (or
// confirmed) deletion.
type WatermarkTarget struct {
	Consumer string `json:"consumer"`
	Alias    string `json:"alias"`
	Path     string `json:"path"` // file path or s3:// uri
}

// PipelineResetResult is the plan (PipelineResetPlan) or the receipt
// (PipelineReset). The plan lists everything that would be deleted; the
// receipt lists only what was actually deleted — targets that turned out
// not to exist are omitted.
type PipelineResetResult struct {
	Pipeline          string            `json:"pipeline"`
	Mode              string            `json:"mode"` // "local" | "cloud"
	TablesDropped     []ResetTarget     `json:"tables_dropped"`
	WatermarksCleared []WatermarkTarget `json:"watermarks_cleared"`
}

// resetNodePlan groups one node's targets so the cloud executor can keep
// the per-node delete ordering (watermarks → S3 data → Glue entry) while
// the flattened result stays in plan order.
type resetNodePlan struct {
	tables     []ResetTarget
	watermarks []WatermarkTarget
}

// resetPlan carries the resolved plan plus the environment facts the
// executor needs (mode, bucket) so execute doesn't re-derive them.
type resetPlan struct {
	pipeline string
	mode     workspace.Mode
	bucket   string // cloud only
	nodes    []resetNodePlan
}

func (p *resetPlan) result() *PipelineResetResult {
	res := &PipelineResetResult{
		Pipeline:          p.pipeline,
		Mode:              string(p.mode),
		TablesDropped:     []ResetTarget{},
		WatermarksCleared: []WatermarkTarget{},
	}
	for _, n := range p.nodes {
		res.TablesDropped = append(res.TablesDropped, n.tables...)
		res.WatermarksCleared = append(res.WatermarksCleared, n.watermarks...)
	}
	return res
}

// PipelineResetPlan resolves what a reset would delete, without deleting
// anything. CLI / UI show this as the confirmation list.
func (s *Service) PipelineResetPlan(ctx context.Context, req PipelineResetRequest) (*PipelineResetResult, error) {
	plan, err := s.resolveResetPlan(ctx, req)
	if err != nil {
		return nil, err
	}
	return plan.result(), nil
}

// PipelineReset derives the plan and executes the deletes. The returned
// result lists only the entries that were actually deleted: a planned
// table whose data didn't exist is omitted.
func (s *Service) PipelineReset(ctx context.Context, req PipelineResetRequest) (*PipelineResetResult, error) {
	plan, err := s.resolveResetPlan(ctx, req)
	if err != nil {
		return nil, err
	}
	if plan.mode == workspace.ModeLocal {
		return s.executeResetLocal(plan)
	}
	return s.executeResetCloud(ctx, plan)
}

// resolveResetPlan walks the pipeline graph in REVERSE topological order
// (consumers before producers): a concurrently-triggered consumer must
// never observe a half-dropped producer table while its own watermark
// still points into it, so the consumer side goes first.
func (s *Service) resolveResetPlan(ctx context.Context, req PipelineResetRequest) (*resetPlan, error) {
	abs := s.resolveDir(req.Dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}
	pipelineName := strings.TrimSuffix(filepathBase(abs), "/")

	order, err := topoSort(&g)
	if err != nil {
		return nil, fmt.Errorf("reset: %w", err)
	}
	nodeByID := map[string]*graph.Node{}
	for i := range g.Nodes {
		nodeByID[g.Nodes[i].ID] = &g.Nodes[i]
	}

	mode := workspace.LoadEnvironmentMode(s.workspace)
	plan := &resetPlan{pipeline: pipelineName, mode: mode}

	// Cloud: the Glue DB comes from the deployed Lambda's env vars
	// (CLAVESA_CATALOG / CLAVESA_SCHEMA) rather than the pipeline .tf —
	// same reasoning as backfill: the .tf carries unresolved terraform
	// references. The env is function-level (one runner Lambda per
	// pipeline since v2.2.0), so one resolution covers every node.
	cloudDB := ""
	if mode == workspace.ModeCloud {
		plan.bucket = workspace.PipelineBucket(s.workspace)
		if plan.bucket == "" {
			return nil, fmt.Errorf("reset: pipeline bucket not found — is the workspace deployed?")
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("load AWS config: %w", err)
		}
		lc := lambda.NewFromConfig(cfg)
		var first *graph.Node
		for _, id := range order {
			if n := nodeByID[id]; n != nil && n.Type == "transform" {
				first = n
				break
			}
		}
		if first == nil {
			return nil, fmt.Errorf("reset: pipeline %s has no transform nodes", req.Dir)
		}
		_, db, _, err := canonicalFromLambdaEnv(ctx, lc, pipelineRunnerLambdaName(pipelineName), first)
		if err != nil {
			return nil, fmt.Errorf("resolve canonical target: %w", err)
		}
		cloudDB = db
	}

	matched := false
	for i := len(order) - 1; i >= 0; i-- {
		n := nodeByID[order[i]]
		if n == nil || n.Type != "transform" {
			continue
		}
		if req.Node != "" && n.ID != req.Node {
			continue
		}
		matched = true

		glueDB := cloudDB
		if mode == workspace.ModeLocal {
			_, db, _, err := s.canonicalTargetFor(n, abs, pipelineName)
			if err != nil {
				return nil, fmt.Errorf("resolve canonical target for %q: %w", n.ID, err)
			}
			glueDB = db
		}
		if err := s.guardSystemDB(glueDB); err != nil {
			return nil, err
		}

		np := resetNodePlan{}
		defs, _ := n.Config["output_definitions"].(map[string]interface{})
		for _, key := range outputKeyList(*n) {
			seg := canonicalTableSegment(n.ID, defs, key)
			loc := ""
			if mode == workspace.ModeLocal {
				// Local writes land in the ADR-019 V2 layout
				// `<warehouse>/<catalog>/<schema>/<table>` (runner.py
				// _v2_layout_path pins it via the DB LOCATION clause);
				// older workspaces may still carry the legacy Hive
				// `<glueDB>.db/<table>` tree. ResolveLocalTablePath probes
				// both so the plan points at the directory that actually
				// holds the data.
				loc = observability.ResolveLocalTablePath(workspace.LocalWarehouseDir(s.workspace), glueDB, seg)
			} else {
				// Trailing slash keeps the delete table-scoped: without it
				// the prefix would also match sibling tables sharing the
				// segment as a name prefix, and `<pipeline>/<node>/_runtime/`
				// logic objects live outside `_warehouse/` entirely.
				loc = fmt.Sprintf("s3://%s/%s/_warehouse/%s.db/%s/", plan.bucket, pipelineName, glueDB, seg)
			}
			np.tables = append(np.tables, ResetTarget{
				Node:      n.ID,
				OutputKey: key,
				Table:     glueDB + "." + seg,
				GlueDB:    glueDB,
				Location:  loc,
			})
		}

		if req.IncludeWatermarks {
			aliases := make([]string, 0)
			for a := range incrementalInputAliases(*n) {
				aliases = append(aliases, a)
			}
			sort.Strings(aliases)
			for _, alias := range aliases {
				// Watermark files are named `<consumer>__<alias>.json` —
				// the alias half matches the runner's _watermark_uri and
				// the orchestration emitter's `id + "__" + alias`.
				name := n.ID + "__" + alias + ".json"
				path := ""
				if mode == workspace.ModeLocal {
					path = filepath.Join(abs, ".clavesa", "watermarks", name)
				} else {
					path = fmt.Sprintf("s3://%s/%s/_watermarks/%s", plan.bucket, pipelineName, name)
				}
				np.watermarks = append(np.watermarks, WatermarkTarget{
					Consumer: n.ID,
					Alias:    alias,
					Path:     path,
				})
			}
		}
		plan.nodes = append(plan.nodes, np)
	}

	if req.Node != "" && !matched {
		return nil, fmt.Errorf("reset: node %q not found in %s (or not a transform)", req.Node, req.Dir)
	}
	return plan, nil
}

// guardSystemDB refuses any reset touching the workspace's system Glue DB
// (`<system_catalog>__pipelines`, holding runs / node_runs / tables).
// Defense in depth — node canonical DBs should never resolve there, but
// the cost of a bug would be the workspace's entire run history.
func (s *Service) guardSystemDB(glueDB string) error {
	m, err := workspace.Load(s.workspace)
	if err != nil || m == nil {
		return nil // no manifest (bare test workspace) — nothing to compare against
	}
	if glueDB == identutil.EncodeGlueDatabase(m.SystemCatalogIdentifier(), "pipelines") {
		return fmt.Errorf("reset: refusing to drop tables in the system database %q", glueDB)
	}
	return nil
}

// executeResetLocal deletes warehouse table dirs and watermark files.
// The directory IS the table in local mode (Hadoop catalog) — no
// catalog calls needed.
func (s *Service) executeResetLocal(plan *resetPlan) (*PipelineResetResult, error) {
	res := &PipelineResetResult{
		Pipeline:          plan.pipeline,
		Mode:              string(plan.mode),
		TablesDropped:     []ResetTarget{},
		WatermarksCleared: []WatermarkTarget{},
	}
	for _, np := range plan.nodes {
		for _, w := range np.watermarks {
			if err := os.Remove(w.Path); err != nil {
				if os.IsNotExist(err) {
					continue // never written — omit from the receipt
				}
				return nil, fmt.Errorf("reset: clear watermark %s: %w", w.Path, err)
			}
			res.WatermarksCleared = append(res.WatermarksCleared, w)
		}
		for _, t := range np.tables {
			if _, err := os.Stat(t.Location); err != nil {
				if os.IsNotExist(err) {
					continue // table never materialized — omit from the receipt
				}
				return nil, fmt.Errorf("reset: stat %s: %w", t.Location, err)
			}
			if err := os.RemoveAll(t.Location); err != nil {
				return nil, fmt.Errorf("reset: drop table %s: %w", t.Table, err)
			}
			res.TablesDropped = append(res.TablesDropped, t)
		}
	}
	return res, nil
}

// executeResetCloud deletes, per node in consumer-first plan order:
// watermark S3 objects → the table's S3 warehouse prefix → the Glue
// catalog entry. Glue goes AFTER S3 on purpose: a mid-failure leaves a
// catalog entry pointing at an empty location (recoverable; the next run
// recreates the data) rather than orphaned data with no catalog entry.
func (s *Service) executeResetCloud(ctx context.Context, plan *resetPlan) (*PipelineResetResult, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return s.executeResetCloudWith(ctx, plan, s3.NewFromConfig(cfg), glue.NewFromConfig(cfg))
}

func (s *Service) executeResetCloudWith(ctx context.Context, plan *resetPlan, sc resetS3API, gc resetGlueAPI) (*PipelineResetResult, error) {
	res := &PipelineResetResult{
		Pipeline:          plan.pipeline,
		Mode:              string(plan.mode),
		TablesDropped:     []ResetTarget{},
		WatermarksCleared: []WatermarkTarget{},
	}
	for _, np := range plan.nodes {
		for _, w := range np.watermarks {
			// Exact key as prefix: list-then-delete tells us whether the
			// watermark existed (a bare DeleteObject succeeds either way),
			// so the receipt only lists watermarks that were really cleared.
			key := strings.TrimPrefix(w.Path, "s3://"+plan.bucket+"/")
			n, err := deleteS3Prefix(ctx, sc, plan.bucket, key)
			if err != nil {
				return nil, fmt.Errorf("reset: clear watermark %s: %w", w.Path, err)
			}
			if n > 0 {
				res.WatermarksCleared = append(res.WatermarksCleared, w)
			}
		}
		for _, t := range np.tables {
			prefix := strings.TrimPrefix(t.Location, "s3://"+plan.bucket+"/")
			objects, err := deleteS3Prefix(ctx, sc, plan.bucket, prefix)
			if err != nil {
				return nil, fmt.Errorf("reset: drop table data %s: %w", t.Table, err)
			}
			_, seg, err := splitDBTable(t.Table)
			if err != nil {
				return nil, fmt.Errorf("reset: %w", err)
			}
			catalogDeleted, err := deleteGlueTables(ctx, gc, t.GlueDB, []string{seg})
			if err != nil {
				return nil, fmt.Errorf("reset: drop table %s: %w", t.Table, err)
			}
			if objects > 0 || catalogDeleted > 0 {
				res.TablesDropped = append(res.TablesDropped, t)
			}
		}
	}
	return res, nil
}

// resetS3API is the subset of the AWS SDK v2 S3 client reset uses.
// Narrow on purpose — keeps the test stub small and the dependency
// surface obvious (mirrors internal/delta/s3fs.S3API).
type resetS3API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// resetGlueAPI is the subset of the Glue client reset uses.
type resetGlueAPI interface {
	DeleteTable(ctx context.Context, params *glue.DeleteTableInput, optFns ...func(*glue.Options)) (*glue.DeleteTableOutput, error)
}

// deleteS3Prefix deletes every object under bucket/prefix, paging the
// listing and batching deletes at S3's 1000-key DeleteObjects limit.
// Returns the number of objects deleted.
func deleteS3Prefix(ctx context.Context, client resetS3API, bucket, prefix string) (int, error) {
	deleted := 0
	var continuation *string
	for {
		page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuation,
		})
		if err != nil {
			return deleted, fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
		}
		ids := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			ids = append(ids, s3types.ObjectIdentifier{Key: obj.Key})
		}
		for len(ids) > 0 {
			batch := ids
			if len(batch) > 1000 {
				batch = ids[:1000]
			}
			ids = ids[len(batch):]
			out, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3types.Delete{Objects: batch, Quiet: aws.Bool(true)},
			})
			if err != nil {
				return deleted, fmt.Errorf("delete objects under s3://%s/%s: %w", bucket, prefix, err)
			}
			if len(out.Errors) > 0 {
				e := out.Errors[0]
				return deleted, fmt.Errorf("delete s3://%s/%s: %s (%s)", bucket, aws.ToString(e.Key), aws.ToString(e.Message), aws.ToString(e.Code))
			}
			deleted += len(batch)
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			return deleted, nil
		}
		continuation = page.NextContinuationToken
	}
}

// deleteGlueTables deletes the named tables from glueDB. A table that
// doesn't exist (EntityNotFoundException) is a no-op, matching the
// local-mode "absent → omit" posture. Returns the number of tables that
// existed and were deleted.
func deleteGlueTables(ctx context.Context, client resetGlueAPI, glueDB string, names []string) (int, error) {
	deleted := 0
	for _, name := range names {
		_, err := client.DeleteTable(ctx, &glue.DeleteTableInput{
			DatabaseName: aws.String(glueDB),
			Name:         aws.String(name),
		})
		if err != nil {
			var enf *gluetypes.EntityNotFoundException
			if errors.As(err, &enf) {
				continue
			}
			var denied *gluetypes.AccessDeniedException
			if errors.As(err, &denied) {
				return deleted, fmt.Errorf("delete glue table %s.%s: Glue table delete denied — Lake Formation accounts need a DROP grant on %s.* for the caller; grant it (or use the AWS console) and retry: %w", glueDB, name, glueDB, err)
			}
			return deleted, fmt.Errorf("delete glue table %s.%s: %w", glueDB, name, err)
		}
		deleted++
	}
	return deleted, nil
}
