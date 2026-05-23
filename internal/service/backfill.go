package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// BackfillStageRequest describes one historical-window replay against a
// single transform node. The runner reads only partitions inside
// [from, to] (inclusive) and writes to a parallel staging table that the
// user inspects + promotes separately (production target untouched).
type BackfillStageRequest struct {
	Dir    string   // pipeline dir, relative to workspace
	Node   string   // transform node id
	From   []string // partition cursor tuple, inclusive
	To     []string // partition cursor tuple, inclusive
	Direct bool     // skip staging — write straight to canonical target (escape hatch)
}

// BackfillRun is the metadata recorded for one staged backfill. The
// staging Iceberg table is the durable artifact; this struct is just the
// pointer back to it. List() reconstructs these from Glue tag scans.
type BackfillRun struct {
	RunID         string    `json:"run_id"`
	Pipeline      string    `json:"pipeline"`
	Node          string    `json:"node"`
	OutputKey     string    `json:"output_key"`
	From          []string  `json:"from_cursor"`
	To            []string  `json:"to_cursor"`
	Direct        bool      `json:"direct"`
	TargetTable   string    `json:"target_table"`            // staging (or canonical, when Direct)
	CanonicalTable string   `json:"canonical_table"`         // production target this would promote into
	StartedAt     time.Time `json:"started_at"`
	StoppedAt     time.Time `json:"stopped_at,omitempty"`
	Status        string    `json:"status"`                  // ok | failed | running
	RowsWritten   int64     `json:"rows_written,omitempty"`
	ErrorMsg      string    `json:"error_msg,omitempty"`
}

// BackfillColumnInfo names one column on a staging table. The UI uses
// these to populate the dedup-column dropdown for append-mode promotes —
// otherwise the user has to remember column names off the top of their
// head, type one into a free-text field, and trust the placeholder.
type BackfillColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// BackfillDiff captures the comparison between a staging table and its
// canonical target over the backfill window. Shape kept narrow so the UI
// can render it with simple list-of-metrics widgets; deeper analysis lives
// in Athena queries the user can run by hand.
type BackfillDiff struct {
	RunID            string `json:"run_id"`
	StagingTable     string `json:"staging_table"`
	CanonicalTable   string `json:"canonical_table"`
	StagingRows      int64  `json:"staging_rows"`
	CanonicalRows    int64  `json:"canonical_rows"` // -1 when target doesn't exist yet
	SchemaMatches    bool   `json:"schema_matches"`
	SchemaDiff       string `json:"schema_diff,omitempty"` // empty when matches
	OutputMode       string `json:"output_mode"`
	MergeKeys        []string `json:"merge_keys,omitempty"`
	MatchingKeyRows  int64  `json:"matching_key_rows,omitempty"` // only set when merge_keys declared
	NewKeyRows       int64  `json:"new_key_rows,omitempty"`
	// Staging columns are surfaced so the UI's append-mode promote screen
	// can render a real column picker instead of a free-text input. The
	// list is always populated when the staging table is queryable; empty
	// only when the schema lookup itself failed.
	StagingColumns []BackfillColumnInfo `json:"staging_columns,omitempty"`
}

// BackfillDedupCheckResult is what `pipeline backfill diff --col <x>`
// would print: how many staging rows already match canonical on the
// proposed dedup column (would UPDATE) vs how many are new (would
// INSERT). Lets the user see the consequence of their column choice
// before they press Promote.
type BackfillDedupCheckResult struct {
	MatchingRows int64 `json:"matching_rows"`
	NewRows      int64 `json:"new_rows"`
}

// BackfillPromoteOpts gates the non-default mode promotions.
//   - append targets refuse a window-overlap promote unless one of these
//     two flags is set. ForceDedup runs a MERGE on the named key to drop
//     duplicates; AllowDuplicates accepts the dupe (`INSERT INTO`).
//   - replace targets refuse promotion unless they declare
//     replace_partitions (v2 — not implemented).
//   - merge targets need neither flag — MERGE INTO with declared keys is
//     idempotent by design.
type BackfillPromoteOpts struct {
	ForceDedup      string // append: column to MERGE on (must be unique in staging)
	AllowDuplicates bool   // append: accept dupes, plain INSERT INTO
}

// stagingSuffix is appended to the canonical table id to form the parallel
// staging table id: `<canonical>__backfill__<run_id>`.
const stagingSuffix = "__backfill__"

// Glue tag keys identifying staging tables. The Catalog page picks these
// up and tags the table as "staging — pending promote/discard".
const (
	glueTagBackfill       = "clavesa:backfill"
	glueTagBackfillRunID  = "clavesa:backfill-run-id"
	glueTagBackfillFrom   = "clavesa:backfill-from"
	glueTagBackfillTo     = "clavesa:backfill-to"
	glueTagBackfillNode   = "clavesa:backfill-node"
	glueTagBackfillCanon  = "clavesa:backfill-canonical-table"
	glueTagBackfillOutput = "clavesa:backfill-output-key"
)

// BackfillStage invokes the target transform Lambda directly with a
// backfill event payload (NOT via SFN — the orchestration module would
// fire the full DAG; we want one node). The runner reads only the [from,
// to] partition window and writes to the staging table. Returns the
// BackfillRun (staging table id + metadata) on success.
func (s *Service) BackfillStage(ctx context.Context, req BackfillStageRequest) (*BackfillRun, error) {
	if len(req.From) == 0 || len(req.To) == 0 {
		return nil, fmt.Errorf("backfill: --from and --to cursors are required")
	}
	if req.Node == "" {
		return nil, fmt.Errorf("backfill: --node is required")
	}

	abs := s.resolveDir(req.Dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}
	var node *graph.Node
	for i := range g.Nodes {
		if g.Nodes[i].ID == req.Node {
			node = &g.Nodes[i]
			break
		}
	}
	if node == nil {
		return nil, fmt.Errorf("backfill: node %q not found in %s", req.Node, req.Dir)
	}
	if node.Type != "transform" {
		return nil, fmt.Errorf("backfill: node %q is %s; only transforms can be backfilled", req.Node, node.Type)
	}

	pipelineName := strings.TrimSuffix(filepathBase(abs), "/")
	runID := newBackfillRunID()

	if workspace.LoadEnvironmentMode(s.workspace) == workspace.ModeLocal {
		return s.backfillStageLocal(ctx, req, &g, node, abs, pipelineName, runID)
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	// Resolve canonical target from the deployed Lambda's env vars
	// rather than the pipeline .tf — the .tf carries unresolved
	// terraform references (e.g. var.schema) that we can't statically
	// resolve without re-running Terraform's evaluator.
	functionName := fmt.Sprintf("%s-%s", pipelineName, req.Node)
	lc := lambda.NewFromConfig(cfg)
	canonicalTable, glueDB, outputKey, err := canonicalFromLambdaEnv(ctx, lc, functionName, req.Node)
	if err != nil {
		return nil, fmt.Errorf("resolve canonical target: %w", err)
	}

	var targetTable string
	if req.Direct {
		targetTable = canonicalTable
	} else {
		targetTable = canonicalTable + stagingSuffix + runID
	}

	// Build the event payload: same shape as the orchestration module's
	// Lambda Task Parameters, plus our `_backfill` override block. Fetch
	// the resolved inputs/outputs from the live SFN definition — that's
	// the only place the post-apply terraform references resolve.
	inputs, outputs, err := loadNodeIO(ctx, sfn.NewFromConfig(cfg), pipelineName, req.Node)
	if err != nil {
		return nil, fmt.Errorf("resolve node I/O from SFN definition: %w", err)
	}
	trigger := "backfill"
	if req.Direct {
		trigger = "backfill-direct"
	}
	payload := map[string]any{
		"inputs":   inputs,
		"outputs":  outputs,
		"_trigger": trigger,
		"_backfill": map[string]any{
			"node":           req.Node,
			"run_id":         runID,
			"from_cursor":    req.From,
			"to_cursor":      req.To,
			"target_outputs": map[string]string{outputKey: targetTable},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	started := time.Now()
	out, err := lc.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      body,
	})
	stopped := time.Now()
	run := &BackfillRun{
		RunID:          runID,
		Pipeline:       pipelineName,
		Node:           req.Node,
		OutputKey:      outputKey,
		From:           req.From,
		To:             req.To,
		Direct:         req.Direct,
		TargetTable:    targetTable,
		CanonicalTable: canonicalTable,
		StartedAt:      started,
		StoppedAt:      stopped,
		Status:         "ok",
	}
	if err != nil {
		run.Status = "failed"
		run.ErrorMsg = err.Error()
		return run, fmt.Errorf("invoke Lambda %q: %w", functionName, err)
	}
	if out.FunctionError != nil && *out.FunctionError != "" {
		run.Status = "failed"
		// Lambda response body carries the error JSON.
		run.ErrorMsg = string(out.Payload)
		return run, fmt.Errorf("Lambda %q returned error: %s", functionName, *out.FunctionError)
	}

	// Tag the staging table so List() and the Catalog UI can recognize it.
	// Skip on --direct: the production table doesn't get the staging tags.
	if !req.Direct {
		if err := tagStagingTable(ctx, glue.NewFromConfig(cfg), glueDB, lastSegment(targetTable), run); err != nil {
			// Tag failure is non-fatal — the table exists; user can find
			// it by name pattern. Log to caller but don't fail the run.
			run.ErrorMsg = fmt.Sprintf("staging table written but Glue tagging failed: %v", err)
		}
	}
	return run, nil
}

// BackfillList returns all open (un-promoted/un-discarded) staging tables
// for the pipeline by scanning Glue for tables matching `*__backfill__*`
// under the pipeline's database. The Glue tags carry the originating
// run_id, window, node — no separate registry needed.
func (s *Service) BackfillList(ctx context.Context, dir string) ([]BackfillRun, error) {
	if workspace.LoadEnvironmentMode(s.workspace) == workspace.ModeLocal {
		return s.backfillListLocal(dir)
	}
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}
	pipelineName := strings.TrimSuffix(filepathBase(abs), "/")
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	lc := lambda.NewFromConfig(cfg)
	_, glueDB, _, err := s.firstTransformDB(ctx, lc, &g, pipelineName)
	if err != nil {
		return nil, err
	}
	gc := glue.NewFromConfig(cfg)
	out, err := gc.GetTables(ctx, &glue.GetTablesInput{DatabaseName: aws.String(glueDB)})
	if err != nil {
		return nil, fmt.Errorf("list Glue tables in %q: %w", glueDB, err)
	}
	runs := make([]BackfillRun, 0)
	for _, t := range out.TableList {
		name := aws.ToString(t.Name)
		if !strings.Contains(name, stagingSuffix) {
			continue
		}
		params := t.Parameters
		if params[glueTagBackfill] != "true" {
			continue
		}
		run := BackfillRun{
			RunID:          params[glueTagBackfillRunID],
			Pipeline:       pipelineName,
			Node:           params[glueTagBackfillNode],
			OutputKey:      params[glueTagBackfillOutput],
			From:           splitCursor(params[glueTagBackfillFrom]),
			To:             splitCursor(params[glueTagBackfillTo]),
			TargetTable:    fmt.Sprintf("clavesa.%s.%s", glueDB, name),
			CanonicalTable: params[glueTagBackfillCanon],
			Status:         "ok",
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// BackfillDiff compares one staging table against its canonical target on
// row count, schema, and (when merge_keys are declared) per-key match count.
// Athena queries; no spark.
func (s *Service) BackfillDiff(ctx context.Context, dir, runID string) (*BackfillDiff, error) {
	if workspace.LoadEnvironmentMode(s.workspace) == workspace.ModeLocal {
		return s.backfillDiffLocal(ctx, dir, runID)
	}
	runs, err := s.BackfillList(ctx, dir)
	if err != nil {
		return nil, err
	}
	var run *BackfillRun
	for i := range runs {
		if runs[i].RunID == runID {
			run = &runs[i]
			break
		}
	}
	if run == nil {
		return nil, fmt.Errorf("backfill: run_id %q not found", runID)
	}

	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return nil, err
	}
	mode, mergeKeys := outputModeAndKeys(&g, run.Node, run.OutputKey)

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	ac := athena.NewFromConfig(cfg)
	wg := s.athenaWorkgroup(ctx, ac)
	bucket, err := s.athenaResultsBucket(ctx)
	if err != nil {
		return nil, err
	}
	out := bucket + "/athena-results/"

	diff := &BackfillDiff{
		RunID:          run.RunID,
		StagingTable:   run.TargetTable,
		CanonicalTable: run.CanonicalTable,
		OutputMode:     mode,
		MergeKeys:      mergeKeys,
	}
	stagingRows, err := athenaRowCount(ctx, ac, wg, out, run.TargetTable)
	if err != nil {
		return nil, fmt.Errorf("count staging rows: %w", err)
	}
	diff.StagingRows = stagingRows

	// Pull the staging table's columns up-front so the UI can render a
	// real dropdown on the append-mode promote screen. Best-effort —
	// schema lookup failure leaves the field empty, which the UI falls
	// back to a free-text input for.
	if cols, err := athenaColumns(ctx, ac, wg, out, run.TargetTable); err == nil {
		diff.StagingColumns = cols
	}

	canonicalExists, err := athenaTableExists(ctx, ac, wg, out, run.CanonicalTable)
	if err != nil {
		return nil, err
	}
	if !canonicalExists {
		diff.CanonicalRows = -1
		diff.SchemaMatches = true // nothing to compare
		return diff, nil
	}
	canonicalRows, err := athenaRowCount(ctx, ac, wg, out, run.CanonicalTable)
	if err != nil {
		return nil, err
	}
	diff.CanonicalRows = canonicalRows

	stagingSchema, err := athenaSchema(ctx, ac, wg, out, run.TargetTable)
	if err != nil {
		return nil, err
	}
	canonicalSchema, err := athenaSchema(ctx, ac, wg, out, run.CanonicalTable)
	if err != nil {
		return nil, err
	}
	if stagingSchema == canonicalSchema {
		diff.SchemaMatches = true
	} else {
		diff.SchemaMatches = false
		diff.SchemaDiff = fmt.Sprintf("staging:\n%s\ncanonical:\n%s", stagingSchema, canonicalSchema)
	}

	if len(mergeKeys) > 0 {
		match, newKey, err := athenaMergeKeyCounts(ctx, ac, wg, out, run.TargetTable, run.CanonicalTable, mergeKeys)
		if err == nil { // best-effort — schema mismatch makes this query nonsense
			diff.MatchingKeyRows = match
			diff.NewKeyRows = newKey
		}
	}
	return diff, nil
}

// BackfillPromote merges the staging table into the canonical target. Mode
// drives the SQL: merge → MERGE INTO with declared keys, append → INSERT
// INTO (with optional dedup), replace → not supported in v1.
//
// On success drops the staging table. On error leaves it in place so the
// user can fix the underlying issue and re-promote.
func (s *Service) BackfillPromote(ctx context.Context, dir, runID string, opts BackfillPromoteOpts) error {
	runs, err := s.BackfillList(ctx, dir)
	if err != nil {
		return err
	}
	var run *BackfillRun
	for i := range runs {
		if runs[i].RunID == runID {
			run = &runs[i]
			break
		}
	}
	if run == nil {
		return fmt.Errorf("backfill: run_id %q not found", runID)
	}

	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return err
	}
	mode, mergeKeys := outputModeAndKeys(&g, run.Node, run.OutputKey)

	// Validate mode/opts combo before invoking the Lambda — clearer errors.
	switch mode {
	case "merge":
		if len(mergeKeys) == 0 {
			return fmt.Errorf("promote: target output declares mode=merge with no merge_keys")
		}
	case "append":
		if opts.ForceDedup == "" && !opts.AllowDuplicates {
			return fmt.Errorf("promote: append-mode targets need --force-dedup <col> or --allow-duplicates; window overlap with target would dupe")
		}
	case "replace":
		return fmt.Errorf("promote: replace-mode targets need replace_partitions support (not in this version) — use --direct to recompute the target in place")
	default:
		return fmt.Errorf("promote: unsupported output mode %q", mode)
	}

	payload := map[string]any{
		"_operation":       "backfill_promote",
		"staging":          run.TargetTable,
		"target":           run.CanonicalTable,
		"mode":             mode,
		"merge_keys":       mergeKeys,
		"force_dedup":      opts.ForceDedup,
		"allow_duplicates": opts.AllowDuplicates,
	}

	if workspace.LoadEnvironmentMode(s.workspace) == workspace.ModeLocal {
		if _, err := s.runOperation(ctx, payload); err != nil {
			return err
		}
		// Runner drops the staging table on success; we drop the sidecar
		// so BackfillList stops surfacing this promoted run as "still
		// pending."
		if node := findGraphNode(&g, run.Node); node != nil {
			if _, glueDB, _, err := s.canonicalTargetFor(node, abs, strings.TrimSuffix(filepathBase(abs), "/")); err == nil {
				_ = s.deleteStagingSidecar(glueDB, lastSegment(run.TargetTable))
			}
		}
		return nil
	}

	// Spark-side MERGE via the runner Lambda. Same engine + IAM scope
	// that wrote the staging table — SparkSQL's MERGE INTO accepts
	// `UPDATE SET *` and `INSERT *`, no column enumeration needed.
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	lc := lambda.NewFromConfig(cfg)
	pipelineName := strings.TrimSuffix(filepathBase(abs), "/")
	functionName := fmt.Sprintf("%s-%s", pipelineName, run.Node)
	body, _ := json.Marshal(payload)
	out2, err := lc.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      body,
	})
	if err != nil {
		return fmt.Errorf("invoke %q: %w", functionName, err)
	}
	if out2.FunctionError != nil && *out2.FunctionError != "" {
		return fmt.Errorf("Lambda %q returned error: %s", functionName, string(out2.Payload))
	}
	return nil
}

// BackfillDedupCheck runs the same matching/new-key counts BackfillDiff
// runs for declared merge_keys, but for one user-supplied column on the
// append-mode promote screen. Lets the user see "X of Y staging rows
// would UPDATE existing rows, Z would INSERT" before they pick the
// dedup column. Validates `col` against the staging schema (anti-
// injection) before composing SQL.
func (s *Service) BackfillDedupCheck(ctx context.Context, dir, runID, col string) (*BackfillDedupCheckResult, error) {
	if col == "" {
		return nil, fmt.Errorf("dedup-check: column is required")
	}
	runs, err := s.BackfillList(ctx, dir)
	if err != nil {
		return nil, err
	}
	var run *BackfillRun
	for i := range runs {
		if runs[i].RunID == runID {
			run = &runs[i]
			break
		}
	}
	if run == nil {
		return nil, fmt.Errorf("backfill: run_id %q not found", runID)
	}
	if workspace.LoadEnvironmentMode(s.workspace) == workspace.ModeLocal {
		return s.backfillDedupCheckLocal(ctx, dir, run, col)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	ac := athena.NewFromConfig(cfg)
	wg := s.athenaWorkgroup(ctx, ac)
	bucket, err := s.athenaResultsBucket(ctx)
	if err != nil {
		return nil, err
	}
	out := bucket + "/athena-results/"

	// Validate col against staging columns so the SQL composition below
	// can't be hijacked by a crafted column name. Athena identifiers are
	// otherwise stringy and the matching/new-key SQL interpolates the
	// name directly.
	cols, err := athenaColumns(ctx, ac, wg, out, run.TargetTable)
	if err != nil {
		return nil, fmt.Errorf("lookup staging columns: %w", err)
	}
	valid := false
	for _, c := range cols {
		if c.Name == col {
			valid = true
			break
		}
	}
	if !valid {
		return nil, fmt.Errorf("column %q not found in staging table", col)
	}

	canonicalExists, err := athenaTableExists(ctx, ac, wg, out, run.CanonicalTable)
	if err != nil {
		return nil, err
	}
	if !canonicalExists {
		// Empty canonical → every staging row is new; nothing to UPDATE.
		n, err := athenaRowCount(ctx, ac, wg, out, run.TargetTable)
		if err != nil {
			return nil, err
		}
		return &BackfillDedupCheckResult{MatchingRows: 0, NewRows: n}, nil
	}
	matching, newKey, err := athenaMergeKeyCounts(ctx, ac, wg, out, run.TargetTable, run.CanonicalTable, []string{col})
	if err != nil {
		return nil, err
	}
	return &BackfillDedupCheckResult{MatchingRows: matching, NewRows: newKey}, nil
}

// BackfillDiscard drops the staging table without promoting. Routed
// through the runner Lambda for engine consistency with promote — same
// Spark/Iceberg path that created the staging table tears it down.
func (s *Service) BackfillDiscard(ctx context.Context, dir, runID string) error {
	runs, err := s.BackfillList(ctx, dir)
	if err != nil {
		return err
	}
	var run *BackfillRun
	for i := range runs {
		if runs[i].RunID == runID {
			run = &runs[i]
			break
		}
	}
	if run == nil {
		return fmt.Errorf("backfill: run_id %q not found", runID)
	}
	payload := map[string]any{
		"_operation": "backfill_discard",
		"staging":    run.TargetTable,
	}
	abs := s.resolveDir(dir)

	if workspace.LoadEnvironmentMode(s.workspace) == workspace.ModeLocal {
		if _, err := s.runOperation(ctx, payload); err != nil {
			return err
		}
		g, err := hclparser.Parse(abs)
		if err == nil {
			if node := findGraphNode(&g, run.Node); node != nil {
				if _, glueDB, _, err := s.canonicalTargetFor(node, abs, strings.TrimSuffix(filepathBase(abs), "/")); err == nil {
					_ = s.deleteStagingSidecar(glueDB, lastSegment(run.TargetTable))
				}
			}
		}
		return nil
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	lc := lambda.NewFromConfig(cfg)
	pipelineName := strings.TrimSuffix(filepathBase(abs), "/")
	functionName := fmt.Sprintf("%s-%s", pipelineName, run.Node)
	body, _ := json.Marshal(payload)
	out, err := lc.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      body,
	})
	if err != nil {
		return fmt.Errorf("invoke %q: %w", functionName, err)
	}
	if out.FunctionError != nil && *out.FunctionError != "" {
		return fmt.Errorf("Lambda %q returned error: %s", functionName, string(out.Payload))
	}
	return nil
}

// findGraphNode returns the node with the given ID from the parsed graph,
// or nil if not present. Used by the local promote/discard branches to look
// up the producing node's config when wiping the sidecar.
func findGraphNode(g *graph.PipelineGraph, nodeID string) *graph.Node {
	for i := range g.Nodes {
		if g.Nodes[i].ID == nodeID {
			return &g.Nodes[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// internals
// ---------------------------------------------------------------------------

func newBackfillRunID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func splitCursor(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "/")
}

func joinCursor(parts []string) string { return strings.Join(parts, "/") }

func lastSegment(tableID string) string {
	i := strings.LastIndex(tableID, ".")
	if i < 0 {
		return tableID
	}
	return tableID[i+1:]
}

func filepathBase(p string) string {
	// Avoid importing path/filepath into this file twice — small helper.
	i := strings.LastIndexAny(p, "/\\")
	if i < 0 {
		return p
	}
	return p[i+1:]
}

// canonicalFromLambdaEnv resolves the transform's canonical Iceberg
// table id by reading the deployed Lambda's environment variables
// (CLAVESA_CATALOG, CLAVESA_SCHEMA) and applying the same
// `clavesa.<encoded_db>.<node>__default` shape the runner uses in
// _table_id_for(). Reading from the Lambda avoids reproducing the
// Terraform variable-resolution dance and stays correct even when the
// pipeline .tf carries unresolved references.
func canonicalFromLambdaEnv(ctx context.Context, lc *lambda.Client, functionName, nodeID string) (string, string, string, error) {
	cfg, err := lc.GetFunctionConfiguration(ctx, &lambda.GetFunctionConfigurationInput{
		FunctionName: aws.String(functionName),
	})
	if err != nil {
		return "", "", "", fmt.Errorf("GetFunctionConfiguration %q: %w", functionName, err)
	}
	if cfg.Environment == nil || cfg.Environment.Variables == nil {
		return "", "", "", fmt.Errorf("Lambda %q has no environment variables", functionName)
	}
	env := cfg.Environment.Variables
	catalog := env["CLAVESA_CATALOG"]
	schema := env["CLAVESA_SCHEMA"]
	if catalog == "" || schema == "" {
		return "", "", "", fmt.Errorf("Lambda %q missing CLAVESA_CATALOG or CLAVESA_SCHEMA (catalog=%q schema=%q)", functionName, catalog, schema)
	}
	db := identutil.EncodeGlueDatabase(catalog, schema)
	outputKey := "default"
	target := fmt.Sprintf("clavesa.%s.%s__%s", db, nodeID, outputKey)
	return target, db, outputKey, nil
}

// canonicalTargetFor computes the canonical Iceberg table id (output-key
// "default") for the named transform, plus the Glue DB it lives in.
// Tracks the same `clavesa.<glue_db>.<node>__<output>` shape the
// runner uses in _table_id_for(key).
//
// Used by List/Diff/Promote/Discard paths that don't have a single Lambda
// to query — they fall back to the workspace manifest + pipeline name.
//
// pipelineDir gives the path to the pipeline so that an unresolved
// `schema = var.schema` reference in the node config can be resolved the
// same way `pipeline run --env local` resolves it: terraform.tfvars first,
// then variables.tf default, then sanitized pipeline name. Without that
// resolution the literal string "var.schema" leaks into the Glue DB name.
func (s *Service) canonicalTargetFor(node *graph.Node, pipelineDir, pipelineName string) (string, string, string, error) {
	ws, _ := workspace.Load(s.workspace)
	catalog := "clavesa"
	if ws != nil {
		catalog = ws.CatalogIdentifier()
	}
	schema, _ := node.Config["schema"].(string)
	if schema == "" || strings.HasPrefix(schema, "var.") {
		// The .tf carries `schema = var.schema` — same shape every
		// pipeline emitter produces — which the HCL parser preserves as
		// a literal string. Re-run the local-mode schema resolver instead.
		if pipelineDir != "" {
			schema = resolvePipelineSchema(pipelineDir, pipelineName)
		} else {
			schema = identutil.Sanitize(pipelineName)
		}
	}
	db := identutil.EncodeGlueDatabase(catalog, schema)
	outputKey := "default"
	target := fmt.Sprintf("clavesa.%s.%s__%s", db, node.ID, outputKey)
	return target, db, outputKey, nil
}

func (s *Service) firstTransformDB(ctx context.Context, lc *lambda.Client, g *graph.PipelineGraph, pipelineName string) (string, string, string, error) {
	for i := range g.Nodes {
		if g.Nodes[i].Type != "transform" {
			continue
		}
		fn := fmt.Sprintf("%s-%s", pipelineName, g.Nodes[i].ID)
		return canonicalFromLambdaEnv(ctx, lc, fn, g.Nodes[i].ID)
	}
	return "", "", "", fmt.Errorf("pipeline has no transforms")
}

// outputModeAndKeys reads the transform's output_definitions for the given
// key. Mirrors outputMode/outputMergeKeys in orchestration.go.
func outputModeAndKeys(g *graph.PipelineGraph, nodeID, key string) (string, []string) {
	for i := range g.Nodes {
		if g.Nodes[i].ID != nodeID {
			continue
		}
		n := g.Nodes[i]
		defs, _ := n.Config["output_definitions"].(map[string]interface{})
		def, _ := defs[key].(map[string]interface{})
		mode, _ := def["mode"].(string)
		var keys []string
		if raw, ok := def["merge_keys"].([]interface{}); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok {
					keys = append(keys, s)
				}
			}
		}
		if mode == "" {
			if len(keys) > 0 {
				mode = "merge"
			} else {
				mode = "replace"
			}
		}
		return mode, keys
	}
	return "replace", nil
}

// loadNodeIO pulls the resolved {inputs, outputs} pair from the deployed
// SFN state machine's definition. Post-apply, these are the concrete
// values (S3 paths, Iceberg table ids) the runner expects — we can't
// rebuild them from the .tf alone because module-output references resolve
// at apply time.
func loadNodeIO(ctx context.Context, client *sfn.Client, pipelineName, nodeID string) (any, any, error) {
	smName := "clavesa-" + pipelineName
	var nextToken *string
	var arn string
	for {
		out, err := client.ListStateMachines(ctx, &sfn.ListStateMachinesInput{MaxResults: 1000, NextToken: nextToken})
		if err != nil {
			return nil, nil, err
		}
		for _, sm := range out.StateMachines {
			if aws.ToString(sm.Name) == smName {
				arn = aws.ToString(sm.StateMachineArn)
				break
			}
		}
		if arn != "" || out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}
	if arn == "" {
		return nil, nil, fmt.Errorf("state machine %q not found — is the pipeline deployed?", smName)
	}
	desc, err := client.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(arn)})
	if err != nil {
		return nil, nil, err
	}
	def := aws.ToString(desc.Definition)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(def), &parsed); err != nil {
		return nil, nil, fmt.Errorf("parse SFN definition: %w", err)
	}
	states, _ := parsed["States"].(map[string]any)
	state, _ := states[nodeID].(map[string]any)
	if state == nil {
		// Parallel-branch nodes live inside a Parallel state's Branches.
		// Walk into any "Parallel" states to find the inner node.
		for _, s := range states {
			st, _ := s.(map[string]any)
			if st["Type"] != "Parallel" {
				continue
			}
			branches, _ := st["Branches"].([]any)
			for _, b := range branches {
				br, _ := b.(map[string]any)
				inner, _ := br["States"].(map[string]any)
				if cand, ok := inner[nodeID].(map[string]any); ok {
					state = cand
					break
				}
			}
			if state != nil {
				break
			}
		}
	}
	if state == nil {
		return nil, nil, fmt.Errorf("node %q not found in SFN definition", nodeID)
	}
	params, _ := state["Parameters"].(map[string]any)
	payload, _ := params["Payload"].(map[string]any)
	if payload == nil {
		return nil, nil, fmt.Errorf("node %q has no Parameters.Payload in SFN definition", nodeID)
	}
	return payload["inputs"], payload["outputs"], nil
}

// tagStagingTable writes the clavesa:backfill-* parameters onto the
// Glue table so List() can find it and the Catalog page can render the
// staging chip without an out-of-band registry.
func tagStagingTable(ctx context.Context, gc *glue.Client, glueDB, tableName string, run *BackfillRun) error {
	// Read first, set Parameters, UpdateTable. Iceberg-managed tables in
	// Glue accept Parameters merges as long as we preserve the
	// table_type / metadata_location keys.
	get, err := gc.GetTable(ctx, &glue.GetTableInput{
		DatabaseName: aws.String(glueDB),
		Name:         aws.String(tableName),
	})
	if err != nil {
		return err
	}
	t := get.Table
	if t.Parameters == nil {
		t.Parameters = map[string]string{}
	}
	t.Parameters[glueTagBackfill] = "true"
	t.Parameters[glueTagBackfillRunID] = run.RunID
	t.Parameters[glueTagBackfillFrom] = joinCursor(run.From)
	t.Parameters[glueTagBackfillTo] = joinCursor(run.To)
	t.Parameters[glueTagBackfillNode] = run.Node
	t.Parameters[glueTagBackfillCanon] = run.CanonicalTable
	t.Parameters[glueTagBackfillOutput] = run.OutputKey

	_, err = gc.UpdateTable(ctx, &glue.UpdateTableInput{
		DatabaseName: aws.String(glueDB),
		TableInput: &gluetypes.TableInput{
			Name:              t.Name,
			Description:       t.Description,
			Owner:             t.Owner,
			LastAccessTime:    t.LastAccessTime,
			LastAnalyzedTime:  t.LastAnalyzedTime,
			Retention:         t.Retention,
			StorageDescriptor: t.StorageDescriptor,
			PartitionKeys:     t.PartitionKeys,
			ViewOriginalText:  t.ViewOriginalText,
			ViewExpandedText:  t.ViewExpandedText,
			TableType:         t.TableType,
			Parameters:        t.Parameters,
			TargetTable:       t.TargetTable,
		},
	})
	return err
}

// athenaWorkgroup falls back to "primary" when the workspace's own
// workgroup isn't reachable. Same convention as runs_writer.
func (s *Service) athenaWorkgroup(ctx context.Context, ac *athena.Client) string {
	return "primary"
}

// athenaResultsBucket returns the workspace's pipeline-bucket as an
// s3:// prefix the Athena queries can dump results into. We read the
// workspace manifest for the bucket name.
func (s *Service) athenaResultsBucket(ctx context.Context) (string, error) {
	ws, err := workspace.Load(s.workspace)
	if err != nil {
		return "", fmt.Errorf("load workspace manifest: %w", err)
	}
	if ws == nil || ws.Name == "" {
		return "", fmt.Errorf("workspace manifest not found — cannot determine results bucket")
	}
	return "s3://" + ws.Name + "-clavesa", nil
}

func athenaRunQuery(ctx context.Context, ac *athena.Client, workgroup, outputLocation, sql string) error {
	out, err := ac.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString:         aws.String(sql),
		ResultConfiguration: &athenatypes.ResultConfiguration{OutputLocation: aws.String(outputLocation)},
		WorkGroup:           aws.String(workgroup),
	})
	if err != nil {
		return fmt.Errorf("StartQueryExecution: %w", err)
	}
	return athenaWait(ctx, ac, aws.ToString(out.QueryExecutionId))
}

func athenaQueryRows(ctx context.Context, ac *athena.Client, workgroup, outputLocation, sql string) ([][]string, error) {
	out, err := ac.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString:         aws.String(sql),
		ResultConfiguration: &athenatypes.ResultConfiguration{OutputLocation: aws.String(outputLocation)},
		WorkGroup:           aws.String(workgroup),
	})
	if err != nil {
		return nil, err
	}
	qid := aws.ToString(out.QueryExecutionId)
	if err := athenaWait(ctx, ac, qid); err != nil {
		return nil, err
	}
	res, err := ac.GetQueryResults(ctx, &athena.GetQueryResultsInput{QueryExecutionId: aws.String(qid)})
	if err != nil {
		return nil, err
	}
	rows := make([][]string, 0, len(res.ResultSet.Rows))
	for _, r := range res.ResultSet.Rows {
		cells := make([]string, len(r.Data))
		for i, d := range r.Data {
			cells[i] = aws.ToString(d.VarCharValue)
		}
		rows = append(rows, cells)
	}
	return rows, nil
}

func athenaWait(ctx context.Context, ac *athena.Client, qid string) error {
	for {
		info, err := ac.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: aws.String(qid)})
		if err != nil {
			return err
		}
		st := info.QueryExecution.Status.State
		switch st {
		case athenatypes.QueryExecutionStateSucceeded:
			return nil
		case athenatypes.QueryExecutionStateFailed, athenatypes.QueryExecutionStateCancelled:
			reason := aws.ToString(info.QueryExecution.Status.StateChangeReason)
			return fmt.Errorf("query %s %s: %s", qid, st, reason)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// athenaTableID strips the leading "clavesa." (Iceberg/Spark catalog
// identifier) from a fully-qualified table id, leaving "<db>.<table>"
// which is what Athena's AwsDataCatalog expects. Idempotent on
// already-Athena-shaped names.
func athenaTableID(fullID string) string {
	return strings.TrimPrefix(fullID, "clavesa.")
}

func athenaRowCount(ctx context.Context, ac *athena.Client, wg, out, table string) (int64, error) {
	rows, err := athenaQueryRows(ctx, ac, wg, out, fmt.Sprintf("SELECT COUNT(*) FROM %s", athenaTableID(table)))
	if err != nil {
		return 0, err
	}
	if len(rows) < 2 || len(rows[1]) == 0 {
		return 0, fmt.Errorf("unexpected COUNT(*) result shape: %v", rows)
	}
	var n int64
	if _, err := fmt.Sscanf(rows[1][0], "%d", &n); err != nil {
		return 0, fmt.Errorf("parse COUNT(*) %q: %w", rows[1][0], err)
	}
	return n, nil
}

func athenaTableExists(ctx context.Context, ac *athena.Client, wg, out, fullTableID string) (bool, error) {
	// fullTableID looks like "clavesa.<db>.<name>". Athena queries against
	// <db>.<name>; the catalog prefix doesn't survive INFORMATION_SCHEMA.
	parts := strings.SplitN(fullTableID, ".", 3)
	if len(parts) != 3 {
		return false, fmt.Errorf("expected clavesa.<db>.<table>, got %q", fullTableID)
	}
	rows, err := athenaQueryRows(ctx, ac, wg, out, fmt.Sprintf(
		"SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s' LIMIT 1",
		parts[1], parts[2],
	))
	if err != nil {
		return false, err
	}
	return len(rows) >= 2, nil
}

func athenaSchema(ctx context.Context, ac *athena.Client, wg, out, fullTableID string) (string, error) {
	cols, err := athenaColumns(ctx, ac, wg, out, fullTableID)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, c := range cols {
		fmt.Fprintf(&b, "  %s %s\n", c.Name, c.Type)
	}
	return b.String(), nil
}

// athenaColumns returns the structured column list for a table — the
// same data athenaSchema renders as text, but in a form the UI can use
// to populate a dropdown. Cheap one-query lookup against
// information_schema.columns.
func athenaColumns(ctx context.Context, ac *athena.Client, wg, out, fullTableID string) ([]BackfillColumnInfo, error) {
	parts := strings.SplitN(fullTableID, ".", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("expected clavesa.<db>.<table>, got %q", fullTableID)
	}
	rows, err := athenaQueryRows(ctx, ac, wg, out, fmt.Sprintf(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position",
		parts[1], parts[2],
	))
	if err != nil {
		return nil, err
	}
	cols := make([]BackfillColumnInfo, 0, len(rows))
	for i, r := range rows {
		if i == 0 {
			continue // header
		}
		if len(r) < 2 {
			continue
		}
		cols = append(cols, BackfillColumnInfo{Name: r[0], Type: r[1]})
	}
	return cols, nil
}


// athenaMergeKeyCounts returns (staging rows that already exist in
// canonical on the key, staging rows that are new). Counts are over
// distinct staging rows — matching + new always sums to staging
// row-count. The earlier shape used a plain JOIN which double-counted
// when canonical contained duplicates on the key (the COUNT(*) of a
// staging-JOIN-canonical pairs them all up); EXISTS clarifies the
// "would this row update something or insert?" question the UI is
// actually asking.
func athenaMergeKeyCounts(ctx context.Context, ac *athena.Client, wg, out, staging, canonical string, keys []string) (int64, int64, error) {
	s := athenaTableID(staging)
	c := athenaTableID(canonical)
	keyEq := make([]string, len(keys))
	for i, k := range keys {
		keyEq[i] = fmt.Sprintf("t.%s = s.%s", k, k)
	}
	on := strings.Join(keyEq, " AND ")
	match, err := athenaRowCount2(ctx, ac, wg, out, fmt.Sprintf(
		"SELECT COUNT(*) FROM %s s WHERE EXISTS (SELECT 1 FROM %s t WHERE %s)",
		s, c, on,
	))
	if err != nil {
		return 0, 0, err
	}
	newKey, err := athenaRowCount2(ctx, ac, wg, out, fmt.Sprintf(
		"SELECT COUNT(*) FROM %s s WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE %s)",
		s, c, on,
	))
	if err != nil {
		return 0, 0, err
	}
	return match, newKey, nil
}

func athenaRowCount2(ctx context.Context, ac *athena.Client, wg, out, sql string) (int64, error) {
	rows, err := athenaQueryRows(ctx, ac, wg, out, sql)
	if err != nil {
		return 0, err
	}
	if len(rows) < 2 || len(rows[1]) == 0 {
		return 0, fmt.Errorf("unexpected count result shape")
	}
	var n int64
	if _, err := fmt.Sscanf(rows[1][0], "%d", &n); err != nil {
		return 0, err
	}
	return n, nil
}

