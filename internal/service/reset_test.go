package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// resetTestPipeline lays out a three-transform chain raw -> split -> agg
// in a temp workspace. split has two outputs (default + errors) and an
// incremental input, so it exercises both the multi-output table-segment
// rule and watermark enumeration.
func resetTestPipeline(t *testing.T) (ws, dir string) {
	t.Helper()
	ws = t.TempDir()
	dir = "demo"
	pdir := filepath.Join(ws, dir)
	if err := os.MkdirAll(pdir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "raw" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "raw"
  sql    = "SELECT 1 AS id"
}

module "split" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "split"
  sql    = "SELECT * FROM x"
  incremental_inputs = ["x"]
  inputs = {
    x = module.raw.outputs["default"]
  }
  output_definitions = { default = {}, errors = {} }
}

module "agg" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "agg"
  sql    = "SELECT COUNT(*) FROM y"
  inputs = {
    y = module.split.outputs["default"]
  }
}
`
	if err := os.WriteFile(filepath.Join(pdir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	return ws, dir
}

// TestPipelineResetPlan locks the plan shape: reverse-topological
// (consumer-first) node order, bare vs `__<key>`-suffixed table segments,
// and watermark enumeration from incremental_inputs. Nothing on disk —
// plan is pure resolution.
func TestPipelineResetPlan(t *testing.T) {
	t.Parallel()
	ws, dir := resetTestPipeline(t)
	svc := New(ws)

	res, err := svc.PipelineResetPlan(context.Background(), PipelineResetRequest{
		Dir: dir, IncludeWatermarks: true,
	})
	if err != nil {
		t.Fatalf("PipelineResetPlan: %v", err)
	}
	if res.Pipeline != "demo" || res.Mode != "local" {
		t.Errorf("pipeline/mode = %q/%q, want demo/local", res.Pipeline, res.Mode)
	}

	// Reverse topo of raw -> split -> agg is agg, split, raw; split's two
	// outputs sort default before errors.
	wantTables := []string{
		"clavesa__demo.agg",
		"clavesa__demo.split__default",
		"clavesa__demo.split__errors",
		"clavesa__demo.raw",
	}
	if len(res.TablesDropped) != len(wantTables) {
		t.Fatalf("TablesDropped = %+v, want %d entries", res.TablesDropped, len(wantTables))
	}
	for i, want := range wantTables {
		if res.TablesDropped[i].Table != want {
			t.Errorf("TablesDropped[%d].Table = %q, want %q", i, res.TablesDropped[i].Table, want)
		}
		if res.TablesDropped[i].GlueDB != "clavesa__demo" {
			t.Errorf("TablesDropped[%d].GlueDB = %q, want clavesa__demo", i, res.TablesDropped[i].GlueDB)
		}
	}
	// Multi-output segment naming carries the output key.
	if res.TablesDropped[1].Node != "split" || res.TablesDropped[1].OutputKey != "default" {
		t.Errorf("split default target = %+v", res.TablesDropped[1])
	}
	// Local locations live under the workspace-shared warehouse in the
	// ADR-019 V2 layout `<warehouse>/<catalog>/<schema>/<table>` (nothing
	// on disk here, so the probe falls through to the canonical V2 path).
	wantLoc := filepath.Join(ws, ".clavesa", "warehouse", "clavesa", "demo", "split__errors")
	if res.TablesDropped[2].Location != wantLoc {
		t.Errorf("split__errors location = %q, want %q", res.TablesDropped[2].Location, wantLoc)
	}

	// Only split declares incremental_inputs.
	if len(res.WatermarksCleared) != 1 {
		t.Fatalf("WatermarksCleared = %+v, want 1 entry", res.WatermarksCleared)
	}
	w := res.WatermarksCleared[0]
	if w.Consumer != "split" || w.Alias != "x" {
		t.Errorf("watermark = %+v, want consumer=split alias=x", w)
	}
	wantWM := filepath.Join(ws, dir, ".clavesa", "watermarks", "split__x.json")
	if w.Path != wantWM {
		t.Errorf("watermark path = %q, want %q", w.Path, wantWM)
	}
}

// TestPipelineResetPlanWatermarksOptOut: IncludeWatermarks=false plans no
// watermark deletions.
func TestPipelineResetPlanWatermarksOptOut(t *testing.T) {
	t.Parallel()
	ws, dir := resetTestPipeline(t)
	svc := New(ws)
	res, err := svc.PipelineResetPlan(context.Background(), PipelineResetRequest{Dir: dir})
	if err != nil {
		t.Fatalf("PipelineResetPlan: %v", err)
	}
	if len(res.WatermarksCleared) != 0 {
		t.Errorf("WatermarksCleared = %+v, want empty", res.WatermarksCleared)
	}
}

// TestPipelineResetPlanNodeFilter: --node selects exactly one transform's
// outputs (and only its watermarks).
func TestPipelineResetPlanNodeFilter(t *testing.T) {
	t.Parallel()
	ws, dir := resetTestPipeline(t)
	svc := New(ws)
	res, err := svc.PipelineResetPlan(context.Background(), PipelineResetRequest{
		Dir: dir, Node: "split", IncludeWatermarks: true,
	})
	if err != nil {
		t.Fatalf("PipelineResetPlan: %v", err)
	}
	if len(res.TablesDropped) != 2 {
		t.Fatalf("TablesDropped = %+v, want split's 2 outputs only", res.TablesDropped)
	}
	for _, tgt := range res.TablesDropped {
		if tgt.Node != "split" {
			t.Errorf("unexpected node %q in filtered plan", tgt.Node)
		}
	}
	if len(res.WatermarksCleared) != 1 || res.WatermarksCleared[0].Consumer != "split" {
		t.Errorf("WatermarksCleared = %+v, want split__x only", res.WatermarksCleared)
	}
}

// TestPipelineResetPlanUnknownNode: a node that doesn't exist (or isn't a
// transform) is a hard error, not an empty plan.
func TestPipelineResetPlanUnknownNode(t *testing.T) {
	t.Parallel()
	ws, dir := resetTestPipeline(t)
	svc := New(ws)
	_, err := svc.PipelineResetPlan(context.Background(), PipelineResetRequest{Dir: dir, Node: "nope"})
	if err == nil || !strings.Contains(err.Error(), `node "nope" not found`) {
		t.Errorf("err = %v, want node-not-found", err)
	}
}

// TestPipelineResetLocal drives the local executor end-to-end against a
// fake warehouse: selected table dirs and watermark files are removed, a
// sibling table from another pipeline survives, and the receipt lists
// exactly what existed — the never-materialized `agg` table and the
// never-written watermark are omitted. Tables are laid out in BOTH
// warehouse layouts — split's outputs in the ADR-019 V2 tree
// (`<warehouse>/<catalog>/<schema>/<table>`, what the runner writes
// today) and raw in the legacy Hive tree (`<glueDB>.db/<table>`) — so
// the location probe's back-compat fallback is covered too.
func TestPipelineResetLocal(t *testing.T) {
	t.Parallel()
	ws, dir := resetTestPipeline(t)
	svc := New(ws)

	v2Dir := filepath.Join(ws, ".clavesa", "warehouse", "clavesa", "demo")
	for _, seg := range []string{"split__default", "split__errors", "sibling_table"} {
		mustWriteResetFile(t, filepath.Join(v2Dir, seg, "_delta_log", "00000000000000000000.json"))
	}
	legacyDir := filepath.Join(ws, ".clavesa", "warehouse", "clavesa__demo.db")
	mustWriteResetFile(t, filepath.Join(legacyDir, "raw", "_delta_log", "00000000000000000000.json"))
	// agg intentionally NOT materialized.
	wmFile := filepath.Join(ws, dir, ".clavesa", "watermarks", "split__x.json")
	mustWriteResetFile(t, wmFile)
	otherWM := filepath.Join(ws, dir, ".clavesa", "watermarks", "other__y.json")
	mustWriteResetFile(t, otherWM)

	res, err := svc.PipelineReset(context.Background(), PipelineResetRequest{
		Dir: dir, IncludeWatermarks: true,
	})
	if err != nil {
		t.Fatalf("PipelineReset: %v", err)
	}

	gotTables := make([]string, 0, len(res.TablesDropped))
	for _, tgt := range res.TablesDropped {
		gotTables = append(gotTables, tgt.Table)
		if _, err := os.Stat(tgt.Location); !os.IsNotExist(err) {
			t.Errorf("table dir %s should be gone (stat err = %v)", tgt.Location, err)
		}
	}
	// agg never existed, so the receipt omits it; consumer-first order kept.
	want := []string{"clavesa__demo.split__default", "clavesa__demo.split__errors", "clavesa__demo.raw"}
	if len(gotTables) != len(want) {
		t.Fatalf("TablesDropped = %v, want %v", gotTables, want)
	}
	for i := range want {
		if gotTables[i] != want[i] {
			t.Errorf("TablesDropped[%d] = %q, want %q", i, gotTables[i], want[i])
		}
	}

	if _, err := os.Stat(filepath.Join(v2Dir, "sibling_table")); err != nil {
		t.Errorf("sibling table must survive a pipeline reset: %v", err)
	}
	if _, err := os.Stat(wmFile); !os.IsNotExist(err) {
		t.Errorf("watermark %s should be gone", wmFile)
	}
	if _, err := os.Stat(otherWM); err != nil {
		t.Errorf("unrelated watermark must survive: %v", err)
	}
	if len(res.WatermarksCleared) != 1 || res.WatermarksCleared[0].Alias != "x" {
		t.Errorf("WatermarksCleared = %+v, want exactly split__x", res.WatermarksCleared)
	}
}

// TestPipelineResetLocalAbsentWatermark: planning a watermark that was
// never written is fine — it just doesn't show up in the receipt.
func TestPipelineResetLocalAbsentWatermark(t *testing.T) {
	t.Parallel()
	ws, dir := resetTestPipeline(t)
	svc := New(ws)
	res, err := svc.PipelineReset(context.Background(), PipelineResetRequest{
		Dir: dir, IncludeWatermarks: true,
	})
	if err != nil {
		t.Fatalf("PipelineReset: %v", err)
	}
	if len(res.WatermarksCleared) != 0 || len(res.TablesDropped) != 0 {
		t.Errorf("nothing on disk, receipt should be empty: %+v", res)
	}
}

// TestResetSystemDBGuard: a reset that resolves to the workspace's system
// Glue DB (`<system_catalog>__pipelines`) is refused before any delete.
// Manifest catalog == system catalog plus a pipeline named "pipelines"
// is the only way to force the collision from public inputs.
func TestResetSystemDBGuard(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	manifest := `{"name":"ws","cloud":"aws","version":1,"catalog":"sys","system_catalog":"sys"}`
	if err := os.WriteFile(filepath.Join(ws, "clavesa.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	pdir := filepath.Join(ws, "pipelines")
	if err := os.MkdirAll(pdir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "t1" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v2.6.0"
  name   = "t1"
  sql    = "SELECT 1"
}
`
	if err := os.WriteFile(filepath.Join(pdir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(ws)
	_, err := svc.PipelineResetPlan(context.Background(), PipelineResetRequest{Dir: "pipelines"})
	if err == nil || !strings.Contains(err.Error(), "system database") {
		t.Errorf("err = %v, want system-DB refusal", err)
	}

	// Direct guard coverage: non-system DB passes.
	if err := svc.guardSystemDB("sys__demo"); err != nil {
		t.Errorf("guardSystemDB(sys__demo) = %v, want nil", err)
	}
	if err := svc.guardSystemDB("sys__pipelines"); err == nil {
		t.Errorf("guardSystemDB(sys__pipelines) should refuse")
	}
}

func mustWriteResetFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fakeResetS3 implements resetS3API over an in-memory key set with a
// configurable list page size, so paging and batching are both
// observable.
type fakeResetS3 struct {
	keys     map[string]bool
	pageSize int
	batches  []int // size of each DeleteObjects call
}

func (f *fakeResetS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(in.Prefix)
	matched := make([]string, 0)
	for k := range f.keys {
		if strings.HasPrefix(k, prefix) {
			matched = append(matched, k)
		}
	}
	// Deterministic paging order.
	for i := 0; i < len(matched); i++ {
		for j := i + 1; j < len(matched); j++ {
			if matched[j] < matched[i] {
				matched[i], matched[j] = matched[j], matched[i]
			}
		}
	}
	// Key-cursor token (everything after the token key), like real S3 —
	// positional tokens would break once the caller deletes prior pages.
	if tok := aws.ToString(in.ContinuationToken); tok != "" {
		i := 0
		for i < len(matched) && matched[i] <= tok {
			i++
		}
		matched = matched[i:]
	}
	end := f.pageSize
	if f.pageSize <= 0 || end > len(matched) {
		end = len(matched)
	}
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(end < len(matched))}
	for _, k := range matched[:end] {
		key := k
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(key)})
	}
	if end < len(matched) {
		out.NextContinuationToken = aws.String(matched[end-1])
	}
	return out, nil
}

func (f *fakeResetS3) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.batches = append(f.batches, len(in.Delete.Objects))
	for _, o := range in.Delete.Objects {
		delete(f.keys, aws.ToString(o.Key))
	}
	return &s3.DeleteObjectsOutput{}, nil
}

// TestDeleteS3PrefixPagingAndScope: two list pages get drained, and keys
// outside the prefix are untouched.
func TestDeleteS3PrefixPagingAndScope(t *testing.T) {
	t.Parallel()
	f := &fakeResetS3{
		keys: map[string]bool{
			"demo/_warehouse/db.db/trips/a": true,
			"demo/_warehouse/db.db/trips/b": true,
			"demo/_warehouse/db.db/trips/c": true,
			// Sibling table sharing the name prefix — the trailing slash
			// in the caller's prefix keeps it out of scope.
			"demo/_warehouse/db.db/trips_extra/a": true,
			"demo/_runtime/logic.py":              true,
		},
		pageSize: 2,
	}
	n, err := deleteS3Prefix(context.Background(), f, "bkt", "demo/_warehouse/db.db/trips/")
	if err != nil {
		t.Fatalf("deleteS3Prefix: %v", err)
	}
	if n != 3 {
		t.Errorf("deleted = %d, want 3", n)
	}
	if len(f.batches) != 2 {
		t.Errorf("DeleteObjects calls = %v, want one per list page (2)", f.batches)
	}
	for _, k := range []string{"demo/_warehouse/db.db/trips_extra/a", "demo/_runtime/logic.py"} {
		if !f.keys[k] {
			t.Errorf("out-of-prefix key %s must survive", k)
		}
	}
	for k := range f.keys {
		if strings.HasPrefix(k, "demo/_warehouse/db.db/trips/") {
			t.Errorf("in-prefix key %s should be gone", k)
		}
	}
}

// TestDeleteS3PrefixBatching: a single list page bigger than S3's
// 1000-key DeleteObjects limit splits into ≤1000-key batches.
func TestDeleteS3PrefixBatching(t *testing.T) {
	t.Parallel()
	f := &fakeResetS3{keys: map[string]bool{}, pageSize: 1500}
	for i := 0; i < 1500; i++ {
		f.keys[fmt.Sprintf("p/%05d", i)] = true
	}
	n, err := deleteS3Prefix(context.Background(), f, "bkt", "p/")
	if err != nil {
		t.Fatalf("deleteS3Prefix: %v", err)
	}
	if n != 1500 {
		t.Errorf("deleted = %d, want 1500", n)
	}
	if len(f.batches) != 2 || f.batches[0] != 1000 || f.batches[1] != 500 {
		t.Errorf("batches = %v, want [1000 500]", f.batches)
	}
}

// fakeResetGlue implements resetGlueAPI: missing[name] returns
// EntityNotFoundException, denied[name] returns the Lake Formation
// AccessDeniedException.
type fakeResetGlue struct {
	deleted []string
	missing map[string]bool
	denied  map[string]bool
}

func (f *fakeResetGlue) DeleteTable(_ context.Context, in *glue.DeleteTableInput, _ ...func(*glue.Options)) (*glue.DeleteTableOutput, error) {
	name := aws.ToString(in.Name)
	if f.missing[name] {
		return nil, &gluetypes.EntityNotFoundException{Message: aws.String("not found")}
	}
	if f.denied[name] {
		return nil, &gluetypes.AccessDeniedException{Message: aws.String("denied")}
	}
	f.deleted = append(f.deleted, aws.ToString(in.DatabaseName)+"."+name)
	return &glue.DeleteTableOutput{}, nil
}

// TestDeleteGlueTables: named tables only, EntityNotFound is a no-op
// (not an error, not counted).
func TestDeleteGlueTables(t *testing.T) {
	t.Parallel()
	f := &fakeResetGlue{missing: map[string]bool{"gone": true}}
	n, err := deleteGlueTables(context.Background(), f, "db1", []string{"trips", "gone", "agg"})
	if err != nil {
		t.Fatalf("deleteGlueTables: %v", err)
	}
	if n != 2 {
		t.Errorf("deleted count = %d, want 2", n)
	}
	want := []string{"db1.trips", "db1.agg"}
	if len(f.deleted) != len(want) || f.deleted[0] != want[0] || f.deleted[1] != want[1] {
		t.Errorf("deleted = %v, want %v", f.deleted, want)
	}
}

// TestDeleteGlueTablesLakeFormationDenied: an AccessDeniedException gets
// the actionable Lake Formation DROP-grant message, wrapping the cause.
func TestDeleteGlueTablesLakeFormationDenied(t *testing.T) {
	t.Parallel()
	f := &fakeResetGlue{denied: map[string]bool{"trips": true}}
	_, err := deleteGlueTables(context.Background(), f, "db1", []string{"trips"})
	if err == nil || !strings.Contains(err.Error(), "Lake Formation") || !strings.Contains(err.Error(), "DROP grant") {
		t.Errorf("err = %v, want actionable Lake Formation message", err)
	}
	var denied *gluetypes.AccessDeniedException
	if !errors.As(err, &denied) {
		t.Errorf("cause should remain unwrappable as AccessDeniedException")
	}
}
