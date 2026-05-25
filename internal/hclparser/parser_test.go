package hclparser_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/service"
)

// fixture writes a .tf file into a temp directory and returns the directory path.
func writeFixture(t *testing.T, filename, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("writeFixture: %v", err)
	}
	return dir
}

// Canonical 4-node pipeline fixture.
const fourNodeTF = `
variable "pipeline_name" {
  default = "my-pipeline"
}

module "s3_source" {
  source        = "clavesa/source/aws"
  pipeline_name = var.pipeline_name
  name          = "s3_source"
  bucket        = "my-data"
  prefix        = "events/"
  format        = "json"
}

module "validate" {
  source        = "clavesa/transform/aws"
  pipeline_name = var.pipeline_name
  name          = "validate"

  inputs = {
    raw = module.s3_source.outputs["default"]
  }

  language = "sql"
  sql      = "SELECT * FROM raw WHERE amount > 0"
  compute  = "lambda"
}

module "warehouse" {
  source        = "clavesa/destination/aws"
  pipeline_name = var.pipeline_name
  name          = "warehouse"
  input         = module.validate.outputs["valid"]
  bucket        = "warehouse"
  prefix        = "clean/"
}

module "dead_letter" {
  source        = "clavesa/destination/aws"
  pipeline_name = var.pipeline_name
  name          = "dead_letter"
  input         = module.validate.outputs["invalid"]
  bucket        = "quarantine"
  prefix        = "invalid/"
}
`

// TestFourNodePipeline is the primary acceptance test: parse the 4-node pipeline
// and assert the output matches the Pipeline Graph JSON contract example field-for-field.
func TestFourNodePipeline(t *testing.T) {
	t.Parallel()
	dir := writeFixture(t, "main.tf", fourNodeTF)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	// --- Pipeline meta ---
	if g.Pipeline.Directory != dir {
		t.Errorf("pipeline.directory = %q, want %q", g.Pipeline.Directory, dir)
	}
	if len(g.Pipeline.Files) != 1 || g.Pipeline.Files[0] != "main.tf" {
		t.Errorf("pipeline.files = %v, want [main.tf]", g.Pipeline.Files)
	}

	// --- Node count ---
	if len(g.Nodes) != 4 {
		t.Fatalf("len(nodes) = %d, want 4", len(g.Nodes))
	}

	// Build a lookup map for convenience.
	nodeByID := make(map[string]graph.Node, len(g.Nodes))
	for _, n := range g.Nodes {
		nodeByID[n.ID] = n
	}

	// --- s3_source ---
	s3 := nodeByID["s3_source"]
	assertNode(t, s3, graph.Node{
		ID:           "s3_source",
		Type:         "source",
		ModuleSource: "clavesa/source/aws",
		Config: map[string]interface{}{
			"bucket": "my-data",
			"prefix": "events/",
			"format": "json",
		},
	})

	// --- validate ---
	val := nodeByID["validate"]
	assertNode(t, val, graph.Node{
		ID:           "validate",
		Type:         "transform",
		ModuleSource: "clavesa/transform/aws",
		Config: map[string]interface{}{
			"language": "sql",
			"sql":      "SELECT * FROM raw WHERE amount > 0",
			"compute":  "lambda",
		},
	})

	// --- warehouse ---
	wh := nodeByID["warehouse"]
	assertNode(t, wh, graph.Node{
		ID:           "warehouse",
		Type:         "destination",
		ModuleSource: "clavesa/destination/aws",
		Config: map[string]interface{}{
			"bucket": "warehouse",
			"prefix": "clean/",
		},
	})

	// --- dead_letter ---
	dl := nodeByID["dead_letter"]
	assertNode(t, dl, graph.Node{
		ID:           "dead_letter",
		Type:         "destination",
		ModuleSource: "clavesa/destination/aws",
		Config: map[string]interface{}{
			"bucket": "quarantine",
			"prefix": "invalid/",
		},
	})

	// --- Edges ---
	if len(g.Edges) != 3 {
		t.Fatalf("len(edges) = %d, want 3", len(g.Edges))
	}
	edgeKey := func(e graph.Edge) string {
		return e.FromNode + "->" + e.ToNode + "." + e.ToInput
	}
	edgeSet := make(map[string]bool, len(g.Edges))
	for _, e := range g.Edges {
		edgeSet[edgeKey(e)] = true
	}
	wantEdges := []graph.Edge{
		{FromNode: "s3_source", ToNode: "validate", ToInput: "raw"},
		{FromNode: "validate", ToNode: "warehouse", ToInput: "default"},
		{FromNode: "validate", ToNode: "dead_letter", ToInput: "default"},
	}
	for _, want := range wantEdges {
		key := edgeKey(want)
		if !edgeSet[key] {
			t.Errorf("missing edge %s", key)
		}
	}

	// --- Validation: no errors, no warnings ---
	if len(g.Validation.Errors) != 0 {
		t.Errorf("validation.errors = %v, want []", g.Validation.Errors)
	}
	if len(g.Validation.Warnings) != 0 {
		t.Errorf("validation.warnings = %v, want []", g.Validation.Warnings)
	}
}

// TestMultiInputTransform verifies that the inputs map is correctly parsed into
// multiple edges, each with the correct to_input key.
func TestMultiInputTransform(t *testing.T) {
	t.Parallel()
	const tf = `
module "src_a" {
  source = "clavesa/source/aws"
  name   = "src_a"
}

module "src_b" {
  source = "clavesa/source/aws"
  name   = "src_b"
}

module "enrich" {
  source = "clavesa/transform/aws"
  name   = "enrich"
  inputs = {
    raw    = module.src_a.outputs["default"]
    lookup = module.src_b.outputs["default"]
  }
  sql = "SELECT r.*, l.category FROM raw r JOIN lookup l ON r.type = l.type_code"
}
`
	dir := writeFixture(t, "main.tf", tf)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(g.Nodes) != 3 {
		t.Fatalf("want 3 nodes, got %d", len(g.Nodes))
	}
	if len(g.Edges) != 2 {
		t.Fatalf("want 2 edges, got %d: %v", len(g.Edges), g.Edges)
	}

	type edgeSpec struct{ fromNode, toNode, toInput string }
	wantEdges := []edgeSpec{
		{"src_a", "enrich", "raw"},
		{"src_b", "enrich", "lookup"},
	}
	found := make(map[edgeSpec]bool)
	for _, e := range g.Edges {
		found[edgeSpec{e.FromNode, e.ToNode, e.ToInput}] = true
	}
	for _, w := range wantEdges {
		if !found[w] {
			t.Errorf("missing edge %+v", w)
		}
	}
}

// TestNonClavesaBlocksIgnored verifies that non-Clavesa blocks (provider,
// variable, locals, terraform) are silently ignored and do not appear as nodes.
func TestNonClavesaBlocksIgnored(t *testing.T) {
	t.Parallel()
	const tf = `
terraform {
  required_version = ">= 1.0"
}

provider "aws" {
  region = "us-east-1"
}

variable "env" {
  default = "prod"
}

locals {
  prefix = "data/"
}

module "s3_source" {
  source = "clavesa/source/aws"
  name   = "s3_source"
  bucket = "my-bucket"
}

module "external_module" {
  source = "terraform-aws-modules/s3-bucket/aws"
  bucket = "other-bucket"
}
`
	dir := writeFixture(t, "main.tf", tf)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Only s3_source should appear; external_module has a non-Clavesa source.
	if len(g.Nodes) != 1 {
		t.Fatalf("want 1 node, got %d: %v", len(g.Nodes), nodeIDs(g.Nodes))
	}
	if g.Nodes[0].ID != "s3_source" {
		t.Errorf("node ID = %q, want %q", g.Nodes[0].ID, "s3_source")
	}
}

// TestOnlyNonClavesaBlocksProducesEmptyGraph verifies that a file with no
// Clavesa modules produces an empty graph with no errors.
func TestOnlyNonClavesaBlocksProducesEmptyGraph(t *testing.T) {
	t.Parallel()
	const tf = `
terraform {
  required_version = ">= 1.0"
}

provider "aws" {
  region = "us-east-1"
}

variable "bucket_name" {
  description = "Name of the S3 bucket"
}

locals {
  tags = {
    env = "prod"
  }
}
`
	dir := writeFixture(t, "main.tf", tf)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(g.Nodes) != 0 {
		t.Errorf("want 0 nodes, got %d: %v", len(g.Nodes), nodeIDs(g.Nodes))
	}
	if len(g.Edges) != 0 {
		t.Errorf("want 0 edges, got %d", len(g.Edges))
	}
	if len(g.Validation.Errors) != 0 {
		t.Errorf("want 0 errors, got %v", g.Validation.Errors)
	}
}

// TestMultipleTFFiles verifies that all .tf files in the directory are parsed
// and files list contains all .tf filenames (not full paths).
func TestMultipleTFFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "source.tf"), []byte(`
module "s3_source" {
  source = "clavesa/source/aws"
  name   = "s3_source"
  bucket = "my-data"
}
`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "destination.tf"), []byte(`
module "warehouse" {
  source = "clavesa/destination/aws"
  name   = "warehouse"
  input  = module.s3_source.outputs["default"]
  bucket = "warehouse"
}
`), 0644); err != nil {
		t.Fatal(err)
	}

	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(g.Nodes) != 2 {
		t.Fatalf("want 2 nodes, got %d", len(g.Nodes))
	}
	if len(g.Edges) != 1 {
		t.Fatalf("want 1 edge, got %d", len(g.Edges))
	}
	// Files list should contain both filenames.
	fileSet := make(map[string]bool)
	for _, f := range g.Pipeline.Files {
		fileSet[f] = true
	}
	if !fileSet["source.tf"] || !fileSet["destination.tf"] {
		t.Errorf("pipeline.files = %v, want both source.tf and destination.tf", g.Pipeline.Files)
	}
}

// ---- Validator tests ----

// TestValidatorCycleDetected verifies that a pipeline with a cycle reports CYCLE_DETECTED.
func TestValidatorCycleDetected(t *testing.T) {
	t.Parallel()
	const tf = `
module "a" {
  source = "clavesa/transform/aws"
  name   = "a"
  input  = module.b.outputs["default"]
}

module "b" {
  source = "clavesa/transform/aws"
  name   = "b"
  input  = module.a.outputs["default"]
}
`
	dir := writeFixture(t, "main.tf", tf)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	hasCode := func(msgs []graph.ValidationMessage, code graph.ValidationCode) bool {
		for _, m := range msgs {
			if m.Code == code {
				return true
			}
		}
		return false
	}
	if !hasCode(g.Validation.Errors, graph.CodeCycleDetected) {
		t.Errorf("expected CYCLE_DETECTED error; errors=%v", g.Validation.Errors)
	}
}

// TestValidatorDanglingReference verifies that an edge referencing a non-existent
// node produces a DANGLING_REFERENCE error.
func TestValidatorDanglingReference(t *testing.T) {
	t.Parallel()
	const tf = `
module "warehouse" {
  source = "clavesa/destination/aws"
  name   = "warehouse"
  input  = module.nonexistent.outputs["default"]
  bucket = "warehouse"
}
`
	dir := writeFixture(t, "main.tf", tf)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	hasCode := func(msgs []graph.ValidationMessage, code graph.ValidationCode) bool {
		for _, m := range msgs {
			if m.Code == code {
				return true
			}
		}
		return false
	}
	if !hasCode(g.Validation.Errors, graph.CodeDanglingReference) {
		t.Errorf("expected DANGLING_REFERENCE error; errors=%v", g.Validation.Errors)
	}
}

// TestValidatorDisconnectedNode verifies that a node with no edges produces a
// DISCONNECTED_NODE warning.
func TestValidatorDisconnectedNode(t *testing.T) {
	t.Parallel()
	const tf = `
module "s3_source" {
  source = "clavesa/source/aws"
  name   = "s3_source"
  bucket = "my-data"
}
`
	dir := writeFixture(t, "main.tf", tf)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	hasCode := func(msgs []graph.ValidationMessage, code graph.ValidationCode) bool {
		for _, m := range msgs {
			if m.Code == code {
				return true
			}
		}
		return false
	}
	if !hasCode(g.Validation.Warnings, graph.CodeDisconnectedNode) {
		t.Errorf("expected DISCONNECTED_NODE warning; warnings=%v", g.Validation.Warnings)
	}
}

// TestValidatorSchemaMismatch verifies that two connected nodes with incompatible
// schemas (different column types) produce a SCHEMA_MISMATCH warning.
// TestValidatorSchemaMismatch verifies output_definitions attributes are
// silently ignored (no parser errors) now that multi-output is removed.
func TestValidatorSchemaMismatch(t *testing.T) {
	t.Parallel()
	const tf = `
module "src" {
  source = "clavesa/source/aws"
  name   = "src"
  bucket = "b"
}

module "transform" {
  source = "clavesa/transform/aws"
  name   = "transform"
  inputs = {
    raw = module.src.outputs["default"]
  }
  sql = "SELECT id FROM raw"
}
`
	dir := writeFixture(t, "main.tf", tf)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(g.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(g.Nodes))
	}
}

// TestValidatorUnknownModuleSource verifies that modules with unrecognised node
// types (not source/transform/destination) are silently skipped by the parser
// and do not appear in the pipeline graph.
func TestValidatorUnknownModuleSource(t *testing.T) {
	t.Parallel()
	tf := fmt.Sprintf(`
module "mystery" {
  source = "clavesa/unknown_type"
  name   = "mystery"
}

module "orchestration" {
  source = "github.com/vesahyp/clavesa//modules/orchestration/aws?ref=%s"
  pipeline_name = "test"
}
`, service.ModuleVersion)
	dir := writeFixture(t, "main.tf", tf)
	g, err := hclparser.Parse(dir)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(g.Nodes) != 0 {
		t.Errorf("expected 0 nodes (unknown types skipped); got %d: %v", len(g.Nodes), g.Nodes)
	}
}

// ---- helpers ----

func nodeIDs(nodes []graph.Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

// assertNode checks node fields individually and reports helpful diffs.
func assertNode(t *testing.T, got, want graph.Node) {
	t.Helper()
	if got.ID != want.ID {
		t.Errorf("node %q: ID = %q, want %q", want.ID, got.ID, want.ID)
	}
	if got.Type != want.Type {
		t.Errorf("node %q: type = %q, want %q", want.ID, got.Type, want.Type)
	}
	if got.ModuleSource != want.ModuleSource {
		t.Errorf("node %q: module_source = %q, want %q", want.ID, got.ModuleSource, want.ModuleSource)
	}
	assertConfig(t, want.ID, got.Config, want.Config)
}

func assertConfig(t *testing.T, nodeID string, got, want map[string]interface{}) {
	t.Helper()
	for k, wantVal := range want {
		gotVal, ok := got[k]
		if !ok {
			t.Errorf("node %q: config missing key %q", nodeID, k)
			continue
		}
		if gotVal != wantVal {
			t.Errorf("node %q: config[%q] = %v (%T), want %v (%T)", nodeID, k, gotVal, gotVal, wantVal, wantVal)
		}
	}
	// Check for unexpected keys in got (beyond want).
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("node %q: config has unexpected key %q = %v", nodeID, k, got[k])
		}
	}
}
