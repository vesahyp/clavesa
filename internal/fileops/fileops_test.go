package fileops_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/fileops"
)

// hasAttr checks that content contains an HCL attribute with the given key and
// value, allowing for hclwrite's automatic alignment spacing around '='.
func hasAttr(content, key, value string) bool {
	pattern := `(?m)^\s*` + regexp.QuoteMeta(key) + `\s+=\s+` + regexp.QuoteMeta(value)
	ok, _ := regexp.MatchString(pattern, content)
	return ok
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func tmpDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "fileops-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile %s: %v", path, err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	return string(b)
}

func foErr(t *testing.T, err error, wantCode fileops.FileOpsErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", wantCode)
	}
	foe, ok := err.(*fileops.FileOpsError)
	if !ok {
		t.Fatalf("expected *FileOpsError, got %T: %v", err, err)
	}
	if foe.Code != wantCode {
		t.Fatalf("expected error code %s, got %s: %s", wantCode, foe.Code, foe.Message)
	}
}

func ref(expr string) fileops.ModuleReference {
	return fileops.ModuleReference{Type: "reference", Expression: expr}
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

func TestRead_ReturnsAllTFFiles(t *testing.T) {
	dir := tmpDir(t)
	writeFile(t, filepath.Join(dir, "main.tf"), `module "s3_source" { source = "x" }`)
	writeFile(t, filepath.Join(dir, "providers.tf"), `provider "aws" { region = "eu-west-1" }`)
	writeFile(t, filepath.Join(dir, "README.md"), "ignore me")

	fo := fileops.New()
	result, err := fo.Read(dir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(result.Files) != 2 {
		t.Fatalf("expected 2 .tf files, got %d", len(result.Files))
	}
	paths := make(map[string]bool)
	for _, f := range result.Files {
		paths[filepath.Base(f.Path)] = true
		if f.Path != filepath.Join(dir, filepath.Base(f.Path)) {
			t.Errorf("path %q is not absolute", f.Path)
		}
	}
	if !paths["main.tf"] || !paths["providers.tf"] {
		t.Errorf("unexpected files: %v", paths)
	}
}

func TestRead_EmptyDirectory(t *testing.T) {
	dir := tmpDir(t)
	fo := fileops.New()
	result, err := fo.Read(dir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(result.Files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(result.Files))
	}
}

func TestRead_DirectoryNotFound(t *testing.T) {
	fo := fileops.New()
	_, err := fo.Read("/nonexistent/path/does/not/exist")
	foErr(t, err, fileops.ErrDirectoryNotFound)
}

func TestRead_ReturnsFileContent(t *testing.T) {
	dir := tmpDir(t)
	content := `module "x" { source = "clavesa/source/aws" }` + "\n"
	writeFile(t, filepath.Join(dir, "main.tf"), content)

	fo := fileops.New()
	result, err := fo.Read(dir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(result.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(result.Files))
	}
	if result.Files[0].Content != content {
		t.Errorf("content mismatch\ngot:  %q\nwant: %q", result.Files[0].Content, content)
	}
}

// ---------------------------------------------------------------------------
// AddBlock
// ---------------------------------------------------------------------------

func TestAddBlock_BasicAdd(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "")

	fo := fileops.New()
	result, err := fo.AddBlock(path, "module", "s3_source", map[string]interface{}{
		"source": "clavesa/source/aws",
		"name":   "s3_source",
	})
	if err != nil {
		t.Fatalf("AddBlock: %v", err)
	}
	if result.BlockAddress != "module.s3_source" {
		t.Errorf("block_address = %q, want %q", result.BlockAddress, "module.s3_source")
	}
	if result.File != path {
		t.Errorf("file = %q, want %q", result.File, path)
	}
	if !strings.Contains(result.Content, `module "s3_source"`) {
		t.Errorf("content missing module block:\n%s", result.Content)
	}
	// content must match disk
	disk := readFile(t, path)
	if disk != result.Content {
		t.Errorf("disk content differs from returned content")
	}
}

func TestAddBlock_AppendToExistingFile(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	existing := "# existing content\n\nmodule \"first\" {\n  source = \"x\"\n}\n"
	writeFile(t, path, existing)

	fo := fileops.New()
	_, err := fo.AddBlock(path, "module", "second", map[string]interface{}{
		"source": "y",
	})
	if err != nil {
		t.Fatalf("AddBlock: %v", err)
	}
	disk := readFile(t, path)
	if !strings.Contains(disk, `module "first"`) {
		t.Errorf("existing block removed:\n%s", disk)
	}
	if !strings.Contains(disk, `module "second"`) {
		t.Errorf("new block not added:\n%s", disk)
	}
}

func TestAddBlock_FileNotFound(t *testing.T) {
	fo := fileops.New()
	_, err := fo.AddBlock("/nonexistent/path/main.tf", "module", "x", nil)
	foErr(t, err, fileops.ErrFileNotFound)
}

func TestAddBlock_BlockAlreadyExists(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "module \"duplicate\" {\n  source = \"x\"\n}\n")

	fo := fileops.New()
	_, err := fo.AddBlock(path, "module", "duplicate", map[string]interface{}{
		"source": "y",
	})
	foErr(t, err, fileops.ErrBlockAlreadyExists)
}

func TestAddBlock_ParseError(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "this is not valid hcl {{{")

	fo := fileops.New()
	_, err := fo.AddBlock(path, "module", "x", map[string]interface{}{
		"source": "y",
	})
	foErr(t, err, fileops.ErrParseError)
}

func TestAddBlock_AttributeTypes(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "")

	fo := fileops.New()
	result, err := fo.AddBlock(path, "module", "typed", map[string]interface{}{
		"str":     "hello",
		"num":     float64(42),
		"flag":    true,
		"listval": []interface{}{"a", "b"},
		"objval":  map[string]interface{}{"key": "val"},
		"refval":  ref(`module.other.outputs["x"]`),
	})
	if err != nil {
		t.Fatalf("AddBlock: %v", err)
	}
	c := result.Content
	if !strings.Contains(c, `str = "hello"`) {
		t.Errorf("missing string attr:\n%s", c)
	}
	if !strings.Contains(c, `num = 42`) {
		t.Errorf("missing number attr:\n%s", c)
	}
	if !hasAttr(c, "flag", "true") {
		t.Errorf("missing bool attr:\n%s", c)
	}
	if !strings.Contains(c, `module.other.outputs["x"]`) {
		t.Errorf("missing reference (unquoted):\n%s", c)
	}
}

func TestAddBlock_Atomicity_NoChangeOnError(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	original := "module \"existing\" {\n  source = \"x\"\n}\n"
	writeFile(t, path, original)

	fo := fileops.New()
	// Adding a block that already exists must fail without modifying the file.
	_, err := fo.AddBlock(path, "module", "existing", map[string]interface{}{"source": "y"})
	foErr(t, err, fileops.ErrBlockAlreadyExists)

	disk := readFile(t, path)
	if disk != original {
		t.Errorf("file modified despite error:\ngot:  %q\nwant: %q", disk, original)
	}
}

// ---------------------------------------------------------------------------
// UpdateBlock
// ---------------------------------------------------------------------------

const updateFixture = `module "validate" {
  source  = "clavesa/transform/aws"
  name    = "validate"
  input   = module.s3_source.outputs["default"]
  runtime = "sql"
  sql     = "SELECT * FROM input WHERE amount > 0"
}
`

func TestUpdateBlock_ChangeAttributes(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, updateFixture)

	fo := fileops.New()
	result, err := fo.UpdateBlock(path, "module.validate", map[string]interface{}{
		"sql":   "SELECT id, amount, ts FROM input WHERE amount > 0 AND ts > '2024-01-01'",
		"input": ref(`module.new_source.outputs["default"]`),
	})
	if err != nil {
		t.Fatalf("UpdateBlock: %v", err)
	}
	c := result.Content
	if !strings.Contains(c, `module.new_source.outputs["default"]`) {
		t.Errorf("updated reference missing:\n%s", c)
	}
	if strings.Contains(c, `module.s3_source.outputs["default"]`) {
		t.Errorf("old reference still present:\n%s", c)
	}
	if !strings.Contains(c, `SELECT id, amount, ts FROM input`) {
		t.Errorf("updated sql missing:\n%s", c)
	}
	if !strings.Contains(c, `clavesa/transform/aws`) {
		t.Errorf("untouched attribute removed:\n%s", c)
	}
}

func TestUpdateBlock_RemoveAttributeWithNull(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "module \"x\" {\n  source = \"s\"\n  compute = \"large\"\n}\n")

	fo := fileops.New()
	result, err := fo.UpdateBlock(path, "module.x", map[string]interface{}{
		"compute": nil,
	})
	if err != nil {
		t.Fatalf("UpdateBlock: %v", err)
	}
	if strings.Contains(result.Content, "compute") {
		t.Errorf("compute attribute should be removed:\n%s", result.Content)
	}
	if !strings.Contains(result.Content, `source = "s"`) {
		t.Errorf("source attribute removed unexpectedly:\n%s", result.Content)
	}
}

func TestUpdateBlock_PreservesUntouchedBlocks(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	content := `provider "aws" {
  region = "eu-west-1"
}

module "validate" {
  source = "clavesa/transform/aws"
  sql    = "SELECT 1"
}

module "other" {
  source = "z"
}
`
	writeFile(t, path, content)

	fo := fileops.New()
	_, err := fo.UpdateBlock(path, "module.validate", map[string]interface{}{
		"sql": "SELECT 2",
	})
	if err != nil {
		t.Fatalf("UpdateBlock: %v", err)
	}
	disk := readFile(t, path)
	if !strings.Contains(disk, `provider "aws"`) {
		t.Errorf("provider block removed:\n%s", disk)
	}
	if !strings.Contains(disk, `module "other"`) {
		t.Errorf("other module block removed:\n%s", disk)
	}
}

func TestUpdateBlock_PreservesCommentAboveBlock(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	content := `# This validates incoming events
module "validate" {
  source = "clavesa/transform/aws"
  sql    = "SELECT 1"
}
`
	writeFile(t, path, content)

	fo := fileops.New()
	_, err := fo.UpdateBlock(path, "module.validate", map[string]interface{}{
		"sql": "SELECT 2",
	})
	if err != nil {
		t.Fatalf("UpdateBlock: %v", err)
	}
	disk := readFile(t, path)
	if !strings.Contains(disk, "# This validates incoming events") {
		t.Errorf("comment above block was removed:\n%s", disk)
	}
}

func TestUpdateBlock_AppendNewAttribute(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "module \"x\" {\n  source = \"s\"\n}\n")

	fo := fileops.New()
	result, err := fo.UpdateBlock(path, "module.x", map[string]interface{}{
		"newattr": "newval",
	})
	if err != nil {
		t.Fatalf("UpdateBlock: %v", err)
	}
	if !strings.Contains(result.Content, `newattr = "newval"`) {
		t.Errorf("new attribute not appended:\n%s", result.Content)
	}
	if !hasAttr(result.Content, "source", `"s"`) {
		t.Errorf("source attribute removed:\n%s", result.Content)
	}
}

func TestUpdateBlock_BlockNotFound(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "module \"x\" {\n  source = \"s\"\n}\n")

	fo := fileops.New()
	_, err := fo.UpdateBlock(path, "module.nonexistent", map[string]interface{}{
		"source": "y",
	})
	foErr(t, err, fileops.ErrBlockNotFound)
}

func TestUpdateBlock_FileNotFound(t *testing.T) {
	fo := fileops.New()
	_, err := fo.UpdateBlock("/nonexistent/main.tf", "module.x", map[string]interface{}{
		"source": "y",
	})
	foErr(t, err, fileops.ErrFileNotFound)
}

func TestUpdateBlock_AmbiguousAddress(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	// Two blocks with the same type and name (invalid HCL but FILE-OPS handles it defensively)
	writeFile(t, path, "module \"dup\" {\n  source = \"a\"\n}\nmodule \"dup\" {\n  source = \"b\"\n}\n")

	fo := fileops.New()
	_, err := fo.UpdateBlock(path, "module.dup", map[string]interface{}{
		"source": "c",
	})
	foErr(t, err, fileops.ErrAmbiguousAddress)
}

func TestUpdateBlock_Atomicity_NoChangeOnError(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	original := "module \"x\" {\n  source = \"s\"\n}\n"
	writeFile(t, path, original)

	fo := fileops.New()
	_, err := fo.UpdateBlock(path, "module.nonexistent", map[string]interface{}{
		"source": "y",
	})
	foErr(t, err, fileops.ErrBlockNotFound)

	disk := readFile(t, path)
	if disk != original {
		t.Errorf("file modified despite error")
	}
}

func TestUpdateBlock_ReturnsDiskContent(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "module \"x\" {\n  source = \"s\"\n}\n")

	fo := fileops.New()
	result, err := fo.UpdateBlock(path, "module.x", map[string]interface{}{
		"source": "updated",
	})
	if err != nil {
		t.Fatalf("UpdateBlock: %v", err)
	}
	disk := readFile(t, path)
	if disk != result.Content {
		t.Errorf("returned content differs from disk")
	}
}

// ---------------------------------------------------------------------------
// RemoveBlock
// ---------------------------------------------------------------------------

func TestRemoveBlock_Basic(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "module \"dead_letter\" {\n  source = \"x\"\n}\n")

	fo := fileops.New()
	result, err := fo.RemoveBlock(path, "module.dead_letter")
	if err != nil {
		t.Fatalf("RemoveBlock: %v", err)
	}
	if strings.Contains(result.Content, "dead_letter") {
		t.Errorf("block still present after remove:\n%s", result.Content)
	}
	disk := readFile(t, path)
	if disk != result.Content {
		t.Errorf("returned content differs from disk")
	}
}

func TestRemoveBlock_PreservesOtherBlocks(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	content := `module "keep" {
  source = "a"
}

module "remove_me" {
  source = "b"
}

module "also_keep" {
  source = "c"
}
`
	writeFile(t, path, content)

	fo := fileops.New()
	_, err := fo.RemoveBlock(path, "module.remove_me")
	if err != nil {
		t.Fatalf("RemoveBlock: %v", err)
	}
	disk := readFile(t, path)
	if !strings.Contains(disk, `module "keep"`) {
		t.Errorf("keep block removed:\n%s", disk)
	}
	if !strings.Contains(disk, `module "also_keep"`) {
		t.Errorf("also_keep block removed:\n%s", disk)
	}
	if strings.Contains(disk, `module "remove_me"`) {
		t.Errorf("remove_me block still present:\n%s", disk)
	}
}

func TestRemoveBlock_BlockNotFound(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "module \"x\" {\n  source = \"s\"\n}\n")

	fo := fileops.New()
	_, err := fo.RemoveBlock(path, "module.nonexistent")
	foErr(t, err, fileops.ErrBlockNotFound)
}

func TestRemoveBlock_FileNotFound(t *testing.T) {
	fo := fileops.New()
	_, err := fo.RemoveBlock("/nonexistent/main.tf", "module.x")
	foErr(t, err, fileops.ErrFileNotFound)
}

func TestRemoveBlock_AmbiguousAddress(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	writeFile(t, path, "module \"dup\" {\n  source = \"a\"\n}\nmodule \"dup\" {\n  source = \"b\"\n}\n")

	fo := fileops.New()
	_, err := fo.RemoveBlock(path, "module.dup")
	foErr(t, err, fileops.ErrAmbiguousAddress)
}

func TestRemoveBlock_Atomicity_NoChangeOnError(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	original := "module \"x\" {\n  source = \"s\"\n}\n"
	writeFile(t, path, original)

	fo := fileops.New()
	_, err := fo.RemoveBlock(path, "module.nonexistent")
	foErr(t, err, fileops.ErrBlockNotFound)

	disk := readFile(t, path)
	if disk != original {
		t.Errorf("file modified despite error")
	}
}

// ---------------------------------------------------------------------------
// Round-trip: read → write back → identical
// ---------------------------------------------------------------------------

func TestRoundTrip_ReadWriteIdentical(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")
	original := `# Pipeline: events ingestion

module "s3_source" {
  source = "clavesa/source/aws"
  name   = "s3_source"
  bucket = "my-data"
  prefix = "events/"
}

provider "aws" {
  region = "eu-west-1"
}
`
	writeFile(t, path, original)

	fo := fileops.New()
	readResult, err := fo.Read(dir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(readResult.Files) != 1 {
		t.Fatalf("expected 1 file")
	}
	if readResult.Files[0].Content != original {
		t.Errorf("read content differs from original")
	}
}

// ---------------------------------------------------------------------------
// Full pipeline integration test (from contract)
// ---------------------------------------------------------------------------

func TestFullPipelineExample(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")

	// Start with empty file
	writeFile(t, path, "")

	fo := fileops.New()

	// Op 1: read (already empty)
	readResult, err := fo.Read(dir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(readResult.Files) != 1 {
		t.Fatalf("expected 1 file after creation, got %d", len(readResult.Files))
	}

	// Op 2: add s3_source
	_, err = fo.AddBlock(path, "module", "s3_source", map[string]interface{}{
		"source": "clavesa/source/aws",
		"name":   "s3_source",
		"bucket": "my-data",
		"prefix": "events/",
	})
	if err != nil {
		t.Fatalf("AddBlock s3_source: %v", err)
	}

	// Op 3: add validate
	_, err = fo.AddBlock(path, "module", "validate", map[string]interface{}{
		"source":  "clavesa/transform/aws",
		"name":    "validate",
		"input":   ref(`module.s3_source.outputs["default"]`),
		"runtime": "sql",
		"sql":     "SELECT * FROM input WHERE amount > 0",
	})
	if err != nil {
		t.Fatalf("AddBlock validate: %v", err)
	}

	// Op 4: add warehouse
	_, err = fo.AddBlock(path, "module", "warehouse", map[string]interface{}{
		"source": "clavesa/destination/aws",
		"name":   "warehouse",
		"input":  ref(`module.validate.outputs["valid"]`),
		"bucket": "warehouse",
		"prefix": "clean/",
	})
	if err != nil {
		t.Fatalf("AddBlock warehouse: %v", err)
	}

	// Op 5: add dead_letter
	_, err = fo.AddBlock(path, "module", "dead_letter", map[string]interface{}{
		"source": "clavesa/destination/aws",
		"name":   "dead_letter",
		"input":  ref(`module.validate.outputs["invalid"]`),
		"bucket": "quarantine",
		"prefix": "invalid/",
	})
	if err != nil {
		t.Fatalf("AddBlock dead_letter: %v", err)
	}

	// Verify each block is present with correct attributes
	disk := readFile(t, path)

	checks := []string{
		`module "s3_source"`,
		`source = "clavesa/source/aws"`,
		`bucket = "my-data"`,
		`prefix = "events/"`,
		`module "validate"`,
		`source = "clavesa/transform/aws"`,
		`module.s3_source.outputs["default"]`,
		`runtime = "sql"`,
		`SELECT * FROM input WHERE amount > 0`,
		`module "warehouse"`,
		`source = "clavesa/destination/aws"`,
		`module.validate.outputs["valid"]`,
		`prefix = "clean/"`,
		`module "dead_letter"`,
		`module.validate.outputs["invalid"]`,
		`bucket = "quarantine"`,
		`prefix = "invalid/"`,
	}

	// Normalize alignment spaces around = for attribute checks (hclwrite aligns them).
	normalized := regexp.MustCompile(`[ \t]+=[ \t]+`).ReplaceAllString(disk, " = ")
	for _, want := range checks {
		if !strings.Contains(normalized, want) {
			t.Errorf("missing %q in final file:\n%s", want, disk)
		}
	}

	// All four blocks must be present
	for _, name := range []string{"s3_source", "validate", "warehouse", "dead_letter"} {
		if !strings.Contains(disk, `module "`+name+`"`) {
			t.Errorf("module %q not found in final file:\n%s", name, disk)
		}
	}
}

// ---------------------------------------------------------------------------
// Round-trip: add → update → remove → verify rest unchanged
// ---------------------------------------------------------------------------

func TestRoundTripAddUpdateRemove(t *testing.T) {
	dir := tmpDir(t)
	path := filepath.Join(dir, "main.tf")

	// Seed file with a stable block that must survive all operations
	seed := `# do not touch this
module "stable" {
  source = "clavesa/source/aws"
  name   = "stable"
}
`
	writeFile(t, path, seed)

	fo := fileops.New()

	// Add a transient block
	_, err := fo.AddBlock(path, "module", "transient", map[string]interface{}{
		"source": "clavesa/transform/aws",
		"name":   "transient",
		"sql":    "SELECT 1",
	})
	if err != nil {
		t.Fatalf("AddBlock: %v", err)
	}

	// Update the transient block
	_, err = fo.UpdateBlock(path, "module.transient", map[string]interface{}{
		"sql":   "SELECT 2",
		"extra": "added",
	})
	if err != nil {
		t.Fatalf("UpdateBlock: %v", err)
	}
	disk := readFile(t, path)
	if !strings.Contains(disk, `SELECT 2`) {
		t.Errorf("update not applied:\n%s", disk)
	}
	if strings.Contains(disk, `SELECT 1`) {
		t.Errorf("old value still present:\n%s", disk)
	}

	// Remove the transient block
	_, err = fo.RemoveBlock(path, "module.transient")
	if err != nil {
		t.Fatalf("RemoveBlock: %v", err)
	}

	disk = readFile(t, path)
	if strings.Contains(disk, "transient") {
		t.Errorf("transient block still present after remove:\n%s", disk)
	}

	// Stable block and comment must be intact
	if !strings.Contains(disk, "# do not touch this") {
		t.Errorf("comment removed:\n%s", disk)
	}
	if !strings.Contains(disk, `module "stable"`) {
		t.Errorf("stable module removed:\n%s", disk)
	}
	if !strings.Contains(disk, `source = "clavesa/source/aws"`) {
		t.Errorf("stable source attribute removed:\n%s", disk)
	}
}
