package service

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pipelineForAttachTest stamps a pipeline dir + a transform module the
// AttachSource flow can target.
func pipelineForAttachTest(t *testing.T) (workspace, dir string) {
	t.Helper()
	workspace = t.TempDir()
	dir = "demo"
	pipelineDir := filepath.Join(workspace, dir)
	if err := os.MkdirAll(pipelineDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "pipeline_name" {
  type    = string
  default = "demo"
}

module "t1" {
  source = "github.com/vesahyp/clavesa//modules/transform/aws?ref=v0.18.0"
  name   = "t1"
  sql    = "SELECT 1"
}
`
	if err := os.WriteFile(filepath.Join(pipelineDir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	return workspace, dir
}

func TestAddListGetDeleteSource(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)
	stored, err := svc.AddSource(SourceSpec{
		Name: "trips", Kind: "http",
		URL: "https://example.com/yellow_tripdata_2024-01.parquet",
	})
	if err != nil {
		t.Fatalf("AddSource: %v", err)
	}
	if stored.Format != "parquet" {
		t.Errorf("format inference failed: got %q, want parquet", stored.Format)
	}
	list, err := svc.ListSources()
	if err != nil || len(list) != 1 || list[0].Name != "trips" {
		t.Fatalf("ListSources = %#v / %v", list, err)
	}
	got, err := svc.GetSource("trips")
	if err != nil || got.URL != stored.URL {
		t.Fatalf("GetSource = %#v / %v", got, err)
	}
	if err := svc.DeleteSource("trips", false); err != nil {
		t.Fatalf("DeleteSource: %v", err)
	}
	if _, err := svc.GetSource("trips"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("GetSource after delete = %v, want os.ErrNotExist", err)
	}
}

func TestAttachSourceWritesInputsBlock(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name: "trips", Kind: "http",
		URL: "https://example.com/yellow_tripdata_2024-01.parquet",
	}); err != nil {
		t.Fatalf("AddSource: %v", err)
	}
	if err := svc.AttachSource(dir, "trips", "t1", "raw"); err != nil {
		t.Fatalf("AttachSource: %v", err)
	}
	main, err := os.ReadFile(filepath.Join(ws, dir, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(main), "sources.trips") {
		t.Errorf("main.tf missing sources.trips reference:\n%s", main)
	}
	if !strings.Contains(string(main), "raw") {
		t.Errorf("main.tf missing input alias 'raw':\n%s", main)
	}
}

func TestAttachSourceMergesIntoExistingInputs(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name: "trips", Kind: "http",
		URL: "https://example.com/trips.parquet",
	}); err != nil {
		t.Fatalf("AddSource trips: %v", err)
	}
	if _, err := svc.AddSource(SourceSpec{
		Name: "weather", Kind: "http",
		URL: "https://example.com/weather.parquet",
	}); err != nil {
		t.Fatalf("AddSource weather: %v", err)
	}
	if err := svc.AttachSource(dir, "trips", "t1", "trips"); err != nil {
		t.Fatalf("AttachSource trips: %v", err)
	}
	if err := svc.AttachSource(dir, "weather", "t1", "weather"); err != nil {
		t.Fatalf("AttachSource weather: %v", err)
	}
	main, err := os.ReadFile(filepath.Join(ws, dir, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	// Both attachments must coexist — the second attach must not clobber
	// the first. Confirmed via end-to-end smoke before adding this test
	// (the prior implementation overwrote inputs entirely).
	if !strings.Contains(string(main), "sources.trips") {
		t.Errorf("main.tf missing sources.trips after second attach:\n%s", main)
	}
	if !strings.Contains(string(main), "sources.weather") {
		t.Errorf("main.tf missing sources.weather:\n%s", main)
	}
}

func TestDetachInputRemovesSourceAttachment(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name: "trips", Kind: "http",
		URL: "https://example.com/trips.parquet",
	}); err != nil {
		t.Fatalf("AddSource: %v", err)
	}
	if _, err := svc.AddSource(SourceSpec{
		Name: "weather", Kind: "http",
		URL: "https://example.com/weather.parquet",
	}); err != nil {
		t.Fatalf("AddSource weather: %v", err)
	}
	if err := svc.AttachSource(dir, "trips", "t1", "trips"); err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachSource(dir, "weather", "t1", "weather"); err != nil {
		t.Fatal(err)
	}
	if err := svc.DetachInput(dir, "t1", "trips"); err != nil {
		t.Fatalf("DetachInput: %v", err)
	}
	main, err := os.ReadFile(filepath.Join(ws, dir, "main.tf"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(main), "sources.trips") {
		t.Errorf("trips not detached:\n%s", main)
	}
	if !strings.Contains(string(main), "sources.weather") {
		t.Errorf("weather attachment clobbered by detach:\n%s", main)
	}
}

func TestDetachInputErrorsOnUnknownAlias(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	err := svc.DetachInput(dir, "t1", "ghost")
	if err == nil || !strings.Contains(err.Error(), "not attached") {
		t.Fatalf("expected not-attached error, got %v", err)
	}
}

func TestDetachInputRejectsNonTransform(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	dir := "demo"
	if err := os.MkdirAll(filepath.Join(ws, dir), 0o755); err != nil {
		t.Fatal(err)
	}
	mainTF := `module "src1" {
  source = "github.com/vesahyp/clavesa//modules/source/aws?ref=v0.18.0"
  name   = "src1"
}
`
	if err := os.WriteFile(filepath.Join(ws, dir, "main.tf"), []byte(mainTF), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(ws)
	err := svc.DetachInput(dir, "src1", "anything")
	if err == nil || !strings.Contains(err.Error(), "only transforms") {
		t.Fatalf("expected only-transforms error, got %v", err)
	}
}

func TestDeleteSourceRefusesWhenInUse(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name: "trips", Kind: "http",
		URL: "https://example.com/x.parquet",
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachSource(dir, "trips", "t1", "raw"); err != nil {
		t.Fatal(err)
	}
	err := svc.DeleteSource("trips", false)
	var inUse *ErrSourceInUse
	if !errors.As(err, &inUse) {
		t.Fatalf("DeleteSource without --force = %v, want *ErrSourceInUse", err)
	}
	if len(inUse.Usages) != 1 || inUse.Usages[0].PipelineDir != "demo" {
		t.Errorf("Usages = %#v", inUse.Usages)
	}
	if inUse.Usages[0].NodeIDs[0] != "t1" {
		t.Errorf("NodeIDs = %#v, want [t1]", inUse.Usages[0].NodeIDs)
	}
	// --force overrides.
	if err := svc.DeleteSource("trips", true); err != nil {
		t.Errorf("DeleteSource(force=true) = %v, want nil", err)
	}
}

func TestAttachSourceRejectsUnregistered(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	err := svc.AttachSource(dir, "ghost", "t1", "")
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Errorf("AttachSource(ghost) = %v, want 'not registered' error", err)
	}
}

func TestAddSourceS3URLShortcut(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)
	stored, err := svc.AddSource(SourceSpec{
		Name: "logs",
		URL:  "s3://my-bucket/events/2024/data.parquet",
	})
	if err != nil {
		t.Fatalf("AddSource: %v", err)
	}
	if stored.Kind != "s3" {
		t.Errorf("kind = %q, want s3", stored.Kind)
	}
	if stored.Bucket != "my-bucket" {
		t.Errorf("bucket = %q, want my-bucket", stored.Bucket)
	}
	if stored.Prefix != "events/2024/data.parquet/" {
		t.Errorf("prefix = %q, want normalized", stored.Prefix)
	}
	if stored.Format != "parquet" {
		t.Errorf("format = %q, want parquet", stored.Format)
	}
	if stored.URL != "" {
		t.Errorf("url should be cleared after s3 derivation, got %q", stored.URL)
	}
}

func TestAddSourceS3PrefixOnly(t *testing.T) {
	t.Parallel()
	// User passes `s3://bucket/prefix/` (no filename) — derivation
	// keeps prefix as-is and demands explicit --format. Catches the
	// "I pointed at a directory" path so users get a useful error
	// rather than a Spark stack trace later.
	ws := t.TempDir()
	svc := New(ws)
	_, err := svc.AddSource(SourceSpec{
		Name: "logs",
		URL:  "s3://my-bucket/events/",
	})
	if err == nil || !strings.Contains(err.Error(), "format is required") {
		t.Errorf("AddSource with prefix-only URL = %v, want format-required error", err)
	}
}

func TestAttachSourceRejectsNonTransformTarget(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{
		Name: "trips", Kind: "http",
		URL: "https://example.com/x.parquet",
	}); err != nil {
		t.Fatal(err)
	}
	// Add a destination node and try to attach to it.
	if _, err := svc.AddNode(dir, "destination", "dst1"); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	err := svc.AttachSource(dir, "trips", "dst1", "")
	if err == nil || !strings.Contains(err.Error(), "transform") {
		t.Errorf("AttachSource on destination = %v, want transform-only error", err)
	}
}
