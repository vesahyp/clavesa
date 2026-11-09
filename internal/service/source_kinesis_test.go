package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/graph"
)

func TestAddSourceKinesisURLPromotes(t *testing.T) {
	t.Parallel()
	svc := New(t.TempDir())
	got, err := svc.AddSource(SourceSpec{Name: "events", URL: "kinesis://my-stream"})
	if err != nil {
		t.Fatalf("AddSource: %v", err)
	}
	if got.Kind != "kinesis" || got.Stream != "my-stream" || got.URL != "" || got.Format != "" {
		t.Errorf("stored = %#v", got)
	}
	if _, err := svc.AddSource(SourceSpec{Name: "bad", URL: "kinesis://bad name"}); err == nil {
		t.Error("stream name with a space should be refused")
	}
}

func TestSyncOrchestrationEmitsKinesisDescriptor(t *testing.T) {
	t.Parallel()
	ws, dir := pipelineForAttachTest(t)
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{Name: "events", Kind: "kinesis", Stream: "my-stream", Endpoint: "http://localstack:4566"}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AttachSource(dir, "events", "t1", "raw"); err != nil {
		t.Fatal(err)
	}
	if err := svc.SyncOrchestration(dir, ""); err != nil {
		t.Fatalf("SyncOrchestration: %v", err)
	}
	body, _ := os.ReadFile(filepath.Join(ws, dir, "orchestration.tf"))
	want := `raw = { kind = "kinesis", spec_name = "events", stream_name = "my-stream", region = "", starting_position = "TRIM_HORIZON", endpoint_url = "http://localstack:4566" }`
	if !strings.Contains(string(body), want) {
		t.Errorf("orchestration.tf missing %s\n%s", want, body)
	}
	for _, w := range []string{`Sid = "KinesisRead"`, `stream/my-stream`} {
		if !strings.Contains(string(body), w) {
			t.Errorf("orchestration.tf missing %s", w)
		}
	}
}

func TestBuildInputsKinesis(t *testing.T) {
	ws := t.TempDir()
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{Name: "orders", Kind: "kinesis", Stream: "s1", Region: "eu-west-1", StartingPosition: "LATEST"}); err != nil {
		t.Fatal(err)
	}
	inputs, err := svc.buildInputs(context.Background(), mirrorTestGraph("orders"), "t", map[string]string{}, map[string]string{}, "cat")
	if err != nil {
		t.Fatalf("buildInputs: %v", err)
	}
	d, ok := inputs["x"].(map[string]any)
	want := map[string]any{"kind": "kinesis", "spec_name": "orders", "stream_name": "s1", "region": "eu-west-1", "starting_position": "LATEST", "endpoint_url": ""}
	if !ok || len(d) != len(want) {
		t.Fatalf("inputs[x] = %#v", inputs["x"])
	}
	for k, v := range want {
		if d[k] != v {
			t.Errorf("%s = %v, want %v", k, d[k], v)
		}
	}
}

func TestValidateKinesisNodes(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)
	if _, err := svc.AddSource(SourceSpec{Name: "events", Kind: "kinesis", Stream: "s"}); err != nil {
		t.Fatal(err)
	}
	node := func(cfg map[string]interface{}) *graph.PipelineGraph {
		cfg["source_inputs"] = map[string]interface{}{"x": "sources.events"}
		return &graph.PipelineGraph{Nodes: []graph.Node{{ID: "t", Type: "transform", Config: cfg}}}
	}
	out := func(def map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"output_definitions": map[string]interface{}{"default": def}}
	}
	ok := []*graph.PipelineGraph{
		node(map[string]interface{}{}),
		node(out(map[string]interface{}{"mode": "append"})),
	}
	for i, g := range ok {
		if err := svc.validateKinesisNodes(g); err != nil {
			t.Errorf("ok[%d]: %v", i, err)
		}
	}
	two := node(map[string]interface{}{"output_definitions": map[string]interface{}{"a": map[string]interface{}{}, "b": map[string]interface{}{}}})
	extra := node(map[string]interface{}{"external_inputs": map[string]interface{}{"y": "s.t"}})
	multi := node(map[string]interface{}{})
	multi.Nodes[0].Config["source_inputs"] = map[string]interface{}{"x": "sources.events", "z": "sources.events"}
	bad := []*graph.PipelineGraph{
		two, extra, multi,
		node(out(map[string]interface{}{"mode": "replace"})),
		node(out(map[string]interface{}{"mode": "replace_partitions"})),
		node(out(map[string]interface{}{"mode": "merge"})),
		node(out(map[string]interface{}{"merge_keys": []interface{}{"id"}})),
	}
	for i, g := range bad {
		if err := svc.validateKinesisNodes(g); err == nil {
			t.Errorf("bad[%d]: want error", i)
		}
	}
	if err := svc.validateKinesisNodes(two); err == nil || !strings.Contains(err.Error(), "only input") {
		t.Errorf("message = %v", err)
	}
}

func TestUpdateSourceKinesis(t *testing.T) {
	t.Parallel()
	svc := New(t.TempDir())
	if _, err := svc.AddSource(SourceSpec{Name: "events", URL: "kinesis://s1"}); err != nil {
		t.Fatal(err)
	}
	spec, _ := svc.GetSource("events")
	spec.Region, spec.StartingPosition, spec.Endpoint = "eu-west-1", "LATEST", "http://localhost:4566"
	got, err := svc.UpdateSource("events", spec)
	if err != nil || got.Region != "eu-west-1" || got.StartingPosition != "LATEST" || got.Stream != "s1" {
		t.Fatalf("UpdateSource = %#v / %v", got, err)
	}
	spec.StartingPosition = "AT_TIMESTAMP"
	if _, err := svc.UpdateSource("events", spec); err == nil {
		t.Error("AT_TIMESTAMP should be refused")
	}
	if _, err := svc.AddSource(SourceSpec{Name: "h", Kind: "http", URL: "https://x/y.csv", Format: "csv"}); err != nil {
		t.Fatal(err)
	}
	h, _ := svc.GetSource("h")
	h.Region = "eu-west-1"
	if _, err := svc.UpdateSource("h", h); err == nil {
		t.Error("region on an http source should be refused")
	}
}
