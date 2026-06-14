package preview

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/graph"
)

// stubRunner is a Docker-free runner for unit tests. It returns inputs as
// outputs (passthrough) so handler tests can verify wiring without booting
// Spark. Tests asserting actual SQL semantics belong in the runner's own
// container-based integration tests, not here.
func stubRunner(passthroughKey string) func(ctx context.Context, localImage, image string, inputs map[string][]map[string]interface{}, sql, python string) (map[string][]map[string]interface{}, error) {
	return func(_ context.Context, _, _ string, inputs map[string][]map[string]interface{}, _, _ string) (map[string][]map[string]interface{}, error) {
		out := map[string][]map[string]interface{}{}
		for _, rows := range inputs {
			out[passthroughKey] = append(out[passthroughKey], rows...)
		}
		return out, nil
	}
}

// ---------------------------------------------------------------------------
// Mock S3 client
// ---------------------------------------------------------------------------

type mockS3Client struct {
	objects map[string]string // key -> content string
}

func (m *mockS3Client) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := ""
	if params.Prefix != nil {
		prefix = *params.Prefix
	}
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			k := key
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{{Key: &k}},
			}, nil
		}
	}
	return &s3.ListObjectsV2Output{Contents: nil}, nil
}

func (m *mockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := ""
	if params.Key != nil {
		key = *params.Key
	}
	content, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(content)),
	}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makeGraph(nodes []graph.Node, edges []graph.Edge) *graph.PipelineGraph {
	if nodes == nil {
		nodes = []graph.Node{}
	}
	if edges == nil {
		edges = []graph.Edge{}
	}
	return &graph.PipelineGraph{
		Pipeline: graph.PipelineMeta{Directory: "/tmp/test"},
		Nodes:    nodes,
		Edges:    edges,
		Validation: graph.Validation{
			Errors:   []graph.ValidationMessage{},
			Warnings: []graph.ValidationMessage{},
		},
	}
}

func alwaysGraph(g *graph.PipelineGraph) func(string) (*graph.PipelineGraph, error) {
	return func(_ string) (*graph.PipelineGraph, error) {
		return g, nil
	}
}

// ---------------------------------------------------------------------------
// Source preview tests
// ---------------------------------------------------------------------------

func TestHandleSourcePreview_ReturnsItems(t *testing.T) {
	ndjsonData := "{\"order_id\":\"ORD-001\",\"amount\":\"49.99\"}\n{\"order_id\":\"ORD-002\",\"amount\":\"99.50\"}\n"

	s3c := &mockS3Client{
		objects: map[string]string{"data/orders.json": ndjsonData},
	}

	g := makeGraph([]graph.Node{
		{
			ID:   "orders_source",
			Type: "source",
			Config: map[string]interface{}{
				"bucket": "test-bucket",
				"prefix": "data/",
				"format": "json",
			},
		},
	}, nil)

	handler := NewHandler(s3c, alwaysGraph(g), nil)
	req := httptest.NewRequest("GET", "/preview/source?dir=/tmp/test&node_id=orders_source", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result PreviewResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(result.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(result.Items))
	}
	if result.Items[0]["order_id"] != "ORD-001" {
		t.Errorf("expected order_id=ORD-001, got %v", result.Items[0]["order_id"])
	}
	// ADR-024 engine badge: the source preview is a raw S3 read in Go —
	// no Spark executes, so the response must NOT claim an engine. (The
	// transform preview, which does run Spark, is asserted to carry the
	// stamp in its own test.)
	if result.Served != nil {
		t.Errorf("Served = %+v, want nil on a raw S3 source preview", result.Served)
	}
}

func TestHandleSourcePreview_OffsetLimit(t *testing.T) {
	var lines []string
	for i := 0; i < 10; i++ {
		lines = append(lines, fmt.Sprintf(`{"id":"%d"}`, i))
	}
	ndjsonData := strings.Join(lines, "\n") + "\n"

	s3c := &mockS3Client{objects: map[string]string{"data/f.json": ndjsonData}}

	g := makeGraph([]graph.Node{
		{
			ID:     "src",
			Type:   "source",
			Config: map[string]interface{}{"bucket": "b", "prefix": "data/", "format": "json"},
		},
	}, nil)

	handler := NewHandler(s3c, alwaysGraph(g), nil)
	req := httptest.NewRequest("GET", "/preview/source?dir=/tmp/test&node_id=src&offset=3&limit=4", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result PreviewResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(result.Items) != 4 {
		t.Errorf("expected 4 items with limit=4, got %d", len(result.Items))
	}
	if result.Items[0]["id"] != "3" {
		t.Errorf("expected first item id=3 (offset=3), got %v", result.Items[0]["id"])
	}
}

func TestHandleSourcePreview_NodeNotFound(t *testing.T) {
	g := makeGraph(nil, nil)
	handler := NewHandler(&mockS3Client{objects: map[string]string{}}, alwaysGraph(g), nil)
	req := httptest.NewRequest("GET", "/preview/source?dir=/tmp/test&node_id=nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandleSourcePreview_InvalidNodeType(t *testing.T) {
	// A node without bucket config should return 400
	g := makeGraph([]graph.Node{
		{
			ID:     "xform",
			Type:   "transform",
			Config: map[string]interface{}{"sql": "SELECT 1"},
		},
	}, nil)

	handler := NewHandler(&mockS3Client{objects: map[string]string{}}, alwaysGraph(g), nil)
	req := httptest.NewRequest("GET", "/preview/source?dir=/tmp/test&node_id=xform", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for non-source node, got %d", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Transform preview tests
// ---------------------------------------------------------------------------

func TestHandleTransformPreview_ExecutesSQL(t *testing.T) {
	// The runner itself is exercised by the container-based integration tests.
	// Here we only verify the handler wires inputs, SQL, and the response shape.
	defer SetRunnerForTest(stubRunner("default"))()

	ndjsonData := "{\"order_id\":\"ORD-001\",\"amount\":\"49.99\",\"status\":\"pending\"}\n{\"order_id\":\"ORD-002\",\"amount\":\"9.99\",\"status\":\"complete\"}\n"

	s3c := &mockS3Client{objects: map[string]string{"data/orders.json": ndjsonData}}

	g := makeGraph(
		[]graph.Node{
			{
				ID:     "orders_source",
				Type:   "source",
				Config: map[string]interface{}{"bucket": "b", "prefix": "data/", "format": "json"},
			},
			{
				ID:     "filter_transform",
				Type:   "transform",
				Config: map[string]interface{}{"sql": `SELECT * FROM orders WHERE status = 'pending'`},
			},
		},
		[]graph.Edge{
			{FromNode: "orders_source", ToNode: "filter_transform", ToInput: "orders"},
		},
	)

	handler := NewHandler(s3c, alwaysGraph(g), nil)
	req := httptest.NewRequest("GET", "/preview/transform?dir=/tmp/test&node_id=filter_transform", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result TransformPreviewResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(result.Pairs) != 2 {
		t.Fatalf("expected 2 pairs (one per input row), got %d", len(result.Pairs))
	}
	if result.Pairs[0].Input["order_id"] != "ORD-001" {
		t.Errorf("pair 0 input: expected order_id=ORD-001, got %v", result.Pairs[0].Input["order_id"])
	}
	if result.Pairs[1].Input["order_id"] != "ORD-002" {
		t.Errorf("pair 1 input: expected order_id=ORD-002, got %v", result.Pairs[1].Input["order_id"])
	}
	if !strings.Contains(result.SQL, "WHERE status = 'pending'") {
		t.Errorf("expected SQL to be forwarded to result, got %q", result.SQL)
	}
	// ADR-024 engine badge on the transform preview response.
	if result.Served == nil || result.Served.Engine != "spark" || result.Served.Warehouse != "local" {
		t.Errorf("Served = %+v, want {spark local}", result.Served)
	}
}

func TestHandleTransformPreview_RowsParam(t *testing.T) {
	defer SetRunnerForTest(stubRunner("default"))()

	// 5 rows in source, request only 3.
	var lines []string
	for i := 0; i < 5; i++ {
		lines = append(lines, fmt.Sprintf(`{"id":"%d","keep":"yes"}`, i))
	}
	ndjsonData := strings.Join(lines, "\n") + "\n"

	s3c := &mockS3Client{objects: map[string]string{"data/f.json": ndjsonData}}

	g := makeGraph(
		[]graph.Node{
			{ID: "src", Type: "source", Config: map[string]interface{}{"bucket": "b", "prefix": "data/", "format": "json"}},
			{ID: "xform", Type: "transform", Config: map[string]interface{}{"sql": `SELECT * FROM src`}},
		},
		[]graph.Edge{{FromNode: "src", ToNode: "xform", ToInput: "src"}},
	)

	handler := NewHandler(s3c, alwaysGraph(g), nil)
	req := httptest.NewRequest("GET", "/preview/transform?dir=/tmp/test&node_id=xform&rows=3", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result TransformPreviewResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(result.Pairs) != 3 {
		t.Errorf("expected 3 pairs with rows=3, got %d", len(result.Pairs))
	}
}

func TestHandleTransformPreview_NodeNotFound(t *testing.T) {
	g := makeGraph(nil, nil)
	handler := NewHandler(&mockS3Client{objects: map[string]string{}}, alwaysGraph(g), nil)
	req := httptest.NewRequest("GET", "/preview/transform?dir=/tmp/test&node_id=missing", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandleTransformPreview_InvalidNodeType(t *testing.T) {
	// Node without SQL config should return 400
	g := makeGraph([]graph.Node{
		{
			ID:     "src",
			Type:   "source",
			Config: map[string]interface{}{"bucket": "b", "prefix": "p/", "format": "json"},
		},
	}, nil)

	handler := NewHandler(&mockS3Client{objects: map[string]string{}}, alwaysGraph(g), nil)
	req := httptest.NewRequest("GET", "/preview/transform?dir=/tmp/test&node_id=src", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for non-transform node, got %d", w.Code)
	}
}
