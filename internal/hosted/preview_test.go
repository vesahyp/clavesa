package hosted

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/ui"
	"github.com/vesahyp/clavesa/internal/wsdeps"
)

const testRegistry = `[
 {"name":"web-traffic","kind":"cloud","account":"471112936790","region":"eu-north-1",
  "reader_role_arn":"arn:aws:iam::471112936790:role/clavesa-ui-reader-web-traffic","bucket":"web-traffic-clavesa"},
 {"name":"smoke2","kind":"cloud","account":"699166197771","region":"eu-north-1",
  "reader_role_arn":"arn:aws:iam::699166197771:role/clavesa-ui-reader-smoke2","bucket":"smoke2-clavesa"}
]`

// TestLambdaScriptTraversal sends the 2026-11-17 review's request through
// the Lambda's own entry (an HTTP API event with its query string) to a
// real workspace mux: 400, and never the environment.
func TestLambdaScriptTraversal(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "leak-test-value")
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "pipe"), 0o755); err != nil {
		t.Fatal(err)
	}
	h := ui.BuildWorkspaceMux(ui.WorkspaceDeps{Deps: wsdeps.New(context.Background(), root, wsdeps.Options{}), ReadOnly: true})
	reg, _ := ParseRegistry([]byte(testRegistry))
	s := NewServer(reg, service.HostedWellKnown{}, newMemStore(), &fakeOpener{ws: &Workspace{Handler: h}})
	for _, q := range []string{"dir=/proc/self&path=environ", "dir=pipe&path=../../../../proc/self/environ", "dir=..&path=etc/hostname"} {
		ev := events.APIGatewayV2HTTPRequest{
			Version: "2.0", RawPath: "/w/smoke2/api/pipeline/script", RawQueryString: q,
			RequestContext: events.APIGatewayV2HTTPRequestContext{
				HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: http.MethodGet, Path: "/w/smoke2/api/pipeline/script"},
			},
		}
		b, _ := json.Marshal(ev)
		out, err := s.Invoke(context.Background(), b)
		if err != nil {
			t.Fatal(err)
		}
		res := out.(events.APIGatewayV2HTTPResponse)
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400; body %q", q, res.StatusCode, res.Body)
		}
		if strings.Contains(res.Body, "leak-test-value") || strings.Contains(res.Body, "AWS_") {
			t.Errorf("%s: body leaks the environment", q)
		}
	}
}

// TestPreviewsRefusedEverywhere (slice 4 review, finding 2): previews are
// 403 on every workspace, before it is opened.
func TestPreviewsRefusedEverywhere(t *testing.T) {
	reg, err := ParseRegistry([]byte(testRegistry))
	if err != nil {
		t.Fatal(err)
	}
	op := &fakeOpener{ws: &Workspace{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}}
	s := NewServer(reg, service.HostedWellKnown{}, newMemStore(), op)
	for _, u := range []string{
		"/w/smoke2/api/preview/source?dir=taxi&node_id=trips",
		"/w/smoke2/api/preview/transform?dir=taxi&node_id=trips",
		"/w/smoke2/api/preview/destination?dir=taxi&node_id=x",
		"/w/smoke2/api/sources/trips_raw/preview",
		"/w/smoke2/api//preview/./source",
	} {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, u, nil))
		if w.Code != http.StatusForbidden {
			t.Errorf("%s: %d, want 403", u, w.Code)
		}
	}
	if op.opens != 0 {
		t.Errorf("a preview opened the workspace %d time(s)", op.opens)
	}
}
