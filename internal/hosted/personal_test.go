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

const personalRegistry = `[
 {"name":"web-traffic","kind":"cloud","account":"471112936790","region":"eu-north-1",
  "reader_role_arn":"arn:aws:iam::471112936790:role/clavesa-ui-reader-web-traffic","bucket":"web-traffic-clavesa","personal_data":true},
 {"name":"smoke2","kind":"cloud","account":"699166197771","region":"eu-north-1",
  "reader_role_arn":"arn:aws:iam::699166197771:role/clavesa-ui-reader-smoke2","bucket":"smoke2-clavesa"}
]`

// TestPersonalDataRefusesRawRows: on a workspace marked personal_data,
// raw files and rows of non-gold tables answer 403 before the workspace
// is opened; gold tables and the other pages still reach the workspace.
// A workspace without the mark is not affected.
func TestPersonalDataRefusesRawRows(t *testing.T) {
	reg, err := ParseRegistry([]byte(personalRegistry))
	if err != nil {
		t.Fatal(err)
	}
	reached := []string{}
	op := &fakeOpener{ws: &Workspace{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = append(reached, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	})}}
	s := NewServer(reg, service.HostedWellKnown{}, newMemStore(), op)
	get := func(u string) int {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, u, nil))
		return w.Code
	}
	for _, u := range []string{
		"/w/web-traffic/api/data/source?bucket=web-traffic-clavesa&prefix=bronze/",
		"/w/web-traffic/api/data/table?catalog_db=clavesa_web_traffic__bronze&catalog_table=cloudfront_raw&limit=1000",
		"/w/web-traffic/api/data/table?catalog_db=clavesa_web_traffic__silver&catalog_table=enriched",
		"/w/web-traffic/api/data/table?catalog_table=cloudfront_raw",
		"/w/web-traffic/api/data/tables/clavesa_web_traffic__bronze/cloudfront_raw/column-stats",
		"/w/web-traffic/api/preview/source?dir=bronze&node_id=x",
		"/w/web-traffic/api/sources/cloudfront_logs/preview",
		"/w/web-traffic/api//data/./source",
	} {
		if code := get(u); code != http.StatusForbidden {
			t.Errorf("%s: status %d, want 403", u, code)
		}
	}
	if len(reached) != 0 {
		t.Errorf("refused requests reached the workspace: %v", reached)
	}
	for _, u := range []string{
		"/w/web-traffic/api/data/table?catalog_db=clavesa_web_traffic__gold&catalog_table=fact_events",
		"/w/web-traffic/api/data/tables/clavesa_web_traffic__gold/fact_events/column-stats",
		"/w/web-traffic/api/data/runs?dir=bronze",
		"/w/smoke2/api/data/table?catalog_db=clavesa_smoke2__taxi&catalog_table=trips",
		"/w/smoke2/api/data/source?bucket=smoke2-clavesa&prefix=seed/",
	} {
		if code := get(u); code != http.StatusOK {
			t.Errorf("%s: status %d, want 200 (reached the workspace)", u, code)
		}
	}
}

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
	reg, _ := ParseRegistry([]byte(personalRegistry))
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
