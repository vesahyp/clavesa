package hosted

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"github.com/vesahyp/clavesa/internal/service"
)

func TestViewerFromClaims(t *testing.T) {
	for raw, want := range map[string][]string{
		"[all ws-smoke2]": {"all", "ws-smoke2"},
		"[ws-smoke2]":     {"ws-smoke2"},
		`["ws-a","ws-b"]`: {"ws-a", "ws-b"},
		"ws-a,ws-b":       {"ws-a", "ws-b"},
		"":                nil,
	} {
		v := viewerFromClaims(map[string]string{"cognito:groups": raw})
		if len(v.Groups) != len(want) {
			t.Errorf("%q: groups %v, want %v", raw, v.Groups, want)
		}
		for _, g := range want {
			if !v.Groups[g] {
				t.Errorf("%q: missing %s", raw, g)
			}
		}
	}
}

// TestPerWorkspaceViewers (ADR-035): through the Lambda's own entry, a
// viewer lists and opens only their workspaces; any other answers 404 as
// if it did not exist; /api/stats is for "all" only; no groups sees
// nothing.
func TestPerWorkspaceViewers(t *testing.T) {
	reg, err := ParseRegistry([]byte(testRegistry)) // web-traffic and smoke2
	if err != nil {
		t.Fatal(err)
	}
	op := &fakeOpener{ws: &Workspace{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}}
	s := NewServer(reg, service.HostedWellKnown{}, newMemStore(), op)
	s.RequireViewer = true
	call := func(groups *string, path string) (int, string) {
		method := http.MethodGet
		if m, rest, ok := strings.Cut(path, " "); ok {
			method, path = m, rest
		}
		ev := events.APIGatewayV2HTTPRequest{
			Version: "2.0", RawPath: path, Body: `{"dir":"bronze","sql":"SELECT 1"}`,
			RequestContext: events.APIGatewayV2HTTPRequestContext{
				HTTP: events.APIGatewayV2HTTPRequestContextHTTPDescription{Method: method, Path: path},
			},
		}
		if groups != nil {
			ev.RequestContext.Authorizer = &events.APIGatewayV2HTTPRequestContextAuthorizerDescription{
				JWT: &events.APIGatewayV2HTTPRequestContextAuthorizerJWTDescription{Claims: map[string]string{"cognito:groups": *groups}},
			}
		}
		b, _ := json.Marshal(ev)
		out, err := s.Invoke(context.Background(), b)
		if err != nil {
			t.Fatal(err)
		}
		res := out.(events.APIGatewayV2HTTPResponse)
		return res.StatusCode, res.Body
	}
	names := func(body string) map[string]bool {
		var v struct {
			Workspaces []struct {
				Name string `json:"name"`
			} `json:"workspaces"`
		}
		var cards []struct {
			Name string `json:"name"`
		}
		out := map[string]bool{}
		if json.Unmarshal([]byte(body), &cards) == nil {
			for _, c := range cards {
				out[c.Name] = true
			}
		} else if json.Unmarshal([]byte(body), &v) == nil {
			for _, c := range v.Workspaces {
				out[c.Name] = true
			}
		}
		return out
	}
	str := func(s string) *string { return &s }

	smokeOnly := str("[ws-smoke2]")
	if c, b := call(smokeOnly, "/api/workspaces"); c != 200 || !names(b)["smoke2"] || names(b)["web-traffic"] {
		t.Errorf("ws-smoke2 lists %d %v", c, names(b))
	}
	if c, _ := call(smokeOnly, "/w/smoke2/api/pipelines"); c != 200 {
		t.Errorf("ws-smoke2 opens smoke2: %d", c)
	}
	opensBefore := op.opens
	if c, b := call(smokeOnly, "/w/web-traffic/api/pipelines"); c != 404 || !json.Valid([]byte(b)) {
		t.Errorf("ws-smoke2 opens web-traffic: %d %s, want 404", c, b)
	}
	if op.opens != opensBefore {
		t.Error("a workspace the viewer may not see was opened")
	}
	// Every route of a workspace the viewer may not see, POST included.
	for _, p := range []string{"/w/web-traffic/api/workspace/summary?live=1", "POST /w/web-traffic/api/dashboards/query"} {
		if c, _ := call(smokeOnly, p); c != 404 {
			t.Errorf("ws-smoke2 %s: %d, want 404", p, c)
		}
	}
	if c, _ := call(smokeOnly, "/api/stats"); c != 404 {
		t.Errorf("ws-smoke2 reads /api/stats: %d, want 404", c)
	}

	// A test viewer of web-traffic only (Cognito group ws-web-traffic).
	wtOnly := str("[ws-web-traffic]")
	if c, b := call(wtOnly, "/api/workspaces"); c != 200 || len(names(b)) != 1 || !names(b)["web-traffic"] {
		t.Errorf("ws-web-traffic lists %d %v, want web-traffic only", c, names(b))
	}
	if c, _ := call(wtOnly, "/w/web-traffic/api/pipelines"); c != 200 {
		t.Errorf("ws-web-traffic opens web-traffic: %d", c)
	}
	for _, p := range []string{"/w/smoke2/api/pipelines", "/w/smoke2/api/workspace/summary", "POST /w/smoke2/api/dashboards/query", "/api/stats"} {
		if c, _ := call(wtOnly, p); c != 404 {
			t.Errorf("ws-web-traffic %s: %d, want 404", p, c)
		}
	}

	all := str("[all]")
	if c, b := call(all, "/api/workspaces"); c != 200 || !names(b)["smoke2"] || !names(b)["web-traffic"] {
		t.Errorf("all lists %d %v", c, names(b))
	}
	if c, _ := call(all, "/w/web-traffic/api/pipelines"); c != 200 {
		t.Errorf("all opens web-traffic: %d", c)
	}

	for _, g := range []*string{nil, str("")} {
		if c, b := call(g, "/api/workspaces"); c != 200 || len(names(b)) != 0 {
			t.Errorf("no groups lists %d %v, want nothing", c, names(b))
		}
		if c, _ := call(g, "/w/smoke2/api/pipelines"); c != 404 {
			t.Errorf("no groups opens smoke2: %d, want 404", c)
		}
	}
	// The well-known document stays public.
	if c, _ := call(nil, "/.well-known/clavesa.json"); c != 200 {
		t.Errorf("well-known: %d", c)
	}
}
