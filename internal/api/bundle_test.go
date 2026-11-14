package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeBundles struct{ dir, target string }

func (f *fakeBundles) PlanBundle(_ context.Context, dir, target string) (any, error) {
	f.dir, f.target = dir, target
	if dir == "bad" {
		return nil, errors.New("no databricks.yml in bad")
	}
	return map[string]any{"bundle": "b", "ready": true}, nil
}

func (f *fakeBundles) BundlePipelines() (any, error) {
	return []map[string]any{{"pipeline": "b_j"}}, nil
}

func TestBundleHandler(t *testing.T) {
	f := &fakeBundles{}
	mux := http.NewServeMux()
	NewBundleHandler(f).RegisterRoutes(mux)
	for _, c := range []struct {
		url, want string
		code      int
	}{
		{"/bundle/plan?bundle=/x/b&target=aws", `"report":{"bundle":"b","ready":true}`, 200},
		{"/bundle/plan?bundle=bad", `"error":"no databricks.yml`, 200},
		{"/bundle/plan", "give the folder", 400},
		{"/bundle/pipelines", `"pipeline":"b_j"`, 200},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", c.url, nil))
		if rec.Code != c.code || !strings.Contains(rec.Body.String(), c.want) {
			t.Errorf("%s: %d %s", c.url, rec.Code, rec.Body.String())
		}
	}
	if f.target != "" || f.dir != "bad" {
		t.Errorf("last call: %q %q", f.dir, f.target)
	}
}
