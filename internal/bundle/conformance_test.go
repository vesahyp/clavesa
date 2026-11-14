package bundle

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const bundlesDir = "../../testdata/bundles"

// fakeHost answers the few calls `databricks bundle validate` makes with a
// token login: the current user and the workspace folder status. With it
// the CLI runs every mutator (target merge, substitutions, presets) and
// prints the fully resolved bundle, with no real workspace.
func fakeHost(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/scim/v2/Me"):
			_, _ = w.Write([]byte(`{"id":"1","userName":"tester@example.com","displayName":"tester"}`))
		default:
			_, _ = w.Write([]byte(`{"object_type":"DIRECTORY","path":"/"}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

var hostRe = regexp.MustCompile(`(?m)^(\s*host:\s*)https?://\S+`)

// copyBundle copies a testdata bundle into a temp folder and points every
// workspace host at the fake host.
func copyBundle(t *testing.T, src, host string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if strings.HasSuffix(p, ".yml") || strings.HasSuffix(p, ".yaml") {
			b = hostRe.ReplaceAll(b, []byte("${1}"+host))
		}
		return os.WriteFile(target, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

// canonical turns a value into JSON-shaped data with tasks sorted by key, so
// two resolutions compare with reflect.DeepEqual.
func canonical(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	sortTasks(out)
	return out
}

func sortTasks(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if k == "tasks" {
				if l, ok := val.([]any); ok {
					sort.SliceStable(l, func(i, j int) bool {
						a, _ := l[i].(map[string]any)
						b, _ := l[j].(map[string]any)
						return a["task_key"].(string) < b["task_key"].(string)
					})
				}
			}
			sortTasks(val)
		}
	case []any:
		for _, e := range t {
			sortTasks(e)
		}
	}
}

// TestConformanceWithDatabricksCLI is the conformance test of ADR-031
// section 2: for every bundle in testdata/bundles and every target that is
// not mode: development, clavesa's resolver gives the same jobs, pipelines
// and variable values as `databricks bundle validate -o json`.
//
// Development-mode targets are left out: there the CLI renames jobs after
// the logged-in user, which clavesa has no login to know, and plan refuses
// a clavesa target in development mode.
func TestConformanceWithDatabricksCLI(t *testing.T) {
	if _, err := exec.LookPath(DatabricksCLI); err != nil {
		t.Skip("databricks CLI not on PATH; brew install databricks/tap/databricks")
	}
	entries, err := os.ReadDir(bundlesDir)
	if err != nil {
		t.Fatal(err)
	}
	srv := fakeHost(t)
	cfg := filepath.Join(t.TempDir(), "databrickscfg")
	if err := os.WriteFile(cfg, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"DATABRICKS_CONFIG_FILE=" + cfg,
		"DATABRICKS_HOST=" + srv.URL,
		"DATABRICKS_TOKEN=fake-token",
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		root := copyBundle(t, filepath.Join(bundlesDir, name), srv.URL)
		targets, err := TargetNames(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, target := range targets {
			t.Run(name+"/"+target, func(t *testing.T) {
				ours, err := Resolve(root, Options{Target: target, CurrentUser: "tester@example.com", Env: func(string) (string, bool) { return "", false }})
				if err != nil {
					t.Fatalf("clavesa resolve: %v", err)
				}
				if ours.Mode == "development" {
					t.Skip("development mode: job names depend on the logged-in user")
				}
				out, err := ValidateJSON(context.Background(), root, target, nil, env)
				if err != nil {
					t.Fatalf("%v\n%s", err, out)
				}
				theirs, err := FromValidateJSON(root, out)
				if err != nil {
					t.Fatal(err)
				}
				for _, f := range []struct {
					what     string
					ours, cl any
				}{
					{"bundle name", ours.Name, theirs.Name},
					{"target", ours.Target, theirs.Target},
					{"mode", ours.Mode, theirs.Mode},
					{"host", ours.Host, theirs.Host},
					{"presets", ours.Presets, theirs.Presets},
					{"variables", ours.Variables, theirs.Variables},
					{"jobs", ours.Jobs, theirs.Jobs},
					{"pipelines", ours.Pipelines, pipelinesWithoutDefaults(theirs.Pipelines, ours.Pipelines)},
				} {
					a, b := canonical(t, f.ours), canonical(t, f.cl)
					if !reflect.DeepEqual(a, b) {
						ja, _ := json.MarshalIndent(a, "", "  ")
						jb, _ := json.MarshalIndent(b, "", "  ")
						t.Errorf("%s differ.\nclavesa:\n%s\ndatabricks CLI:\n%s", f.what, ja, jb)
					}
				}
			})
		}
	}
}

// pipelinesWithoutDefaults drops the server-side defaults the CLI adds to a
// Lakeflow pipeline (channel, edition) when the bundle does not set them;
// clavesa never deploys a pipeline, so it does not model them.
func pipelinesWithoutDefaults(cli, ours map[string]map[string]any) map[string]map[string]any {
	for k, p := range cli {
		for _, d := range []string{"channel", "edition"} {
			if _, set := ours[k][d]; !set {
				delete(p, d)
			}
		}
	}
	return cli
}
