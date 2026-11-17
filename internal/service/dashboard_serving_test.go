package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/servingsql"
)

// TestDashboardServingCacheFiles is the hosted case of ADR-034: the Mac
// translates every dashboard dataset and ships the translation cache in
// the tree copy; the hosted Lambda, whose translator always fails (no
// Docker), then reads each widget's serving template from those files,
// with its {{control}} placeholders kept.
func TestDashboardServingCacheFiles(t *testing.T) {
	ctx := context.Background()
	mac := t.TempDir()
	spec := `{"slug":"runs","title":"Runs","datasets":[` +
		`{"name":"n","dir":"taxi","sql":"SELECT COUNT(*) AS n FROM t WHERE started_at > current_timestamp - INTERVAL 1 DAY"},` +
		`{"name":"by_day","dir":"taxi","sql":"SELECT d FROM t WHERE d >= {{from}}"}],"widgets":[]}`
	if err := os.MkdirAll(filepath.Join(mac, ".clavesa", "dashboards"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mac, ".clavesa", "dashboards", "runs.json"), []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	// The Mac's translator: marks its output so the test can see it ran.
	translate := func(_ context.Context, sql string) (string, error) { return "/*trino*/ " + sql, nil }
	svc := New(mac).WithTranspiler(servingsql.NewCachedTranspiler(filepath.Join(mac, TranspileCacheDir), translate))
	files, errs := svc.DashboardServingCacheFiles(ctx)
	if len(errs) != 0 || len(files) != 2 {
		t.Fatalf("files %d errs %v, want 2 files", len(files), errs)
	}

	// The Lambda: the tree copy holds the spec and those cache files.
	lambda := t.TempDir()
	copyFile := func(key, from string) {
		b, err := os.ReadFile(from)
		if err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(lambda, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range files {
		if !strings.HasPrefix(f.Key, TranspileCacheDir+"/") {
			t.Errorf("key %q is not in the cache dir", f.Key)
		}
		copyFile(f.Key, f.Path)
	}
	noDocker := func(context.Context, string) (string, error) {
		return "", errors.New("docker: executable file not found")
	}
	hosted := New(lambda).WithTranspiler(servingsql.NewCachedTranspiler(filepath.Join(lambda, TranspileCacheDir), noDocker))
	got, err := hosted.ServingTemplate(ctx, "SELECT d FROM t WHERE d >= {{from}}")
	if err != nil {
		t.Fatalf("ServingTemplate on the Lambda: %v", err)
	}
	if !strings.HasPrefix(got, "/*trino*/ ") || !strings.Contains(got, "{{from}}") {
		t.Errorf("template = %q, want the cached translation with {{from}} kept", got)
	}
	// A dataset that was not deployed has no cached translation.
	if _, err := hosted.ServingTemplate(ctx, "SELECT 2"); err == nil {
		t.Error("an undeployed dataset translated on the Lambda")
	}
}

// TestCheckServedSelect: only one SELECT or WITH statement passes.
func TestCheckServedSelect(t *testing.T) {
	for sql, ok := range map[string]bool{
		"SELECT 1":                              true,
		"  select n FROM t WHERE d >= {{from}}": true,
		"WITH a AS (SELECT 1) SELECT * FROM a":  true,
		"(SELECT 1) UNION ALL (SELECT 2)":       true,
		"/* trino */ SELECT 1":                  true,
		"-- c\nSELECT 1":                        true,
		"SELECT 1; DROP TABLE t":                false,
		"SELECT 1;":                             false,
		"UNLOAD (SELECT * FROM t) TO 's3://b/x' WITH (format='JSON')": false,
		"INSERT INTO t SELECT 1":     false,
		"CREATE TABLE x AS SELECT 1": false,
		"DROP TABLE t":               false,
		"/* unterminated":            false,
		"":                           false,
		"   ":                        false,
		"-- only a comment":          false,
	} {
		if err := CheckServedSelect(sql); (err == nil) != ok {
			t.Errorf("CheckServedSelect(%q) = %v, want ok = %v", sql, err, ok)
		}
	}
}
