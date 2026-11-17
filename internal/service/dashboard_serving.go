package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vesahyp/clavesa/internal/servingsql"
)

// TranspileCacheDir is the workspace-relative directory of the serving
// translation cache (servingsql.CachedTranspiler, wired in wsdeps).
const TranspileCacheDir = ".clavesa/cache/transpile"

// ServingTemplate returns a dashboard dataset's SQL in the serving
// dialect (Athena/Trino) with its {{control}} placeholders kept, from the
// translation cache when it is there (ADR-034: the hosted Lambda has no
// translator and reads the cache files the tree copy ships).
func (s *Service) ServingTemplate(ctx context.Context, datasetSQL string) (string, error) {
	return s.transpileServingTemplate(ctx, datasetSQL)
}

// DashboardServingCacheFiles translates every dataset of every dashboard
// (filling the translation cache on the Mac) and returns their cache
// files as tree-copy files. A dataset that does not translate is left out,
// with its error in the list; its widget then fails hosted with that
// cause, and the rest still work.
func (s *Service) DashboardServingCacheFiles(ctx context.Context) ([]HostedTreeFile, []error) {
	var files []HostedTreeFile
	var errs []error
	list, err := s.ListDashboards(ctx)
	if err != nil {
		return nil, []error{err}
	}
	seen := map[string]bool{}
	for _, sum := range list {
		d, err := s.GetDashboard(ctx, sum.Slug)
		if err != nil {
			errs = append(errs, fmt.Errorf("dashboard %s: %w", sum.Slug, err))
			continue
		}
		for _, ds := range d.Datasets {
			if _, err := s.transpileServingTemplate(ctx, ds.SQL); err != nil {
				errs = append(errs, fmt.Errorf("dashboard %s dataset %s: %w", sum.Slug, ds.Name, err))
				continue
			}
			name := servingsql.CacheFileName(servingsql.SentinelizeTemplate(ds.SQL))
			path := filepath.Join(s.workspace, TranspileCacheDir, name)
			if seen[name] {
				continue
			}
			if _, err := os.Stat(path); err != nil {
				errs = append(errs, fmt.Errorf("dashboard %s dataset %s: no cached translation (no translator?)", sum.Slug, ds.Name))
				continue
			}
			seen[name] = true
			files = append(files, HostedTreeFile{Key: TranspileCacheDir + "/" + name, Path: path})
		}
	}
	return files, errs
}

// errNotOneSelect is CheckServedSelect's answer.
var errNotOneSelect = fmt.Errorf("the deployed translation is not one SELECT or WITH statement; it was not run (ADR-034)")

// CheckServedSelect accepts a translated template only when it is one
// SELECT or WITH statement: after leading whitespace, comments and
// parentheses its first word is SELECT or WITH, and it has no ';'. A
// translation the hosted Lambda reads from the tree copy runs without
// being translated or parsed again, so a tampered copy must not be able
// to run UNLOAD, INSERT or anything else (slice 3 review, finding 1).
func CheckServedSelect(sql string) error {
	if strings.Contains(sql, ";") {
		return errNotOneSelect
	}
	s := sql
	for {
		s = strings.TrimLeft(s, " \t\r\n(")
		switch {
		case strings.HasPrefix(s, "--"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return errNotOneSelect
		case strings.HasPrefix(s, "/*"):
			if i := strings.Index(s, "*/"); i >= 0 {
				s = s[i+2:]
				continue
			}
			return errNotOneSelect
		}
		break
	}
	words := strings.FieldsFunc(s, func(r rune) bool {
		return !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z')
	})
	if len(words) == 0 {
		return errNotOneSelect
	}
	word := strings.ToUpper(words[0])
	if word != "SELECT" && word != "WITH" {
		return errNotOneSelect
	}
	return nil
}
