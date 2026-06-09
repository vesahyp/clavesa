// Package servingsql holds the serving-dialect transpile primitives:
// the {{name}}-placeholder sentinel round-trip and a content-addressed
// on-disk cache for transpile results.
//
// Why a sentinel round-trip? Dashboard dataset SQL carries `{{name}}`
// placeholders that ExpandPlaceholders substitutes with single-quoted
// string literals at render time. We want to transpile the TEMPLATE
// (placeholders intact) once and cache it so render-time does no
// transpile and the cache key is param-independent. But sqlglot cannot
// parse a raw `{{name}}`. So we replace each `{{name}}` with a
// string-literal SENTINEL before transpile — a string literal is exactly
// what the placeholder becomes at render time anyway, so the substitution
// is faithful — transpile, then swap the sentinel back to `{{name}}` in
// the Trino output.
//
// This package is a leaf: it depends only on dashboardsql for the
// placeholder grammar. service imports servingsql, never the reverse,
// so the cachedTranspiler.ToServing signature structurally matches
// service.Transpiler.
package servingsql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"regexp"

	"github.com/vesahyp/clavesa/internal/dashboardsql"
)

// TranspilerVersion tags the cache key so a sqlglot/dialect upgrade
// invalidates every cached entry cleanly: the key folds this string in,
// so a bump changes every path and orphans the old `.trino` files
// (harmless leftovers, never read again).
const TranspilerVersion = "sqlglot-30.9.0-spark-athena-v1"

// Sentinel delimiters. We chose plain-ASCII markers wrapped in single
// quotes (`'__CLV_PH__<name>__CLV_END__'`) for three reasons:
//  1. Validity: the content is pure ASCII identifier characters and
//     underscores, so the surrounding single-quoted string literal is
//     well-formed SQL in both Spark and Trino dialects.
//  2. No collision: the `__CLV_PH__`/`__CLV_END__` markers are
//     deliberately unlikely in real dashboard SQL or in placeholder
//     names (the placeholder-name charset is
//     `[A-Za-z_][A-Za-z0-9_.\-]*`, which never produces this exact
//     bracketing run on its own), and because the name sits BETWEEN two
//     fixed markers the inverse regex can recover it unambiguously even
//     when it contains `.` or `-`.
//  3. Survives transpile: sqlglot treats a single-quoted literal as an
//     opaque value and round-trips it verbatim, so the marker comes out
//     the far side intact (validated end-to-end in Slice 4, not here —
//     this package only unit-tests the pure round-trip).
const (
	sentinelPrefix = "__CLV_PH__"
	sentinelSuffix = "__CLV_END__"
)

// sentinelRE matches the single-quoted sentinel literal produced by
// SentinelizeTemplate. Group 1 captures the placeholder name
// non-greedily so a name containing `.` or `-` is recovered whole and
// adjacent sentinels in one string don't bleed together. This is a
// dedicated matcher — NOT dashboardsql.PlaceholderRE — because by this
// point the `{{ }}` form is gone and we are matching the string-literal
// form instead.
var sentinelRE = regexp.MustCompile(`'` + regexp.QuoteMeta(sentinelPrefix) + `(.+?)` + regexp.QuoteMeta(sentinelSuffix) + `'`)

// SentinelizeTemplate replaces every `{{name}}` placeholder (matched via
// dashboardsql.PlaceholderRE, so the grammar is shared not redefined)
// with a single-quoted string-literal sentinel encoding the name. The
// name is captured from PlaceholderRE's group 1. Multiple occurrences of
// the same placeholder all map to the same sentinel. The result contains
// no `{{` or `}}`.
func SentinelizeTemplate(sql string) string {
	return dashboardsql.PlaceholderRE.ReplaceAllStringFunc(sql, func(match string) string {
		m := dashboardsql.PlaceholderRE.FindStringSubmatch(match)
		name := m[1]
		return "'" + sentinelPrefix + name + sentinelSuffix + "'"
	})
}

// DesentinelizeTrino is the inverse of SentinelizeTemplate: it finds each
// single-quoted sentinel literal and replaces it with `{{name}}`. Names
// containing `.` and `-` are handled via the non-greedy capture in
// sentinelRE.
//
// Round-trip invariant (unit-tested here):
// DesentinelizeTrino(SentinelizeTemplate(x)) == x for any x. In
// production the real transpiler sits between the two calls; because it
// preserves string literals the sentinel survives, so the pure
// round-trip is the testable invariant.
func DesentinelizeTrino(sql string) string {
	return sentinelRE.ReplaceAllString(sql, "{{$1}}")
}

// TranspileFunc is the inner transpiler as a plain func so this package
// stays free of interface coupling to service. The wiring slice passes
// service.Transpiler.ToServing (or the sidecar's ToServing) as this
// func.
type TranspileFunc func(ctx context.Context, sql string) (string, error)

// cachedTranspiler memoizes successful transpiles on disk, keyed by a
// content hash of the input SQL plus TranspilerVersion. Its ToServing
// method structurally matches service.Transpiler, so the wiring slice
// can wrap the sidecar in the cache and pass the cache to WithTranspiler.
type cachedTranspiler struct {
	cacheDir string
	inner    TranspileFunc
}

// NewCachedTranspiler builds a cachedTranspiler over cacheDir using inner
// as the underlying transpiler.
func NewCachedTranspiler(cacheDir string, inner TranspileFunc) *cachedTranspiler {
	return &cachedTranspiler{cacheDir: cacheDir, inner: inner}
}

// cacheKey is hex(sha256(TranspilerVersion + "\n" + sql)). Folding the
// version in means a TranspilerVersion bump changes every path.
func cacheKey(sql string) string {
	h := sha256.Sum256([]byte(TranspilerVersion + "\n" + sql))
	return hex.EncodeToString(h[:])
}

// ToServing returns the cached transpile of sql when present, otherwise
// calls inner and best-effort caches a successful result. Read errors
// are treated as a miss (never a hard failure). Inner errors are
// returned as-is and never cached — so errors.As for *DialectError still
// works at the caller, and a transient failure does not poison the
// cache.
func (c *cachedTranspiler) ToServing(ctx context.Context, sql string) (string, error) {
	key := cacheKey(sql)
	path := filepath.Join(c.cacheDir, key+".trino")

	// Hit: any read error falls through to a miss.
	if data, err := os.ReadFile(path); err == nil {
		return string(data), nil
	}

	// Miss: call inner. Never cache a failure; never wrap the error.
	out, err := c.inner(ctx, sql)
	if err != nil {
		return "", err
	}

	c.writeCache(path, out)
	return out, nil
}

// writeCache best-effort persists out to path via a same-dir temp file +
// atomic rename, so a concurrent reader never sees a torn file. Any
// failure is logged and ignored: the transpile already succeeded and the
// returned value is authoritative.
func (c *cachedTranspiler) writeCache(path, out string) {
	if err := os.MkdirAll(c.cacheDir, 0o755); err != nil {
		log.Printf("servingsql: cache mkdir %q failed (ignored): %v", c.cacheDir, err)
		return
	}
	tmp, err := os.CreateTemp(c.cacheDir, "transpile-*.tmp")
	if err != nil {
		log.Printf("servingsql: cache temp create failed (ignored): %v", err)
		return
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(out); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		log.Printf("servingsql: cache temp write failed (ignored): %v", err)
		return
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		log.Printf("servingsql: cache temp close failed (ignored): %v", err)
		return
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		log.Printf("servingsql: cache rename failed (ignored): %v", err)
	}
}
