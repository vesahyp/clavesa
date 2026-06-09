package servingsql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSentinelRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "no placeholders",
			sql:  "SELECT SUM(amount) AS total FROM orders WHERE status = 'paid'",
		},
		{
			name: "dotted names twice",
			sql: "SELECT SUM(x) FROM t " +
				"WHERE event_date >= CAST(substr({{period.start}}, 1, 10) AS DATE) " +
				"AND event_date < CAST(substr({{period.end}}, 1, 10) AS DATE)",
		},
		{
			name: "hyphenated and dotted name",
			sql:  "SELECT * FROM t WHERE region = {{control-name.value}}",
		},
		{
			name: "same placeholder twice",
			sql:  "SELECT * FROM t WHERE a = {{period.start}} OR b = {{period.start}}",
		},
		{
			name: "single simple placeholder",
			sql:  "SELECT * FROM t WHERE id = {{user_id}}",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sent := SentinelizeTemplate(tc.sql)
			if strings.Contains(sent, "{{") || strings.Contains(sent, "}}") {
				t.Fatalf("sentinelized form still contains placeholder braces: %q", sent)
			}
			got := DesentinelizeTrino(sent)
			if got != tc.sql {
				t.Fatalf("round-trip mismatch\n in: %q\nout: %q\nsent: %q", tc.sql, got, sent)
			}
		})
	}
}

func TestDesentinelizeTrinoDirect(t *testing.T) {
	// As it would appear in transpiler output: the sentinel single-quoted
	// literal embedded in Trino SQL, including a dotted name.
	in := "SELECT * FROM t WHERE d >= CAST(substr('__CLV_PH__period.start__CLV_END__', 1, 10) AS DATE)"
	want := "SELECT * FROM t WHERE d >= CAST(substr({{period.start}}, 1, 10) AS DATE)"
	if got := DesentinelizeTrino(in); got != want {
		t.Fatalf("desentinelize mismatch\ngot:  %q\nwant: %q", got, want)
	}
}

func TestCacheKeyMatchesExpectedPath(t *testing.T) {
	// The cache file name must equal sha256(TranspilerVersion+"\n"+sql),
	// so a TranspilerVersion bump changes the path and orphans old entries.
	dir := t.TempDir()
	calls := 0
	inner := func(ctx context.Context, sql string) (string, error) {
		calls++
		return "TRINO:" + sql, nil
	}
	c := NewCachedTranspiler(dir, inner)
	sql := "SELECT 1"
	if _, err := c.ToServing(context.Background(), sql); err != nil {
		t.Fatalf("ToServing: %v", err)
	}
	wantPath := filepath.Join(dir, cacheKey(sql)+".trino")
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("expected cache file at %q: %v", wantPath, err)
	}
}

func TestCacheHitMissAndInvalidation(t *testing.T) {
	dir := t.TempDir()
	calls := 0
	inner := func(ctx context.Context, sql string) (string, error) {
		calls++
		return "TRINO:" + sql, nil
	}
	c := NewCachedTranspiler(dir, inner)
	ctx := context.Background()

	// First call: miss, inner invoked once, file created.
	got, err := c.ToServing(ctx, "SELECT a")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got != "TRINO:SELECT a" {
		t.Fatalf("unexpected result: %q", got)
	}
	if calls != 1 {
		t.Fatalf("expected 1 inner call, got %d", calls)
	}
	if _, err := os.Stat(filepath.Join(dir, cacheKey("SELECT a")+".trino")); err != nil {
		t.Fatalf("cache file not created: %v", err)
	}

	// Second identical call: hit, inner NOT called again, same result.
	got2, err := c.ToServing(ctx, "SELECT a")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got2 != got {
		t.Fatalf("hit result differs: %q vs %q", got2, got)
	}
	if calls != 1 {
		t.Fatalf("hit should not re-call inner; got %d calls", calls)
	}

	// Different SQL: miss, inner called again.
	if _, err := c.ToServing(ctx, "SELECT b"); err != nil {
		t.Fatalf("third call: %v", err)
	}
	if calls != 2 {
		t.Fatalf("different SQL should re-call inner; got %d calls", calls)
	}
}

type dialectError struct{ msg string }

func (e *dialectError) Error() string { return e.msg }

func TestCacheDoesNotCacheFailures(t *testing.T) {
	dir := t.TempDir()
	calls := 0
	sentinelErr := &dialectError{msg: "cannot map construct"}
	inner := func(ctx context.Context, sql string) (string, error) {
		calls++
		return "", sentinelErr
	}
	c := NewCachedTranspiler(dir, inner)
	ctx := context.Background()

	_, err := c.ToServing(ctx, "SELECT bad")
	if err == nil {
		t.Fatal("expected error from inner")
	}
	// Error propagated as-is so errors.As still works at the caller.
	var de *dialectError
	if !errors.As(err, &de) {
		t.Fatalf("error not propagated as-is: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 inner call, got %d", calls)
	}
	// No cache file written for a failure.
	if _, statErr := os.Stat(filepath.Join(dir, cacheKey("SELECT bad")+".trino")); !os.IsNotExist(statErr) {
		t.Fatalf("failure should not be cached; stat err = %v", statErr)
	}

	// Subsequent identical call calls inner again (failures aren't cached).
	if _, err := c.ToServing(ctx, "SELECT bad"); err == nil {
		t.Fatal("expected error again")
	}
	if calls != 2 {
		t.Fatalf("failure must re-call inner; got %d calls", calls)
	}
}
