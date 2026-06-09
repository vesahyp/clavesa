package service

import (
	"context"
	"errors"
	"testing"

	"github.com/vesahyp/clavesa/internal/observability"
)

// fakeTranspiler is a docker-free Transpiler stub: its toServing func
// field decides each call's outcome, so a test can drive every branch of
// Service.TranspileServing without the sqlglot sidecar. seen records every
// input it was handed so the dashboard-save tests can assert the template
// was sentinelized before transpile.
type fakeTranspiler struct {
	toServing func(ctx context.Context, sparkSQL string) (string, error)
	seen      []string
}

func (f *fakeTranspiler) ToServing(ctx context.Context, sparkSQL string) (string, error) {
	f.seen = append(f.seen, sparkSQL)
	return f.toServing(ctx, sparkSQL)
}

func TestTranspileServing(t *testing.T) {
	ctx := context.Background()
	const in = "SELECT datediff(d2, d1) AS n FROM t"

	t.Run("no transpiler wired returns input unchanged", func(t *testing.T) {
		s := New(t.TempDir())
		got, err := s.TranspileServing(ctx, in)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if got != in {
			t.Errorf("got %q, want input unchanged %q", got, in)
		}
	})

	t.Run("transpiled SQL returned verbatim", func(t *testing.T) {
		const out = "SELECT DATE_DIFF('day', d1, d2) AS n FROM t"
		s := New(t.TempDir()).WithTranspiler(&fakeTranspiler{
			toServing: func(context.Context, string) (string, error) { return out, nil },
		})
		got, err := s.TranspileServing(ctx, in)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if got != out {
			t.Errorf("got %q, want %q", got, out)
		}
	})

	t.Run("observability.DialectError translated to service.DialectError", func(t *testing.T) {
		obsErr := &observability.DialectError{Message: "cannot transpile FOO()", Line: 3, Col: 17}
		s := New(t.TempDir()).WithTranspiler(&fakeTranspiler{
			toServing: func(context.Context, string) (string, error) { return "", obsErr },
		})
		_, err := s.TranspileServing(ctx, in)
		if err == nil {
			t.Fatal("err = nil, want a *service.DialectError")
		}
		var de *DialectError
		if !errors.As(err, &de) {
			t.Fatalf("err = %T (%v), want *service.DialectError", err, err)
		}
		if de.Message != obsErr.Message || de.Line != obsErr.Line || de.Col != obsErr.Col {
			t.Errorf("got %+v, want {%q %d %d}", de, obsErr.Message, obsErr.Line, obsErr.Col)
		}
	})

	t.Run("plain error returned as-is", func(t *testing.T) {
		transportErr := errors.New("transpile server HTTP 503: down")
		s := New(t.TempDir()).WithTranspiler(&fakeTranspiler{
			toServing: func(context.Context, string) (string, error) { return "", transportErr },
		})
		_, err := s.TranspileServing(ctx, in)
		if !errors.Is(err, transportErr) {
			t.Fatalf("err = %v, want the transport error returned as-is", err)
		}
		var de *DialectError
		if errors.As(err, &de) {
			t.Errorf("transport error was wrongly translated into *service.DialectError: %+v", de)
		}
	})
}
