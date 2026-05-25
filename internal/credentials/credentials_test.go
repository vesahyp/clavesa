package credentials

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestAddListGetDelete(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)

	if list, err := st.List(); err != nil || len(list) != 0 {
		t.Fatalf("List on empty workspace = %v / %v, want empty", list, err)
	}

	spec := Spec{
		Name: "stripe", Kind: "header",
		HeaderName: "Authorization", ValuePrefix: "Bearer ",
		Secret: "env:STRIPE_KEY",
	}
	if err := st.Add(spec); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := st.Get("stripe")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.HeaderName != spec.HeaderName || got.ValuePrefix != spec.ValuePrefix || got.Secret != spec.Secret {
		t.Errorf("round-trip mismatch: got %#v want %#v", got, spec)
	}
	if got.SecretBackend() != "env" {
		t.Errorf("SecretBackend = %q, want env", got.SecretBackend())
	}

	if err := st.Add(spec); err == nil {
		t.Errorf("Add of existing credential should refuse")
	}

	if err := st.Delete("stripe"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := st.Get("stripe"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Get after delete = %v, want os.ErrNotExist", err)
	}
}

func TestSecretBackendDispatch(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"arn:aws:secretsmanager:eu-north-1:123:secret:foo": "arn",
		"env:STRIPE_KEY":       "env",
		"file:./stripe.secret": "file",
		"weird:thing":          "",
	}
	for ref, want := range cases {
		got := Spec{Secret: ref}.SecretBackend()
		if got != want {
			t.Errorf("SecretBackend(%q) = %q, want %q", ref, got, want)
		}
	}
}

func TestValidationRejectsBadInputs(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	st := New(ws)
	cases := []struct {
		name string
		spec Spec
		want string
	}{
		{"missing name", Spec{Kind: "header", HeaderName: "Authorization", Secret: "env:K"}, "name is required"},
		{"unknown kind", Spec{Name: "stripe", Kind: "oauth", Secret: "env:K"}, "unsupported credential kind"},
		{"missing header", Spec{Name: "stripe", Kind: "header", Secret: "env:K"}, "header_name is required"},
		{"missing secret", Spec{Name: "stripe", Kind: "header", HeaderName: "Authorization"}, "secret reference is required"},
		{"bad backend", Spec{Name: "stripe", Kind: "header", HeaderName: "Authorization", Secret: "weird:thing"}, "known backend prefix"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := st.Add(c.spec)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Add(%#v) err = %v, want substring %q", c.spec, err, c.want)
			}
		})
	}
}
