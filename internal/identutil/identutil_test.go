package identutil

import (
	"strings"
	"testing"
)

func TestSanitize(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"demo", "demo"},
		{"demo-ws", "demo_ws"},
		{"cloudfront-pipeline", "cloudfront_pipeline"},
		{"a-b-c", "a_b_c"},
		{"already_underscored", "already_underscored"},
		{"", ""},
	}
	for _, c := range cases {
		if got := Sanitize(c.in); got != c.want {
			t.Errorf("Sanitize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestEncodeExternalTableRef(t *testing.T) {
	cases := []struct {
		name, catalog, ref, want string
		wantErr                  bool
	}{
		{
			name:    "plain schema.table",
			catalog: "clavesa_demo_ws",
			ref:     "cloudfront.trips",
			want:    "clavesa_demo_ws__cloudfront.trips",
		},
		{
			name:    "dashed schema sanitized at boundary",
			catalog: "clavesa-demo-ws",
			ref:     "cloudfront-pipeline.dim_customers",
			want:    "clavesa_demo_ws__cloudfront_pipeline.dim_customers",
		},
		{
			name:    "short clavesa catalog",
			catalog: "clavesa",
			ref:     "cloudfront.trips",
			want:    "clavesa__cloudfront.trips",
		},
		{
			name:    "missing dot is rejected",
			catalog: "clavesa",
			ref:     "trips",
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := EncodeExternalTableRef(c.catalog, c.ref)
			if c.wantErr {
				if err == nil {
					t.Errorf("EncodeExternalTableRef(%q, %q) = %q, want error", c.catalog, c.ref, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("EncodeExternalTableRef(%q, %q) error: %v", c.catalog, c.ref, err)
			}
			if got != c.want {
				t.Errorf("EncodeExternalTableRef(%q, %q) = %q, want %q", c.catalog, c.ref, got, c.want)
			}
			// v2.0.0: Delta lives under spark_catalog; identifier MUST
			// NOT carry the legacy "clavesa." prefix.
			if strings.HasPrefix(got, "clavesa.") {
				t.Errorf("EncodeExternalTableRef(%q, %q) = %q still starts with legacy clavesa. prefix", c.catalog, c.ref, got)
			}
		})
	}
}

func TestEncodeGlueDatabase(t *testing.T) {
	cases := []struct {
		name, catalog, schema, want string
	}{
		{
			name:    "default catalog + plain schema",
			catalog: "clavesa_demo_ws", schema: "cloudfront",
			want: "clavesa_demo_ws__cloudfront",
		},
		{
			name:    "both dashed → both sanitized at boundary",
			catalog: "clavesa-demo-ws", schema: "cloudfront-pipeline",
			want: "clavesa_demo_ws__cloudfront_pipeline",
		},
		{
			name:    "explicit override to short `clavesa` catalog",
			catalog: "clavesa", schema: "cloudfront",
			want: "clavesa__cloudfront",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := EncodeGlueDatabase(c.catalog, c.schema); got != c.want {
				t.Errorf("EncodeGlueDatabase(%q, %q) = %q, want %q",
					c.catalog, c.schema, got, c.want)
			}
		})
	}
}
