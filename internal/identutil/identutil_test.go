package identutil

import "testing"

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
