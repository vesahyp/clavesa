package access

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	ok := Config{Tags: []string{"open", "ip", "raw"}, Audiences: map[string][]string{"raw": {"open", "ip", "raw"}}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"no open", Config{Tags: []string{"ip"}}, `must include "open"`},
		{"unreviewed listed", Config{Tags: []string{"open", "unreviewed"}}, "clavesa's own"},
		{"twice", Config{Tags: []string{"open", "open"}}, "listed twice"},
		{"bad value", Config{Tags: []string{"open", "IP"}}, "must match"},
		{"colon audience", Config{Tags: []string{"open"}, Audiences: map[string][]string{"a:b": {"open"}}}, "must match"},
		{"reserved audience", Config{Tags: []string{"open"}, Audiences: map[string][]string{"all": {"open"}}}, "reserved"},
		{"unknown value", Config{Tags: []string{"open"}, Audiences: map[string][]string{"raw": {"ip"}}}, `reads "ip"`},
		{"too many", Config{Tags: []string{"open"}, Audiences: map[string][]string{"a": nil, "b": nil, "c": nil, "d": nil}}, "at most 3"},
	}
	for _, c := range cases {
		err := c.cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want it to contain %q", c.name, err, c.want)
		}
	}
}

func TestParseOutputAndCheck(t *testing.T) {
	cfg := &Config{Tags: []string{"open", "ip"}}
	o, err := ParseOutput(map[string]any{
		"table_tag":   "open",
		"column_tags": map[string]any{"c_ip": "ip"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.TableTag != "open" || o.Columns["c_ip"] != "ip" {
		t.Fatalf("parsed %+v", o)
	}
	if err := o.Check(cfg); err != nil {
		t.Fatalf("check: %v", err)
	}
	if err := o.Check(nil); err == nil || !strings.Contains(err.Error(), "no \"access\" block") {
		t.Errorf("no block: %v", err)
	}
	bad, _ := ParseOutput(map[string]any{"column_tags": map[string]any{"email": "pii"}})
	if err := bad.Check(cfg); err == nil || !strings.Contains(err.Error(), `column_tags["email"] = "pii"`) {
		t.Errorf("undeclared value: %v", err)
	}
	if _, err := ParseOutput(map[string]any{"column_tags": map[string]any{"c_ip": []any{"ip"}}}); err == nil {
		t.Error("a list value must fail: a column has one value")
	}
	if _, err := ParseOutput(map[string]any{"table_tag": true}); err == nil {
		t.Error("a non-string table_tag must fail")
	}
	none, _ := ParseOutput(map[string]any{"stats": true})
	if !none.Empty() || none.Check(nil) != nil {
		t.Error("an output with no tags needs no block")
	}
}

func TestNames(t *testing.T) {
	if got := TagKey("web-traffic"); got != "clavesa_web_traffic" {
		t.Errorf("TagKey = %q", got)
	}
	if got := HostedGroup("smoke2", "raw"); got != "aud:smoke2:raw" {
		t.Errorf("HostedGroup = %q", got)
	}
}
