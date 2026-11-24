// Package access holds column tags (docs/ideas/column-tags.md): the
// "access" block of clavesa.json and the table_tag and column_tags of a
// transform output. At deploy, the tags become Lake Formation tags under
// one key per workspace, and Lake Formation grants each reader role the
// values its audience may read. This package only checks and reads the
// declarations; it hides nothing itself.
package access

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Unreviewed is the value every pipeline database carries, so a table or
// column that no declaration tags inherits it. No audience reads it.
const Unreviewed = "unreviewed"

// Open is the value the default audience reads.
const Open = "open"

// MaxAudiences caps the named audiences of one workspace.
const MaxAudiences = 3

// valueRE is what a tag value or audience name must match: lowercase, no
// ":" (the Cognito group separator, aud:<workspace>:<audience>).
var valueRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,49}$`)

// reservedAudiences are Cognito group names or prefixes (ADR-035).
var reservedAudiences = map[string]bool{"ws": true, "all": true, "aud": true}

// Config is the "access" block of clavesa.json.
type Config struct {
	// Tags are the tag values the workspace allows. "open" is required;
	// "unreviewed" is clavesa's own and may not be listed.
	Tags []string `json:"tags"`
	// Audiences map a named audience to the tag values it reads. The
	// default audience (no audience group) reads "open" only.
	Audiences map[string][]string `json:"audiences,omitempty"`
	// AdminRole makes the hosted admin role clavesa-ui-admin-<workspace>,
	// with which the hosted UI's admins (group "all") read every table
	// and column. Off by default: a new IAM role.
	AdminRole bool `json:"admin_role,omitempty"`
}

// Validate checks the block.
func (c *Config) Validate() error {
	seen := map[string]bool{}
	for _, t := range c.Tags {
		if !valueRE.MatchString(t) {
			return fmt.Errorf("access: tag %q must match %s", t, valueRE)
		}
		if t == Unreviewed {
			return fmt.Errorf("access: tag %q is clavesa's own (the value of untagged tables and columns); remove it from \"tags\"", Unreviewed)
		}
		if seen[t] {
			return fmt.Errorf("access: tag %q is listed twice", t)
		}
		seen[t] = true
	}
	if !seen[Open] {
		return fmt.Errorf("access: \"tags\" must include %q, the value every viewer reads", Open)
	}
	if len(c.Audiences) > MaxAudiences {
		return fmt.Errorf("access: %d audiences; at most %d", len(c.Audiences), MaxAudiences)
	}
	for _, name := range c.AudienceNames() {
		if !valueRE.MatchString(name) {
			return fmt.Errorf("access: audience %q must match %s", name, valueRE)
		}
		if reservedAudiences[name] {
			return fmt.Errorf("access: audience name %q is reserved", name)
		}
		for _, v := range c.Audiences[name] {
			if !seen[v] {
				return fmt.Errorf("access: audience %q reads %q, which is not in \"tags\"", name, v)
			}
		}
	}
	return nil
}

// Allows reports whether v is a declared tag value.
func (c *Config) Allows(v string) bool {
	for _, t := range c.Tags {
		if t == v {
			return true
		}
	}
	return false
}

// AudienceNames returns the named audiences, sorted.
func (c *Config) AudienceNames() []string {
	names := make([]string, 0, len(c.Audiences))
	for n := range c.Audiences {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// TagKey is the workspace's Lake Formation tag key: clavesa_<workspace>,
// with every character outside [a-z0-9_] as "_".
func TagKey(workspace string) string {
	var b strings.Builder
	b.WriteString("clavesa_")
	for _, r := range strings.ToLower(workspace) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// HostedGroup is the Cognito group of one audience of one workspace.
func HostedGroup(workspace, audience string) string { return "aud:" + workspace + ":" + audience }

// OutputTags are the tags of one transform output.
type OutputTags struct {
	// TableTag is the table's value; "" leaves the database's
	// "unreviewed".
	TableTag string `json:"table_tag,omitempty"`
	// Columns maps a column name to its value.
	Columns map[string]string `json:"column_tags,omitempty"`
}

// Empty reports whether the output declares no tag.
func (o OutputTags) Empty() bool { return o.TableTag == "" && len(o.Columns) == 0 }

// ParseOutput reads table_tag and column_tags from one entry of a
// transform's output_definitions, as the HCL parser gives it.
func ParseOutput(def map[string]any) (OutputTags, error) {
	var o OutputTags
	if raw, ok := def["table_tag"]; ok && raw != nil {
		s, ok := raw.(string)
		if !ok {
			return o, fmt.Errorf("table_tag must be a string")
		}
		o.TableTag = s
	}
	if raw, ok := def["column_tags"]; ok && raw != nil {
		m, ok := raw.(map[string]any)
		if !ok {
			return o, fmt.Errorf("column_tags must be a map of column name to tag value")
		}
		o.Columns = make(map[string]string, len(m))
		for col, v := range m {
			s, ok := v.(string)
			if !ok {
				return o, fmt.Errorf("column_tags[%q] must be one tag value (a string)", col)
			}
			o.Columns[col] = s
		}
	}
	return o, nil
}

// Check reports the first value of o that cfg does not declare. A nil cfg
// (no "access" block) makes every tag an error.
func (o OutputTags) Check(cfg *Config) error {
	if o.Empty() {
		return nil
	}
	if cfg == nil {
		return fmt.Errorf("the output has tags, but clavesa.json has no \"access\" block that declares them")
	}
	if o.TableTag != "" && !cfg.Allows(o.TableTag) {
		return fmt.Errorf("table_tag %q is not in clavesa.json \"access\" \"tags\"", o.TableTag)
	}
	cols := make([]string, 0, len(o.Columns))
	for c := range o.Columns {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	for _, c := range cols {
		if !cfg.Allows(o.Columns[c]) {
			return fmt.Errorf("column_tags[%q] = %q is not in clavesa.json \"access\" \"tags\"", c, o.Columns[c])
		}
	}
	return nil
}
