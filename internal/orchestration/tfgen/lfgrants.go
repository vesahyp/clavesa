package tfgen

import (
	"fmt"
	"regexp"
	"strings"
)

// lfGrantStartRE matches the first line of an emitted Lake Formation grant.
var lfGrantStartRE = regexp.MustCompile(`^resource "aws_lakeformation_permissions" "([A-Za-z0-9_]+)" \{$`)

// lfPermissionsRE matches a grant's own permissions line.
var lfPermissionsRE = regexp.MustCompile(`^  permissions\s*=\s*(\[.*\])$`)

// withGrantLifecycles makes every emitted aws_lakeformation_permissions
// stop drifting. The AWS provider reads a grant back with permissions the
// principal holds from elsewhere: IAM_ALLOWED_PRINCIPALS' ALL on a database
// created with IAM-only access, an admin's own grants, ALL with grant
// option on a table its principal created. Every plan then showed the
// grant as "must be replaced" (2026-11-17, smoke2, ucpub, ucguide,
// web-traffic), and applying that replace revokes what the state read
// back. So each grant ignores the read-back (permissions and
// permissions_with_grant_option) and is replaced only when its declared
// permissions change: a terraform_data "<name>_permissions" holds them and
// is the grant's replace_triggered_by. Adding the terraform_data to an
// existing stack creates it and replaces nothing.
//
// Before you change a declared permission set: the replace that follows
// still revokes what the STATE holds, and refresh keeps writing the
// read-back there (ALL from IAM_ALLOWED_PRINCIPALS; ALTER, DROP and ALL
// with grant option for a deployer that created the tables). Apply such a
// release on smoke2 with a real apply before it reaches a shared workspace
// such as web-traffic (review of 679682a, finding 2).
//
// permissions_with_grant_option is ignored but not in the trigger: no
// grant declares it today. A grant that does must put both lists in its
// terraform_data input, or a change to the grant option never applies.
//
// Every grant must have its permissions on one line; a grant this pass
// cannot read is an error, so a new emitter cannot bring the false
// replace back unnoticed.
func withGrantLifecycles(tf string) (string, error) {
	lines := strings.Split(tf, "\n")
	var out []string
	for i := 0; i < len(lines); i++ {
		m := lfGrantStartRE.FindStringSubmatch(lines[i])
		if m == nil {
			out = append(out, lines[i])
			continue
		}
		name := m[1]
		// The block ends at the first line that is exactly "}".
		end := i + 1
		for end < len(lines) && lines[end] != "}" {
			end++
		}
		if end == len(lines) {
			return "", fmt.Errorf("tfgen: Lake Formation grant %q has no closing brace at column 0", name)
		}
		perms := ""
		for _, l := range lines[i+1 : end] {
			if pm := lfPermissionsRE.FindStringSubmatch(l); pm != nil {
				perms = pm[1]
			}
		}
		if perms == "" {
			return "", fmt.Errorf("tfgen: Lake Formation grant %q has no one-line permissions = [...]; its lifecycle cannot be emitted", name)
		}
		out = append(out,
			fmt.Sprintf(`resource "terraform_data" "%s_permissions" {`, name),
			"  input = "+perms,
			"}",
			"",
		)
		out = append(out, lines[i:end]...)
		out = append(out,
			"",
			"  lifecycle {",
			"    ignore_changes       = [permissions, permissions_with_grant_option]",
			fmt.Sprintf("    replace_triggered_by = [terraform_data.%s_permissions]", name),
			"  }",
			"}",
		)
		i = end
	}
	return strings.Join(out, "\n"), nil
}
