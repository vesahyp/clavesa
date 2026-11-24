package modules

import (
	"fmt"
	"io/fs"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// The admins role of the hosted reader reads every table and column, so it
// must never share anything out or change grants (docs/ideas/access-model.md).
// adminProblems checks the module in fsys (the hosted-reader/aws directory)
// and returns what breaks that rule.

// deniedToAdmin must all be in the admin policy's Deny statement.
var deniedToAdmin = []string{
	"ram:*",
	"lakeformation:Grant*",
	"lakeformation:BatchGrant*",
	"glue:PutResourcePolicy",
	"s3:PutBucketPolicy",
	"s3:PutObjectAcl",
}

// adminReferrers are the only resources that may refer to aws_iam_role.admin.
var adminReferrers = map[string]bool{
	"aws_iam_role_policy.admin":                  true,
	"aws_lakeformation_permissions.admin_db":     true,
	"aws_lakeformation_permissions.admin_tables": true,
}

type statement struct {
	keys map[string]bool      // every key the statement sets
	lit  map[string]cty.Value // the keys whose value is a literal
}

func adminProblems(fsys fs.FS) ([]string, error) {
	var problems []string
	bad := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }

	resources := map[string]*hclsyntax.Block{}
	names, err := fs.Glob(fsys, "*.tf")
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		src, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, err
		}
		file, diags := hclsyntax.ParseConfig(src, name, hcl.InitialPos)
		if diags.HasErrors() {
			return nil, diags
		}
		for _, b := range file.Body.(*hclsyntax.Body).Blocks {
			if b.Type == "resource" && len(b.Labels) == 2 {
				resources[b.Labels[0]+"."+b.Labels[1]] = b
			}
		}
	}

	for addr, b := range resources {
		if !adminReferrers[addr] && refersToAdminRole(b.Body) {
			bad("%s refers to aws_iam_role.admin", addr)
		}
	}

	policy, ok := resources["aws_iam_role_policy.admin"]
	if !ok {
		return nil, fmt.Errorf("no aws_iam_role_policy.admin")
	}
	statements, err := policyStatements(policy)
	if err != nil {
		return nil, err
	}
	denyFound := false
	for _, st := range statements {
		sid := strs(st.lit["Sid"])
		for _, k := range []string{"NotAction", "NotResource", "NotPrincipal"} {
			if st.keys[k] {
				bad("statement %v uses %s", sid, k)
			}
		}
		effect := strs(st.lit["Effect"])
		if len(effect) != 1 {
			bad("statement %v has no literal Effect", sid)
			continue
		}
		actions := strs(st.lit["Action"])
		switch effect[0] {
		case "Allow":
			if _, lit := st.lit["Action"]; !lit {
				bad("Allow statement %v: Action is not a literal", sid)
			}
			for _, a := range actions {
				la := strings.ToLower(a)
				if strings.HasPrefix(la, "ram:") || la == "*" ||
					(strings.HasPrefix(la, "lakeformation:") && la != "lakeformation:getdataaccess") {
					bad("Allow statement %v allows %q", sid, a)
				}
			}
		case "Deny":
			res := strs(st.lit["Resource"])
			if len(res) != 1 || res[0] != "*" {
				continue
			}
			have := map[string]bool{}
			for _, a := range actions {
				have[a] = true
			}
			if !have["ram:*"] {
				continue
			}
			denyFound = true
			if st.keys["Condition"] {
				bad("the Deny statement %v has a Condition", sid)
			}
			for _, want := range deniedToAdmin {
				if !have[want] {
					bad("the Deny statement %v does not deny %s", sid, want)
				}
			}
		}
	}
	if !denyFound {
		bad(`admin policy has no statement {Effect = "Deny", Action = ["ram:*", ...], Resource = "*"}`)
	}

	for _, addr := range []string{"aws_lakeformation_permissions.admin_db", "aws_lakeformation_permissions.admin_tables"} {
		b, ok := resources[addr]
		if !ok {
			bad("no %s", addr)
			continue
		}
		if attr, ok := b.Body.Attributes["permissions_with_grant_option"]; ok {
			v, d := attr.Expr.Value(nil)
			if d.HasErrors() || !v.IsKnown() || v.IsNull() || !v.CanIterateElements() || v.LengthInt() != 0 {
				bad("%s sets permissions_with_grant_option", addr)
			}
		}
		for _, lc := range b.Body.Blocks {
			if lc.Type != "lifecycle" {
				continue
			}
			if attr, ok := lc.Body.Attributes["ignore_changes"]; ok {
				if tuple, ok := attr.Expr.(*hclsyntax.TupleConsExpr); ok {
					for _, e := range tuple.Exprs {
						if tr, d := hcl.AbsTraversalForExpr(e); !d.HasErrors() && tr.RootName() == "permissions_with_grant_option" {
							bad("%s ignores changes to permissions_with_grant_option", addr)
						}
					}
				} else {
					bad("%s: ignore_changes is not a list", addr)
				}
			}
		}
	}
	return problems, nil
}

// refersToAdminRole reports whether any expression in body, nested blocks
// included, refers to aws_iam_role.admin.
func refersToAdminRole(body *hclsyntax.Body) bool {
	for _, attr := range body.Attributes {
		for _, tr := range attr.Expr.Variables() {
			if tr.RootName() != "aws_iam_role" || len(tr) < 2 {
				continue
			}
			if step, ok := tr[1].(hcl.TraverseAttr); ok && step.Name == "admin" {
				return true
			}
		}
	}
	for _, b := range body.Blocks {
		if refersToAdminRole(b.Body) {
			return true
		}
	}
	return false
}

// policyStatements returns the statements of the jsonencode()d policy of
// b: the keys each one sets, and the values of those that are literals.
func policyStatements(b *hclsyntax.Block) ([]statement, error) {
	attr, ok := b.Body.Attributes["policy"]
	if !ok {
		return nil, fmt.Errorf("aws_iam_role_policy.admin has no policy")
	}
	call, ok := attr.Expr.(*hclsyntax.FunctionCallExpr)
	if !ok || call.Name != "jsonencode" || len(call.Args) != 1 {
		return nil, fmt.Errorf("aws_iam_role_policy.admin policy is not jsonencode({...})")
	}
	doc, ok := call.Args[0].(*hclsyntax.ObjectConsExpr)
	if !ok {
		return nil, fmt.Errorf("aws_iam_role_policy.admin policy is not an object")
	}
	for _, item := range doc.Items {
		if key, _ := item.KeyExpr.Value(nil); key.Type() != cty.String || key.AsString() != "Statement" {
			continue
		}
		tuple, ok := item.ValueExpr.(*hclsyntax.TupleConsExpr)
		if !ok {
			return nil, fmt.Errorf("aws_iam_role_policy.admin Statement is not a list")
		}
		var out []statement
		for _, e := range tuple.Exprs {
			obj, ok := e.(*hclsyntax.ObjectConsExpr)
			if !ok {
				return nil, fmt.Errorf("aws_iam_role_policy.admin has a statement that is not an object")
			}
			st := statement{keys: map[string]bool{}, lit: map[string]cty.Value{}}
			for _, f := range obj.Items {
				k, d := f.KeyExpr.Value(nil)
				if d.HasErrors() || k.Type() != cty.String {
					return nil, fmt.Errorf("aws_iam_role_policy.admin has a statement key that is not a literal")
				}
				st.keys[k.AsString()] = true
				if v, d := f.ValueExpr.Value(nil); !d.HasErrors() {
					st.lit[k.AsString()] = v
				}
			}
			out = append(out, st)
		}
		return out, nil
	}
	return nil, fmt.Errorf("aws_iam_role_policy.admin policy has no Statement")
}

func strs(v cty.Value) []string {
	if !v.IsKnown() || v.IsNull() {
		return nil
	}
	if v.Type() == cty.String {
		return []string{v.AsString()}
	}
	var out []string
	if v.CanIterateElements() {
		for it := v.ElementIterator(); it.Next(); {
			_, e := it.Element()
			if e.Type() == cty.String {
				out = append(out, e.AsString())
			}
		}
	}
	return out
}

// hostedReaderFS is the embedded module, or the directory in
// CLAVESA_HOSTED_READER_DIR (to check a changed copy by hand).
func hostedReaderFS(t *testing.T) fs.FS {
	if dir := os.Getenv("CLAVESA_HOSTED_READER_DIR"); dir != "" {
		return os.DirFS(dir)
	}
	sub, err := fs.Sub(FS, "files/hosted-reader/aws")
	if err != nil {
		t.Fatal(err)
	}
	return sub
}

func TestHostedReaderAdminCannotShare(t *testing.T) {
	problems, err := adminProblems(hostedReaderFS(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// Each check fails on a module changed to break it.
func TestHostedReaderAdminChecksCatchMutations(t *testing.T) {
	if os.Getenv("CLAVESA_HOSTED_READER_DIR") != "" {
		t.Skip("checking a copy by hand")
	}
	base := hostedReaderFS(t)
	mainTF, err := fs.ReadFile(base, "main.tf")
	if err != nil {
		t.Fatal(err)
	}
	const deny = `        Sid    = "NoResourceSharing"
        Effect = "Deny"`
	cases := []struct {
		name, old, new, want string
		extra                string // another .tf file
	}{
		{"no deny", `"ram:*",`, ``, "no statement", ""},
		{"deny misses an action", `"s3:PutObjectAcl",`, ``, "does not deny s3:PutObjectAcl", ""},
		{"deny with condition", deny, deny + "\n        Condition = { Bool = { \"aws:ViaAWSService\" = \"false\" } }", "has a Condition", ""},
		{"not action", `Sid      = "LakeFormationDataAccess"`, `Sid      = "LakeFormationDataAccess"
        NotAction = ["s3:GetObject"]`, "uses NotAction", ""},
		{"not resource", `Sid      = "LakeFormationDataAccess"`, `Sid      = "LakeFormationDataAccess"
        NotResource = ["*"]`, "uses NotResource", ""},
		{"allow action not literal", `Action   = ["lakeformation:GetDataAccess"]`, `Action   = local.admin_actions`, "Action is not a literal", ""},
		{"allow lake formation grant", `Action   = ["lakeformation:GetDataAccess"]`, `Action   = ["lakeformation:GetDataAccess", "lakeformation:GrantPermissions"]`, `allows "lakeformation:GrantPermissions"`, ""},
		{"grant option on database", "  permissions_with_grant_option = []\n  database {", "  permissions_with_grant_option = [\"DESCRIBE\"]\n  database {", "admin_db sets permissions_with_grant_option", ""},
		{"grant option ignored", "  lifecycle {\n    ignore_changes       = [permissions]\n    replace_triggered_by = [terraform_data.reader_tables_permissions]",
			"  lifecycle {\n    ignore_changes       = [permissions, permissions_with_grant_option]\n    replace_triggered_by = [terraform_data.reader_tables_permissions]",
			"admin_tables ignores changes to permissions_with_grant_option", ""},
		{"other policy attached", "", "", "aws_iam_role_policy_attachment.extra refers to aws_iam_role.admin",
			"resource \"aws_iam_role_policy_attachment\" \"extra\" {\n  role       = aws_iam_role.admin[0].name\n  policy_arn = \"arn:aws:iam::aws:policy/AdministratorAccess\"\n}\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			changed := string(mainTF)
			if c.old != "" {
				if strings.Count(changed, c.old) < 1 {
					t.Fatalf("mutation does not apply: %q not in main.tf", c.old)
				}
				// The last match: the admin resources come after the
				// reader's, which share some lines.
				i := strings.LastIndex(changed, c.old)
				changed = changed[:i] + c.new + changed[i+len(c.old):]
			}
			fsys := fstest.MapFS{"main.tf": {Data: []byte(changed)}}
			if c.extra != "" {
				fsys["extra.tf"] = &fstest.MapFile{Data: []byte(c.extra)}
			}
			problems, err := adminProblems(fsys)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(problems, "\n"), c.want) {
				t.Errorf("want a problem with %q, got %q", c.want, problems)
			}
		})
	}
}
