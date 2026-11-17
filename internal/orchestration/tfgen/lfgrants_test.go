package tfgen

import (
	"strings"
	"testing"
)

// TestWithGrantLifecycles: a grant gets a terraform_data holding its
// declared permissions and a lifecycle that ignores the provider's
// read-back and replaces it only when that terraform_data changes. Other
// resources are not touched.
func TestWithGrantLifecycles(t *testing.T) {
	in := `resource "aws_iam_role" "r" {
  name = "x"
}

resource "aws_lakeformation_permissions" "owner_db" {
  principal   = data.aws_caller_identity.clavesa_owner.arn
  permissions = ["DESCRIBE"]
  database {
    name = aws_glue_catalog_database.pipeline.name
  }
}
`
	want := `resource "aws_iam_role" "r" {
  name = "x"
}

resource "terraform_data" "owner_db_permissions" {
  input = ["DESCRIBE"]
}

resource "aws_lakeformation_permissions" "owner_db" {
  principal   = data.aws_caller_identity.clavesa_owner.arn
  permissions = ["DESCRIBE"]
  database {
    name = aws_glue_catalog_database.pipeline.name
  }

  lifecycle {
    ignore_changes       = [permissions, permissions_with_grant_option]
    replace_triggered_by = [terraform_data.owner_db_permissions]
  }
}
`
	got, err := withGrantLifecycles(in)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

// TestWithGrantLifecyclesRefusesUnreadableGrant: a grant whose
// permissions are not on one line, or that never closes, is an error, not
// a grant copied without its lifecycle.
func TestWithGrantLifecyclesRefusesUnreadableGrant(t *testing.T) {
	for name, in := range map[string]string{
		"multi-line permissions": `resource "aws_lakeformation_permissions" "x" {
  principal   = "p"
  permissions = [
    "DESCRIBE",
  ]
}
`,
		"no closing brace": `resource "aws_lakeformation_permissions" "x" {
  permissions = ["DESCRIBE"]
`,
	} {
		if _, err := withGrantLifecycles(in); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// TestEmitEveryGrantHasLifecycle: every Lake Formation grant Emit writes
// has its terraform_data and lifecycle.
func TestEmitEveryGrantHasLifecycle(t *testing.T) {
	got, err := Emit(basePipelineForLF("clavesa_demo", []string{"bronze"}))
	if err != nil {
		t.Fatal(err)
	}
	grants := strings.Count(got, `resource "aws_lakeformation_permissions"`)
	if grants == 0 {
		t.Fatal("no grants emitted")
	}
	if n := strings.Count(got, "replace_triggered_by = [terraform_data."); n != grants {
		t.Errorf("%d grants, %d with a lifecycle", grants, n)
	}
	if n := strings.Count(got, `_permissions" {`); n != grants {
		t.Errorf("%d grants, %d terraform_data", grants, n)
	}
}

// TestRunsWriterCannotWriteTreeCopy: the runs_writer role may write its
// bucket, except the hosted UI's tree copy under _clavesa/ (slice 3
// review, finding 1: its specs and translations choose the SQL the
// hosted reader runs).
func TestRunsWriterCannotWriteTreeCopy(t *testing.T) {
	got, err := Emit(basePipelineForLF("clavesa_demo", nil))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `resource "aws_iam_role_policy" "runs_writer"`) {
		t.Skip("this pipeline emits no runs_writer")
	}
	want := `{ Sid = "NoHostedTreeCopy", Effect = "Deny", Action = ["s3:PutObject", "s3:DeleteObject"], Resource = ["arn:aws:s3:::${`
	if !strings.Contains(got, want) || !strings.Contains(got, `}/_clavesa/*"] },`) {
		t.Errorf("runs_writer policy has no Deny on _clavesa/*:\n%s", got)
	}
}
