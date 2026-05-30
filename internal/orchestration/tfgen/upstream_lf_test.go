package tfgen

import (
	"strings"
	"testing"
)

// basePipelineForLF returns a minimal valid Pipeline (passes validate())
// with the given UpstreamSchemas, so the GH #4 cross-pipeline Lake Formation
// grants are the only thing under test.
func basePipelineForLF(catalog string, upstreamSchemas []string) Pipeline {
	return Pipeline{
		PipelineNameExpr: "var.pipeline_name",
		Catalog:          catalog,
		SchemaExpr:       "var.schema",
		SystemCatalog:    "clavesa_demo_system",
		BucketExpr:       "data.terraform_remote_state.workspace.outputs.pipeline_bucket",
		RunnerImageExpr:  "data.terraform_remote_state.workspace.outputs.runner_image",
		Transforms: []TransformConfig{
			{
				NodeID:      "silver",
				Language:    "sql",
				LogicS3URI:  `"s3://bucket/p/silver/_runtime/logic.sql"`,
				InputsExpr:  "{}",
				OutputsExpr: `{ default = "" }`,
			},
		},
		UpstreamSchemas: upstreamSchemas,
	}
}

func TestEmit_UpstreamLakeFormationGrants(t *testing.T) {
	t.Parallel()
	// Two distinct upstream schemas. (The service layer dedupes; tfgen
	// receives the already-deduped slice and must render exactly one grant
	// pair per entry.)
	got, err := Emit(basePipelineForLF("clavesa_demo", []string{"bronze", "raw_events"}))
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}

	for _, schema := range []string{"bronze", "raw_events"} {
		dbRes := `resource "aws_lakeformation_permissions" "input_` + schema + `_db"`
		tblRes := `resource "aws_lakeformation_permissions" "input_` + schema + `_tables"`
		if n := strings.Count(got, dbRes); n != 1 {
			t.Errorf("schema %q: want exactly 1 db grant resource, got %d", schema, n)
		}
		if n := strings.Count(got, tblRes); n != 1 {
			t.Errorf("schema %q: want exactly 1 tables grant resource, got %d", schema, n)
		}
		dbBlock := `resource "aws_lakeformation_permissions" "input_` + schema + `_db" {
  principal   = aws_iam_role.pipeline_runner.arn
  permissions = ["DESCRIBE"]
  database {
    name = "clavesa_demo__` + schema + `"
  }
}`
		if !strings.Contains(got, dbBlock) {
			t.Errorf("schema %q: db grant block not found verbatim.\nwant:\n%s\n\ngot:\n%s", schema, dbBlock, got)
		}
		tblBlock := `resource "aws_lakeformation_permissions" "input_` + schema + `_tables" {
  principal   = aws_iam_role.pipeline_runner.arn
  permissions = ["SELECT", "DESCRIBE"]
  table {
    database_name = "clavesa_demo__` + schema + `"
    wildcard      = true
  }
}`
		if !strings.Contains(got, tblBlock) {
			t.Errorf("schema %q: tables grant block not found verbatim.\nwant:\n%s", schema, tblBlock)
		}
	}

	// Exactly two distinct schemas → exactly four input_* resources total
	// (2 schemas × {db, tables}); no stray pair for a third / own schema.
	if n := strings.Count(got, `"aws_lakeformation_permissions" "input_`); n != 4 {
		t.Errorf("want 4 input_* LF grant resources (2 schemas × {db,tables}), got %d", n)
	}
}

func TestEmit_NoUpstreamSchemasEmitsNoInputGrants(t *testing.T) {
	t.Parallel()
	got, err := Emit(basePipelineForLF("clavesa_demo", nil))
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if strings.Contains(got, `"aws_lakeformation_permissions" "input_`) {
		t.Errorf("empty UpstreamSchemas must emit no input_* LF grants, but found one:\n%s", got)
	}
	// Own-DB grants must still be present.
	if !strings.Contains(got, `"aws_lakeformation_permissions" "pipeline_runner_db"`) {
		t.Error("own pipeline_runner_db LF grant missing")
	}
}

func TestEmit_UpstreamLakeFormationDashSanitized(t *testing.T) {
	t.Parallel()
	// A dashed catalog must be hyphen→underscore sanitized in both the
	// resource label and the DB name.
	got, err := Emit(basePipelineForLF("clavesa-acme", []string{"bronze"}))
	if err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if !strings.Contains(got, `name = "clavesa_acme__bronze"`) {
		t.Errorf("dashed catalog not sanitized in upstream DB name:\n%s", got)
	}
}
