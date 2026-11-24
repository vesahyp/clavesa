package tfgen

import (
	"strings"
	"testing"
)

func TestEmit_LakeFormationOffByDefault(t *testing.T) {
	t.Parallel()
	got, err := Emit(basePipelineForLF("clavesa_demo", nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "aws_lakeformation_resource") || strings.Contains(got, "DATA_LOCATION_ACCESS") {
		t.Errorf("no access block, yet Lake Formation registration emitted")
	}
	if strings.Contains(got, "CLAVESA_GLUE_SYNC_REQUIRED") {
		t.Errorf("no access block, yet the Glue sync is required")
	}
}

func TestEmit_LakeFormationRegistersWarehouseAndGrantsLocations(t *testing.T) {
	t.Parallel()
	p := basePipelineForLF("clavesa_demo", nil)
	p.LakeFormation = &LakeFormationConfig{
		RoleExpr:           "data.terraform_remote_state.workspace.outputs.lf_register_role_arn",
		SystemLocationExpr: "data.terraform_remote_state.workspace.outputs.lf_system_location_arn",
		TagKeyExpr:         "data.terraform_remote_state.workspace.outputs.lf_tag_key",
	}
	got, err := Emit(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`resource "aws_lakeformation_resource" "warehouse" {`,
		`arn                     = "arn:aws:s3:::${data.terraform_remote_state.workspace.outputs.pipeline_bucket}/${var.pipeline_name}/_warehouse"`,
		`role_arn                = data.terraform_remote_state.workspace.outputs.lf_register_role_arn`,
		`use_service_linked_role = false`,
		`hybrid_access_enabled   = false`,
		`resource "aws_lakeformation_permissions" "pipeline_runner_warehouse_location" {`,
		`resource "aws_lakeformation_permissions" "pipeline_runner_system_location" {`,
		`resource "aws_lakeformation_permissions" "runs_writer_system_location" {`,
		`arn = aws_lakeformation_resource.warehouse.arn`,
		`arn = data.terraform_remote_state.workspace.outputs.lf_system_location_arn`,
		// withGrantLifecycles handled the new grants.
		`resource "terraform_data" "runs_writer_system_location_permissions" {`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q", want)
		}
	}
	// The warehouse database location sits under the registered prefix.
	if !strings.Contains(got, `/${var.pipeline_name}/_warehouse/clavesa_demo__`) {
		t.Errorf("database location_uri is no longer under <pipeline>/_warehouse/")
	}
	// Column tags: the pipeline database is "unreviewed".
	for _, want := range []string{
		`resource "aws_lakeformation_resource_lf_tags" "pipeline_db" {`,
		`key   = data.terraform_remote_state.workspace.outputs.lf_tag_key`,
		`value = "unreviewed"`,
		// The Glue schema sync is required (the column tags need it).
		`CLAVESA_GLUE_SYNC_REQUIRED  = "1"`,
		`CLAVESA_COLUMN_TAGS         = "1"`,
		`CLAVESA_COLUMN_TAGS      = "1"`, // the runs writer
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q", want)
		}
	}
	// The deployer (local compute) gets the runner's write rights.
	for _, want := range []string{
		`resource "aws_lakeformation_permissions" "owner_write_db" {`,
		`permissions = ["INSERT", "DELETE", "ALTER", "DROP"]`,
		`name = "clavesa_demo_system__pipelines"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q", want)
		}
	}
	if n := strings.Count(got, `permissions = ["DATA_LOCATION_ACCESS"]`); n != 3 {
		t.Errorf("%d DATA_LOCATION_ACCESS grants, want 3", n)
	}
}
