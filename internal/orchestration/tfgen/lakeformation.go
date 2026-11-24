package tfgen

import (
	"fmt"
	"strings"
)

// LakeFormationConfig turns on Lake Formation for the pipeline's data
// (docs/ideas/column-tags.md, slice 1a): a workspace whose clavesa.json
// has an "access" block. Nil leaves the pipeline as before.
type LakeFormationConfig struct {
	// RoleExpr is the registration role ARN, an HCL expression
	// (data.terraform_remote_state.workspace.outputs.lf_register_role_arn).
	RoleExpr string
	// SystemLocationExpr is the registered _system/ location, an HCL
	// expression (…outputs.lf_system_location_arn).
	SystemLocationExpr string
	// TagKeyExpr is the workspace's tag key, an HCL expression
	// (…outputs.lf_tag_key). Empty: the database is not tagged.
	TagKeyExpr string
}

// emitLakeFormation registers the pipeline's <pipeline>/_warehouse/ prefix
// with the workspace's registration role and gives the principals that
// create tables there DATA_LOCATION_ACCESS: the runner on the warehouse
// and on the system catalog's _system/, the runs writer on _system/. The
// database and table grants they need already exist (emitGlueCatalogDB,
// emitRunsWriter). The deploy then empties each database's default
// permissions and revokes IAM_ALLOWED_PRINCIPALS (service.LakeFormationLock).
func emitLakeFormation(b *strings.Builder, p Pipeline) {
	lf := p.LakeFormation
	if lf == nil {
		return
	}
	fmt.Fprintf(b, "# Lake Formation (docs/ideas/column-tags.md): this pipeline's tables are\n")
	fmt.Fprintf(b, "# read through Lake Formation. Its warehouse prefix is registered with the\n")
	fmt.Fprintf(b, "# workspace's registration role (lakeformation.tf in the workspace root).\n")
	fmt.Fprintf(b, "resource \"aws_lakeformation_resource\" \"warehouse\" {\n")
	fmt.Fprintf(b, "  arn                     = \"arn:aws:s3:::${%s}/${%s}/_warehouse\"\n", p.BucketExpr, p.PipelineNameExpr)
	fmt.Fprintf(b, "  role_arn                = %s\n", lf.RoleExpr)
	fmt.Fprintf(b, "  use_service_linked_role = false\n")
	fmt.Fprintf(b, "  hybrid_access_enabled   = false\n")
	fmt.Fprintf(b, "}\n\n")

	if lf.TagKeyExpr != "" {
		// Column tags: the database is "unreviewed", so a table and a
		// column no declaration tags inherit it and no audience reads them.
		fmt.Fprintf(b, "resource \"aws_lakeformation_resource_lf_tags\" \"pipeline_db\" {\n")
		fmt.Fprintf(b, "  database {\n")
		fmt.Fprintf(b, "    name = aws_glue_catalog_database.pipeline.name\n")
		fmt.Fprintf(b, "  }\n")
		fmt.Fprintf(b, "  lf_tag {\n")
		fmt.Fprintf(b, "    key   = %s\n", lf.TagKeyExpr)
		fmt.Fprintf(b, "    value = \"unreviewed\"\n")
		fmt.Fprintf(b, "  }\n")
		fmt.Fprintf(b, "}\n\n")
	}

	grant := func(name, principal, location string) {
		fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" %q {\n", name)
		fmt.Fprintf(b, "  principal   = %s\n", principal)
		fmt.Fprintf(b, "  permissions = [\"DATA_LOCATION_ACCESS\"]\n")
		fmt.Fprintf(b, "  data_location {\n")
		fmt.Fprintf(b, "    arn = %s\n", location)
		fmt.Fprintf(b, "  }\n")
		fmt.Fprintf(b, "}\n\n")
	}
	grant("pipeline_runner_warehouse_location", "aws_iam_role.pipeline_runner.arn", "aws_lakeformation_resource.warehouse.arn")
	grant("pipeline_runner_system_location", "aws_iam_role.pipeline_runner.arn", lf.SystemLocationExpr)
	grant("runs_writer_system_location", "aws_iam_role.runs_writer.arn", lf.SystemLocationExpr)

	// The deployer runs the runner on the Mac (`--compute local`, local
	// backfills) as itself. A data lake admin has no ALTER or INSERT on a
	// table it did not create; IAM_ALLOWED_PRINCIPALS covered that until
	// the lock revoked it. These grants add only what the owner grants of
	// emitGlueCatalogDB and emitRunsWriter lack (they give DESCRIBE and
	// SELECT), so neither replaces the other.
	ownerGrant := func(name, perms, block string) {
		fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" %q {\n", name)
		fmt.Fprintf(b, "  principal   = data.aws_caller_identity.clavesa_owner.arn\n")
		fmt.Fprintf(b, "  permissions = %s\n", perms)
		fmt.Fprintf(b, "%s", block)
		fmt.Fprintf(b, "}\n\n")
	}
	db := "aws_glue_catalog_database.pipeline.name"
	sys := fmt.Sprintf("%q", safeCatalogLiteral(p.SystemCatalog)+"__pipelines")
	ownerGrant("owner_write_db", `["CREATE_TABLE", "ALTER", "DROP"]`, fmt.Sprintf("  database {\n    name = %s\n  }\n", db))
	ownerGrant("owner_write_tables", `["INSERT", "DELETE", "ALTER", "DROP"]`, fmt.Sprintf("  table {\n    database_name = %s\n    wildcard      = true\n  }\n", db))
	ownerGrant("owner_write_system_db", `["CREATE_TABLE", "ALTER"]`, fmt.Sprintf("  database {\n    name = %s\n  }\n", sys))
	ownerGrant("owner_write_system_tables", `["INSERT", "ALTER"]`, fmt.Sprintf("  table {\n    database_name = %s\n    wildcard      = true\n  }\n", sys))
}
