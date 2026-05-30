// Package tfgen emits the full orchestration.tf body for a clavesa pipeline.
//
// v2.2.0: replaces the v2.1.x "multi-state ASL — one Task per transform,
// each invoking its own Lambda" with a single per-pipeline Lambda invoked
// by a single SFN Task. Phase A of v2.2.0 already landed the local mirror
// (`clavesa pipeline run` runs one container that loops every transform
// in one Spark session via `runner.pipeline_handler`); ADR-014 local-cloud
// parity demands cloud Lambda mirrors the same shape. The runner's
// `_SPARK` singleton amortises JVM + Glue catalog init across transforms
// inside one invocation, and the SFN graph collapses to a single Task
// that hands the full ordered transform list to the runner.
//
// Why we still bother with Step Functions for a single Task: the
// EventBridge → runs_writer wiring keyed off SFN execution status change
// remains the source of truth for the runs Delta table, and the
// scheduled/upstream/SQS triggers all start SFN executions. Keeping the
// state machine preserves that observability spine; only the ASL shape
// shrinks.
package tfgen

import (
	"fmt"
	"strings"
)

// Pipeline carries everything tfgen needs to emit orchestration.tf.
// Expressions are bare HCL (no quoting); literal-string fields are
// quoted by the emitter.
type Pipeline struct {
	// PipelineNameExpr is the HCL expression for the pipeline-name value
	// used in resource names and tags. Conventionally "var.pipeline_name".
	PipelineNameExpr string

	// Three-level namespace (ADR-016). Catalog and SystemCatalog are
	// literal identifiers; SchemaExpr is an HCL expression (typically
	// "var.schema") because the schema may be overridden per pipeline.
	Catalog       string
	SchemaExpr    string
	SystemCatalog string

	// BucketExpr is the HCL expression for the warehouse bucket name.
	// runs_writer is unconditional today (always emitted) — every pipeline
	// has a bucket. Typically "data.terraform_remote_state.workspace.outputs.pipeline_bucket".
	BucketExpr string

	// RunnerImageExpr is the HCL expression for the workspace's runner
	// image ECR URI (e.g.
	// `data.terraform_remote_state.workspace.outputs.runner_image`).
	// Both the per-pipeline runner Lambda and runs_writer pin to this
	// image at plan time via aws_ecr_image; pushing a new image under
	// the same tag is picked up on the next apply.
	RunnerImageExpr string

	// ScheduleExpr is the HCL expression for an EventBridge schedule
	// (e.g. "var.trigger_schedule"). The emitter wraps in a conditional
	// at plan time — Terraform's count = X != null ? 1 : 0 pattern.
	ScheduleExpr string

	// BatchWindowExpr is the HCL expression for the SQS-poller cadence
	// (typically "var.trigger_batch_window"). Same conditional treatment.
	BatchWindowExpr string

	// TriggerQueueExprs is the list of HCL expressions resolving to SQS
	// queue ARNs (one per source). Empty disables the poller.
	TriggerQueueExprs []string

	// UpstreamPipelines is the list of sibling pipeline names whose
	// state-machine SUCCEEDED events should auto-start this pipeline
	// (ADR-016 §6 cross-pipeline trigger). Each name becomes one
	// EventBridge rule + IAM role + target wired against
	// `arn:aws:states:<region>:<account>:stateMachine:clavesa-<name>`.
	// Empty disables auto-trigger; the cross-pipeline reference in the
	// pipeline's `inputs` is the opt-in (no separate knob).
	UpstreamPipelines []string

	// Transforms is the ordered list of transform invocations the per-
	// pipeline Lambda will iterate through inside one Spark session.
	// Order matters: pipeline_handler executes them sequentially, with
	// downstream skips driven by `Parents`. Replaces the v2.1.x
	// StateMachine + NodeMeta pair.
	Transforms []TransformConfig

	// ExternalBuckets is the list of S3 buckets the pipeline's transforms
	// read from that live OUTSIDE the workspace bucket (kind=s3 sources,
	// cross-account inputs). Empty when every input comes from inside the
	// workspace; non-empty entries are emitted as a sixth IAM Statement
	// (S3ReadExternal) granting s3:GetObject + s3:ListBucket on the
	// listed bucket + bucket/* ARNs. The workspace bucket itself is
	// already covered by the S3Read statement and must NOT appear here
	// (the IAM merge wouldn't widen anything but it'd be confusing in
	// the emitted .tf).
	ExternalBuckets []string

	// UpstreamSchemas is the list of DISTINCT sanitized schema identifiers
	// this pipeline reads cross-pipeline via `external_inputs` (GH #4). Each
	// schema gets one pair of aws_lakeformation_permissions in the consumer's
	// orchestration.tf — a DESCRIBE on the upstream Glue DB and a
	// SELECT+DESCRIBE wildcard on its tables — so the runner role can read
	// the upstream schema's tables on LF-gated accounts. The pipeline's OWN
	// schema is excluded (already covered by the pipeline_runner_db /
	// pipeline_runner_tables grants). ADR-016 puts one catalog per workspace,
	// so the upstream DB lives under the SAME catalog as this pipeline;
	// p.Catalog is used for the DB-name encoding. Empty list emits nothing.
	UpstreamSchemas []string
}

// TransformConfig describes one transform invocation rendered into the
// per-pipeline Lambda's input payload. Inputs/Outputs are pre-rendered
// HCL map literals — the same shape the historical NodeMeta carried —
// because they contain `module.X.outputs[...]` references that must
// resolve at plan time, not at runtime inside the Lambda.
type TransformConfig struct {
	// NodeID is the bare node id (e.g. "enriched"). Mirrors what the
	// runner sets as CLAVESA_NODE per-iteration.
	NodeID string

	// Language is "sql" or "python".
	Language string

	// LogicS3URI is a pre-rendered HCL string expression for the S3 URI
	// of the transform's logic.{sql,py}, e.g.
	// `"s3://${var.bucket}/${var.pipeline_name}/enriched/_runtime/logic.sql"`.
	LogicS3URI string

	// InputsExpr is a pre-rendered HCL map literal — same shape today's
	// NodeMeta.InputsExpr produces, e.g. `{ bronze = "<db>.<table>" }`.
	InputsExpr string

	// OutputsExpr is a pre-rendered HCL map literal — same shape today's
	// NodeMeta.OutputsExpr produces.
	OutputsExpr string

	// Parents lists the intra-pipeline upstream node ids. pipeline_handler
	// uses this to cascade-skip downstream nodes when a parent fails (so
	// a broken bronze short-circuits silver+gold without surprise
	// behaviour from stale upstream data).
	Parents []string
}

// Emit returns the full orchestration.tf body (excluding the `module "src_*"`
// blocks the caller emits separately for kind=s3 sources).
func Emit(p Pipeline) (string, error) {
	if err := p.validate(); err != nil {
		return "", err
	}

	var b strings.Builder

	emitTagsLocal(&b, p)
	emitGlueCatalogDB(&b, p)
	emitLogGroup(&b, p)
	emitIAMRole(&b, p)
	emitPipelineLambda(&b, p)
	emitStateMachine(&b, p)
	emitSchedule(&b, p)
	emitPoller(&b, p)
	emitUpstreamTriggers(&b, p)
	emitRunsWriter(&b, p)

	return b.String(), nil
}

// emitTagsLocal writes the shared `local.clavesa_tags` block referenced by
// every taggable resource below — pipeline name + a fixed "clavesa:type"
// label so a customer's tag-cost report can attribute spend per pipeline.
func emitTagsLocal(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# Shared resource tags — every clavesa-owned AWS resource carries the\n")
	fmt.Fprintf(b, "# pipeline name + a `clavesa:type` label so tag-cost reporting can\n")
	fmt.Fprintf(b, "# attribute spend per pipeline.\n")
	fmt.Fprintf(b, "locals {\n")
	fmt.Fprintf(b, "  clavesa_tags = {\n")
	fmt.Fprintf(b, "    \"clavesa:pipeline\" = %s\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "    \"clavesa:type\"     = \"orchestration\"\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")
}

func (p Pipeline) validate() error {
	if p.PipelineNameExpr == "" {
		return fmt.Errorf("tfgen: PipelineNameExpr is required")
	}
	if p.Catalog == "" || p.SchemaExpr == "" || p.SystemCatalog == "" {
		return fmt.Errorf("tfgen: Catalog, SchemaExpr, SystemCatalog all required (ADR-016 three-level namespace)")
	}
	if p.BucketExpr == "" {
		return fmt.Errorf("tfgen: BucketExpr is required (runs_writer needs a bucket)")
	}
	if p.RunnerImageExpr == "" {
		return fmt.Errorf("tfgen: RunnerImageExpr is required (pipeline Lambda + runs_writer deploy as the runner image)")
	}
	if len(p.Transforms) == 0 {
		return fmt.Errorf("tfgen: at least one transform is required")
	}
	for i, t := range p.Transforms {
		if t.NodeID == "" {
			return fmt.Errorf("tfgen: Transforms[%d].NodeID is required", i)
		}
		if t.Language == "" {
			return fmt.Errorf("tfgen: Transforms[%d].Language is required (transform %q)", i, t.NodeID)
		}
		if t.LogicS3URI == "" {
			return fmt.Errorf("tfgen: Transforms[%d].LogicS3URI is required (transform %q)", i, t.NodeID)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Glue catalog DB — per-pipeline output namespace (ADR-016 encoded name)
// ---------------------------------------------------------------------------

func emitGlueCatalogDB(b *strings.Builder, p Pipeline) {
	// catalog_db encoding mirrors identutil.EncodeGlueDatabase / the
	// runner's _glue_db / the transform module's local.catalog_db. All
	// encoders must stay byte-identical so the runner writes to the DB
	// terraform created. Schema isn't a literal (it's var.schema), so the
	// encoded name is itself an HCL string with an embedded replace().
	fmt.Fprintf(b, "# Glue catalog database — per-pipeline output namespace (ADR-016).\n")
	fmt.Fprintf(b, "# location_uri is required for Spark's Glue Hive Client to resolve\n")
	fmt.Fprintf(b, "# the DB's warehouse path on saveAsTable. Without it Hive trips\n")
	fmt.Fprintf(b, "# `IllegalArgumentException: Can not create a Path from an empty string`.\n")
	fmt.Fprintf(b, "resource \"aws_glue_catalog_database\" \"pipeline\" {\n")
	fmt.Fprintf(b, "  name         = \"%s__${replace(%s, \"-\", \"_\")}\"\n",
		safeCatalogLiteral(p.Catalog), p.SchemaExpr)
	fmt.Fprintf(b, "  description  = \"Clavesa pipeline output tables — ${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  location_uri = \"s3://${%s}/${%s}/_warehouse/%s__${replace(%s, \"-\", \"_\")}.db\"\n",
		p.BucketExpr, p.PipelineNameExpr, safeCatalogLiteral(p.Catalog), p.SchemaExpr)
	fmt.Fprintf(b, "  tags         = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	// Lake Formation grants — required on accounts where Lake Formation
	// governs the Glue Catalog (default on accounts created after
	// Aug 2023). Without these, the runner Lambda's IAM principal has
	// zero LF grants on the DB we just created, so the Hive metastore
	// client gets `AccessDeniedException: Required Describe on …` on
	// the first `saveAsTable` (GH #1).
	//
	// On accounts that pre-date the LF default, these grants pass
	// through to IAM (the DB still carries IAMAllowedPrincipals); LF
	// acts as a transparent layer and the IAM-only path keeps working.
	//
	// Deploying principal must be a Lake Formation DataLakeAdmin —
	// `aws_lakeformation_permissions` requires it. See README "Lake
	// Formation-enabled accounts" callout.
	fmt.Fprintf(b, "# Lake Formation grants — pipeline runner role on this DB and its tables.\n")
	fmt.Fprintf(b, "# Mirrors the IAM grants in aws_iam_role_policy.pipeline_runner so LF\n")
	fmt.Fprintf(b, "# doesn't override IAM on LF-gated accounts (GH #1).\n")
	fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" \"pipeline_runner_db\" {\n")
	fmt.Fprintf(b, "  principal   = aws_iam_role.pipeline_runner.arn\n")
	fmt.Fprintf(b, "  permissions = [\"DESCRIBE\", \"CREATE_TABLE\", \"ALTER\", \"DROP\"]\n")
	fmt.Fprintf(b, "  database {\n")
	fmt.Fprintf(b, "    name = aws_glue_catalog_database.pipeline.name\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" \"pipeline_runner_tables\" {\n")
	fmt.Fprintf(b, "  principal   = aws_iam_role.pipeline_runner.arn\n")
	fmt.Fprintf(b, "  permissions = [\"SELECT\", \"INSERT\", \"DELETE\", \"ALTER\", \"DROP\", \"DESCRIBE\"]\n")
	fmt.Fprintf(b, "  table {\n")
	fmt.Fprintf(b, "    database_name = aws_glue_catalog_database.pipeline.name\n")
	fmt.Fprintf(b, "    wildcard      = true\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	// Lake Formation grants on UPSTREAM schemas this pipeline reads
	// cross-pipeline (GH #4). The own-DB grants above only cover this
	// pipeline's schema; a downstream pipeline reading an upstream schema's
	// tables (silver reading `bronze.cloudfront_raw`) needs DESCRIBE on the
	// upstream Glue DB + SELECT/DESCRIBE on its tables or the runner trips
	// `Insufficient Lake Formation permission(s): Required Describe on
	// <catalog>__<upstream>` at read time. One catalog per workspace
	// (ADR-016), so the upstream DB lives under p.Catalog — the same catalog
	// this pipeline writes to. Cross-pipeline WRITES are forbidden, so these
	// are read-only grants. The schema identifiers arrive deduped + sorted +
	// sanitized + own-schema-excluded from the service layer.
	for _, schema := range p.UpstreamSchemas {
		db := safeCatalogLiteral(p.Catalog) + "__" + safeCatalogLiteral(schema)
		label := safeCatalogLiteral(schema)
		fmt.Fprintf(b, "# Lake Formation read grants — upstream schema %q (cross-pipeline read, GH #4).\n", schema)
		fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" \"input_%s_db\" {\n", label)
		fmt.Fprintf(b, "  principal   = aws_iam_role.pipeline_runner.arn\n")
		fmt.Fprintf(b, "  permissions = [\"DESCRIBE\"]\n")
		fmt.Fprintf(b, "  database {\n")
		fmt.Fprintf(b, "    name = %q\n", db)
		fmt.Fprintf(b, "  }\n")
		fmt.Fprintf(b, "}\n\n")

		fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" \"input_%s_tables\" {\n", label)
		fmt.Fprintf(b, "  principal   = aws_iam_role.pipeline_runner.arn\n")
		fmt.Fprintf(b, "  permissions = [\"SELECT\", \"DESCRIBE\"]\n")
		fmt.Fprintf(b, "  table {\n")
		fmt.Fprintf(b, "    database_name = %q\n", db)
		fmt.Fprintf(b, "    wildcard      = true\n")
		fmt.Fprintf(b, "  }\n")
		fmt.Fprintf(b, "}\n\n")
	}
}

// safeCatalogLiteral applies the same hyphen→underscore replacement the
// runner / identutil / transform module apply, so we can hard-code the
// encoded prefix when the catalog is a literal.
func safeCatalogLiteral(catalog string) string {
	return strings.ReplaceAll(catalog, "-", "_")
}

// ---------------------------------------------------------------------------
// CloudWatch log group — SFN execution logging
// ---------------------------------------------------------------------------

func emitLogGroup(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# CloudWatch log group — Step Functions execution logging (90-day retention).\n")
	fmt.Fprintf(b, "resource \"aws_cloudwatch_log_group\" \"sfn_logs\" {\n")
	fmt.Fprintf(b, "  name              = \"/clavesa/${%s}/sfn\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  retention_in_days = 90\n")
	fmt.Fprintf(b, "  tags              = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// IAM execution role for the Step Functions state machine
//
// v2.2.0: the state machine only invokes one Lambda (the per-pipeline
// runner), so the Lambda invoke statement is scoped to that ARN instead
// of "*".
// ---------------------------------------------------------------------------

func emitIAMRole(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# IAM execution role for the Step Functions state machine.\n")
	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"sfn_assume\" {\n")
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    sid     = \"StepFunctionsTrust\"\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"states.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"sfn_exec\" {\n")
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-orchestration\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"sfn_exec_policy\" {\n")
	fmt.Fprintf(b, "  name = \"clavesa-${%s}-orchestration\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role = aws_iam_role.sfn_exec.id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [\n")
	// v2.2.0: only the per-pipeline runner Lambda is invoked from this SFN.
	fmt.Fprintf(b, "      { Sid = \"LambdaInvoke\", Effect = \"Allow\", Action = [\"lambda:InvokeFunction\"], Resource = [aws_lambda_function.pipeline_runner.arn] },\n")
	fmt.Fprintf(b, "      { Sid = \"CloudWatchLogsDelivery\", Effect = \"Allow\", Action = [\n")
	fmt.Fprintf(b, "          \"logs:CreateLogDelivery\", \"logs:GetLogDelivery\", \"logs:UpdateLogDelivery\",\n")
	fmt.Fprintf(b, "          \"logs:DeleteLogDelivery\", \"logs:ListLogDeliveries\", \"logs:PutResourcePolicy\",\n")
	fmt.Fprintf(b, "          \"logs:DescribeResourcePolicies\", \"logs:DescribeLogGroups\",\n")
	fmt.Fprintf(b, "      ], Resource = \"*\" },\n")
	fmt.Fprintf(b, "    ]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// Per-pipeline runner Lambda (v2.2.0)
//
// One image-based Lambda per pipeline, hosting runner.pipeline_handler.
// Receives the full ordered transform list in its event payload and runs
// every transform inside one Spark session (the runner's `_SPARK`
// singleton). Replaces the v2.1.x "one Lambda per transform" shape.
//
// IAM is the union of what each transform's module previously asked for
// (modules/transform/aws/main.tf:84-238), aggregated to pipeline scope —
// every input bucket the workspace can see, this pipeline's warehouse
// + watermarks + system warehouse prefixes for writes, the workspace
// catalog DBs for reads, this pipeline's catalog DB + system pipelines DB
// for writes.
// ---------------------------------------------------------------------------

// moduleVersionLiteral is baked into the pipeline Lambda env as
// CLAVESA_MODULE_VERSION. The orchestration emitter doesn't have access
// to internal/service/version.go (cyclic import), and pushing it through
// Pipeline as a field has no other consumer today — hardcoded for
// v2.2.2; bump alongside ModuleVersion in version.go. (v2.2.1 missed
// this bump; threading the value through Pipeline would prevent the
// next miss — filed in TODO.md.)
const moduleVersionLiteral = "v2.2.2"

func emitPipelineLambda(b *strings.Builder, p Pipeline) {
	safeCatalog := safeCatalogLiteral(p.Catalog)
	sysCatalogSafe := safeCatalogLiteral(p.SystemCatalog)

	fmt.Fprintf(b, "# Per-pipeline runner Lambda — image-based, hosts runner.pipeline_handler.\n")
	fmt.Fprintf(b, "# v2.2.0: one Lambda per pipeline runs every transform sequentially in\n")
	fmt.Fprintf(b, "# one Spark session via the runner's `_SPARK` singleton, mirroring the\n")
	fmt.Fprintf(b, "# local bundle execution Phase A landed (ADR-014 local-cloud parity).\n")

	// Pin to the runner image digest at plan time — same content-
	// addressed pattern emitRunsWriter and modules/transform/aws use.
	fmt.Fprintf(b, "locals {\n")
	fmt.Fprintf(b, "  pipeline_runner_image_match = regex(\"^([^:]+):(.+)$\", %s)\n", p.RunnerImageExpr)
	fmt.Fprintf(b, "  pipeline_runner_repo_uri    = local.pipeline_runner_image_match[0]\n")
	fmt.Fprintf(b, "  pipeline_runner_tag         = local.pipeline_runner_image_match[1]\n")
	fmt.Fprintf(b, "  pipeline_runner_repo_name   = regex(\"^[^/]+/(.+)$\", local.pipeline_runner_repo_uri)[0]\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_ecr_image\" \"pipeline_runner\" {\n")
	fmt.Fprintf(b, "  repository_name = local.pipeline_runner_repo_name\n")
	fmt.Fprintf(b, "  image_tag       = local.pipeline_runner_tag\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"pipeline_runner_assume\" {\n")
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"lambda.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"pipeline_runner\" {\n")
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-runner\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.pipeline_runner_assume.json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	// Aggregated IAM. Mirrors modules/transform/aws/main.tf:84-238 but
	// scoped to the whole pipeline. Reads are intentionally broad
	// (whole workspace bucket — ADR-016 cross-pipeline reads + every
	// transform's input bucket); writes stay scoped to this pipeline's
	// own warehouse / watermarks / system pipelines DB.
	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"pipeline_runner\" {\n")
	fmt.Fprintf(b, "  name = \"clavesa-${%s}-runner\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role = aws_iam_role.pipeline_runner.id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [\n")

	// S3 read — workspace bucket + everything inside. Logic uploads,
	// every transform's inputs (including same-account s3 sources), and
	// cross-pipeline warehouse reads all land in this bucket.
	fmt.Fprintf(b, "      { Sid = \"S3Read\", Effect = \"Allow\", Action = [\"s3:GetObject\", \"s3:ListBucket\", \"s3:GetBucketLocation\"], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}\",\n", p.BucketExpr)
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}/*\",\n", p.BucketExpr)
	fmt.Fprintf(b, "      ]},\n")

	// S3 read — external buckets (kind=s3 sources, cross-account inputs).
	// Emitted only when ExternalBuckets is non-empty; v2.1.x summed every
	// transform's input_buckets into per-transform IAM, so collapsing to
	// the per-pipeline Lambda must preserve that grant or pipelines that
	// read external S3 start 403'ing on upgrade.
	if len(p.ExternalBuckets) > 0 {
		fmt.Fprintf(b, "      { Sid = \"S3ReadExternal\", Effect = \"Allow\", Action = [\"s3:GetObject\", \"s3:ListBucket\", \"s3:GetBucketLocation\"], Resource = [\n")
		for _, bucket := range p.ExternalBuckets {
			fmt.Fprintf(b, "          \"arn:aws:s3:::%s\",\n", bucket)
			fmt.Fprintf(b, "          \"arn:aws:s3:::%s/*\",\n", bucket)
		}
		fmt.Fprintf(b, "      ]},\n")
	}

	// S3 write — this pipeline's prefixes only.
	fmt.Fprintf(b, "      { Sid = \"S3Write\", Effect = \"Allow\", Action = [\n")
	fmt.Fprintf(b, "          \"s3:PutObject\", \"s3:DeleteObject\", \"s3:AbortMultipartUpload\", \"s3:ListMultipartUploadParts\",\n")
	fmt.Fprintf(b, "      ], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}/${%s}/_warehouse/*\",\n", p.BucketExpr, p.PipelineNameExpr)
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}/${%s}/_watermarks/*\",\n", p.BucketExpr, p.PipelineNameExpr)
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}/${%s}/*/*\",\n", p.BucketExpr, p.PipelineNameExpr)
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}/_system/pipelines/*\",\n", p.BucketExpr)
	fmt.Fprintf(b, "      ]},\n")

	// Glue read — workspace catalog + system catalog + `default` DB
	// (Hive metastore probes it during session init).
	fmt.Fprintf(b, "      { Sid = \"GlueCatalogRead\", Effect = \"Allow\", Action = [\n")
	fmt.Fprintf(b, "          \"glue:GetDatabase\", \"glue:GetDatabases\", \"glue:GetTable\", \"glue:GetTables\",\n")
	fmt.Fprintf(b, "          \"glue:GetPartition\", \"glue:GetPartitions\",\n")
	fmt.Fprintf(b, "      ], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:catalog\",\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/default\",\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s__*\",\n", safeCatalog)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:table/%s__*/*\",\n", safeCatalog)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s__*\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:table/%s__*/*\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "      ]},\n")

	// Glue write — this pipeline's user-schema DB plus the system
	// pipelines DB (node_runs / runs / tables append from every pipeline).
	fmt.Fprintf(b, "      { Sid = \"GlueCatalogWrite\", Effect = \"Allow\", Action = [\n")
	fmt.Fprintf(b, "          \"glue:CreateTable\", \"glue:UpdateTable\", \"glue:DeleteTable\",\n")
	fmt.Fprintf(b, "          \"glue:CreatePartition\", \"glue:UpdatePartition\", \"glue:DeletePartition\",\n")
	fmt.Fprintf(b, "          \"glue:BatchCreatePartition\", \"glue:BatchDeletePartition\",\n")
	fmt.Fprintf(b, "      ], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:catalog\",\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s__${replace(%s, \"-\", \"_\")}\",\n", safeCatalog, p.SchemaExpr)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:table/%s__${replace(%s, \"-\", \"_\")}/*\",\n", safeCatalog, p.SchemaExpr)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s__pipelines\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:table/%s__pipelines/*\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "      ]},\n")

	// First-run database creation — Hive metastore's `CREATE DATABASE
	// IF NOT EXISTS` path on first write goes through glue:CreateDatabase.
	fmt.Fprintf(b, "      { Sid = \"GlueDatabaseCreate\", Effect = \"Allow\", Action = [\"glue:CreateDatabase\"], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:catalog\",\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s__${replace(%s, \"-\", \"_\")}\",\n", safeCatalog, p.SchemaExpr)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s__pipelines\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "      ]},\n")

	fmt.Fprintf(b, "      { Sid = \"Logs\", Effect = \"Allow\", Action = [\"logs:CreateLogGroup\", \"logs:CreateLogStream\", \"logs:PutLogEvents\"], Resource = [\"arn:aws:logs:*:*:*\"] },\n")
	fmt.Fprintf(b, "    ]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	// Lambda. Mirrors emitRunsWriter's image-based shape; differs in
	// handler (pipeline_handler vs runs_writer_handler), timeout (15min
	// vs 2min), memory (3008MB vs 1.5GB — new AWS accounts cap per-function
	// memory at 3008MB; users with a raised quota can bump this in the
	// generated .tf), and env (per-pipeline knobs).
	fmt.Fprintf(b, "resource \"aws_lambda_function\" \"pipeline_runner\" {\n")
	fmt.Fprintf(b, "  function_name = \"clavesa-${%s}-runner\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role          = aws_iam_role.pipeline_runner.arn\n")
	fmt.Fprintf(b, "  package_type  = \"Image\"\n")
	fmt.Fprintf(b, "  image_uri     = \"${local.pipeline_runner_repo_uri}@${data.aws_ecr_image.pipeline_runner.image_digest}\"\n")
	fmt.Fprintf(b, "  timeout       = 900   # 15min — the Lambda max; one container handles every transform\n")
	fmt.Fprintf(b, "  memory_size   = 3008  # New AWS accounts default to a 3008MB per-function quota; bump via Service Quotas + raise this if you need more headroom for Spark broadcast tables\n\n")
	fmt.Fprintf(b, "  image_config {\n")
	fmt.Fprintf(b, "    command = [\"runner.pipeline_handler\"]\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  environment {\n")
	fmt.Fprintf(b, "    variables = {\n")
	// pipeline_handler sets per-transform CLAVESA_NODE / CLAVESA_LANGUAGE
	// / CLAVESA_LOGIC_S3_PATH from the event payload before each iteration;
	// only workspace-level invariants live here.
	fmt.Fprintf(b, "      CLAVESA_PIPELINE            = %s\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "      CLAVESA_CATALOG             = \"%s\"\n", safeCatalog)
	fmt.Fprintf(b, "      CLAVESA_SCHEMA              = %s\n", p.SchemaExpr)
	fmt.Fprintf(b, "      CLAVESA_SYSTEM_CATALOG      = \"%s\"\n", sysCatalogSafe)
	fmt.Fprintf(b, "      CLAVESA_WAREHOUSE           = \"s3://${%s}/${%s}/_warehouse/\"\n", p.BucketExpr, p.PipelineNameExpr)
	fmt.Fprintf(b, "      CLAVESA_SYSTEM_WAREHOUSE    = \"s3://${%s}/_system/pipelines/\"\n", p.BucketExpr)
	fmt.Fprintf(b, "      CLAVESA_WATERMARKS          = \"s3://${%s}/${%s}/_watermarks/\"\n", p.BucketExpr, p.PipelineNameExpr)
	fmt.Fprintf(b, "      CLAVESA_RUNNER_IMAGE_DIGEST = data.aws_ecr_image.pipeline_runner.image_digest\n")
	fmt.Fprintf(b, "      CLAVESA_MODULE_VERSION      = \"%s\"\n", moduleVersionLiteral)
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// Step Functions state machine — single Task ASL.
//
// v2.2.0: collapses to one Task that invokes the per-pipeline runner
// Lambda with the full ordered transform list. No per-state Retry/Catch
// + no PipelineFailed terminal state — runs_writer's EventBridge rule
// already captures terminal SFN execution status changes and writes
// FAILED/TIMED_OUT/ABORTED rows to the runs table. A Lambda-side error
// surfaces as Task failure → SFN execution FAILED → runs_writer row.
// ---------------------------------------------------------------------------

func emitStateMachine(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# Step Functions state machine — single Task that hands the full\n")
	fmt.Fprintf(b, "# ordered transform list to the per-pipeline runner Lambda.\n")
	fmt.Fprintf(b, "# v2.2.0: was multi-state (one Task per transform); collapsed because\n")
	fmt.Fprintf(b, "# pipeline_handler now loops transforms inside one Spark session.\n")
	fmt.Fprintf(b, "resource \"aws_sfn_state_machine\" \"pipeline\" {\n")
	fmt.Fprintf(b, "  name     = \"clavesa-${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role_arn = aws_iam_role.sfn_exec.arn\n")
	fmt.Fprintf(b, "  type     = \"STANDARD\"\n\n")
	fmt.Fprintf(b, "  definition = jsonencode({\n")
	fmt.Fprintf(b, "    Comment = \"Clavesa pipeline: ${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "    StartAt = \"RunPipeline\"\n")
	fmt.Fprintf(b, "    States = {\n")
	fmt.Fprintf(b, "      RunPipeline = {\n")
	fmt.Fprintf(b, "        Type           = \"Task\"\n")
	fmt.Fprintf(b, "        Resource       = \"arn:aws:states:::lambda:invoke\"\n")
	fmt.Fprintf(b, "        TimeoutSeconds = 900\n")
	fmt.Fprintf(b, "        Parameters = {\n")
	fmt.Fprintf(b, "          FunctionName = aws_lambda_function.pipeline_runner.arn\n")
	fmt.Fprintf(b, "          Payload = {\n")
	fmt.Fprintf(b, "            _pipeline_run = true\n")
	fmt.Fprintf(b, "            pipeline      = %s\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "            transforms = [\n")
	for _, t := range p.Transforms {
		fmt.Fprintf(b, "              {\n")
		fmt.Fprintf(b, "                node       = %q\n", t.NodeID)
		fmt.Fprintf(b, "                language   = %q\n", t.Language)
		fmt.Fprintf(b, "                logic_path = %s\n", t.LogicS3URI)
		fmt.Fprintf(b, "                inputs     = %s\n", t.InputsExpr)
		fmt.Fprintf(b, "                outputs    = %s\n", t.OutputsExpr)
		fmt.Fprintf(b, "                parents    = %s\n", renderParents(t.Parents))
		fmt.Fprintf(b, "              },\n")
	}
	fmt.Fprintf(b, "            ]\n")
	// SFN context-object substitutions — runner attributes node_runs
	// rows to the parent execution. Same three fields the multi-state
	// ASL used; pipeline_handler propagates them through every node.
	fmt.Fprintf(b, "            \"_sf_execution_arn.$\"        = \"$$.Execution.Id\"\n")
	fmt.Fprintf(b, "            \"_sf_execution_started_at.$\" = \"$$.Execution.StartTime\"\n")
	fmt.Fprintf(b, "            \"_execution_input.$\"         = \"$$.Execution.Input\"\n")
	fmt.Fprintf(b, "          }\n")
	fmt.Fprintf(b, "        }\n")
	fmt.Fprintf(b, "        End = true\n")
	fmt.Fprintf(b, "      }\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  })\n\n")
	fmt.Fprintf(b, "  logging_configuration {\n")
	fmt.Fprintf(b, "    log_destination        = \"${aws_cloudwatch_log_group.sfn_logs.arn}:*\"\n")
	fmt.Fprintf(b, "    include_execution_data = true\n")
	fmt.Fprintf(b, "    level                  = \"ERROR\"\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  tags       = local.clavesa_tags\n")
	fmt.Fprintf(b, "  depends_on = [aws_iam_role_policy.sfn_exec_policy]\n")
	fmt.Fprintf(b, "}\n\n")
}

// renderParents renders a []string as an HCL list literal — `[]` when
// empty (must be explicit; jsonencode rejects a Go nil slice mid-tree
// silently turning into null on the ASL side).
func renderParents(parents []string) string {
	if len(parents) == 0 {
		return "[]"
	}
	quoted := make([]string, len(parents))
	for i, p := range parents {
		quoted[i] = fmt.Sprintf("%q", p)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// ---------------------------------------------------------------------------
// Schedule trigger (optional — gated on the schedule expression at plan time)
// ---------------------------------------------------------------------------

func emitSchedule(b *strings.Builder, p Pipeline) {
	if p.ScheduleExpr == "" {
		return
	}
	fmt.Fprintf(b, "# Schedule trigger — EventBridge rule + IAM role to start the state\n")
	fmt.Fprintf(b, "# machine on a cron / rate cadence. Created only when var.trigger_schedule\n")
	fmt.Fprintf(b, "# is non-null (count pattern).\n")
	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"schedule\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  name                = \"clavesa-${%s}-schedule\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  description         = \"Scheduled trigger for Clavesa pipeline: ${%s}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  schedule_expression = %s\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  tags                = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"events_assume\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    sid     = \"EventBridgeTrust\"\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"events.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"events_trigger\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-trigger\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.events_assume[0].json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"events_trigger_policy\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  name = \"clavesa-${%s}-trigger\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role = aws_iam_role.events_trigger[0].id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version   = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [{ Sid = \"StartExecution\", Effect = \"Allow\", Action = [\"states:StartExecution\"], Resource = [aws_sfn_state_machine.pipeline.arn] }]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"schedule\" {\n")
	fmt.Fprintf(b, "  count = %s != null ? 1 : 0\n\n", p.ScheduleExpr)
	fmt.Fprintf(b, "  rule     = aws_cloudwatch_event_rule.schedule[0].name\n")
	fmt.Fprintf(b, "  arn      = aws_sfn_state_machine.pipeline.arn\n")
	fmt.Fprintf(b, "  role_arn = aws_iam_role.events_trigger[0].arn\n\n")
	fmt.Fprintf(b, "  # _trigger gets read by runs_writer (see runner/runner.py:_RUNS_TRIGGER_VALUES)\n")
	fmt.Fprintf(b, "  # to label the row in `<system_catalog>__pipelines.runs.trigger`.\n")
	fmt.Fprintf(b, "  input = jsonencode({\n")
	fmt.Fprintf(b, "    pipeline = %s\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "    _trigger = \"scheduled\"\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// SQS poller (optional — gated on TriggerQueueExprs + BatchWindowExpr)
// ---------------------------------------------------------------------------

func emitPoller(b *strings.Builder, p Pipeline) {
	if len(p.TriggerQueueExprs) == 0 || p.BatchWindowExpr == "" {
		return
	}
	queueListExpr := "[" + strings.Join(p.TriggerQueueExprs, ", ") + "]"

	fmt.Fprintf(b, "# SQS poller — Lambda that checks source queues on a schedule and starts\n")
	fmt.Fprintf(b, "# the state machine when any queue has messages.\n")
	fmt.Fprintf(b, "locals {\n")
	fmt.Fprintf(b, "  _poller_queue_arns = %s\n", queueListExpr)
	fmt.Fprintf(b, "  _poller_enabled    = length(local._poller_queue_arns) > 0 && %s != null\n", p.BatchWindowExpr)
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"archive_file\" \"poller\" {\n")
	fmt.Fprintf(b, "  count       = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  type        = \"zip\"\n")
	fmt.Fprintf(b, "  source_file = \"${path.module}/%s/poller.py\"\n", sidecarDirName)
	fmt.Fprintf(b, "  output_path = \"${path.module}/%s/poller.zip\"\n", sidecarDirName)
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"poller_assume\" {\n")
	fmt.Fprintf(b, "  count = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"lambda.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"poller\" {\n")
	fmt.Fprintf(b, "  count              = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-poller\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.poller_assume[0].json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"poller\" {\n")
	fmt.Fprintf(b, "  count = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  name  = \"clavesa-${%s}-poller\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role  = aws_iam_role.poller[0].id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [\n")
	fmt.Fprintf(b, "      { Sid = \"SQSPoll\",  Effect = \"Allow\", Action = [\"sqs:ReceiveMessage\", \"sqs:PurgeQueue\"], Resource = local._poller_queue_arns },\n")
	fmt.Fprintf(b, "      { Sid = \"SFNStart\", Effect = \"Allow\", Action = [\"states:StartExecution\"], Resource = [aws_sfn_state_machine.pipeline.arn] },\n")
	fmt.Fprintf(b, "      { Sid = \"Logs\",     Effect = \"Allow\", Action = [\"logs:CreateLogGroup\", \"logs:CreateLogStream\", \"logs:PutLogEvents\"], Resource = [\"arn:aws:logs:*:*:*\"] },\n")
	fmt.Fprintf(b, "    ]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lambda_function\" \"poller\" {\n")
	fmt.Fprintf(b, "  count            = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  function_name    = \"clavesa-${%s}-poller\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role             = aws_iam_role.poller[0].arn\n")
	fmt.Fprintf(b, "  runtime          = \"python3.12\"\n")
	fmt.Fprintf(b, "  handler          = \"poller.handler\"\n")
	fmt.Fprintf(b, "  filename         = data.archive_file.poller[0].output_path\n")
	fmt.Fprintf(b, "  source_code_hash = data.archive_file.poller[0].output_base64sha256\n")
	fmt.Fprintf(b, "  timeout          = 30\n\n")
	fmt.Fprintf(b, "  environment {\n")
	fmt.Fprintf(b, "    variables = {\n")
	fmt.Fprintf(b, "      QUEUE_ARNS        = jsonencode(local._poller_queue_arns)\n")
	fmt.Fprintf(b, "      STATE_MACHINE_ARN = aws_sfn_state_machine.pipeline.arn\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"poller\" {\n")
	fmt.Fprintf(b, "  count               = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  name                = \"clavesa-${%s}-poller\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  description         = \"Polls source queues for ${%s} at ${%s}\"\n", p.PipelineNameExpr, p.BatchWindowExpr)
	fmt.Fprintf(b, "  schedule_expression = %s\n", p.BatchWindowExpr)
	fmt.Fprintf(b, "  tags                = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"poller\" {\n")
	fmt.Fprintf(b, "  count = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  rule  = aws_cloudwatch_event_rule.poller[0].name\n")
	fmt.Fprintf(b, "  arn   = aws_lambda_function.poller[0].arn\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lambda_permission\" \"poller\" {\n")
	fmt.Fprintf(b, "  count         = local._poller_enabled ? 1 : 0\n")
	fmt.Fprintf(b, "  statement_id  = \"AllowEventBridgeInvoke\"\n")
	fmt.Fprintf(b, "  action        = \"lambda:InvokeFunction\"\n")
	fmt.Fprintf(b, "  function_name = aws_lambda_function.poller[0].function_name\n")
	fmt.Fprintf(b, "  principal     = \"events.amazonaws.com\"\n")
	fmt.Fprintf(b, "  source_arn    = aws_cloudwatch_event_rule.poller[0].arn\n")
	fmt.Fprintf(b, "}\n\n")
}

// ---------------------------------------------------------------------------
// Cross-pipeline auto-trigger (ADR-016 §6) — one EventBridge rule per
// upstream producer pipeline. Fires this pipeline's state machine when
// the producer's Step Functions execution succeeds.
//
// ARN construction: state-machine names follow the
// `clavesa-<pipeline_name>` convention emitted by emitStateMachine, so
// the producer's ARN is well-known at plan time without a cross-
// pipeline remote-state lookup. EventBridge matches the literal string,
// so producer-creation order doesn't matter — the rule sits inert until
// the producer's state machine exists, then starts firing.
// ---------------------------------------------------------------------------

func emitUpstreamTriggers(b *strings.Builder, p Pipeline) {
	if len(p.UpstreamPipelines) == 0 {
		return
	}

	// data sources for account + region — built once even with multiple
	// producers. `data.aws_caller_identity.current` and
	// `data.aws_region.current` already exist at workspace level in the
	// generated workspace main.tf, but emitting them again here is a
	// no-op (Terraform deduplicates by address); cheaper than threading
	// the workspace declarations through.
	fmt.Fprintf(b, "# Cross-pipeline auto-trigger — one EventBridge rule per upstream\n")
	fmt.Fprintf(b, "# producer pipeline (derived from `external_inputs` references at\n")
	fmt.Fprintf(b, "# sync time). Each rule starts this pipeline's state machine when\n")
	fmt.Fprintf(b, "# the producer's Step Functions execution reaches SUCCEEDED.\n")
	fmt.Fprintf(b, "data \"aws_caller_identity\" \"clavesa_upstream\" {}\n")
	fmt.Fprintf(b, "data \"aws_region\" \"clavesa_upstream\" {}\n\n")

	for _, producer := range p.UpstreamPipelines {
		// Terraform resource addresses require [A-Za-z_][A-Za-z0-9_]*;
		// the state-machine name itself can carry the dash (SFN allows
		// hyphens). Mirrors safeCatalogLiteral's hyphen→underscore fold.
		safe := strings.ReplaceAll(producer, "-", "_")
		arnExpr := fmt.Sprintf(
			"arn:aws:states:${data.aws_region.clavesa_upstream.region}:${data.aws_caller_identity.clavesa_upstream.account_id}:stateMachine:clavesa-%s",
			producer,
		)

		// EventBridge rule on the producer's state machine.
		fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"upstream_%s\" {\n", safe)
		fmt.Fprintf(b, "  name        = \"clavesa-${%s}-from-%s\"\n", p.PipelineNameExpr, producer)
		fmt.Fprintf(b, "  description = \"Auto-start ${%s} when upstream pipeline %s succeeds\"\n", p.PipelineNameExpr, producer)
		fmt.Fprintf(b, "  event_pattern = jsonencode({\n")
		fmt.Fprintf(b, "    source        = [\"aws.states\"]\n")
		fmt.Fprintf(b, "    \"detail-type\" = [\"Step Functions Execution Status Change\"]\n")
		fmt.Fprintf(b, "    detail = {\n")
		fmt.Fprintf(b, "      stateMachineArn = [\"%s\"]\n", arnExpr)
		fmt.Fprintf(b, "      status          = [\"SUCCEEDED\"]\n")
		fmt.Fprintf(b, "    }\n")
		fmt.Fprintf(b, "  })\n")
		fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
		fmt.Fprintf(b, "}\n\n")

		// IAM role for EventBridge → SFN StartExecution. Role per
		// producer keeps the resource set self-contained and avoids
		// permission drift across multiple targets sharing one role.
		fmt.Fprintf(b, "resource \"aws_iam_role\" \"upstream_trigger_%s\" {\n", safe)
		fmt.Fprintf(b, "  name = \"clavesa-${%s}-from-%s\"\n", p.PipelineNameExpr, producer)
		fmt.Fprintf(b, "  assume_role_policy = jsonencode({\n")
		fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
		fmt.Fprintf(b, "    Statement = [{ Effect = \"Allow\", Action = \"sts:AssumeRole\", Principal = { Service = \"events.amazonaws.com\" } }]\n")
		fmt.Fprintf(b, "  })\n")
		fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
		fmt.Fprintf(b, "}\n\n")

		fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"upstream_trigger_%s\" {\n", safe)
		fmt.Fprintf(b, "  name = \"clavesa-${%s}-from-%s\"\n", p.PipelineNameExpr, producer)
		fmt.Fprintf(b, "  role = aws_iam_role.upstream_trigger_%s.id\n", safe)
		fmt.Fprintf(b, "  policy = jsonencode({\n")
		fmt.Fprintf(b, "    Version   = \"2012-10-17\"\n")
		fmt.Fprintf(b, "    Statement = [{ Sid = \"StartExecution\", Effect = \"Allow\", Action = [\"states:StartExecution\"], Resource = [aws_sfn_state_machine.pipeline.arn] }]\n")
		fmt.Fprintf(b, "  })\n")
		fmt.Fprintf(b, "}\n\n")

		// Target wires rule → our state machine. role_arn at target
		// level (mirrors the schedule pattern) gives EventBridge the
		// permission to call StartExecution.
		fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"upstream_%s\" {\n", safe)
		fmt.Fprintf(b, "  rule     = aws_cloudwatch_event_rule.upstream_%s.name\n", safe)
		fmt.Fprintf(b, "  arn      = aws_sfn_state_machine.pipeline.arn\n")
		fmt.Fprintf(b, "  role_arn = aws_iam_role.upstream_trigger_%s.arn\n", safe)
		fmt.Fprintf(b, "  # _trigger is read by runs_writer (see runner/runner.py\n")
		fmt.Fprintf(b, "  # :_RUNS_TRIGGER_VALUES) and stored on runs.trigger; the\n")
		fmt.Fprintf(b, "  # producer pipeline name is carried separately in case a\n")
		fmt.Fprintf(b, "  # future runs column wants to surface it directly.\n")
		fmt.Fprintf(b, "  input = jsonencode({\n")
		fmt.Fprintf(b, "    pipeline           = %s\n", p.PipelineNameExpr)
		fmt.Fprintf(b, "    _trigger           = \"upstream\"\n")
		fmt.Fprintf(b, "    _upstream_pipeline = \"%s\"\n", producer)
		fmt.Fprintf(b, "  })\n")
		fmt.Fprintf(b, "}\n\n")
	}
}

// ---------------------------------------------------------------------------
// runs_writer (always emitted — every pipeline has a bucket today)
// ---------------------------------------------------------------------------

func emitRunsWriter(b *strings.Builder, p Pipeline) {
	// ADR-018 (v2.0.0): runs_writer used to be a boto3-only Python zip
	// Lambda that did `INSERT INTO <runs>` via Athena. With the swap to
	// Delta, Athena's INSERT path is gone (Athena's Delta support is
	// read-only). runs_writer now deploys as the same image-based
	// Lambda the transform nodes use — the runner image already
	// carries Spark + Delta + the IAM scope to write the workspace's
	// system catalog. Cold start is heavier (~5s vs ~1s for the zip),
	// but the path is proven and adds zero new packaging
	// infrastructure (delta-rs in a zip would require a Lambda layer
	// or in-archive pip install). The thin Lambda handler lives in
	// runner.py as runs_writer_handler.
	fmt.Fprintf(b, "# runs_writer — image-based Lambda that appends one row per\n")
	fmt.Fprintf(b, "# terminal SFN execution to <system_catalog>__pipelines.runs (Delta\n")
	fmt.Fprintf(b, "# via the runner image's Spark session, ADR-018). Pairs with the\n")
	fmt.Fprintf(b, "# runner-populated node_runs table; joining on sf_execution_arn\n")
	fmt.Fprintf(b, "# answers \"which nodes ran in this execution?\".\n")

	// The Lambda is pinned to the runner image digest at plan time so
	// the function picks up a freshly-pushed runner image without an
	// explicit `terraform apply` to the Lambda — same content-
	// addressed pattern modules/transform/aws/main.tf uses for runner
	// nodes (`aws_ecr_image` data source).
	fmt.Fprintf(b, "locals {\n")
	fmt.Fprintf(b, "  runs_writer_image_match = regex(\"^([^:]+):(.+)$\", %s)\n", p.RunnerImageExpr)
	fmt.Fprintf(b, "  runs_writer_repo_uri    = local.runs_writer_image_match[0]\n")
	fmt.Fprintf(b, "  runs_writer_tag         = local.runs_writer_image_match[1]\n")
	fmt.Fprintf(b, "  runs_writer_repo_name   = regex(\"^[^/]+/(.+)$\", local.runs_writer_repo_uri)[0]\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_ecr_image\" \"runs_writer\" {\n")
	fmt.Fprintf(b, "  repository_name = local.runs_writer_repo_name\n")
	fmt.Fprintf(b, "  image_tag       = local.runs_writer_tag\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "data \"aws_iam_policy_document\" \"runs_writer_assume\" {\n")
	fmt.Fprintf(b, "  statement {\n")
	fmt.Fprintf(b, "    actions = [\"sts:AssumeRole\"]\n")
	fmt.Fprintf(b, "    principals {\n")
	fmt.Fprintf(b, "      type        = \"Service\"\n")
	fmt.Fprintf(b, "      identifiers = [\"lambda.amazonaws.com\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_iam_role\" \"runs_writer\" {\n")
	fmt.Fprintf(b, "  name               = \"clavesa-${%s}-runs-writer\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.runs_writer_assume.json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	// IAM scoped to the workspace system catalog DB (ADR-016). Drops
	// the Athena permissions the v1.x runs_writer needed for its
	// `INSERT INTO` path; keeps Glue (for CREATE DATABASE / table
	// metadata) and S3 (Delta writes the transaction log + parquet
	// files directly).
	sysCatalogSafe := safeCatalogLiteral(p.SystemCatalog)
	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"runs_writer\" {\n")
	fmt.Fprintf(b, "  name = \"clavesa-${%s}-runs-writer\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role = aws_iam_role.runs_writer.id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [\n")
	fmt.Fprintf(b, "      { Sid = \"GlueCatalog\", Effect = \"Allow\", Action = [\"glue:GetDatabase\", \"glue:CreateDatabase\", \"glue:GetTable\", \"glue:GetTables\", \"glue:CreateTable\", \"glue:UpdateTable\", \"glue:GetPartition\", \"glue:GetPartitions\"], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:catalog\",\n")
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s__pipelines\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "          \"arn:aws:glue:*:*:table/%s__pipelines/*\",\n", sysCatalogSafe)
	fmt.Fprintf(b, "      ]},\n")
	fmt.Fprintf(b, "      { Sid = \"S3Warehouse\", Effect = \"Allow\", Action = [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\", \"s3:ListBucket\", \"s3:GetBucketLocation\"], Resource = [\n")
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}\",\n", p.BucketExpr)
	fmt.Fprintf(b, "          \"arn:aws:s3:::${%s}/*\",\n", p.BucketExpr)
	fmt.Fprintf(b, "      ]},\n")
	fmt.Fprintf(b, "      { Sid = \"Logs\", Effect = \"Allow\", Action = [\"logs:CreateLogGroup\", \"logs:CreateLogStream\", \"logs:PutLogEvents\"], Resource = [\"arn:aws:logs:*:*:*\"] },\n")
	fmt.Fprintf(b, "    ]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	// Lake Formation grants on the system catalog DB — same shape as the
	// per-pipeline DB grants in emitGlueCatalogDB, but here both the
	// runs_writer role AND the pipeline runner role need access (the
	// runner appends node_runs rows to <system_catalog>__pipelines). The
	// system DB itself is created in modules/workspace/aws/main.tf; we
	// reference its name by string, not by resource (this orchestration
	// stack doesn't own the workspace module).
	fmt.Fprintf(b, "# Lake Formation grants on the workspace system catalog (GH #1).\n")
	fmt.Fprintf(b, "# References the system DB by name string since it's created in the\n")
	fmt.Fprintf(b, "# workspace module's state, not this pipeline's.\n")
	fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" \"runs_writer_system_db\" {\n")
	fmt.Fprintf(b, "  principal   = aws_iam_role.runs_writer.arn\n")
	fmt.Fprintf(b, "  permissions = [\"DESCRIBE\", \"CREATE_TABLE\", \"ALTER\"]\n")
	fmt.Fprintf(b, "  database {\n")
	fmt.Fprintf(b, "    name = \"%s__pipelines\"\n", sysCatalogSafe)
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" \"runs_writer_system_tables\" {\n")
	fmt.Fprintf(b, "  principal   = aws_iam_role.runs_writer.arn\n")
	fmt.Fprintf(b, "  permissions = [\"SELECT\", \"INSERT\", \"ALTER\", \"DESCRIBE\"]\n")
	fmt.Fprintf(b, "  table {\n")
	fmt.Fprintf(b, "    database_name = \"%s__pipelines\"\n", sysCatalogSafe)
	fmt.Fprintf(b, "    wildcard      = true\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" \"pipeline_runner_system_db\" {\n")
	fmt.Fprintf(b, "  principal   = aws_iam_role.pipeline_runner.arn\n")
	fmt.Fprintf(b, "  permissions = [\"DESCRIBE\", \"CREATE_TABLE\", \"ALTER\"]\n")
	fmt.Fprintf(b, "  database {\n")
	fmt.Fprintf(b, "    name = \"%s__pipelines\"\n", sysCatalogSafe)
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lakeformation_permissions\" \"pipeline_runner_system_tables\" {\n")
	fmt.Fprintf(b, "  principal   = aws_iam_role.pipeline_runner.arn\n")
	fmt.Fprintf(b, "  permissions = [\"SELECT\", \"INSERT\", \"ALTER\", \"DESCRIBE\"]\n")
	fmt.Fprintf(b, "  table {\n")
	fmt.Fprintf(b, "    database_name = \"%s__pipelines\"\n", sysCatalogSafe)
	fmt.Fprintf(b, "    wildcard      = true\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lambda_function\" \"runs_writer\" {\n")
	fmt.Fprintf(b, "  function_name = \"clavesa-${%s}-runs-writer\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  role          = aws_iam_role.runs_writer.arn\n")
	fmt.Fprintf(b, "  package_type  = \"Image\"\n")
	fmt.Fprintf(b, "  image_uri     = \"${local.runs_writer_repo_uri}@${data.aws_ecr_image.runs_writer.image_digest}\"\n")
	fmt.Fprintf(b, "  timeout       = 120  # cold start ~5s + first Delta write ~30s on a fresh DB\n")
	fmt.Fprintf(b, "  memory_size   = 1536 # PySpark needs the headroom even for one-row writes\n\n")
	fmt.Fprintf(b, "  image_config {\n")
	fmt.Fprintf(b, "    # Lambda invokes runner.runs_writer_handler — a thin handler in\n")
	fmt.Fprintf(b, "    # runner.py that builds a runs-table row from the EventBridge\n")
	fmt.Fprintf(b, "    # `detail` payload and calls _record_run() (Spark + Delta append).\n")
	fmt.Fprintf(b, "    command = [\"runner.runs_writer_handler\"]\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  environment {\n")
	fmt.Fprintf(b, "    variables = {\n")
	fmt.Fprintf(b, "      CLAVESA_PIPELINE         = %s\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "      CLAVESA_SYSTEM_CATALOG   = \"%s\"\n", sysCatalogSafe)
	fmt.Fprintf(b, "      CLAVESA_SYSTEM_WAREHOUSE = \"s3://${%s}/_system/pipelines/\"\n", p.BucketExpr)
	fmt.Fprintf(b, "      CLAVESA_WAREHOUSE        = \"s3://${%s}/${%s}/_warehouse/\"\n", p.BucketExpr, p.PipelineNameExpr)
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }\n\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"runs\" {\n")
	fmt.Fprintf(b, "  name        = \"clavesa-${%s}-runs\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  description = \"Captures terminal Step Functions execution events for ${%s} into the runs Delta table\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  event_pattern = jsonencode({\n")
	fmt.Fprintf(b, "    source        = [\"aws.states\"]\n")
	fmt.Fprintf(b, "    \"detail-type\" = [\"Step Functions Execution Status Change\"]\n")
	fmt.Fprintf(b, "    detail = {\n")
	fmt.Fprintf(b, "      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]\n")
	fmt.Fprintf(b, "      status          = [\"SUCCEEDED\", \"FAILED\", \"TIMED_OUT\", \"ABORTED\"]\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"runs\" {\n")
	fmt.Fprintf(b, "  rule = aws_cloudwatch_event_rule.runs.name\n")
	fmt.Fprintf(b, "  arn  = aws_lambda_function.runs_writer.arn\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_lambda_permission\" \"runs\" {\n")
	fmt.Fprintf(b, "  statement_id  = \"AllowEventBridgeInvoke\"\n")
	fmt.Fprintf(b, "  action        = \"lambda:InvokeFunction\"\n")
	fmt.Fprintf(b, "  function_name = aws_lambda_function.runs_writer.function_name\n")
	fmt.Fprintf(b, "  principal     = \"events.amazonaws.com\"\n")
	fmt.Fprintf(b, "  source_arn    = aws_cloudwatch_event_rule.runs.arn\n")
	fmt.Fprintf(b, "}\n\n")
}

// sidecarDirName is the directory inside the pipeline dir where tfgen
// expects the runs_writer/ tree and poller.py to be materialised by the
// caller (the service layer copies from embed.FS at SyncOrchestration time).
// Underscore-prefixed so it sorts before transforms in directory listings
// and reads as "managed sidecar, don't edit".
const sidecarDirName = "_clavesa_sidecar"

// SidecarDirName exposes the constant for the service layer that has to
// materialise the directory contents.
func SidecarDirName() string { return sidecarDirName }
