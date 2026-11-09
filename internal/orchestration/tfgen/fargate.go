package tfgen

import (
	"fmt"
	"strings"
)

// ComputeFargate is the Pipeline.Compute value that runs a pipeline run as
// one ECS Fargate task instead of the runner Lambda.
const ComputeFargate = "fargate"

func (p Pipeline) fargate() bool { return p.Compute == ComputeFargate }

// ---------------------------------------------------------------------------
// compute = "fargate"
//
// One Step Functions execution starts one ECS Fargate task that runs the
// whole pipeline bundle (runner.py CLAVESA_FARGATE=1 mode), the same
// pipeline_handler the Lambda runs. For runs that outgrow Lambda's 15
// minutes, 10 GB and 6 vCPU: a day of tens of GB. The size is per
// pipeline (fargate_cpu / fargate_memory_gb in terraform.tfvars).
//
// The bundle (transforms with resolved inputs and outputs) is too big for
// ECS container overrides (8 KB), so it goes to S3 as
// <pipeline>/_runtime/bundle.json and the state machine passes its URI.
// The state machine still carries the bundle in a Pass state, so
// everything that reads the transforms from the definition (cloud-local
// runs, backfill) keeps working. The runner Lambda stays deployed: the
// backfill and maintenance operations and the cloud-local env lookup use
// it.
// ---------------------------------------------------------------------------

// fargateArchExpr maps the runner image's Lambda architecture
// (x86_64 / arm64) to ECS's (X86_64 / ARM64).
func (p Pipeline) fargateArchExpr() string {
	if p.RunnerArchExpr == "" {
		return `"X86_64"`
	}
	return fmt.Sprintf(`try(%s, "x86_64") == "arm64" ? "ARM64" : "X86_64"`, p.RunnerArchExpr)
}

// writeBundle writes the pipeline bundle's fields (the Lambda payload
// without the execution-context keys) as HCL object attributes.
func writeBundle(b *strings.Builder, p Pipeline, in string) {
	fmt.Fprintf(b, "%s_pipeline_run = true\n", in)
	fmt.Fprintf(b, "%spipeline      = %s\n", in, p.PipelineNameExpr)
	fmt.Fprintf(b, "%stransforms = [\n", in)
	for _, t := range p.Transforms {
		fmt.Fprintf(b, "%s  {\n", in)
		fmt.Fprintf(b, "%s    node       = %q\n", in, t.NodeID)
		fmt.Fprintf(b, "%s    language   = %q\n", in, t.Language)
		fmt.Fprintf(b, "%s    logic_path = %s\n", in, t.LogicS3URI)
		fmt.Fprintf(b, "%s    inputs     = %s\n", in, t.InputsExpr)
		fmt.Fprintf(b, "%s    outputs    = %s\n", in, t.OutputsExpr)
		fmt.Fprintf(b, "%s    parents    = %s\n", in, renderParents(t.Parents))
		fmt.Fprintf(b, "%s  },\n", in)
	}
	fmt.Fprintf(b, "%s]\n", in)
}

func emitFargate(b *strings.Builder, p Pipeline) {
	if !p.fargate() {
		return
	}
	base := p.awsBase()
	b.WriteString(`# compute = "fargate": one ECS Fargate task per pipeline run. Size and
# network are pipeline variables; set them in terraform.tfvars.
variable "fargate_cpu" {
  description = "vCPUs of the Fargate task (1, 2, 4, 8 or 16)."
  type        = number
  default     = 16
}

variable "fargate_memory_gb" {
  description = "Memory of the Fargate task in GB. 16 vCPU takes 32 to 120 in steps of 8; see the Fargate CPU and memory table."
  type        = number
  default     = 32
}

variable "fargate_ephemeral_gb" {
  description = "Task disk in GB (21 to 200): Spark spill and shuffle space."
  type        = number
  default     = 100
}

variable "fargate_subnets" {
  description = "Subnets the task runs in. Empty: the default VPC's default subnets. The task needs a route to S3, ECR, Glue and Databricks (public IP or NAT)."
  type        = list(string)
  default     = []
}

variable "fargate_security_groups" {
  description = "Security groups of the task. Empty: one clavesa creates, outbound only."
  type        = list(string)
  default     = []
}

variable "fargate_assign_public_ip" {
  description = "Give the task a public IP (needed in a public subnet without NAT)."
  type        = bool
  default     = true
}

variable "fargate_timeout_seconds" {
  description = "Longest a run may take before Step Functions stops it."
  type        = number
  default     = 14400
}

data "aws_region" "fargate" {}

`)
	fmt.Fprintf(b, "locals {\n")
	fmt.Fprintf(b, "  pipeline_runner_env = {\n")
	writeRunnerEnv(b, p, "    ")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "  pipeline_bundle = {\n")
	writeBundle(b, p, "    ")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "  fargate_bundle_key      = \"${%s}/_runtime/bundle.json\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  fargate_subnets         = length(var.fargate_subnets) > 0 ? var.fargate_subnets : data.aws_subnets.fargate_default[0].ids\n")
	fmt.Fprintf(b, "  fargate_security_groups = length(var.fargate_security_groups) > 0 ? var.fargate_security_groups : [aws_security_group.fargate[0].id]\n")
	fmt.Fprintf(b, "}\n\n")

	b.WriteString(`data "aws_vpc" "fargate_default" {
  count   = length(var.fargate_subnets) == 0 ? 1 : 0
  default = true
}

data "aws_subnets" "fargate_default" {
  count = length(var.fargate_subnets) == 0 ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.fargate_default[0].id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_subnet" "fargate_first" {
  id = local.fargate_subnets[0]
}

`)
	fmt.Fprintf(b, "resource \"aws_security_group\" \"fargate\" {\n")
	fmt.Fprintf(b, "  count       = length(var.fargate_security_groups) == 0 ? 1 : 0\n")
	fmt.Fprintf(b, "  name        = \"%s-fargate\"\n", base)
	fmt.Fprintf(b, "  description = \"clavesa pipeline runner task: outbound only\"\n")
	fmt.Fprintf(b, "  vpc_id      = data.aws_subnet.fargate_first.vpc_id\n")
	fmt.Fprintf(b, "  egress {\n")
	fmt.Fprintf(b, "    from_port   = 0\n")
	fmt.Fprintf(b, "    to_port     = 0\n")
	fmt.Fprintf(b, "    protocol    = \"-1\"\n")
	fmt.Fprintf(b, "    cidr_blocks = [\"0.0.0.0/0\"]\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_s3_object\" \"pipeline_bundle\" {\n")
	fmt.Fprintf(b, "  bucket       = %s\n", p.BucketExpr)
	fmt.Fprintf(b, "  key          = local.fargate_bundle_key\n")
	fmt.Fprintf(b, "  content      = jsonencode(local.pipeline_bundle)\n")
	fmt.Fprintf(b, "  content_type = \"application/json\"\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_ecs_cluster\" \"pipeline\" {\n")
	fmt.Fprintf(b, "  name = \"%s\"\n", base)
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	b.WriteString(`data "aws_iam_policy_document" "fargate_exec_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

`)
	fmt.Fprintf(b, "resource \"aws_iam_role\" \"fargate_exec\" {\n")
	fmt.Fprintf(b, "  name               = \"%s-ecs-exec\"\n", base)
	fmt.Fprintf(b, "  assume_role_policy = data.aws_iam_policy_document.fargate_exec_assume.json\n")
	fmt.Fprintf(b, "  tags               = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")
	fmt.Fprintf(b, "resource \"aws_iam_role_policy_attachment\" \"fargate_exec\" {\n")
	fmt.Fprintf(b, "  role       = aws_iam_role.fargate_exec.name\n")
	fmt.Fprintf(b, "  policy_arn = \"arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy\"\n")
	fmt.Fprintf(b, "}\n\n")
	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"fargate_exec_logs\" {\n")
	fmt.Fprintf(b, "  name = \"%s-ecs-exec-logs\"\n", base)
	fmt.Fprintf(b, "  role = aws_iam_role.fargate_exec.id\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version   = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [{ Sid = \"CreateLogGroup\", Effect = \"Allow\", Action = [\"logs:CreateLogGroup\"], Resource = [\"*\"] }]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	// The task logs into the runner Lambda's log group (stream prefix
	// "fargate"), so the CLI and UI log views read one place for both.
	fmt.Fprintf(b, "resource \"aws_ecs_task_definition\" \"pipeline_runner\" {\n")
	fmt.Fprintf(b, "  family                   = \"%s-runner\"\n", base)
	fmt.Fprintf(b, "  requires_compatibilities = [\"FARGATE\"]\n")
	fmt.Fprintf(b, "  network_mode             = \"awsvpc\"\n")
	fmt.Fprintf(b, "  cpu                      = tostring(var.fargate_cpu * 1024)\n")
	fmt.Fprintf(b, "  memory                   = tostring(var.fargate_memory_gb * 1024)\n")
	fmt.Fprintf(b, "  execution_role_arn       = aws_iam_role.fargate_exec.arn\n")
	fmt.Fprintf(b, "  task_role_arn            = aws_iam_role.pipeline_runner.arn\n")
	fmt.Fprintf(b, "  runtime_platform {\n")
	fmt.Fprintf(b, "    operating_system_family = \"LINUX\"\n")
	fmt.Fprintf(b, "    cpu_architecture        = %s\n", p.fargateArchExpr())
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "  ephemeral_storage {\n")
	fmt.Fprintf(b, "    size_in_gib = var.fargate_ephemeral_gb\n")
	fmt.Fprintf(b, "  }\n")
	fmt.Fprintf(b, "  container_definitions = jsonencode([{\n")
	fmt.Fprintf(b, "    name      = \"runner\"\n")
	fmt.Fprintf(b, "    image     = \"${local.pipeline_runner_repo_uri}@${data.aws_ecr_image.pipeline_runner.image_digest}\"\n")
	fmt.Fprintf(b, "    essential = true\n")
	fmt.Fprintf(b, "    environment = concat([for k, v in local.pipeline_runner_env : { name = k, value = tostring(v) }], [\n")
	fmt.Fprintf(b, "      { name = \"CLAVESA_FARGATE\", value = \"1\" },\n")
	fmt.Fprintf(b, "      { name = \"CLAVESA_COMPUTE\", value = \"fargate\" },\n")
	fmt.Fprintf(b, "      { name = \"CLAVESA_COMPUTE_MEMORY_MB\", value = tostring(var.fargate_memory_gb * 1024) },\n")
	fmt.Fprintf(b, "      { name = \"CLAVESA_JVM_HEAP_MB\", value = tostring(floor(var.fargate_memory_gb * 1024 * 0.75)) },\n")
	fmt.Fprintf(b, "      { name = \"CLAVESA_BUNDLE_URI\", value = \"s3://${%s}/${local.fargate_bundle_key}\" },\n", p.BucketExpr)
	fmt.Fprintf(b, "      { name = \"AWS_REGION\", value = data.aws_region.fargate.region },\n")
	fmt.Fprintf(b, "    ])\n")
	fmt.Fprintf(b, "    logConfiguration = {\n")
	fmt.Fprintf(b, "      logDriver = \"awslogs\"\n")
	fmt.Fprintf(b, "      options = {\n")
	fmt.Fprintf(b, "        awslogs-group         = \"/aws/lambda/%s-runner\"\n", base)
	fmt.Fprintf(b, "        awslogs-region        = data.aws_region.fargate.region\n")
	fmt.Fprintf(b, "        awslogs-stream-prefix = \"fargate\"\n")
	fmt.Fprintf(b, "        awslogs-create-group  = \"true\"\n")
	fmt.Fprintf(b, "      }\n")
	fmt.Fprintf(b, "    }\n")
	fmt.Fprintf(b, "  }])\n")
	fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")
}

// emitFargateStateMachine is emitStateMachine for compute = "fargate":
// a Pass state that carries the bundle (for readers of the definition),
// then ecs:runTask.sync, which waits for the task and fails the
// execution when the container exits non-zero.
func emitFargateStateMachine(b *strings.Builder, p Pipeline) {
	fmt.Fprintf(b, "# Step Functions state machine, compute = \"fargate\": one ECS task runs\n")
	fmt.Fprintf(b, "# the whole pipeline bundle (runner.py CLAVESA_FARGATE=1).\n")
	fmt.Fprintf(b, "resource \"aws_sfn_state_machine\" \"pipeline\" {\n")
	fmt.Fprintf(b, "  name     = \"%s\"\n", p.awsBase())
	fmt.Fprintf(b, "  role_arn = aws_iam_role.sfn_exec.arn\n")
	fmt.Fprintf(b, "  type     = \"STANDARD\"\n\n")
	fmt.Fprintf(b, "  definition = jsonencode({\n")
	fmt.Fprintf(b, "    Comment = \"Clavesa pipeline: ${%s} (fargate)\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "    StartAt = \"Bundle\"\n")
	fmt.Fprintf(b, "    States = {\n")
	fmt.Fprintf(b, "      Bundle = {\n")
	fmt.Fprintf(b, "        Type = \"Pass\"\n")
	fmt.Fprintf(b, "        Parameters = {\n")
	fmt.Fprintf(b, "          Payload = merge(local.pipeline_bundle, {\n")
	fmt.Fprintf(b, "            \"_sf_execution_arn.$\"        = \"$$.Execution.Id\"\n")
	fmt.Fprintf(b, "            \"_sf_execution_started_at.$\" = \"$$.Execution.StartTime\"\n")
	fmt.Fprintf(b, "            \"_execution_input.$\"         = \"$$.Execution.Input\"\n")
	fmt.Fprintf(b, "          })\n")
	fmt.Fprintf(b, "        }\n")
	fmt.Fprintf(b, "        Next = \"RunPipeline\"\n")
	fmt.Fprintf(b, "      }\n")
	fmt.Fprintf(b, "      RunPipeline = {\n")
	fmt.Fprintf(b, "        Type           = \"Task\"\n")
	fmt.Fprintf(b, "        Resource       = \"arn:aws:states:::ecs:runTask.sync\"\n")
	fmt.Fprintf(b, "        TimeoutSeconds = var.fargate_timeout_seconds\n")
	fmt.Fprintf(b, "        Parameters = {\n")
	fmt.Fprintf(b, "          Cluster        = aws_ecs_cluster.pipeline.arn\n")
	fmt.Fprintf(b, "          TaskDefinition = aws_ecs_task_definition.pipeline_runner.arn\n")
	fmt.Fprintf(b, "          LaunchType     = \"FARGATE\"\n")
	fmt.Fprintf(b, "          NetworkConfiguration = {\n")
	fmt.Fprintf(b, "            AwsvpcConfiguration = {\n")
	fmt.Fprintf(b, "              Subnets        = local.fargate_subnets\n")
	fmt.Fprintf(b, "              SecurityGroups = local.fargate_security_groups\n")
	fmt.Fprintf(b, "              AssignPublicIp = var.fargate_assign_public_ip ? \"ENABLED\" : \"DISABLED\"\n")
	fmt.Fprintf(b, "            }\n")
	fmt.Fprintf(b, "          }\n")
	fmt.Fprintf(b, "          Overrides = {\n")
	fmt.Fprintf(b, "            ContainerOverrides = [{\n")
	fmt.Fprintf(b, "              Name = \"runner\"\n")
	fmt.Fprintf(b, "              Environment = [\n")
	fmt.Fprintf(b, "                { Name = \"CLAVESA_SF_EXECUTION_ARN\", \"Value.$\" = \"$$.Execution.Id\" },\n")
	fmt.Fprintf(b, "                { Name = \"CLAVESA_SF_STARTED_AT\", \"Value.$\" = \"$$.Execution.StartTime\" },\n")
	fmt.Fprintf(b, "                { Name = \"CLAVESA_EXECUTION_INPUT\", \"Value.$\" = \"States.JsonToString($$.Execution.Input)\" },\n")
	fmt.Fprintf(b, "              ]\n")
	fmt.Fprintf(b, "            }]\n")
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
	fmt.Fprintf(b, "  depends_on = [aws_iam_role_policy.sfn_exec_policy, aws_s3_object.pipeline_bundle]\n")
	fmt.Fprintf(b, "}\n\n")
}

// fargateSFNStatements are the state machine role's extra permissions for
// ecs:runTask.sync: run and watch the task, pass its two roles, and the
// managed EventBridge rule Step Functions uses to learn the task stopped.
func fargateSFNStatements(p Pipeline) string {
	if !p.fargate() {
		return ""
	}
	return `      { Sid = "EcsRunTask", Effect = "Allow", Action = ["ecs:RunTask"], Resource = [replace(aws_ecs_task_definition.pipeline_runner.arn, "/:[0-9]+$/", ":*")] },
      { Sid = "EcsWatchTask", Effect = "Allow", Action = ["ecs:StopTask", "ecs:DescribeTasks"], Resource = ["*"] },
      { Sid = "EcsPassRoles", Effect = "Allow", Action = ["iam:PassRole"], Resource = [aws_iam_role.pipeline_runner.arn, aws_iam_role.fargate_exec.arn] },
      { Sid = "EcsSyncRule", Effect = "Allow", Action = ["events:PutTargets", "events:PutRule", "events:DescribeRule"], Resource = ["arn:aws:events:${data.aws_region.fargate.region}:${data.aws_caller_identity.clavesa_owner.account_id}:rule/StepFunctionsGetEventsForECSTaskRule"] },
`
}
