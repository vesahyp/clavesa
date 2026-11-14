package tfgen

import (
	"fmt"
	"sort"
	"strings"
)

// BundleConfig is the AWS side of a pipeline generated from a Databricks
// bundle job (ADR-031 section 7): the job file and the bundle's files in
// S3, the schedule with its time zone, the triggers, the notifications,
// the health alarm and the Glue databases the job's tables live in. Nil
// for every other pipeline: the output is then unchanged.
type BundleConfig struct {
	// FilesDir is the pipeline-relative folder with the bundle's files
	// ("bundle"); JobFile the pipeline-relative bundle-job.json.
	FilesDir string
	JobFile  string

	// Schedule: an AWS cron expression (cron(...)) or rate expression
	// (rate(...)), its time zone (empty for rate), and whether it is
	// paused. Empty Expression: no schedule.
	ScheduleExpression string
	ScheduleTimezone   string
	SchedulePaused     bool

	// FileArrival starts the job when an object lands under the prefix
	// (EventBridge notifications must be on for the bucket).
	FileArrivalBucket string
	FileArrivalPrefix string
	FileArrivalPaused bool

	// Notify maps "start", "success", "failure" to email addresses.
	Notify map[string][]string
	// HealthMaxSeconds > 0 adds a CloudWatch alarm on the state machine's
	// ExecutionTime above it, published to the failure topic.
	HealthMaxSeconds float64

	// GlueDatabases are the <catalog>__<schema> databases of the tables
	// the job reads and writes (catalog: glue); the runner may create them.
	GlueDatabases []string
	// ReadBuckets are S3 buckets outside the workspace the job's code
	// reads (file_arrival, s3:// paths in parameters).
	ReadBuckets []string
	// SecretsPrefix is the Secrets Manager name prefix of the job's
	// Databricks secrets: clavesa/<workspace>/dbx/.
	SecretsPrefix string

	// A mixed job (ADR-031 section 8): its segments, the Databricks job
	// the Databricks segments run in, and the job file's S3 URI (HCL).
	Segments []PipelineSegment
	// LambdaMemoryMB is the runner Lambda's memory from the job's size; 0
	// keeps the default.
	LambdaMemoryMB  int
	DatabricksJobID int64
	JobURI          string
}

func (p Pipeline) bundleNotifyTopics() []string {
	if p.Bundle == nil {
		return nil
	}
	var kinds []string
	for k, addrs := range p.Bundle.Notify {
		if len(addrs) > 0 {
			kinds = append(kinds, k)
		}
	}
	if p.Bundle.HealthMaxSeconds > 0 && len(p.Bundle.Notify["failure"]) == 0 {
		kinds = append(kinds, "failure")
	}
	sort.Strings(kinds)
	return kinds
}

// emitBundle writes the bundle resources.
func emitBundle(b *strings.Builder, p Pipeline) {
	bc := p.Bundle
	if bc == nil {
		return
	}
	base := p.awsBase()
	b.WriteString("# ---------------------------------------------------------------------------\n")
	b.WriteString("# Databricks bundle job (ADR-031): the job file and the bundle's files in S3,\n")
	b.WriteString("# the job's schedule and triggers, notifications and Glue databases.\n")
	b.WriteString("# ---------------------------------------------------------------------------\n\n")

	fmt.Fprintf(b, "resource \"aws_s3_object\" \"bundle_job\" {\n")
	fmt.Fprintf(b, "  bucket = %s\n", p.BucketExpr)
	fmt.Fprintf(b, "  key    = \"${%s}/_bundle/bundle-job.json\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  source = \"${path.module}/%s\"\n", bc.JobFile)
	fmt.Fprintf(b, "  etag   = filemd5(\"${path.module}/%s\")\n", bc.JobFile)
	fmt.Fprintf(b, "  tags   = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	fmt.Fprintf(b, "resource \"aws_s3_object\" \"bundle_files\" {\n")
	fmt.Fprintf(b, "  for_each = fileset(\"${path.module}/%s\", \"**\")\n", bc.FilesDir)
	fmt.Fprintf(b, "  bucket   = %s\n", p.BucketExpr)
	fmt.Fprintf(b, "  key      = \"${%s}/_bundle/files/${each.value}\"\n", p.PipelineNameExpr)
	fmt.Fprintf(b, "  source   = \"${path.module}/%s/${each.value}\"\n", bc.FilesDir)
	fmt.Fprintf(b, "  etag     = filemd5(\"${path.module}/%s/${each.value}\")\n", bc.FilesDir)
	fmt.Fprintf(b, "  tags     = local.clavesa_tags\n")
	fmt.Fprintf(b, "}\n\n")

	// Runner access the job needs beyond the pipeline's own: the Glue
	// databases of its tables, the warehouse folder those databases use,
	// external buckets it reads and its Databricks secrets.
	fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"bundle_runner\" {\n")
	fmt.Fprintf(b, "  name = \"%s-bundle\"\n", base)
	fmt.Fprintf(b, "  role = aws_iam_role.pipeline_runner.id\n\n")
	fmt.Fprintf(b, "  policy = jsonencode({\n")
	fmt.Fprintf(b, "    Version = \"2012-10-17\"\n")
	fmt.Fprintf(b, "    Statement = [\n")
	fmt.Fprintf(b, "      { Sid = \"BundleWarehouseWrite\", Effect = \"Allow\", Action = [\"s3:PutObject\", \"s3:DeleteObject\", \"s3:AbortMultipartUpload\", \"s3:ListMultipartUploadParts\"], Resource = [\"arn:aws:s3:::${%s}/_bundle_warehouse/*\"] },\n", p.BucketExpr)
	if len(bc.GlueDatabases) > 0 {
		fmt.Fprintf(b, "      { Sid = \"BundleGlue\", Effect = \"Allow\", Action = [\n")
		fmt.Fprintf(b, "          \"glue:GetDatabase\", \"glue:GetDatabases\", \"glue:CreateDatabase\", \"glue:GetTable\", \"glue:GetTables\",\n")
		fmt.Fprintf(b, "          \"glue:CreateTable\", \"glue:UpdateTable\", \"glue:DeleteTable\", \"glue:GetPartition\", \"glue:GetPartitions\",\n")
		fmt.Fprintf(b, "          \"glue:CreatePartition\", \"glue:UpdatePartition\", \"glue:DeletePartition\", \"glue:BatchCreatePartition\", \"glue:BatchDeletePartition\",\n")
		fmt.Fprintf(b, "      ], Resource = [\n")
		fmt.Fprintf(b, "          \"arn:aws:glue:*:*:catalog\",\n")
		for _, db := range bc.GlueDatabases {
			fmt.Fprintf(b, "          \"arn:aws:glue:*:*:database/%s\",\n", db)
			fmt.Fprintf(b, "          \"arn:aws:glue:*:*:table/%s/*\",\n", db)
		}
		fmt.Fprintf(b, "      ]},\n")
	}
	if len(bc.ReadBuckets) > 0 {
		fmt.Fprintf(b, "      { Sid = \"BundleS3Read\", Effect = \"Allow\", Action = [\"s3:GetObject\", \"s3:ListBucket\", \"s3:GetBucketLocation\"], Resource = [\n")
		for _, rb := range bc.ReadBuckets {
			fmt.Fprintf(b, "          \"arn:aws:s3:::%s\",\n          \"arn:aws:s3:::%s/*\",\n", rb, rb)
		}
		fmt.Fprintf(b, "      ]},\n")
	}
	if bc.SecretsPrefix != "" {
		fmt.Fprintf(b, "      { Sid = \"BundleSecrets\", Effect = \"Allow\", Action = [\"secretsmanager:GetSecretValue\"], Resource = [\"arn:aws:secretsmanager:*:*:secret:%s*\"] },\n", bc.SecretsPrefix)
	}
	fmt.Fprintf(b, "    ]\n")
	fmt.Fprintf(b, "  })\n")
	fmt.Fprintf(b, "}\n\n")

	needsStartRole := bc.ScheduleExpression != "" || bc.FileArrivalBucket != ""
	if needsStartRole {
		fmt.Fprintf(b, "resource \"aws_iam_role\" \"bundle_start\" {\n")
		fmt.Fprintf(b, "  name = \"%s-bundle-start\"\n", base)
		fmt.Fprintf(b, "  assume_role_policy = jsonencode({\n")
		fmt.Fprintf(b, "    Version   = \"2012-10-17\"\n")
		fmt.Fprintf(b, "    Statement = [{ Effect = \"Allow\", Action = \"sts:AssumeRole\", Principal = { Service = [\"scheduler.amazonaws.com\", \"events.amazonaws.com\"] } }]\n")
		fmt.Fprintf(b, "  })\n")
		fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
		fmt.Fprintf(b, "}\n\n")
		fmt.Fprintf(b, "resource \"aws_iam_role_policy\" \"bundle_start\" {\n")
		fmt.Fprintf(b, "  name = \"%s-bundle-start\"\n", base)
		fmt.Fprintf(b, "  role = aws_iam_role.bundle_start.id\n")
		fmt.Fprintf(b, "  policy = jsonencode({\n")
		fmt.Fprintf(b, "    Version   = \"2012-10-17\"\n")
		fmt.Fprintf(b, "    Statement = [{ Effect = \"Allow\", Action = [\"states:StartExecution\"], Resource = [aws_sfn_state_machine.pipeline.arn] }]\n")
		fmt.Fprintf(b, "  })\n")
		fmt.Fprintf(b, "}\n\n")
	}

	// Schedule: EventBridge Scheduler, which takes a time zone.
	if bc.ScheduleExpression != "" {
		state := "ENABLED"
		if bc.SchedulePaused {
			state = "DISABLED"
		}
		fmt.Fprintf(b, "resource \"aws_scheduler_schedule\" \"bundle\" {\n")
		fmt.Fprintf(b, "  name                = \"%s\"\n", base)
		fmt.Fprintf(b, "  description         = \"Schedule of the bundle job behind ${%s}\"\n", p.PipelineNameExpr)
		fmt.Fprintf(b, "  schedule_expression = %q\n", bc.ScheduleExpression)
		if bc.ScheduleTimezone != "" {
			fmt.Fprintf(b, "  schedule_expression_timezone = %q\n", bc.ScheduleTimezone)
		}
		fmt.Fprintf(b, "  state               = %q\n\n", state)
		fmt.Fprintf(b, "  flexible_time_window {\n    mode = \"OFF\"\n  }\n\n")
		fmt.Fprintf(b, "  target {\n")
		fmt.Fprintf(b, "    arn      = aws_sfn_state_machine.pipeline.arn\n")
		fmt.Fprintf(b, "    role_arn = aws_iam_role.bundle_start.arn\n")
		fmt.Fprintf(b, "    input    = jsonencode({ pipeline = %s, _trigger = \"scheduled\" })\n", p.PipelineNameExpr)
		fmt.Fprintf(b, "  }\n")
		fmt.Fprintf(b, "}\n\n")
	}

	// file_arrival: an S3 Object Created event under the prefix.
	if bc.FileArrivalBucket != "" {
		state := "ENABLED"
		if bc.FileArrivalPaused {
			state = "DISABLED"
		}
		pattern := fmt.Sprintf(`{ source = ["aws.s3"], "detail-type" = ["Object Created"], detail = { bucket = { name = [%q] }, object = { key = [{ prefix = %q }] } } }`, bc.FileArrivalBucket, bc.FileArrivalPrefix)
		fmt.Fprintf(b, "# file_arrival: needs EventBridge notifications on bucket %s.\n", bc.FileArrivalBucket)
		fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"bundle_file_arrival\" {\n")
		fmt.Fprintf(b, "  name          = \"%s-file-arrival\"\n", base)
		fmt.Fprintf(b, "  state         = %q\n", state)
		fmt.Fprintf(b, "  event_pattern = jsonencode(%s)\n", pattern)
		fmt.Fprintf(b, "  tags          = local.clavesa_tags\n")
		fmt.Fprintf(b, "}\n\n")
		fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"bundle_file_arrival\" {\n")
		fmt.Fprintf(b, "  rule     = aws_cloudwatch_event_rule.bundle_file_arrival.name\n")
		fmt.Fprintf(b, "  arn      = aws_sfn_state_machine.pipeline.arn\n")
		fmt.Fprintf(b, "  role_arn = aws_iam_role.bundle_start.arn\n")
		fmt.Fprintf(b, "  input    = jsonencode({ pipeline = %s, _trigger = \"event\" })\n", p.PipelineNameExpr)
		fmt.Fprintf(b, "}\n\n")
	}

	// Notifications: one topic per event kind with the job's addresses,
	// and an EventBridge rule on the state machine's status.
	statuses := map[string]string{
		"start":   `["RUNNING"]`,
		"success": `["SUCCEEDED"]`,
		"failure": `["FAILED", "TIMED_OUT", "ABORTED"]`,
	}
	for _, kind := range p.bundleNotifyTopics() {
		fmt.Fprintf(b, "resource \"aws_sns_topic\" \"bundle_%s\" {\n", kind)
		fmt.Fprintf(b, "  name = \"%s-%s\"\n", base, kind)
		fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
		fmt.Fprintf(b, "}\n\n")
		fmt.Fprintf(b, "resource \"aws_sns_topic_policy\" \"bundle_%s\" {\n", kind)
		fmt.Fprintf(b, "  arn = aws_sns_topic.bundle_%s.arn\n", kind)
		fmt.Fprintf(b, "  policy = jsonencode({\n")
		fmt.Fprintf(b, "    Version   = \"2012-10-17\"\n")
		fmt.Fprintf(b, "    Statement = [{ Effect = \"Allow\", Action = \"sns:Publish\", Principal = { Service = [\"events.amazonaws.com\", \"cloudwatch.amazonaws.com\"] }, Resource = aws_sns_topic.bundle_%s.arn }]\n", kind)
		fmt.Fprintf(b, "  })\n")
		fmt.Fprintf(b, "}\n\n")
		for i, addr := range bc.Notify[kind] {
			fmt.Fprintf(b, "resource \"aws_sns_topic_subscription\" \"bundle_%s_%d\" {\n", kind, i)
			fmt.Fprintf(b, "  topic_arn = aws_sns_topic.bundle_%s.arn\n", kind)
			fmt.Fprintf(b, "  protocol  = \"email\"\n")
			fmt.Fprintf(b, "  endpoint  = %q\n", addr)
			fmt.Fprintf(b, "}\n\n")
		}
		fmt.Fprintf(b, "resource \"aws_cloudwatch_event_rule\" \"bundle_%s\" {\n", kind)
		fmt.Fprintf(b, "  name = \"%s-%s\"\n", base, kind)
		fmt.Fprintf(b, "  event_pattern = jsonencode({ source = [\"aws.states\"], \"detail-type\" = [\"Step Functions Execution Status Change\"], detail = { stateMachineArn = [aws_sfn_state_machine.pipeline.arn], status = %s } })\n", statuses[kind])
		fmt.Fprintf(b, "  tags = local.clavesa_tags\n")
		fmt.Fprintf(b, "}\n\n")
		fmt.Fprintf(b, "resource \"aws_cloudwatch_event_target\" \"bundle_%s\" {\n", kind)
		fmt.Fprintf(b, "  rule = aws_cloudwatch_event_rule.bundle_%s.name\n", kind)
		fmt.Fprintf(b, "  arn  = aws_sns_topic.bundle_%s.arn\n", kind)
		fmt.Fprintf(b, "}\n\n")
	}
	if bc.HealthMaxSeconds > 0 {
		fmt.Fprintf(b, "# health RUN_DURATION_SECONDS: an alarm on the execution time.\n")
		fmt.Fprintf(b, "resource \"aws_cloudwatch_metric_alarm\" \"bundle_duration\" {\n")
		fmt.Fprintf(b, "  alarm_name          = \"%s-duration\"\n", base)
		fmt.Fprintf(b, "  namespace           = \"AWS/States\"\n")
		fmt.Fprintf(b, "  metric_name         = \"ExecutionTime\"\n")
		fmt.Fprintf(b, "  dimensions          = { StateMachineArn = aws_sfn_state_machine.pipeline.arn }\n")
		fmt.Fprintf(b, "  statistic           = \"Maximum\"\n")
		fmt.Fprintf(b, "  period              = 300\n")
		fmt.Fprintf(b, "  evaluation_periods  = 1\n")
		fmt.Fprintf(b, "  comparison_operator = \"GreaterThanThreshold\"\n")
		fmt.Fprintf(b, "  threshold           = %g\n", bc.HealthMaxSeconds*1000)
		fmt.Fprintf(b, "  treat_missing_data  = \"notBreaching\"\n")
		fmt.Fprintf(b, "  alarm_actions       = [aws_sns_topic.bundle_failure.arn]\n")
		fmt.Fprintf(b, "  tags                = local.clavesa_tags\n")
		fmt.Fprintf(b, "}\n\n")
	}
}
