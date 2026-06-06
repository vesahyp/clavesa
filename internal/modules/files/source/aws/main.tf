locals {
  output_name = "default"
  # Pass-through: the source's "output" is just the user-configured S3 location.
  # No staging step, no Glue ETL job — the runner Lambda reads directly from
  # this path via spark.read.<format>(...).
  table_path = "s3://${var.bucket}/${var.prefix}"

  # Glue catalog naming kept for downstream object shape compatibility (the
  # transform module's `inputs` variable accepts {table_path, catalog_db,
  # catalog_table, schema}). Both fields are unused today (we write plain
  # Parquet, not Iceberg). Will become real when Iceberg lands per ADR-007.
  catalog_db    = "clavesa_${replace(var.pipeline_name, "-", "_")}"
  catalog_table = "${replace(var.name, "-", "_")}__${local.output_name}"

  tags = merge(var.tags, {
    "clavesa:pipeline" = var.pipeline_name
    "clavesa:node"     = var.name
    "clavesa:type"     = "source"
  })
}

# ---------------------------------------------------------------------------
# SQS trigger queue — buffers S3 events for pipeline orchestration.
# The orchestration module polls this queue and starts the state machine
# when messages are present, providing configurable batch-window debouncing.
#
# This is the only AWS resource a source creates. Reads happen at transform
# time via the runner Lambda's IAM (which is granted s3:GetObject on the
# source bucket via input_buckets in modules/transform/aws/main.tf).
# ---------------------------------------------------------------------------

resource "aws_sqs_queue" "trigger" {
  name                      = "clavesa-${var.pipeline_name}-${var.name}-trigger"
  message_retention_seconds = 86400 # 1 day

  # The runner Lambda drains this queue with ReceiveMessage and only deletes a
  # message after the Delta write commits. Keep received-but-undeleted messages
  # invisible for the whole run (SFN task TimeoutSeconds is 900); a shorter
  # window would resurface them mid-run and a concurrent/next fire would re-read
  # the same objects.
  visibility_timeout_seconds = var.trigger_visibility_timeout_seconds

  # Messages that repeatedly fail to process (poison / unreadable object) drop
  # to the DLQ instead of cycling forever.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.trigger_dlq.arn
    maxReceiveCount     = var.trigger_max_receive_count
  })

  tags = local.tags
}

# Dead-letter queue for the trigger queue. Failures land here for inspection;
# a long retention keeps them around. It only receives messages via the
# trigger queue's redrive policy (not EventBridge), so it needs no queue policy
# of its own.
resource "aws_sqs_queue" "trigger_dlq" {
  name                      = "clavesa-${var.pipeline_name}-${var.name}-trigger-dlq"
  message_retention_seconds = 1209600 # 14 days
  tags                      = local.tags
}

resource "aws_sqs_queue_policy" "trigger" {
  queue_url = aws_sqs_queue.trigger.url
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridgeS3Trigger"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.trigger.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.s3_trigger.arn }
      }
    }]
  })
}

resource "aws_cloudwatch_event_rule" "s3_trigger" {
  name        = "clavesa-${var.pipeline_name}-${var.name}-s3-trigger"
  description = "Fires when new objects arrive at s3://${var.bucket}/${var.prefix}"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.bucket]
      }
      object = {
        key = [{
          prefix = var.prefix
        }]
      }
    }
  })

  tags = local.tags
}

resource "aws_cloudwatch_event_target" "sqs_trigger" {
  rule = aws_cloudwatch_event_rule.s3_trigger.name
  arn  = aws_sqs_queue.trigger.arn
}

# ---------------------------------------------------------------------------
# Bucket-level EventBridge notification toggle. Opt-in because
# `aws_s3_bucket_notification` is authoritative — Terraform owns the full
# notification config on the bucket and any pre-existing SNS/Lambda
# subscriptions disappear on apply. Leave the var false when clavesa
# doesn't own the bucket.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_notification" "source_bucket" {
  count       = var.manage_bucket_notifications ? 1 : 0
  bucket      = var.bucket
  eventbridge = true
}
