output "trigger_queue_arn" {
  description = "SQS queue ARN for pipeline orchestration. Pass to the orchestration module's trigger_queue_arns variable."
  value       = aws_sqs_queue.trigger.arn
}

output "outputs" {
  description = "Named output map. Sources are pass-through — table_path points at the user-configured S3 location; no staging happens."
  value = {
    "default" = {
      table_path    = local.table_path
      catalog_db    = local.catalog_db
      catalog_table = local.catalog_table
      schema        = var.schema
      # Incremental processing metadata. Empty partitions list signals
      # full-re-read mode to the orchestration emitter.
      partitions    = var.partitions
      start_from    = var.start_from
    }
  }
}
