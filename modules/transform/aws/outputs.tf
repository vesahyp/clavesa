# Output map — one entry per output_definitions key.
# Shape matches the TF-MODULE-SCHEMA Output Map Convention (Sub-artifact A).
# Downstream modules reference these as: module.<name>.outputs["<key>"]
output "outputs" {
  description = "Named output map. Each entry is a materialized Iceberg table reference."
  value = {
    for k, v in var.output_definitions : k => {
      table_path    = "s3://${var.bucket}/${var.pipeline_name}/${var.name}/${k}/"
      catalog_db    = local.catalog_db
      catalog_table = "${replace(var.name, "-", "_")}__${k}"
      schema        = v.schema
    }
  }
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function (compute = lambda only). Empty string for other compute targets."
  value       = var.compute == "lambda" ? aws_lambda_function.runner[0].arn : ""
}
