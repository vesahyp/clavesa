locals {
  effective_compression = var.compression != null ? var.compression : (
    var.format == "parquet" ? "snappy" : "none"
  )

  # Pass-through: the destination is just a declared output path. No compute,
  # no Glue ETL job. The orchestration emitter picks up `target_path` from
  # the destination and routes the upstream transform's runner Lambda to
  # write there directly. The runner already supports arbitrary S3 output
  # paths via its handler() event payload.
  target_path = "s3://${var.bucket}/${var.prefix}/${var.name}"

  tags = merge(var.tags, {
    "clavesa:pipeline" = var.pipeline_name
    "clavesa:node"     = var.name
    "clavesa:type"     = "destination"
  })
}

# Destinations create no resources today — they're a pure path declaration.
# Future additions might include: SNS notification on write completion, S3
# lifecycle rules on the prefix, or cross-account access policies. Each
# would be opt-in via a variable; the default destination stays zero-cost.

# Keep var.input referenced so terraform doesn't drop the dependency edge
# the HCL parser needs to construct the pipeline graph.
locals {
  _input_dependency = var.input.table_path
}
