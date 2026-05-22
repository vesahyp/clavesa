# Destination modules are terminal — no downstream-consumable outputs.
output "outputs" {
  description = "Always empty. Destination modules are terminal — they produce no downstream-consumable outputs."
  value       = {}
}

output "target_path" {
  description = "Final S3 path where the upstream transform writes. Used by the orchestration emitter to route the transform's runner Lambda output."
  value       = local.target_path
}

output "metadata" {
  description = "Destination metadata for observability. Not consumed by other pipeline nodes."
  value = {
    target_path = local.target_path
    format      = var.format
    write_mode  = var.write_mode
    compression = local.effective_compression
  }
}
