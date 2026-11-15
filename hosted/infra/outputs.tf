# Read by hosted/scripts/deploy-spa.sh (terraform output -json). None of
# these is a secret; the allowlist and the check user's password are
# never outputs.

output "ui_url" {
  value = "https://${var.ui_domain}"
}

output "spa_bucket" {
  value = aws_s3_bucket.spa.bucket
}

output "distribution_id" {
  value = aws_cloudfront_distribution.ui.id
}

output "region" {
  value = var.region
}

output "user_pool_id" {
  value = aws_cognito_user_pool.ui.id
}

output "spa_client_id" {
  value = aws_cognito_user_pool_client.spa.id
}

output "check_client_id" {
  value = aws_cognito_user_pool_client.check.id
}

output "check_user_parameter" {
  value = var.check_user_email == "" ? "" : aws_ssm_parameter.check_user[0].name
}

output "cognito_domain" {
  value = "${aws_cognito_user_pool_domain.auth.domain}.auth.${var.region}.amazoncognito.com"
}

output "google_enabled" {
  value = local.google_enabled
}

output "google_redirect_uri" {
  description = "The redirect URI to register on the Google OAuth client."
  value       = "https://${aws_cognito_user_pool_domain.auth.domain}.auth.${var.region}.amazoncognito.com/oauth2/idpresponse"
}

output "mail_from" {
  value = "no-reply@${var.zone_domain}"
}

output "api_endpoint" {
  value = aws_apigatewayv2_api.ui.api_endpoint
}

output "api_function" {
  value = aws_lambda_function.api.function_name
}

output "data_bucket" {
  value = aws_s3_bucket.data.bucket
}

output "reader_principal" {
  description = "The role every workspace's reader role trusts (the well-known reader_principal)."
  value       = aws_iam_role.api.arn
}

output "event_bus_arn" {
  value = aws_cloudwatch_event_bus.ui.arn
}

output "publisher_role_arn" {
  value = aws_iam_role.publisher.arn
}
