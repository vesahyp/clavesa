########################################################################
# The hosted API (ADR-032 slice 6): one Lambda (cmd/clavesa-hosted)
# behind an HTTP API with a Cognito JWT authorizer, reached through the
# CloudFront distribution at /api/*, /w/*/api/* and /.well-known/*.
#
#   CloudFront ──> HTTP API ──JWT──> Lambda ──AssumeRole──> clavesa-ui-reader-<ws>
#                                      │
#                                      ├──> s3://<data bucket>/summaries/<ws>.json
#                                      └──> s3://<data bucket>/stats/ui.json (read)
#
# The Lambda zip is hosted/infra/.build/lambda/bootstrap, built by
# make hosted-build-lambda (make hosted-plan runs it first).
########################################################################

locals {
  api_name         = "${var.pool_name}-api"
  data_bucket_name = coalesce(var.data_bucket_name, "${var.pool_name}-data-${local.account_id}")
}

########################################################################
# The data bucket: summaries/<name>.json. Private, not a CloudFront
# origin: only the Lambda reads it, and the publisher role writes the
# local workspaces' names.
########################################################################

resource "aws_s3_bucket" "data" {
  bucket = local.data_bucket_name
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

########################################################################
# The Lambda.
########################################################################

data "archive_file" "api" {
  type        = "zip"
  source_file = "${path.module}/.build/lambda/bootstrap"
  output_path = "${path.module}/.build/api.zip"
}

resource "aws_iam_role" "api" {
  name = local.api_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "api" {
  name = local.api_name
  role = aws_iam_role.api.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.api.arn}:*"
      },
      {
        # Every workspace's reader role; each one's trust policy names
        # this role and an ExternalId of the workspace name.
        Sid      = "AssumeReaders"
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = "arn:aws:iam::*:role/clavesa-ui-reader-*"
      },
      {
        Sid      = "ListData"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.data.arn
      },
      {
        Sid      = "Summaries"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.data.arn}/summaries/*"
      },
      {
        # GET /api/stats: the UI's own traffic rollup, written from the
        # Mac by the site-traffic workspace's traffic pipeline. Read only.
        Sid      = "Stats"
        Effect   = "Allow"
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.data.arn}/stats/*"
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${local.api_name}"
  retention_in_days = 14
}

resource "aws_lambda_function" "api" {
  function_name    = local.api_name
  description      = "Hosted clavesa UI: read-only API and summary refresh (ADR-032)"
  role             = aws_iam_role.api.arn
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  handler          = "bootstrap"
  filename         = data.archive_file.api.output_path
  source_code_hash = data.archive_file.api.output_base64sha256
  memory_size      = 512
  # A cap on how many run at once, so a loop or a leaked token cannot use
  # the account's 1000. A page opens about a dozen calls at once and the
  # live checks reached 54 (2026-11-17), so 30 (review finding 7).
  reserved_concurrent_executions = 30
  # An event refresh waits for the runs_writer (up to about a minute);
  # HTTP requests are cut at 30 s by the HTTP API.
  timeout = 120

  ephemeral_storage { size = 512 }

  environment {
    variables = {
      REGISTRY           = local.registry_json
      DATA_BUCKET        = aws_s3_bucket.data.bucket
      READER_PRINCIPAL   = aws_iam_role.api.arn
      EVENT_BUS_ARN      = aws_cloudwatch_event_bus.ui.arn
      PUBLISHER_ROLE_ARN = aws_iam_role.publisher.arn
      WORK_ROOT          = "/tmp/ws"
      HOME               = "/tmp"
      CLAVESA_TIME_ZONE  = var.time_zone
    }
  }

  depends_on = [aws_cloudwatch_log_group.api, aws_iam_role_policy.api]
}

# An EventBridge refresh that fails is not retried: the alarm reports
# it, and the next event or the daily schedule refreshes again.
resource "aws_lambda_function_event_invoke_config" "api" {
  function_name                = aws_lambda_function.api.function_name
  maximum_retry_attempts       = 0
  maximum_event_age_in_seconds = 3600
}

########################################################################
# The HTTP API. A JWT authorizer on every route except the well-known
# document, so the Lambda never runs for an unsigned request. The
# audience is the SPA client and the scripted check client (slice 9):
# the id token's aud is the client that signed the user in.
########################################################################

resource "aws_apigatewayv2_api" "ui" {
  name          = local.api_name
  protocol_type = "HTTP"
  description   = "Hosted clavesa UI API (ADR-032)"
}

resource "aws_apigatewayv2_authorizer" "cognito" {
  api_id           = aws_apigatewayv2_api.ui.id
  name             = "cognito"
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]

  jwt_configuration {
    issuer   = "https://cognito-idp.${var.region}.amazonaws.com/${aws_cognito_user_pool.ui.id}"
    audience = [aws_cognito_user_pool_client.spa.id, aws_cognito_user_pool_client.check.id]
  }
}

resource "aws_apigatewayv2_integration" "lambda" {
  api_id                 = aws_apigatewayv2_api.ui.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 30000
}

resource "aws_apigatewayv2_route" "well_known" {
  api_id    = aws_apigatewayv2_api.ui.id
  route_key = "GET /.well-known/clavesa.json"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

# Everything else, signed. Unknown paths reach the Lambda too (signed),
# which answers a JSON 404.
resource "aws_apigatewayv2_route" "signed" {
  api_id             = aws_apigatewayv2_api.ui.id
  route_key          = "$default"
  target             = "integrations/${aws_apigatewayv2_integration.lambda.id}"
  authorization_type = "JWT"
  authorizer_id      = aws_apigatewayv2_authorizer.cognito.id
}

# The stage's access log: one line per request, also for requests sent
# straight to the execute-api endpoint, which bypass CloudFront and its
# log. The endpoint stays on because CloudFront reaches the API through
# it (spa.tf origin). An HTTP API access log has no query string
# variable; the Lambda answers 400 to any dir or path outside the
# workspace (ui.ContainPathParams) whatever the log says.
resource "aws_cloudwatch_log_group" "api_access" {
  name              = "/aws/apigateway/${local.api_name}"
  retention_in_days = 14
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.ui.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn
    format = jsonencode({
      requestId = "$context.requestId"
      time      = "$context.requestTime"
      method    = "$context.httpMethod"
      path      = "$context.path"
      status    = "$context.status"
      domain    = "$context.domainName"
      sourceIp  = "$context.identity.sourceIp"
      userAgent = "$context.identity.userAgent"
      user      = "$context.authorizer.claims.email"
      authError = "$context.authorizer.error"
      latencyMs = "$context.responseLatency"
    })
  }

  # One viewer: a low ceiling caps what a leaked token or a loop costs.
  # 10/20 throttled a person tapping through the menu (429s, 2026-11-17:
  # each page opens with about a dozen calls), so 25 a second, burst 50.
  default_route_settings {
    throttling_burst_limit = 50
    throttling_rate_limit  = 25
  }
}

resource "aws_lambda_permission" "api" {
  statement_id  = "AllowHTTPAPI"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.ui.execution_arn}/*/*"
}
