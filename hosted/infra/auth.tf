########################################################################
# Sign-in: Cognito, emailed one-time codes first, Google as the
# alternative, no password anywhere a person can reach (jeeves
# practices/auth.md; copied from keitos/infra/app.tf and auth.tf).
#
#   SPA client   ALLOW_USER_AUTH (EMAIL_OTP) + refresh, code flow + PKCE
#                for Google. No password flow.
#   check client ALLOW_ADMIN_USER_PASSWORD_AUTH + refresh only. The admin
#                flow needs IAM credentials, so only a principal of this
#                account can use the check user's password (slice 9).
########################################################################

resource "aws_cognito_user_pool" "ui" {
  name           = var.pool_name
  user_pool_tier = "ESSENTIALS"

  username_attributes      = ["email"]
  auto_verified_attributes = ["email"]

  # Self-service sign-up stays open at the pool level so Google
  # federation can create users; the pre-signup Lambda is the gate on
  # every self-service path.
  admin_create_user_config {
    allow_admin_create_user_only = false
  }

  # One function, two triggers: the allowlist at sign-up, and at sign-in
  # "the check user only through the check client" (presignup/index.mjs).
  lambda_config {
    pre_sign_up        = aws_lambda_function.presignup.arn
    pre_authentication = aws_lambda_function.presignup.arn
  }

  # EMAIL_OTP is the sign-in. PASSWORD is listed only because Cognito
  # refuses to remove it; the SPA client has no password flow and the
  # sign-in page has no password field.
  sign_in_policy {
    allowed_first_auth_factors = ["EMAIL_OTP", "PASSWORD"]
  }

  email_configuration {
    email_sending_account = "DEVELOPER"
    source_arn            = aws_ses_domain_identity.mail.arn
    from_email_address    = "Clavesa <no-reply@${var.zone_domain}>"
  }

  password_policy {
    minimum_length                   = 12
    require_lowercase                = true
    require_numbers                  = true
    require_symbols                  = false
    require_uppercase                = true
    temporary_password_validity_days = 7
  }

  account_recovery_setting {
    recovery_mechanism {
      name     = "verified_email"
      priority = 1
    }
  }

  # Cognito checks the SES identity when the email configuration is set.
  depends_on = [aws_ses_domain_identity_verification.mail]
}

resource "aws_cognito_user_pool_client" "spa" {
  name         = var.pool_name
  user_pool_id = aws_cognito_user_pool.ui.id

  explicit_auth_flows = [
    "ALLOW_USER_AUTH",
    "ALLOW_REFRESH_TOKEN_AUTH",
  ]

  # Public SPA client; the code flow with PKCE is the Google path.
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_scopes                 = ["openid", "email", "profile"]
  supported_identity_providers = concat(
    local.google_enabled ? ["Google"] : [],
    ["COGNITO"],
  )
  callback_urls = ["https://${var.ui_domain}/"]
  logout_urls   = ["https://${var.ui_domain}/"]

  generate_secret               = false
  prevent_user_existence_errors = "ENABLED"
  enable_token_revocation       = true

  access_token_validity  = 60
  id_token_validity      = 60
  refresh_token_validity = 30
  token_validity_units {
    access_token  = "minutes"
    id_token      = "minutes"
    refresh_token = "days"
  }

  depends_on = [aws_cognito_identity_provider.google]
}

resource "aws_cognito_user_pool_client" "check" {
  name         = "${var.pool_name}-check"
  user_pool_id = aws_cognito_user_pool.ui.id

  explicit_auth_flows = [
    "ALLOW_ADMIN_USER_PASSWORD_AUTH",
    "ALLOW_REFRESH_TOKEN_AUTH",
  ]

  generate_secret               = false
  prevent_user_existence_errors = "ENABLED"

  access_token_validity  = 60
  id_token_validity      = 60
  refresh_token_validity = 1
  token_validity_units {
    access_token  = "minutes"
    id_token      = "minutes"
    refresh_token = "days"
  }
}

########################################################################
# The scripted check user (ADR-032 slice 9). Created confirmed, with no
# mail sent; its password is random and kept in SSM for the check script.
########################################################################

resource "random_password" "check" {
  count   = var.check_user_email == "" ? 0 : 1
  length  = 32
  special = false
}

resource "aws_cognito_user" "check" {
  count = var.check_user_email == "" ? 0 : 1

  user_pool_id   = aws_cognito_user_pool.ui.id
  username       = var.check_user_email
  password       = random_password.check[0].result
  message_action = "SUPPRESS"

  attributes = {
    email          = var.check_user_email
    email_verified = "true"
  }

  lifecycle {
    ignore_changes = [attributes]
  }
}

resource "aws_ssm_parameter" "check_user" {
  count = var.check_user_email == "" ? 0 : 1

  name        = var.check_user_parameter
  description = "Scripted phone check user of the hosted clavesa UI (admin password flow on the check client only)."
  type        = "SecureString"
  value = jsonencode({
    username = var.check_user_email
    password = random_password.check[0].result
  })
}

########################################################################
# Pre-SignUp + Pre-Authentication trigger: allowlist from SSM,
# Google-to-native linking, the check user kept to the check client.
########################################################################

data "archive_file" "presignup" {
  type        = "zip"
  source_file = "${path.module}/../presignup/index.mjs"
  output_path = "${path.module}/.build/presignup.zip"
}

resource "aws_iam_role" "presignup" {
  name = "${var.pool_name}-presignup"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "presignup" {
  name = "${var.pool_name}-presignup"
  role = aws_iam_role.presignup.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.presignup.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "cognito-idp:ListUsers",
          "cognito-idp:AdminLinkProviderForUser",
          "cognito-idp:DescribeUserPoolClient",
        ]
        Resource = aws_cognito_user_pool.ui.arn
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "presignup" {
  name              = "/aws/lambda/${var.pool_name}-presignup"
  retention_in_days = 14
}

resource "aws_lambda_function" "presignup" {
  function_name    = "${var.pool_name}-presignup"
  role             = aws_iam_role.presignup.arn
  runtime          = "nodejs22.x"
  architectures    = ["arm64"]
  handler          = "index.handler"
  filename         = data.archive_file.presignup.output_path
  source_code_hash = data.archive_file.presignup.output_base64sha256
  timeout          = 10

  environment {
    variables = {
      REGISTRATION_OPEN = local.registration_open ? "1" : "0"
      CHECK_USER        = lower(var.check_user_email)
      CHECK_CLIENT_NAME = "${var.pool_name}-check"
      # sensitive(): addresses never show in a plan or an output.
      ALLOWED_EMAILS = sensitive(join(",", local.allowlist))
    }
  }

  depends_on = [aws_cloudwatch_log_group.presignup]
}

resource "aws_lambda_permission" "cognito_presignup" {
  statement_id  = "AllowCognito"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.presignup.function_name
  principal     = "cognito-idp.amazonaws.com"
  source_arn    = aws_cognito_user_pool.ui.arn
}

########################################################################
# Prefix domain (the Google OAuth redirect needs it) and the Google IdP.
#
# The one hand step: an OAuth client in Google Cloud Console with the
# redirect URI https://<prefix>.auth.<region>.amazoncognito.com/oauth2/idpresponse
# (output google_redirect_uri), its id and secret in var.google_parameter,
# then make hosted-plan hosted-apply hosted-deploy-spa.
########################################################################

resource "aws_cognito_user_pool_domain" "auth" {
  domain       = local.cognito_domain_prefix
  user_pool_id = aws_cognito_user_pool.ui.id
}

resource "aws_cognito_identity_provider" "google" {
  count = local.google_enabled ? 1 : 0

  user_pool_id  = aws_cognito_user_pool.ui.id
  provider_name = "Google"
  provider_type = "Google"

  # The last six are AWS's own defaults, declared so plans stay quiet.
  provider_details = {
    client_id        = local.google_client_id
    client_secret    = local.google_client_secret
    authorize_scopes = "openid email profile"

    authorize_url                 = "https://accounts.google.com/o/oauth2/v2/auth"
    token_url                     = "https://www.googleapis.com/oauth2/v4/token"
    oidc_issuer                   = "https://accounts.google.com"
    attributes_url                = "https://people.googleapis.com/v1/people/me?personFields="
    attributes_url_add_attributes = "true"
    token_request_method          = "POST"
  }

  attribute_mapping = {
    email          = "email"
    username       = "sub"
    email_verified = "email_verified"
  }
}
