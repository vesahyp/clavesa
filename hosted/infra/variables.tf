########################################################################
# Every name comes from here (ADR-032 decision 1: hosted is a feature any
# user deploys into their own account). The defaults are the first
# deploy, ui.clavesa.dev in account 699166197771. Domains and bucket
# names are fine as defaults; no email address of a person is, so none is
# here: who may sign in lives in the SSM parameter var.config_parameter.
########################################################################

variable "aws_profile" {
  description = "AWS named profile. null = the default credential chain."
  type        = string
  default     = "personal"
}

variable "region" {
  description = "Region for Cognito, SES, the bucket and the Lambda. Cognito and the SES identity must share it."
  type        = string
  default     = "eu-north-1"
}

variable "zone_domain" {
  description = "The existing Route 53 public zone. The SES identity is this domain; mail comes from no-reply@ it."
  type        = string
  default     = "clavesa.dev"
}

variable "ui_domain" {
  description = "Host name of the hosted UI, inside zone_domain."
  type        = string
  default     = "ui.clavesa.dev"
}

variable "mail_from_subdomain" {
  description = "Custom MAIL FROM subdomain label (SPF alignment). Must not exist in the zone yet."
  type        = string
  default     = "mail"
}

variable "pool_name" {
  description = "Cognito user pool name. Also the prefix of the other resource names."
  type        = string
  default     = "clavesa-ui"
}

variable "cognito_domain_prefix" {
  description = "Cognito prefix domain (region-unique). null = <pool_name>-<account id>. Changing it means changing the Google OAuth redirect URI."
  type        = string
  default     = null
}

variable "spa_bucket_name" {
  description = "Globally unique S3 bucket for the SPA build. null = <pool_name>-spa-<account id>."
  type        = string
  default     = null
}

variable "config_parameter" {
  description = "SSM SecureString with JSON {\"allowlist\":[...],\"registration_open\":false}. Created by hand, see hosted/README.md. Empty = no config: sign-up is shut for everyone."
  type        = string
  default     = "/clavesa-ui/config"
}

variable "google_parameter" {
  description = "SSM SecureString with JSON {\"client_id\":\"...\",\"client_secret\":\"...\"} for the Google OAuth client. While it does not exist there is no Google IdP and sign-in is by emailed code only."
  type        = string
  default     = "/clavesa-ui/google"
}

variable "check_user_parameter" {
  description = "SSM SecureString this stack writes: JSON {\"username\",\"password\"} of the scripted check user."
  type        = string
  default     = "/clavesa-ui/check-user"
}

variable "check_user_email" {
  description = "Username of the scripted phone check user (ADR-032 slice 9). An address on your own domain that never receives mail; it signs in only through the admin password flow on the check client. Empty = no check user."
  type        = string
  default     = "ui-check@clavesa.dev"
}

variable "data_bucket_name" {
  description = "Globally unique private S3 bucket for the stored summaries (summaries/<name>.json). null = <pool_name>-data-<account id>."
  type        = string
  default     = null
}

variable "logs_bucket_name" {
  description = "Existing S3 bucket for the distribution's standard access logs (legacy, gzipped TSV), the input of the traffic pipeline. It must have ACLs enabled (BucketOwnerPreferred): legacy logging grants the log-delivery account by ACL. Its lifecycle is the bucket owner's. Empty = no access logs."
  type        = string
  default     = "clavesa-cloudfront-logs"
}

variable "logs_prefix" {
  description = "Key prefix of the access logs in logs_bucket_name."
  type        = string
  default     = "ui/"
}

variable "publisher_principals" {
  description = "IAM principals that may assume the publisher role to write local-warehouse summaries: the Mac's existing identity. Not an email address."
  type        = list(string)
  default     = ["arn:aws:iam::699166197771:user/Vesa"]
}

variable "alarm_topic_arn" {
  description = "SNS topic the Lambda error and refresh-failure alarms publish to. Empty = no alarm action."
  type        = string
  default     = "arn:aws:sns:eu-north-1:699166197771:jeeves-push"
}

variable "daily_refresh_schedule" {
  description = "EventBridge schedule of the daily refresh of every cloud workspace's summary (UTC)."
  type        = string
  default     = "cron(0 5 * * ? *)"
}
