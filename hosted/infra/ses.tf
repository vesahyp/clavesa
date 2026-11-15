########################################################################
# SES: the sender of the sign-in codes, no-reply@<zone_domain>.
# Cognito's default sender caps at 50 a day and often lands nowhere
# (jeeves practices/auth.md). DKIM verifies the domain, the custom MAIL
# FROM aligns SPF, _dmarc completes the three. Copied from keitos ses.tf.
#
# The email.cognito-idp.amazonaws.com service-linked role is
# account-global and already exists in 699166197771 (owned by
# omatienoo's Terraform), so it is not declared here. A fresh account
# gets it from Cognito the first time a pool uses DEVELOPER mail.
#
# A new account starts in the SES sandbox: request production access in
# var.region before inviting anyone (hosted/README.md).
########################################################################

data "aws_route53_zone" "primary" {
  name         = "${var.zone_domain}."
  private_zone = false
}

locals {
  mail_from_domain = "${var.mail_from_subdomain}.${var.zone_domain}"
}

resource "aws_ses_domain_identity" "mail" {
  domain = var.zone_domain
}

resource "aws_ses_domain_dkim" "mail" {
  domain = aws_ses_domain_identity.mail.domain
}

resource "aws_route53_record" "ses_dkim" {
  count   = 3
  zone_id = data.aws_route53_zone.primary.zone_id
  name    = "${aws_ses_domain_dkim.mail.dkim_tokens[count.index]}._domainkey.${var.zone_domain}"
  type    = "CNAME"
  ttl     = 600
  records = ["${aws_ses_domain_dkim.mail.dkim_tokens[count.index]}.dkim.amazonses.com"]
}

# The domain counts as verified once SES sees the DKIM records. Cognito
# refuses an email configuration on an unverified identity, so the pool
# waits for this.
resource "aws_ses_domain_identity_verification" "mail" {
  domain     = aws_ses_domain_identity.mail.domain
  depends_on = [aws_route53_record.ses_dkim]

  timeouts {
    create = "30m"
  }
}

resource "aws_ses_domain_mail_from" "mail" {
  domain           = aws_ses_domain_identity.mail.domain
  mail_from_domain = local.mail_from_domain
}

resource "aws_route53_record" "ses_mail_from_mx" {
  zone_id = data.aws_route53_zone.primary.zone_id
  name    = local.mail_from_domain
  type    = "MX"
  ttl     = 600
  records = ["10 feedback-smtp.${var.region}.amazonses.com"]
}

resource "aws_route53_record" "ses_mail_from_spf" {
  zone_id = data.aws_route53_zone.primary.zone_id
  name    = local.mail_from_domain
  type    = "TXT"
  ttl     = 600
  records = ["v=spf1 include:amazonses.com -all"]
}

# p=none observes without blocking; no rua, so no report address.
resource "aws_route53_record" "dmarc" {
  zone_id = data.aws_route53_zone.primary.zone_id
  name    = "_dmarc.${var.zone_domain}"
  type    = "TXT"
  ttl     = 600
  records = ["v=DMARC1; p=none;"]
}
