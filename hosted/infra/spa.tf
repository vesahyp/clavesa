########################################################################
# The SPA at https://<ui_domain>: the same ui/ build as `clavesa ui`.
#
#   S3 (private) ── OAC ──> CloudFront <── ACM cert (us-east-1)
#                              ▲
#                     Route 53 alias A/AAAA <ui_domain>
#
# /api/*, /w/*/api/* and /.well-known/* go to the HTTP API (api.tf);
# everything else is the bucket, with extensionless paths rewritten to
# /index.html by a CloudFront Function.
#
# Copied from site/infra/main.tf. index.html is no-cache and the hashed
# /assets/* are immutable (Cache-Control set per object by
# hosted/scripts/deploy-spa.sh; the cache policy follows the origin).
# No index: robots.txt disallows everything and every response carries
# X-Robots-Tag: noindex.
########################################################################

locals {
  # CloudFront's fixed hosted-zone id (the same for every distribution).
  cloudfront_zone_id = "Z2FDTNDATAQYW2"
  spa_origin_id      = "s3-${var.pool_name}-spa"
  api_origin_id      = "api-${var.pool_name}"
}

resource "aws_s3_bucket" "spa" {
  bucket = local.spa_bucket_name
}

resource "aws_s3_bucket_public_access_block" "spa" {
  bucket                  = aws_s3_bucket.spa.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "spa" {
  bucket = aws_s3_bucket.spa.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "spa" {
  bucket = aws_s3_bucket.spa.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_policy" "spa" {
  bucket = aws_s3_bucket.spa.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowCloudFrontOAC"
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.spa.arn}/*"
      Condition = {
        StringEquals = {
          "AWS:SourceArn" = aws_cloudfront_distribution.ui.arn
        }
      }
    }]
  })
}

########################################################################
# ACM (us-east-1), DNS-validated in the existing zone.
########################################################################

resource "aws_acm_certificate" "ui" {
  provider          = aws.us_east_1
  domain_name       = var.ui_domain
  validation_method = "DNS"

  lifecycle { create_before_destroy = true }
}

resource "aws_route53_record" "ui_cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.ui.domain_validation_options :
    dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      record = dvo.resource_record_value
    }
  }

  zone_id = data.aws_route53_zone.primary.zone_id
  name    = each.value.name
  type    = each.value.type
  records = [each.value.record]
  ttl     = 60
}

resource "aws_acm_certificate_validation" "ui" {
  provider                = aws.us_east_1
  certificate_arn         = aws_acm_certificate.ui.arn
  validation_record_fqdns = [for r in aws_route53_record.ui_cert_validation : r.fqdn]
}

########################################################################
# CloudFront.
########################################################################

resource "aws_cloudfront_origin_access_control" "spa" {
  name                              = "${var.pool_name}-spa-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_response_headers_policy" "ui" {
  name    = "${var.pool_name}-headers"
  comment = "Security headers and noindex for the hosted clavesa UI"

  security_headers_config {
    strict_transport_security {
      access_control_max_age_sec = 63072000
      include_subdomains         = true
      override                   = true
    }
    content_type_options {
      override = true
    }
    frame_options {
      frame_option = "DENY"
      override     = true
    }
    referrer_policy {
      referrer_policy = "strict-origin"
      override        = true
    }
  }

  custom_headers_config {
    items {
      header   = "X-Robots-Tag"
      value    = "noindex, nofollow"
      override = true
    }
  }
}

# Extensionless paths (/, /w/smoke2/pipelines, ...) are the SPA's own
# routes: serve /index.html. Files (/assets/x.js, /robots.txt) pass.
resource "aws_cloudfront_function" "spa_routes" {
  name    = "${var.pool_name}-spa-routes"
  runtime = "cloudfront-js-2.0"
  comment = "SPA deep links: extensionless paths -> /index.html"
  publish = true
  code    = <<-EOT
    function handler(event) {
      var req = event.request;
      var last = req.uri.substring(req.uri.lastIndexOf("/") + 1);
      if (last.indexOf(".") === -1) {
        req.uri = "/index.html";
      }
      return req;
    }
  EOT
}

# tracker.js sends its beacons with navigator.sendBeacon, which is a POST
# to /t.gif. The S3 behaviour allows only GET and HEAD, so CloudFront
# answered every beacon 403 (a console error on every page). This
# function answers any non-GET beacon with 204 at the edge: the request
# and its query string still land in the access log, which is all the
# traffic pipeline reads, and nothing reaches the bucket. The Image
# fallback (GET) still gets the no-store pixel from S3.
resource "aws_cloudfront_function" "beacon" {
  name    = "${var.pool_name}-beacon"
  runtime = "cloudfront-js-2.0"
  comment = "tracker beacons: non-GET /t.gif -> 204 at the edge"
  publish = true
  code    = <<-EOT
    function handler(event) {
      var m = event.request.method;
      if (m !== "GET" && m !== "HEAD") {
        return { statusCode: 204, statusDescription: "No Content",
                 headers: { "cache-control": { value: "no-store" } } };
      }
      return event.request;
    }
  EOT
}

# No caching (every TTL 0 but the 1 s maximum that lets the
# Authorization header be part of the key, which is what forwards it).
resource "aws_cloudfront_cache_policy" "api" {
  name        = "${var.pool_name}-api"
  comment     = "Hosted clavesa UI API: not cached, Authorization forwarded"
  min_ttl     = 0
  default_ttl = 0
  max_ttl     = 1

  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
    headers_config {
      header_behavior = "whitelist"
      headers { items = ["Authorization"] }
    }
    query_strings_config { query_string_behavior = "all" }
    cookies_config { cookie_behavior = "none" }
  }
}

resource "aws_cloudfront_distribution" "ui" {
  enabled             = true
  is_ipv6_enabled     = true
  default_root_object = "index.html"
  price_class         = "PriceClass_100"
  comment             = "clavesa hosted UI (${var.ui_domain})"
  aliases             = [var.ui_domain]

  # Legacy standard access logs, gzipped TSV, as site/infra writes them:
  # the /t.gif beacons of tracker.js ride in cs-uri-query, and the
  # site-traffic workspace's traffic pipeline reads the prefix as its
  # second host. The bucket and its 90-day expiry are site/infra's.
  dynamic "logging_config" {
    for_each = var.logs_bucket_name == "" ? [] : [1]
    content {
      bucket          = "${var.logs_bucket_name}.s3.amazonaws.com"
      prefix          = var.logs_prefix
      include_cookies = false
    }
  }

  origin {
    domain_name              = aws_s3_bucket.spa.bucket_regional_domain_name
    origin_id                = local.spa_origin_id
    origin_access_control_id = aws_cloudfront_origin_access_control.spa.id
  }

  # The HTTP API (api.tf). Same origin as the SPA, so no CORS.
  origin {
    domain_name = replace(aws_apigatewayv2_api.ui.api_endpoint, "https://", "")
    origin_id   = local.api_origin_id

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  # The API paths go to the HTTP API, uncached, with the Authorization
  # header and every query string. All methods are allowed through so
  # that a write gets the server's 405 (read-only guard), not a
  # CloudFront 403.
  dynamic "ordered_cache_behavior" {
    for_each = ["/api/*", "/w/*/api/*", "/.well-known/*"]
    content {
      path_pattern               = ordered_cache_behavior.value
      target_origin_id           = local.api_origin_id
      viewer_protocol_policy     = "https-only"
      allowed_methods            = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
      cached_methods             = ["GET", "HEAD"]
      compress                   = true
      cache_policy_id            = aws_cloudfront_cache_policy.api.id
      origin_request_policy_id   = "b689b0a8-53d0-40ab-baf2-68738e2966ac" # Managed-AllViewerExceptHostHeader
      response_headers_policy_id = aws_cloudfront_response_headers_policy.ui.id
    }
  }

  # Tracker beacons: every method allowed so sendBeacon's POST reaches the
  # beacon function (above) instead of a CloudFront 403. Never cached.
  ordered_cache_behavior {
    path_pattern               = "/t.gif"
    target_origin_id           = local.spa_origin_id
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods             = ["GET", "HEAD"]
    compress                   = false
    cache_policy_id            = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad" # Managed-CachingDisabled
    response_headers_policy_id = aws_cloudfront_response_headers_policy.ui.id

    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.beacon.arn
    }
  }

  # SPA routing: a path with no file extension is a client-side route
  # and gets /index.html (CloudFront Function below). Not custom error
  # responses: those apply to every origin and would turn the API's
  # 401, 403 and 404 into index.html.
  default_cache_behavior {
    target_origin_id           = local.spa_origin_id
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD"]
    cached_methods             = ["GET", "HEAD"]
    compress                   = true
    cache_policy_id            = "658327ea-f89d-4fab-a63d-7e88639e58f6" # Managed-CachingOptimized
    response_headers_policy_id = aws_cloudfront_response_headers_policy.ui.id

    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.spa_routes.arn
    }
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.ui.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}

resource "aws_route53_record" "ui_a" {
  zone_id = data.aws_route53_zone.primary.zone_id
  name    = var.ui_domain
  type    = "A"
  alias {
    name                   = aws_cloudfront_distribution.ui.domain_name
    zone_id                = local.cloudfront_zone_id
    evaluate_target_health = false
  }
}

resource "aws_route53_record" "ui_aaaa" {
  zone_id = data.aws_route53_zone.primary.zone_id
  name    = var.ui_domain
  type    = "AAAA"
  alias {
    name                   = aws_cloudfront_distribution.ui.domain_name
    zone_id                = local.cloudfront_zone_id
    evaluate_target_health = false
  }
}
