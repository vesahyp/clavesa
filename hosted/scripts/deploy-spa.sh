#!/usr/bin/env bash
# make hosted-deploy-spa: the ui/ build (make build-ui → internal/ui/dist)
# with the hosted runtime config, to the SPA bucket of hosted/infra.
#
#   index.html          window.__CLAVESA__ = {mode:"hosted", readOnly:true,
#                       cognito:{...}} from the Terraform outputs, plus a
#                       noindex meta. no-cache.
#   assets/*            Vite-hashed, immutable.
#   robots.txt          Disallow everything.
#
# /api/*, /w/*/api/* and /.well-known/* are the HTTP API (CloudFront
# behaviours in hosted/infra/spa.tf), never objects in this bucket.
#
# Then invalidates /index.html (the only mutable key a browser asks for
# by name). AWS profile: $AWS_PROFILE, default "personal".
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TF="terraform -chdir=$ROOT/hosted/infra"
export AWS_PROFILE="${AWS_PROFILE:-personal}"
DIST="$ROOT/internal/ui/dist"
OUT="$ROOT/hosted/.dist"

test -f "$DIST/index.html" || { echo "no $DIST/index.html: run make build-ui" >&2; exit 1; }

OUTPUTS="$($TF output -json)"
out() { node -e 'const o=JSON.parse(process.argv[1]);process.stdout.write(String(o[process.argv[2]].value))' "$OUTPUTS" "$1"; }

BUCKET="$(out spa_bucket)"
DIST_ID="$(out distribution_id)"
REGION="$(out region)"

rm -rf "$OUT"
mkdir -p "$OUT"
cp -R "$DIST/." "$OUT/"
rm -f "$OUT/.gitkeep"

node "$ROOT/hosted/scripts/render-index.mjs" "$DIST/index.html" "$OUT/index.html" \
  "$REGION" "$(out user_pool_id)" "$(out spa_client_id)" "$(out cognito_domain)" "$(out google_enabled)"
cp "$ROOT/hosted/static/robots.txt" "$OUT/robots.txt"

echo "→ s3://$BUCKET"
aws s3 sync "$OUT" "s3://$BUCKET" --delete --region "$REGION" \
  --exclude "index.html" --exclude "assets/*" \
  --cache-control "public,no-cache"
aws s3 sync "$OUT/assets" "s3://$BUCKET/assets" --delete --region "$REGION" \
  --cache-control "public,max-age=31536000,immutable"
# index.html last, so it never names an asset that is not there yet.
aws s3 cp "$OUT/index.html" "s3://$BUCKET/index.html" --region "$REGION" \
  --content-type "text/html; charset=utf-8" --cache-control "no-cache"

echo "→ invalidating /index.html on $DIST_ID"
aws cloudfront create-invalidation --distribution-id "$DIST_ID" \
  --paths "/index.html" "/" --query "Invalidation.Id" --output text
echo "deployed: $(out ui_url)"
