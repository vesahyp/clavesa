#!/usr/bin/env bash
# Preflight for the synth-verify cloud verification workspace.
#
# Idempotent: re-run every session. Confirms the AWS profile is what we expect,
# creates the synth S3 bucket if missing, and prints the env vars to export.
#
# Usage:
#     source scripts/synth_setup.sh           # sets vars in your shell
#     # or:
#     eval "$(scripts/synth_setup.sh)"        # same, alternate form
#
# Override anything via env before invoking:
#     SYNTH_BUCKET=clavesa-synth-foo scripts/synth_setup.sh
set -euo pipefail

: "${AWS_PROFILE:=personal}"
: "${AWS_REGION:=eu-north-1}"
: "${SYNTH_BUCKET:=clavesa-synth-vk}"
: "${SYNTH_WORKSPACE_DIR:=$HOME/clavesa-workspaces/synth-verify}"
: "${EXPECTED_ACCOUNT:=}"   # set to your AWS account ID to enable the identity sanity check

err() { printf "synth_setup: %s\n" "$*" >&2; }

# 1. Caller identity sanity (only if EXPECTED_ACCOUNT is set).
caller_json=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --output json)
actual_account=$(printf "%s" "$caller_json" | python3 -c "import json,sys;print(json.load(sys.stdin)['Account'])")
if [[ -n "$EXPECTED_ACCOUNT" && "$actual_account" != "$EXPECTED_ACCOUNT" ]]; then
    err "AWS_PROFILE=$AWS_PROFILE resolves to account $actual_account, expected $EXPECTED_ACCOUNT"
    err "If the account changed, update EXPECTED_ACCOUNT or pass it via env."
    exit 1
fi

# 2. Bucket: create if missing.
if aws s3api head-bucket --bucket "$SYNTH_BUCKET" --profile "$AWS_PROFILE" --region "$AWS_REGION" >/dev/null 2>&1; then
    err "bucket s3://$SYNTH_BUCKET exists"
else
    err "creating bucket s3://$SYNTH_BUCKET in $AWS_REGION"
    aws s3api create-bucket \
        --bucket "$SYNTH_BUCKET" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
fi

# 3. EventBridge notifications on the bucket (Gate 2 S3-event trigger needs
#    this; S3 doesn't forward to EventBridge by default). Idempotent.
notif=$(aws s3api get-bucket-notification-configuration --bucket "$SYNTH_BUCKET" \
    --profile "$AWS_PROFILE" --region "$AWS_REGION" --output json 2>/dev/null)
if printf "%s" "$notif" | grep -q "EventBridgeConfiguration"; then
    err "bucket $SYNTH_BUCKET already forwards to EventBridge"
else
    err "enabling EventBridge notifications on $SYNTH_BUCKET"
    aws s3api put-bucket-notification-configuration \
        --bucket "$SYNTH_BUCKET" \
        --profile "$AWS_PROFILE" \
        --region "$AWS_REGION" \
        --notification-configuration '{"EventBridgeConfiguration": {}}'
fi

# 4. Workspace dir (just announce; clavesa workspace init creates it on demand).
if [[ -d "$SYNTH_WORKSPACE_DIR" ]]; then
    err "workspace dir $SYNTH_WORKSPACE_DIR exists"
else
    err "workspace dir $SYNTH_WORKSPACE_DIR not yet created (clavesa workspace init will create it)"
fi

# 4. Emit exports for the calling shell.
cat <<EOF
export AWS_PROFILE=$AWS_PROFILE
export AWS_REGION=$AWS_REGION
export SYNTH_BUCKET=$SYNTH_BUCKET
export SYNTH_WORKSPACE_DIR=$SYNTH_WORKSPACE_DIR
EOF
