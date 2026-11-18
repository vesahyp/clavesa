#!/usr/bin/env bash
# make hosted-viewer EMAIL=<address> WS=<workspace>|all [REMOVE=1]: make an
# existing hosted UI user a viewer of one workspace, or of all (ADR-035),
# or with REMOVE=1 take that away. The user is found by a verified email
# (exactly one) and granted by their id, so an unverified or changed email
# can never receive the grant. A removal also signs the user out
# everywhere (refresh tokens revoked); the current id token still holds
# the old groups for up to an hour. Sends no mail. No address is written
# to the repository.
set -euo pipefail

EMAIL="${1:?usage: viewer.sh <email> <workspace|all> [remove]}"
WS="${2:?usage: viewer.sh <email> <workspace|all> [remove]}"
REMOVE="${3:-}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export AWS_PROFILE="${AWS_PROFILE:-personal}"

POOL="$(terraform -chdir="$ROOT/hosted/infra" output -raw user_pool_id)"
REGION="$(terraform -chdir="$ROOT/hosted/infra" output -raw region)"
GROUP="ws-$WS"
[ "$WS" = "all" ] && GROUP="all"

# Exactly one user with this email, and it is verified.
USERS="$(aws cognito-idp list-users --region "$REGION" --user-pool-id "$POOL" \
  --filter "email = \"$EMAIL\"" --output json)"
ID="$(printf '%s' "$USERS" | python3 -c '
import json, sys
users = json.load(sys.stdin)["Users"]
ok = [u for u in users if any(a["Name"] == "email_verified" and a["Value"] == "true" for a in u.get("Attributes", []))]
if len(users) != 1 or len(ok) != 1:
    sys.exit("want exactly one user with this email, verified; found %d (%d verified)" % (len(users), len(ok)))
print(ok[0]["Username"])
')"

if [ -n "$REMOVE" ]; then
  aws cognito-idp admin-remove-user-from-group --region "$REGION" \
    --user-pool-id "$POOL" --username "$ID" --group-name "$GROUP"
  aws cognito-idp admin-user-global-sign-out --region "$REGION" \
    --user-pool-id "$POOL" --username "$ID"
else
  aws cognito-idp admin-add-user-to-group --region "$REGION" \
    --user-pool-id "$POOL" --username "$ID" --group-name "$GROUP"
fi
aws cognito-idp admin-list-groups-for-user --region "$REGION" \
  --user-pool-id "$POOL" --username "$ID" \
  --query "Groups[].GroupName" --output text
