#!/usr/bin/env bash
# make hosted-add-user EMAIL=<address>: let one person sign in to the
# hosted UI with emailed codes.
#
# Cognito's EMAIL_OTP sign-in (USER_AUTH) does not create a user: the
# user must exist and be CONFIRMED first. This creates it with no mail
# sent now (SUPPRESS), the email marked verified, and a random permanent
# password that nobody knows and no client of the SPA accepts; the
# password only moves the account out of FORCE_CHANGE_PASSWORD so the
# code sign-in works. The address also belongs in the allowlist of the
# config parameter if it should be able to sign in with Google.
#
# Sends no mail. The first mail is the code, when the person signs in.
set -euo pipefail

EMAIL="${1:?usage: add-user.sh <email>}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export AWS_PROFILE="${AWS_PROFILE:-personal}"

POOL="$(terraform -chdir="$ROOT/hosted/infra" output -raw user_pool_id)"
REGION="$(terraform -chdir="$ROOT/hosted/infra" output -raw region)"

aws cognito-idp admin-create-user --region "$REGION" \
  --user-pool-id "$POOL" --username "$EMAIL" \
  --message-action SUPPRESS \
  --user-attributes Name=email,Value="$EMAIL" Name=email_verified,Value=true \
  --query "User.UserStatus" --output text

# 32 random characters plus one of each class the password policy wants.
PW="$(openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | cut -c1-32)Aa1"
aws cognito-idp admin-set-user-password --region "$REGION" \
  --user-pool-id "$POOL" --username "$EMAIL" --password "$PW" --permanent

aws cognito-idp admin-get-user --region "$REGION" \
  --user-pool-id "$POOL" --username "$EMAIL" \
  --query "UserStatus" --output text
