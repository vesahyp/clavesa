# Hosted clavesa UI

A read-only copy of `clavesa ui` that you deploy into your own AWS account
and open on your phone: the same React pages, behind a sign-in with an
emailed code (ADR-032, `docs/decisions/032-hosted-read-only-ui.md`). There
is no clavesa-run service: each deploy is one account's own copy. The
first deploy is `https://ui.clavesa.dev`.

What `hosted/infra/` creates (all names are variables in
`infra/variables.tf`; the defaults are the ui.clavesa.dev deploy):

- A Cognito user pool (ESSENTIALS tier). Sign-in is the emailed one-time
  code (`EMAIL_OTP`); Google is the alternative once you add an OAuth
  client. No client a person uses accepts a password.
- A pre-signup Lambda that admits only the addresses in your allowlist
  (an SSM parameter, never the repo) and links a Google sign-in to the
  existing account with the same address.
- An SES domain identity for your zone (DKIM, custom MAIL FROM, `_dmarc`),
  so the codes come from `no-reply@<your domain>`.
- A private S3 bucket and a CloudFront distribution for the SPA at
  `https://<ui_domain>`, with an ACM certificate, security headers and
  `noindex`.
- A check user and a second app client for a scripted phone check
  (admin password flow only, which needs IAM credentials). The same
  Lambda, as the pre-authentication trigger, refuses the check user on
  every other client.

- The hosted API (`infra/api.tf`): one Lambda, `cmd/clavesa-hosted`
  (arm64, `provided.al2023`), behind an HTTP API with a Cognito JWT
  authorizer, and a private data bucket `<pool_name>-data-<account>` for
  the stored summaries. CloudFront sends `/api/*`, `/w/*/api/*` and
  `/.well-known/*` to it, uncached, with the `Authorization` header.
- The refresh (`infra/events.tf`): an event bus `<pool_name>` that
  accepts events only from the registry's accounts, a rule on it to the
  Lambda, a daily schedule (05:00 UTC), and the publisher role.
- Alarms (`infra/alarms.tf`) to the `jeeves-push` SNS topic.

## The API

Every route needs a Cognito id token (`Authorization: Bearer <id
token>`; the SPA sends it) except the well-known document. The Lambda
answers only GET and HEAD: every other method gets 405, whatever the
route (`ui.ReadOnlyGuard`), and each workspace's reader role can only
read (ADR-032 section 2).

| Path | What |
|------|------|
| `GET /.well-known/clavesa.json` | Public. `reader_principal` (the Lambda's role), `event_bus_arn`, `publisher_role_arn`, `data_bucket`, `region`. `clavesa workspace hosted enable --url` reads it. |
| `GET /api/workspaces` | The registry, each entry with its stored summary (`summary`, null until one is stored). Reads only the data bucket: no Athena. |
| `GET /w/<name>/api/workspace/summary` | The stored summary. `?live=1` computes it live through the reader role (Athena, some seconds); so does a cloud workspace with no stored summary yet. |
| `GET /w/<name>/api/...` | The same routes as `clavesa ui` (`ui.BuildWorkspaceMux`), read live through the workspace's reader role. Cloud workspaces only. |

Anything else is a JSON 404. A cloud workspace is read like this: the
Lambda assumes `clavesa-ui-reader-<name>` (ExternalId = the name, 15
minutes, renewed before expiry), copies `s3://<bucket>/_clavesa/tree/`
to `/tmp/ws/<name>/` (again when `deployed.json` changes), and registers
the assumed credentials for that directory, so every AWS client of the
read paths uses them. Athena runs in the `clavesa-ui-<name>` workgroup
that `deployed.json` names.

Deep links such as `https://<ui_domain>/w/<name>/pipelines` get
`index.html` from a CloudFront Function on the SPA behaviour (a path with
no file extension). There are no custom error responses: they would
apply to the API too and turn its 401 and 404 into HTML.

## The registry: adding a workspace

All signed-in users see all registered workspaces. (The ADR's
per-workspace `viewers` list is not built: there is one viewer.)

`infra/workspaces.tf` lists the workspaces: `name`, `kind` (`cloud` or
`local`), `account`, `region`, and for a cloud workspace
`reader_role_arn` and `bucket` (its pipeline bucket). The list reaches
the Lambda as JSON in its `REGISTRY` variable, opens the bus to the
accounts, and lets the publisher role write the local names.

A cloud workspace joins like this:

1. It needs the S3 Terraform backend (ADR-025).
2. `clavesa workspace hosted enable --url https://<ui_domain>`. It reads
   the Lambda's role and the bus from the well-known document.
   `--reader-principal <ARN>` keeps more principals beside the Lambda's
   (the smoke gate's reader leg assumes the role as the `personal` IAM
   user, so smoke2 has `--reader-principal arn:aws:iam::699166197771:user/Vesa`).
3. `clavesa workspace deploy`: the reader role, the workgroup, the rules
   that forward its run events to the bus, and the tree copy.
4. Add its entry to `infra/workspaces.tf`, then `make hosted-plan` and
   `make hosted-apply`.

The card fills on the next run (the Step Functions status change reaches
the Lambda, which stores `summaries/<name>.json`) or at 05:00 UTC.

A local-warehouse workspace has no reader role: add it with `kind =
"local"`, and publish from the Mac with `clavesa workspace summary
--publish` (it assumes the publisher role, which may write only the
local names in the registry). Publishing at the end of every run is
ADR-032 slice 8.

If the Lambda's role is ever replaced (a destroy and create), every
reader role's trust must be refreshed: `clavesa workspace deploy` in
each workspace.

## Alarms

Two CloudWatch alarms push to the phone through `jeeves-push`
(`var.alarm_topic_arn`): the Lambda's `Errors` metric at 1 or more in 5
minutes (a crash, or an event refresh that failed), and a log metric
filter on `REFRESH_FAILED` lines (`Clavesa/HostedUI RefreshFailures`).
A failed refresh is not retried; the next run or the daily schedule
refreshes again. Logs: `/aws/lambda/<pool_name>-api`, 14 days.

## Deploy your own copy

Needs: Terraform 1.10 or newer, Node 20, the AWS CLI, a Route 53 public
zone for your domain in the account, and SES production access in the
region (a new account is in the SES sandbox and can mail only verified
addresses).

1. Create the access config, a SecureString. It is read at plan time and
   never written to the repo, a plan or an output:

   ```
   aws ssm put-parameter --type SecureString --name /clavesa-ui/config \
     --value '{"allowlist":["you@example.com"],"registration_open":false}'
   ```

   A missing parameter, a missing list or an empty list admits nobody.
   `registration_open: true` admits every address; leave it false.
2. Point the state at your own bucket and set your names, then plan and
   apply. Each step is a make target; `hosted-apply` applies only the
   plan that `hosted-plan` saved:

   ```
   make hosted-init HOSTED_BACKEND_FLAGS="-reconfigure -backend-config=bucket=<state bucket> -backend-config=profile=<profile>"
   make hosted-plan HOSTED_TF_VARS="-var zone_domain=example.com -var ui_domain=ui.example.com -var check_user_email=ui-check@example.com -var aws_profile=<profile>"
   make hosted-apply
   ```

   The first apply waits for SES to see the DKIM records (some minutes).
   `make hosted-plan` builds the Lambda first (`make
   hosted-build-lambda`, Go 1.24, no Docker).
3. Build and upload the SPA: `make hosted-deploy-spa` (`AWS_PROFILE`
   picks the profile, default `personal`).
4. Create your own user. Cognito does not create a user on the first
   code sign-in, so:

   ```
   make hosted-add-user EMAIL=you@example.com
   ```

   It sends no mail. Then open `https://<ui_domain>`, type the address,
   and type the code from the mail.

`make hosted-outputs` prints the pool, the clients, the bucket and the
Google redirect URI.

## Hand steps

- **Google sign-in (optional).** In Google Cloud Console, create an OAuth
  client of type "Web application" with the authorized redirect URI from
  `make hosted-outputs` (`google_redirect_uri`,
  `https://<prefix>.auth.<region>.amazoncognito.com/oauth2/idpresponse`).
  Put its id and secret in SSM:

  ```
  aws ssm put-parameter --type SecureString --name /clavesa-ui/google \
    --value '{"client_id":"...apps.googleusercontent.com","client_secret":"..."}'
  ```

  Then `make hosted-plan`, `make hosted-apply`, `make hosted-deploy-spa`.
  The IdP and the "Continue with Google" button turn on together. The
  first Google sign-in of an address that already has an account links
  the two and asks you to sign in again; the second goes straight in.
- **The first code mail.** Sign in once on the phone with your own
  address, and check that the mail comes from `no-reply@<your domain>`
  and passes DKIM.

## Where things are

- `infra/` Terraform: `auth.tf` (pool, clients, check user, pre-signup,
  Google), `ses.tf`, `spa.tf` (bucket, CloudFront, certificate, DNS),
  `api.tf` (Lambda, HTTP API, data bucket), `events.tf` (bus, rules,
  publisher role), `alarms.tf`, `workspaces.tf` (the registry),
  `config.tf` (the SSM reads).
- `../cmd/clavesa-hosted` and `../internal/hosted` the Lambda.
- `presignup/index.mjs` the pre-signup Lambda.
- `scripts/deploy-spa.sh` and `scripts/render-index.mjs` write the
  hosted `index.html` (`window.__CLAVESA__ = {mode: "hosted", readOnly:
  true, cognito: {...}}`) and upload the build.
- `scripts/add-user.sh` the user that `make hosted-add-user` creates.
- The sign-in screen and the session are in `ui/src/hosted/`; the local
  UI never loads them.
