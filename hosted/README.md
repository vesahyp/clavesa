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
- Access logs of the distribution (legacy standard logs, gzipped TSV,
  no cookies) to an existing bucket, `logs_bucket_name` under
  `logs_prefix` (default `clavesa-cloudfront-logs`, `ui/`). The bucket
  must have ACLs enabled, and its lifecycle is its owner's: for
  ui.clavesa.dev that is `site/infra`, which expires `ui/` after 90
  days. Set `logs_bucket_name = ""` for no logs.

## The API

Every route needs a Cognito id token (`Authorization: Bearer <id
token>`; the SPA sends it) except the well-known document. The Lambda
answers only GET and HEAD: every other method gets 405, whatever the
route (`ui.ReadOnlyGuard`), and each workspace's reader role can only
read (ADR-032 section 2).

| Path | What |
|------|------|
| `GET /.well-known/clavesa.json` | Public. `reader_principal` (the Lambda's role), `event_bus_arn`, `publisher_role_arn`, `data_bucket`, `region`. `clavesa workspace hosted enable --url` reads it. |
| `GET /api/stats` | The UI's own traffic rollup, `s3://<data bucket>/stats/ui.json` as stored (`[]` until one is). See "Tracking and errors". |
| `GET /api/workspaces` | The registry, each entry with its stored summary (`summary`, null until one is stored). Reads only the data bucket: no Athena. A local workspace's entry also has `summary_stale` (true when its summary is older than its expected interval plus a quarter of it) and `summary_interval_seconds`. |
| `GET /w/<name>/api/workspace/summary` | The stored summary. `?live=1` computes it live through the reader role (Athena, some seconds); so does a cloud workspace with no stored summary yet. A local workspace has only the stored one. |
| `GET /w/<name>/api/...` | The same routes as `clavesa ui` (`ui.BuildWorkspaceMux`). A cloud workspace is read live through its reader role; a local workspace is served from its stored summary (see "Adding a local workspace"). |

Anything else is a JSON 404. A `dir` or `path` query parameter that
leaves the workspace (an absolute path outside it, `..`, or a symlink
out) answers 400 on every route (`ui.ContainPathParams`). A cloud workspace is read like this: the
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
`reader_role_arn` and `bucket` (its pipeline bucket). Set
`personal_data = true` when the workspace's raw data holds personal data
(web-traffic: visitor IPs): the Lambda then answers 403 to raw row reads
of it (`/data/source`, `/preview/*`, `/sources/<name>/preview`, and
`/data/table` and column statistics of any database that is not gold).
Its runs, logs, catalog and gold tables still show. The list reaches
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

### Adding a local workspace

A local-warehouse workspace (its runs are on a Mac) has no reader role
and nothing to deploy. Its Mac pushes the summary instead:

1. Add `{ name = "<name>", kind = "local" }` to `infra/workspaces.tf`
   (the name in its `clavesa.json`), then `make hosted-plan` and `make
   hosted-apply`. That lets the publisher role write
   `summaries/<name>.json` and nothing else.
2. On the Mac: `clavesa workspace hosted enable --url https://<ui_domain>
   --workspace <dir>`. It records the URL, the publisher role, the data
   bucket and its region from the well-known document in `clavesa.json`.
   No `hosted.tf`, no reader principals.
3. From then on every local `clavesa pipeline run` in that workspace,
   failed or not, ends by publishing the summary through the publisher
   role, with the Mac's own AWS identity (the profile the workspace
   uses; no new key). Best effort: a failed publish prints one line on
   stderr (`Hosted: summary not published ...`) and never changes the
   run's exit code or its output. `--no-publish` or
   `CLAVESA_NO_PUBLISH=1` turns it off for a run. `clavesa workspace
   summary --publish` publishes by hand.

The published file carries a drill-down beside the summary
(`drill`): the workspace's `clavesa.json` and pipeline `.tf` files, the
newest 50 runs of each pipeline, the node rows of the newest 10 runs, the
newest 20 run-timing records, and the last 50 log lines of each failed
run among the newest 10. No path on the Mac, no state, no `.env` files.
About 100 to 160 KB for the site-traffic pipelines. The Lambda writes
that tree to `/tmp/ws/<name>/` and serves it with the same handlers as
any workspace, with the runs read from the drill, so the Pipeline and
Run pages draw from it with no live call. Only those pages' routes
answer; table state, catalog tables, lineage, backfills and the rest
are a JSON 404 ("not kept for local workspaces"), and the pages skip
them (`GET /workspace` says `summary_only`). An older run says that node
timings are kept for the 10 newest runs only.

The card turns amber and says "No summary from the Mac since <time>"
when the stored summary is older than the shortest pipeline interval
(one day when no pipeline has a `trigger_schedule`) plus a quarter of
it: the Mac did not run or could not publish.

To see what the Lambda serves without deploying: put published
summaries in a directory as `<name>.json` and run `make hosted-local
SUMMARIES=<dir>` (the API over those files with the SPA in front, no
sign-in), then `BASE_URL=http://127.0.0.1:8090 WS_NAME=<name>
PIPELINE=<dir> make ui-phone-shots`.

If the Lambda's role is ever replaced (a destroy and create), every
reader role's trust must be refreshed: `clavesa workspace deploy` in
each workspace.

## Tracking and errors

The hosted UI tracks its own use and its client errors with the
canonical tracker (`web-tracker/tracker.js`, ADR-032 section 4). Only
the hosted `index.html` loads it: `scripts/render-index.mjs` adds
`/tracker-config.js` and `/tracker.js`, and `make hosted-deploy-spa`
uploads `web-tracker/tracker.js` byte for byte (no-cache),
`static/tracker-config.js` (no-cache) and `static/t.gif` (`no-store`).
The local `clavesa ui` serves its own `index.html` and sends nothing.

- Every event is a `/t.gif` beacon; the data rides in the query string
  and lands in the distribution's access log under `ui/`. `spa: true`
  counts each client-side route as a page view.
- `sanitize` (`static/tracker-config.js`) runs over the path and every
  value: execution ARNs and run ids become `:run`, JWTs and other long
  token strings `:token`, email addresses `:email`. Workspace names
  stay. The tracker never reads the session in sessionStorage, and no
  beacon says who is signed in.
- Errors: tracker.js reports uncaught errors (`type=js`) and rejections
  (`type=promise`); the top-level boundary (`ui/src/hosted/
  CrashBoundary.tsx`) reports render crashes (`type=boundary`, msg cut
  to 200 characters) and shows a reload screen. `?__throw=1` throws one
  test error, in hosted mode only.
- The rollup: the `traffic` pipeline of the site-traffic workspace
  (`site/analytics/clavesa`, nightly on the Mac) reads `ui/` as its
  second host and writes `stats/ui.json` to the data bucket, one row per
  Helsinki day: sessions, page views, page views per path, sessions per
  device, LCP/INP/CLS p75, errors with the list of distinct msg + path +
  type, and bot counts. Bots and headless check runs are left out of the
  human numbers.
- `/stats` in the signed-in app (beside Workspaces; "Stats" in the
  bottom nav on a phone) draws it: sessions, page views, top pages,
  devices, vitals and errors per day as charts, plus the distinct errors
  as a list to act on. Window 7, 30 or 90 days.
- `make errors` prints the last 7 days of errors from `stats/ui.json`,
  one line per msg + path + type with its count (`ERRORS_DAYS=30` for
  more).
- `make hosted-tracking-check` is the proof: it serves the hosted build
  locally and sees the `e=error&type=boundary` beacon leave the page with
  a sanitized msg, sees no beacon carry the session token or email, and
  sees the local build load no tracker. `LIVE_URL=https://ui.clavesa.dev`
  runs the error check against the deployed site.

## Live check

`make hosted-live-check` opens the deployed site on an emulated iPhone 16
as the scripted check user and walks five pages: Workspaces, the Workspace
page of `WS` (default smoke2), its first pipeline, its newest run, and
`/stats`. It reads `/clavesa-ui/check-user` from SSM at run time with the
AWS CLI (`PROFILE`, default `personal`), takes the pool and the check
client from `terraform output` (run `make hosted-init` once), signs in with
`admin-initiate-auth` and puts the session in the page as `auth.ts` stores
it. It never prints the password or a token. Per page: no console error,
no failed `/api` or `/w/*/api` request, no horizontal scroll. A 503 from
Lambda throttling gets one reload, said in the output. Shots go to
`.shots/live/`. `BASE_URL`, `WS`, `PIPELINE` and `RUN` override the
defaults; the exit status is 1 on any failure. The run sends its beacons
with a headless user agent, so the traffic pipeline files it as a bot.

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
  true, cognito: {...}, timeZone}`) and upload the build. `timeZone` is
  `var.time_zone` (default Europe/Helsinki), which the Lambda also gets
  as `CLAVESA_TIME_ZONE`: the cards count days in the same zone as the
  summaries.
- `scripts/add-user.sh` the user that `make hosted-add-user` creates.
- `static/tracker-config.js`, `static/t.gif` the tracker's settings
  and pixel; `scripts/errors.sh` is `make errors`;
  `../ui/scripts/hosted-tracking-check.mjs` is `make
  hosted-tracking-check`.
- The sign-in screen and the session are in `ui/src/hosted/`; the local
  UI never loads them.
