---
adr: 32
title: A hosted, read-only clavesa UI at ui.clavesa.dev shows every workspace on the phone, from the same Go handlers and React pages as clavesa ui
date: 2026-11-15
status: Accepted; section 1 superseded by 033; "every signed-in user sees every workspace" (section 2) superseded by 035
deciders: Vesa
---

# ADR 032: A hosted, read-only UI for all workspaces, phone first

**Status**: Accepted (2026-11-15). The three decisions at the end are
made, all as recommended.

## Context

Vesa, 2026-11-15: "Next up for clavesa is a hosted version of the local ui.
I want to be able to see my different workspaces on mobile."

What exists today:

- `clavesa ui --workspace <dir>` serves one workspace at
  `http://localhost:8080`. The workspace path is fixed when the process
  starts and is passed into every handler constructor
  (`internal/cli/ui.go:584-592`, `:742-794`, `internal/cli/helpers.go:376-395`).
  The router is a plain `net/http` ServeMux: the API is at `/api/`
  (`ui.go:860`), the embedded SPA at `/` (`ui.go:869`).
- There is no auth, no read-only mode and no CORS layer. Every route
  registers with its HTTP method, so the routes that change state are all
  POST, PUT or DELETE. Two read POSTs exist: `POST /data/query` and
  `POST /dashboards/query`.
- The frontend is React 18, Vite, Tailwind, TanStack Query, React Router
  (`BrowserRouter`, no `basename`) and Recharts. It is a desktop sidebar
  layout. Only 10 files use breakpoint classes.
- The workspaces are of two kinds, and most of Vesa's live pipelines are the
  second kind:
  - **Cloud warehouse.** Run history is in the Delta tables `runs` and
    `node_runs` in `<system_catalog>__pipelines` (Glue, read through
    Athena). Execution state is in Step Functions, logs in CloudWatch Logs.
    Examples: `~/clavesa-workspaces/smoke2` (personal account, S3 backend),
    `ucpub` and `ucguide` (personal, local Terraform state),
    `monorepo/analytics/web-traffic` (webbaa account, S3 backend).
  - **Local warehouse.** The nightly site-traffic pipelines that cron runs
    on the main Mac: `site-traffic` (clavesa.dev), `omatienoo-traffic`,
    `podbay-traffic`, kivikko, keitos, vinkki, rakka and more. Their run
    history is only on the Mac (`~/.local/state/clavesa/runs`). jeeves
    `scripts/runs-export` already exports the run timings of these to the
    podbay app's Nightly tab.
- The read paths need the workspace tree as well as AWS: `clavesa.json` for
  the catalog names, the pipeline `.tf` files for the pipeline list, the node
  graphs and `freshness_sla`, and Terraform state for `pipeline_bucket` and
  the state machine ARNs (`internal/workspace/tfstate.go:199`).
- Athena queries write their results to
  `s3://<pipeline_bucket>/athena-results/` in the `primary` workgroup
  (`internal/observability/cloud.go:1233`). A "read-only" principal that
  uses Athena therefore needs one write grant.
- ADR 005 (Withdrawn) said clavesa has no hosted product and that a hosted
  deployment needs a new ADR. This is that ADR.
- House rules from jeeves `practices/`: a low-traffic hosted thing starts on
  Lambda, HTTP API and S3 with CloudFront; login is Cognito email one-time
  code with Google as the alternative, no passwords, SES from day one
  (`auth.md`); every site ships first-party tracking and error reporting
  (`web-tracking.md`); Terraform in the repo, state in the personal backup
  bucket, `default_tags { project = "clavesa" }` (`infrastructure.md`).
- Route 53: the `clavesa.dev` zone is in the personal account (699166197771)
  and holds only the apex, `www` and their ACM records. `ui.clavesa.dev`
  is free. There is no SES identity for `clavesa.dev` yet.

## Decision

### 1. What the phone shows

Four pages, all read-only in this ADR. Every number with a history is a
chart; the only tables are lists of runs or nodes that Vesa acts on.

1. **Workspaces** (`/`). One card per workspace, problems first. Each card:
   - Name, account and region, and a status dot that is the worst of:
     last run failed, a table past its `freshness_sla`, no run inside the
     expected interval.
   - Last run: time since, and its result.
   - A 14-day strip of runs per day, succeeded and failed stacked.
   - Freshness: the number of tables past their limit, and the oldest age.
   - Cost: compute cost per day for 30 days as a sparkline, with the
     30-day total and the cost per billion records (ADR north star). A
     local-warehouse workspace shows $0 and its records per second.
   - "Summary from <time>", so a stale summary is visible.
2. **Workspace** (`/w/<name>`). Runs per day for 30 days (succeeded and
   failed), cost per day split by pipeline, then one card per pipeline with
   its last run, a duration sparkline and its stale tables.
3. **Pipeline** (`/w/<name>/pipelines/dashboard`, the existing
   `PipelineDashboard`). Duration per run as bars coloured by result, cost
   per billion records over time (`RunTimingsCard`, `PipelineHealthHeader`),
   and the list of recent runs.
4. **Run** (`/w/<name>/pipelines/run`, the existing `RunDetailView`). The
   node timeline, rows in and out per node, and for a failed run the error
   and the last 50 log lines.

Catalog, Query, Notebooks, Sources, Credentials, Runner, Bundles and the
DAG editor are not in the hosted navigation in this ADR.

**Read-only first.** The hosted server refuses every method except GET and
HEAD, and the IAM roles it uses can read only (section 2). The two layers
are independent: a bug in one leaves the other in place.

**Acting is a later ADR.** The first action worth having is "run this
failed pipeline again". It needs:

- A second role per workspace, `clavesa-ui-operator`, with
  `states:StartExecution` on that workspace's state machines and nothing
  else. A local-warehouse workspace cannot be acted on from the phone,
  because its compute is the Mac.
- An allowlist of one route, `POST /pipeline/run`, in the method guard.
- A fresh sign-in for the action (a code from the last 5 minutes), an
  `Origin` check, and an audit line per action (who, when, which run) in
  the hosted bucket.
- No `.tf` edits and no deploy from the phone, ever under this design: the
  hosted tree is a copy taken at deploy, and git on the Mac is the source.

### 2. How the hosted UI reads workspaces across accounts

One summary format, two ways to fill it, and live drill-down only where
the data is in AWS.

**The summary.** A new service function `service.WorkspaceSummary`
returns a typed struct: pipelines, the runs of the last 30 days with result,
duration, rows and cost, the node timings and error of each pipeline's last
run, and table freshness against `freshness_sla`. It is computed from the
same `observability` providers the UI uses today, so a local and a cloud
workspace give the same shape (ADR 014). The hosted UI stores one JSON file
per workspace at `s3://clavesa-ui-<account>/summaries/<name>.json`. The
Workspaces page reads only these files, so it opens in one request and
makes no Athena query.

**Cloud-warehouse workspaces: pulled by the hosted Lambda, through a
read-only role.**

- When `clavesa.json` has a `hosted` block, the workspace Terraform that
  clavesa emits adds a role `clavesa-ui-reader-<workspace>` in the
  workspace's own account. Its trust policy names only the hosted Lambda's
  role ARN, with an `sts:ExternalId` equal to the workspace name. The
  hosted Lambda uses `sts:AssumeRole` and gets 15-minute credentials. No
  access key exists anywhere in this design.
- The reader policy is scoped to that one workspace:
  - S3 `GetObject` and `ListBucket` on the workspace's pipeline bucket.
  - S3 `PutObject` on `athena-results/clavesa-ui/*` in that bucket, and
    nothing else that writes.
  - Glue `GetDatabase(s)`, `GetTable(s)`, `GetPartitions` on the
    workspace's catalog and system databases.
  - Athena `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`
    on a new workgroup `clavesa-ui-<workspace>` with its own output
    location and a per-query scan limit of 1 GB.
  - Step Functions `ListExecutions`, `DescribeExecution`,
    `GetExecutionHistory` on the workspace's state machines.
  - CloudWatch Logs `FilterLogEvents` on the workspace's runner log groups.
  - EventBridge `DescribeRule` on the workspace's schedule and poller
    rules, so a schedule paused by hand shows as paused instead of as an
    overdue pipeline (v2.27.4). Without the time it was paused: CloudTrail
    `LookupEvents` was granted for that in v2.27.5 and removed on
    2026-11-17 (Vesa: one date is not worth an account-wide read).
  - `kms:Decrypt` only if the bucket uses a customer key. Lake Formation
    `GetDataAccess` only if slice 4 shows it is needed.
  - No `GetObject` on Terraform state: state can hold secrets.
- `clavesa deploy` uploads a read-only copy of the workspace tree to
  `s3://<pipeline_bucket>/_clavesa/tree/`: `clavesa.json`, the pipeline
  `.tf` files and a `deployed.json` with the Terraform outputs the read
  paths need (`pipeline_bucket`, state machine ARNs). It never uploads
  state, `.env` files or credential material; the credential registry holds
  only references. The resolver reads `deployed.json` when it runs with no
  Terraform state beside it. The copy matches what is deployed, which is
  what the phone must show.
- The same emitted Terraform adds an EventBridge rule in the workspace
  account. It forwards Step Functions "Execution Status Change" events for
  the workspace's state machines, and the `_progress/*/_run.json` writes of
  cloud-local runs, to the event bus `clavesa-ui` in the personal account.
  That bus's policy accepts events only from the registered accounts. Each
  event starts a refresh of that one workspace's summary. A daily schedule
  refreshes all of them, so freshness ages stay correct on days with no
  run.
- Drill-down (Workspace, Pipeline, Run pages) goes to the live handlers
  under the same role, so a run detail shows the real log lines. Athena
  result reuse (5 minutes, already on) keeps a repeat view free.

**Local-warehouse workspaces: pushed from the Mac.**

- When such a workspace has a `hosted` block, `clavesa pipeline run` ends
  by writing that workspace's summary to the hosted bucket. It assumes a
  role `clavesa-ui-publisher` whose only grant is `s3:PutObject` on
  `summaries/<name>.json` for the names in the registry. This uses the
  Mac's existing `personal` profile; no new key is made. The summary
  carries enough (30 days of runs, the last run's node timings and error)
  for the Pipeline and Run pages to draw from it with no live call.
- A summary that is older than the workspace's expected interval turns the
  card's dot amber. That is also how the phone shows "the Mac did not run
  tonight".

**Rejected alternatives.**

- *Pull for everything.* The hosted side cannot reach a Mac disk.
- *Push for everything,* from the runner at run end. Then a cloud run's
  summary depends on a writer in every account, and drill-down still needs
  a read path. Pull with a read-only role keeps all writes in one account.
- *Live Athena for the Workspaces page.* About 3 queries per workspace, 2
  to 5 seconds each, on every open. The stored summary opens at once.

**How a new workspace joins.**

1. `clavesa workspace hosted enable --url https://ui.clavesa.dev`
   (and the same switch in the local UI's workspace settings, ADR 015).
   It writes the `hosted` block into `clavesa.json` with the hosted
   Lambda's role ARN, which it reads from
   `https://ui.clavesa.dev/.well-known/clavesa.json`.
2. A cloud workspace needs the S3 Terraform backend (ADR 025) first:
   `clavesa workspace backend migrate`. Then `clavesa deploy` creates the
   reader role, the workgroup, the EventBridge rule and the tree copy.
3. Add one entry to `hosted/infra/workspaces.tf` in this repo (name, kind,
   account, region, reader role ARN) and `make hosted-apply`. The entry
   lets the bus accept that account and the publisher role write that name.
4. The card appears after the next run, or after
   `clavesa workspace summary --publish`.

### 3. One app: the hosted UI is the local UI's code

- **Go.** Slice 2 moves the handler wiring in `internal/cli/ui.go` into one
  function, `ui.BuildWorkspaceMux(deps) http.Handler`, that `clavesa ui`
  and the Lambda both call. A small router mounts one cached mux per
  workspace under `/w/<name>/api/`. The constructors are cheap and lazy
  (`helpers.go:349-366`), so a Lambda instance builds a workspace's mux on
  first use. The Spark warmup, Docker sweeps, notebook pool and
  `syscall.Exec` restart stay in the local command.
- **Lambda entry.** `cmd/clavesa-hosted` wraps the same mux with
  `aws-lambda-go` and the HTTP API adapter, plus a second handler for the
  EventBridge refresh events. Build: `GOARCH=arm64`, `provided.al2023`,
  without the embedded `dist/`, because CloudFront serves the SPA from S3.
- **Local first.** The same multi-workspace router ships as
  `clavesa ui --workspaces <dir> [--read-only]`. It is the local test bench
  for every hosted page, and it gives the local surface the Workspaces page
  too, so CLI, local UI and hosted UI stay one capability (ADR 015):
  `clavesa workspace summary [--json] [--publish]` is the CLI wrapper of
  the same service call.
- **Read-only guard.** A middleware that answers 405 to every method except
  GET and HEAD. `--read-only` turns it on locally; the Lambda always has it.
  A Go test walks the registered route table and asserts that every
  non-GET route answers 405, so a new route cannot open a write.
- **Frontend.** The same `ui/` build. A runtime config in `index.html`
  (`window.__CLAVESA__ = { mode, basename, apiBase }`) replaces the
  build-time `VITE_API_BASE_URL`; `BrowserRouter` gets the `basename`; the
  direct `fetch` calls in `api/workspace.ts` and `api/pipeline.ts` move to
  the shared client. A `useReadOnly()` hook hides the controls that change
  state. The four pages in section 1 get phone layouts (single column below
  `md`, bottom navigation); the desktop layout stays as it is.

### 4. Login, who may see it, domain, tracking

- **Domain.** `ui.clavesa.dev`: an alias record in the existing
  `clavesa.dev` zone (read as a data source), ACM certificate in
  us-east-1. One CloudFront distribution: `/*` from a private S3 bucket
  with OAC (SPA, 403 and 404 answer `/index.html` with 200), and `/api/*`
  plus `/w/*/api/*` to the HTTP API. Same origin, so no CORS.
- **Login.** Copy `keitos/infra/auth.tf`, `app.tf` and `ses.tf`:
  - Cognito pool `clavesa-ui` in eu-north-1, ESSENTIALS tier, `EMAIL_OTP`
    first factor, `ALLOW_USER_AUTH`, password flows removed from the client.
  - Google IdP with code flow and PKCE, through the Cognito prefix domain.
    The Google OAuth client is the one hand step (Google Cloud Console);
    the IdP is conditional, so sign-in with a code works before it exists.
  - SES identity for `clavesa.dev`: DKIM, custom MAIL FROM, `_dmarc`;
    mail from `no-reply@clavesa.dev`. The account already has SES
    production access in eu-north-1.
  - A pre-signup Lambda with an email allowlist and
    `AdminLinkProviderForUser`. The allowlist and the "registration open"
    flag live in the SSM SecureString `/clavesa-ui/config`, read by a
    `data` source, and fail shut when missing. No address is written into
    the repo.
- **Who may see it.** Vesa only. The HTTP API has a Cognito JWT authorizer
  on every route except `/.well-known/clavesa.json`; the Lambda never runs
  for an unsigned request. Each registry entry has a `viewers` list for a
  later per-workspace grant through Cognito groups; in this ADR it holds
  only Vesa.
- **Tracking and errors.** `tracker.js` from `web-tracker/` (symlink, as
  `site/` does), `spa: true`, and a `sanitize` that replaces run ids and
  execution ARNs in paths with `:run`. A top-level error boundary sends
  `track("error", { msg, type: "boundary" })`. `t.gif` with `no-store`.
  CloudFront standard logs go to the existing `clavesa-cloudfront-logs`
  bucket under the prefix `ui/`. The existing `site-traffic` workspace
  reads that prefix as a second host (the omatienoo shape: a `site` column),
  so no new cron slot is needed. The board is a `/stats` page inside the
  signed-in app, with an errors panel; `make errors` prints 7 days.
- **Alarms.** Lambda errors and a summary refresh that fails push to the
  phone through the `jeeves-push` SNS topic.

### 5. Monthly cost

eu-north-1 prices, one viewer, about 10 workspaces, about 20 cloud runs a
day. The account's always-free tiers (Lambda 1M requests and 400,000 GB-s,
CloudFront 1 TB and 10M requests, Cognito 10,000 monthly users) are shared
with the other personal projects; the second column assumes they are used up.

| Item | Idle | In use (10 visits a day) | No free tier, in use |
|------|------|--------------------------|----------------------|
| CloudFront, S3 static | $0.00 | $0.00 | $0.05 |
| HTTP API ($1.11 per million) | $0.00 | $0.02 | $0.02 |
| Lambda arm64, 512 MB | $0.00 | $0.00 | $0.10 |
| Athena, summary refresh (10 MB minimum per query) | $0.10 | $0.10 | $0.10 |
| Athena, drill-down | $0.00 | $0.10 | $0.10 |
| EventBridge cross-account events | $0.00 | $0.00 | $0.01 |
| CloudWatch Logs, 14-day retention | $0.05 | $0.10 | $0.10 |
| Cognito, SES codes | $0.00 | $0.00 | $0.02 |
| Route 53 (zone exists), ACM, SSM standard | $0.00 | $0.00 | $0.00 |
| **Total** | **about $0.15** | **about $0.30** | **about $0.50** |

**The comparison, Fargate with an ALB** (always on, public subnets, no NAT):
one task of 0.25 vCPU and 0.5 GB arm64 is about $7; the ALB is about $16
plus LCUs; three public IPv4 addresses (two for the ALB, one for the task)
are about $11. **About $34 a month, idle or in use**, plus a container image to
keep patched. The Lambda design is about 1% of
that.

### 6. Slices

Agent time is the time for one implementation subagent plus the lead's
review and gate. Each slice ends with a commit.

| # | Slice | Agent time | How it is tested |
|---|-------|-----------|------------------|
| 1 | `service.WorkspaceSummary`, `clavesa workspace summary [--json]`, and a health card on the local UI's workspace home | 3 h | Go tests on a fixture local workspace and on the cloud provider with fakes; CLI JSON golden file; `playwright-cli` on `clavesa ui` |
| 2 | `ui.BuildWorkspaceMux`, `clavesa ui --workspaces <dir> --read-only`, the method guard, runtime `basename`/`apiBase`, `useReadOnly()` | 4 h | Go test that walks the route table and gets 405 for every non-GET route in read-only mode; `playwright-cli` against `--workspaces ~/clavesa-workspaces` |
| 3 | Phone layouts of Workspaces, Workspace, Pipeline and Run pages, with the charts in section 1 | 5 h | `make ui-phone-shots`: a committed Playwright script, iPhone 16 emulation, local multi-workspace mode; Vesa looks at the shots |
| 4 | Emitted reader role, Athena workgroup, EventBridge rule, the tree copy and `deployed.json` on deploy, resolver reads it | 5 h | Emitter golden tests and `terraform validate`; in `make smoke-cloud` on smoke2: assume the role, read a run, and `aws iam simulate-principal-policy` shows `StartExecution` and a `PutObject` outside `athena-results/clavesa-ui/` as denied |
| 5 | `hosted/infra/`: Cognito, SES for clavesa.dev, pre-signup allowlist from SSM, S3 and CloudFront for the SPA, ACM, `ui.clavesa.dev`, sign-in screen | 4 h | `make hosted-plan`, then apply; sign in with a code at `https://ui.clavesa.dev` and get the empty Workspaces page; a code mail arrives from `no-reply@clavesa.dev` with DKIM pass |
| 6 | `cmd/clavesa-hosted`, HTTP API with the JWT authorizer, the bus `clavesa-ui`, refresh handler, publisher role, alarms to `jeeves-push` | 5 h | `curl` with no token gets 401; with a token, GET gets 200 and POST gets 405; a smoke2 run puts a fresh `summaries/smoke2.json` in the bucket within a minute |
| 7 | Tracking and errors: tracker, `sanitize`, error boundary, logs under `ui/`, `site-traffic` second host, `/stats` board, `make errors` | 3 h | A test build throws one error and sees the `error` beacon leave the page; the next `site-traffic` run shows `ui.clavesa.dev` rows |
| 8 | Join the workspaces: smoke2; ucpub and ucguide after the backend move; the local-warehouse site-traffic pipelines; webbaa `web-traffic` (reader role in the monorepo Terraform, draft PR) | 3 h | Every registered workspace has a card with a summary less than one day old; `clavesa workspace summary --json` and the card show the same numbers |
| 9 | Live phone check | 2 h, then Vesa | A committed Playwright iPhone 16 run against `https://ui.clavesa.dev` with a check user from SSM (the `omatienoo` `showcase-screenshots` pattern) opens all four pages with no console error; then Vesa opens `https://ui.clavesa.dev` on his phone, signs in with the mailed code, and checks one card against `clavesa workspace summary` on the Mac |

Total about 34 hours of agent time. Slices 1 to 3 need no AWS and can run
before the decisions below are made. Slice 3 and slice 4 are independent;
everything after slice 4 is in order.

## Consequences

**Positive**

- Vesa sees every workspace, cloud and local, on the phone in one page,
  with the failed runs and stale tables first.
- One codebase: the hosted UI is the local UI with a read-only guard and a
  multi-workspace router. A fix in a page reaches both.
- The local UI gains `--workspaces`, and the CLI gains
  `workspace summary`; ADR 015 parity holds.
- No long-lived key is created. Each workspace account controls its own
  reader role and can remove it with one deploy.
- About $0.30 a month in use.

**Negative and costs**

- Every workspace account trusts a role in the personal account. That
  includes webbaa, which collaborators share; they will see the role and
  its trust in the monorepo Terraform.
- The "read-only" reader can write Athena results under one prefix. The
  scan limit on the workgroup caps the cost of a bad query.
- Local-warehouse summaries depend on the Mac running and on the Mac's
  existing static `personal` keys. A Mac that is asleep shows as amber
  cards.
- Deploy now uploads a copy of the workspace tree. A workspace whose `.tf`
  files hold something that must not be in its own bucket must not enable
  `hosted`.
- Cloud workspaces on local Terraform state (ucpub, ucguide) must move to
  the S3 backend before they can join.
- Drill-down on a cloud workspace waits 2 to 5 seconds for Athena on the
  first view. The summary content draws first.
- About 34 hours of agent work, and the phone layouts add a second layout
  to the four pages to maintain.
- The podbay app's Nightly tab and this UI both show run timings for the
  local pipelines. The Nightly tab can link to `https://ui.clavesa.dev`
  once slice 8 is live; removing the overlap is podbay's decision.

## Decisions made (2026-11-15)

All three are accepted as recommended.

1. **Accepted.** **Hosted is a clavesa feature any user can deploy into their own account,
   and Vesa's `ui.clavesa.dev` is the first deploy.** There is no
   clavesa-run multi-tenant service. Recommendation: yes. It costs only
   that `hosted/infra/` takes its names from variables, and it keeps
   ADR 005's single trust boundary per AWS account.
2. **Accepted.** **A reader role in the webbaa account, trusted by the personal
   account.** Recommendation: yes, in the monorepo's Terraform where
   collaborators can see it, scoped as in section 2.
3. **Accepted.** **Local-warehouse pipelines publish their summary from the Mac at run
   end,** with the Mac's existing `personal` keys, through a role that can
   only write the summary files. Recommendation: yes. The other way to show them is to move each
   one to a cloud warehouse, which costs a deploy per site and a monthly
   bill per pipeline for no gain on the phone.

## Update 2026-11-17

Section 1 (the four pages, and "Catalog, Query, Notebooks, Sources,
Credentials, Runner, Bundles and the DAG editor are not in the hosted
navigation") is superseded by ADR-033: the hosted UI is the local UI
minus writes. The rest of this ADR stays.

Later the same day: the registry note "every signed-in user sees every
workspace" is superseded by ADR-035 (per-workspace viewers by Cognito
group).
