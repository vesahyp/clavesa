---
adr: 30
title: Ad hoc questions go to an agent whose queries run in DuckDB on Lambda through MCP, as the asking user; the warehouse stays for dashboards and fallback
date: 2026-11-10
status: Proposed
deciders: Vesa
---

# ADR 030: A question agent over MCP, with no SQL warehouse

**Status**: Proposed (2026-11-10). A spike proved that the query path works.
One link is not proven yet: carrying the user's identity from the
Databricks AI Playground through Unity Gateway. See "Not proven".
The spike ran on the `ucpub` test workspace and AWS account 699166197771,
us-east-2. Everything the test made was deleted (section "The test").

## Context

ADR-029 found that ad hoc analyst SQL is the expensive part at work. A
right-sized serverless SQL warehouse (2X-Small, auto-stop 1 minute) costs
about **336 USD a month** at the work volume model (3,000 queries, about
5 GB each, about 120 awake hours). ADR-029 also proved that DuckDB can read
Unity Catalog (UC) tables with the short-lived S3 credentials that UC vends,
so UC grants still decide who reads what.

Vesa (2026-11-10): "an agent that everyone uses that by default comms with
an external service through mcp for the queries." The idea: people ask
questions in one agent. Its query tool is an MCP server outside
Databricks that runs DuckDB on Lambda. A warehouse runs a query only
when the server cannot.

### What Databricks supports (docs read 2026-11-10)

- **Registration.** You register an external MCP server as an *MCP
  service* in Unity Gateway. It uses a UC HTTP connection, and its
  endpoint is `/ai-gateway/mcp-services/<catalog>.<schema>.<name>`
  (<https://docs.databricks.com/aws/en/ai-gateway/register-mcp-service>).
  The older `/api/2.0/mcp/external/<connection>` route is not on the
  current pages. Callers need `EXECUTE` on the service. Calls are audited
  as `mcpCall` in `system.access.audit`
  (<https://docs.databricks.com/aws/en/ai-gateway/govern-mcp-service>).
- **Surfaces that can use it.** The AI Playground (Tools > MCP Servers),
  the Supervisor Agent of Agent Bricks (now marked legacy), and custom
  agents in code (`DatabricksMCPClient`, deployed to Model Serving or
  Databricks Apps) can all use it. Genie spaces, now "Genie Agents", list
  "MCP connections attached to an agent" as a source, but they still
  require a Pro or serverless SQL warehouse
  (<https://docs.databricks.com/aws/en/genie/set-up>). **Genie does not
  remove the warehouse.**
- **Identity.** The proxy strips the caller's `Authorization` and
  `X-Databricks-*` headers and injects the credential stored on the
  connection (<https://docs.databricks.com/aws/en/query-federation/http>).
  The connection can hold a bearer token, OAuth M2M, OAuth U2M shared,
  dynamic client registration, or **OAuth U2M per user**
  (<https://docs.databricks.com/aws/en/agents/mcp-tools/authentication-networking>).
  Only the per-user option can reach the server as the asking user. A
  custom agent can instead run as the user, through Databricks Apps user
  authorization (`x-forwarded-access-token`) or Model Serving
  on-behalf-of-user (Public Preview), and call the server with that
  token
  (<https://docs.databricks.com/aws/en/agents/custom-agents/agent-authentication>).
- **Billing.**
  - Pay-per-token models cost 0.07 USD per DBU (the workspace's
    `system.billing.list_prices`, SKU `SERVERLESS_REAL_TIME_INFERENCE`).
    GPT-OSS-120B costs 2.143 DBU per 1M input tokens and 8.571 DBU per 1M
    output tokens, which is 0.15 and 0.60 USD. Llama 4 Maverick costs
    0.50 and 1.50 USD
    (<https://www.databricks.com/product/pricing/foundation-model-serving>).
    No Claude endpoint is listed in the test workspace.
  - Databricks Apps cost 0.5 DBU an hour at Medium size
    (<https://docs.databricks.com/aws/en/dev-tools/databricks-apps/compute-size>),
    at 0.75 USD per DBU (all-purpose serverless). That is 0.375 USD an
    hour while running.
  - The HTTP connection "may incur Databricks data transfer charges". No
    other proxy charge is documented.
  - Genie: "Genie One and Genie Agents usage by users is free through
    January 31, 2027" (<https://docs.databricks.com/aws/en/genie/>). The
    test workspace bills it as SKU `GENIE_FREE_USAGE`. The warehouse is
    billed as usual. Genie's price after January 2027 is not published.

## The spike

Code: `spikes/uc-mcp/` on branch `spike/uc-mcp-agent`.

- **`server.py`, the MCP server** (streamable HTTP, stateless, JSON-RPC)
  runs on Lambda (Python 3.13, arm64, 3 GB) with the DuckDB 2.0 `delta`
  and `httpfs` extensions packed in the zip. It has three tools:
  `list_tables`, `describe_table` and `run_sql`. It authenticates each
  request by calling SCIM `Me` with the bearer token it receives, and
  it calls UC with that token only:
  - It lists and describes the tables through UC REST.
  - For each table that `run_sql` names, it calls
    `temporary-table-credentials` (READ). UC refuses that call without
    SELECT and EXTERNAL USE SCHEMA.
  - It creates a DuckDB secret scoped to the table's folder, and a view
    with the table's three-part name, so the SQL runs as written.

  Limits:
  - One SELECT or WITH statement, and 200 rows.
  - A scan limit of 10 GB, checked against the bytes under each table's
    folder.
  - A 60-second timeout.
  - After the views are made, `enable_external_access = false` with
    `allowed_directories` set to the tables' folders, then
    `lock_configuration`. A query can read nothing else. Tested: reading
    `/etc/passwd` or another S3 path is refused.

  The Lambda role can only write logs.
- **`agent.py`, the agent loop.** It runs a Databricks-hosted model
  (`databricks-gpt-oss-120b`) with the three tools, and it runs as one
  principal: Vesa's CLI login, or a service principal's M2M secret. It
  sends that principal's token to the MCP server. This is the shape of a
  custom agent deployed with on-behalf-of-user auth. In the spike the
  loop ran on the Mac, and the model ran on Databricks.
- **`genie.py`** asks the same questions of a Genie space. The space has
  the same single table, no instructions, and the serverless 2X-Small
  warehouse with a 1-minute auto-stop.

The table is `team_ucpub.sales.daily_events_clavesa`. Clavesa wrote it as
an external Delta table: 230 rows, 125 KB.

### Results

| Question | Agent (DuckDB on Lambda) | Genie (warehouse) |
|---|---|---|
| Purchase revenue per day | 2,169.51 / 3,604.38 / 7,934.39 / 2,169.51 / 2,169.51 (sum of amount × quantity), twice | 505.31 / 1,151.25 / 2,275.14 / 505.31 / 505.31 (sum of amount) |
| Page with most views | /p/1, 19 | /p/1, 19 |
| Cart users, and how many bought | 18, 16 | 18, 16 |
| Average purchase, items sold | 93.25, 185 | 93.25, 185 |
| Busiest purchase hour (UTC) | 03, 8 | 03, 8 |

The two routes agree on four of the five answers. On revenue they
read `amount` differently. The table does not say whether `amount` is a
line total, so either answer can be defended. A UC column comment fixes
this for both routes: `describe_table` returns comments, and Genie reads
them.

| Measure | Agent | Genie |
|---|---|---|
| Time per question | 6 to 22 s (median 10 s). One 66 s outlier was model latency, with 2.6 s in the tools | 12 to 23 s. 23 s was from a stopped warehouse |
| Query time inside that | 0.5 to 0.9 s per `run_sql` warm. Lambda cold start 0.6 s | not separable |
| Tokens per question | about 2,300 in and 400 out (1,650 to 4,330 in) | not billed until 2027-02 |
| Cost per question at test size | about **0.0008 USD** (0.0006 tokens, 0.0002 Lambda) | about **0.02 USD** in a burst of five, **0.07 USD** alone (the warehouse wakes for about 1.5 minutes: start, query, 1 minute idle) |

One of the ten agent runs got an HTTP error from the model endpoint
and succeeded when run again.

### What was proven, and how

1. **Queries run with no warehouse, as the asking user.** Each run of
   the five questions as Vesa used three tool calls. The Lambda log
   records `user: vesa.hypponen@gmail.com` for each call, and each
   `run_sql` reports "engine DuckDB, no warehouse".
2. **UC grants decide (direct route).** A new service principal,
   `adr030-noselect`, had USE CATALOG, USE SCHEMA and EXTERNAL USE SCHEMA,
   but no SELECT:
   - `list_tables` returned nothing.
   - The agent answered "I don't have permission to read the table".
   - A direct `run_sql` returned UC's 403 "Principal 'adr030-noselect'
     does not have SELECT on Table ...". No credentials were vended.
   - With SELECT granted, the same question returned the right numbers.
   - After the revoke, it was refused again.
   - A token that Databricks does not accept gets a 401 before any tool
     runs.
3. **Unity Gateway can register and proxy the server.** The MCP service
   `team_ucpub.sales.uc_duckdb` was created through the API, and
   `tools/call` through `/ai-gateway/mcp-services/...` reached the
   Lambda. Two findings:
   - **Lambda function URLs (`*.on.aws`) are refused** by the gateway
     with "denied because of serverless network policy", although the
     account's policy is FULL_ACCESS. An API Gateway HTTP API
     (`*.execute-api.amazonaws.com`) in front of the same Lambda
     works. Use API Gateway, or a custom domain.
   - **A shared credential on the connection erases identity.** The
     connection held the token of `adr030-noselect`. Vesa called through
     the gateway and the server saw `adr030-noselect`: no tables, and
     the query was refused. The headers that arrive are
     `user-agent: Databricks-MCP-Proxy/1.0`, a `traceparent`, an
     `x-request-id` and the connection's `Authorization`. Nothing names
     the user. The gateway also replaces a tool's error text with "The
     external MCP tool failed", so the server's refusal reason does not
     reach the agent.

### Not proven

- **Per-user identity through Unity Gateway, so that the AI Playground
  works as each user.** The configuration would be:
  - a UC connection with OAuth U2M per user, where the OAuth provider is
    the Databricks workspace itself (a custom OAuth app integration,
    scopes `all-apis offline_access`);
  - each person signs in once on the MCP service's sign-in page;
  - the gateway then injects a Databricks token for that person, and
    the server uses it for UC exactly as in the direct route.

  This was not run. Creating an account-level OAuth app that hands
  Vesa's all-apis token to an outside service needs his approval, and
  his sign-in in a browser. Until then, the proven identity route is a
  custom agent that runs as the user and calls the server directly (or
  through the gateway with a header of its own).
- **The AI Playground and the Supervisor Agent were not used.** Both are
  UI-driven, and with a shared credential they could only repeat
  finding 3.
- **Throughput at work size.** The test table is 125 KB. The Lambda
  numbers below rest on ADR-029's unmeasured guess (a 4 to 6 vCPU DuckDB
  reads about 5 GB in 15 to 30 s).
- **The warehouse fallback** (the managed DBSQL MCP server
  `/api/2.0/mcp/sql` as a second tool) was not built.

## Cost at work volume

ADR-029's model: 3,000 ad hoc queries a month, about 5 GB each. The
spike assumes about 1.5 queries per question, so 2,000 questions.

| Item | Agent route, USD/month | Warehouse only, USD/month |
|---|---|---|
| Queries on Lambda, 10 GB memory, about 30 s each (0.004 USD) | 12 | |
| Model tokens: GPT-OSS-120B to Llama 4 Maverick, 1 to 3 times the test's tokens for a bigger schema | 1 to 10 | |
| Warehouse fallback: 10 % of queries (too big, views, row filters or masks), spread over the month, about 2 awake minutes each | 28 | |
| Where the agent runs: the Playground (no charge), a Databricks App during working hours (8 h × 20 days at 0.375 USD), or the same App always on | 0 / 60 / 274 | |
| Serverless 2X-Small warehouse, auto-stop 1 min, 120 awake hours | | 336 |
| **Total** | **about 41 to 50 with the Playground; 100 to 110 with a working-hours App** | **336** |

Genie on the same warehouse costs about the same as the warehouse
alone today. From February 2027 Genie usage is billed on top, at a price
not yet published.

The saving depends on two things: the share of queries that fit on
Lambda, and where the agent runs. **An always-on Databricks App cancels
most of the saving.** The Playground route has no hosting cost, but it
needs the per-user identity that is not yet proven.

## What is lost

- **Dashboards.** AI/BI dashboards and the SQL editor run only on a
  warehouse. They keep it. The agent saves only the ad hoc share.
- **Genie's curated instructions.** A Genie space carries:
  - instructions;
  - example SQL;
  - trusted assets;
  - benchmarks (`genie-create-eval-run`).

  The agent has a system prompt and UC comments. The revenue
  disagreement above is the kind of error that curation prevents. The
  same text can be kept in UC comments and in the agent's prompt, but
  someone has to maintain it, and nothing evaluates it.
- **Audit detail.**
  - On the warehouse, `system.query.history` has every statement, its
    user and the bytes read.
  - On the agent route, UC audit has the credential vending per user
    and table (`generateTemporaryTableCredential`), and the gateway
    audit has `mcpCall`. The SQL text and its result size are only in
    the server's own log (in the spike: a CloudWatch line per tool call
    with the user, the tool and the SQL).
  - Lineage for these reads is not recorded in UC.
- **Fine-grained access.** UC does not vend credentials for a table with
  row filters or column masks, or for a view. Those go to the warehouse.
- **SQL dialect.** The agent writes DuckDB SQL. People who read its
  queries see DuckDB SQL, not Databricks SQL.
- **A token leaves Databricks.** The server receives a Databricks token
  that can do anything its user can. It must not log or store the token,
  and it must run in an account the data owners trust. A narrower OAuth
  scope for UC reads was not found.

## Decision

1. **Do not move work's ad hoc SQL to an agent yet.** The query path is
   proven and is about 25 to 90 times cheaper per question than a
   warehouse wake. The route people would use, the Playground with
   per-user identity, is not proven. The other route, a Databricks App,
   costs 60 to 274 USD a month to host.
2. **Next, run the per-user OAuth test (next slice 1).** If the
   Playground reaches the server as each user, the agent route becomes
   the recommended default for ad hoc questions. The warehouse stays,
   with a 1-minute auto-stop, for dashboards, the SQL editor and the
   fallback.
3. **Any production version uses API Gateway (or a custom domain) in
   front of Lambda,** not a function URL. It writes UC column comments
   for the tables it serves, and it logs every SQL statement with the
   user.

## Next slices (not built)

1. **Per-user OAuth through Unity Gateway.** Steps:
   - a custom OAuth app integration in the test account, with redirect
     URL `<workspace>/login/oauth/http.html`;
   - a connection with OAuth U2M per user against the workspace's
     `/oidc/v1/authorize` and `/oidc/v1/token`;
   - Vesa signs in once and asks the same five questions in the AI
     Playground;
   - a second user without SELECT is refused.

   This needs Vesa's approval and his sign-in. Head Jeeves (2026-11-10)
   chose to tear the spike down and keep this as the next slice, to be
   run when Vesa can sign in. `up.sh` rebuilds the server and its API
   Gateway in about two minutes.
2. **Throughput at work size.** Run `run_sql` on Lambda (10 GB) over a
   67 GB day partition in the test account, and record seconds and
   bytes. This replaces the 30 s guess.
3. **The warehouse fallback as a second MCP tool,** and a rule in the
   agent's prompt for when to use it.

## Consequences

- Ad hoc cost would fall from about 336 to about 45 to 110 USD a month
  at the model volume. The range depends on hosting, the fallback share
  and unmeasured throughput.
- One more service to run: the MCP server, its API Gateway, its
  deployment and its logs. Today this is spike code, not clavesa code.
- Answers depend on a model and on table comments. Genie's curation and
  benchmarks are not reproduced, and nothing evaluates the agent's
  answers.
- The SQL text that people's questions produce lives in the server's
  log, not in `system.query.history`.
- A Databricks user token is sent outside Databricks to the server.

## The test

Run 2026-11-10 on the `ucpub` test workspace and AWS account
699166197771, us-east-2. No work workspace or work account was used.

Made and deleted:
- AWS: Lambda `adr030-uc-mcp`, its log group, its function URL and its
  role (logs only); the API Gateway HTTP API `adr030-uc-mcp`; the
  deploy bucket `adr030-uc-mcp-699166197771`.
- Databricks:
  - the HTTP connection `adr030_uc_mcp`;
  - the MCP service `team_ucpub.sales.uc_duckdb`;
  - the Genie space "adr030 events";
  - the service principal `adr030-noselect`, its OAuth secret and its
    grants (deleted at account level);
  - the EXTERNAL USE SCHEMA grants to Vesa on `team_ucpub.sales`,
    `clavesa_ucpub.orders` and `clavesa_ucpub.daily`.

Checked as gone afterwards: no AWS resource named or tagged `adr030`
(Lambda, API Gateway, IAM, S3, logs, tagging API); in Databricks no app,
no custom serving endpoint, no connection, no MCP service other than the
built-in `system.ai.*` ones, no test Genie space, no test service
principal in the workspace or the account, and the schema grants are
back to the clavesa service principal only. Both warehouses are
stopped with a 1-minute auto-stop.

Cost:
- Model tokens: about 40,000 in and 7,000 out on GPT-OSS-120B, about
  0.01 USD.
- Lambda, API Gateway and S3: under 0.01 USD.
- The serverless 2X-Small warehouse: Genie's five questions and five
  system-table queries, about 12 awake minutes with the 1-minute
  auto-stop, about 0.8 DBU, so about 0.56 USD. This is an estimate,
  because `system.billing.usage` had not yet received the day.
- Genie: no charge (free usage SKU).
- **Total about 0.6 USD.**
