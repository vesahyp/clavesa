---
adr: 34
title: The hosted UI answers two reads that use POST, and runs no SQL that a request writes
date: 2026-11-17
status: Accepted
deciders: Vesa
---

# ADR 034: Reads that use POST in the hosted UI

**Status**: Accepted (2026-11-17, Vesa: go on with the parity plan).
Extends ADR-032's method guard; part of ADR-033 (parity).

## Context

`ui.ReadOnlyGuard` answers 405 to every method except GET and HEAD. Two
reads use POST because their input is SQL: `POST /dashboards/query`
(dashboard widgets) and `POST /data/query` (the Query page, table
previews, notebook cells). Both take the SQL from the request body and
run it through the shared query seam (`service.Query`, ADR-023).

If the hosted UI simply allowed them, any signed-in session could run
any SELECT the reader role can read, at up to 1 GiB per query: ad-hoc
query by another name. Ad-hoc query needs per-workspace viewers first
(ADR-035) and its own controls (parity slice 7). The 2026-11-17 review
also found raw visitor IPs in web-traffic's bronze and silver, now
denied to the reader (personal_data).

## Decision

1. **The guard allows exactly two POST routes**, by path, in hosted and
   `--read-only` mode: `POST /dashboards/query` (this ADR, now) and
   `POST /data/query` (parity slice 7, under the rules in point 5). Every
   other non-GET route still answers 405, and the route-table test keeps
   asserting it.
2. **A hosted dashboard query runs only deployed SQL.** In read-only
   mode the server answers `POST /dashboards/query` only when the
   request's pipeline dir and SQL are exactly a dataset of a dashboard
   spec in the deployed tree copy (`.clavesa/dashboards/<slug>.json`);
   anything else is 403. The `{{control}}` placeholders are expanded
   after the match, so control values cannot change the statement's
   shape beyond what the spec's placeholders allow. The page sends the
   same request as on the Mac. So the hosted UI can only run the SQL that
   was deployed from the Mac.
3. **Translation happens on the Mac.** A cloud workspace runs widget
   SQL on Athena, so SparkSQL is translated to Trino first, by a
   translator that runs in Docker. The hosted Lambda has no Docker. The
   deploy therefore translates every dashboard dataset on the Mac and the
   tree copy ships the translation cache files
   (`.clavesa/cache/transpile/<key>.trino`, the cache the translator
   already keeps); the Lambda's translator finds them and never needs
   Docker. A dataset that did not translate at deploy fails on the phone
   with that cause.
4. **Cost.** The reader workgroup's 1 GiB per-query cap stays. Athena
   result reuse is already on for every query clavesa sends (5 minutes,
   `internal/observability/cloud.go`), so a dashboard reopened within
   that time costs nothing.
5. **Ad-hoc query, later (slice 7), cloud workspaces only:** SELECT and
   WITH only, checked by the parser before Athena; only serving
   databases (gold by default, silver when the workspace adds it in its
   `clavesa.json` "hosted" block, never bronze by default, never a
   database of a personal_data workspace except gold); a per-workspace
   daily scan budget kept by the Lambda; one query at a time per user;
   an audit line per query (who, when, workspace, bytes, SQL hash) in the
   hosted bucket; and only after ADR-035's viewers.

## Consequences

**Positive**

- Dashboards work on the phone for cloud workspaces, with the same
  widgets as the Mac.
- No request can make the hosted Lambda run SQL that nobody deployed.

**Negative and costs**

- A dashboard edited on the Mac shows hosted only after the next
  `clavesa deploy` (the tree copy). The page says "as deployed <time>".
- Every hosted widget query reads the specs to match it (a few small
  files in the tree copy under /tmp).
- The tree copy now holds the dashboard specs (`*.json` in
  `.clavesa/dashboards/` and `dashboards/`): widget SQL and layout, no
  data. They show only after a deploy uploads them.
- A widget over a denied table (web-traffic's silver) fails on the phone
  with the reader's access error. It shows as a widget error, not as an
  empty chart.
- Athena cost: about 6 widgets x 10 opens a day at the 10 MB minimum,
  about $0.10 a month per workspace, less with result reuse.
