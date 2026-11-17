---
adr: 33
title: The hosted UI is the local UI minus writes; every page is in its navigation, and a page that cannot work hosted says what to do instead
date: 2026-11-17
status: Accepted
deciders: Vesa
---

# ADR 033: Hosted UI parity with the local UI, read-only

**Status**: Accepted (2026-11-17, Vesa: go on with the parity plan).
Supersedes ADR-032 section 1 (the four pages and the line "Catalog,
Query, Notebooks, Sources, Credentials, Runner, Bundles and the DAG
editor are not in the hosted navigation"). The rest of ADR-032 stays.

## Context

Vesa, 2026-11-16: "Ideally the local UI and the hosted UI would be
identical except the hosted cannot write."

The hosted app already mounts the whole local router and API
(`ui/src/main.tsx`, `internal/hosted/server.go`), so the hosted and the
local UI run the same code. What differs comes from four limits: the
method guard (`ui.ReadOnlyGuard`, GET and HEAD only), the tree copy
(`HostedTreeFiles`: `clavesa.json`, pipeline `.tf` and tfvars only), the
reader role (reads one workspace, no secrets, no compute), and
local-warehouse workspaces (their data is only on the Mac; the hosted UI
has their published summary).

Today read-only mode hides Query, Credentials and Runner from the
sidebar and redirects their routes to the workspace home. A page that
needs something the hosted side does not have fails with an API error.

The plan with the page-by-page table is `docs/ideas/hosted-local-parity.md`.

## Decision

1. **The rule.** The hosted UI is the local UI minus writes. Every page of
   the local navigation is in the hosted navigation, in the same order.
   Write controls stay hidden (`useReadOnly()`); the server refuses
   writes (ReadOnlyGuard, and ADR-034 for the reads that use POST).
2. **A page that cannot work hosted is not removed and does not
   redirect.** It opens and says in one line why, and what to do instead.
   The alternatives:
   - Credentials: "Credentials are managed on the Mac." Never hosted: the
     credential registry and any secret stay on the Mac.
   - Query, until ADR-034 opens it for serving databases of cloud
     workspaces: "Ad-hoc queries run in `clavesa ui` on the Mac." For a
     local-warehouse workspace that stays the answer (ADR-036).
   - Runner: "The runner image is built and edited on the Mac." The
     requirements file joins the tree copy later as a read-only view.
   - Running notebook cells, source previews, Unity Catalog and the
     Databricks compare in Bundles: they need a Spark kernel or
     credentials that the Lambda must not hold. They show the stored
     metadata, and say that the action runs on the Mac.
   - Every data page of a local-warehouse workspace (Catalog, table
     detail, Dashboards, Query, Backfills): "The data of this workspace is
     on the Mac." until ADR-036 publishes snapshots.
3. **The tree copy grows by allow-list only.** Each slice that needs a
   file adds it to `HostedTreeFiles` by name, never by a deny-list:
   dashboards specs, notebooks with their saved outputs, the source
   registry (references only, no credentials), the runner requirements.
   Terraform state, `.env` files and credential material never go.
4. **Order of the work** (the plan's slices): status pages, navigation
   (this ADR), dashboards on cloud workspaces, notebooks and sources
   read-only, local-warehouse snapshots (ADR-036), per-workspace viewers
   (ADR-035), ad-hoc query on serving databases (ADR-034).

## Consequences

**Positive**

- One UI to learn: a page found on the Mac is in the same place on the
  phone, and a page that cannot work says why instead of an error.
- A fix in a page reaches both surfaces, as before.

**Negative and costs**

- More pages open on the phone, so more read paths are reachable by a
  signed-in user. Each one needs the same care as the 2026-11-17 review
  findings (paths inside the workspace, no raw rows of personal data).
  ADR-035 (viewers) must land before anyone but Vesa gets a session.
- The hosted navigation shows entries that only explain an alternative
  (Credentials, and Query until ADR-034). Some taps lead to a sentence,
  not a page.
- The live check grows: it opens every sidebar entry on a cloud and a
  local workspace, and every slice that adds a page adds to it.
- The tree copy grows, and with it what a deploy uploads to the
  workspace's own bucket.
