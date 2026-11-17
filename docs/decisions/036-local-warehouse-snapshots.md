---
adr: 36
title: A local-warehouse workspace publishes a catalog snapshot and its dashboards' widget results at run end; ad-hoc query stays on the Mac
date: 2026-11-17
status: Accepted
deciders: Vesa
---

# ADR 036: Local-warehouse snapshots for hosted pages

**Status**: Accepted (2026-11-17, Vesa: go on with the parity plan; parity slice 5).

## Context

Seven of the eleven hosted workspaces keep their data on the Mac (the
nightly site-traffic pipelines). The hosted UI shows them from the
summary the Mac publishes at the end of each run (ADR-032 section 2),
with a drill-down of runs, node rows, timings and failed-run logs
(`SummaryDrill`). Their data pages (Catalog, tables, Dashboards) say
"The data of this workspace is on the Mac" (ADR-033).

The hosted side cannot reach the Mac. To show a local workspace's
catalog and dashboards on the phone, the Mac must publish them.

## Decision

1. **What is published, with the summary, at the end of every local
   `clavesa pipeline run`** (the existing publish through the publisher
   role, `summaries/<name>.json`, or a sibling object):
   - **A catalog snapshot**: the catalog listing exactly as `GET
     /workspace/tables` answers on the Mac (tables, schemas, owning
     pipelines; no rows), and each table's newest 20 Delta snapshots
     (commit times and row counts), as the Catalog page reads them.
   - **Dashboard widget results**: each dataset of each dashboard in
     `.clavesa/dashboards/`, run with its controls' defaults, capped at
     the hosted row cap (1000 rows), with the specs themselves.
   - The time the snapshot was taken.
   The hosted Lambda answers those three reads from the stored snapshot
   and runs no SQL for these workspaces: a widget whose dataset is not in
   the snapshot is 403.
2. **No raw rows and no ad-hoc query.** Table previews, source previews
   and the Query page stay on the Mac for these workspaces ("Query runs
   on the Mac for this workspace").
3. **Personal data.** A workspace whose `clavesa.json` says
   `personal_data` publishes no widget results (a widget's SQL can read
   any schema); its catalog snapshot still goes, since it holds no rows.
4. **Size.** The published object stays under a fixed cap (proposed
   2 MB); widgets past the cap are left out with a note, never truncated
   silently.
5. **Freshness.** Pages show "as of <time>" for snapshot data. A
   dashboard with controls shows the default selection only: whatever
   the controls say, the stored result is the defaults' one.

## Consequences

- The phone shows the site-traffic dashboards and catalog with the data
  of the last run, at no Athena cost.
- The publish grows from about 100 to 160 KB to up to the cap, once per
  nightly run per workspace (S3 PUT, about $0).
- Widget results are aggregates the dashboard already shows, but they are
  copied to the personal account's hosted bucket. A workspace whose
  dashboards show personal data must not enable this (point 3 covers the
  registry flag; the dashboards themselves are the owner's choice).
- The local run takes longer by the time to run every widget once
  (seconds for the current dashboards).

## Update 2026-11-17 (slice 5 review)

Changed before it shipped (v2.27.9):

1. **Opt-in per workspace, off by default.** `"hosted": {"snapshot":
   true}` in `clavesa.json` turns it on (`clavesa workspace hosted enable
   --snapshot`; `--no-snapshot` turns it off). An upgrade publishes no
   snapshot until Vesa turns it on for that workspace.
2. **Personal data fails closed.** Values (widget results) are published
   only when `clavesa.json` says `"personal_data": false` explicitly;
   absent or true publishes names, types and counts only. The registry
   marks the seven traffic workspaces `personal_data`, and the Lambda
   serves no widget result for such an entry, whatever the Mac sent.
3. **The snapshot never costs the summary.** It has its own 15-second
   budget and publishes what finished, with notes; the role assume and
   the upload have their own 15 seconds.
4. **No path on the Mac.** Every catalog `location` is dropped, paths
   under the workspace become relative, and the publish drops a
   snapshot whose bytes still name the workspace root.

