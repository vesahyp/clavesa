# Changing HTTP source — re-fetch into history

> **When you have one:** a public HTTP API whose data keeps moving — an RSS-style feed, a "latest N items" endpoint, a leaderboard. You don't get a file per day; you get the *current* state every time you ask. This recipe turns that into a table you can trust.

An `http` source is **full-refetch only** — no partitions, no watermarks. Every `pipeline run` re-fetches the whole URL. That sounds like a limitation. It isn't: a full re-fetch plus the right output mode is exactly what you need to accumulate a moving feed into a durable, deduplicated table — and to reconstruct change history the API never hands you directly.

We'll use the **Hacker News Algolia API** — no auth, no key, and the numbers genuinely move between fetches.

## What you'll end up with

- `stories` — one keyed row per HN story, accumulating across runs. New stories are inserted; stories you've already seen have their score refreshed. This is your **story dimension**.
- `fact_story_snapshot` — one row per story *per fetch*. As you re-run, this becomes a periodic-snapshot fact: the score trajectory of every story over time, something the API itself never exposes.
- A pipeline you can run on a schedule or ad-hoc, as often as you like, that never grows duplicates and never loses old rows.

## Prerequisites

- A workspace per the [README quick-start](../../README.md#quick-start).
- Nothing else — the HN API is public and unauthenticated.

## The recipe

```bash
bin/clavesa workspace init hn-demo
cd hn-demo

# 1. Register the source. The URL has no file extension, so format
#    can't be inferred — pass --format json explicitly.
bin/clavesa source register hn \
  --from 'https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=100' \
  --format json

bin/clavesa pipeline create newsfeed

# 2. The story table. The API returns an envelope object
#    {"hits": [ {...story...}, ... ], ...} on a single line, so Spark
#    reads it as ONE row with a `hits` array column. explode(hits)
#    fans it back out to one row per story.
bin/clavesa node add newsfeed --type transform --name stories
bin/clavesa node edit newsfeed stories --set "sql=
  SELECT
    hit.objectID                         AS objectID,
    hit.title                            AS title,
    hit.url                              AS url,
    parse_url(hit.url, 'HOST')           AS domain,
    hit.author                           AS author,
    CAST(hit.points       AS INT)        AS points,
    CAST(hit.num_comments AS INT)        AS num_comments,
    CAST(hit.created_at_i AS TIMESTAMP)  AS created_at
  FROM (SELECT explode(hits) AS hit FROM hn)"

# 3. Key the output on objectID. This flips the output to merge mode.
bin/clavesa node edit newsfeed stories --output-merge-keys objectID

# 4. Wire the source in. `--as hn` is the table alias the SQL uses.
bin/clavesa source attach newsfeed hn --to stories --as hn

bin/clavesa pipeline run newsfeed
```

The UI equivalent: register the source on `/sources`, create the pipeline and transform in the editor, expand **Output** on the right panel and set Merge Keys to `objectID`.

## Why merge, and not append or replace

The output mode is the whole recipe. The same source, the same SQL, three different tables:

- **`replace`** (the default) — every run *overwrites* the table. You only ever see the last 100 stories fetched. As HN's feed rolls forward, older stories silently vanish. Fine for a "current snapshot" view; useless as a record.
- **`append`** — every run *adds* all 100 hits. Consecutive runs overlap heavily (the feed moves slowly), so the table fills with duplicate `objectID`s.
- **`merge`** on `objectID` — new stories `INSERT`, stories already present `UPDATE` in place to their latest `points` / `num_comments`. The table **grows by exactly the stories you hadn't seen before** and stays deduplicated. This is what you want for a moving feed.

`--output-merge-keys objectID` sets `mode = "merge"` for you — no `--output-mode` needed.

## Model the change: a snapshot fact

`stories` keeps the *current* state of each story. But `points` is the interesting part, and `points` changes between fetches. To keep that history, add a second transform that **appends** every fetch instead of merging it.

```bash
bin/clavesa node add newsfeed --type transform --name fact_story_snapshot
bin/clavesa node edit newsfeed fact_story_snapshot --set "sql=
  SELECT
    hit.objectID                          AS objectID,
    CAST(hit.points       AS INT)         AS points,
    CAST(hit.num_comments AS INT)         AS num_comments,
    ROW_NUMBER() OVER (ORDER BY hit.created_at_i DESC) AS feed_rank,
    current_timestamp()                   AS fetched_at
  FROM (SELECT explode(hits) AS hit FROM hn)"

bin/clavesa node edit newsfeed fact_story_snapshot --output-mode append
bin/clavesa source attach newsfeed hn --to fact_story_snapshot --as hn
```

`fetched_at` stamps every row with the run time, so the grain is `(objectID, fetched_at)` — one row per story per fetch. Run the pipeline a few times over a day and you have a **periodic-snapshot fact**: `points` over time per story, peak `feed_rank`, time-to-100-points. The HN API has no "score history" endpoint — you reconstructed it from full re-fetches.

That's the pattern, beyond Hacker News: **a full-refetch HTTP source + an `append` keyed on `(entity, fetched_at)` is a snapshot fact table.** Pair it with a merge-keyed dimension (`stories`) and you have a star schema fed entirely by an API that only ever tells you "here's right now."

## Run it

```bash
# Run three times, spaced well apart — ideally 15+ minutes, or hours —
# so the HN feed actually moves between fetches. Back-to-back runs a
# minute apart will usually fetch an identical 100 stories, and then
# `stories` won't grow (every objectID merge-matches; nothing new to
# insert). The feed only adds a handful of stories every few minutes.
bin/clavesa pipeline run newsfeed
# ... wait ...
bin/clavesa pipeline run newsfeed
# ... wait ...
bin/clavesa pipeline run newsfeed
```

## What you should see

- `pipeline run` reports both `stories` and `fact_story_snapshot` as `ok`.
- `/` (Catalog) lists `stories` and `fact_story_snapshot` under `clavesa_hn-demo__newsfeed`.
- **`stories`** — after run 1, ~100 rows. After a later run, the row count grows by *only the new stories* since the previous run — a handful, not another 100; overlapping `objectID`s were updated in place. (If two runs were close together the feed may not have moved at all, and the count stays flat — that's correct, not a bug.) `SELECT count(*), count(DISTINCT objectID)` returns two equal numbers — no duplicates, ever. A story seen in several runs shows its **most recent** `points`.
- **`fact_story_snapshot`** — grows by ~100 every run. A story fetched in three runs has three rows, with three `fetched_at` values and (usually) three different `points`. This is the history `stories` throws away.
- TableDetail **snapshot timeline**: one snapshot per run. For `stories`, run 1 is `append +N` (initial create) and later runs are `overwrite` (the merge rewrites via copy-on-write). For `fact_story_snapshot`, every run is `append +~100`.
- The **lineage** panel on each table shows the producing node tracing back to the `hn` source.

## Going further — a domain rollup

The story dimension carries a `domain` column (`parse_url` of the URL). Join the fact to it for a gold rollup — "which sites get traction on HN, by day":

```bash
bin/clavesa node add newsfeed --type transform --name domain_traction_daily
bin/clavesa node edit newsfeed domain_traction_daily --set "sql=
  SELECT
    s.domain                    AS domain,
    date(f.fetched_at)          AS day,
    count(DISTINCT f.objectID)  AS stories,
    round(avg(f.points), 1)     AS avg_points,
    max(f.points)               AS top_points
  FROM snapshots f
  JOIN stories s ON f.objectID = s.objectID
  WHERE s.domain IS NOT NULL
  GROUP BY s.domain, date(f.fetched_at)"

bin/clavesa node connect newsfeed --from fact_story_snapshot --to domain_traction_daily --input snapshots
bin/clavesa node connect newsfeed --from stories --to domain_traction_daily --input stories
bin/clavesa pipeline run newsfeed
```

Default `replace` mode is right here — a rollup is fully recomputed from the fact each run.

## Troubleshooting

**`stories` only ever has ~100 rows; old stories disappear.** The output isn't keyed — it's running in `replace` mode. Confirm `--output-merge-keys objectID` was set; check the transform's `.tf` has `mode = "merge"` and a non-empty `merge_keys`.

**Row count jumps by ~100 every run, full of duplicate `objectID`s.** That's `append` mode on a table you meant to be `merge`. Re-run `node edit ... --output-merge-keys objectID`.

**`stories` has the same row count after two runs.** The HN feed didn't move between the fetches — every `objectID` in the second fetch already existed, so the merge updated all 100 in place and inserted nothing. Expected for runs a few minutes apart; space them 15+ minutes (or hours) to see the table grow. The snapshot timeline still shows a new `overwrite` snapshot each run — the scores were refreshed even when the row count held.

**`AnalysisException: cannot resolve 'hits'`.** The source format wasn't set to `json`. The URL has no `.json` extension, so format can't be inferred — re-register with `--format json`.

**All story columns come back `null`.** You selected `objectID` / `title` directly off `hn` instead of off the exploded row. The source DataFrame has a single row whose `hits` column is an array; you must `explode(hits)` and project from the exploded alias (`hit.objectID`, `hit.title`, …).

**`source preview hn` shows a single row with a giant `hits` blob.** Expected. Preview reads the envelope as one JSON line and does not explode it. The explode happens inside the transform — run the pipeline and inspect `stories` instead.

**MERGE fails: "matched a single row from target with multiple rows of source".** The fetch contained a duplicate `objectID` (rare for HN). De-dupe in the SQL with a window: `ROW_NUMBER() OVER (PARTITION BY hit.objectID ORDER BY hit.created_at_i DESC)` and keep `= 1`.

**A retried run added a near-duplicate batch to `fact_story_snapshot`.** `append` mode is at-least-once: a retry re-appends with a fresh `fetched_at`. For a snapshot fact this is usually acceptable — dedupe in rollups with a window over `fetched_at`. If exact-once matters, key the fact on `(objectID, fetched_at)` with `--output-merge-keys` instead.

## See also

- [merge-cdf](merge-cdf.md) — the merge-keyed dimension pattern in depth; `stories` here is a merge dimension.
- [scheduled-rollup](scheduled-rollup.md) — put `newsfeed` on a cron so the snapshot fact fills itself.
