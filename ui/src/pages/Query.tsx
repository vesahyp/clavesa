/**
 * Query — top-level workspace SQL editor.
 *
 * The minimum-viable "ask a question of your catalog" surface — what TODO
 * bucket 10 sketched as part of the interactive-query gap. Notebooks (Slice 1)
 * cover the multi-cell + persistent case; /query is the single-cell, zero-
 * persistence equivalent. SQL-only by design (Python scratchpad belongs in
 * notebooks).
 */

import { useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { useChrome } from "@/components/PageChrome";
import { AdhocQuery } from "@/components/AdhocQuery";

const DEFAULT_SQL = `-- Workspace catalog query. Tables are addressed
-- <catalog>__<schema>.<table>, e.g.
--   SELECT * FROM clavesa_web_traffic__gold.fact_page_views LIMIT 20
-- Start by listing the schemas:
SHOW DATABASES`;

export function Query() {
  // Allow ?sql=... seed via URL so the table-detail "Open in /query" link
  // can deep-link a pre-filled editor. Falls back to the friendly default.
  const [params] = useSearchParams();
  const seed = params.get("sql") || DEFAULT_SQL;
  const [initialSql] = useState(seed);

  useChrome(
    useMemo(
      () => ({
        breadcrumbs: [{ label: "Query", to: "/query" }],
      }),
      [],
    ),
  );

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
      <div className="mb-4">
        <h1 className="font-mono text-2xl font-semibold tracking-tight">
          Query
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          SparkSQL against the workspace catalog, validated for Trino/Athena
          portability on every warehouse so what runs here also runs in cloud
          dashboards. It runs on Athena (transpiled to Trino) on a cloud
          warehouse and on Spark locally. Single-shot and not saved; for
          multi-cell Spark exploration with persistent Python state, use{" "}
          <a className="text-primary underline" href="/notebooks">
            Notebooks
          </a>
          .
        </p>
      </div>
      <AdhocQuery initialSql={initialSql} showCatalog />
    </div>
  );
}
