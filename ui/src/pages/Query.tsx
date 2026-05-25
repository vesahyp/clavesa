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

const DEFAULT_SQL = `-- Workspace catalog query.
-- Tables live at clavesa.<workspace>__<schema>.<table>.
-- Example: SELECT * FROM clavesa.\`information_schema\`.\`tables\` LIMIT 20;
SHOW NAMESPACES IN clavesa`;

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
          Ad-hoc SparkSQL against the workspace catalog. No persistence — for a
          multi-cell scratchpad with persistent Python state, use{" "}
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
