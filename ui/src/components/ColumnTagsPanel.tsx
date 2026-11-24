/**
 * ColumnTagsPanel — the workspace's column tags (docs/ideas/column-tags.md),
 * as clavesa.json and the pipelines declare them: the twin of `clavesa
 * workspace access show`. It names hidden columns, so it shows only in
 * the Mac's own UI: the hosted (read-only) server has no
 * /workspace/access route, and the panel is not even asked for there.
 */

import { useQuery } from "@tanstack/react-query";

import { request } from "@/api/client";
import { runtimeConfig } from "@/lib/runtimeConfig";

interface AccessColumn {
  name: string;
  tag: string;
}

interface AccessTable {
  pipeline: string;
  database: string;
  table: string;
  table_tag: string;
  columns: AccessColumn[];
}

export interface AccessReport {
  enabled: boolean;
  tag_key?: string;
  tags: string[];
  unreviewed: string;
  audiences: Record<string, string[]>;
  tables: AccessTable[];
  problems: string[];
}

function Tag({ value, unreviewed }: { value: string; unreviewed: string }) {
  const tone =
    value === "open"
      ? "border-status-success/40 text-status-success"
      : value === unreviewed
        ? "border-border text-muted-foreground"
        : "border-status-running/40 text-status-running";
  return <span className={`rounded border px-1.5 py-0.5 font-mono text-[11px] ${tone}`}>{value}</span>;
}

export function ColumnTagsPanel() {
  const macOnly = runtimeConfig.mode !== "hosted" && !runtimeConfig.readOnly;
  const q = useQuery({
    queryKey: ["workspace-access"],
    queryFn: () => request<AccessReport>("/workspace/access"),
    enabled: macOnly,
    staleTime: 60_000,
    retry: false,
  });
  const rep = q.data;
  if (!macOnly || !rep || (!rep.enabled && rep.problems.length === 0)) return null;
  return (
    <section data-testid="column-tags" className="rounded-lg border border-border p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-sm font-medium">Column tags</h2>
        {rep.tag_key && <span className="font-mono text-xs text-muted-foreground">{rep.tag_key}</span>}
      </div>
      <p className="mt-1 text-xs text-muted-foreground">
        Hosted viewers read what is tagged <span className="font-mono">open</span>. A table or column with no tag is{" "}
        <span className="font-mono">{rep.unreviewed}</span>, which no viewer reads. Shown only here on the Mac.
      </p>
      {rep.problems.length > 0 && (
        <ul data-testid="column-tags-problems" className="mt-2 space-y-0.5 text-xs text-status-failed">
          {rep.problems.map((p) => (
            <li key={p}>{p}</li>
          ))}
        </ul>
      )}
      <ul className="mt-3 space-y-3">
        {rep.tables.map((t) => (
          <li key={`${t.database}.${t.table}`} data-testid="column-tags-table" data-table={t.table}>
            <div className="flex flex-wrap items-center gap-2 text-sm">
              <span className="min-w-0 break-all font-mono">
                {t.database}.{t.table}
              </span>
              <Tag value={t.table_tag} unreviewed={rep.unreviewed} />
            </div>
            {t.columns.length > 0 && (
              <ul className="mt-1 space-y-1 pl-3">
                {t.columns.map((c) => (
                  <li key={c.name} data-testid="column-tags-column" className="flex flex-wrap items-center gap-2 text-xs">
                    <span className="break-all font-mono">{c.name}</span>
                    <Tag value={c.tag} unreviewed={rep.unreviewed} />
                  </li>
                ))}
              </ul>
            )}
          </li>
        ))}
      </ul>
    </section>
  );
}
