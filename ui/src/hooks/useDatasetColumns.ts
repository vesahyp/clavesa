/**
 * useDatasetColumns — result columns (and the rows behind them) for every
 * dataset on a dashboard.
 *
 * Widget field pickers (X / Y / value) need the column names a dataset's
 * SQL actually returns. The number of datasets is dynamic, so a plain
 * `useDashboardQuery` per dataset would break the rules of hooks;
 * `useQueries` takes a dynamic-length array instead.
 *
 * The query key matches `useDashboardQuery`, so both share one cache
 * entry per (dir, sql, params) — running a widget query warms the
 * dropdowns and vice versa.
 *
 * The hook also exposes `rows` / `rowCount` / `truncated` straight off
 * the same result, so the widget drawer can render a results preview
 * without firing a second request.
 *
 * `params` carries the substituted values for any `{{name}}` tokens in
 * the dataset SQL. Without them the editor's column probe would 400 on
 * any dataset that references a control placeholder; the editor passes
 * a synthesized default map from the dashboard's declared controls.
 */

import { useRef } from "react";
import { useQueries } from "@tanstack/react-query";

import {
  paramsCacheKey,
  runDashboardQuery,
  type DashboardDataset,
  type ServedInfo,
} from "@/lib/queries";

export interface DatasetColumn {
  name: string;
  type: string;
}

export interface DatasetColumns {
  columns: DatasetColumn[];
  /** Result rows from the same query — empty when the query hasn't returned yet. */
  rows: string[][];
  rowCount: number;
  truncated: boolean;
  /** ADR-024 engine identity from the last result — absent on old servers. */
  served?: ServedInfo;
  /** True once a result has arrived at least once for this dataset. */
  hasData: boolean;
  isLoading: boolean;
  error: unknown;
}

export function useDatasetColumns(
  datasets: DashboardDataset[],
  params?: Record<string, string>,
): Map<string, DatasetColumns> {
  const paramsKey = paramsCacheKey(params);
  const results = useQueries({
    queries: datasets.map((d) => ({
      queryKey: ["dashboards", "query", d.dir, d.sql, paramsKey],
      enabled: Boolean(d.dir) && Boolean(d.sql.trim()),
      staleTime: 5 * 60_000,
      retry: false,
      queryFn: () => runDashboardQuery(d.dir, d.sql, params),
    })),
  });

  // Sticky cache of the last-known-good column set per dataset name. The
  // chart-first drawer's auto-preview fires queries against partial SQL
  // mid-edit; those return errors and would otherwise blink the field
  // pickers to empty. Reusing the previous good result keeps the UI
  // stable through transient errors while the SQL settles.
  //
  // Rows are NOT made sticky — the preview should reflect what the
  // current SQL returns, including an empty result.
  const lastGoodRef = useRef(new Map<string, DatasetColumn[]>());
  const liveNames = new Set(datasets.map((d) => d.name));
  for (const k of Array.from(lastGoodRef.current.keys())) {
    if (!liveNames.has(k)) lastGoodRef.current.delete(k);
  }

  const map = new Map<string, DatasetColumns>();
  datasets.forEach((d, i) => {
    const r = results[i];
    const fresh =
      r?.data?.columns.map((c) => ({ name: c.name, type: c.type })) ?? [];
    if (fresh.length > 0) lastGoodRef.current.set(d.name, fresh);
    const sticky = lastGoodRef.current.get(d.name) ?? [];
    map.set(d.name, {
      columns: fresh.length > 0 ? fresh : sticky,
      rows: r?.data?.rows ?? [],
      rowCount: r?.data?.row_count ?? 0,
      truncated: r?.data?.truncated ?? false,
      served: r?.data?.served,
      hasData: r?.data != null,
      isLoading: r?.isLoading ?? false,
      error: r?.error ?? null,
    });
  });
  return map;
}
