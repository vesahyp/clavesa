/**
 * useDatasetColumns — result columns for every dataset on a dashboard.
 *
 * Widget field pickers (X / Y / value) need the column names a dataset's
 * SQL actually returns. The number of datasets is dynamic, so a plain
 * `useDashboardQuery` per dataset would break the rules of hooks;
 * `useQueries` takes a dynamic-length array instead.
 *
 * The query key matches `useDashboardQuery` / `SqlPreview`, so all three
 * share one cache entry per (dir, sql) — running a SQL preview warms the
 * dropdowns and vice versa.
 */

import { useQueries } from "@tanstack/react-query";

import { runDashboardQuery, type DashboardDataset } from "@/lib/queries";

export interface DatasetColumns {
  columns: string[];
  isLoading: boolean;
  error: unknown;
}

export function useDatasetColumns(
  datasets: DashboardDataset[],
): Map<string, DatasetColumns> {
  const results = useQueries({
    queries: datasets.map((d) => ({
      queryKey: ["dashboards", "query", d.dir, d.sql],
      enabled: Boolean(d.dir) && Boolean(d.sql.trim()),
      staleTime: 5 * 60_000,
      retry: false,
      queryFn: () => runDashboardQuery(d.dir, d.sql),
    })),
  });

  const map = new Map<string, DatasetColumns>();
  datasets.forEach((d, i) => {
    const r = results[i];
    map.set(d.name, {
      columns: r?.data?.columns.map((c) => c.name) ?? [],
      isLoading: r?.isLoading ?? false,
      error: r?.error ?? null,
    });
  });
  return map;
}
