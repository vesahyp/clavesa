/**
 * dashboards.ts — saved SQL dashboards over the catalog: specs, controls,
 * widget-query execution, and the create/save/delete mutations.
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { request } from "@/api/client";
import { ServedInfo, requestParsed, requestVoid } from "./core";

const DashboardSummary = z.object({
  slug: z.string(),
  title: z.string(),
});
export type DashboardSummary = z.infer<typeof DashboardSummary>;

const DashboardsListResponse = z.object({
  dashboards: z.array(DashboardSummary).default([]),
});

const DashboardWidgetLayout = z.object({
  x: z.number(),
  y: z.number(),
  w: z.number(),
  h: z.number(),
});

// DashboardDataset is a named, reusable SQL query. Each carries its own
// pipeline dir, so one dashboard can blend tables from multiple pipelines
// and mix local + cloud. Widgets bind to a dataset by name.
const DashboardDataset = z.object({
  name: z.string(),
  dir: z.string(),
  sql: z.string(),
});
export type DashboardDataset = z.infer<typeof DashboardDataset>;

const DashboardWidget = z.object({
  id: z.string(),
  // big_number | line | bar | table — the UI tolerates unknown types
  // (renders as a blank card) so adding a widget type doesn't break old
  // builds.
  type: z.string(),
  title: z.string(),
  // dataset references a DashboardDataset by name; the widget renders
  // that query's result.
  dataset: z.string().optional().default(""),
  value_field: z.string().optional().default(""),
  x_field: z.string().optional().default(""),
  y_field: z.string().optional().default(""),
  // series_fields are the value columns a stacked_bar stacks per x;
  // line_field is the line series of a bar_line combo.
  series_fields: z.array(z.string()).optional().default([]),
  line_field: z.string().optional().default(""),
  // world_map-only: ISO 3166-1 alpha-2 or alpha-3 country code column,
  // and an optional column shown in the hover tooltip.
  region_field: z.string().optional().default(""),
  tooltip_field: z.string().optional().default(""),
  layout: DashboardWidgetLayout,
});
export type DashboardWidget = z.infer<typeof DashboardWidget>;

// DashboardControl is a dashboard-level filter substituted into dataset
// SQL at render time. `time_range` writes `{{<name>.start}}` and
// `{{<name>.end}}`; `select` writes `{{<name>}}`. Backend tolerates
// unknown control types (renders nothing) so future control kinds don't
// break old builds.
const DashboardControl = z.object({
  name: z.string(),
  type: z.string(),
  label: z.string().optional().default(""),
  default: z.string().optional().default(""),
  dir: z.string().optional().default(""),
  sql: z.string().optional().default(""),
  options: z.array(z.string()).optional().default([]),
});
export type DashboardControl = z.infer<typeof DashboardControl>;

const Dashboard = z.object({
  slug: z.string(),
  title: z.string(),
  datasets: z.array(DashboardDataset).default([]),
  widgets: z.array(DashboardWidget).default([]),
  controls: z.array(DashboardControl).default([]),
  updated_at: z.string().optional().default(""),
});
export type Dashboard = z.infer<typeof Dashboard>;

/**
 * resolveControlDefaults — synthesize a params map from a dashboard's
 * declared control defaults. Mirrors the Go resolveControlDefaults
 * helper. Used by the editor when probing dataset columns — without
 * substituted values, a dataset referencing `{{name}}` would 400 on
 * the column-fetch call before the editor can even render its field
 * pickers.
 */
export function resolveControlDefaults(
  controls: DashboardControl[],
): Record<string, string> {
  const out: Record<string, string> = {};
  const now = new Date();
  for (const c of controls) {
    if (c.type === "time_range") {
      const { start, end } = resolveTimePreset(c.default || "last_30d", now);
      out[`${c.name}.start`] = start;
      out[`${c.name}.end`] = end;
    } else if (c.type === "select") {
      if (c.default) {
        out[c.name] = c.default;
      } else if (c.options.length > 0) {
        out[c.name] = c.options[0];
      }
    }
  }
  return out;
}

function resolveTimePreset(
  preset: string,
  now: Date,
): { start: string; end: string } {
  const end = now.toISOString();
  let ms = 30 * 24 * 60 * 60 * 1000;
  switch (preset) {
    case "last_24h":
      ms = 24 * 60 * 60 * 1000;
      break;
    case "last_7d":
      ms = 7 * 24 * 60 * 60 * 1000;
      break;
    case "last_90d":
      ms = 90 * 24 * 60 * 60 * 1000;
      break;
    case "last_30d":
    default:
      ms = 30 * 24 * 60 * 60 * 1000;
  }
  return { start: new Date(now.getTime() - ms).toISOString(), end };
}

const DashboardQueryColumn = z.object({
  name: z.string(),
  type: z.string().optional().default(""),
});

const DashboardQueryResult = z.object({
  columns: z.array(DashboardQueryColumn).default([]),
  rows: z.array(z.array(z.string())).default([]),
  row_count: z.number(),
  truncated: z.boolean(),
  // The /dashboards/query handler writes the provider QueryResult through
  // verbatim, so the ADR-024 stamp rides along here too.
  served: ServedInfo.optional(),
});
export type DashboardQueryResult = z.infer<typeof DashboardQueryResult>;

/** GET /api/dashboards — workspace's dashboard list. */
export function useDashboards() {
  return useQuery({
    queryKey: ["dashboards"],
    queryFn: async () => {
      const raw = await request<unknown>("/dashboards");
      return DashboardsListResponse.parse(raw);
    },
  });
}

/** GET /api/dashboards/:slug — full dashboard spec. */
export function useDashboard(slug: string) {
  return useQuery({
    queryKey: ["dashboards", slug],
    enabled: Boolean(slug),
    queryFn: () =>
      requestParsed(`/dashboards/${encodeURIComponent(slug)}`, Dashboard, {
        errorLabel: `GET /dashboards/${slug}`,
      }),
  });
}

/**
 * POST /api/dashboards/query — run one widget's SQL.
 *
 * `dir` scopes the Provider dispatch (cloud vs local) and is required by
 * the backend regardless of which provider serves the request — both
 * fail the same way (ADR-014 parity) when it's absent. The hook
 * gates `enabled` on `dir` for the same reason: no point firing a
 * request that will 400.
 *
 * Stale time bumped to 5 minutes so flipping between widgets / tabs
 * doesn't re-hit Athena every time. Refresh on demand by invalidating
 * the query.
 */
export function useDashboardQuery(
  sql: string,
  dir: string,
  params?: Record<string, string>,
) {
  // Cache key includes a stable string of the params so distinct
  // control selections cache distinctly. Object identity would defeat
  // the cache; sorted JSON gives a deterministic key. Empty/undefined
  // params collapse to "" so dashboards without controls keep the same
  // cache key shape they had before.
  const paramsKey = paramsCacheKey(params);
  return useQuery({
    queryKey: ["dashboards", "query", dir, sql, paramsKey],
    enabled: Boolean(sql) && Boolean(dir),
    meta: { spark: true },
    staleTime: 5 * 60_000,
    retry: false,
    queryFn: () => runDashboardQuery(dir, sql, params),
  });
}

/**
 * paramsCacheKey — deterministic cache-key fragment for a control-params
 * map. Exported because useDatasetColumns builds the same
 * `["dashboards","query",dir,sql,paramsKey]` key; both sides MUST derive
 * it identically or the shared cache entry silently splits (G P3-5).
 */
export function paramsCacheKey(
  params: Record<string, string> | undefined,
): string {
  if (!params) return "";
  const keys = Object.keys(params).sort();
  if (keys.length === 0) return "";
  return keys.map((k) => `${k}=${params[k]}`).join("&");
}

/**
 * runDashboardQuery — imperative form of the widget-SQL query.
 *
 * Same request as `useDashboardQuery`'s queryFn, factored out so
 * button-driven callers (the dataset SQL preview) and `useQueries`-based
 * callers can share it. Reuse the `["dashboards","query",dir,sql,params]`
 * query key when caching so all three paths hit one cache entry.
 */
export async function runDashboardQuery(
  dir: string,
  sql: string,
  params?: Record<string, string>,
): Promise<DashboardQueryResult> {
  const body: { dir: string; sql: string; params?: Record<string, string> } = {
    dir,
    sql,
  };
  if (params && Object.keys(params).length > 0) {
    body.params = params;
  }
  return requestParsed("/dashboards/query", DashboardQueryResult, {
    method: "POST",
    body: JSON.stringify(body),
    errorLabel: "POST /dashboards/query",
  });
}

// DashboardInput is the body of a create/replace — the spec without the
// server-stamped `updated_at`.
export interface DashboardInput {
  slug: string;
  title: string;
  datasets: DashboardDataset[];
  widgets: DashboardWidget[];
  controls: DashboardControl[];
}

/** POST /api/dashboards — create a dashboard (409 if the slug is taken). */
export async function createDashboard(d: DashboardInput): Promise<Dashboard> {
  return requestParsed("/dashboards", Dashboard, {
    method: "POST",
    body: JSON.stringify(d),
    errorLabel: "POST /dashboards",
  });
}

/** PUT /api/dashboards/:slug — create or replace a dashboard. */
export async function saveDashboard(
  slug: string,
  d: DashboardInput,
): Promise<Dashboard> {
  return requestParsed(`/dashboards/${encodeURIComponent(slug)}`, Dashboard, {
    method: "PUT",
    body: JSON.stringify(d),
    errorLabel: `PUT /dashboards/${slug}`,
  });
}

/** DELETE /api/dashboards/:slug. */
export async function deleteDashboard(slug: string): Promise<void> {
  return requestVoid(`/dashboards/${encodeURIComponent(slug)}`, {
    method: "DELETE",
    errorLabel: `DELETE /dashboards/${slug}`,
  });
}
