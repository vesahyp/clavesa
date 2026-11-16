/**
 * Stats — how the hosted UI itself is used, and where it breaks (ADR-032
 * section 4, "Tracking and errors"). Hosted mode only, at /stats beside
 * the Workspaces page; lazy, so recharts stays out of the other pages.
 *
 * Data: GET /api/stats, the data bucket's stats/ui.json, one row per
 * Helsinki day written by the site-traffic workspace's traffic pipeline
 * (node ui_json, site/analytics/clavesa) from the distribution's
 * CloudFront logs. The pipeline does every aggregation; this page only
 * picks the window, sums days and draws. Every number with a history is
 * a chart; the one table is the list of distinct errors, rows to act on.
 * Bot traffic (the scripted checks among it) is left out by the pipeline.
 */

import { dayMonth } from "@/lib/clock";
import { useMemo, useState, type ReactElement } from "react";
import { useQuery } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { z } from "zod";

import { RootHeader, RootNav } from "@/components/RootNav";
import { requestParsed } from "@/lib/queries/core";
import { cn } from "@/lib/utils";

const count = z.number().int().nonnegative().catch(0);
const optNum = z.number().nullable().optional().catch(null);

export const StatsDay = z.object({
  date: z.string(),
  sessions: count,
  pageviews: count,
  events: count,
  errors: count,
  pages: z.record(z.string(), count).nullable().catch({}),
  devices: z
    .object({ mobile: count, desktop: count, tablet: count })
    .catch({ mobile: 0, desktop: 0, tablet: 0 }),
  vitals: z
    .object({ lcpP75: optNum, inpP75: optNum, clsP75: optNum })
    .catch({ lcpP75: null, inpP75: null, clsP75: null }),
  errorList: z
    .array(
      z.object({
        msg: z.string().catch(""),
        type: z.string().catch("error"),
        path: z.string().catch(""),
        n: count,
      }),
    )
    .nullable()
    .catch([]),
});
export type StatsDay = z.infer<typeof StatsDay>;

const StatsResponse = z.array(StatsDay);

function useHostedStats() {
  return useQuery({
    queryKey: ["hosted-stats"],
    queryFn: () => requestParsed("/stats", StatsResponse, { errorLabel: "GET /stats" }),
    staleTime: 5 * 60_000,
  });
}

const WINDOWS = [7, 30, 90] as const;
type Window = (typeof WINDOWS)[number];

/** The window's days, oldest first, as YYYY-MM-DD in Helsinki time (the
 * pipeline's day boundary), each with its row or zeros. */
function windowDays(rows: StatsDay[], days: Window): StatsDay[] {
  const byDate = new Map(rows.map((r) => [r.date, r]));
  const fmt = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Helsinki" });
  const out: StatsDay[] = [];
  const now = Date.now();
  for (let i = days - 1; i >= 0; i--) {
    const date = fmt.format(new Date(now - i * 86_400_000));
    out.push(
      byDate.get(date) ?? {
        date,
        sessions: 0,
        pageviews: 0,
        events: 0,
        errors: 0,
        pages: {},
        devices: { mobile: 0, desktop: 0, tablet: 0 },
        vitals: { lcpP75: null, inpP75: null, clsP75: null },
        errorList: [],
      },
    );
  }
  return out;
}

function sum(rows: StatsDay[], f: (r: StatsDay) => number) {
  return rows.reduce((a, r) => a + f(r), 0);
}

export function Stats() {
  const { data, error, isLoading } = useHostedStats();
  const [days, setDays] = useState<Window>(30);
  const rows = useMemo(() => windowDays(data ?? [], days), [data, days]);

  const topPages = useMemo(() => {
    const m = new Map<string, number>();
    for (const r of rows) for (const [p, n] of Object.entries(r.pages ?? {})) m.set(p, (m.get(p) ?? 0) + n);
    return [...m.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([path, n]) => ({ path, n }));
  }, [rows]);

  const errorRows = useMemo(() => {
    const m = new Map<string, { msg: string; type: string; path: string; n: number; last: string }>();
    for (const r of rows)
      for (const e of r.errorList ?? []) {
        const k = `${e.msg}\u0000${e.path}\u0000${e.type}`;
        const cur = m.get(k);
        if (cur) {
          cur.n += e.n;
          cur.last = r.date > cur.last ? r.date : cur.last;
        } else m.set(k, { ...e, last: r.date });
      }
    return [...m.values()].sort((a, b) => b.n - a.n || b.last.localeCompare(a.last));
  }, [rows]);

  const label = `${days} days`;
  return (
    <div className="min-h-screen bg-background text-foreground">
      <RootHeader title="Site usage" />
      <main
        data-testid="stats-page"
        className="mx-auto flex max-w-3xl flex-col gap-4 px-4 py-4 pb-[calc(4.75rem+env(safe-area-inset-bottom))] md:px-6 md:py-6 md:pb-6"
      >
        <div className="flex items-center gap-2">
          <p className="flex-1 text-sm text-muted-foreground">
            How this UI is used, from its own CloudFront logs. Bots and check runs are left out.
            Updated nightly.
          </p>
          <div className="flex shrink-0 rounded-md border border-border p-0.5" role="group" aria-label="Window">
            {WINDOWS.map((w) => (
              <button
                key={w}
                type="button"
                data-testid={`stats-window-${w}`}
                onClick={() => setDays(w)}
                className={cn(
                  "min-h-11 min-w-11 rounded px-2 text-xs tabular-nums",
                  w === days ? "bg-muted text-foreground" : "text-muted-foreground",
                )}
              >
                {w}d
              </button>
            ))}
          </div>
        </div>

        {isLoading && <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />}
        {error && (
          <div data-testid="stats-error" className="text-sm text-status-failed">
            {error instanceof Error ? error.message : String(error)}
          </div>
        )}
        {data && data.length === 0 && (
          <div data-testid="stats-empty" className="text-sm text-muted-foreground">
            No stats yet. They appear after the next run of the site-traffic pipeline.
          </div>
        )}

        {data && data.length > 0 && (
          <>
            <ChartCard
              testid="stats-sessions"
              title="Sessions per day"
              total={`${sum(rows, (r) => r.sessions)} in ${label}`}
            >
              <DailyBars rows={rows} dataKey="sessions" name="Sessions" />
            </ChartCard>
            <ChartCard
              testid="stats-pageviews"
              title="Page views per day"
              total={`${sum(rows, (r) => r.pageviews)} in ${label}`}
            >
              <DailyBars rows={rows} dataKey="pageviews" name="Page views" />
            </ChartCard>
            <ChartCard testid="stats-pages" title="Top pages" total={`page views, ${label}`}>
              {topPages.length === 0 ? (
                <Empty />
              ) : (
                <TopPages data={topPages} />
              )}
            </ChartCard>
            <ChartCard testid="stats-devices" title="Sessions by device" total={label}>
              <DeviceBars rows={rows} />
            </ChartCard>
            <ChartCard testid="stats-vitals" title="Web vitals, p75" total="LCP and INP in ms">
              <VitalsLines rows={rows} />
            </ChartCard>
            <ChartCard
              testid="stats-errors"
              title="Errors per day"
              total={`${sum(rows, (r) => r.errors)} in ${label}`}
            >
              <DailyBars rows={rows} dataKey="errors" name="Errors" color="failed" />
            </ChartCard>
            <ErrorsTable rows={errorRows} label={label} />
          </>
        )}
      </main>
      <RootNav />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Charts. Recharts writes colors as SVG attributes, which do not resolve
// CSS var(), so the theme tokens are read once as literal colors.
// ---------------------------------------------------------------------------

function themeColor(token: string, fallback: string): string {
  if (typeof document === "undefined") return fallback;
  return getComputedStyle(document.documentElement).getPropertyValue(token).trim() || fallback;
}

function useColors() {
  return useMemo(
    () => ({
      primary: themeColor("--primary", "#2563eb"),
      failed: themeColor("--status-failed", "#ef4444"),
      // Devices and vitals: the primary, then two hues apart from it.
      series: [themeColor("--primary", "#2563eb"), "#14b8a6", "#f59e0b"],
    }),
    [],
  );
}

const axis = {
  tick: { fill: "currentColor", fontSize: 10 },
  stroke: "currentColor",
  tickLine: false,
  axisLine: false,
} as const;

const tooltipBase = {
  contentStyle: {
    background: "rgb(20 20 22)",
    border: "1px solid rgb(255 255 255 / 0.1)",
    borderRadius: 6,
    fontSize: 12,
  },
  labelStyle: { color: "rgb(255 255 255 / 0.55)" },
  itemStyle: { color: "rgb(255 255 255 / 0.9)" },
};

// Day keys as day.month ("16.11"), the same as every other page.
const shortDate = dayMonth;

function ChartCard({
  testid,
  title,
  total,
  children,
}: {
  testid: string;
  title: string;
  total: string;
  children: ReactElement;
}) {
  return (
    <section data-testid={testid} className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 flex items-baseline gap-2">
        <h2 className="text-sm font-medium">{title}</h2>
        <span className="text-xs text-muted-foreground tabular-nums">{total}</span>
      </div>
      <div className="h-44 text-muted-foreground">{children}</div>
    </section>
  );
}

function Empty() {
  return <div className="flex h-full items-center justify-center text-xs">Nothing in this window.</div>;
}

function DailyBars({
  rows,
  dataKey,
  name,
  color = "primary",
}: {
  rows: StatsDay[];
  dataKey: "sessions" | "pageviews" | "errors";
  name: string;
  color?: "primary" | "failed";
}) {
  const c = useColors();
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={rows} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
        <CartesianGrid stroke="rgb(127 127 127 / 0.15)" vertical={false} />
        <XAxis dataKey="date" tickFormatter={shortDate} minTickGap={16} {...axis} />
        <YAxis allowDecimals={false} width={32} {...axis} />
        <Tooltip {...tooltipBase} cursor={{ fill: "rgb(127 127 127 / 0.12)" }} />
        <Bar dataKey={dataKey} name={name} fill={c[color]} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}

function TopPages({ data }: { data: { path: string; n: number }[] }) {
  const c = useColors();
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={data} layout="vertical" margin={{ top: 0, right: 8, left: 0, bottom: 0 }}>
        <XAxis type="number" allowDecimals={false} hide />
        <YAxis
          type="category"
          dataKey="path"
          width={130}
          interval={0}
          tickFormatter={(p: string) => (p.length > 22 ? `…${p.slice(-21)}` : p)}
          {...axis}
        />
        <Tooltip {...tooltipBase} cursor={{ fill: "rgb(127 127 127 / 0.12)" }} />
        <Bar dataKey="n" name="Page views" fill={c.primary} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}

function DeviceBars({ rows }: { rows: StatsDay[] }) {
  const c = useColors();
  const data = rows.map((r) => ({ date: r.date, ...r.devices }));
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
        <CartesianGrid stroke="rgb(127 127 127 / 0.15)" vertical={false} />
        <XAxis dataKey="date" tickFormatter={shortDate} minTickGap={16} {...axis} />
        <YAxis allowDecimals={false} width={32} {...axis} />
        <Tooltip {...tooltipBase} cursor={{ fill: "rgb(127 127 127 / 0.12)" }} />
        <Legend iconSize={8} wrapperStyle={{ fontSize: 11 }} />
        <Bar dataKey="mobile" name="Phone" stackId="d" fill={c.series[0]} isAnimationActive={false} />
        <Bar dataKey="tablet" name="Tablet" stackId="d" fill={c.series[2]} isAnimationActive={false} />
        <Bar dataKey="desktop" name="Desktop" stackId="d" fill={c.series[1]} isAnimationActive={false} />
      </BarChart>
    </ResponsiveContainer>
  );
}

function VitalsLines({ rows }: { rows: StatsDay[] }) {
  const c = useColors();
  const data = rows.map((r) => ({ date: r.date, lcp: r.vitals.lcpP75, inp: r.vitals.inpP75 }));
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
        <CartesianGrid stroke="rgb(127 127 127 / 0.15)" vertical={false} />
        <XAxis dataKey="date" tickFormatter={shortDate} minTickGap={16} {...axis} />
        <YAxis width={40} {...axis} />
        <Tooltip {...tooltipBase} formatter={(v) => (v == null ? "-" : `${v} ms`)} />
        <Legend iconSize={8} wrapperStyle={{ fontSize: 11 }} />
        <Line dataKey="lcp" name="LCP" stroke={c.series[0]} strokeWidth={1.5} dot={{ r: 2 }} connectNulls isAnimationActive={false} />
        <Line dataKey="inp" name="INP" stroke={c.series[1]} strokeWidth={1.5} dot={{ r: 2 }} connectNulls isAnimationActive={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}

function ErrorsTable({
  rows,
  label,
}: {
  rows: { msg: string; type: string; path: string; n: number; last: string }[];
  label: string;
}) {
  return (
    <section data-testid="stats-error-list" className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 flex items-baseline gap-2">
        <h2 className="text-sm font-medium">Distinct errors</h2>
        <span className="text-xs text-muted-foreground">{label}, most frequent first</span>
      </div>
      {rows.length === 0 ? (
        <div className="text-xs text-muted-foreground">No errors in this window.</div>
      ) : (
        <ul className="divide-y divide-border">
          {rows.map((e) => (
            <li key={`${e.msg}|${e.path}|${e.type}`} data-testid="stats-error-row" className="flex gap-3 py-2">
              <span className="w-8 shrink-0 text-right text-sm font-medium tabular-nums text-status-failed">
                {e.n}
              </span>
              <div className="min-w-0 flex-1">
                <div className="break-words font-mono text-xs">{e.msg || "(no message)"}</div>
                <div className="mt-0.5 text-[11px] text-muted-foreground">
                  {e.path || "(no path)"} · {e.type} · last {e.last}
                </div>
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
