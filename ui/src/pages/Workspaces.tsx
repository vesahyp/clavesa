/**
 * Workspaces — the root page of a multi-workspace or hosted server
 * (ADR-032 section 1, page 1). One card per workspace, problems first:
 * failing, then needs attention, then healthy, then by name. A workspace
 * whose summary fails sorts with the failing ones and shows an error
 * card; one still loading sorts last and shows a skeleton.
 *
 * GET /api/workspaces draws the list at once. On the hosted server it
 * carries each workspace's stored summary, so the page makes no other
 * request. On the local multi-workspace server (no summary in the list)
 * each card's summary is its own query against that workspace's
 * `/w/<name>/api/workspace/summary`, so a slow or broken workspace never
 * holds up the others; the hosted server does the same for a workspace
 * with no stored summary yet.
 *
 * A workspace link is a plain <a>: each workspace is its own page load
 * with its own basename and API base (see lib/runtimeConfig.ts).
 */

import { useMemo } from "react";
import { useQueries } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";

import { RootHeader, RootNav, hasStats } from "@/components/RootNav";
import { WorkspaceCard } from "@/components/WorkspaceCard";
import { WorkspaceSummary, requestParsed, useWorkspaces } from "@/lib/queries";
import { STATUS_RANK, cardStatus } from "@/lib/summary";

export function Workspaces() {
  const { data, error, isLoading } = useWorkspaces();
  const list = data ?? [];
  // The hosted server's stored summaries; null where none is stored or
  // one does not parse (the card then loads it live).
  const stored = useMemo(
    () =>
      list.map((ws) => {
        const p = WorkspaceSummary.safeParse(ws.summary);
        return p.success ? p.data : null;
      }),
    [list],
  );
  const summaries = useQueries({
    queries: list.map((ws, i) => ({
      // Same key as useWorkspaceSummary(apiBase), so the cache is shared.
      queryKey: ["workspace-summary", ws.api_base],
      queryFn: () =>
        requestParsed("/workspace/summary", WorkspaceSummary, {
          errorLabel: "GET /workspace/summary",
          apiBase: ws.api_base,
        }),
      refetchInterval: 60_000,
      retry: false,
      enabled: !stored[i],
    })),
  });
  const summaryOf = (i: number) => stored[i] ?? summaries[i].data;
  const loadingOf = (i: number) => !stored[i] && summaries[i].isLoading;
  const errorOf = (i: number) => (stored[i] ? null : summaries[i].error);

  const ordered = useMemo(() => {
    const rank = (i: number): number => {
      const data = stored[i] ?? summaries[i].data;
      if (!stored[i] && summaries[i].isLoading) return 3;
      if ((!stored[i] && summaries[i].error) || !data) return STATUS_RANK.fail;
      return STATUS_RANK[cardStatus(data, Date.now(), list[i]).status];
    };
    return list
      .map((ws, i) => ({ ws, i, r: rank(i) }))
      .sort((a, b) => a.r - b.r || a.ws.name.localeCompare(b.ws.name));
  }, [list, stored, summaries]);

  // "1 needs attention · 9 healthy": the answer before the first card.
  const counts = useMemo(() => {
    const c = { fail: 0, warn: 0, ok: 0, loading: 0 };
    list.forEach((ws, i) => {
      const d = summaryOf(i);
      if (loadingOf(i)) c.loading++;
      else if (errorOf(i) || !d) c.fail++;
      else c[cardStatus(d, Date.now(), ws).status]++;
    });
    return c;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [list, stored, summaries]);
  const summaryLine = [
    counts.fail && `${counts.fail} failing`,
    counts.warn && `${counts.warn} need${counts.warn === 1 ? "s" : ""} attention`,
    counts.ok && `${counts.ok} healthy`,
    counts.loading && `${counts.loading} loading`,
  ].filter(Boolean);

  return (
    <div className="min-h-screen bg-background text-foreground">
      <RootHeader title="Workspaces" />
      <main
        data-testid="workspaces-page"
        className={
          "mx-auto grid max-w-5xl grid-cols-1 gap-2 px-4 py-3 md:grid-cols-2 md:gap-3 md:px-6 md:py-6 " +
          // Room for RootNav below md (hosted), else for the home indicator.
          (hasStats
            ? "pb-[calc(4.75rem+env(safe-area-inset-bottom))] md:pb-6"
            : "pb-[calc(1rem+env(safe-area-inset-bottom))]")
        }
      >
        {isLoading && (
          <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
        )}
        {error && (
          <div data-testid="workspaces-error" className="text-sm text-status-failed">
            {error instanceof Error ? error.message : String(error)}
          </div>
        )}
        {data?.length === 0 && (
          <div data-testid="workspaces-empty" className="text-sm text-muted-foreground">
            No workspaces yet.
          </div>
        )}
        {list.length > 0 && (
          <p
            data-testid="workspaces-summary"
            data-fail={counts.fail}
            data-warn={counts.warn}
            data-ok={counts.ok}
            className="text-sm md:col-span-2"
          >
            {summaryLine.map((t, k) => (
              <span key={String(t)}>
                {k > 0 && <span className="text-muted-foreground"> · </span>}
                <span
                  className={
                    String(t).includes("failing")
                      ? "font-medium text-status-failed"
                      : String(t).includes("attention")
                        ? "font-medium text-status-running"
                        : "text-muted-foreground"
                  }
                >
                  {t}
                </span>
              </span>
            ))}
          </p>
        )}
        {ordered.map(({ ws, i }) => (
          <WorkspaceCard
            key={ws.slug}
            entry={ws}
            summary={summaryOf(i)}
            error={errorOf(i)}
            isLoading={loadingOf(i)}
          />
        ))}
      </main>
      <RootNav />
    </div>
  );
}
