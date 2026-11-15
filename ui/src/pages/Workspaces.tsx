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

import { SignOutButton } from "@/components/SignOutButton";
import { WorkspaceCard } from "@/components/WorkspaceCard";
import { WorkspaceSummary, requestParsed, useWorkspaces } from "@/lib/queries";
import { runtimeConfig } from "@/lib/runtimeConfig";
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
      return STATUS_RANK[cardStatus(data).status];
    };
    return list
      .map((ws, i) => ({ ws, i, r: rank(i) }))
      .sort((a, b) => a.r - b.r || a.ws.name.localeCompare(b.ws.name));
  }, [list, stored, summaries]);

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="flex h-14 items-center gap-3 border-b border-border px-4 pt-[env(safe-area-inset-top)] md:px-6">
        <span className="text-base font-semibold tracking-tight">Clavesλ</span>
        <span className="text-sm text-muted-foreground">Workspaces</span>
        <div className="ml-auto flex items-center gap-2">
          {runtimeConfig.readOnly && (
            <span
              data-testid="read-only-badge"
              className="rounded-md border border-border px-2 py-0.5 text-[11px] text-muted-foreground"
            >
              Read-only
            </span>
          )}
          <SignOutButton compact />
        </div>
      </header>
      <main
        data-testid="workspaces-page"
        className="mx-auto grid max-w-5xl grid-cols-1 gap-4 px-4 py-4 pb-[calc(1rem+env(safe-area-inset-bottom))] md:grid-cols-2 md:px-6 md:py-6"
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
    </div>
  );
}
