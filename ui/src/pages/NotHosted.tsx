/**
 * NotHosted — the page a route shows when it cannot work in the hosted,
 * read-only UI (ADR-033): its title and one line saying why and what to
 * do instead. It replaces the redirect to the workspace home, so every
 * entry of the local navigation opens in the hosted one too.
 */

import { type ReactNode, useMemo } from "react";
import { Laptop } from "lucide-react";

import { useLocation } from "react-router-dom";

import { stamp } from "@/lib/clock";

import { useChrome } from "@/components/PageChrome";
import { useSnapshotAt, useSummaryOnly } from "@/hooks/useSummaryOnly";

export function NotHosted({ title, line }: { title: string; line: string }) {
  const { pathname } = useLocation();
  useChrome(useMemo(() => ({ breadcrumbs: [{ label: title, to: pathname }] }), [title, pathname]));
  return (
    <div className="mx-auto w-full max-w-3xl px-4 py-8 md:px-6" data-testid="not-hosted">
      <h1 className="font-mono text-2xl font-semibold tracking-tight">{title}</h1>
      <p className="mt-4 flex items-start gap-3 rounded-md border border-border bg-card p-4 text-sm text-muted-foreground">
        <Laptop className="mt-0.5 h-4 w-4 shrink-0" aria-hidden />
        <span data-testid="not-hosted-line">{line}</span>
      </p>
    </div>
  );
}

/** The one-line alternatives of ADR-033, by page. */
export const NOT_HOSTED = {
  credentials: "Credentials are managed on the Mac, in clavesa ui. They are never sent to the hosted UI.",
  query: "Ad-hoc queries run in clavesa ui on the Mac.",
  localData: "The data of this workspace is on the Mac. Open it in clavesa ui there; this page shows its runs and pipelines only.",
} as const;

/**
 * OnTheMac renders its page, or NotHosted with the local-data line when
 * the workspace is a hosted local-warehouse workspace served from its
 * summary (useSummaryOnly): its catalog, tables, dashboards, notebooks
 * and sources stay on the Mac until ADR-036 publishes snapshots.
 */
export function OnTheMac({
  title,
  children,
  snapshot = false,
}: {
  title: string;
  children: ReactNode;
  // snapshot: the page can show a local workspace's published snapshot
  // (ADR-036: Catalog, Dashboards), with when it was taken.
  snapshot?: boolean;
}) {
  const summaryOnly = useSummaryOnly();
  const snapshotAt = useSnapshotAt();
  if (!summaryOnly) return <>{children}</>;
  if (snapshot && snapshotAt) {
    return (
      <>
        <p
          className="mx-auto mt-4 w-full max-w-6xl px-4 text-xs text-muted-foreground md:px-6"
          data-testid="snapshot-as-of"
        >
          As of {formatSnapshotAt(snapshotAt)}, from the Mac's last run. Dashboards show their default selection.
        </p>
        {children}
      </>
    );
  }
  return <NotHosted title={title} line={NOT_HOSTED.localData} />;
}

function formatSnapshotAt(at: string): string {
  return Number.isNaN(Date.parse(at)) ? at : stamp(at);
}
