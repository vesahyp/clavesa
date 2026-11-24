/**
 * WorkspaceSwitcher — where am I, at workspace level, in the hosted and
 * multi-workspace UI (docs/ideas/hosted-navigation.md). The current
 * workspace's name and health always show: in the sidebar's top button on
 * a desktop, in the chip at the top right on a phone. Either opens one
 * panel (a bottom sheet on a phone, a popover on a desktop):
 *   - the workspaces this user may open, the current one checked;
 *   - for the group "all" (hosted): All workspaces and Site usage;
 *   - who is signed in, and Sign out (hosted).
 * Every workspace link is a full page load ("/w/<name>/" is outside this
 * page's basename).
 */

import { useEffect, useMemo, useState } from "react";
import { Check, ChartColumn, ChevronDown, LayoutGrid } from "lucide-react";

import { SignOutButton } from "@/components/SignOutButton";
import { hasStats } from "@/components/RootNav";
import { WorkspaceSummary, useWorkspaces, type WorkspaceEntry } from "@/lib/queries";
import { runtimeConfig } from "@/lib/runtimeConfig";
import { cardStatus } from "@/lib/summary";
import { cn } from "@/lib/utils";
import { seesAll, viewerEmail } from "@/lib/viewerGroups";

type Health = "ok" | "warn" | "fail" | "unknown";

const DOT: Record<Health, string> = {
  ok: "bg-status-success",
  warn: "bg-status-running",
  fail: "bg-status-failed",
  unknown: "bg-status-pending",
};

const LABEL: Record<Health, string> = {
  ok: "Healthy",
  warn: "Needs attention",
  fail: "Failing",
  unknown: "Status not known",
};

function healthOf(ws: WorkspaceEntry | undefined): Health {
  if (!ws || ws.summary == null) return "unknown";
  const parsed = WorkspaceSummary.safeParse(ws.summary);
  if (!parsed.success) return "unknown";
  return cardStatus(parsed.data, Date.now(), ws).status as Health;
}

function useSwitcherList() {
  // The root API lists the workspaces this user may open.
  const q = useWorkspaces({ apiBase: "/api" });
  const list = useMemo(() => q.data ?? [], [q.data]);
  const current = list.find((w) => w.slug === runtimeConfig.workspace || w.name === runtimeConfig.workspace);
  return { list, current, health: healthOf(current) };
}

function Dot({ health, className }: { health: Health; className?: string }) {
  return <span aria-hidden className={cn("inline-block h-2 w-2 shrink-0 rounded-full", DOT[health], className)} />;
}

/** The panel. `open` and `onClose` belong to the trigger. */
function SwitcherPanel({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { list } = useSwitcherList();
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);
  if (!open) return null;
  const hosted = runtimeConfig.mode === "hosted";
  const all = !hosted || seesAll();
  const email = hosted ? viewerEmail() : "";
  const head = "mb-1 mt-3 px-2 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground";
  const row = "flex min-h-12 items-center gap-3 rounded-md px-2 text-sm hover:bg-secondary/60";
  return (
    <>
      <div className="fixed inset-0 z-50 bg-black/50 md:bg-transparent" onClick={onClose} aria-hidden />
      <div
        role="dialog"
        aria-label="Workspaces"
        data-testid="ws-switcher-panel"
        className={cn(
          "fixed z-[60] overflow-y-auto border-border bg-card p-3 shadow-xl",
          // Phone: a bottom sheet. Desktop: a popover under the sidebar button.
          "inset-x-0 bottom-0 max-h-[80dvh] rounded-t-2xl border-t pb-[calc(1rem+env(safe-area-inset-bottom))]",
          "md:inset-x-auto md:bottom-auto md:left-3 md:top-16 md:max-h-[70vh] md:w-80 md:rounded-xl md:border",
        )}
      >
        <div aria-hidden className="mx-auto mb-2 h-1 w-10 rounded-full bg-border md:hidden" />
        <div className={head}>Workspace</div>
        {list.map((w) => {
          const cur = w.slug === runtimeConfig.workspace || w.name === runtimeConfig.workspace;
          const h = healthOf(w);
          return (
            <a
              key={w.slug}
              href={`/w/${encodeURIComponent(w.slug)}/`}
              data-testid="ws-switcher-item"
              aria-current={cur ? "page" : undefined}
              className={cn(row, cur && "font-semibold")}
            >
              <Dot health={h} />
              <span className="min-w-0 flex-1">
                <span className="block truncate font-mono">{w.name}</span>
                {h !== "unknown" && <span className="block text-xs font-normal text-muted-foreground">{LABEL[h]}</span>}
              </span>
              {cur && <Check className="h-4 w-4 shrink-0" />}
            </a>
          );
        })}
        {all && (
          <>
            <div className={head}>Everything</div>
            <a href="/" data-testid="ws-switcher-all" className={row}>
              <LayoutGrid className="h-4 w-4 shrink-0 text-muted-foreground" />
              All workspaces
            </a>
            {hasStats() && (
              <a href="/stats" data-testid="ws-switcher-stats" className={row}>
                <ChartColumn className="h-4 w-4 shrink-0 text-muted-foreground" />
                Site usage
              </a>
            )}
          </>
        )}
        {hosted && (
          <>
            <div className={head}>You</div>
            <div className="px-2 py-1 text-sm">
              <span className="block truncate">{email || "Signed in"}</span>
              <span className="block text-xs text-muted-foreground">
                {runtimeConfig.readOnly ? "Read-only viewer" : "Viewer"}
              </span>
            </div>
            <SignOutButton className="mt-1 min-h-12 w-full px-2" />
          </>
        )}
      </div>
    </>
  );
}

/** The sidebar's top button (desktop). */
export function SwitcherButton({ collapsed }: { collapsed: boolean }) {
  const [open, setOpen] = useState(false);
  const { health } = useSwitcherList();
  return (
    <>
      <button
        type="button"
        data-testid="ws-switcher"
        onClick={() => setOpen(true)}
        aria-label={`Workspace ${runtimeConfig.workspace}, switch`}
        title={collapsed ? runtimeConfig.workspace : undefined}
        className={cn(
          "flex w-full items-center gap-2 rounded-lg border border-border text-left transition-colors hover:bg-secondary/50",
          collapsed ? "h-10 justify-center" : "px-2.5 py-1.5",
        )}
      >
        <Dot health={health} />
        {!collapsed && (
          <>
            <span className="min-w-0 flex-1">
              <span className="block text-[11px] text-muted-foreground">Workspace</span>
              <span className="block truncate font-mono text-sm font-semibold">{runtimeConfig.workspace}</span>
            </span>
            <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
          </>
        )}
      </button>
      <SwitcherPanel open={open} onClose={() => setOpen(false)} />
    </>
  );
}

/** The chip at the top right of a phone page. */
export function SwitcherChip() {
  const [open, setOpen] = useState(false);
  const { health } = useSwitcherList();
  return (
    <>
      <button
        type="button"
        data-testid="ws-chip"
        onClick={() => setOpen(true)}
        aria-label={`Workspace ${runtimeConfig.workspace}, switch`}
        className="flex min-h-11 max-w-[50%] shrink-0 items-center gap-1.5 rounded-full border border-border px-3 md:hidden"
      >
        <Dot health={health} />
        <span className="min-w-0 truncate font-mono text-sm font-semibold">{runtimeConfig.workspace}</span>
        <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
      </button>
      <SwitcherPanel open={open} onClose={() => setOpen(false)} />
    </>
  );
}
