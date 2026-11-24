/**
 * AppSidebar — collapsible left navigation rail.
 *
 * Expanded: brand + icon-and-label nav links. Collapsed: a ~56px icon rail;
 * labels move into hover tooltips. The collapse choice is global and
 * persisted (see SidebarContext).
 */

import { NavLink, useLocation, useSearchParams } from "react-router-dom";
import {
  Activity,
  BookOpen,
  Boxes,
  Database,
  FileInput,
  KeyRound,
  LayoutDashboard,
  ChartColumn,
  LayoutGrid,
  Package2,
  PanelLeftClose,
  PanelLeftOpen,
  Share2,
  Terminal,
  Workflow,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { hasStats } from "@/components/RootNav";
import { useDashboards, usePipelines } from "@/lib/queries";
import { runtimeConfig } from "@/lib/runtimeConfig";
import { cn } from "@/lib/utils";
import { seesAll } from "@/lib/viewerGroups";
import { SECTIONS } from "@/lib/workspaceNav";
import { SwitcherButton } from "./WorkspaceSwitcher";
import { useSidebar } from "./SidebarContext";
import { SignOutButton } from "./SignOutButton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

interface NavItem {
  to: string;
  label: string;
  icon: LucideIcon;
  /** Treats these path prefixes as "inside" this nav item (active state). */
  match: (pathname: string) => boolean;
}

// Order: the three things you spend time *in* — the data front door,
// where you build, where you watch — then the two registries that feed
// them. Grouping beats interleaving config between the work surfaces.
//
// Inside a workspace of a multi-workspace or hosted server (ADR-032) the
// workspace home leads and the Catalog moves to /catalog; the local UI's
// list is unchanged.
const HOME_ITEMS: NavItem[] =
  runtimeConfig.mode === "local"
    ? [{ to: "/", label: "Catalog", icon: Database, match: (p) => p === "/" || p.startsWith("/tables") }]
    : [
        { to: "/", label: "Overview", icon: Activity, match: (p) => p === "/" || p === "/overview" },
        {
          to: "/catalog",
          label: "Catalog",
          icon: Database,
          match: (p) => p.startsWith("/catalog") || p.startsWith("/tables"),
        },
      ];

const NAV: NavItem[] = [
  ...HOME_ITEMS,
  {
    to: "/pipelines",
    label: "Pipelines",
    icon: Workflow,
    // The dashboard, run-detail, and backfill pages are all
    // conceptually "Pipelines" even though their paths diverge.
    match: (p) =>
      p.startsWith("/pipelines") || p.startsWith("/backfills"),
  },
  { to: "/bundles", label: "Bundles", icon: Package2, match: (p) => p.startsWith("/bundles") },
  { to: "/dashboards", label: "Dashboards", icon: LayoutDashboard, match: (p) => p.startsWith("/dashboards") },
  { to: "/notebooks", label: "Notebooks", icon: BookOpen, match: (p) => p.startsWith("/notebooks") },
  { to: "/query", label: "Query", icon: Terminal, match: (p) => p.startsWith("/query") },
  { to: "/sources", label: "Sources", icon: FileInput, match: (p) => p.startsWith("/sources") },
  { to: "/credentials", label: "Credentials", icon: KeyRound, match: (p) => p.startsWith("/credentials") },
  { to: "/unity-catalog", label: "Unity Catalog", icon: Share2, match: (p) => p.startsWith("/unity-catalog") },
  { to: "/runner", label: "Runner", icon: Boxes, match: (p) => p.startsWith("/runner") },
];

// The local UI shows every entry. The hosted UI shows only the workspace's
// four sections (docs/ideas/hosted-navigation.md, Vesa 2026-11-21): the
// other pages are on the Mac or not hosted; their routes still answer a
// bookmark. The multi-workspace UI (`clavesa ui --workspaces`) keeps the
// rest under the four sections.
const FOUR = new Set(SECTIONS.map((s) => s.to));
const navItems: NavItem[] =
  runtimeConfig.mode === "local"
    ? NAV
    : [
        ...SECTIONS.map((s) => ({ to: s.to, label: s.label, icon: s.icon, match: s.match })),
        ...(runtimeConfig.mode === "hosted" ? [] : NAV.filter((n) => !FOUR.has(n.to) && n.to !== "/overview")),
      ];

/** The open section's own items, under it: its pipelines or dashboards. */
function SectionItems({ to }: { to: string }) {
  const { pathname } = useLocation();
  const [params] = useSearchParams();
  const pipelines = usePipelines();
  const dashboards = useDashboards();
  let items: { key: string; label: string; to: string; on: boolean }[] = [];
  if (to === "/pipelines") {
    const dir = params.get("dir") ?? "";
    items = (pipelines.data ?? []).map((p) => ({
      key: p.dir,
      label: p.name,
      to: `/pipelines/dashboard?dir=${encodeURIComponent(p.dir)}`,
      on: pathname === "/pipelines/dashboard" && dir === p.dir,
    }));
  } else if (to === "/dashboards") {
    items = (dashboards.data?.dashboards ?? []).map((d) => ({
      key: d.slug,
      label: d.title,
      to: `/dashboards/${encodeURIComponent(d.slug)}`,
      on: pathname === `/dashboards/${d.slug}`,
    }));
  }
  if (items.length === 0) return null;
  return (
    <div data-testid="nav-section-items" className="mb-1 ml-5 flex flex-col border-l border-border">
      {items.map((it) => (
        <NavLink
          key={it.key}
          to={it.to}
          aria-current={it.on ? "page" : undefined}
          className={cn(
            "truncate py-1.5 pl-3 pr-2 font-mono text-xs transition-colors",
            it.on ? "text-foreground shadow-[inset_2px_0_0_var(--primary)]" : "text-muted-foreground hover:text-foreground",
          )}
        >
          {it.label}
        </NavLink>
      ))}
    </div>
  );
}

export function AppSidebar() {
  const { collapsed, toggle } = useSidebar();
  const { pathname } = useLocation();

  return (
    <aside
      className={cn(
        // Phone (below md): hidden; PhoneNav is the bottom navigation.
        "hidden h-full shrink-0 flex-col border-r border-border bg-card transition-[width] duration-150 md:flex",
        collapsed ? "w-14" : "w-56",
      )}
    >
      {/* Brand; the workspace switcher in the hosted and multi UI. */}
      {runtimeConfig.mode !== "local" ? (
        <div className={cn("flex h-14 shrink-0 items-center border-b border-border", collapsed ? "px-2" : "px-2.5")}>
          <SwitcherButton collapsed={collapsed} />
        </div>
      ) : (
      <div
        className={cn(
          "flex h-14 shrink-0 items-center border-b border-border",
          collapsed ? "justify-center px-0" : "px-4",
        )}
      >
        <span className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-primary/15 text-primary">
          <Database className="h-4 w-4" />
        </span>
        {!collapsed && (
          <span className="ml-2 truncate text-base font-semibold tracking-tight">
            Clavesλ
          </span>
        )}
      </div>
      )}

      {/* Nav */}
      <nav className="flex flex-1 flex-col gap-1 overflow-y-auto p-2">
        {navItems.map((item) => {
          const active = item.match(pathname);
          const Icon = item.icon;
          const link = (
            <NavLink
              to={item.to}
              aria-label={item.label}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                collapsed && "justify-center px-0",
                active
                  ? "bg-secondary text-foreground"
                  : "text-muted-foreground hover:bg-secondary/50 hover:text-foreground",
              )}
            >
              <Icon className="h-4 w-4 shrink-0" />
              {!collapsed && <span className="truncate">{item.label}</span>}
            </NavLink>
          );
          return collapsed ? (
            <Tooltip key={item.to}>
              <TooltipTrigger asChild>{link}</TooltipTrigger>
              <TooltipContent side="right">{item.label}</TooltipContent>
            </Tooltip>
          ) : (
            <span key={item.to}>
              {link}
              {active && runtimeConfig.mode !== "local" && <SectionItems to={item.to} />}
            </span>
          );
        })}
      </nav>

      {/* Collapse toggle (and Sign out in hosted mode) */}
      <div className="shrink-0 border-t border-border p-2">
        {runtimeConfig.mode !== "local" && (runtimeConfig.mode !== "hosted" || seesAll()) && (
          <a
            href="/"
            aria-label="All workspaces"
            title={collapsed ? "All workspaces" : undefined}
            data-testid="nav-all-workspaces"
            className={cn(
              "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-secondary/50 hover:text-foreground",
              collapsed && "justify-center px-0",
            )}
          >
            <LayoutGrid className="h-4 w-4 shrink-0" />
            {!collapsed && <span>All workspaces</span>}
          </a>
        )}
        {hasStats() && (
          <a
            href="/stats"
            aria-label="Site usage"
            title={collapsed ? "Site usage" : undefined}
            data-testid="nav-site-usage"
            className={cn(
              "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-secondary/50 hover:text-foreground",
              collapsed && "justify-center px-0",
            )}
          >
            <ChartColumn className="h-4 w-4 shrink-0" />
            {!collapsed && <span>Site usage</span>}
          </a>
        )}
        <SignOutButton compact={collapsed} className="w-full font-medium" />
        <button
          type="button"
          onClick={toggle}
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          className={cn(
            "flex w-full items-center gap-3 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-secondary/50 hover:text-foreground",
            collapsed && "justify-center px-0",
          )}
        >
          {collapsed ? (
            <PanelLeftOpen className="h-4 w-4 shrink-0" />
          ) : (
            <>
              <PanelLeftClose className="h-4 w-4 shrink-0" />
              <span>Collapse</span>
            </>
          )}
        </button>
      </div>
    </aside>
  );
}
