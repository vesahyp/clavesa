/**
 * AppSidebar — collapsible left navigation rail.
 *
 * Expanded: brand + icon-and-label nav links. Collapsed: a ~56px icon rail;
 * labels move into hover tooltips. The collapse choice is global and
 * persisted (see SidebarContext).
 */

import { NavLink, useLocation } from "react-router-dom";
import {
  Activity,
  BookOpen,
  Boxes,
  Database,
  FileInput,
  KeyRound,
  LayoutDashboard,
  LayoutGrid,
  Package2,
  PanelLeftClose,
  PanelLeftOpen,
  Share2,
  Terminal,
  Workflow,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { runtimeConfig } from "@/lib/runtimeConfig";
import { cn } from "@/lib/utils";
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

// Read-only (the hosted UI, `clavesa ui --read-only`): no entry for pages
// whose point is to run or change something. The server refuses every
// method but GET and HEAD anyway (ReadOnlyGuard); the pages are not shown.
const READ_ONLY_HIDDEN = new Set(["/query", "/credentials", "/runner"]);
const navItems = runtimeConfig.readOnly ? NAV.filter((i) => !READ_ONLY_HIDDEN.has(i.to)) : NAV;

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
      {/* Brand */}
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

      {/* Nav */}
      <nav className="flex flex-1 flex-col gap-1 overflow-y-auto p-2">
        {/* Multi-workspace / hosted (ADR-032): back to the Workspaces
            list. A plain <a>: "/" is outside this workspace's basename,
            so it is a full page load with the root's runtime config. */}
        {runtimeConfig.mode !== "local" && (
          <a
            href="/"
            aria-label="All workspaces"
            title={collapsed ? "All workspaces" : undefined}
            data-testid="nav-all-workspaces"
            className={cn(
              "mb-1 flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-secondary/50 hover:text-foreground",
              collapsed && "justify-center px-0",
            )}
          >
            <LayoutGrid className="h-4 w-4 shrink-0" />
            {!collapsed && (
              <span className="truncate">
                All workspaces
                <span className="block truncate font-mono text-[11px] font-normal">
                  {runtimeConfig.workspace}
                </span>
              </span>
            )}
          </a>
        )}
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
            <span key={item.to}>{link}</span>
          );
        })}
      </nav>

      {/* Collapse toggle (and Sign out in hosted mode) */}
      <div className="shrink-0 border-t border-border p-2">
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
