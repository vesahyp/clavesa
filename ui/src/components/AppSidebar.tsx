/**
 * AppSidebar — collapsible left navigation rail.
 *
 * Expanded: brand + icon-and-label nav links. Collapsed: a ~56px icon rail;
 * labels move into hover tooltips. The collapse choice is global and
 * persisted (see SidebarContext).
 */

import { NavLink, useLocation } from "react-router-dom";
import {
  Database,
  FileInput,
  KeyRound,
  LayoutDashboard,
  PanelLeftClose,
  PanelLeftOpen,
  Workflow,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { cn } from "@/lib/utils";
import { useSidebar } from "./SidebarContext";
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
const NAV: NavItem[] = [
  { to: "/", label: "Catalog", icon: Database, match: (p) => p === "/" || p.startsWith("/tables") },
  {
    to: "/pipelines",
    label: "Pipelines",
    icon: Workflow,
    // The dashboard, run-detail, and backfill pages are all
    // conceptually "Pipelines" even though their paths diverge.
    match: (p) =>
      p.startsWith("/pipelines") || p.startsWith("/backfills"),
  },
  { to: "/dashboards", label: "Dashboards", icon: LayoutDashboard, match: (p) => p.startsWith("/dashboards") },
  { to: "/sources", label: "Sources", icon: FileInput, match: (p) => p.startsWith("/sources") },
  { to: "/credentials", label: "Credentials", icon: KeyRound, match: (p) => p.startsWith("/credentials") },
];

export function AppSidebar() {
  const { collapsed, toggle } = useSidebar();
  const { pathname } = useLocation();

  return (
    <aside
      className={cn(
        "flex h-full shrink-0 flex-col border-r border-border bg-card transition-[width] duration-150",
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
        {NAV.map((item) => {
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

      {/* Collapse toggle */}
      <div className="shrink-0 border-t border-border p-2">
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
