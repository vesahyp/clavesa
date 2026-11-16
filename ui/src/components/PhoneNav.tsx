/**
 * PhoneNav — the bottom navigation below the md breakpoint (ADR-032
 * section 3), where the sidebar is hidden. Hosted, it is the same bar as
 * on the root pages (SiteNav in RootNav.tsx). Local: all workspaces, this
 * workspace's home and its pipelines. The local UI has no Workspaces page, so its first item
 * is the Catalog. Padded for the home indicator (safe-area-inset-bottom).
 */

import { NavLink, useLocation } from "react-router-dom";
import { Activity, Database, LayoutGrid, Workflow } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { SiteNav } from "@/components/RootNav";
import { homePath, runtimeConfig } from "@/lib/runtimeConfig";
import { cn } from "@/lib/utils";

const item =
  "flex min-w-0 flex-1 flex-col items-center justify-center gap-0.5 py-2 text-[11px] font-medium";

function Inner({ icon: Icon, label }: { icon: LucideIcon; label: string }) {
  return (
    <>
      <Icon className="h-5 w-5" />
      <span className="max-w-full truncate">{label}</span>
    </>
  );
}

export function PhoneNav() {
  const { pathname } = useLocation();
  // Hosted: the one bar of every page (RootNav.tsx); inside a workspace
  // its active item is Workspaces. Deeper pages get PhoneBack at the top.
  if (runtimeConfig.mode === "hosted") return <SiteNav active="workspaces" inWorkspace />;
  const multi = runtimeConfig.mode !== "local";
  const homeActive = pathname === homePath || pathname === "/overview";
  const pipelinesActive =
    pathname.startsWith("/pipelines") || pathname.startsWith("/backfills");
  const tone = (active: boolean) =>
    active ? "text-foreground" : "text-muted-foreground";
  return (
    <nav
      data-testid="phone-nav"
      aria-label="Main"
      className="fixed inset-x-0 bottom-0 z-40 flex border-t border-border bg-card pb-[env(safe-area-inset-bottom)] md:hidden"
    >
      {multi ? (
        // "/" outside this workspace's basename: a full page load.
        <a href="/" data-testid="phone-nav-workspaces" className={cn(item, tone(false))}>
          <Inner icon={LayoutGrid} label="Workspaces" />
        </a>
      ) : (
        <NavLink
          to="/"
          data-testid="phone-nav-catalog"
          className={cn(item, tone(pathname === "/" || pathname.startsWith("/tables")))}
        >
          <Inner icon={Database} label="Catalog" />
        </NavLink>
      )}
      <NavLink
        to={homePath}
        data-testid="phone-nav-home"
        className={cn(item, tone(homeActive))}
      >
        <Inner icon={Activity} label={multi ? runtimeConfig.workspace || "Overview" : "Overview"} />
      </NavLink>
      <NavLink
        to="/pipelines"
        data-testid="phone-nav-pipelines"
        className={cn(item, tone(pipelinesActive))}
      >
        <Inner icon={Workflow} label="Pipelines" />
      </NavLink>
    </nav>
  );
}
