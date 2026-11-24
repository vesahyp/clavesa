/**
 * PhoneNav — the bottom bar below the md breakpoint, where the sidebar is
 * hidden. Hosted and multi-workspace: the workspace's four sections
 * (lib/workspaceNav.ts), the same on every page of the workspace; only
 * the active marker moves (Vesa, 2026-11-16: "The ui bottom buttons
 * change, which is not good ux"). All workspaces and Site usage are in
 * the workspace switcher (docs/ideas/hosted-navigation.md). Tapping the
 * active section opens its first page. Local: Catalog, Overview,
 * Pipelines. Padded for the home indicator (safe-area-inset-bottom).
 */

import { Link, NavLink, useLocation } from "react-router-dom";
import { Activity, Database, Workflow } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { runtimeConfig } from "@/lib/runtimeConfig";
import { cn } from "@/lib/utils";
import { SECTIONS, activeSection } from "@/lib/workspaceNav";

const item =
  "relative flex min-w-0 flex-1 flex-col items-center justify-center gap-0.5 py-2 text-[11px]";

function Inner({ icon: Icon, label, on }: { icon: LucideIcon; label: string; on: boolean }) {
  return (
    <>
      {/* The active marker: a bar on the top edge of the item. */}
      <span
        aria-hidden
        className={cn("absolute inset-x-6 top-0 h-0.5 rounded-b-full", on ? "bg-foreground" : "bg-transparent")}
      />
      <Icon className="h-5 w-5" strokeWidth={on ? 2.5 : 2} />
      <span className="max-w-full truncate">{label}</span>
    </>
  );
}

export function PhoneNav() {
  const { pathname } = useLocation();
  const tone = (on: boolean) => (on ? "font-semibold text-foreground" : "font-medium text-muted-foreground");
  const nav = "fixed inset-x-0 bottom-0 z-40 flex border-t border-border bg-card pb-[env(safe-area-inset-bottom)] md:hidden";
  if (runtimeConfig.mode !== "local") {
    const active = activeSection(pathname);
    return (
      <nav data-testid="phone-nav" aria-label="Sections" className={nav}>
        {SECTIONS.map((s) => {
          const on = s.key === active;
          return (
            // Link, not NavLink: NavLink sets aria-current itself, only on
            // the section's own page, and would clear it on the pages below.
            <Link
              key={s.key}
              to={s.to}
              data-testid={`phone-nav-${s.key}`}
              data-active={on ? "true" : "false"}
              aria-current={on ? "page" : undefined}
              className={cn(item, tone(on))}
            >
              <Inner icon={s.icon} label={s.label} on={on} />
            </Link>
          );
        })}
      </nav>
    );
  }
  const catalogOn = pathname === "/" || pathname.startsWith("/tables");
  const homeOn = pathname === "/overview";
  const pipelinesOn = pathname.startsWith("/pipelines") || pathname.startsWith("/backfills");
  return (
    <nav data-testid="phone-nav" aria-label="Main" className={nav}>
      <NavLink to="/" data-testid="phone-nav-catalog" className={cn(item, tone(catalogOn))}>
        <Inner icon={Database} label="Catalog" on={catalogOn} />
      </NavLink>
      <NavLink to="/overview" data-testid="phone-nav-home" className={cn(item, tone(homeOn))}>
        <Inner icon={Activity} label="Overview" on={homeOn} />
      </NavLink>
      <NavLink to="/pipelines" data-testid="phone-nav-pipelines" className={cn(item, tone(pipelinesOn))}>
        <Inner icon={Workflow} label="Pipelines" on={pipelinesOn} />
      </NavLink>
    </nav>
  );
}
