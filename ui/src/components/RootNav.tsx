/**
 * RootHeader and SiteNav — the frame of the hosted UI and of the pages
 * outside any workspace of a multi-workspace server: Workspaces at "/"
 * and, hosted only, Stats at "/stats" (ADR-032 section 4). The header
 * carries the page links from md up; below md the bottom bar does.
 *
 * Hosted, the bottom bar is the same on every page (Vesa, 2026-11-16:
 * "The ui bottom buttons change, which is not good ux"): Workspaces and
 * Stats, in that order, also inside a workspace, where Workspaces is the
 * active item. Only the active marker moves. A page below the top level
 * gets a back button at the top (PhoneBack), never more bottom buttons.
 * The local multi-workspace UI has no Stats page (its server has no
 * /api/stats), so its root gets no bar: one page needs none.
 */

import { Link, NavLink, useLocation } from "react-router-dom";
import { ChartColumn, LayoutGrid } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { SignOutButton } from "@/components/SignOutButton";
import { runtimeConfig } from "@/lib/runtimeConfig";
import { seesAll } from "@/lib/viewerGroups";
import { cn } from "@/lib/utils";

/** True where the root pages have the bottom bar: the hosted UI. */
export const hasSiteNav = runtimeConfig.mode === "hosted";

/**
 * True where the bar offers Site usage: the hosted UI, for a user in the
 * group "all" (ADR-035; the server answers 404 to anyone else, since the
 * page names every workspace).
 */
export function hasStats(): boolean {
  return hasSiteNav && seesAll();
}

/** The bar's pages for this user. */
function sitePages() {
  return hasStats() ? PAGES : PAGES.filter((p) => p.key !== "stats");
}

type SitePage = "workspaces" | "stats";

const PAGES: { key: SitePage; to: string; label: string; icon: LucideIcon }[] = [
  { key: "workspaces", to: "/", label: "Workspaces", icon: LayoutGrid },
  // "Site usage", not "Stats": it is how this UI is used, not pipeline
  // stats (review of 2026-11-16).
  { key: "stats", to: "/stats", label: "Site usage", icon: ChartColumn },
];

export function RootHeader({ title }: { title: string }) {
  return (
    <header className="flex h-14 items-center gap-3 border-b border-border px-4 pt-[env(safe-area-inset-top)] md:px-6">
      <span className="text-base font-semibold tracking-tight">Clavesλ</span>
      <span className="text-sm text-muted-foreground md:hidden">{title}</span>
      {hasSiteNav ? (
        <nav aria-label="Pages" className="hidden items-center gap-1 md:flex">
          {sitePages().map((p) => (
            <NavLink
              key={p.to}
              to={p.to}
              end
              className={({ isActive }) =>
                cn(
                  "rounded-md px-2 py-1 text-sm",
                  isActive ? "bg-muted text-foreground" : "text-muted-foreground hover:text-foreground",
                )
              }
            >
              {p.label}
            </NavLink>
          ))}
        </nav>
      ) : (
        <span className="hidden text-sm text-muted-foreground md:inline">{title}</span>
      )}
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
  );
}

/**
 * The hosted bottom bar. `inWorkspace` is true under /w/<name>/, where the
 * router's basename is the workspace: "/" and "/stats" are then outside
 * it and load as full pages through a plain <a>. The rendered hrefs are
 * the same either way, which `make hosted-live-check` asserts.
 */
export function SiteNav({ active, inWorkspace }: { active: SitePage; inWorkspace: boolean }) {
  return (
    <nav
      data-testid="phone-nav"
      aria-label="Main"
      className="fixed inset-x-0 bottom-0 z-40 flex border-t border-border bg-card pb-[env(safe-area-inset-bottom)] md:hidden"
    >
      {sitePages().map(({ key, to, label, icon: Icon }) => {
        const on = key === active;
        const cls = cn(
          "relative flex min-w-0 flex-1 flex-col items-center justify-center gap-0.5 py-2 text-[11px]",
          on ? "font-semibold text-foreground" : "font-medium text-muted-foreground",
        );
        const body = (
          <>
            {/* The active marker: a bar on the top edge of the item. */}
            <span
              aria-hidden
              className={cn(
                "absolute inset-x-6 top-0 h-0.5 rounded-b-full",
                on ? "bg-foreground" : "bg-transparent",
              )}
            />
            <Icon className="h-5 w-5" strokeWidth={on ? 2.5 : 2} />
            <span className="max-w-full truncate">{label}</span>
          </>
        );
        const common = {
          "data-testid": `phone-nav-${key}`,
          "data-active": on ? "true" : "false",
          "aria-current": on ? ("page" as const) : undefined,
          className: cls,
        };
        return inWorkspace ? (
          <a key={key} href={to} {...common}>
            {body}
          </a>
        ) : (
          <Link key={key} to={to} {...common}>
            {body}
          </Link>
        );
      })}
    </nav>
  );
}

/** The bottom bar of the root pages (Workspaces, Stats). */
export function RootNav() {
  const { pathname } = useLocation();
  if (!hasSiteNav) return null;
  return <SiteNav active={pathname === "/stats" ? "stats" : "workspaces"} inWorkspace={false} />;
}
