/**
 * RootHeader and RootNav — the frame of the pages outside any workspace
 * of a multi-workspace or hosted server: Workspaces at "/" and, hosted
 * only, Stats at "/stats" (ADR-032 section 4). The header carries the
 * page links from md up; below md the bottom nav does, as PhoneNav does
 * inside a workspace. The local multi-workspace UI has no Stats page
 * (its server has no /api/stats), so it gets no nav: one page needs none.
 */

import { NavLink } from "react-router-dom";
import { ChartColumn, LayoutGrid } from "lucide-react";
import type { LucideIcon } from "lucide-react";

import { SignOutButton } from "@/components/SignOutButton";
import { runtimeConfig } from "@/lib/runtimeConfig";
import { cn } from "@/lib/utils";

/** True where the root pages have Stats beside Workspaces. */
export const hasStats = runtimeConfig.mode === "hosted";

const PAGES: { to: string; label: string; icon: LucideIcon; testid: string }[] = [
  { to: "/", label: "Workspaces", icon: LayoutGrid, testid: "root-nav-workspaces" },
  { to: "/stats", label: "Stats", icon: ChartColumn, testid: "root-nav-stats" },
];

export function RootHeader({ title }: { title: string }) {
  return (
    <header className="flex h-14 items-center gap-3 border-b border-border px-4 pt-[env(safe-area-inset-top)] md:px-6">
      <span className="text-base font-semibold tracking-tight">Clavesλ</span>
      <span className="text-sm text-muted-foreground md:hidden">{title}</span>
      {hasStats ? (
        <nav aria-label="Pages" className="hidden items-center gap-1 md:flex">
          {PAGES.map((p) => (
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

export function RootNav() {
  if (!hasStats) return null;
  return (
    <nav
      data-testid="root-nav"
      aria-label="Main"
      className="fixed inset-x-0 bottom-0 z-40 flex border-t border-border bg-card pb-[env(safe-area-inset-bottom)] md:hidden"
    >
      {PAGES.map(({ to, label, icon: Icon, testid }) => (
        <NavLink
          key={to}
          to={to}
          end
          data-testid={testid}
          className={({ isActive }) =>
            cn(
              "flex min-w-0 flex-1 flex-col items-center justify-center gap-0.5 py-2 text-[11px] font-medium",
              isActive ? "text-foreground" : "text-muted-foreground",
            )
          }
        >
          <Icon className="h-5 w-5" />
          <span className="max-w-full truncate">{label}</span>
        </NavLink>
      ))}
    </nav>
  );
}
