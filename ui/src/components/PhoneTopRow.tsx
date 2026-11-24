/**
 * PhoneTopRow — the top of every workspace page below md, in the hosted
 * and multi-workspace UI (docs/ideas/hosted-navigation.md):
 *   [<] [... >] parent                       [workspace chip]
 * The back arrow and the parent's name go one level up: to the
 * second-to-last crumb the page declares (useChrome), so a link from a
 * push or a bookmark has a way up too. Browser back still goes back in
 * history. "..." appears when there are more levels above the parent and
 * opens the full path. A section's own page has no parent: it shows the
 * brand. The page's name is its title below this row.
 */

import { useState } from "react";
import { Link } from "react-router-dom";
import { Check, ChevronLeft, ChevronRight } from "lucide-react";

import type { Crumb } from "./Breadcrumbs";
import { SwitcherChip } from "./WorkspaceSwitcher";

export function PhoneTopRow({ crumbs }: { crumbs: Crumb[] }) {
  const [pathOpen, setPathOpen] = useState(false);
  const parent = crumbs.length > 1 ? crumbs[crumbs.length - 2] : null;
  const more = crumbs.length > 2;
  return (
    <div className="flex min-w-0 flex-1 items-center gap-1 md:hidden">
      {parent ? (
        <>
          <Link
            to={parent.to}
            data-testid="phone-back"
            aria-label={`Back to ${typeof parent.label === "string" ? parent.label : "the level above"}`}
            className="-ml-2 flex min-h-11 min-w-11 shrink-0 items-center justify-center rounded-md text-foreground"
          >
            <ChevronLeft className="h-5 w-5" />
          </Link>
          {more && (
            <>
              <button
                type="button"
                data-testid="phone-path"
                onClick={() => setPathOpen(true)}
                aria-label="Show where you are"
                className="flex min-h-11 min-w-11 shrink-0 items-center justify-center rounded-md text-sm font-semibold text-muted-foreground"
              >
                <span className="rounded-md bg-secondary px-2 py-0.5">…</span>
              </button>
              <ChevronRight aria-hidden className="h-3.5 w-3.5 shrink-0 text-muted-foreground/60" />
            </>
          )}
          <Link
            to={parent.to}
            data-testid="phone-parent"
            className="flex min-h-11 min-w-11 items-center truncate font-mono text-sm text-muted-foreground"
          >
            <span className="truncate">{parent.label}</span>
          </Link>
        </>
      ) : (
        <span className="text-base font-semibold tracking-tight">Clavesλ</span>
      )}
      <span className="ml-auto" />
      <SwitcherChip />
      {pathOpen && (
        <>
          <div className="fixed inset-0 z-50 bg-black/50" onClick={() => setPathOpen(false)} aria-hidden />
          <div
            role="dialog"
            aria-label="Where you are"
            data-testid="phone-path-panel"
            className="fixed inset-x-0 bottom-0 z-[60] max-h-[80dvh] overflow-y-auto rounded-t-2xl border-t border-border bg-card p-3 pb-[calc(1rem+env(safe-area-inset-bottom))]"
          >
            <div aria-hidden className="mx-auto mb-2 h-1 w-10 rounded-full bg-border" />
            <div className="mb-1 mt-2 px-2 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
              Where you are
            </div>
            {crumbs.map((c, i) => (
              <Link
                key={`${c.to}-${i}`}
                to={c.to}
                onClick={() => setPathOpen(false)}
                aria-current={i === crumbs.length - 1 ? "page" : undefined}
                className="flex min-h-12 items-center gap-2 rounded-md pr-2 font-mono text-sm hover:bg-secondary/60"
                style={{ paddingLeft: `${0.5 + i * 0.9}rem` }}
              >
                <span className="min-w-0 flex-1 break-all">{c.label}</span>
                {i === crumbs.length - 1 && <Check className="h-4 w-4 shrink-0" />}
              </Link>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
