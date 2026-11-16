/**
 * PhoneBack — the back button at the top of a page below the top level,
 * below md, in the hosted and multi-workspace UI. The bottom bar stays the
 * same on every page (RootNav.tsx, SiteNav), so going up is this button.
 *
 * It goes to the logical parent, not to history.back(), so a deep link
 * from a push or a bookmark also has a way up:
 *   workspace home ("/" in /w/<name>/)  -> Workspaces (a full page load)
 *   a pipeline                         -> the workspace home
 *   any other workspace page           -> the workspace home
 * A run (the pipeline page with ?run=) has its back button in the run
 * sheet header (RunDetailSheet), because the sheet covers this header.
 */

import { Link, useLocation } from "react-router-dom";
import { ChevronLeft } from "lucide-react";

import { homePath, runtimeConfig } from "@/lib/runtimeConfig";

const cls =
  "-ml-1.5 flex min-h-11 min-w-11 max-w-[45%] shrink-0 items-center gap-0.5 rounded-md pr-1.5 text-sm text-muted-foreground hover:text-foreground md:hidden";

export function PhoneBack() {
  const { pathname } = useLocation();
  if (runtimeConfig.mode === "local") return null;
  if (pathname === homePath) {
    // "/" outside this workspace's basename: a full page load.
    return (
      <a href="/" data-testid="phone-back" aria-label="Back to Workspaces" className={cls}>
        <ChevronLeft className="h-5 w-5 shrink-0" />
        <span className="truncate">Workspaces</span>
      </a>
    );
  }
  const name = runtimeConfig.workspace || "Overview";
  return (
    <Link to={homePath} data-testid="phone-back" aria-label={`Back to ${name}`} className={cls}>
      <ChevronLeft className="h-5 w-5 shrink-0" />
      <span className="truncate">{name}</span>
    </Link>
  );
}

/** The same button for the run sheet: back to its pipeline. */
export function PhoneBackButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button
      type="button"
      data-testid="phone-back"
      aria-label={`Back to ${label}`}
      onClick={onClick}
      className={cls.replace("max-w-[45%] shrink-0", "max-w-full self-start")}
    >
      <ChevronLeft className="h-5 w-5 shrink-0" />
      <span className="truncate font-mono">{label}</span>
    </button>
  );
}
