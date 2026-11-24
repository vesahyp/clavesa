/**
 * PhoneBackButton — the back button in the run sheet's header below md,
 * which covers the page header: back to its pipeline, one level up
 * (docs/ideas/hosted-navigation.md). The page header's own way up is
 * PhoneTopRow.
 */

import { ChevronLeft } from "lucide-react";

const cls =
  "-ml-1.5 flex min-h-11 min-w-11 max-w-[45%] shrink-0 items-center gap-0.5 rounded-md pr-1.5 text-sm text-muted-foreground hover:text-foreground md:hidden";

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
