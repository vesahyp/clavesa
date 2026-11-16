/**
 * SignOutButton — hosted mode only (ADR-032 section 4); renders nothing
 * in the local and multi-workspace UIs. The auth module is imported on
 * click, so this component costs the local bundle a few bytes.
 */

import { LogOut } from "lucide-react";

import { runtimeConfig } from "@/lib/runtimeConfig";
import { cn } from "@/lib/utils";

export function SignOutButton({
  compact = false,
  className,
}: {
  compact?: boolean;
  className?: string;
}) {
  if (runtimeConfig.mode !== "hosted") return null;
  return (
    <button
      type="button"
      data-testid="signout"
      aria-label="Sign out"
      title="Sign out"
      onClick={() => void import("@/hosted/auth").then((m) => m.signOut())}
      className={cn(
        "flex items-center gap-2 rounded-md px-3 py-2 text-sm text-muted-foreground transition-colors hover:bg-secondary/50 hover:text-foreground",
        compact && "min-h-11 min-w-11 justify-center px-2",
        className,
      )}
    >
      <LogOut className="h-4 w-4 shrink-0" />
      {!compact && <span>Sign out</span>}
    </button>
  );
}
