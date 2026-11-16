/**
 * Breadcrumbs — the clickable location path in the app header.
 *
 * Every segment is a link, including the last (current) one, so going "up
 * one level" is always one click. The last segment is styled as current.
 */

import { Link } from "react-router-dom";
import { ChevronRight } from "lucide-react";
import type { ReactNode } from "react";

import { runtimeConfig } from "@/lib/runtimeConfig";
import { cn } from "@/lib/utils";

// Hosted and multi-workspace, below md: PhoneBack is the way up, so only
// the current page's crumb shows beside it.
const phoneLastOnly = runtimeConfig.mode !== "local";

export interface Crumb {
  label: ReactNode;
  to: string;
}

export function Breadcrumbs({ items }: { items: Crumb[] }) {
  return (
    <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1.5">
      {items.map((c, i) => {
        const isLast = i === items.length - 1;
        return (
          <span
            key={`${c.to}-${i}`}
            className={cn(
              "flex min-w-0 items-center gap-1.5",
              phoneLastOnly && !isLast && "hidden md:flex",
            )}
          >
            {i > 0 && (
              <ChevronRight
                className={cn(
                  "h-3.5 w-3.5 shrink-0 text-muted-foreground/60",
                  phoneLastOnly && "hidden md:block",
                )}
              />
            )}
            <Link
              to={c.to}
              aria-current={isLast ? "page" : undefined}
              className={cn(
                "inline-flex min-h-11 min-w-11 items-center truncate text-sm transition-colors",
                isLast
                  ? "font-medium text-foreground"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {c.label}
            </Link>
          </span>
        );
      })}
    </nav>
  );
}
