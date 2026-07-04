/**
 * RegistryList — shared scaffold for the workspace registry list pages
 * (Sources, Credentials, Notebooks, Dashboards), which all render the
 * same shape after their header: loading skeletons → error Card → empty
 * state → filter bar with "N of M" → no-match Card → row list (G P2-2).
 *
 * The page keeps its own filter state, filtering rules, and row/empty
 * markup; this component owns only the shared structure and the shared
 * copy ("Couldn't load {noun}", "No {noun} match").
 */

import type { ReactNode } from "react";

import { QueryShell, type QueryShellState } from "@/components/QueryShell";
import { ListSearch } from "@/components/ListSearch";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

interface RegistryListProps<T> {
  /** The page's list query — drives the loading / error / loaded triad. */
  query: QueryShellState<unknown>;
  /** Every registry entry (only read once the query has data). */
  items: T[];
  /** Entries surviving the page's free-text filter. */
  filtered: T[];
  /** Free-text filter value + setter (state lives in the page). */
  search: string;
  onSearchChange: (value: string) => void;
  searchPlaceholder: string;
  /** Plural noun for the shared copy: "Couldn't load {noun} — …",
   * "No {noun} match …". */
  noun: string;
  /** Skeleton row class; most registry pages use h-16, Dashboards h-12. */
  skeletonClassName?: string;
  /** Empty-state card, shown when the registry has no entries at all.
   * Suppressed while `showEmpty` is false (e.g. the inline create form
   * is open). */
  empty: ReactNode;
  showEmpty?: boolean;
  /** One row per filtered entry; must set its own key. */
  renderItem: (item: T) => ReactNode;
}

export function RegistryList<T>({
  query,
  items,
  filtered,
  search,
  onSearchChange,
  searchPlaceholder,
  noun,
  skeletonClassName = "h-16 w-full",
  empty,
  showEmpty = true,
  renderItem,
}: RegistryListProps<T>) {
  const q = search.trim().toLowerCase();
  return (
    <QueryShell
      query={query}
      loading={
        <div className="space-y-3">
          <Skeleton className={skeletonClassName} />
          <Skeleton className={skeletonClassName} />
        </div>
      }
      errorPrefix={`Couldn't load ${noun}`}
    >
      {() => (
        <>
          {items.length === 0 && showEmpty && empty}

          {items.length > 0 && (
            <div className="mb-4 flex items-center gap-3">
              <ListSearch
                value={search}
                onChange={onSearchChange}
                placeholder={searchPlaceholder}
              />
              {q && (
                <span className="text-xs text-muted-foreground">
                  <span className="font-semibold text-foreground">
                    {filtered.length}
                  </span>{" "}
                  of {items.length}
                </span>
              )}
            </div>
          )}

          {items.length > 0 && filtered.length === 0 && (
            <Card>
              <CardContent className="py-10 text-center text-sm text-muted-foreground">
                No {noun} match{" "}
                <span className="font-mono text-foreground">{search}</span>.
              </CardContent>
            </Card>
          )}

          {filtered.length > 0 && (
            <ul className="grid gap-3">{filtered.map(renderItem)}</ul>
          )}
        </>
      )}
    </QueryShell>
  );
}
