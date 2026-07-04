/**
 * QueryShell — the shared loading / error / loaded triad for panels that
 * render a TanStack Query result (G P2-2).
 *
 * Renders the three branches with the exact semantics the hand-copied
 * panels used, so converting a panel is behavior-preserving:
 *   - `loading` while the first load is in flight (query.isLoading);
 *   - the error branch when the query has an error (and the first load is
 *     done — in TanStack v5 the two never co-occur);
 *   - `children(data)` whenever data is cached — including alongside the
 *     error branch when a background refetch fails but stale data is
 *     still present, matching the sibling-conditional panels this
 *     replaces.
 *
 * The copy stays per-page: pass `errorPrefix` for the standard
 * "Couldn't load X — <message>" Card, or `renderError` for bespoke error
 * markup. Empty states live inside `children`, where each page keeps its
 * own gate and wording.
 */

import type { ReactNode } from "react";

import { Card, CardContent } from "@/components/ui/card";

export interface QueryShellState<T> {
  isLoading: boolean;
  error: unknown;
  data: T | undefined;
}

interface QueryShellProps<T> {
  query: QueryShellState<T>;
  /** Rendered while the first load is in flight. */
  loading?: ReactNode;
  /** Prefix for the default error Card: "{errorPrefix} — {message}". */
  errorPrefix?: string;
  /** Custom error markup; wins over errorPrefix. Return null to render
   * nothing on error (panels that swallow errors). */
  renderError?: (error: unknown) => ReactNode;
  /** Rendered once data has arrived. */
  children?: (data: T) => ReactNode;
}

export function QueryShell<T>({
  query,
  loading,
  errorPrefix,
  renderError,
  children,
}: QueryShellProps<T>) {
  return (
    <>
      {query.isLoading && loading}
      {!query.isLoading &&
        query.error != null &&
        (renderError ? (
          renderError(query.error)
        ) : (
          <Card>
            <CardContent className="p-6 text-sm text-destructive">
              {errorPrefix} —{" "}
              {query.error instanceof Error
                ? query.error.message
                : "unknown error"}
            </CardContent>
          </Card>
        ))}
      {children && query.data !== undefined && children(query.data)}
    </>
  );
}
