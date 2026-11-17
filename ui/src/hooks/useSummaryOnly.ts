/**
 * useSummaryOnly — true when the workspace is a hosted local-warehouse
 * workspace, which the hosted UI serves from the summary the Mac
 * publishes at run end (ADR-032 section 2). Only the reads the summary
 * keeps answer there (runs, node rows of the newest runs, run timings,
 * log tails of failed runs); the rest are 404. The queries for the rest
 * (table state, catalog tables, lineage, backfills) are skipped, and the
 * cards that draw them stay hidden, so the pages render with no failed
 * request.
 *
 * GET /workspace says so (`summary_only`). WorkspaceGate loads that
 * query before any page renders, so this reads the cache.
 */

import { useQuery } from "@tanstack/react-query";

import { getWorkspace } from "@/api/workspace";

export function useSummaryOnly(): boolean {
  const { data } = useQuery({
    queryKey: ["workspace"],
    queryFn: getWorkspace,
    staleTime: 60_000,
  });
  return data?.summary_only === true;
}

/**
 * useSnapshotAt — when a summary-only workspace's catalog and dashboard
 * snapshot was taken (ADR-036), or "" when it has none. Its Catalog and
 * Dashboards pages show that snapshot.
 */
export function useSnapshotAt(): string {
  const { data } = useQuery({
    queryKey: ["workspace"],
    queryFn: getWorkspace,
    staleTime: 60_000,
  });
  return data?.summary_only ? (data.snapshot_at ?? "") : "";
}
