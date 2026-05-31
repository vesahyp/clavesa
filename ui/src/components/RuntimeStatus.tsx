/**
 * RuntimeStatus — persistent warm-Spark worker indicator.
 *
 * Sits in the app header and ALWAYS surfaces Spark's state, because
 * silent Spark-startup races and dead-container "no data yet" surfaces
 * have repeatedly confused users into thinking the UI was broken.
 *
 *   - grey "Spark idle"       — no worker yet (will spawn on first query)
 *   - amber "Starting Spark…" — container booting (~30s cold)
 *   - green "Spark ready"     — warm worker accepting queries
 *
 * Steady state is a single dim line — easy to ignore once you trust it,
 * but unmissable when it changes. Backed by GET /api/runtime/workers
 * (see useRuntimeWorkers).
 */

import { useIsFetching } from "@tanstack/react-query";

import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useRuntimeWorkers } from "@/lib/queries";

function formatAge(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

export function RuntimeStatus() {
  const { data } = useRuntimeWorkers();
  const workers = data?.workers ?? [];
  const spawning = workers.find((w) => w.state === "spawning");
  const ready = workers.find((w) => w.state === "ready");
  // Any in-flight Spark-backed query (table sample, snapshots, column
  // stats, dashboard widget) tags its TanStack query with meta.spark.
  // Counting them gives a live "a query is running" signal that the
  // worker-lifecycle states (idle/spawning/ready) never expose.
  const sparkBusy =
    useIsFetching({ predicate: (q) => q.meta?.spark === true }) > 0;

  if (spawning) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <span className="h-2 w-2 animate-pulse rounded-full bg-status-running" />
            <span>Starting Spark…</span>
          </div>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs">
          The local Spark worker is booting (~30s on a cold start). The
          first catalog / dashboard query waits for it; every query
          after that is near-instant.
        </TooltipContent>
      </Tooltip>
    );
  }

  if (ready && sparkBusy) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <span className="h-2 w-2 animate-pulse rounded-full bg-status-running" />
            <span>Spark · running query…</span>
          </div>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs">
          A query is running on the warm Spark worker.
        </TooltipContent>
      </Tooltip>
    );
  }

  if (ready) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <span className="h-2 w-2 rounded-full bg-status-success" />
            <span>Spark ready</span>
          </div>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs">
          <div className="space-y-1 text-left">
            <div className="font-medium text-foreground">
              Warm Spark worker running
            </div>
            <div>Up for {formatAge(ready.age_ms)}.</div>
            <div className="break-all font-mono text-[10px] text-muted-foreground">
              {ready.warehouse}
            </div>
            <div className="text-muted-foreground">
              Powers catalog reads, table samples, and the run-detail
              drill-down. The dashboard's runs grid sources directly
              from state.json files and doesn't need Spark.
            </div>
          </div>
        </TooltipContent>
      </Tooltip>
    );
  }

  // No worker yet — fresh `clavesa ui` session, or evicted during a
  // pipeline run (the orchestrator releases the warm worker to free
  // memory for transform containers). Distinct from "spawning" so the
  // user can tell at a glance whether their next query will pay the
  // cold-start tax.
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground/70">
          <span className="h-2 w-2 rounded-full bg-muted-foreground/40" />
          <span>Spark idle</span>
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">
        No warm Spark worker is running. The next catalog / table /
        drill-down query spawns one (~30s on cold start). Dashboard
        reads (runs, node_runs, tables-state) go through the filesystem
        and don't need Spark.
      </TooltipContent>
    </Tooltip>
  );
}
