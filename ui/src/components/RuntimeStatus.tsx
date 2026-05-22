/**
 * RuntimeStatus — ambient warm-Spark worker indicator.
 *
 * The first Catalog/table query of a fresh `clavesa ui` session
 * blocks ~30s while the warm-Spark container boots, with no UI hint.
 * This sits in the app header and surfaces that:
 *
 *   - invisible when nothing is happening (the steady state)
 *   - amber pulsing "Starting Spark…" while a worker is spawning
 *   - a brief green "Spark ready" once it finishes, then gone
 *
 * Backed by GET /api/runtime/workers (see useRuntimeWorkers).
 */

import { useEffect, useRef, useState } from "react";

import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useRuntimeWorkers } from "@/lib/queries";

// How long the green "Spark ready" confirmation stays up after a spawn
// finishes before the indicator goes quiet again.
const READY_FLASH_MS = 1500;

export function RuntimeStatus() {
  const { data } = useRuntimeWorkers();
  const spawning = (data?.workers ?? []).some((w) => w.state === "spawning");

  // Show a brief "Spark ready" confirmation on the spawning→done edge.
  const [justReady, setJustReady] = useState(false);
  const wasSpawning = useRef(false);
  useEffect(() => {
    if (wasSpawning.current && !spawning) {
      setJustReady(true);
      const t = setTimeout(() => setJustReady(false), READY_FLASH_MS);
      wasSpawning.current = spawning;
      return () => clearTimeout(t);
    }
    wasSpawning.current = spawning;
  }, [spawning]);

  if (spawning) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <div className="flex animate-in fade-in-0 items-center gap-1.5 text-xs text-muted-foreground">
            <span className="h-2 w-2 animate-pulse rounded-full bg-status-running" />
            <span>Starting Spark…</span>
          </div>
        </TooltipTrigger>
        <TooltipContent className="max-w-xs">
          The local Spark worker is booting (~30s on a cold start). The
          first Catalog or table query waits for it; every query after
          that is near-instant.
        </TooltipContent>
      </Tooltip>
    );
  }

  if (justReady) {
    return (
      <div className="flex animate-in fade-in-0 items-center gap-1.5 text-xs text-muted-foreground">
        <span className="h-2 w-2 rounded-full bg-status-success" />
        <span>Spark ready</span>
      </div>
    );
  }

  return null;
}
