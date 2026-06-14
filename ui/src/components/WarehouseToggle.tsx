/**
 * WarehouseToggle — workspace-wide local/cloud warehouse switch.
 *
 * Lives in the persistent AppShell header. Local and Cloud are two
 * separate warehouses: Local is the on-disk catalog on this machine;
 * Cloud is the deployed Glue catalog and S3 data. Switching flips which
 * warehouse every page reads and writes — it does not move data between
 * them. Where heavy work runs (local docker vs Lambda) is a separate,
 * per-action choice made on the Run and Backfill dialogs.
 *
 * The backend already dispatches by the warehouse, so a switch just
 * needs to persist the choice and invalidate every query so the pages
 * refetch against the new warehouse.
 */

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { setWarehouse, useWarehouse, type Warehouse } from "@/lib/queries";

const WAREHOUSES: ReadonlyArray<Warehouse["warehouse"]> = ["local", "cloud"];

export function WarehouseToggle() {
  const qc = useQueryClient();
  const { data, isLoading } = useWarehouse();
  const mutation = useMutation({
    mutationFn: setWarehouse,
    onSuccess: (res) => {
      qc.setQueryData(["warehouse"], res);
      // The warehouse changes what every endpoint returns — refetch all.
      void qc.invalidateQueries();
    },
    onError: (err) => {
      toast.error(
        err instanceof Error ? err.message : "Failed to switch warehouse",
      );
    },
  });

  const warehouse = data?.warehouse ?? "local";
  const busy = isLoading || mutation.isPending;

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div
          className="flex items-center gap-1.5"
          data-testid="warehouse-toggle"
        >
          <span className="text-xs font-medium text-muted-foreground">
            Warehouse
          </span>
          <div
            role="group"
            aria-label="Workspace warehouse"
            className="inline-flex items-center rounded-md border border-border p-0.5"
          >
            {WAREHOUSES.map((w) => (
              <button
                key={w}
                type="button"
                data-testid={`warehouse-toggle-${w}`}
                disabled={busy || w === warehouse}
                aria-pressed={w === warehouse}
                onClick={() => mutation.mutate(w)}
                className={cn(
                  "rounded-sm px-2.5 py-1 text-xs font-medium capitalize transition-colors",
                  w === warehouse
                    ? "bg-secondary text-secondary-foreground"
                    : "text-muted-foreground hover:text-foreground",
                  busy && w !== warehouse && "cursor-not-allowed opacity-50",
                )}
              >
                {w}
              </button>
            ))}
          </div>
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">
        <span className="font-medium capitalize">{warehouse}</span> warehouse.
        The warehouse picks which world every page reads and writes: Local is
        the on-disk catalog on this machine; Cloud is the deployed Glue
        catalog and S3 data. Switching changes what you&apos;re looking at —
        it never moves data. Where heavy work runs (local docker vs Lambda)
        is chosen per action on the Run and Backfill dialogs.
      </TooltipContent>
    </Tooltip>
  );
}
