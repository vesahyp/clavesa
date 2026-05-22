/**
 * EnvModeToggle — workspace-wide local/cloud environment switch.
 *
 * Lives in the persistent AppShell header. Local and Cloud are two
 * separate worlds: Local reads the on-disk Hadoop catalog and runs
 * transforms in the local runner; Cloud reads Glue/Athena and operates
 * the deployed pipeline (Step Functions). Switching flips which world
 * every page reads from — it does not move data between them.
 *
 * The backend already dispatches by the mode, so a
 * switch just needs to persist the choice and invalidate every query so
 * the pages refetch against the new world.
 */

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import {
  setEnvironmentMode,
  useEnvironmentMode,
  type EnvironmentMode,
} from "@/lib/queries";

const MODES: ReadonlyArray<EnvironmentMode["mode"]> = ["local", "cloud"];

export function EnvModeToggle() {
  const qc = useQueryClient();
  const { data, isLoading } = useEnvironmentMode();
  const mutation = useMutation({
    mutationFn: setEnvironmentMode,
    onSuccess: (res) => {
      qc.setQueryData(["environment"], res);
      // The mode changes what every endpoint returns — refetch all.
      void qc.invalidateQueries();
    },
    onError: (err) => {
      toast.error(
        err instanceof Error ? err.message : "Failed to switch environment",
      );
    },
  });

  const mode = data?.mode ?? "local";
  const busy = isLoading || mutation.isPending;

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div className="flex items-center gap-1.5">
          <span className="text-xs font-medium text-muted-foreground">
            Env
          </span>
          <div
            role="group"
            aria-label="Workspace environment mode"
            className="inline-flex items-center rounded-md border border-border p-0.5"
          >
            {MODES.map((m) => (
              <button
                key={m}
                type="button"
                disabled={busy || m === mode}
                aria-pressed={m === mode}
                onClick={() => mutation.mutate(m)}
                className={cn(
                  "rounded-sm px-2.5 py-1 text-xs font-medium capitalize transition-colors",
                  m === mode
                    ? "bg-secondary text-secondary-foreground"
                    : "text-muted-foreground hover:text-foreground",
                  busy && m !== mode && "cursor-not-allowed opacity-50",
                )}
              >
                {m}
              </button>
            ))}
          </div>
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-xs">
        <span className="font-medium capitalize">{mode}</span> environment.
        Local and Cloud are separate warehouses — Local runs transforms in
        the local runner against the on-disk catalog; Cloud operates the
        deployed pipeline via Glue, Athena, and Step Functions. Switching
        changes which one every page reads; it does not move data between
        them.
      </TooltipContent>
    </Tooltip>
  );
}
