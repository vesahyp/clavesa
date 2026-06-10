/**
 * PipelineResetDialog — drop a pipeline's canonical output tables (and
 * optionally its consumer-side watermarks) from the pipeline dashboard.
 * Mirrors the CLI's `clavesa pipeline reset` surface (ADR-015).
 *
 * On open the dialog fetches the dry-run plan (POST /pipeline/reset/plan)
 * and renders exactly what would be deleted; toggling the watermark
 * checkbox re-fetches because include_watermarks changes the list. The
 * confirm button executes the reset and reports the receipt counts —
 * only what was actually deleted, not what was planned.
 */

import { useState } from "react";
import { Loader2 } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { planPipelineReset, resetPipeline } from "@/lib/queries";

export interface PipelineResetDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  dir: string;
  /** Fires on successful reset so the parent can invalidate caches. */
  onReset: () => void;
}

export function PipelineResetDialog({
  open,
  onOpenChange,
  dir,
  onReset,
}: PipelineResetDialogProps) {
  // Default ON: most resets exist to replay everything from the start,
  // and a kept watermark after a drop means an empty table until new
  // upstream data shows up — surprising unless explicitly chosen.
  const [includeWatermarks, setIncludeWatermarks] = useState(true);
  const [running, setRunning] = useState(false);

  // include_watermarks is part of the key so toggling the checkbox
  // re-fetches the plan (the watermark list comes from the server).
  const plan = useQuery({
    queryKey: ["pipeline-reset-plan", dir, includeWatermarks],
    enabled: open && Boolean(dir),
    retry: false,
    queryFn: () =>
      planPipelineReset({ dir, include_watermarks: includeWatermarks }),
  });

  async function handleReset() {
    setRunning(true);
    try {
      const res = await resetPipeline({
        dir,
        include_watermarks: includeWatermarks,
      });
      const n = res.tables_dropped.length;
      const m = res.watermarks_cleared.length;
      toast.success(
        `Dropped ${n} table${n === 1 ? "" : "s"}, cleared ${m} watermark${m === 1 ? "" : "s"}`,
      );
      onOpenChange(false);
      onReset();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  }

  const tables = plan.data?.tables_dropped ?? [];
  const watermarks = plan.data?.watermarks_cleared ?? [];

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        if (!running) onOpenChange(o);
      }}
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Reset pipeline data</DialogTitle>
          <DialogDescription>
            Drop every transform's canonical output table so the next run
            rebuilds from scratch. Deployed infrastructure (Lambda, Step
            Functions, IAM) is not touched — reset is a data operation,
            not a destroy.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-4">
          {plan.isLoading && (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Resolving what would be deleted…
            </div>
          )}
          {plan.error instanceof Error && (
            <p className="text-sm text-destructive">{plan.error.message}</p>
          )}

          {plan.data && (
            <>
              <div>
                <p className="text-xs font-medium">
                  Tables to drop ({tables.length})
                </p>
                {tables.length === 0 ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    No output tables resolved for this pipeline.
                  </p>
                ) : (
                  <ul className="mt-1 max-h-40 overflow-y-auto rounded-md border border-border bg-muted/40 p-2">
                    {tables.map((t) => (
                      <li
                        key={`${t.node}:${t.output_key}`}
                        className="font-mono text-xs"
                      >
                        {t.table}
                      </li>
                    ))}
                  </ul>
                )}
              </div>

              {includeWatermarks && (
                <div>
                  <p className="text-xs font-medium">
                    Watermarks to clear ({watermarks.length})
                  </p>
                  {watermarks.length === 0 ? (
                    <p className="mt-1 text-xs text-muted-foreground">
                      No incremental inputs — nothing to clear.
                    </p>
                  ) : (
                    <ul className="mt-1 max-h-32 overflow-y-auto rounded-md border border-border bg-muted/40 p-2">
                      {watermarks.map((w) => (
                        <li
                          key={`${w.consumer}:${w.alias}`}
                          className="font-mono text-xs"
                        >
                          {w.consumer} ← {w.alias}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              )}
            </>
          )}

          <label className="flex items-start gap-2 text-xs">
            <input
              type="checkbox"
              checked={includeWatermarks}
              onChange={(e) => setIncludeWatermarks(e.target.checked)}
              className="mt-0.5"
            />
            <span>
              Clear watermarks (replay from start). Unchecked, dropped
              tables stay empty until new upstream data arrives.
            </span>
          </label>
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            size="sm"
            onClick={() => onOpenChange(false)}
            disabled={running}
          >
            Cancel
          </Button>
          <Button
            variant="destructive"
            size="sm"
            onClick={handleReset}
            disabled={running || plan.isLoading || !plan.data}
          >
            {running && <Loader2 className="h-4 w-4 animate-spin" />}
            {running ? "Resetting…" : "Drop tables"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
