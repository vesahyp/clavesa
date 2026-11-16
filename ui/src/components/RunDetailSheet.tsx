/**
 * RunDetailSheet — the per-execution drill-down rendered as a right-side
 * Sheet on top of the pipeline dashboard. Open / close state lives in
 * the URL (`?run=…`), so deep links work and the back button does the
 * intuitive thing.
 *
 * The body is `RunDetailView`; this is just the chrome. (The old
 * standalone /pipelines/run page is a redirect-only shell now.)
 */
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { useIsPhone } from "@/hooks/useIsPhone";
import { stamp } from "@/lib/clock";
import { formatDuration } from "@/lib/format";
import { useRuns } from "@/lib/queries";
import { cn } from "@/lib/utils";
import { runtimeConfig } from "@/lib/runtimeConfig";
import { PhoneBackButton } from "./PhoneBack";
import { RunDetailView } from "./RunDetailView";
import { RunPhoneView } from "./RunPhoneView";

export interface RunDetailSheetProps {
  dir: string;
  runId: string | null;
  onClose: () => void;
}

export function RunDetailSheet({ dir, runId, onClose }: RunDetailSheetProps) {
  const isPhone = useIsPhone();
  // Hosted phone: the run is a page of its own. The sheet stops above the
  // bottom bar, which stays usable (non-modal: no dim, no focus trap), and
  // a back button to the pipeline replaces the X.
  const phonePage = isPhone && runtimeConfig.mode === "hosted";
  const pipelineName = dir.split("/").pop() || dir;
  // The run's start and duration name it on a phone.
  // Same query as RunPhoneView, so it shares the cache.
  const runs = useRuns(pipelineName, { dir, limit: 200, enabled: phonePage && !!runId });
  const run = runs.data?.rows.find((r) => r.run_id === runId);
  return (
    <Sheet
      modal={!phonePage}
      open={!!runId}
      onOpenChange={(o) => {
        if (!o) onClose();
      }}
    >
      <SheetContent
        data-testid="run-detail-sheet"
        hideClose={phonePage}
        // No focus ring on the back button when the run page opens.
        onOpenAutoFocus={phonePage ? (e) => e.preventDefault() : undefined}
        className={
          phonePage
            ? "top-0 bottom-[calc(3.75rem+env(safe-area-inset-bottom))] border-l-0 shadow-none"
            : undefined
        }
      >
        <SheetHeader>
          {phonePage && (
            <PhoneBackButton label={pipelineName} onClick={onClose} />
          )}
          <SheetTitle data-testid="run-title" className={cn("text-sm", !phonePage && "font-mono")}>
            {phonePage
              ? run
                ? `Run ${stamp(run.started_at)}${run.duration_ms != null ? `, ${formatDuration(run.duration_ms)}` : ""}`
                : "Run"
              : `Run${runId ? ` ${runId.slice(0, 12)}` : ""}`}
          </SheetTitle>
          {/* The run ID stays small in RunPhoneView, under the result line. */}
          <SheetDescription>
            {isPhone
              ? "Node timeline, rows per node, errors."
              : "Drill-down: status, DAG, per-node breakdown."}
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1">
          {/* Phone (below md, ADR-032): timeline, rows per node and
              the error with its log tail instead of the DAG. */}
          {runId &&
            (isPhone ? (
              <RunPhoneView dir={dir} runId={runId} />
            ) : (
              <RunDetailView dir={dir} runId={runId} />
            ))}
        </div>
      </SheetContent>
    </Sheet>
  );
}
