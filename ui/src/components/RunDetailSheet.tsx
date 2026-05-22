/**
 * RunDetailSheet — the per-execution drill-down rendered as a right-side
 * Sheet on top of the pipeline dashboard. Open / close state lives in
 * the URL (`?run=…`), so deep links work and the back button does the
 * intuitive thing.
 *
 * The body is the same `RunDetailView` the standalone /pipelines/run
 * route renders; this is just the chrome.
 */
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { RunDetailView } from "./RunDetailView";

export interface RunDetailSheetProps {
  dir: string;
  runId: string | null;
  onClose: () => void;
}

export function RunDetailSheet({ dir, runId, onClose }: RunDetailSheetProps) {
  return (
    <Sheet
      open={!!runId}
      onOpenChange={(o) => {
        if (!o) onClose();
      }}
    >
      <SheetContent>
        <SheetHeader>
          <SheetTitle className="font-mono text-sm">
            Run{runId ? ` ${runId.slice(0, 12)}` : ""}
          </SheetTitle>
          <SheetDescription>
            Drill-down: status, DAG, per-node breakdown.
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1">
          {runId && <RunDetailView dir={dir} runId={runId} embedded />}
        </div>
      </SheetContent>
    </Sheet>
  );
}
