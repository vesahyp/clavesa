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
import { RunDetailView } from "./RunDetailView";
import { RunPhoneView } from "./RunPhoneView";

export interface RunDetailSheetProps {
  dir: string;
  runId: string | null;
  onClose: () => void;
}

export function RunDetailSheet({ dir, runId, onClose }: RunDetailSheetProps) {
  const isPhone = useIsPhone();
  return (
    <Sheet
      open={!!runId}
      onOpenChange={(o) => {
        if (!o) onClose();
      }}
    >
      <SheetContent data-testid="run-detail-sheet">
        <SheetHeader>
          <SheetTitle className="font-mono text-sm">
            Run{runId ? ` ${runId.slice(0, 12)}` : ""}
          </SheetTitle>
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
