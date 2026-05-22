/**
 * BackfillStageDialog — stage a new backfill window from the pipeline
 * dashboard. Mirrors the CLI's `clavesa pipeline backfill stage`
 * surface field-for-field (ADR-015): node + from + to + direct.
 *
 * The Lambda invoke blocks the dialog for the full transform run (a Spark
 * cold start plus however long the window takes to process); the Loader
 * spinner is the entire progress UX while that happens. On success the
 * caller's onStaged fires so the parent can invalidate the backfills
 * query and navigate to the diff page.
 */

import { useEffect, useState } from "react";
import { Loader2 } from "lucide-react";
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
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { stageBackfill, type BackfillRun } from "@/lib/queries";

export interface BackfillStageDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  dir: string;
  /** Transform-node IDs the user can pick from. */
  transformNodes: string[];
  /** Fires on successful stage. */
  onStaged: (run: BackfillRun) => void;
}

export function BackfillStageDialog({
  open,
  onOpenChange,
  dir,
  transformNodes,
  onStaged,
}: BackfillStageDialogProps) {
  const [node, setNode] = useState(transformNodes[0] ?? "");
  const [fromCursor, setFromCursor] = useState("");
  const [toCursor, setToCursor] = useState("");
  const [direct, setDirect] = useState(false);
  const [running, setRunning] = useState(false);

  // The parent dashboard always mounts this dialog (just toggles its
  // `open` prop). At mount time the pipeline graph fetch hasn't
  // returned yet, so transformNodes is []. useState above captures that
  // empty initial value and node stays "" — even though the rendered
  // <select> visually shows the first option (browsers fall back when
  // the controlled value doesn't match any option). handleStage then
  // hits the "no node" early-return and the user sees a Cancel-shaped
  // dead end. Sync from the prop once the graph lands.
  useEffect(() => {
    if (!node && transformNodes.length > 0) {
      setNode(transformNodes[0]);
    }
  }, [transformNodes, node]);

  function reset() {
    setNode(transformNodes[0] ?? "");
    setFromCursor("");
    setToCursor("");
    setDirect(false);
  }

  async function handleStage() {
    if (!node || !fromCursor || !toCursor) {
      toast.error("Pick a node and both cursors");
      return;
    }
    setRunning(true);
    try {
      const run = await stageBackfill({
        dir,
        node,
        from: fromCursor.split("/").map((s) => s.trim()).filter(Boolean),
        to: toCursor.split("/").map((s) => s.trim()).filter(Boolean),
        direct: direct || undefined,
      });
      toast.success(
        direct
          ? "Backfill written directly to canonical"
          : "Staging table created",
      );
      reset();
      onOpenChange(false);
      onStaged(run);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        if (!running) onOpenChange(o);
      }}
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Stage a backfill</DialogTitle>
          <DialogDescription>
            Replay a transform over a historical partition window. The
            runner reads only the [from, to] partitions and writes to a
            parallel staging table you can review before promoting.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-4">
          <div>
            <Label htmlFor="bf-node" className="text-xs">
              Node
            </Label>
            <select
              id="bf-node"
              value={node}
              onChange={(e) => setNode(e.target.value)}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs"
            >
              {transformNodes.length === 0 && (
                <option value="">(no transform nodes)</option>
              )}
              {transformNodes.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </div>

          <div className="grid grid-cols-2 gap-2">
            <div>
              <Label htmlFor="bf-from" className="text-xs">
                From cursor
              </Label>
              <Input
                id="bf-from"
                value={fromCursor}
                onChange={(e) => setFromCursor(e.target.value)}
                placeholder="2026/04/26/00"
                className="font-mono text-xs"
              />
            </div>
            <div>
              <Label htmlFor="bf-to" className="text-xs">
                To cursor
              </Label>
              <Input
                id="bf-to"
                value={toCursor}
                onChange={(e) => setToCursor(e.target.value)}
                placeholder="2026/04/27/00"
                className="font-mono text-xs"
              />
            </div>
          </div>
          <p className="text-[10px] text-muted-foreground">
            Slash-separated partition cursor (inclusive on both ends).
            Matches the source's <code>partitions</code> declaration.
          </p>

          <label className="flex items-start gap-2 text-xs">
            <input
              type="checkbox"
              checked={direct}
              onChange={(e) => setDirect(e.target.checked)}
              className="mt-0.5"
            />
            <span>
              <code className="font-mono">--direct</code> — skip staging,
              write straight to the canonical target. Only safe for
              merge-keyed outputs.
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
          <Button onClick={handleStage} size="sm" disabled={running}>
            {running && <Loader2 className="h-4 w-4 animate-spin" />}
            {running ? "Staging…" : "Stage"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
