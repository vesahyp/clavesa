/**
 * NodePalette — a floating "Add node" control that overlays the editor
 * canvas (top-left). Replaces the old permanent left sidebar so the DAG
 * canvas gets the full width.
 *
 * Sources are *not* listed here — ADR-017 moved source authoring to the
 * workspace registry at `/sources`. The popover links there so an empty
 * pipeline still has an obvious next action.
 *
 * Each entry calls `POST /pipeline/typed-nodes` (service.AddNode under the
 * hood), which threads pipeline_name / bucket / catalog / schema /
 * runner_image and pins `?ref=` to the current ModuleVersion — matches
 * the `clavesa node add` CLI exactly.
 */

import { useEffect, useRef, useState } from "react";
import { Database, Workflow, Upload, Loader2, ExternalLink, Plus, X } from "lucide-react";
import { Link } from "react-router-dom";

import { addTypedNode } from "../api/pipeline";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface NodePaletteProps {
  dir: string;
  onGraphUpdate: () => void;
}

// ---------------------------------------------------------------------------
// Node-type catalogue (transforms + destinations only)
// ---------------------------------------------------------------------------

type NodeTypeEntry = {
  label: string;
  /** Wire value passed to `addTypedNode` */
  type: "transform" | "destination";
};

const NODE_TYPES: {
  section: string;
  icon: React.ReactNode;
  entries: NodeTypeEntry[];
}[] = [
  {
    section: "Transforms",
    icon: <Workflow className="h-3.5 w-3.5" />,
    entries: [{ label: "SQL Transform", type: "transform" }],
  },
  {
    section: "Destinations",
    icon: <Upload className="h-3.5 w-3.5" />,
    entries: [{ label: "S3 Destination", type: "destination" }],
  },
];

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function NodePalette({ dir, onGraphUpdate }: NodePaletteProps) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  // The user can stage a name before adding. Empty = let the backend
  // auto-generate (`transform1`, `transform2`, ...). A meaningful name
  // here flows straight through to the Iceberg table id
  // (`<name>__default`), so the SQL alias on downstream consumers and
  // the Catalog row label both come out readable from the first edit.
  const [pendingName, setPendingName] = useState<string>("");
  const rootRef = useRef<HTMLDivElement>(null);

  // Close the popover on outside-click and Escape — it's a transient
  // menu, not an editing surface, so dismiss-on-click-away is the right
  // affordance (unlike the config drawer).
  useEffect(() => {
    if (!open) return;
    function onPointerDown(e: PointerEvent) {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    function onKey(e: KeyboardEvent) {
      if (e.key !== "Escape") return;
      // Capture-phase + stopImmediatePropagation so this Escape closes
      // only the popover — the editor's own Escape handler (which closes
      // the config drawer) never sees it.
      e.stopImmediatePropagation();
      setOpen(false);
    }
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [open]);

  async function handleAdd(entry: NodeTypeEntry) {
    setLoading(entry.type);
    setError(null);
    try {
      await addTypedNode(dir, entry.type, pendingName.trim());
      setPendingName("");
      onGraphUpdate();
      setOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(null);
    }
  }

  return (
    <div ref={rootRef} className="absolute left-3 top-3 z-10" data-testid="node-palette">
      <Button
        onClick={() => setOpen((v) => !v)}
        size="sm"
        className="h-8 gap-1.5 shadow-md"
        data-testid="add-node-toggle"
      >
        {open ? <X className="h-3.5 w-3.5" /> : <Plus className="h-3.5 w-3.5" />}
        Add node
      </Button>

      {open && (
        <div className="mt-1.5 w-60 rounded-lg border border-border bg-background p-3 text-foreground shadow-xl">
          {error && (
            <div
              role="alert"
              className="mb-2 rounded-md border border-status-failed/40 bg-status-failed/10 p-2 text-xs text-status-failed"
            >
              {error}
            </div>
          )}

          <label
            htmlFor="palette-node-name"
            className="block pb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground"
          >
            Node name{" "}
            <span className="font-normal normal-case text-muted-foreground/60">
              (optional)
            </span>
          </label>
          <Input
            id="palette-node-name"
            value={pendingName}
            onChange={(e) => setPendingName(e.target.value)}
            disabled={loading !== null}
            placeholder="e.g. revenue_by_payment"
            className="mb-3 h-7 text-xs"
            data-testid="palette-node-name"
          />

          {NODE_TYPES.map(({ section, icon, entries }) => (
            <div key={section} className="mb-3">
              <div className="flex items-center gap-1.5 pb-1 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                {icon}
                {section}
              </div>
              {entries.map((entry) => (
                <Button
                  key={entry.type}
                  onClick={() => handleAdd(entry)}
                  disabled={loading !== null}
                  variant="secondary"
                  size="sm"
                  className="my-0.5 w-full justify-start font-normal"
                  data-testid={`add-${entry.type}`}
                >
                  {loading === entry.type && (
                    <Loader2 className="h-3 w-3 animate-spin" />
                  )}
                  {loading === entry.type ? "Adding…" : entry.label}
                </Button>
              ))}
            </div>
          ))}

          <div className="flex items-center gap-1.5 border-t border-border pt-2 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            <Database className="h-3.5 w-3.5" />
            Sources
          </div>
          <Link
            to="/sources"
            className="my-0.5 flex w-full items-center justify-between rounded-md border border-border bg-background px-2 py-1.5 text-xs font-normal text-foreground hover:bg-accent"
            data-testid="sources-link"
          >
            <span>Manage sources</span>
            <ExternalLink className="h-3 w-3 text-muted-foreground" />
          </Link>
          <p className="mt-1 text-[10px] leading-tight text-muted-foreground">
            Sources live in the workspace registry. Register one at{" "}
            <code className="text-foreground">/sources</code>, then attach it
            to a transform&apos;s input.
          </p>
        </div>
      )}
    </div>
  );
}

export default NodePalette;
