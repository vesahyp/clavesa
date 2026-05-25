/**
 * Notebook — the editor page for one .ipynb.
 *
 * Vertical stack of Cell components. Loads via useNotebook, mutates local
 * state on edits, debounced-autosaves through saveNotebook. Cell run
 * triggers the runner via runNotebookCell; while running, the cell shows
 * a spinner + Cancel button.
 *
 * Stays simple — no undo stack, no multi-select. v1 surface; matches the
 * plan's "small, focused" scope.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { Plus, Save, StopCircle, Eraser, Table2 } from "lucide-react";
import { EditorView } from "@codemirror/view";
import { toast } from "sonner";

import { useChrome } from "@/components/PageChrome";
import { CatalogBrowser } from "@/components/CatalogBrowser";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Cell } from "@/components/notebook/Cell";
import {
  cancelNotebookCell,
  clearNotebookOutputs,
  graduateNotebookCell,
  runNotebookCell,
  saveNotebook,
  stopNotebookSession,
  usePipelines,
  useNotebook,
  type Notebook as NbType,
  type NotebookCell,
} from "@/lib/queries";

const AUTOSAVE_DEBOUNCE_MS = 600;

export function Notebook() {
  const { name = "" } = useParams<{ name: string }>();
  const query = useNotebook(name);
  const qc = useQueryClient();
  const navigate = useNavigate();

  // Local mirror of the notebook for edits. We sync from server data once
  // on load (and on hard refetch); subsequent edits live here until save.
  const [draft, setDraft] = useState<NbType | null>(null);
  // Map cell.id → last cell_run_id so /cancel knows which tag to interrupt.
  const runningCellRef = useRef<Map<string, string>>(new Map());
  const [runningIds, setRunningIds] = useState<Set<string>>(new Set());
  const [graduatingCell, setGraduatingCell] = useState<NotebookCell | null>(null);
  const pipelines = usePipelines();

  // Catalog browser side panel: per-cell EditorView refs so the sidebar's
  // click-to-insert lands at the cursor of whichever code cell the user
  // most recently clicked into. View handle comes from CodeEditor's
  // onReady; lastFocusedCellId updates on cell mousedown.
  const [showCatalog, setShowCatalog] = useState(false);
  const editorViewsRef = useRef<Map<string, EditorView | null>>(new Map());
  const [activeCellId, setActiveCellId] = useState<string | null>(null);

  function insertIdent(text: string) {
    let targetId = activeCellId;
    // Fall back to first code cell if nothing focused yet — covers the
    // "user opens the notebook, clicks a table" case before any cell
    // got focus.
    if (!targetId && draft) {
      const first = draft.cells.find((c) => c.cell_type === "code");
      targetId = first?.id ?? null;
    }
    if (!targetId) return;
    const view = editorViewsRef.current.get(targetId);
    if (!view) return;
    const { from, to } = view.state.selection.main;
    view.dispatch({
      changes: { from, to, insert: text },
      selection: { anchor: from + text.length },
    });
    view.focus();
  }

  useEffect(() => {
    if (query.data) setDraft(query.data);
  }, [query.data]);

  // Debounced autosave. Re-fires whenever draft changes; cancels any
  // pending save so we don't pile up requests.
  const saveTimer = useRef<number | null>(null);
  const [saving, setSaving] = useState(false);

  const persist = useCallback(
    async (nb: NbType) => {
      setSaving(true);
      try {
        const out = await saveNotebook(name, nb);
        qc.setQueryData(["notebook", name], out);
      } catch (e) {
        toast.error(e instanceof Error ? e.message : String(e));
      } finally {
        setSaving(false);
      }
    },
    [name, qc],
  );

  useEffect(() => {
    if (!draft) return;
    if (saveTimer.current) window.clearTimeout(saveTimer.current);
    saveTimer.current = window.setTimeout(() => {
      void persist(draft);
    }, AUTOSAVE_DEBOUNCE_MS);
    return () => {
      if (saveTimer.current) window.clearTimeout(saveTimer.current);
    };
  }, [draft, persist]);

  useChrome(
    useMemo(
      () => ({
        breadcrumbs: [
          { label: "Notebooks", to: "/notebooks" },
          { label: name, to: `/notebooks/${encodeURIComponent(name)}` },
        ],
        trailing: (
          <div className="flex items-center gap-2">
            {saving && (
              <span className="flex items-center gap-1 text-xs text-muted-foreground">
                <Save className="h-3 w-3" /> Saving…
              </span>
            )}
            <Button
              size="sm"
              variant={showCatalog ? "secondary" : "ghost"}
              title="Show workspace tables — click to insert at cursor"
              onClick={() => setShowCatalog((v) => !v)}
            >
              <Table2 className="mr-1 h-4 w-4" />
              Catalog
            </Button>
            <Button
              size="sm"
              variant="ghost"
              title="Clear all outputs (git-friendly)"
              onClick={async () => {
                try {
                  const out = await clearNotebookOutputs(name);
                  setDraft(out);
                  toast.success("Cleared outputs");
                } catch (e) {
                  toast.error(e instanceof Error ? e.message : String(e));
                }
              }}
            >
              <Eraser className="mr-1 h-4 w-4" />
              Clear outputs
            </Button>
            <Button
              size="sm"
              variant="ghost"
              title="Stop the notebook's REPL subprocess (resets Python globals)"
              onClick={async () => {
                try {
                  await stopNotebookSession(name);
                  toast.success("Session stopped");
                } catch (e) {
                  toast.error(e instanceof Error ? e.message : String(e));
                }
              }}
            >
              <StopCircle className="mr-1 h-4 w-4" />
              Stop session
            </Button>
          </div>
        ),
      }),
      [name, saving, showCatalog],
    ),
  );

  if (query.isLoading || !draft) {
    return (
      <div className="mx-auto w-full max-w-5xl space-y-3 px-6 py-8">
        <Skeleton className="h-10 w-64" />
        <Skeleton className="h-48 w-full" />
        <Skeleton className="h-48 w-full" />
      </div>
    );
  }

  if (query.error) {
    return (
      <div className="mx-auto w-full max-w-5xl px-6 py-8">
        <Card>
          <CardContent className="p-6 text-sm text-destructive">
            Couldn't load notebook —{" "}
            {query.error instanceof Error
              ? query.error.message
              : "unknown error"}
          </CardContent>
        </Card>
        <Button className="mt-4" variant="outline" onClick={() => navigate("/notebooks")}>
          ← Back to notebooks
        </Button>
      </div>
    );
  }

  function setCells(updater: (cells: NotebookCell[]) => NotebookCell[]) {
    setDraft((d) => (d ? { ...d, cells: updater(d.cells) } : d));
  }

  async function runCell(cell: NotebookCell) {
    setRunningIds((s) => new Set(s).add(cell.id));
    runningCellRef.current.set(cell.id, cell.id);
    try {
      const res = await runNotebookCell(name, cell.id);
      // Patch our local draft with the freshly persisted cell (outputs +
      // metadata.clavesa.*). Other cells unchanged.
      setCells((cells) =>
        cells.map((c) => (c.id === cell.id ? res.cell : c)),
      );
      // Toast non-OK statuses so the user notices even if they scrolled away.
      if (res.result.status === "error") {
        toast.error(`Cell errored: ${res.result.error?.ename ?? "error"}`);
      } else if (res.result.status === "cancelled") {
        toast.info("Cell cancelled");
      }
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setRunningIds((s) => {
        const next = new Set(s);
        next.delete(cell.id);
        return next;
      });
      runningCellRef.current.delete(cell.id);
    }
  }

  async function cancelCell(cell: NotebookCell) {
    const tag = runningCellRef.current.get(cell.id);
    if (!tag) return;
    try {
      await cancelNotebookCell(name, tag);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    }
  }

  function newCell(cellType: "code" | "markdown"): NotebookCell {
    return {
      cell_type: cellType,
      id: crypto.randomUUID().replace(/-/g, ""),
      source: cellType === "code" ? ["%%python\n"] : [""],
      metadata: {},
      outputs: [],
    };
  }

  return (
    <div className="mx-auto w-full max-w-5xl px-6 py-8">
      <div className="mb-6 flex items-baseline justify-between">
        <h1 className="font-mono text-2xl font-semibold tracking-tight">
          {name}
        </h1>
        <p className="text-xs text-muted-foreground">
          {draft.cells.length} cell{draft.cells.length === 1 ? "" : "s"}
        </p>
      </div>

      <div className={showCatalog ? "flex gap-4" : ""}>
        <div className={showCatalog ? "min-w-0 flex-1 space-y-4" : "space-y-4"}>
        {draft.cells.map((c, idx) => (
          <Cell
            key={c.id}
            cell={c}
            busy={runningIds.has(c.id)}
            onEditorReady={(view) => editorViewsRef.current.set(c.id, view)}
            onFocus={() => setActiveCellId(c.id)}
            onChange={(source) =>
              setCells((cells) =>
                cells.map((x) => (x.id === c.id ? { ...x, source: [source] } : x)),
              )
            }
            onChangeType={(t) =>
              setCells((cells) =>
                cells.map((x) =>
                  x.id === c.id
                    ? {
                        ...x,
                        cell_type: t,
                        // When flipping code → markdown, drop outputs since
                        // nbformat rejects markdown cells with outputs.
                        outputs: t === "markdown" ? [] : x.outputs,
                        execution_count: t === "markdown" ? null : x.execution_count,
                      }
                    : x,
                ),
              )
            }
            onRun={() => void runCell(c)}
            onCancel={() => void cancelCell(c)}
            onDelete={() =>
              setCells((cells) => cells.filter((x) => x.id !== c.id))
            }
            onGraduate={() => setGraduatingCell(c)}
            onMoveUp={() => {
              if (idx === 0) return;
              setCells((cells) => {
                const next = [...cells];
                [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
                return next;
              });
            }}
            onMoveDown={() => {
              if (idx === draft.cells.length - 1) return;
              setCells((cells) => {
                const next = [...cells];
                [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
                return next;
              });
            }}
          />
        ))}
        </div>
        {showCatalog && (
          <div className="sticky top-4 self-start">
            <CatalogBrowser scope="workspace" onInsert={insertIdent} />
          </div>
        )}
      </div>

      <div className="mt-6 flex justify-center gap-2">
        <Button
          variant="outline"
          onClick={() => setCells((cells) => [...cells, newCell("code")])}
        >
          <Plus className="mr-1 h-4 w-4" />
          Code cell
        </Button>
        <Button
          variant="outline"
          onClick={() => setCells((cells) => [...cells, newCell("markdown")])}
        >
          <Plus className="mr-1 h-4 w-4" />
          Markdown cell
        </Button>
      </div>

      {graduatingCell && (
        <GraduateModal
          cell={graduatingCell}
          pipelines={pipelines.data ?? []}
          notebookName={name}
          onClose={() => setGraduatingCell(null)}
        />
      )}
    </div>
  );
}

function GraduateModal({
  cell,
  pipelines,
  notebookName,
  onClose,
}: {
  cell: NotebookCell;
  pipelines: { name: string; dir: string }[];
  notebookName: string;
  onClose: () => void;
}) {
  const [pipeline, setPipeline] = useState(pipelines[0]?.dir ?? "");
  const [transformName, setTransformName] = useState("");
  const [busy, setBusy] = useState(false);
  const navigate = useNavigate();

  async function submit() {
    if (!pipeline || !transformName.trim()) return;
    setBusy(true);
    try {
      await graduateNotebookCell(notebookName, cell.id, {
        pipeline,
        transform_name: transformName.trim(),
      });
      toast.success(`Graduated to ${pipeline}/transforms/${transformName.trim()}`);
      onClose();
      navigate(`/pipelines/dashboard?dir=${encodeURIComponent(pipeline)}`);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onClick={onClose}
    >
      <Card className="w-full max-w-md" onClick={(e) => e.stopPropagation()}>
        <CardContent className="space-y-4 p-6">
          <div>
            <h2 className="font-mono text-lg font-semibold">Graduate cell to transform</h2>
            <p className="mt-1 text-xs text-muted-foreground">
              Writes the cell source to <code className="rounded bg-muted px-1 py-0.5">transforms/&lt;name&gt;.{cell.source.join("").trimStart().startsWith("%%sql") ? "sql" : "py"}</code>{" "}
              and registers a transform node in the chosen pipeline. The new
              transform has no inputs wired — attach sources or upstream
              nodes via the editor afterward.
            </p>
          </div>

          <label className="block">
            <span className="mb-1 block text-xs font-semibold uppercase text-muted-foreground">
              Target pipeline
            </span>
            <select
              className="h-9 w-full rounded border bg-background px-2 text-sm"
              value={pipeline}
              onChange={(e) => setPipeline(e.target.value)}
              disabled={busy}
            >
              {pipelines.length === 0 && <option value="">(no pipelines)</option>}
              {pipelines.map((p) => (
                <option key={p.dir} value={p.dir}>
                  {p.name} ({p.dir})
                </option>
              ))}
            </select>
          </label>

          <label className="block">
            <span className="mb-1 block text-xs font-semibold uppercase text-muted-foreground">
              Transform name
            </span>
            <Input
              autoFocus
              value={transformName}
              onChange={(e) => setTransformName(e.target.value)}
              placeholder="enrich_orders"
              disabled={busy}
              onKeyDown={(e) => {
                if (e.key === "Enter") void submit();
              }}
            />
          </label>

          <div className="flex justify-end gap-2">
            <Button variant="ghost" onClick={onClose} disabled={busy}>
              Cancel
            </Button>
            <Button
              onClick={submit}
              disabled={busy || !pipeline || !transformName.trim()}
            >
              Graduate
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
