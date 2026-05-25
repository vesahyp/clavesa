/**
 * Cell — one notebook cell: editor + toolbar + outputs.
 *
 * Code cells (Python/SQL) get the CodeMirror wrapper with language
 * highlighting derived from the `%%magic` on the first line. Markdown
 * cells render through CellMarkdown. Cells emit `onChange` on every edit
 * so the parent Notebook page can autosave (debounced).
 */

import { useMemo, useState } from "react";
import {
  ChevronDown,
  ChevronUp,
  GitBranch,
  Loader2,
  Play,
  Trash2,
  X,
} from "lucide-react";
import { EditorView } from "@codemirror/view";

import { CodeEditor } from "@/components/CodeEditor";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { NotebookCell } from "@/lib/queries";
import { CellOutput } from "./CellOutput";
import { CellMarkdown } from "./CellMarkdown";

export interface CellProps {
  cell: NotebookCell;
  busy?: boolean;          // a run is in flight for this cell
  onChange: (source: string) => void;
  onChangeType: (cellType: "code" | "markdown") => void;
  onRun: () => void;
  onCancel: () => void;
  onDelete: () => void;
  onGraduate: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
  /** Called with the CodeMirror view once the editor mounts — lets the
   * notebook page route catalog-browser inserts into the focused cell. */
  onEditorReady?: (view: EditorView | null) => void;
  /** Fired when the cell gains focus, so the notebook page can track
   * "where do inserts from the catalog sidebar land." */
  onFocus?: () => void;
}

export function Cell({
  cell,
  busy,
  onChange,
  onChangeType,
  onRun,
  onCancel,
  onDelete,
  onGraduate,
  onMoveUp,
  onMoveDown,
  onEditorReady,
  onFocus,
}: CellProps) {
  const source = useMemo(() => cell.source.join(""), [cell.source]);
  const language = useMemo<"sql" | "python">(() => {
    const trimmed = source.replace(/^[\s]+/, "");
    return trimmed.startsWith("%%sql") ? "sql" : "python";
  }, [source]);

  const lastStatus = cell.metadata?.clavesa?.last_status ?? "";
  const lastDuration = cell.metadata?.clavesa?.last_duration_ms ?? 0;
  const lastRunAt = cell.metadata?.clavesa?.last_run_at ?? "";

  return (
    <Card
      className={cn("transition-shadow", busy && "ring-1 ring-primary/40")}
      onMouseDownCapture={onFocus}
    >
      <CardContent className="space-y-3 p-3">
        <Toolbar
          cellType={cell.cell_type}
          language={language}
          busy={!!busy}
          lastStatus={lastStatus}
          lastDuration={lastDuration}
          lastRunAt={lastRunAt}
          onChangeType={onChangeType}
          onRun={onRun}
          onCancel={onCancel}
          onDelete={onDelete}
          onGraduate={onGraduate}
          onMoveUp={onMoveUp}
          onMoveDown={onMoveDown}
        />

        {cell.cell_type === "code" ? (
          <CodeEditor
            value={source}
            onValueChange={onChange}
            language={language}
            height={Math.max(80, Math.min(420, source.split("\n").length * 18 + 20))}
            lineNumbers
            wordWrap
            onReady={(view) => onEditorReady?.(view)}
          />
        ) : (
          <MarkdownEditor source={source} onChange={onChange} />
        )}

        {cell.cell_type === "code" && cell.outputs.length > 0 && (
          <div className="space-y-2">
            {cell.outputs.map((o, i) => (
              <CellOutput key={i} output={o} />
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function Toolbar({
  cellType,
  language,
  busy,
  lastStatus,
  lastDuration,
  lastRunAt,
  onChangeType,
  onRun,
  onCancel,
  onDelete,
  onGraduate,
  onMoveUp,
  onMoveDown,
}: {
  cellType: "code" | "markdown";
  language: "sql" | "python";
  busy: boolean;
  lastStatus: string;
  lastDuration: number;
  lastRunAt: string;
  onChangeType: (t: "code" | "markdown") => void;
  onRun: () => void;
  onCancel: () => void;
  onDelete: () => void;
  onGraduate: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
}) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <select
        className="h-7 rounded border bg-background px-2 text-xs"
        value={cellType}
        onChange={(e) => onChangeType(e.target.value as "code" | "markdown")}
      >
        <option value="code">code</option>
        <option value="markdown">markdown</option>
      </select>
      {cellType === "code" && (
        <span className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] uppercase text-muted-foreground">
          {language}
        </span>
      )}
      {cellType === "code" && lastStatus && (
        <StatusBadge
          status={lastStatus}
          durationMs={lastDuration}
          atISO={lastRunAt}
        />
      )}
      <div className="ml-auto flex items-center gap-1">
        <Button
          size="sm"
          variant="ghost"
          onClick={onMoveUp}
          title="Move up"
          className="h-7 w-7 p-0"
        >
          <ChevronUp className="h-4 w-4" />
        </Button>
        <Button
          size="sm"
          variant="ghost"
          onClick={onMoveDown}
          title="Move down"
          className="h-7 w-7 p-0"
        >
          <ChevronDown className="h-4 w-4" />
        </Button>
        {cellType === "code" && (
          busy ? (
            <Button size="sm" variant="secondary" onClick={onCancel}>
              <X className="mr-1 h-3.5 w-3.5" />
              Cancel
            </Button>
          ) : (
            <Button size="sm" onClick={onRun}>
              <Play className="mr-1 h-3.5 w-3.5" />
              Run
            </Button>
          )
        )}
        {busy && cellType === "code" && (
          <Loader2 className="h-4 w-4 animate-spin text-primary" />
        )}
        {cellType === "code" && (
          <Button
            size="sm"
            variant="ghost"
            onClick={onGraduate}
            title="Graduate this cell into a pipeline transform"
            className="h-7 w-7 p-0 text-muted-foreground hover:text-primary"
          >
            <GitBranch className="h-4 w-4" />
          </Button>
        )}
        <Button
          size="sm"
          variant="ghost"
          onClick={onDelete}
          title="Delete cell"
          className="h-7 w-7 p-0 text-muted-foreground hover:text-destructive"
        >
          <Trash2 className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}

function StatusBadge({
  status,
  durationMs,
  atISO,
}: {
  status: string;
  durationMs: number;
  atISO: string;
}) {
  const color =
    status === "ok"
      ? "bg-green-500/10 text-green-700 dark:text-green-400"
      : status === "error"
        ? "bg-destructive/10 text-destructive"
        : "bg-amber-500/10 text-amber-700 dark:text-amber-400";
  return (
    <span
      className={cn(
        "rounded px-2 py-0.5 font-mono text-[11px]",
        color,
      )}
      title={atISO ? `Last run: ${atISO}` : undefined}
    >
      {status} · {Math.round(durationMs)}ms
    </span>
  );
}

function MarkdownEditor({
  source,
  onChange,
}: {
  source: string;
  onChange: (s: string) => void;
}) {
  const [editing, setEditing] = useState(source === "");
  if (!editing) {
    return (
      <div onDoubleClick={() => setEditing(true)} className="cursor-text">
        <CellMarkdown source={source} />
        <div className="mt-1 text-[11px] text-muted-foreground">
          Double-click to edit
        </div>
      </div>
    );
  }
  return (
    <div>
      <textarea
        autoFocus
        value={source}
        onChange={(e) => onChange(e.target.value)}
        onBlur={() => setEditing(false)}
        className="h-32 w-full resize-y rounded border bg-background p-3 font-mono text-sm"
        placeholder="# Markdown"
      />
      <div className="mt-1 text-[11px] text-muted-foreground">
        Click outside to render
      </div>
    </div>
  );
}
