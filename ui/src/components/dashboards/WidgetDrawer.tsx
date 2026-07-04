/**
 * WidgetDrawer — chart-first widget editing surface.
 *
 * Opens on grid click (URL `?widget=<id>`) and holds every per-widget
 * concern in one place: title + type, the SQL that drives it, the field
 * mapping, and (collapsed by default) the layout coords. Replaces the
 * old three-tab editor's split between the Datasets and Widgets tabs.
 *
 * Datasets stay first-class on the wire. The drawer offers two modes:
 *
 *  - **Inline query** — the widget owns its SQL. Stored as a dataset
 *    named `__widget_<id>` so the wire schema is unchanged; the
 *    datasets sidebar filters out `__`-prefixed names.
 *  - **Shared dataset** — the widget binds to a user-named dataset.
 *    Two widgets reading the same shared dataset share one query
 *    (the React Query cache key is `[dir, sql, params]`, so identical
 *    SQL already dedupes; sharing buys naming + reuse, not network).
 *
 * Auto-preview rides on the existing `useDatasetColumns` hook in the
 * parent: when the SQL editor idles or blurs, the drawer commits the
 * draft to the bound dataset, which triggers the column refresh that
 * populates every field picker. `useDatasetColumns` keeps the last good
 * column set sticky through transient errors so the pickers don't blink
 * mid-edit.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import type { EditorView } from "@codemirror/view";
import { Copy, Loader2, Pencil, Plus, Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { CodeEditor } from "@/components/CodeEditor";
import { EngineBadge } from "@/components/EngineBadge";
import type { DatasetColumns } from "@/hooks/useDatasetColumns";
import type {
  DashboardControl,
  DashboardDataset,
  DashboardWidget,
  PipelineInfo,
} from "@/lib/queries";
import { cn } from "@/lib/utils";

import { CatalogBrowser } from "@/components/CatalogBrowser";
import { ColumnMultiSelect, ColumnSelect } from "./ColumnSelect";
import {
  availablePlaceholders,
  placeholderLinter,
} from "./placeholderLinter";
import { uniqueName } from "./uniqueName";

const WIDGET_TYPES = [
  "big_number",
  "line",
  "bar",
  "stacked_bar",
  "bar_line",
  "pie",
  "donut",
  "table",
] as const;

/** Prefix the wire schema doesn't care about; the sidebar uses it to hide. */
export const INLINE_DATASET_PREFIX = "__widget_";

export function inlineDatasetName(widgetId: string): string {
  return `${INLINE_DATASET_PREFIX}${widgetId}`;
}

export function isInlineDataset(name: string): boolean {
  return name.startsWith(INLINE_DATASET_PREFIX);
}

interface WidgetDrawerProps {
  widget: DashboardWidget | null;
  datasets: DashboardDataset[];
  pipelines: PipelineInfo[];
  controls: DashboardControl[];
  columnsByDataset: Map<string, DatasetColumns>;
  onChangeWidget: (widget: DashboardWidget) => void;
  onChangeDatasets: (datasets: DashboardDataset[]) => void;
  onDuplicate: (widgetId: string) => void;
  onDelete: (widgetId: string) => void;
  onClose: () => void;
}

export function WidgetDrawer({
  widget,
  datasets,
  pipelines,
  controls,
  columnsByDataset,
  onChangeWidget,
  onChangeDatasets,
  onDuplicate,
  onDelete,
  onClose,
}: WidgetDrawerProps) {
  return (
    <Sheet
      open={!!widget}
      onOpenChange={(o) => {
        if (!o) onClose();
      }}
    >
      <SheetContent className="overflow-y-auto">
        {widget && (
          <DrawerBody
            widget={widget}
            datasets={datasets}
            pipelines={pipelines}
            controls={controls}
            columnsByDataset={columnsByDataset}
            onChangeWidget={onChangeWidget}
            onChangeDatasets={onChangeDatasets}
            onDuplicate={onDuplicate}
            onDelete={onDelete}
            onClose={onClose}
          />
        )}
      </SheetContent>
    </Sheet>
  );
}

function DrawerBody({
  widget,
  datasets,
  pipelines,
  controls,
  columnsByDataset,
  onChangeWidget,
  onChangeDatasets,
  onDuplicate,
  onDelete,
  onClose,
}: Required<Omit<WidgetDrawerProps, "widget">> & { widget: DashboardWidget }) {
  // Memoise the lint source per controls array — a new function every
  // render would re-install the CodeMirror linter extension on every
  // keystroke and lose debounce.
  const lintSource = useMemo(() => placeholderLinter(controls), [controls]);
  const placeholders = useMemo(
    () => availablePlaceholders(controls),
    [controls],
  );
  const inlineName = inlineDatasetName(widget.id);
  const isInline = widget.dataset === "" || widget.dataset === inlineName;

  // Local SQL draft keeps every keystroke off the dataset-level state
  // (which fires a network query on change). Commit on idle / blur.
  const boundDs = datasets.find((d) => d.name === widget.dataset);
  const [draftSql, setDraftSql] = useState(boundDs?.sql ?? "");
  const [draftDir, setDraftDir] = useState(
    boundDs?.dir ?? pipelines[0]?.dir ?? "",
  );
  // Re-seed local draft whenever the selected widget changes (or its
  // bound dataset's SQL/dir was edited from elsewhere — e.g. another
  // widget sharing the same dataset).
  useEffect(() => {
    setDraftSql(boundDs?.sql ?? "");
    setDraftDir(boundDs?.dir ?? pipelines[0]?.dir ?? "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [widget.id, boundDs?.sql, boundDs?.dir]);

  function commitInlineSql(sql: string) {
    if (!isInline) return;
    const idx = datasets.findIndex((d) => d.name === inlineName);
    if (idx === -1) {
      onChangeDatasets([
        ...datasets,
        { name: inlineName, dir: draftDir, sql },
      ]);
      if (widget.dataset !== inlineName) {
        onChangeWidget({ ...widget, dataset: inlineName });
      }
    } else if (datasets[idx].sql !== sql) {
      const next = datasets.slice();
      next[idx] = { ...next[idx], sql };
      onChangeDatasets(next);
    }
  }

  function commitInlineDir(dir: string) {
    setDraftDir(dir);
    if (!isInline) return;
    const idx = datasets.findIndex((d) => d.name === inlineName);
    if (idx === -1) {
      onChangeDatasets([
        ...datasets,
        { name: inlineName, dir, sql: draftSql },
      ]);
      if (widget.dataset !== inlineName) {
        onChangeWidget({ ...widget, dataset: inlineName });
      }
    } else if (datasets[idx].dir !== dir) {
      const next = datasets.slice();
      next[idx] = { ...next[idx], dir };
      onChangeDatasets(next);
    }
  }

  function bindToShared(name: string) {
    onChangeWidget({ ...widget, dataset: name });
  }

  function switchToInline() {
    // Carry the currently-bound shared dataset's SQL/dir as the seed,
    // so toggling Inline doesn't blank the editor.
    const seedSql = boundDs?.sql ?? "";
    const seedDir = boundDs?.dir ?? pipelines[0]?.dir ?? "";
    const idx = datasets.findIndex((d) => d.name === inlineName);
    if (idx === -1) {
      onChangeDatasets([
        ...datasets,
        { name: inlineName, dir: seedDir, sql: seedSql },
      ]);
    }
    onChangeWidget({ ...widget, dataset: inlineName });
  }

  // CM6 view ref for the catalog browser's insert-at-cursor.
  const viewRef = useRef<EditorView | null>(null);
  function insertAtCursor(text: string) {
    const view = viewRef.current;
    if (!view) return;
    const { from, to } = view.state.selection.main;
    view.dispatch({
      changes: { from, to, insert: text },
      selection: { anchor: from + text.length },
    });
    view.focus();
  }

  const columns = columnsByDataset.get(widget.dataset);
  const isLoadingCols = !!columns?.isLoading && (columns?.columns.length ?? 0) === 0;

  return (
    <div className="flex h-full flex-col gap-4 px-6 pb-6">
      {/* Pull the header back to the drawer edges so its bottom border
          spans the full width, while keeping the body sections padded. */}
      <SheetHeader className="-mx-6 space-y-1">
        <SheetTitle className="font-mono text-sm">
          {widget.title || "Untitled widget"}
        </SheetTitle>
        <SheetDescription>
          {widget.type} · binds to{" "}
          <code className="font-mono">
            {isInline ? "(inline query)" : widget.dataset || "(unbound)"}
          </code>
        </SheetDescription>
      </SheetHeader>

      {/* Identity — title, type, duplicate, delete. */}
      <section className="space-y-3">
        <div className="flex items-end gap-3">
          <div className="flex-1 space-y-1">
            <Label className="text-xs">Title</Label>
            <Input
              value={widget.title}
              onChange={(e) => onChangeWidget({ ...widget, title: e.target.value })}
            />
          </div>
          <div className="w-36 space-y-1">
            <Label className="text-xs">Type</Label>
            <Select
              value={widget.type}
              onValueChange={(v) => onChangeWidget({ ...widget, type: v })}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {WIDGET_TYPES.map((t) => (
                  <SelectItem key={t} value={t}>
                    {t}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <Button
            size="icon"
            variant="ghost"
            onClick={() => onDuplicate(widget.id)}
            aria-label="Duplicate widget"
            title="Duplicate"
          >
            <Copy className="h-4 w-4 text-muted-foreground" />
          </Button>
          <Button
            size="icon"
            variant="ghost"
            onClick={() => {
              onDelete(widget.id);
              onClose();
            }}
            aria-label="Delete widget"
            title="Delete"
          >
            <Trash2 className="h-4 w-4 text-muted-foreground" />
          </Button>
        </div>
      </section>

      {/* Data — inline SQL or shared dataset binding. */}
      <section className="space-y-3 border-t border-border pt-4">
        <div className="flex items-center justify-between">
          <h3 className="font-mono text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Data
          </h3>
          <DataModeSegmented
            isInline={isInline}
            onPickInline={switchToInline}
            onPickShared={() => {
              // Drop to the dropdown by binding nothing (user picks next).
              // If a shared dataset already exists, default to the first.
              const firstShared = datasets.find((d) => !isInlineDataset(d.name));
              if (firstShared) bindToShared(firstShared.name);
              else bindToShared("");
            }}
          />
        </div>

        {isInline ? (
          <>
            <SqlEditor
              pipelines={pipelines}
              dir={draftDir}
              sql={draftSql}
              onDirChange={commitInlineDir}
              onSqlChange={setDraftSql}
              onSqlIdle={commitInlineSql}
              onEditorReady={(v) => {
                viewRef.current = v;
              }}
              onInsert={insertAtCursor}
              lintSource={lintSource}
              placeholders={placeholders}
            />
            <PromoteToShared
              onPromote={(name) => {
                const taken = datasets.map((d) => d.name);
                const finalName = uniqueName(name || "dataset", taken);
                const filtered = datasets.filter((d) => d.name !== inlineName);
                onChangeDatasets([
                  ...filtered,
                  { name: finalName, dir: draftDir, sql: draftSql },
                ]);
                onChangeWidget({ ...widget, dataset: finalName });
              }}
            />
          </>
        ) : (
          <SharedDataEditor
            datasets={datasets.filter((d) => !isInlineDataset(d.name))}
            currentName={widget.dataset}
            currentDir={draftDir}
            currentSql={draftSql}
            onPick={(name) => {
              bindToShared(name);
              const ds = datasets.find((d) => d.name === name);
              if (ds) {
                setDraftSql(ds.sql);
                setDraftDir(ds.dir);
              }
            }}
            onSqlChange={setDraftSql}
            onSqlIdle={(sql) => {
              if (!widget.dataset) return;
              const idx = datasets.findIndex((d) => d.name === widget.dataset);
              if (idx === -1) return;
              if (datasets[idx].sql === sql) return;
              const next = datasets.slice();
              next[idx] = { ...next[idx], sql };
              onChangeDatasets(next);
            }}
            onDirChange={(dir) => {
              setDraftDir(dir);
              if (!widget.dataset) return;
              const idx = datasets.findIndex((d) => d.name === widget.dataset);
              if (idx === -1) return;
              if (datasets[idx].dir === dir) return;
              const next = datasets.slice();
              next[idx] = { ...next[idx], dir };
              onChangeDatasets(next);
            }}
            onCreate={(name, dir, sql) => {
              const taken = datasets.map((d) => d.name);
              const finalName = uniqueName(name || "dataset", taken);
              onChangeDatasets([
                ...datasets.filter((d) => d.name !== inlineName),
                { name: finalName, dir, sql },
              ]);
              onChangeWidget({ ...widget, dataset: finalName });
              setDraftSql(sql);
              setDraftDir(dir);
            }}
            onEditorReady={(v) => {
              viewRef.current = v;
            }}
            onInsert={insertAtCursor}
            pipelines={pipelines}
            lintSource={lintSource}
            placeholders={placeholders}
          />
        )}

        {!!columns?.error && !isLoadingCols && (
          <p className="text-xs text-destructive">
            Query failed:{" "}
            {columns?.error instanceof Error
              ? columns.error.message
              : String(columns?.error)}
          </p>
        )}
        {isLoadingCols && (
          <p className="flex items-center gap-1 text-xs text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" /> running query…
          </p>
        )}
        <RowsPreview columns={columns} />
        {/* ADR-024: where this widget's query runs. Predicted from the
            warehouse before the first run; confirmed from the response's own
            `served` stamp after (the /dashboards/query handler writes the
            provider QueryResult through), never derived from workspace
            state at render time. */}
        {!columns?.error && (
          <div className="flex justify-end">
            <EngineBadge
              served={columns?.served}
              surface="serving"
              testid="engine-badge-widget-editor"
            />
          </div>
        )}
      </section>

      {/* Field mapping — type-specific column pickers. */}
      <section className="space-y-3 border-t border-border pt-4">
        <h3 className="font-mono text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Field mapping
        </h3>
        <FieldMapping
          widget={widget}
          columns={columns}
          onChange={onChangeWidget}
        />
      </section>

      {/* Layout — collapsed by default; drag-resize the grid for most uses. */}
      <LayoutEditor widget={widget} onChange={onChangeWidget} />
    </div>
  );
}

/* ----------------------------------------------------------------------- */
/* Sub-pieces.                                                             */
/* ----------------------------------------------------------------------- */

function DataModeSegmented({
  isInline,
  onPickInline,
  onPickShared,
}: {
  isInline: boolean;
  onPickInline: () => void;
  onPickShared: () => void;
}) {
  return (
    <div className="flex rounded-md border border-border bg-muted/40 p-0.5 text-xs">
      <button
        type="button"
        onClick={onPickInline}
        className={cn(
          "rounded px-2 py-0.5 transition-colors",
          isInline
            ? "bg-background text-foreground shadow-sm"
            : "text-muted-foreground hover:text-foreground",
        )}
      >
        Inline query
      </button>
      <button
        type="button"
        onClick={onPickShared}
        className={cn(
          "rounded px-2 py-0.5 transition-colors",
          !isInline
            ? "bg-background text-foreground shadow-sm"
            : "text-muted-foreground hover:text-foreground",
        )}
      >
        Shared dataset
      </button>
    </div>
  );
}

function SqlEditor({
  pipelines,
  dir,
  sql,
  onDirChange,
  onSqlChange,
  onSqlIdle,
  onEditorReady,
  onInsert,
  height = 180,
  lintSource,
  placeholders,
}: {
  pipelines: PipelineInfo[];
  dir: string;
  sql: string;
  onDirChange: (v: string) => void;
  onSqlChange: (v: string) => void;
  onSqlIdle: (v: string) => void;
  onEditorReady: (v: EditorView) => void;
  onInsert: (text: string) => void;
  height?: number;
  /** CodeMirror lint source — typically the placeholder linter. */
  lintSource?: (doc: string, view: EditorView) => import("@codemirror/lint").Diagnostic[];
  /** Available `{{name}}` tokens, surfaced as click-to-insert chips. */
  placeholders?: string[];
}) {
  return (
    <div className="space-y-2">
      <div className="space-y-1">
        <Label className="text-xs">Pipeline</Label>
        {pipelines.length > 0 ? (
          <Select value={dir} onValueChange={onDirChange}>
            <SelectTrigger>
              <SelectValue placeholder="pick a pipeline" />
            </SelectTrigger>
            <SelectContent>
              {pipelines.map((p) => (
                <SelectItem key={p.dir} value={p.dir}>
                  {p.dir}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        ) : (
          <Input
            value={dir}
            onChange={(e) => onDirChange(e.target.value)}
            placeholder="pipeline dir"
            className="font-mono"
          />
        )}
      </div>
      <div className="space-y-1">
        <Label className="text-xs">SQL</Label>
        <div className="flex gap-2">
          <div className="min-w-0 flex-1 overflow-hidden rounded-md border border-border">
            <CodeEditor
              value={sql}
              onValueChange={onSqlChange}
              onIdle={onSqlIdle}
              language="sql"
              height={height}
              lineNumbers={false}
              wordWrap
              onReady={onEditorReady}
              sqlLint={lintSource}
            />
          </div>
          <CatalogBrowser dir={dir} onInsert={onInsert} />
        </div>
        <p className="text-xs text-muted-foreground">
          Auto-runs after a pause; field pickers refresh from the result.
        </p>
        {placeholders && placeholders.length > 0 && (
          <PlaceholderChips placeholders={placeholders} onInsert={onInsert} />
        )}
      </div>
    </div>
  );
}

const ROWS_PREVIEW_CAP = 10;

function RowsPreview({ columns }: { columns: DatasetColumns | undefined }) {
  // Nothing useful to show until a query has actually returned. The
  // sibling loading/error lines already cover those states.
  if (!columns || !columns.hasData || columns.error) return null;
  const cols = columns.columns;
  const rows = columns.rows;
  if (cols.length === 0) return null;
  return (
    <div className="max-h-64 overflow-auto rounded-md border border-border">
      {rows.length === 0 ? (
        <p className="p-3 text-xs text-muted-foreground">No rows.</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              {cols.map((c) => (
                <TableHead key={c.name} className="whitespace-nowrap">
                  {c.name}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.slice(0, ROWS_PREVIEW_CAP).map((row, i) => (
              <TableRow key={i}>
                {row.map((cell, j) => (
                  <TableCell
                    key={j}
                    className="whitespace-nowrap font-mono text-xs"
                  >
                    {cell === "" ? (
                      <span className="text-muted-foreground">—</span>
                    ) : (
                      cell
                    )}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
      {rows.length > ROWS_PREVIEW_CAP && (
        <p className="border-t border-border p-2 text-xs text-muted-foreground">
          Showing first {ROWS_PREVIEW_CAP} of {columns.rowCount}
          {columns.truncated && "+"} rows.
        </p>
      )}
    </div>
  );
}

function PlaceholderChips({
  placeholders,
  onInsert,
}: {
  placeholders: string[];
  onInsert: (text: string) => void;
}) {
  return (
    <div className="flex flex-wrap items-center gap-1.5 text-xs">
      <span className="text-muted-foreground">Available:</span>
      {placeholders.map((p) => {
        const token = `{{${p}}}`;
        return (
          <button
            key={p}
            type="button"
            onClick={() => onInsert(token)}
            className="rounded bg-muted px-1.5 py-0.5 font-mono text-[11px] text-muted-foreground hover:bg-muted/70 hover:text-foreground"
            title={`Insert ${token}`}
          >
            {token}
          </button>
        );
      })}
    </div>
  );
}

function PromoteToShared({
  onPromote,
}: {
  onPromote: (name: string) => void;
}) {
  const [promoteName, setPromoteName] = useState("");
  const [promoteOpen, setPromoteOpen] = useState(false);
  return (
    <div className="flex items-center justify-end gap-2">
      {promoteOpen ? (
        <>
          <Input
            value={promoteName}
            onChange={(e) => setPromoteName(e.target.value)}
            placeholder="dataset name"
            className="h-8 w-44 font-mono text-xs"
            autoFocus
            onKeyDown={(e) => {
              if (e.key === "Enter" && promoteName.trim()) {
                onPromote(promoteName.trim());
                setPromoteOpen(false);
                setPromoteName("");
              } else if (e.key === "Escape") {
                setPromoteOpen(false);
                setPromoteName("");
              }
            }}
          />
          <Button
            size="sm"
            variant="outline"
            onClick={() => {
              if (!promoteName.trim()) return;
              onPromote(promoteName.trim());
              setPromoteOpen(false);
              setPromoteName("");
            }}
          >
            Save
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => {
              setPromoteOpen(false);
              setPromoteName("");
            }}
          >
            Cancel
          </Button>
        </>
      ) : (
        <Button
          size="sm"
          variant="ghost"
          onClick={() => setPromoteOpen(true)}
          title="Save this query as a named dataset other widgets can reuse"
        >
          <Pencil className="mr-1 h-3.5 w-3.5" /> Save as shared
        </Button>
      )}
    </div>
  );
}

function SharedDataEditor({
  datasets,
  currentName,
  currentDir,
  currentSql,
  pipelines,
  onPick,
  onSqlChange,
  onSqlIdle,
  onDirChange,
  onCreate,
  onEditorReady,
  onInsert,
  lintSource,
  placeholders,
}: {
  datasets: DashboardDataset[];
  currentName: string;
  currentDir: string;
  currentSql: string;
  pipelines: PipelineInfo[];
  onPick: (name: string) => void;
  onSqlChange: (v: string) => void;
  onSqlIdle: (v: string) => void;
  onDirChange: (v: string) => void;
  onCreate: (name: string, dir: string, sql: string) => void;
  onEditorReady: (v: EditorView) => void;
  onInsert: (text: string) => void;
  lintSource?: (doc: string, view: EditorView) => import("@codemirror/lint").Diagnostic[];
  placeholders?: string[];
}) {
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");
  const [createDir, setCreateDir] = useState(pipelines[0]?.dir ?? "");
  const [createSql, setCreateSql] = useState("SELECT 1 AS n");
  const isBound = currentName !== "" && !!datasets.find((d) => d.name === currentName);
  return (
    <div className="space-y-2">
      <div className="flex items-end gap-2">
        <div className="flex-1 space-y-1">
          <Label className="text-xs">Pick a dataset</Label>
          <Select value={currentName || undefined} onValueChange={onPick}>
            <SelectTrigger>
              <SelectValue placeholder="(none yet — create one)" />
            </SelectTrigger>
            <SelectContent>
              {datasets.map((d) => (
                <SelectItem key={d.name} value={d.name} className="font-mono">
                  {d.name}
                </SelectItem>
              ))}
              {datasets.length === 0 && (
                <p className="px-3 py-2 text-xs text-muted-foreground">
                  No shared datasets yet — create one.
                </p>
              )}
            </SelectContent>
          </Select>
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={() => setCreating(true)}
        >
          <Plus className="mr-1 h-3.5 w-3.5" /> Create new
        </Button>
      </div>

      {/* When a shared dataset is bound, expose its SQL/dir inline so
          the editor doesn't bounce away to a different page to tweak it.
          Edits flow back to the dataset itself, so every widget bound
          to the same dataset re-renders together — that's the point of
          sharing. */}
      {isBound && !creating && (
        <SqlEditor
          pipelines={pipelines}
          dir={currentDir}
          sql={currentSql}
          onDirChange={onDirChange}
          onSqlChange={onSqlChange}
          onSqlIdle={onSqlIdle}
          onEditorReady={onEditorReady}
          onInsert={onInsert}
          height={160}
          lintSource={lintSource}
          placeholders={placeholders}
        />
      )}

      {creating && (
        <div className="space-y-2 rounded-md border border-border bg-muted/30 p-2">
          <div className="flex items-end gap-2">
            <div className="flex-1 space-y-1">
              <Label className="text-xs">Name</Label>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="revenue"
                className="font-mono"
                autoFocus
              />
            </div>
            <div className="flex-1 space-y-1">
              <Label className="text-xs">Pipeline</Label>
              {pipelines.length > 0 ? (
                <Select value={createDir} onValueChange={setCreateDir}>
                  <SelectTrigger>
                    <SelectValue placeholder="pick a pipeline" />
                  </SelectTrigger>
                  <SelectContent>
                    {pipelines.map((p) => (
                      <SelectItem key={p.dir} value={p.dir}>
                        {p.dir}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <Input
                  value={createDir}
                  onChange={(e) => setCreateDir(e.target.value)}
                  placeholder="pipeline dir"
                  className="font-mono"
                />
              )}
            </div>
          </div>
          <div className="space-y-1">
            <Label className="text-xs">SQL</Label>
            <div className="overflow-hidden rounded-md border border-border">
              <CodeEditor
                value={createSql}
                onValueChange={setCreateSql}
                language="sql"
                height={120}
                lineNumbers={false}
                wordWrap
              />
            </div>
          </div>
          <div className="flex justify-end gap-2">
            <Button
              size="sm"
              variant="ghost"
              onClick={() => setCreating(false)}
            >
              Cancel
            </Button>
            <Button
              size="sm"
              onClick={() => {
                onCreate(name.trim(), createDir, createSql);
                setCreating(false);
              }}
            >
              Create
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}

function FieldMapping({
  widget,
  columns,
  onChange,
}: {
  widget: DashboardWidget;
  columns: DatasetColumns | undefined;
  onChange: (w: DashboardWidget) => void;
}) {
  if (widget.type === "table") {
    return (
      <p className="text-xs text-muted-foreground">
        Table widgets render every result column — no field mapping.
      </p>
    );
  }
  const t = widget.type;
  return (
    <div className="flex flex-wrap gap-3">
      {t === "big_number" && (
        <ColumnSelect
          label="Value field"
          value={widget.value_field}
          columns={columns}
          onChange={(v) => onChange({ ...widget, value_field: v })}
          expect="numeric"
        />
      )}
      {(t === "line" || t === "bar") && (
        <>
          <ColumnSelect
            label="X field"
            value={widget.x_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, x_field: v })}
          />
          <ColumnSelect
            label="Y field"
            value={widget.y_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, y_field: v })}
            expect="numeric"
          />
        </>
      )}
      {t === "stacked_bar" && (
        <>
          <ColumnSelect
            label="X field"
            value={widget.x_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, x_field: v })}
          />
          <ColumnMultiSelect
            label="Stacked value fields"
            value={widget.series_fields}
            columns={columns}
            exclude={widget.x_field}
            onChange={(v) => onChange({ ...widget, series_fields: v })}
            expect="numeric"
          />
        </>
      )}
      {t === "bar_line" && (
        <>
          <ColumnSelect
            label="X field"
            value={widget.x_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, x_field: v })}
          />
          <ColumnSelect
            label="Bar field"
            value={widget.y_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, y_field: v })}
            expect="numeric"
          />
          <ColumnSelect
            label="Line field"
            value={widget.line_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, line_field: v })}
            expect="numeric"
          />
        </>
      )}
      {(t === "pie" || t === "donut") && (
        <>
          <ColumnSelect
            label="Category field"
            value={widget.x_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, x_field: v })}
          />
          <ColumnSelect
            label="Value field"
            value={widget.value_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, value_field: v })}
            expect="numeric"
          />
        </>
      )}
      {t === "world_map" && (
        <>
          <ColumnSelect
            label="Region field"
            value={widget.region_field ?? ""}
            columns={columns}
            onChange={(v) => onChange({ ...widget, region_field: v })}
          />
          <ColumnSelect
            label="Value field"
            value={widget.value_field}
            columns={columns}
            onChange={(v) => onChange({ ...widget, value_field: v })}
            expect="numeric"
          />
          <ColumnSelect
            label="Tooltip field"
            value={widget.tooltip_field ?? ""}
            columns={columns}
            onChange={(v) => onChange({ ...widget, tooltip_field: v })}
          />
        </>
      )}
    </div>
  );
}

function LayoutEditor({
  widget,
  onChange,
}: {
  widget: DashboardWidget;
  onChange: (w: DashboardWidget) => void;
}) {
  const [open, setOpen] = useState(false);
  function patch(p: Partial<DashboardWidget["layout"]>) {
    onChange({ ...widget, layout: { ...widget.layout, ...p } });
  }
  return (
    <section className="space-y-2 border-t border-border pt-4">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="font-mono text-xs font-semibold uppercase tracking-wide text-muted-foreground hover:text-foreground"
      >
        Layout {open ? "▾" : "▸"}{" "}
        <span className="ml-1 lowercase tracking-normal text-muted-foreground/70">
          x={widget.layout.x} y={widget.layout.y} w={widget.layout.w} h={widget.layout.h}
        </span>
      </button>
      {open && (
        <div className="grid grid-cols-4 gap-2 text-xs">
          {(["x", "y", "w", "h"] as const).map((k) => (
            <div key={k} className="space-y-1">
              <Label className="text-xs">{k}</Label>
              <Input
                type="number"
                value={widget.layout[k]}
                onChange={(e) => patch({ [k]: Number(e.target.value) })}
                className="h-8 font-mono"
              />
            </div>
          ))}
        </div>
      )}
      <p className="text-xs text-muted-foreground">
        Drag the widget by its handle on the grid; corner to resize.
      </p>
    </section>
  );
}

/* ----------------------------------------------------------------------- */
/* Defaults used by DashboardEditor when materialising a fresh widget.     */
/* ----------------------------------------------------------------------- */

/** Per-type starting layout size on the 12-column grid. */
const DEFAULT_SIZE: Record<DashboardWidget["type"], { w: number; h: number }> = {
  big_number: { w: 3, h: 2 },
  line: { w: 6, h: 4 },
  bar: { w: 6, h: 4 },
  stacked_bar: { w: 6, h: 4 },
  bar_line: { w: 6, h: 4 },
  pie: { w: 4, h: 4 },
  donut: { w: 4, h: 4 },
  table: { w: 6, h: 5 },
  world_map: { w: 6, h: 5 },
};

const FALLBACK_SIZE = { w: 6, h: 4 };

/**
 * makeWidget — fresh widget seed, stacked below any existing one so the
 * first paint never overlaps. Field mappings come up empty; the drawer's
 * auto-preview populates them as soon as the user writes SQL.
 */
export function makeWidget(
  type: DashboardWidget["type"],
  existingWidgets: DashboardWidget[],
): DashboardWidget {
  const id = uniqueName("widget", existingWidgets.map((w) => w.id));
  const y = existingWidgets.reduce(
    (m, w) => Math.max(m, w.layout.y + w.layout.h),
    0,
  );
  const size = DEFAULT_SIZE[type] ?? FALLBACK_SIZE;
  return {
    id,
    type,
    title: "New widget",
    dataset: inlineDatasetName(id),
    value_field: "",
    x_field: "",
    y_field: "",
    series_fields: [],
    line_field: "",
    region_field: "",
    tooltip_field: "",
    layout: { x: 0, y, w: size.w, h: size.h },
  };
}
