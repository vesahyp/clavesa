/**
 * DataPreview — a modal that shows sampled data for the selected pipeline
 * node.
 *
 * Source / destination nodes: a single data grid.
 * Transform nodes: input and output grids side by side, with the SQL shown
 *   above.
 *
 * Rendered as a centered modal (Radix Dialog) so wide tables — CloudFront
 * logs have 30+ columns — get real horizontal room. Closes on Esc or an
 * outside click; it's a read-only inspection surface, so dismiss-on-click-
 * away is the right affordance (unlike the config drawer).
 */

import { useEffect, useState, useCallback, useRef } from "react";
import { Loader2 } from "lucide-react";

import {
  getSourcePreview,
  getTransformPreview,
  getDestinationPreview,
} from "../api/data";
import type { PreviewResult, TransformPreviewResult } from "../api/data";
import type { Column } from "../types/pipeline";
import { EngineBadge } from "@/components/EngineBadge";
import {
  Dialog,
  DialogContent,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const ROW_COUNT_OPTIONS = [10, 15, 25, 50];

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface DataPreviewProps {
  dir: string;
  nodeId: string;
  nodeType: string;
  sqlOverride?: string;
  onClose: () => void;
  /** Called with inferred schemas. Map of nodeId → columns for all nodes in the preview chain. */
  onSchema?: (schemas: Map<string, Column[]>) => void;
  /** Called when loading state changes */
  onLoadingChange?: (loading: boolean) => void;
}

// ---------------------------------------------------------------------------
// DataGrid — renders rows as a column-header table (sticky header,
// horizontal + vertical scroll). The fix for wide tables: the old preview
// transposed each record into one row per field, so 30+ columns became an
// unnavigable vertical scroll.
// ---------------------------------------------------------------------------

/** A preview row that is actually a record (defends against null / scalar
 * entries the runner may emit for unpaired aggregate rows). */
function isRecord(v: unknown): v is Record<string, unknown> {
  return v != null && typeof v === "object" && !Array.isArray(v);
}

/** Column order: union of every row's keys, first-seen order. */
function columnsOf(rows: Record<string, unknown>[]): string[] {
  const seen = new Set<string>();
  const cols: string[] = [];
  for (const row of rows) {
    if (!isRecord(row)) continue;
    for (const k of Object.keys(row)) {
      if (!seen.has(k)) {
        seen.add(k);
        cols.push(k);
      }
    }
  }
  return cols;
}

function formatCell(v: unknown): { text: string; isNull: boolean } {
  if (v == null) return { text: "null", isNull: true };
  if (typeof v === "object") return { text: JSON.stringify(v), isNull: false };
  return { text: String(v), isNull: false };
}

function DataGrid({ rows: rawRows }: { rows: Record<string, unknown>[] }) {
  const rows = rawRows.filter(isRecord);
  if (rows.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-xs text-muted-foreground">
        (no rows)
      </div>
    );
  }
  const columns = columnsOf(rows);
  return (
    <div className="h-full overflow-auto">
      <table className="border-collapse font-mono text-xs">
        <thead className="sticky top-0 z-10 bg-muted">
          <tr>
            <th className="sticky left-0 z-20 border-b border-r border-border bg-muted px-2 py-1 text-right text-[10px] font-semibold text-muted-foreground">
              #
            </th>
            {columns.map((c) => (
              <th
                key={c}
                className="whitespace-nowrap border-b border-border px-2 py-1 text-left font-semibold text-muted-foreground"
              >
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="hover:bg-muted/40">
              <td className="sticky left-0 z-10 border-b border-r border-border bg-background px-2 py-1 text-right text-[10px] text-muted-foreground">
                {i + 1}
              </td>
              {columns.map((c) => {
                const { text, isNull } = formatCell(row[c]);
                return (
                  <td
                    key={c}
                    className="max-w-[28rem] truncate border-b border-border px-2 py-1 text-foreground"
                    title={text}
                  >
                    {isNull ? (
                      <span className="text-muted-foreground/50">null</span>
                    ) : (
                      text
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// DataPreview component
// ---------------------------------------------------------------------------

export function DataPreview({
  dir,
  nodeId,
  nodeType,
  sqlOverride,
  onClose,
  onSchema,
  onLoadingChange,
}: DataPreviewProps) {
  const isTransform = nodeType === "transform" || nodeType === "sql_transform";
  const isDestination = nodeType === "destination";

  const onLoadingRef = useRef(onLoadingChange);
  onLoadingRef.current = onLoadingChange;

  const [loading, setLoadingState] = useState(true);
  const setLoading = useCallback((v: boolean) => {
    setLoadingState(v);
    onLoadingRef.current?.(v);
  }, []);
  const [error, setError] = useState<string | null>(null);
  const [sourceResult, setSourceResult] = useState<PreviewResult | null>(null);
  const [transformResult, setTransformResult] =
    useState<TransformPreviewResult | null>(null);
  const [rows, setRows] = useState(15);

  const onSchemaRef = useRef(onSchema);
  onSchemaRef.current = onSchema;

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      if (isTransform) {
        const r = await getTransformPreview(dir, nodeId, rows, sqlOverride);
        setTransformResult(r);
        if (r.pairs.length > 0) {
          const schemas = new Map<string, Column[]>();
          if (r.pairs[0].output.length > 0) {
            const outputKeys = Object.keys(r.pairs[0].output[0]);
            schemas.set(
              nodeId,
              outputKeys.map((k) => ({ name: k, type: "string", nullable: true }))
            );
          }
          if (r.pairs[0].input) {
            const inputKeys = Object.keys(r.pairs[0].input);
            schemas.set(
              "__input__",
              inputKeys.map((k) => ({ name: k, type: "string", nullable: true }))
            );
          }
          onSchemaRef.current?.(schemas);
        }
      } else if (isDestination) {
        const r = await getDestinationPreview(dir, nodeId, rows);
        setSourceResult(r);
      } else {
        const r = await getSourcePreview(dir, nodeId);
        setSourceResult(r);
        if (r.schema && r.schema.length > 0) {
          const schemas = new Map<string, Column[]>();
          schemas.set(nodeId, r.schema);
          onSchemaRef.current?.(schemas);
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [dir, nodeId, isTransform, isDestination, rows, sqlOverride]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Flatten the input/output sides of every pair into two grids — the
  // whole sample at once, scrollable, instead of one record behind a
  // prev/next stepper. Filter to records: the runner can emit a null
  // input (an aggregate output row maps to no single input row).
  const inputRows = (transformResult?.pairs ?? [])
    .map((p) => p.input)
    .filter(isRecord);
  const outputRows = (transformResult?.pairs ?? [])
    .flatMap((p) => p.output ?? [])
    .filter(isRecord);

  return (
    <Dialog open onOpenChange={(o) => { if (!o) onClose(); }}>
      <DialogContent
        className="flex h-[85vh] max-w-[92vw] flex-col gap-0 p-0"
        data-testid="preview-panel"
        aria-describedby={undefined}
      >
        {/* Header */}
        <div className="flex flex-shrink-0 items-center gap-3 border-b border-border px-4 py-2.5 pr-12">
          <DialogTitle className="text-sm font-semibold text-muted-foreground">
            Preview: <span className="font-mono text-foreground">{nodeId}</span>
          </DialogTitle>
          {!loading && !error && (
            <EngineBadge
              served={
                isTransform ? transformResult?.served : sourceResult?.served
              }
              // Transform preview runs Spark (authoring) → predictable. A
              // source preview is a raw S3 read with no engine (slice 3), so
              // no surface → no prediction, and it stays badge-less.
              surface={isTransform ? "authoring" : undefined}
              testid="engine-badge-preview"
            />
          )}
          {isTransform && (
            <div className="ml-auto">
              <Select value={String(rows)} onValueChange={(v) => setRows(Number(v))}>
                <SelectTrigger aria-label="Row count" className="h-7 w-28 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {ROW_COUNT_OPTIONS.map((n) => (
                    <SelectItem key={n} value={String(n)}>
                      {n} rows
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}
        </div>

        {loading && (
          <div className="flex flex-1 items-center justify-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading preview…
          </div>
        )}

        {error && !loading && (
          <div
            role="alert"
            className="m-4 overflow-auto rounded-md border border-status-failed/40 bg-status-failed/10 px-3 py-2 font-mono text-xs text-status-failed"
          >
            {error}
          </div>
        )}

        {/* Source / destination — single grid */}
        {!loading && !error && !isTransform && sourceResult && (
          <div className="flex flex-1 flex-col overflow-hidden">
            <div className="flex-shrink-0 border-b border-border px-4 py-1 text-[11px] text-muted-foreground">
              {sourceResult.items.length} item
              {sourceResult.items.length !== 1 ? "s" : ""} loaded
            </div>
            <div className="flex-1 overflow-hidden" data-testid="preview-item">
              <DataGrid rows={sourceResult.items} />
            </div>
          </div>
        )}

        {/* Transform — SQL above, input | output grids side by side */}
        {!loading && !error && isTransform && transformResult && (
          <div className="flex flex-1 flex-col overflow-hidden">
            {transformResult.sql && (
              <pre
                className="m-0 max-h-20 flex-shrink-0 overflow-auto border-b border-border px-4 py-1.5 font-mono text-[11px] text-foreground"
                data-testid="preview-sql"
              >
                {transformResult.sql.trim()}
              </pre>
            )}
            <div className="flex flex-1 overflow-hidden">
              <div
                className="flex flex-1 flex-col overflow-hidden border-r border-border"
                data-testid="preview-pane-input"
              >
                <div className="flex-shrink-0 border-b border-border px-4 py-1 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                  Input · {inputRows.length} row{inputRows.length !== 1 ? "s" : ""}
                </div>
                <div className="flex-1 overflow-hidden" data-testid="preview-item">
                  <DataGrid rows={inputRows} />
                </div>
              </div>
              <div
                className="flex flex-1 flex-col overflow-hidden"
                data-testid="preview-pane-output"
              >
                <div className="flex-shrink-0 border-b border-border px-4 py-1 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                  Output · {outputRows.length} row{outputRows.length !== 1 ? "s" : ""}
                </div>
                <div className="flex-1 overflow-hidden">
                  <DataGrid rows={outputRows} />
                </div>
              </div>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

export default DataPreview;
