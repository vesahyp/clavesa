/**
 * TransformInputsSection — surfaces a transform's attached inputs and
 * offers an "Add input" affordance with three modes:
 *
 *   - Source (ADR-017): pick from the workspace source registry.
 *     POSTs to /api/sources/{name}/attach.
 *   - Workspace table (ADR-016 slice 2): pick from any Delta table
 *     produced by *another* pipeline in this workspace. POSTs to
 *     /api/pipeline/external-table/attach.
 *   - Pipeline node: wire an upstream transform in *this* pipeline as
 *     an input. POSTs to /api/pipeline/edges — the keyboard-form
 *     equivalent of dragging an edge in the DAG editor, mirroring the
 *     CLI's `node connect --from <node> --to <this> --input <alias>`.
 *
 * Renders above the TransformEditor in ConfigPanel. Reads existing
 * attachments from the parsed graph's synthetic `source_inputs` and
 * `external_inputs` config keys (populated by hclparser when
 * `inputs = { x = "sources.y" }` or `{ x = "schema.table" }`).
 *
 * Out of scope for this slice:
 *   - External (non-clavesa-tagged) Glue tables in the workspace
 *     catalog. No data source exists yet — the catalog handler only
 *     surfaces clavesa-managed DBs.
 *   - Removing / renaming attachments (the user can delete the node
 *     or hand-edit `.tf` for now).
 */

import { useMemo, useState } from "react";
import { Loader2, Plus, Database, Workflow, X } from "lucide-react";

import {
  attachExternalTable,
  attachSource,
  useCatalogTables,
  useSources,
  type CatalogTable,
  type SourceSpec,
} from "@/lib/queries";
import { displayTableName } from "@/lib/format";
import { addEdge, detachInput } from "@/api/pipeline";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

// ---------------------------------------------------------------------------
// Source-input value
// ---------------------------------------------------------------------------

/** The typed resolved descriptor a kind=s3 source attachment carries. */
interface SourceInputDescriptor {
  spec_name?: string;
  bucket?: string;
  prefix?: string;
  format?: string;
}

/** A `source_inputs` value: the `"sources.<name>"` sentinel or the descriptor. */
export type SourceInputValue = string | SourceInputDescriptor;

/**
 * Display label for a source-input value. kind=http attachments are the
 * `"sources.<name>"` string; kind=s3 attachments are an object — rendering
 * that object directly as a React child throws (React error #31), which
 * blanked the whole ConfigPanel.
 */
function sourceLabel(v: SourceInputValue): string {
  if (typeof v === "string") return v;
  if (v && typeof v === "object" && v.spec_name) return `sources.${v.spec_name}`;
  return JSON.stringify(v);
}

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface TransformInputsSectionProps {
  dir: string;
  nodeId: string;
  /**
   * Map of alias → source attachment from `config.source_inputs`. The value
   * is either the `"sources.<name>"` string sentinel (kind=http) or the
   * typed resolved descriptor `{spec_name, bucket, prefix, format}`
   * (kind=s3, ADR-017 v0.22.0) — `sourceLabel` normalises both.
   */
  sourceInputs?: Record<string, SourceInputValue>;
  /** Map of alias → "<schema>.<table>" string from `config.external_inputs`. */
  externalInputs?: Record<string, string>;
  /** Transform node IDs in this pipeline eligible as upstream inputs. */
  upstreamNodeIds?: string[];
  /**
   * Map of alias → upstream transform node ID for every transform→transform
   * edge into this node. Distinct from sourceInputs/externalInputs — these
   * are intra-pipeline DAG edges (`pipeline.edges`), not config refs — but
   * they belong in the same Inputs list so the panel shows every input.
   */
  nodeInputs?: Record<string, string>;
  onAttached: () => void;
}

type Mode = "source" | "table" | "node";

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function TransformInputsSection({
  dir,
  nodeId,
  sourceInputs,
  externalInputs,
  upstreamNodeIds,
  nodeInputs,
  onAttached,
}: TransformInputsSectionProps) {
  const sources = useSources();
  const catalog = useCatalogTables();
  const [showForm, setShowForm] = useState(false);
  const [mode, setMode] = useState<Mode>("source");
  const [pickedSource, setPickedSource] = useState<string>("");
  const [pickedTable, setPickedTable] = useState<string>("");
  const [pickedNode, setPickedNode] = useState<string>("");
  const [alias, setAlias] = useState<string>("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detaching, setDetaching] = useState<string | null>(null);

  async function handleDetach(aliasToDetach: string) {
    setDetaching(aliasToDetach);
    setError(null);
    try {
      await detachInput(dir, nodeId, aliasToDetach);
      onAttached();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setDetaching(null);
    }
  }

  const nodeList: string[] = upstreamNodeIds ?? [];

  const sourceEntries = Object.entries(sourceInputs ?? {});
  const externalEntries = Object.entries(externalInputs ?? {});
  const nodeEntries = Object.entries(nodeInputs ?? {});
  const sourceList: SourceSpec[] = sources.data?.sources ?? [];

  // Workspace tables produced by OTHER pipelines (ADR-016 slice 2).
  // The current pipeline's own outputs go through the drag-edge path,
  // so filter them out — keeping them in the picker would double up.
  // System catalog tables (runs/node_runs/tables) are filtered too:
  // they're observability rollups, not source data.
  const tableList: CatalogTable[] = useMemo(() => {
    const all = catalog.data?.tables ?? [];
    const currentPipeline = dir.replace(/^.*\//, "");
    return all.filter((t) => {
      if (!t.schema) return false;
      if (t.catalog.endsWith("_system")) return false;
      if (t.owning_pipeline === currentPipeline) return false;
      return true;
    });
  }, [catalog.data, dir]);

  function openForm(initial: Mode = "source") {
    setShowForm(true);
    setError(null);
    setMode(initial);
    const firstSource = sourceList[0]?.name ?? "";
    setPickedSource(firstSource);
    const firstTable = tableList[0];
    const firstRef = firstTable ? `${firstTable.schema}.${firstTable.name}` : "";
    setPickedTable(firstRef);
    const firstNode = nodeList[0] ?? "";
    setPickedNode(firstNode);
    if (initial === "table") setAlias(defaultAliasForTable(firstRef));
    else if (initial === "node") setAlias(firstNode);
    else setAlias(firstSource);
  }

// defaultAliasForTable maps a `<schema>.<table>` reference back to the
// natural SQL alias — the table-name portion with the `__<output_key>`
// suffix stripped. Clavesa transform outputs follow the
// `<node>__<key>` convention, so `marketing.dim_customers__default`
// becomes `dim_customers` and `marketing.dim_customers` stays
// `dim_customers`. Users can override the alias before submitting.
function defaultAliasForTable(ref: string): string {
  const dot = ref.indexOf(".");
  const tableSegment = dot >= 0 ? ref.slice(dot + 1) : ref;
  const sep = tableSegment.lastIndexOf("__");
  return sep > 0 ? tableSegment.slice(0, sep) : tableSegment;
}

  function onModeChange(next: Mode) {
    setMode(next);
    if (next === "source") {
      setAlias(pickedSource);
    } else if (next === "node") {
      setAlias(pickedNode);
    } else {
      setAlias(defaultAliasForTable(pickedTable));
    }
  }

  async function handleAttach(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      if (mode === "source") {
        if (!pickedSource || !alias) return;
        await attachSource(pickedSource, { dir, to: nodeId, alias });
      } else if (mode === "node") {
        if (!pickedNode || !alias) return;
        await addEdge(dir, pickedNode, nodeId, alias);
      } else {
        if (!pickedTable || !alias) return;
        await attachExternalTable({ dir, ref: pickedTable, to: nodeId, alias });
      }
      setShowForm(false);
      setPickedSource("");
      setPickedTable("");
      setPickedNode("");
      setAlias("");
      onAttached();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  const canOpen =
    sourceList.length > 0 || tableList.length > 0 || nodeList.length > 0;

  return (
    <section
      data-testid="transform-inputs"
      className="mb-3 rounded-md border border-border bg-muted/30 px-3 py-2.5"
    >
      <div className="mb-1.5 flex items-center justify-between">
        <div className="text-[11px] font-bold uppercase tracking-wider text-muted-foreground">
          Inputs
        </div>
        {!showForm && (
          <Button
            onClick={() => openForm()}
            disabled={!canOpen}
            variant="ghost"
            size="sm"
            className="h-6 px-2 text-xs"
            data-testid="add-input"
          >
            <Plus className="h-3 w-3" />
            Add
          </Button>
        )}
      </div>

      {sourceEntries.length === 0 &&
        externalEntries.length === 0 &&
        nodeEntries.length === 0 &&
        !showForm && (
          <p className="text-xs text-muted-foreground">
            No inputs yet.{" "}
            {!canOpen ? (
              <>
                Register a source on the{" "}
                <a href="/sources" className="underline hover:text-foreground">
                  Sources
                </a>{" "}
                page, or have another pipeline produce a table first.
              </>
            ) : (
              <>Click <strong>Add</strong> to wire one in.</>
            )}
          </p>
        )}

      {(sourceEntries.length > 0 ||
        externalEntries.length > 0 ||
        nodeEntries.length > 0) && (
        <ul className="space-y-1">
          {sourceEntries.map(([k, v]) => (
            <li
              key={`src-${k}`}
              className="group flex items-center gap-2 font-mono text-xs"
              data-testid={`input-row-${k}`}
            >
              <Database className="h-3 w-3 flex-shrink-0 text-muted-foreground" />
              <span className="font-medium">{k}</span>
              <span className="text-muted-foreground">→</span>
              <code className="flex-1 truncate">{sourceLabel(v)}</code>
              <DetachButton
                alias={k}
                busy={detaching === k}
                onClick={() => handleDetach(k)}
              />
            </li>
          ))}
          {externalEntries.map(([k, v]) => (
            <li
              key={`ext-${k}`}
              className="group flex items-center gap-2 font-mono text-xs"
              data-testid={`input-row-${k}`}
            >
              <Database className="h-3 w-3 flex-shrink-0 text-indigo-500" />
              <span className="font-medium">{k}</span>
              <span className="text-muted-foreground">→</span>
              <code className="flex-1 truncate" title={v}>
                {v.replace(/__default$/, "")}
              </code>
              <span className="rounded bg-indigo-500/10 px-1 py-0.5 font-mono text-[10px] text-indigo-700 dark:text-indigo-300">
                cross-pipeline
              </span>
              <DetachButton
                alias={k}
                busy={detaching === k}
                onClick={() => handleDetach(k)}
              />
            </li>
          ))}
          {nodeEntries.map(([k, v]) => (
            <li
              key={`node-${k}`}
              className="group flex items-center gap-2 font-mono text-xs"
              data-testid={`input-row-${k}`}
            >
              <Workflow className="h-3 w-3 flex-shrink-0 text-muted-foreground" />
              <span className="font-medium">{k}</span>
              <span className="text-muted-foreground">→</span>
              <code className="flex-1 truncate">{v}</code>
              <span className="rounded bg-muted px-1 py-0.5 font-mono text-[10px] text-muted-foreground">
                node
              </span>
              <DetachButton
                alias={k}
                busy={detaching === k}
                onClick={() => handleDetach(k)}
              />
            </li>
          ))}
        </ul>
      )}

      {showForm && (
        <form
          onSubmit={handleAttach}
          className="mt-2 flex flex-col gap-2"
          data-testid="add-input-form"
        >
          <div className="flex gap-1" role="tablist">
            <button
              type="button"
              role="tab"
              onClick={() => onModeChange("source")}
              disabled={sourceList.length === 0}
              data-testid="mode-source"
              className={
                "h-7 flex-1 rounded border text-[11px] font-medium " +
                (mode === "source"
                  ? "border-foreground bg-foreground text-background"
                  : "border-border bg-background text-muted-foreground hover:border-foreground/40 disabled:opacity-50")
              }
            >
              Source
            </button>
            <button
              type="button"
              role="tab"
              onClick={() => onModeChange("table")}
              disabled={tableList.length === 0}
              data-testid="mode-table"
              className={
                "h-7 flex-1 rounded border text-[11px] font-medium " +
                (mode === "table"
                  ? "border-foreground bg-foreground text-background"
                  : "border-border bg-background text-muted-foreground hover:border-foreground/40 disabled:opacity-50")
              }
            >
              Workspace table
            </button>
            <button
              type="button"
              role="tab"
              onClick={() => onModeChange("node")}
              disabled={nodeList.length === 0}
              data-testid="mode-node"
              className={
                "h-7 flex-1 rounded border text-[11px] font-medium " +
                (mode === "node"
                  ? "border-foreground bg-foreground text-background"
                  : "border-border bg-background text-muted-foreground hover:border-foreground/40 disabled:opacity-50")
              }
            >
              Pipeline node
            </button>
          </div>

          {mode === "source" && (
            <>
              <label className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                Source
              </label>
              <select
                value={pickedSource}
                onChange={(e) => {
                  setPickedSource(e.target.value);
                  setAlias(e.target.value);
                }}
                disabled={busy}
                className="h-8 rounded-md border border-border bg-background px-2 text-xs"
                data-testid="add-input-source"
              >
                {sourceList.map((s) => (
                  <option key={s.name} value={s.name}>
                    {s.name} ({s.kind})
                  </option>
                ))}
              </select>
            </>
          )}

          {mode === "table" && (
            <>
              <label className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                Table (cross-pipeline)
              </label>
              <select
                value={pickedTable}
                onChange={(e) => {
                  setPickedTable(e.target.value);
                  setAlias(defaultAliasForTable(e.target.value));
                }}
                disabled={busy}
                className="h-8 rounded-md border border-border bg-background px-2 text-xs"
                data-testid="add-input-table"
              >
                {tableList.map((t) => {
                  const ref = `${t.schema}.${t.name}`;
                  const display = `${t.schema}.${displayTableName(t)}`;
                  return (
                    <option key={ref} value={ref}>
                      {display}
                      {t.owning_pipeline && t.owning_pipeline !== t.schema
                        ? ` — ${t.owning_pipeline}`
                        : ""}
                    </option>
                  );
                })}
              </select>
            </>
          )}

          {mode === "node" && (
            <>
              <label className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                Upstream transform (this pipeline)
              </label>
              <select
                value={pickedNode}
                onChange={(e) => {
                  setPickedNode(e.target.value);
                  setAlias(e.target.value);
                }}
                disabled={busy}
                className="h-8 rounded-md border border-border bg-background px-2 text-xs"
                data-testid="add-input-node"
              >
                {nodeList.map((id) => (
                  <option key={id} value={id}>
                    {id}
                  </option>
                ))}
              </select>
            </>
          )}

          <label className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            Alias (referenced from SQL as <code>{alias || "<alias>"}</code>)
          </label>
          <Input
            value={alias}
            onChange={(e) => setAlias(e.target.value)}
            disabled={busy}
            placeholder="alias"
            className="h-8 text-xs"
            data-testid="add-input-alias"
          />

          {error && (
            <div
              role="alert"
              className="rounded-md border border-status-failed/40 bg-status-failed/10 p-2 text-xs text-status-failed"
            >
              {error}
            </div>
          )}

          <div className="flex gap-2">
            <Button
              type="submit"
              disabled={
                busy ||
                !alias ||
                (mode === "source"
                  ? !pickedSource
                  : mode === "node"
                    ? !pickedNode
                    : !pickedTable)
              }
              size="sm"
              className="h-7 flex-1"
              data-testid="add-input-submit"
            >
              {busy && <Loader2 className="h-3 w-3 animate-spin" />}
              {busy ? "Attaching…" : "Attach"}
            </Button>
            <Button
              type="button"
              onClick={() => {
                setShowForm(false);
                setError(null);
              }}
              disabled={busy}
              variant="ghost"
              size="sm"
              className="h-7"
            >
              Cancel
            </Button>
          </div>
        </form>
      )}
    </section>
  );
}

function DetachButton({
  alias,
  busy,
  onClick,
}: {
  alias: string;
  busy: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={busy}
      title={`Remove input "${alias}"`}
      aria-label={`Remove input ${alias}`}
      data-testid={`detach-input-${alias}`}
      className="ml-1 inline-flex h-5 w-5 flex-shrink-0 items-center justify-center rounded text-muted-foreground opacity-0 transition-opacity hover:bg-muted hover:text-foreground focus:opacity-100 group-hover:opacity-100 disabled:opacity-50"
    >
      {busy ? <Loader2 className="h-3 w-3 animate-spin" /> : <X className="h-3 w-3" />}
    </button>
  );
}

export default TransformInputsSection;
