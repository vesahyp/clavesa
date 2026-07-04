/**
 * ConfigPanel — right panel showing configuration fields for the selected node.
 *
 * Renders form fields appropriate for the node's module type:
 *   - source       : bucket, prefix, format (select)
 *   - transform    : sql (TransformEditor / Monaco)
 *   - destination  : bucket, prefix, format, write_mode + Live Data sub-tab
 *
 * Saving calls PIPELINE-API updateNode and notifies the parent via
 * onGraphUpdate(). Errors are shown inline. A spinner is shown while saving.
 *
 * Delete Node calls deleteNode() with confirmation and notifies via
 * onNodeDeleted().
 *
 * After source preview loads, an "Inferred Schema" section shows the columns
 *
 */

import { useEffect, useMemo, useState, useCallback } from "react";
import { Loader2, Maximize2, Minimize2, X } from "lucide-react";

import { BASE_URL, request } from "../api/client";
import { updateNode, deleteNode, renameNode } from "../api/pipeline";
import { getVars } from "../api/workspace";
import type { Column } from "../types/pipeline";
import { useCatalogTables, useTableSample } from "@/lib/queries";
import { TransformEditor } from "./TransformEditor";
import {
  TransformInputsSection,
  type SourceInputValue,
} from "./TransformInputsSection";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { NativeSelect } from "@/components/ui/native-select";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

// Shared section-header / inline-error class recipes used by the Settings
// sections below.
const sectionLabelCls =
  "mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground";
const errorNoteCls =
  "rounded-md border border-status-failed/40 bg-status-failed/10 p-2 text-xs text-status-failed";

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface ConfigPanelProps {
  dir: string;
  selectedNode: {
    id: string;
    type: string;
    data: Record<string, unknown>;
  } | null;
  onGraphUpdate: () => void;
  onPreview?: (nodeId: string, sql?: string) => void;
  onNodeDeleted?: () => void;
  /** Called after a successful node rename, with the new id, so the
   *  editor can keep the renamed node selected. */
  onNodeRenamed?: (newId: string) => void;
  /** Closes the config drawer (the editor renders this as an overlay). */
  onClose?: () => void;
  /** Schema inferred from the last source preview, if any */
  /**
   * Per-node inferred schemas (keyed by node id), from previews. Used to
   * give the transform's SQL editor column-aware autocomplete for its
   * upstream inputs.
   */
  nodeSchemas?: Map<string, Column[]>;
  /**
   * The node's own Delta output table, if one exists in the catalog.
   * When set, the panel auto-samples a few rows so the user sees current
   * output without clicking Preview. Absent on never-run nodes.
   */
  output?: { catalog: string; schema: string; table: string };
  /**
   * Aliases of incoming transform→transform edges into this node, used
   * to render the "Incremental upstream reads" panel. The parent
   * computes from `pipeline.edges` + node types and passes down so the
   * panel doesn't need its own copy of the graph.
   */
  incomingTransformAliases?: string[];
  /**
   * IDs of transform nodes in this pipeline eligible as upstream inputs
   * (every transform except the selected one). Feeds the keyboard
   * "Pipeline node" input form in TransformInputsSection — the typed
   * mirror of dragging an edge.
   */
  upstreamNodeIds?: string[];
  /**
   * Map of alias → upstream node ID for every transform→transform edge
   * into the selected node. Lets TransformInputsSection list intra-
   * pipeline node inputs alongside source / external-table inputs.
   */
  nodeInputs?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Field — small wrapper for a labeled form input
// ---------------------------------------------------------------------------

function Field({
  label,
  htmlFor,
  children,
}: {
  label: string;
  htmlFor: string;
  children: React.ReactNode;
}) {
  return (
    <div className="mb-3 space-y-1.5">
      <Label
        htmlFor={htmlFor}
        className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground"
      >
        {label}
      </Label>
      {children}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Per-type form renderers
// ---------------------------------------------------------------------------

function S3SourceForm({
  config,
  onChange,
}: {
  config: Record<string, unknown>;
  onChange: (c: Record<string, unknown>) => void;
}) {
  const bucket = String(config.bucket ?? "");
  const prefix = String(config.prefix ?? "");
  const format = String(config.format ?? "csv");
  const jsonPath = String(config.json_path ?? "");

  return (
    <>
      <Field label="Bucket" htmlFor="cfg-bucket">
        <Input
          id="cfg-bucket"
          value={bucket}
          onChange={(e) => onChange({ ...config, bucket: e.target.value })}
          className="font-mono text-xs"
        />
      </Field>
      <Field label="Prefix" htmlFor="cfg-prefix">
        <Input
          id="cfg-prefix"
          value={prefix}
          onChange={(e) => onChange({ ...config, prefix: e.target.value })}
          className="font-mono text-xs"
        />
      </Field>
      <Field label="Format" htmlFor="cfg-format">
        <NativeSelect
          id="cfg-format"
          value={format}
          onChange={(e) => onChange({ ...config, format: e.target.value })}
        >
          <option value="csv">csv</option>
          <option value="json">json</option>
          <option value="parquet">parquet</option>
        </NativeSelect>
      </Field>
      {format === "json" && (
        <Field label="JSON Path (optional)" htmlFor="cfg-json-path">
          <Input
            id="cfg-json-path"
            value={jsonPath}
            placeholder="e.g. cars or data.items"
            onChange={(e) => onChange({ ...config, json_path: e.target.value })}
            className="font-mono text-xs"
          />
        </Field>
      )}
    </>
  );
}

function S3DestinationForm({
  config,
  onChange,
}: {
  config: Record<string, unknown>;
  onChange: (c: Record<string, unknown>) => void;
}) {
  const bucket = String(config.bucket ?? "");
  const prefix = String(config.prefix ?? "");
  const format = String(config.format ?? "parquet");
  const writeMode = String(config.write_mode ?? "append");

  return (
    <>
      <Field label="Bucket" htmlFor="cfg-dest-bucket">
        <Input
          id="cfg-dest-bucket"
          value={bucket}
          onChange={(e) => onChange({ ...config, bucket: e.target.value })}
          className="font-mono text-xs"
        />
      </Field>
      <Field label="Prefix" htmlFor="cfg-dest-prefix">
        <Input
          id="cfg-dest-prefix"
          value={prefix}
          onChange={(e) => onChange({ ...config, prefix: e.target.value })}
          className="font-mono text-xs"
        />
      </Field>
      <Field label="Format" htmlFor="cfg-dest-format">
        <NativeSelect
          id="cfg-dest-format"
          value={format}
          onChange={(e) => onChange({ ...config, format: e.target.value })}
        >
          <option value="csv">csv</option>
          <option value="json">json</option>
          <option value="parquet">parquet</option>
        </NativeSelect>
      </Field>
      <Field label="Write Mode" htmlFor="cfg-dest-write-mode">
        <NativeSelect
          id="cfg-dest-write-mode"
          value={writeMode}
          onChange={(e) => onChange({ ...config, write_mode: e.target.value })}
        >
          <option value="append">append</option>
          <option value="overwrite">overwrite</option>
        </NativeSelect>
      </Field>
    </>
  );
}

// ---------------------------------------------------------------------------
// TransformOutputSection — write-mode + merge-keys for transform default output.
//
// Reads/writes output_definitions["default"]; multi-output transforms (rare)
// still get the legacy hand-author flow. Saving calls updateNode immediately
// so the field doesn't depend on the SQL editor's Save click.
// ---------------------------------------------------------------------------

interface OutputDefault {
  mode?: string;
  merge_keys?: string[];
  cluster_by?: string[];
  merge_update?: Record<string, string>;
}

function readDefaultOutput(config: Record<string, unknown>): OutputDefault {
  const defs = config.output_definitions as Record<string, unknown> | undefined;
  const def = defs?.default as Record<string, unknown> | undefined;
  if (!def) return {};
  const mode = typeof def.mode === "string" ? def.mode : undefined;
  const mk = Array.isArray(def.merge_keys)
    ? (def.merge_keys.filter((k) => typeof k === "string") as string[])
    : undefined;
  const cb = Array.isArray(def.cluster_by)
    ? (def.cluster_by.filter((k) => typeof k === "string") as string[])
    : undefined;
  let mu: Record<string, string> | undefined;
  if (
    def.merge_update &&
    typeof def.merge_update === "object" &&
    !Array.isArray(def.merge_update)
  ) {
    const out: Record<string, string> = {};
    for (const [col, spec] of Object.entries(
      def.merge_update as Record<string, unknown>
    )) {
      if (typeof spec === "string") out[col] = spec;
    }
    if (Object.keys(out).length > 0) mu = out;
  }
  return { mode, merge_keys: mk, cluster_by: cb, merge_update: mu };
}

// readTransformStats reports whether every declared output key has
// stats=true. The editor exposes one transform-wide checkbox today; the
// underlying data model is per-key (matches the runner). A partial state
// (some keys on, some off) reads as "off" so toggling and saving
// resolves the discrepancy to a clean uniform value.
function readTransformStats(config: Record<string, unknown>): boolean {
  const defs = config.output_definitions as Record<string, unknown> | undefined;
  if (!defs) return false;
  const keys = Object.keys(defs);
  if (keys.length === 0) return false;
  return keys.every((k) => {
    const def = defs[k] as Record<string, unknown> | undefined;
    return Boolean(def && def.stats === true);
  });
}

function TransformOutputSection({
  dir,
  nodeId,
  config,
  onSaved,
}: {
  dir: string;
  nodeId: string;
  config: Record<string, unknown>;
  onSaved: () => void;
}) {
  const parsed = readDefaultOutput(config);
  const initialStats = readTransformStats(config);
  const [mode, setMode] = useState<string>(parsed.mode ?? "");
  const [keysText, setKeysText] = useState<string>(
    (parsed.merge_keys ?? []).join(", ")
  );
  const [clusterByText, setClusterByText] = useState<string>(
    (parsed.cluster_by ?? []).join(", ")
  );
  const [mergeUpdateText, setMergeUpdateText] = useState<string>(
    Object.entries(parsed.merge_update ?? {})
      .map(([col, spec]) => `${col} = ${spec}`)
      .join("\n")
  );
  const [stats, setStats] = useState<boolean>(initialStats);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [savedAt, setSavedAt] = useState<number | null>(null);

  // Sync from upstream when selection changes; key={nodeId} on the parent
  // would also work but this keeps the panel's identity stable.
  const [prevNode, setPrevNode] = useState(nodeId);
  if (prevNode !== nodeId) {
    setPrevNode(nodeId);
    setMode(parsed.mode ?? "");
    setKeysText((parsed.merge_keys ?? []).join(", "));
    setClusterByText((parsed.cluster_by ?? []).join(", "));
    setMergeUpdateText(
      Object.entries(parsed.merge_update ?? {})
        .map(([col, spec]) => `${col} = ${spec}`)
        .join("\n")
    );
    setStats(initialStats);
    setError(null);
    setSavedAt(null);
  }

  async function handleSave() {
    setSaving(true);
    setError(null);
    try {
      // Round-trip the parser-emitted output_definitions so other output keys
      // (multi-output transforms) survive untouched. Strip parser-synthetic
      // input shims for the same reason as TransformEditor's Save.
      const defs = {
        ...((config.output_definitions as Record<string, unknown>) ?? {}),
      };
      if (!defs.default) defs.default = {};
      const existingDefault =
        (defs.default as Record<string, unknown> | undefined) ?? {};
      const nextDefault: Record<string, unknown> = { ...existingDefault };
      if (mode === "") {
        delete nextDefault.mode;
      } else {
        nextDefault.mode = mode;
      }
      const keys = keysText
        .split(",")
        .map((k) => k.trim())
        .filter((k) => k.length > 0);
      if (keys.length === 0) {
        delete nextDefault.merge_keys;
      } else {
        nextDefault.merge_keys = keys;
      }
      const clusterBy = clusterByText
        .split(/[\s,]+/)
        .map((k) => k.trim())
        .filter((k) => k.length > 0);
      if (clusterBy.length === 0) {
        delete nextDefault.cluster_by;
      } else {
        nextDefault.cluster_by = clusterBy;
      }
      const mergeUpdate: Record<string, string> = {};
      for (const line of mergeUpdateText.split("\n")) {
        const eq = line.indexOf("=");
        if (eq === -1) continue;
        const col = line.slice(0, eq).trim();
        const spec = line.slice(eq + 1).trim();
        if (col.length === 0 || spec.length === 0) continue;
        mergeUpdate[col] = spec;
      }
      if (Object.keys(mergeUpdate).length === 0) {
        delete nextDefault.merge_update;
      } else {
        nextDefault.merge_update = mergeUpdate;
      }
      defs.default = nextDefault;
      // stats lives on the data model per-key, but the editor exposes one
      // transform-wide checkbox: write the same boolean into every key so
      // the UI's all-or-nothing read stays consistent with the underlying
      // shape (a future multi-output edit surface can split).
      for (const k of Object.keys(defs)) {
        const def = (defs[k] as Record<string, unknown> | undefined) ?? {};
        const nextDef: Record<string, unknown> = { ...def };
        if (stats) {
          nextDef.stats = true;
        } else {
          delete nextDef.stats;
        }
        defs[k] = nextDef;
      }
      const {
        source_inputs: _src,
        external_inputs: _ext,
        ...restConfig
      } = config;
      void _src;
      void _ext;
      await updateNode(dir, nodeId, {
        ...restConfig,
        output_definitions: defs,
      });
      setSavedAt(Date.now());
      onSaved();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="mb-3 border-t border-border pt-3">
      <div className={sectionLabelCls}>
        Output
      </div>
      <Field label="Mode" htmlFor="cfg-output-mode">
        <NativeSelect
          id="cfg-output-mode"
          value={mode}
          onChange={(e) => setMode(e.target.value)}
        >
          <option value="">(default — replace)</option>
          <option value="replace">replace</option>
          <option value="append">append</option>
          <option value="merge">merge</option>
        </NativeSelect>
      </Field>
      <Field label="Merge Keys" htmlFor="cfg-output-merge-keys">
        <Input
          id="cfg-output-merge-keys"
          value={keysText}
          placeholder="customer_id, as_of_date"
          onChange={(e) => setKeysText(e.target.value)}
          className="font-mono text-xs"
        />
      </Field>
      <p className="mb-2 text-[11px] text-muted-foreground">
        Comma-separated. Setting merge keys implies <code>mode = merge</code>{" "}
        when mode is left at the default.
      </p>
      <Field label="Cluster by" htmlFor="cfg-output-cluster-by">
        <Input
          id="cfg-output-cluster-by"
          value={clusterByText}
          placeholder="event_date, customer_id"
          onChange={(e) => setClusterByText(e.target.value)}
          className="font-mono text-xs"
        />
      </Field>
      <p className="mb-2 text-[11px] text-muted-foreground">
        Columns to liquid-cluster the output table on (prune-friendly reads).
        Optional; merge outputs already cluster by their merge keys.
      </p>
      <Field label="Merge update (aggregate-aware)" htmlFor="cfg-output-merge-update">
        <Textarea
          id="cfg-output-merge-update"
          value={mergeUpdateText}
          rows={3}
          placeholder={"total = additive\nlast_seen = max\nfingerprints = sketch"}
          onChange={(e) => setMergeUpdateText(e.target.value)}
          className="font-mono text-xs"
        />
      </Field>
      <p className="mb-2 text-[11px] text-muted-foreground">
        One <code>column = spec</code> per line for mode=merge outputs. Spec is a
        keyword (additive, min, max, sketch) or a raw SparkSQL expression over
        target./source. Accumulates instead of overwriting; unlisted columns
        replace as usual.
      </p>
      <label className="mb-3 flex items-start gap-2 text-xs text-foreground">
        <input
          type="checkbox"
          className="mt-0.5"
          checked={stats}
          onChange={(e) => setStats(e.target.checked)}
          data-testid="stats-checkbox"
        />
        <span>
          Compute column stats
          <span className="ml-2 block text-[11px] text-muted-foreground">
            Adds one aggregation pass per run; results appear on the
            Catalog table page.
          </span>
        </span>
      </label>
      <ExtraOutputsField dir={dir} nodeId={nodeId} config={config} onSaved={onSaved} />
      <div className="flex items-center gap-2">
        <Button
          onClick={handleSave}
          disabled={saving}
          size="sm"
          variant="outline"
          data-testid="save-output-config"
        >
          {saving ? (
            <>
              <Loader2 className="h-3 w-3 animate-spin" />
              Saving…
            </>
          ) : (
            "Save Output Config"
          )}
        </Button>
        {savedAt && !error && (
          <span className="text-[11px] text-muted-foreground">Saved</span>
        )}
      </div>
      {error && (
        <div
          role="alert"
          className={cn("mt-2", errorNoteCls)}
        >
          {error}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// ComputeSection — the transform's `compute` deploy target.
//
// `compute` is the AWS target a deployed pipeline runs this transform on;
// it has no effect on local runs. Saving is immediate (single field, no
// separate Save button), and reverts the select on a backend rejection.
// ---------------------------------------------------------------------------

function ComputeSection({
  dir,
  nodeId,
  config,
  onSaved,
}: {
  dir: string;
  nodeId: string;
  config: Record<string, unknown>;
  onSaved: () => void;
}) {
  const current = typeof config.compute === "string" ? config.compute : "lambda";
  const [value, setValue] = useState(current);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [savedAt, setSavedAt] = useState<number | null>(null);

  const [prevNode, setPrevNode] = useState(nodeId);
  if (prevNode !== nodeId) {
    setPrevNode(nodeId);
    setValue(current);
    setError(null);
    setSavedAt(null);
  }

  async function handleChange(next: string) {
    const prev = value;
    setValue(next);
    setSaving(true);
    setError(null);
    try {
      await updateNode(dir, nodeId, { compute: next });
      setSavedAt(Date.now());
      onSaved();
    } catch (e) {
      setValue(prev);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="mb-3 border-t border-border pt-3">
      <div className={sectionLabelCls}>
        Compute
      </div>
      <Field label="Deploy target" htmlFor="cfg-compute">
        <NativeSelect
          id="cfg-compute"
          value={value}
          disabled={saving}
          onChange={(e) => handleChange(e.target.value)}
        >
          <option value="lambda">lambda</option>
          <option value="fargate">fargate</option>
          <option value="emr-serverless">emr-serverless</option>
        </NativeSelect>
      </Field>
      <p className="mb-1 text-[11px] text-muted-foreground">
        AWS target for a deployed pipeline. Local runs ignore it — pick
        <code> fargate</code> or <code>emr-serverless</code> only when the
        cloud workload outgrows Lambda.
      </p>
      {savedAt && !error && (
        <span className="text-[11px] text-muted-foreground">Saved</span>
      )}
      {error && (
        <div
          role="alert"
          className={cn("mt-2", errorNoteCls)}
        >
          {error}
        </div>
      )}
    </div>
  );
}

// IncrementalUpstreamSection — per-alias toggle for CDF-bounded reads
// of upstream transform tables.
//
// Each transform→transform edge into this node shows up as one row.
// Toggling the checkbox flips the alias on/off in this node's
// `incremental_inputs` list (committed via updateNode). The runner
// then reads only Delta CDF rows committed since the consumer's last
// successful run, tracking watermark per (consumer, alias). Defaults
// off (full-read every run, the historical behaviour).
function IncrementalUpstreamSection({
  dir,
  nodeId,
  config,
  incomingTransformAliases,
  onSaved,
}: {
  dir: string;
  nodeId: string;
  config: Record<string, unknown>;
  incomingTransformAliases: string[];
  onSaved: () => void;
}) {
  const current = new Set(
    Array.isArray(config.incremental_inputs)
      ? (config.incremental_inputs as unknown[]).filter(
          (v) => typeof v === "string",
        ) as string[]
      : [],
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Cross-pipeline inputs (external_inputs) aren't graph edges, so they don't
  // show up in incomingTransformAliases — but they're equally valid CDF
  // targets (the runner reads a cross-pipeline Delta table's change feed by
  // name as of v2.6.0). List them alongside the same-pipeline transform edges
  // so the toggle covers both (CLI/UI parity, ADR-015).
  const externalAliases = (
    config.external_inputs && typeof config.external_inputs === "object"
      ? Object.keys(config.external_inputs as Record<string, unknown>)
      : []
  ).sort();
  const rows: { alias: string; cross: boolean }[] = [
    ...incomingTransformAliases.map((alias) => ({ alias, cross: false })),
    ...externalAliases.map((alias) => ({ alias, cross: true })),
  ];

  if (rows.length === 0) {
    return null;
  }

  async function toggle(alias: string) {
    setBusy(true);
    setError(null);
    try {
      const next = new Set(current);
      if (next.has(alias)) next.delete(alias);
      else next.add(alias);
      const sorted = Array.from(next).sort();
      const {
        source_inputs: _src,
        external_inputs: _ext,
        ...restConfig
      } = config;
      void _src;
      void _ext;
      await updateNode(dir, nodeId, {
        ...restConfig,
        incremental_inputs: sorted,
      });
      onSaved();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mb-3 border-t border-border pt-3">
      <div className={sectionLabelCls}>
        Incremental upstream reads
      </div>
      <ul className="mb-2 space-y-1">
        {rows.map(({ alias, cross }) => (
          <li
            key={alias}
            className="flex items-center justify-between rounded-md border border-border px-2 py-1 font-mono text-xs"
          >
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={current.has(alias)}
                disabled={busy}
                onChange={() => void toggle(alias)}
              />
              <code>{alias}</code>
              {cross && (
                <span className="rounded bg-muted px-1 text-[10px] text-muted-foreground">
                  cross-pipeline
                </span>
              )}
            </label>
            <span className="text-[11px] text-muted-foreground">
              {current.has(alias) ? "CDF range" : "full read"}
            </span>
          </li>
        ))}
      </ul>
      <p className="text-[11px] text-muted-foreground">
        When on, the runner reads only Delta CDF rows committed since this
        node's last successful run. Defaults off (full read every run).
      </p>
      {error && (
        <div className="mt-1 text-[11px] text-status-failed">{error}</div>
      )}
    </div>
  );
}

// ExtraOutputsField — list non-default output keys with add/remove.
//
// Multi-output Python transforms declare each returned key under
// output_definitions; the emitter passes each through to the runner so
// per-key mode/merge_keys flow at execution time. The default key has
// its own dedicated mode/merge_keys controls above; extra keys get the
// inherited replace mode for now and are tuned via direct .tf edit when
// users need per-key descriptors.
function ExtraOutputsField({
  dir,
  nodeId,
  config,
  onSaved,
}: {
  dir: string;
  nodeId: string;
  config: Record<string, unknown>;
  onSaved: () => void;
}) {
  const defs = (config.output_definitions as Record<string, unknown> | undefined) ?? {};
  const extraKeys = Object.keys(defs).filter((k) => k !== "default").sort();
  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function isValidKey(k: string): boolean {
    return /^[A-Za-z_][A-Za-z0-9_]*$/.test(k);
  }

  async function commit(next: Record<string, unknown>) {
    setBusy(true);
    setError(null);
    try {
      const {
        source_inputs: _src,
        external_inputs: _ext,
        ...restConfig
      } = config;
      void _src;
      void _ext;
      await updateNode(dir, nodeId, {
        ...restConfig,
        output_definitions: next,
      });
      onSaved();
      setDraft("");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  async function addOutput() {
    const key = draft.trim();
    if (!key) return;
    if (!isValidKey(key)) {
      setError(`Output key must match [A-Za-z_][A-Za-z0-9_]*`);
      return;
    }
    if (defs[key]) {
      setError(`Output ${key} already exists`);
      return;
    }
    await commit({ ...defs, [key]: {} });
  }

  async function removeOutput(key: string) {
    const { [key]: _drop, ...rest } = defs;
    void _drop;
    await commit(rest);
  }

  return (
    <div className="mb-3">
      <div className="mb-1 text-[11px] text-muted-foreground">
        Extra outputs (multi-output Python transforms)
      </div>
      {extraKeys.length > 0 && (
        <ul className="mb-2 space-y-1">
          {extraKeys.map((k) => (
            <li
              key={k}
              className="flex items-center justify-between rounded-md border border-border px-2 py-1 font-mono text-xs"
            >
              <code>{k}</code>
              <button
                type="button"
                disabled={busy}
                onClick={() => removeOutput(k)}
                className="text-[11px] text-muted-foreground hover:text-status-failed"
              >
                remove
              </button>
            </li>
          ))}
        </ul>
      )}
      <div className="flex items-center gap-2">
        <Input
          value={draft}
          placeholder="outliers"
          disabled={busy}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              void addOutput();
            }
          }}
          className="h-7 font-mono text-xs"
        />
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={busy || !draft.trim()}
          onClick={() => void addOutput()}
        >
          Add output
        </Button>
      </div>
      {error && (
        <div className="mt-1 text-[11px] text-status-failed">{error}</div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// LiveDataTab — Athena/Delta live data for destination nodes
// ---------------------------------------------------------------------------

interface LiveDataRow {
  [col: string]: unknown;
}

interface LiveDataResult {
  columns: string[];
  rows: LiveDataRow[];
}

async function fetchLiveData(
  catalogDb: string,
  catalogTable: string,
): Promise<LiveDataResult> {
  const url = `${BASE_URL}/data/table?catalog_db=${encodeURIComponent(catalogDb)}&catalog_table=${encodeURIComponent(catalogTable)}&limit=100`;
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`GET /data/table → ${res.status}: ${text}`);
  }
  return res.json() as Promise<LiveDataResult>;
}

function LiveDataTab({
  dir,
  nodeId,
}: {
  dir: string;
  nodeId: string;
}) {
  const [data, setData] = useState<LiveDataResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const vars = await getVars(dir);
      const pipelineName = (vars.pipeline_name ?? "").replace(/-/g, "_");
      const nodeIdClean = nodeId.replace(/-/g, "_");
      const catalogDb = `clavesa_${pipelineName}`;
      const catalogTable = `${nodeIdClean}__default`;
      const result = await fetchLiveData(catalogDb, catalogTable);
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [dir, nodeId]);

  useEffect(() => {
    load();
  }, [load]);

  if (loading) {
    return (
      <div className="flex items-center gap-2 py-2 text-xs text-muted-foreground">
        <Loader2 className="h-3 w-3 animate-spin" />
        Querying live data…
      </div>
    );
  }

  if (error) {
    const isNotFound = error.includes("404") || error.toLowerCase().includes("not found");
    return (
      <div
        className={cn(
          "mt-1 rounded-md border p-2 text-xs",
          isNotFound
            ? "border-border bg-muted text-muted-foreground"
            : "border-status-failed/40 bg-status-failed/10 text-status-failed"
        )}
      >
        {isNotFound
          ? "Table not found — deploy the pipeline first."
          : error}
      </div>
    );
  }

  if (!data || data.rows.length === 0) {
    return (
      <div className="py-1 text-xs text-muted-foreground">No rows returned.</div>
    );
  }

  return (
    <div className="mt-2">
      <div className="mb-1.5 flex items-center gap-2 text-[11px] text-muted-foreground">
        <span>
          {data.rows.length} row{data.rows.length !== 1 ? "s" : ""} (limit 100)
        </span>
        <button
          onClick={load}
          className="text-primary hover:underline"
        >
          Refresh
        </button>
      </div>
      <div className="max-h-60 overflow-auto rounded-md border border-border bg-background font-mono text-[11px]">
        <Table>
          <TableHeader className="sticky top-0 bg-background">
            <TableRow className="hover:bg-transparent">
              {data.columns.map((col) => (
                <TableHead key={col} className="px-2 py-1 text-[10px]">
                  {col}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.rows.map((row, i) => (
              <TableRow key={i}>
                {data.columns.map((col) => (
                  <TableCell
                    key={col}
                    className="max-w-44 truncate px-2 py-0.5 font-mono text-[11px]"
                    title={String(row[col] ?? "")}
                  >
                    {String(row[col] ?? "")}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}


// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------
// NodeNameEditor — inline-editable node id in the panel header.
//
// Click the name to edit; Enter or blur commits via renameNode, Escape
// cancels. A rename also moves the node's Delta output table, so the
// caption says so. On success the parent reselects the node under its new
// id, which resyncs this component.
// ---------------------------------------------------------------------------

function NodeNameEditor({
  dir,
  nodeId,
  onRenamed,
}: {
  dir: string;
  nodeId: string;
  onRenamed: (newId: string) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(nodeId);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [prev, setPrev] = useState(nodeId);
  if (prev !== nodeId) {
    setPrev(nodeId);
    setValue(nodeId);
    setEditing(false);
    setError(null);
  }

  function cancel() {
    setEditing(false);
    setValue(nodeId);
    setError(null);
  }

  async function commit() {
    const next = value.trim();
    if (next === nodeId || next === "") {
      cancel();
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await renameNode(dir, nodeId, next);
      onRenamed(next);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  if (!editing) {
    return (
      <button
        type="button"
        title="Rename node"
        onClick={() => setEditing(true)}
        className="font-mono text-sm font-semibold hover:underline"
      >
        {nodeId}
      </button>
    );
  }
  return (
    <div>
      <Input
        autoFocus
        value={value}
        disabled={busy}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            commit();
          } else if (e.key === "Escape") {
            cancel();
          }
        }}
        onBlur={cancel}
        className="h-7 w-48 font-mono text-sm"
      />
      <p className="mt-1 text-[11px] text-muted-foreground">
        Renames the node&apos;s Delta output table too.
      </p>
      {error && (
        <div className="mt-1 text-[11px] text-status-failed">{error}</div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------

export function ConfigPanel({
  dir,
  selectedNode,
  onGraphUpdate,
  onPreview,
  onNodeDeleted,
  onNodeRenamed,
  onClose,
  nodeSchemas,
  incomingTransformAliases,
  upstreamNodeIds,
  nodeInputs,
  output,
}: ConfigPanelProps) {
  const nodeId = selectedNode?.id ?? null;
  const [editedConfig, setEditedConfig] = useState<Record<string, unknown>>(
    selectedNode ? { ...selectedNode.data } : {}
  );
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [togglingEnabled, setTogglingEnabled] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [destTab, setDestTab] = useState<"config" | "live">("config");
  // Transform-only: which tab — Code (editor + immediate I/O) or Settings
  // (output mode / merge keys / compute / incremental upstream toggles).
  const [mode, setMode] = useState<"code" | "settings">("code");
  // Transform-only: drawer ↔ full-viewport overlay. Reset on selection
  // change so a new node always opens in the cheap drawer.
  const [expanded, setExpanded] = useState(false);
  const [prevNodeId, setPrevNodeId] = useState(nodeId);

  // Reset local edits when the selection changes
  if (nodeId !== prevNodeId) {
    setPrevNodeId(nodeId);
    setEditedConfig(selectedNode ? { ...selectedNode.data } : {});
    setError(null);
    setDestTab("config");
    setMode("code");
    setExpanded(false);
  }

  // Esc collapses the overlay before closing the panel — owned here so
  // PipelineDashboard's outer Esc handler doesn't double-handle (it
  // dropped selectedNode-clearing when the panel started managing its
  // own positioning). Capture phase so we beat any later handler.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key !== "Escape") return;
      if (expanded) {
        e.stopPropagation();
        setExpanded(false);
        return;
      }
      if (onClose) onClose();
    }
    document.addEventListener("keydown", onKey, true);
    return () => document.removeEventListener("keydown", onKey, true);
  }, [expanded, onClose]);

  // Eagerly fetched column lists for source-input aliases — keyed by
  // alias. nodeSchemas only populates after a DataPreview run, which
  // means the SQL editor's completion would never see source columns
  // until the user hits Preview. Filled below by an effect that calls
  // /api/sources/{name}/preview?limit=1 for each `sources.<name>`
  // attachment on the selected transform. Survives only the lifetime
  // of the selection — switching nodes resets it.
  const [sourceColumns, setSourceColumns] = useState<Map<string, Column[]>>(
    new Map(),
  );
  // sourceInputsKey is a stable JSON signature of the selected node's
  // source_inputs map; the effect only refires when an attachment
  // actually changes, not on every editedConfig keystroke.
  const sourceInputsKey = useMemo(
    () => JSON.stringify(selectedNode?.data?.source_inputs ?? {}),
    [selectedNode?.data?.source_inputs],
  );
  useEffect(() => {
    setSourceColumns(new Map());
    if (!selectedNode || selectedNode.type !== "transform") return;
    const m = selectedNode.data?.source_inputs as
      | Record<string, SourceInputValue>
      | undefined;
    if (!m) return;
    // Map alias → registry source name. The descriptor shape carries
    // spec_name; the string shape is `sources.<name>`.
    const aliasToSource: Record<string, string> = {};
    for (const [alias, v] of Object.entries(m)) {
      if (typeof v === "string") {
        const match = v.match(/^sources\.([A-Za-z_][A-Za-z0-9_-]*)$/);
        if (match) aliasToSource[alias] = match[1];
      } else if (v && typeof v === "object" && v.spec_name) {
        aliasToSource[alias] = v.spec_name;
      }
    }
    if (Object.keys(aliasToSource).length === 0) return;
    let cancelled = false;
    (async () => {
      const entries = await Promise.all(
        Object.entries(aliasToSource).map(async ([alias, name]) => {
          try {
            const res = await request<{ schema?: Column[] }>(
              `/sources/${encodeURIComponent(name)}/preview?limit=1`,
            );
            return [alias, res.schema ?? []] as const;
          } catch {
            // A failed source fetch leaves the alias empty —
            // autocomplete still surfaces the alias itself, just
            // without column hints. Don't toast or block.
            return [alias, [] as Column[]] as const;
          }
        }),
      );
      if (cancelled) return;
      setSourceColumns((prev) => {
        const next = new Map(prev);
        for (const [alias, cols] of entries) next.set(alias, cols);
        return next;
      });
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedNode, sourceInputsKey]);

  // Catalog tables for this workspace — used to resolve column lists
  // for upstream-transform inputs (transform→transform edges) without
  // requiring a Preview run. The query is workspace-wide and cached by
  // TanStack Query; we filter to this pipeline's tables below.
  const catalogQuery = useCatalogTables();

  // Upstream inputs the transform's SQL can read, with their column
  // lists — feeds the SQL editor's autocomplete and the inputs browser
  // panel. Three column sources, in preference order: a fresh live
  // preview (richest), the catalog table that an upstream transform
  // already produced, and the eager source-preview fetch. Falls back to
  // empty when none of those exist yet.
  const sqlInputs = useMemo(() => {
    const out: { alias: string; columns: Column[] }[] = [];
    const seen = new Set<string>();
    const add = (alias: string, columns: Column[]) => {
      if (!alias || seen.has(alias)) return;
      seen.add(alias);
      out.push({ alias, columns });
    };
    const catalogByNode = new Map<string, Column[]>();
    for (const t of catalogQuery.data?.tables ?? []) {
      if (t.dir !== dir || !t.owning_node) continue;
      // The first table per owning_node is its primary output (the
      // `__default` key). Skipping subsequent multi-output rows is
      // fine — autocomplete on the alias should match the primary.
      if (catalogByNode.has(t.owning_node)) continue;
      catalogByNode.set(
        t.owning_node,
        t.columns.map((c) => ({
          name: c.name,
          type: c.type,
          nullable: true,
        })),
      );
    }
    for (const [alias, fromId] of Object.entries(nodeInputs ?? {})) {
      const cols =
        nodeSchemas?.get(fromId) ?? catalogByNode.get(fromId) ?? [];
      add(alias, cols);
    }
    for (const key of ["source_inputs", "external_inputs"] as const) {
      const m = editedConfig[key];
      if (m && typeof m === "object") {
        for (const alias of Object.keys(m as Record<string, unknown>)) {
          const cols =
            nodeSchemas?.get(alias) ?? sourceColumns.get(alias) ?? [];
          add(alias, cols);
        }
      }
    }
    return out;
  }, [
    nodeInputs,
    nodeSchemas,
    editedConfig,
    sourceColumns,
    catalogQuery.data,
    dir,
  ]);

  // After an Attach roundtrip the parent re-fetches the pipeline, and
  // `selectedNode.data` arrives with fresh `source_inputs` /
  // `external_inputs` — synthetic parser keys the user never edits
  // locally. Sync those two specifically so the Inputs section
  // reflects the new attachment without the user having to re-click
  // the node. Other config (sql, etc.) stays at whatever the user has
  // typed.
  const upstreamSourceInputs = selectedNode?.data?.source_inputs;
  const upstreamExternalInputs = selectedNode?.data?.external_inputs;
  // Output keys are added/removed by ExtraOutputsField (committed to the
  // server, not into editedConfig), so the underlying selectedNode.data
  // is the source of truth. Sync upstream → editedConfig on identity
  // changes so the Extra outputs list reflects an add/remove without a
  // page reload. Safe because the form's own Save (TransformOutputSection)
  // also commits via the server then re-fetches; nothing here depends on
  // editedConfig.output_definitions being preserved mid-flight.
  const upstreamOutputDefinitions = selectedNode?.data?.output_definitions;
  const upstreamIncrementalInputs = selectedNode?.data?.incremental_inputs;
  useEffect(() => {
    setEditedConfig((prev) => {
      const next = { ...prev };
      if (upstreamSourceInputs === undefined) {
        delete next.source_inputs;
      } else {
        next.source_inputs = upstreamSourceInputs;
      }
      if (upstreamExternalInputs === undefined) {
        delete next.external_inputs;
      } else {
        next.external_inputs = upstreamExternalInputs;
      }
      if (upstreamOutputDefinitions === undefined) {
        delete next.output_definitions;
      } else {
        next.output_definitions = upstreamOutputDefinitions;
      }
      if (upstreamIncrementalInputs === undefined) {
        delete next.incremental_inputs;
      } else {
        next.incremental_inputs = upstreamIncrementalInputs;
      }
      return next;
    });
  }, [
    upstreamSourceInputs,
    upstreamExternalInputs,
    upstreamOutputDefinitions,
    upstreamIncrementalInputs,
  ]);

  // Auto-sample of this node's output Delta table. Drives both the
  // inline sample-rows panel and the right-side TransformInputsBrowser
  // (column list + loader). One query keyed on (database, table) — the
  // panel below uses the same hook so TanStack dedupes the network call.
  const outputSample = useTableSample(
    output ? `${output.catalog}__${output.schema}` : "",
    output?.table ?? "",
    10,
    { dir, enabled: !!output },
  );
  const outputColumnsForBrowser = useMemo<Column[]>(() => {
    if (!selectedNode) return [];
    return nodeSchemas?.get(selectedNode.id) ?? [];
  }, [selectedNode, nodeSchemas]);

  if (!selectedNode) return null;

  async function handleSave() {
    setSaving(true);
    setError(null);
    try {
      await updateNode(dir, selectedNode!.id, editedConfig);
      onGraphUpdate();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete() {
    if (!window.confirm(`Delete node "${selectedNode!.id}"? This cannot be undone.`)) return;
    setDeleting(true);
    setError(null);
    try {
      await deleteNode(dir, selectedNode!.id);
      onGraphUpdate();
      onNodeDeleted?.();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setDeleting(false);
    }
  }


  const isTransformNode = selectedNode!.type === "transform";
  // A node is enabled unless its config explicitly says enabled = false.
  const nodeEnabled =
    (selectedNode!.data as Record<string, unknown> | undefined)?.enabled !==
    false;

  async function handleToggleEnabled() {
    setTogglingEnabled(true);
    setError(null);
    try {
      await updateNode(dir, selectedNode!.id, { enabled: !nodeEnabled });
      onGraphUpdate();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setTogglingEnabled(false);
    }
  }

  const showPreview =
    selectedNode!.type === "s3_source" ||
    selectedNode!.type === "source" ||
    selectedNode!.type === "sql_transform" ||
    selectedNode!.type === "transform" ||
    selectedNode!.type === "destination";

  // Transform Code tab — inputs + editor + auto-sampled output. Lays out
  // stacked in the drawer; switches to a three-column grid when expanded
  // so the editor gets the middle third of a wide viewport.
  function renderTransformCodeTab() {
    const inputs = (
      <TransformInputsSection
        dir={dir}
        nodeId={selectedNode!.id}
        sourceInputs={editedConfig.source_inputs as Record<string, SourceInputValue> | undefined}
        externalInputs={editedConfig.external_inputs as Record<string, string> | undefined}
        upstreamNodeIds={upstreamNodeIds ?? []}
        nodeInputs={nodeInputs ?? {}}
        onAttached={onGraphUpdate}
      />
    );
    const editor = (
      <TransformEditor
        key={selectedNode!.id}
        dir={dir}
        nodeId={selectedNode!.id}
        config={editedConfig}
        sqlInputs={sqlInputs}
        output={
          output
            ? {
                columns: outputColumnsForBrowser,
                loading: outputSample.isFetching,
              }
            : undefined
        }
        onSaved={() => onGraphUpdate()}
      />
    );
    const sample = output ? (
      <OutputSamplePanel
        dir={dir}
        database={`${output.catalog}__${output.schema}`}
        table={output.table}
      />
    ) : null;

    return (
      <div className={cn("px-4 py-3", expanded && "mx-auto w-full max-w-5xl")}>
        {inputs}
        {editor}
        {sample}
      </div>
    );
  }

  function renderTransformSettingsTab() {
    return (
      <div className={cn("px-4 py-3", expanded && "mx-auto w-full max-w-2xl")}>
        <IncrementalUpstreamSection
          dir={dir}
          nodeId={selectedNode!.id}
          config={editedConfig}
          incomingTransformAliases={incomingTransformAliases ?? []}
          onSaved={onGraphUpdate}
        />
        <TransformOutputSection
          dir={dir}
          nodeId={selectedNode!.id}
          config={editedConfig}
          onSaved={onGraphUpdate}
        />
        <ComputeSection
          dir={dir}
          nodeId={selectedNode!.id}
          config={editedConfig}
          onSaved={onGraphUpdate}
        />
      </div>
    );
  }

  function renderNonTransformBody() {
    const body = (() => {
      if (selectedNode!.type === "destination" && destTab === "live") {
        return <LiveDataTab dir={dir} nodeId={selectedNode!.id} />;
      }
      switch (selectedNode!.type) {
        case "source":
          return <S3SourceForm config={editedConfig} onChange={setEditedConfig} />;
        case "destination":
          return <S3DestinationForm config={editedConfig} onChange={setEditedConfig} />;
        default:
          return (
            <div className="text-xs text-muted-foreground">
              No configuration panel for node type &quot;{selectedNode!.type}&quot;.
            </div>
          );
      }
    })();
    return (
      <div className="px-4 py-3">
        {body}
        {output && destTab !== "live" && (
          <OutputSamplePanel
            dir={dir}
            database={`${output.catalog}__${output.schema}`}
            table={output.table}
          />
        )}
        {destTab !== "live" && error && (
          <div
            role="alert"
            className={cn("mt-2.5", errorNoteCls)}
          >
            {error}
          </div>
        )}
      </div>
    );
  }

  // Footer — Save (non-transform only; transforms save per-section),
  // Preview, Delete. Hidden when the destination's Live tab is open.
  function renderFooter() {
    if (selectedNode!.type === "destination" && destTab === "live") return null;
    return (
      <div className="flex flex-shrink-0 flex-col gap-2 border-t border-border px-4 py-3">
        {!isTransformNode && (
          <Button onClick={handleSave} disabled={saving} size="sm">
            {saving ? (
              <>
                <Loader2 className="h-3 w-3 animate-spin" />
                Saving…
              </>
            ) : (
              "Save"
            )}
          </Button>
        )}
        {isTransformNode && error && (
          <div
            role="alert"
            className={errorNoteCls}
          >
            {error}
          </div>
        )}
        {onPreview && showPreview && (
          <Button
            onClick={() => onPreview(selectedNode!.id, editedConfig.sql as string | undefined)}
            variant="outline"
            size="sm"
            className="justify-start"
          >
            Preview
          </Button>
        )}
        {isTransformNode && (
          <Button
            onClick={handleToggleEnabled}
            disabled={togglingEnabled}
            variant="outline"
            size="sm"
            className="justify-start"
            title={
              nodeEnabled
                ? "Skip this node in runs without deleting it; its existing output table stays for downstream"
                : "Run this node again"
            }
          >
            {togglingEnabled ? (
              <>
                <Loader2 className="h-3 w-3 animate-spin" />
                {nodeEnabled ? "Disabling…" : "Enabling…"}
              </>
            ) : nodeEnabled ? (
              "Disable node"
            ) : (
              "Enable node"
            )}
          </Button>
        )}
        <Button
          onClick={handleDelete}
          disabled={deleting}
          variant="outline"
          size="sm"
          className="border-status-failed/40 text-status-failed hover:bg-status-failed/10 hover:text-status-failed"
        >
          {deleting ? (
            <>
              <Loader2 className="h-3 w-3 animate-spin" />
              Deleting…
            </>
          ) : (
            "Delete Node"
          )}
        </Button>
      </div>
    );
  }

  return (
    <aside
      className={cn(
        "flex flex-col border-l border-border bg-background text-foreground shadow-xl",
        expanded
          ? "absolute inset-0 z-50"
          : cn(
              "fixed bottom-0 right-0 top-14 z-40",
              isTransformNode ? "w-[500px]" : "w-[320px]"
            )
      )}
    >
      {/* Header */}
      <div className="flex flex-shrink-0 items-start justify-between gap-3 border-b border-border px-4 py-3">
        <div className="min-w-0">
          <div className="text-[11px] font-bold uppercase tracking-wider text-muted-foreground">
            {selectedNode.type}
          </div>
          <NodeNameEditor
            dir={dir}
            nodeId={selectedNode.id}
            onRenamed={(newId) => {
              // The editor (App) re-fetches the graph and reselects the
              // node under its new id. Fall back to a plain refresh if no
              // rename handler is wired.
              if (onNodeRenamed) onNodeRenamed(newId);
              else onGraphUpdate();
            }}
          />
        </div>
        <div className="flex items-center gap-1">
          {isTransformNode && (
            <button
              onClick={() => setExpanded((v) => !v)}
              aria-label={expanded ? "Collapse" : "Expand"}
              title={expanded ? "Collapse (Esc)" : "Expand"}
              className="flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
            >
              {expanded ? (
                <Minimize2 className="h-4 w-4" />
              ) : (
                <Maximize2 className="h-4 w-4" />
              )}
            </button>
          )}
          {onClose && (
            <button
              onClick={onClose}
              aria-label="Close"
              data-testid="close-node-panel"
              className="-mr-1 flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
      </div>

      {/* Transform Code/Settings tabs */}
      {isTransformNode && (
        <div className="flex flex-shrink-0 border-b border-border px-4">
          {(["code", "settings"] as const).map((m) => (
            <button
              key={m}
              onClick={() => setMode(m)}
              data-testid={m === "settings" ? "node-settings" : undefined}
              className={cn(
                "border-b-2 px-3 py-1.5 text-xs font-semibold capitalize transition-colors",
                mode === m
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              )}
            >
              {m}
            </button>
          ))}
        </div>
      )}

      {/* Destination Config/Live sub-tabs */}
      {selectedNode.type === "destination" && (
        <div className="flex flex-shrink-0 border-b border-border px-4">
          {(["config", "live"] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setDestTab(tab)}
              className={cn(
                "border-b-2 px-3 py-1.5 text-xs font-semibold capitalize transition-colors",
                destTab === tab
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              )}
            >
              {tab === "live" ? "Live Data" : "Config"}
            </button>
          ))}
        </div>
      )}

      {/* Body — for transforms, keep both tabs mounted (hide the
          inactive one) so the editor's unsaved SQL/Python buffer
          survives a tab switch. Scrolls per-tab; in expanded Code mode
          each column manages its own scroll. */}
      {isTransformNode ? (
        <div className="relative flex-1 min-h-0">
          <div
            className={cn(
              "h-full overflow-y-auto",
              mode === "code" ? "" : "hidden"
            )}
          >
            {renderTransformCodeTab()}
          </div>
          <div
            className={cn(
              "h-full overflow-y-auto",
              mode === "settings" ? "" : "hidden"
            )}
          >
            {renderTransformSettingsTab()}
          </div>
        </div>
      ) : (
        <div className="flex-1 min-h-0 overflow-y-auto">
          {renderNonTransformBody()}
        </div>
      )}

      {renderFooter()}
    </aside>
  );
}

/**
 * OutputSamplePanel — inline sample of the node's current Delta output.
 *
 * Fires `useTableSample` automatically; no button press. This is the
 * "preview should be automatic" affordance — open a node whose output
 * exists and you see its current data, not just its config. The Preview
 * button below still exists for re-executing with unsaved SQL edits.
 */
function OutputSamplePanel({
  dir,
  database,
  table,
}: {
  dir: string;
  database: string;
  table: string;
}) {
  const sample = useTableSample(database, table, 10, { dir });
  return (
    <div className="mt-4 border-t border-border pt-3">
      <div className="mb-1.5 flex items-center justify-between text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
        <span>Current output · 10 rows</span>
        {sample.isFetching && (
          <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
        )}
      </div>
      {sample.error && (
        <div className="rounded-md border border-status-failed/40 bg-status-failed/5 p-2 text-[11px] text-muted-foreground">
          Sample unavailable: {sample.error instanceof Error ? sample.error.message : String(sample.error)}
        </div>
      )}
      {!sample.error && sample.data && sample.data.rows.length === 0 && (
        <div className="rounded-md border border-border bg-muted/30 p-2 text-[11px] text-muted-foreground">
          Table exists but has no rows yet. Run the pipeline to populate it.
        </div>
      )}
      {!sample.error && sample.data && sample.data.rows.length > 0 && (
        <div className="max-h-56 overflow-auto rounded-md border border-border bg-background font-mono text-[11px]">
          <Table>
            <TableHeader className="sticky top-0 bg-background">
              <TableRow className="hover:bg-transparent">
                {sample.data.columns.map((c) => (
                  <TableHead key={c.name} className="px-2 py-1 text-[10px]">
                    {c.name}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {sample.data.rows.map((row, i) => (
                <TableRow key={i} className="hover:bg-muted/40">
                  {row.map((cell, j) => (
                    <TableCell
                      key={j}
                      className="max-w-44 truncate px-2 py-0.5 font-mono text-[11px]"
                      title={cell}
                    >
                      {cell}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}

export default ConfigPanel;
