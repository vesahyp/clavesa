/**
 * PipelineGraph — renders a PipelineGraph JSON document as an interactive DAG.
 *
 * Layout is computed by dagre (left-to-right). Positions are never hardcoded
 * or stored — they are a pure function of the graph topology on every render.
 *
 * Each non-destination node has a single output handle on the right.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useReactFlow,
  type Node as RFNode,
  type Edge as RFEdge,
} from "@xyflow/react";
import dagre from "@dagrejs/dagre";
import {
  addEdge as apiAddEdge,
  addTypedNode,
  deleteEdge as apiDeleteEdge,
} from "../api/pipeline";
import { attachSource, useSources } from "../lib/queries";

import type { PipelineGraph as PipelineGraphType, Column } from "../types/pipeline";
import { PipelineNode } from "./PipelineNode";
import type { PipelineNodeData, NodeOutput } from "./PipelineNode";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default node dimensions passed to dagre so it can compute proper spacing. */
const NODE_WIDTH = 200;
const NODE_HEIGHT = 100; // approximate; dagre uses this for rank separation

/** dagre graph options */
const DAGRE_GRAPH_OPTS = {
  rankdir: "LR",   // left-to-right layout
  ranksep: 80,     // horizontal separation between ranks
  nodesep: 40,     // vertical separation between nodes in the same rank
};

// ---------------------------------------------------------------------------
// nodeTypes registration — must be stable (defined outside the component)
// ---------------------------------------------------------------------------
const nodeTypes = {
  pipelineNode: PipelineNode,
};

// ---------------------------------------------------------------------------
// Layout helper
// ---------------------------------------------------------------------------

/**
 * Per-node height used by both dagre (rank/node separation) and React Flow
 * (rendered card height). When a node has a column profile, the card grows
 * to fit up to 5 rows plus a "+N more" footer — dagre needs the real height
 * or downstream nodes pile up on top of each other (#layout-overlap).
 */
function nodeHeightFor(nodeId: string, nodeSchemas?: Map<string, Column[]>): number {
  const columns = nodeSchemas?.get(nodeId);
  const displayCount = columns ? Math.min(columns.length, 5) : 0;
  const hasMore = columns && columns.length > 5;
  if (displayCount === 0) return NODE_HEIGHT;
  return 80 + displayCount * 18 + (hasMore ? 16 : 0);
}

function computeLayout(
  nodes: PipelineGraphType["nodes"],
  edges: PipelineGraphType["edges"],
  nodeSchemas?: Map<string, Column[]>
): Map<string, { x: number; y: number }> {
  const g = new dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph(DAGRE_GRAPH_OPTS);

  const heights = new Map<string, number>();
  for (const node of nodes) {
    const h = nodeHeightFor(node.id, nodeSchemas);
    heights.set(node.id, h);
    g.setNode(node.id, { width: NODE_WIDTH, height: h });
  }

  for (const edge of edges) {
    g.setEdge(edge.from_node, edge.to_node);
  }

  dagre.layout(g);

  const positions = new Map<string, { x: number; y: number }>();
  for (const nodeId of g.nodes()) {
    const { x, y } = g.node(nodeId);
    const h = heights.get(nodeId) ?? NODE_HEIGHT;
    // dagre positions are centred — React Flow expects top-left corner
    positions.set(nodeId, {
      x: x - NODE_WIDTH / 2,
      y: y - h / 2,
    });
  }
  return positions;
}

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export type PipelineGraphProps = {
  graph: PipelineGraphType;
  /** Absolute path to the Terraform directory — required for edge mutations. */
  dir: string;
  /** Called after any mutation so the parent can re-fetch the pipeline. */
  onGraphUpdate: () => void;
  /** Surfaces a canvas-mutation failure (quick-add) to the editor. */
  onError?: (message: string) => void;
  /**
   * When true, render the guided (+) menu on each node's output handle
   * (create downstream node / connect to existing). The editor enables it;
   * the read-only dashboard and run-detail DAGs leave it off.
   */
  enableQuickAdd?: boolean;
  onNodeClick?: (nodeId: string) => void;
  onEdgeClick?: (edgeId: string) => void;
  /** Per-node schema columns inferred from preview results */
  nodeSchemas?: Map<string, Column[]>;
  /** Per-node Delta output table, shown in the node footer. */
  nodeOutputs?: Map<string, NodeOutput>;
  /** Node ID currently loading preview (null if none) */
  loadingNodeId?: string | null;
  /** Node ID whose preview completed (null if none) */
  previewedNodeId?: string | null;
  /**
   * Live per-node run status from an in-flight Step Functions execution.
   * Keyed by node id; values are "running" / "succeeded" / "failed".
   * Nodes absent from the map are treated as idle (idle/preview state takes over).
   */
  nodeStatuses?: Map<string, "running" | "succeeded" | "failed">;
  /**
   * When true, surface a transform's upstream data as synthetic nodes in
   * the DAG: ADR-017 registered sources (`config.source_inputs`) and
   * cross-pipeline reads (`config.external_inputs`). Neither is a real
   * graph node, so a pipeline whose only inputs are sources / external
   * tables would otherwise render a lone disconnected node. The synthetic
   * nodes are read-only — callers must ignore clicks on `source:` /
   * `external:` ids (the editor does).
   */
  showSources?: boolean;
  /**
   * Node the editor has selected. When set, the canvas pans so the node
   * sits in the area left of the config drawer.
   */
  focusNodeId?: string | null;
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Width of the config drawer the editor overlays on the right — the pan
 * offset that keeps a selected node clear of it. */
const DRAWER_WIDTH = 500;

/** isSyntheticNode reports whether a node id is one of the read-only
 * synthetic upstream renderings (`source:<name>` / `external:<ref>`)
 * rather than a real pipeline node. */
/**
 * extractDefaultOutputMode reads the write mode for a transform's
 * `default` output from its HCL `output_definitions`. The runner defaults
 * to `replace` when unspecified, so the PipelineNode footer fills that in.
 * Source / destination nodes don't carry the attribute — returns "".
 */
function extractDefaultOutputMode(config: Record<string, unknown> | undefined): string | undefined {
  if (!config) return undefined;
  const defs = config.output_definitions as Record<string, unknown> | undefined;
  const def = defs?.default as Record<string, unknown> | undefined;
  const mode = def?.mode;
  return typeof mode === "string" ? mode : undefined;
}

function isSyntheticNode(id: string): boolean {
  return id.startsWith("source:") || id.startsWith("external:");
}

/**
 * FocusController pans the canvas to the editor-selected node, offset left
 * so the config drawer doesn't cover it. Rendered inside <ReactFlow> so it
 * can use the flow instance.
 */
function FocusController({ focusNodeId }: { focusNodeId?: string | null }) {
  const rf = useReactFlow();
  useEffect(() => {
    if (!focusNodeId) return;
    const n = rf.getNode(focusNodeId);
    if (!n) return;
    const zoom = rf.getZoom();
    const w = n.measured?.width ?? NODE_WIDTH;
    const h = n.measured?.height ?? NODE_HEIGHT;
    rf.setCenter(
      n.position.x + w / 2 + DRAWER_WIDTH / 2 / zoom,
      n.position.y + h / 2,
      { zoom, duration: 350 },
    );
  }, [focusNodeId, rf]);
  return null;
}

/**
 * deriveNodeOutputs maps each transform node to the Delta table it writes,
 * for the DAG node footers. `catalog` / `schema` are the queried pipeline's
 * own ADR-016 namespace (from the lineage response) — every transform in a
 * pipeline writes into that one `<catalog>.<schema>`. Lineage parses `.tf`,
 * so this works before the pipeline has ever run. Returns an empty map when
 * the namespace is unknown; nodes then fall back to the bare "out" label.
 *
 * Shared by the editor (App.tsx) and the pipeline dashboard so both DAGs
 * render output tables identically.
 */
export function deriveNodeOutputs(
  nodes: PipelineGraphType["nodes"],
  catalog: string,
  schema: string,
): Map<string, NodeOutput> {
  const m = new Map<string, NodeOutput>();
  if (!catalog || !schema) return m;
  for (const n of nodes) {
    if (n.type !== "transform") continue;
    // ADR-019: single-output transforms drop the `__default` suffix on disk.
    // Wire form is the bare sanitized node id; multi-output editing isn't
    // surfaced in this derivation today so a single-output assumption is fine.
    m.set(n.id, {
      catalog,
      schema,
      table: n.id.replace(/-/g, "_"),
    });
  }
  return m;
}

/** Walk edges backwards from nodeId and return all upstream node IDs (inclusive). */
function findUpstreamChain(edges: PipelineGraphType["edges"], nodeId: string): Set<string> {
  const upstream = new Map<string, string[]>();
  for (const e of edges) {
    const arr = upstream.get(e.to_node) ?? [];
    arr.push(e.from_node);
    upstream.set(e.to_node, arr);
  }
  const visited = new Set<string>();
  const queue = [nodeId];
  while (queue.length > 0) {
    const curr = queue.shift()!;
    if (visited.has(curr)) continue;
    visited.add(curr);
    for (const parent of upstream.get(curr) ?? []) {
      queue.push(parent);
    }
  }
  return visited;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function PipelineGraph({
  graph,
  dir,
  onGraphUpdate,
  onError,
  enableQuickAdd,
  onNodeClick,
  onEdgeClick,
  nodeSchemas,
  nodeOutputs,
  loadingNodeId,
  previewedNodeId,
  nodeStatuses,
  showSources,
  focusNodeId,
}: PipelineGraphProps) {
  // Source registry — feeds the synthetic source nodes' kind / format /
  // location labels so users see "http · parquet" or "s3 · csv · my-bucket"
  // on the card without having to click it.
  const sources = useSources();
  const sourceSpecsByName = useMemo(() => {
    const m = new Map<string, { kind: string; format: string; url: string; bucket: string; prefix: string }>();
    for (const s of sources.data?.sources ?? []) {
      m.set(s.name, {
        kind: s.kind ?? "",
        format: s.format ?? "",
        url: s.url ?? "",
        bucket: s.bucket ?? "",
        prefix: s.prefix ?? "",
      });
    }
    return m;
  }, [sources.data]);
  // Guided (+) menu — create a downstream node and wire this node into it.
  // Replaces drag-to-connect: a drag can't carry the SQL alias an edge
  // needs, so authoring goes through explicit menu choices instead.
  const handleQuickCreate = useCallback(
    async (sourceId: string, type: "transform" | "destination") => {
      try {
        const before = new Set(graph.nodes.map((n) => n.id));
        const updated = await addTypedNode(dir, type);
        const created = updated.nodes.find((n) => !before.has(n.id));
        if (created) {
          await apiAddEdge(dir, sourceId, created.id);
        }
        onGraphUpdate();
      } catch (err) {
        onError?.(
          `Could not add node: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    },
    [dir, graph, onGraphUpdate, onError],
  );

  // Guided (+) menu — wire this node into an already-existing node.
  const handleQuickConnect = useCallback(
    async (sourceId: string, targetId: string) => {
      try {
        await apiAddEdge(dir, sourceId, targetId);
        onGraphUpdate();
      } catch (err) {
        onError?.(
          `Could not connect ${sourceId} → ${targetId}: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }
    },
    [dir, onGraphUpdate, onError],
  );

  // Guided (+) menu on a source node — registered sources aren't wired
  // with an edge (their id carries a colon and isn't a real module). They
  // attach into a transform's inputs via AttachSource instead.
  const handleSourceCreate = useCallback(
    async (sourceName: string) => {
      try {
        const before = new Set(graph.nodes.map((n) => n.id));
        const updated = await addTypedNode(dir, "transform");
        const created = updated.nodes.find((n) => !before.has(n.id));
        if (created) {
          await attachSource(sourceName, { dir, to: created.id });
        }
        onGraphUpdate();
      } catch (err) {
        onError?.(
          `Could not add node: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    },
    [dir, graph, onGraphUpdate, onError],
  );

  const handleSourceConnect = useCallback(
    async (sourceName: string, targetId: string) => {
      try {
        await attachSource(sourceName, { dir, to: targetId });
        onGraphUpdate();
      } catch (err) {
        onError?.(
          `Could not attach source ${sourceName} → ${targetId}: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }
    },
    [dir, onGraphUpdate, onError],
  );

  // Edge selection + delete. React Flow v12 only runs its own delete-key
  // flow for edges it owns in state; ours are derived from `graph`, so we
  // track the selected edge here and delete it on Backspace/Delete.
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);

  const handleDeleteEdge = useCallback(
    async (edgeId: string) => {
      try {
        await apiDeleteEdge(dir, edgeId);
        setSelectedEdgeId(null);
        onGraphUpdate();
      } catch (err) {
        onError?.(
          `Could not delete edge: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }
    },
    [dir, onGraphUpdate, onError],
  );

  useEffect(() => {
    if (!selectedEdgeId) return;
    function onKey(e: KeyboardEvent) {
      if (e.key !== "Backspace" && e.key !== "Delete") return;
      // Skip when focus is in any editor surface — INPUT, TEXTAREA,
      // SELECT, or anything contenteditable (Monaco / CodeMirror set
      // the editing host as contenteditable on a regular div, so the
      // tagName check alone misses them).
      const target = e.target as HTMLElement | null;
      const tag = target?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if (target?.isContentEditable) return;
      e.preventDefault();
      handleDeleteEdge(selectedEdgeId!);
    }
    // Capture phase so we win over any descendant component that might
    // stop propagation on the way up — historically the keybinding fired
    // intermittently because of this.
    document.addEventListener("keydown", onKey, true);
    return () => document.removeEventListener("keydown", onKey, true);
  }, [selectedEdgeId, handleDeleteEdge]);

  const { nodes: rfNodes, edges: rfEdges } = useMemo(() => {
    // ADR-017 registered sources aren't graph nodes — they live in each
    // transform's config.source_inputs. With showSources on, derive one
    // synthetic source node per referenced source plus an edge into each
    // consuming transform, so the DAG shows where data enters.
    const syntheticNodes: PipelineGraphType["nodes"] = [];
    const syntheticEdges: PipelineGraphType["edges"] = [];
    if (showSources) {
      const seen = new Set<string>();
      const addSyntheticUpstream = (
        sid: string,
        toNode: string,
        toInput: string,
      ) => {
        if (!seen.has(sid)) {
          seen.add(sid);
          syntheticNodes.push({
            id: sid,
            type: "source",
            module_source: "",
            config: {},
          });
        }
        syntheticEdges.push({ from_node: sid, to_node: toNode, to_input: toInput });
      };
      for (const node of graph.nodes) {
        if (node.type !== "transform") continue;
        // ADR-017 registered sources — config.source_inputs is
        // alias → "sources.<name>" (http) or {spec_name, …} (s3).
        const si = node.config?.source_inputs;
        if (si && typeof si === "object") {
          for (const raw of Object.values(si as Record<string, unknown>)) {
            let name = "";
            if (typeof raw === "string") {
              name = raw.replace(/^sources\./, "");
            } else if (raw && typeof raw === "object" && "spec_name" in raw) {
              name = String((raw as Record<string, unknown>).spec_name ?? "");
            }
            if (!name) continue;
            addSyntheticUpstream(`source:${name}`, node.id, name);
          }
        }
        // Cross-pipeline reads (ADR-016) — config.external_inputs is
        // alias → "<schema>.<table>". Surface the upstream table too, so a
        // pipeline whose only inputs are cross-pipeline reads doesn't
        // render a lone disconnected node.
        const ext = node.config?.external_inputs;
        if (ext && typeof ext === "object") {
          for (const [alias, raw] of Object.entries(
            ext as Record<string, unknown>,
          )) {
            if (typeof raw !== "string" || !raw) continue;
            addSyntheticUpstream(`external:${raw}`, node.id, alias);
          }
        }
      }
    }
    const allNodes = [...syntheticNodes, ...graph.nodes];
    const allEdges = [...graph.edges, ...syntheticEdges];

    const positions = computeLayout(allNodes, allEdges, nodeSchemas);
    const loadingChain = loadingNodeId
      ? findUpstreamChain(allEdges, loadingNodeId)
      : new Set<string>();
    const previewedChain = previewedNodeId
      ? findUpstreamChain(allEdges, previewedNodeId)
      : new Set<string>();

    // Candidate targets for "connect to existing node" in the (+) menu.
    // A real node can feed any transform or destination; a registered
    // source can only attach into a transform.
    const connectTargets = graph.nodes
      .filter((n) => n.type === "transform" || n.type === "destination")
      .map((n) => n.id);
    const transformTargets = graph.nodes
      .filter((n) => n.type === "transform")
      .map((n) => n.id);

    const rfNodes: RFNode<PipelineNodeData>[] = allNodes.map((node) => {
      const pos = positions.get(node.id) ?? { x: 0, y: 0 };
      const columns = nodeSchemas?.get(node.id);
      const nodeHeight = nodeHeightFor(node.id, nodeSchemas);
      // The (+) menu shows in the editor on real transform/source nodes
      // and on registered-source renderings. Destinations have no output;
      // external-table renderings stay read-only. `allEdges` includes the
      // synthetic source→transform edges, so a source's already-fed
      // transforms are excluded from its "connect to existing" list.
      const directDownstream = new Set(
        allEdges.filter((e) => e.from_node === node.id).map((e) => e.to_node),
      );
      let quickAdd: PipelineNodeData["quickAdd"];
      if (enableQuickAdd && node.id.startsWith("source:")) {
        const sourceName = node.id.slice("source:".length);
        quickAdd = {
          allowDestination: false,
          targets: transformTargets.filter((id) => !directDownstream.has(id)),
          onCreate: () => handleSourceCreate(sourceName),
          onConnect: (targetId: string) =>
            handleSourceConnect(sourceName, targetId),
        };
      } else if (
        enableQuickAdd &&
        !isSyntheticNode(node.id) &&
        node.type !== "destination"
      ) {
        quickAdd = {
          allowDestination: true,
          targets: connectTargets.filter(
            (id) => id !== node.id && !directDownstream.has(id),
          ),
          onCreate: (type: "transform" | "destination") =>
            handleQuickCreate(node.id, type),
          onConnect: (targetId: string) =>
            handleQuickConnect(node.id, targetId),
        };
      }
      // Resolve registered-source metadata for synthetic `source:<name>`
      // nodes so the card shows kind / format / location without a click.
      let sourceKind: string | undefined;
      let sourceFormat: string | undefined;
      let sourceLocation: string | undefined;
      if (node.id.startsWith("source:")) {
        const spec = sourceSpecsByName.get(node.id.slice("source:".length));
        if (spec) {
          sourceKind = spec.kind || undefined;
          sourceFormat = spec.format || undefined;
          if (spec.kind === "s3" && spec.bucket) {
            sourceLocation = `s3://${spec.bucket}/${spec.prefix ?? ""}`;
          } else if (spec.url) {
            sourceLocation = spec.url;
          }
        }
      }
      return {
        id: node.id,
        type: "pipelineNode",
        position: pos,
        width: NODE_WIDTH,
        height: nodeHeight,
        data: {
          label: node.id.startsWith("source:")
            ? node.id.slice("source:".length)
            : node.id.startsWith("external:")
              ? node.id.slice("external:".length)
              : node.id,
          nodeType: node.type,
          language: node.config?.language as string | undefined,
          compute: node.config?.compute as string | undefined,
          outputMode: extractDefaultOutputMode(node.config),
          sourceKind,
          sourceFormat,
          sourceLocation,
          columns,
          output: nodeOutputs?.get(node.id),
          loading: loadingChain.has(node.id),
          previewed: previewedChain.has(node.id),
          runStatus: nodeStatuses?.get(node.id),
          quickAdd,
        },
      };
    });

    // Read mode per edge: a transform→transform edge is incremental when
    // the consumer lists the edge's input alias in `incremental_inputs`.
    // The runner then reads only Delta CDF rows committed since the
    // consumer's last run, instead of a full table scan. Incremental edges
    // are drawn dashed + animated; full-read edges stay plain.
    const nodeById = new Map(allNodes.map((n) => [n.id, n]));
    const rfEdges: RFEdge[] = allEdges.map((edge) => {
      const toNode = nodeById.get(edge.to_node);
      const incList = toNode?.config?.incremental_inputs;
      const incremental =
        Array.isArray(incList) && incList.includes(edge.to_input);
      const aliasLabel =
        edge.to_input !== edge.from_node ? `→ ${edge.to_input}` : "";
      const label = incremental
        ? aliasLabel
          ? `${aliasLabel} · incremental`
          : "incremental"
        : aliasLabel || undefined;
      const edgeId = `${edge.from_node}->${edge.to_node}`;
      const isSelected = edgeId === selectedEdgeId;
      return {
        id: edgeId,
        source: edge.from_node,
        sourceHandle: "output",
        target: edge.to_node,
        targetHandle: "input",
        label,
        animated: incremental,
        style: isSelected
          ? { stroke: "#f87171", strokeWidth: 2 }
          : incremental
            ? { stroke: "#38bdf8" }
            : undefined,
        // Dark-theme the label pill so it doesn't render as React Flow's
        // default white box against the dark canvas.
        labelStyle: {
          fill: incremental ? "#7dd3fc" : "#94a3b8",
          fontWeight: incremental ? 600 : 400,
          fontSize: 11,
        },
        labelBgStyle: { fill: "#1e293b" },
        labelBgPadding: [6, 3] as [number, number],
        labelBgBorderRadius: 4,
      };
    });

    return { nodes: rfNodes, edges: rfEdges };
  }, [graph, nodeSchemas, nodeOutputs, loadingNodeId, previewedNodeId, nodeStatuses, showSources, enableQuickAdd, handleQuickCreate, handleQuickConnect, handleSourceCreate, handleSourceConnect, selectedEdgeId, sourceSpecsByName]);

  return (
    <div className="h-full w-full" data-testid="pipeline-graph" tabIndex={0}>
      <ReactFlow
        nodes={rfNodes}
        edges={rfEdges}
        nodeTypes={nodeTypes}
        fitView
        nodesConnectable={false}
        onNodeClick={(_evt, node) => {
          setSelectedEdgeId(null);
          onNodeClick?.(node.id);
        }}
        onEdgeClick={(_evt, edge) => {
          setSelectedEdgeId(edge.id);
          onEdgeClick?.(edge.id);
        }}
        onPaneClick={() => setSelectedEdgeId(null)}
        proOptions={{ hideAttribution: true }}
      >
        <FocusController focusNodeId={focusNodeId} />
        <Background />
        <Controls className="!shadow-md" />
        <MiniMap
          nodeColor="#334155"
          maskColor="rgba(15, 23, 42, 0.6)"
          style={{ background: "#1e293b" }}
        />
      </ReactFlow>
    </div>
  );
}

export default PipelineGraph;
