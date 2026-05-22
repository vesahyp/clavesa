# ADR 004: UI Framework and DAG Library

**Status**: Accepted

## Context

Clavesa needs a frontend framework for the visual DAG editor (Phase 2). The editor must render pipeline graphs from parsed HCL, support drag-and-drop node creation and edge connection, and write changes back to `.tf` files via the backend API.

Requirements from existing decisions and design principles:

- **Bidirectional HCL ↔ visual** — the UI reads Pipeline Graph JSON from HCL-PARSER and writes changes back via FILE-OPS-API; HCL is always the source of truth (ADR 001)
- **Multi-output nodes** — transforms can have multiple named output ports (e.g. "valid", "invalid"); edges connect from specific ports, not from the node as a whole (Pipeline Graph JSON contract)
- **Schema-aware edges** — edges carry schema context; the UI must visually represent port names and support schema-aware connection validation
- **Interactive editing** — drag-and-drop node creation from a palette, edge drawing between specific ports, node selection for config panels and data preview
- **Performance** — must handle at least 20 nodes without degradation (DAG-RENDERER acceptance criteria)
- **Deterministic auto-layout** — layout is computed from graph topology; same pipeline always renders the same way

### Framework choice: React

React was chosen early in the project for these reasons:

- **Ecosystem depth** — largest selection of DAG/graph visualization libraries, UI component libraries, and developer tooling
- **Hiring and contribution** — most widely known frontend framework; lowest barrier for contributors
- **Competitor validation** — Brainboard, Prophecy, Kestra, and AWS Infrastructure Composer all use React-based frontends
- **Backend alignment** — Go backend ([ADR 008](008-backend-language.md)) communicates via JSON APIs; React is independent of backend language choice

Vue and Svelte were considered but rejected: Vue's DAG library ecosystem (vue-flow) is a community port of React Flow with smaller adoption. Svelte has no mature DAG editor library — building one from scratch would be the dominant Phase 2 effort.

### DAG library options considered

**React Flow (@xyflow/react)** — the dominant React library for node-based editors. MIT licensed. Custom nodes with multiple handles (input/output ports), custom edges, built-in minimap/controls, keyboard navigation, touch support. Layout is bring-your-own via dagre or elkjs integration. Used by Stripe and widely adopted in open-source. Active development, strong documentation.

**Cytoscape.js + react wrapper** — powerful graph theory and visualization library. Strength is graph analysis (shortest path, clustering, centrality), not interactive editing. React integration is a thin wrapper, not React-native. No first-class concept of node ports/handles — multi-output nodes would require a custom abstraction layer on top. Better suited for read-only graph visualization than for an interactive editor.

**JointJS / Rappid** — diagramming toolkit with a commercial Rappid tier. Supports ports natively. Not React-native — renders to SVG via its own DOM management, which conflicts with React's rendering model. Rappid's commercial license adds cost and vendor dependency for a core component. JointJS (open-source) lacks key features (undo/redo, keyboard shortcuts).

**D3.js custom implementation** — maximum flexibility, maximum effort. D3 manages its own DOM, creating friction with React. Building node ports, edge routing, drag-and-drop, selection, zoom/pan, minimap, undo/redo, and keyboard shortcuts from scratch would dominate Phase 2 timeline. No justification when React Flow provides all of this out of the box.

### React Flow fit for Clavesa

React Flow's data model maps directly to Pipeline Graph JSON:

| Pipeline Graph JSON | React Flow concept |
|---|---|
| `Node` | Custom node component |
| `Node.outputs[]` | Source handles with unique IDs |
| `Edge.from_output` → `Edge.to_input` | Edge `sourceHandle` / `targetHandle` |
| dagre output | Node position (computed from topology) |
| `Node.type` (source/transform/destination) | Custom node types with distinct rendering |

Key capabilities that match Clavesa requirements:

- **Multiple named handles per node** — each `OutputPort` becomes a source handle with an ID matching the port name; edges connect to specific handles, not to the node. This is React Flow's native model.
- **Custom node rendering** — source, transform, and destination nodes can have distinct visual treatment (color, shape, icons, port layout) via custom React components
- **Custom edge rendering** — edges can display schema compatibility indicators or validation warnings
- **Connection validation** — `isValidConnection` callback can enforce rules (no cycles, schema compatibility) before an edge is created
- **Controlled mode** — React Flow can operate in fully controlled mode where the parent component owns all state; this is essential for keeping the UI in sync with HCL-PARSER output
- **Event system** — node/edge selection, connection, deletion, and drag events drive CONFIG-PANELS, EDGE-EDITOR, and DATA-PREVIEW
- **Layout integration** — dagre computes deterministic layout from graph topology on every render

### Layout strategy

React Flow does not include a built-in layout algorithm. Clavesa will use **dagre** (via `@dagrejs/dagre`) for automatic left-to-right DAG layout:

- **Deterministic layout** — dagre computes a left-to-right layout from the graph topology every time; the same pipeline always renders identically regardless of where it's opened
- **No position persistence** — positions are not stored in `.tf` files or sidecar files; layout is a pure function of the graph

Dagre over elkjs: dagre is simpler, smaller (no WASM), and sufficient for the DAG shapes Clavesa produces (pipelines are typically wide and shallow). Elkjs provides more layout algorithms but adds complexity not needed for pipeline DAGs.

## Decision

**React** as the UI framework. **React Flow (@xyflow/react v12+)** as the DAG rendering and interaction library. **Dagre** for automatic layout.

### Package versions

- `@xyflow/react` — v12+ (the current major version; uses the `@xyflow` npm scope)
- `@dagrejs/dagre` — for automatic DAG layout

### How it maps to components

| Component | Role of React Flow |
|---|---|
| DAG-RENDERER | Core — renders nodes, edges, handles zoom/pan/selection |
| NODE-PALETTE | Sidebar that creates new React Flow nodes via drag-and-drop |
| EDGE-EDITOR | Responds to edge selection events from React Flow |
| CONFIG-PANELS | Responds to node selection events from React Flow |
| DATA-PREVIEW | Responds to edge/port selection events from React Flow |

### Custom node architecture

Each Clavesa node type renders as a custom React Flow node component:

```
┌─────────────────────────────────┐
│ [icon] source: s3_source        │  ← header with type + module name
│                                 │
│  bucket: my-data                │  ← summary config (read-only)
│  format: json                   │
│                                 │
│                        default ●│  ← source handle (output port)
└─────────────────────────────────┘

┌─────────────────────────────────┐
│ [icon] transform: validate      │
│                                 │
│  runtime: sql                   │
│  compute: lambda                │
│                                 │
│●                        valid ● │  ← target handle (left), source handles (right)
│                       invalid ● │
└─────────────────────────────────┘
```

- Target handles (inputs) on the left edge of the node
- Source handles (outputs) on the right edge, labeled with port names
- Node header shows type icon and module name
- Node body shows key config summary

## Consequences

**Positive:**
- **Unblocks all Phase 2 UI components** — DAG-RENDERER, NODE-PALETTE, EDGE-EDITOR, CONFIG-PANELS, and DATA-PREVIEW can all proceed
- **Native multi-port support** — React Flow handles map directly to Clavesa's named output ports; no custom abstraction needed
- **Controlled state model** — React Flow's controlled mode keeps the visual state synchronized with Pipeline Graph JSON from HCL-PARSER
- **Rich interaction out of the box** — zoom, pan, minimap, keyboard shortcuts, selection, drag-and-drop, undo/redo are built-in or trivially added
- **Active ecosystem** — React Flow has strong documentation, active maintenance, and a large user base; bugs and edge cases are well-covered
- **Composable with React ecosystem** — config panels, data preview tables, and other UI can use any React component library without integration friction

**Negative:**
- **React Flow's rendering model** — all nodes render as React components in the DOM; for very large graphs (100+ nodes) this may require virtualization or canvas-based rendering. The 20-node acceptance criterion is well within limits, but future scaling may need attention.
- **Layout is external** — dagre integration is manual (compute positions, apply to nodes). Layout changes in dagre or React Flow could require maintenance. No position persistence means layout is always recomputed, but dagre is fast enough for pipeline-sized graphs.
- **React Flow updates** — v11 → v12 was a breaking change (new npm scope, API changes). Future major versions could require migration effort.

**Tradeoffs accepted:**
- React Flow's DOM-based rendering over canvas/WebGL in exchange for React component composability and simpler custom node rendering
- Dagre's simpler layout over elkjs's richer algorithms in exchange for smaller bundle size and sufficient quality for pipeline DAGs
- Dependency on a third-party library for a core feature in exchange for not building a DAG editor from scratch
