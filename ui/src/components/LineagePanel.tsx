/**
 * LineagePanel — upstream + downstream neighbors for one Iceberg table.
 *
 * Reads pipeline edges from /api/pipeline/lineage and partitions them into
 * "upstream of this table" (edges where this node is the consumer) and
 * "downstream of this table" (edges where this node is the producer).
 *
 * Identification keys:
 *  - downstream: edges whose `via_table === <database>.<tableName>`. The
 *    server pre-formats `via_table` so the UI doesn't have to know how
 *    auto-table names get composed (sanitization, output_key, etc.).
 *  - upstream: edges whose to_node matches this table's owning_node. We
 *    can't filter by `via_table` here because the upstream might be a
 *    source (no via_table) and we still want to surface it.
 *
 * The component is intentionally dumb about routing — it renders link
 * stubs as "<NavLink to=…>" and lets TableDetail control which target
 * pages exist. Source-typed upstreams aren't catalog tables, so they
 * render as a non-link <code> chip with the source node id.
 */

import { useMemo } from "react";
import { NavLink } from "react-router-dom";
import { ArrowRight, Database } from "lucide-react";

import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useLineage, type LineageEdge } from "@/lib/queries";

interface LineagePanelProps {
  /** Pipeline directory the table belongs to (relative to workspace root). */
  dir: string;
  /** Glue/Hadoop catalog database — `clavesa_<sanitized-pipeline>`. */
  database: string;
  /** Catalog table name — `<sanitized-node>__<output_key>`. */
  table: string;
  /** Sanitized node id — used to match upstream edges' to_node. Empty
   *  when the catalog response doesn't surface owning_node (defensive
   *  fallback to a no-render). */
  owningNode: string;
}

export function LineagePanel({ dir, database, table, owningNode }: LineagePanelProps) {
  const lineage = useLineage(dir);

  const fullTable = `${database}.${table}`;
  const { upstream, downstream } = useMemo(() => {
    const edges = lineage.data?.edges ?? [];
    return {
      // Edges where THIS node is the consumer — its producers are upstream.
      upstream: edges.filter((e) => e.to_node === owningNode),
      // Edges where THIS table is the via_table — its consumers are
      // downstream. via_table is the catalog identifier so we can match
      // exactly against `<database>.<table>` without re-deriving the
      // sanitization rules client-side.
      downstream: edges.filter((e) => e.via_table === fullTable),
    };
  }, [lineage.data, owningNode, fullTable]);

  if (!dir) {
    // Lineage requires a workspace dir to anchor the .tf parse. Cloud
    // tables that the server couldn't match to a workspace pipeline
    // arrive with empty `dir`; render nothing rather than a misleading
    // "no upstream" empty state.
    return null;
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle>Lineage</CardTitle>
      </CardHeader>
      <CardContent>
        {lineage.isLoading && (
          <div className="space-y-2">
            <Skeleton className="h-4 w-2/3" />
            <Skeleton className="h-4 w-1/2" />
          </div>
        )}
        {lineage.error && (
          <p className="text-sm text-muted-foreground">
            Couldn't load lineage —{" "}
            {lineage.error instanceof Error ? lineage.error.message : "unknown error"}
          </p>
        )}
        {lineage.data && (
          <div className="grid gap-6 sm:grid-cols-2">
            <LineageColumn
              heading="Upstream"
              caption="Tables and sources this one reads from"
              edges={upstream}
              direction="upstream"
              database={database}
            />
            <LineageColumn
              heading="Downstream"
              caption="Tables that consume this one"
              edges={downstream}
              direction="downstream"
              database={database}
            />
          </div>
        )}
      </CardContent>
    </Card>
  );
}

interface LineageColumnProps {
  heading: string;
  caption: string;
  edges: LineageEdge[];
  direction: "upstream" | "downstream";
  database: string;
}

function LineageColumn({ heading, caption, edges, direction, database }: LineageColumnProps) {
  return (
    <div>
      <div className="mb-2 flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
        <ArrowRight
          className={`h-3 w-3 ${direction === "upstream" ? "rotate-180" : ""}`}
        />
        <span>{heading}</span>
      </div>
      {edges.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          {direction === "upstream"
            ? "No upstream — this table reads only from sources outside the catalog."
            : "No downstream — nothing in this pipeline consumes this table yet."}
        </p>
      ) : (
        <ul className="space-y-1.5">
          {edges.map((e, i) => (
            <li key={`${e.from_node}->${e.to_node}-${i}`}>
              <LineageEdgeRow edge={e} direction={direction} database={database} />
            </li>
          ))}
        </ul>
      )}
      {edges.length > 0 && (
        <p className="mt-3 text-[11px] text-muted-foreground">{caption}</p>
      )}
    </div>
  );
}

interface LineageEdgeRowProps {
  edge: LineageEdge;
  direction: "upstream" | "downstream";
  database: string;
}

function LineageEdgeRow({ edge, direction, database }: LineageEdgeRowProps) {
  // For upstream edges the "other side" is the producer; for downstream
  // it's the consumer. We compute (a) what to label the row with and (b)
  // whether the row links to a catalog table or is a non-link chip.
  const isUpstream = direction === "upstream";
  const otherType = isUpstream ? edge.from_type : edge.to_type;
  const otherNode = isUpstream ? edge.from_node : edge.to_node;
  // Cross-pipeline edges (ADR-016 slice 2) carry from_pipeline /
  // to_pipeline. When set, the other node lives in a different pipeline
  // than the one being viewed — render distinctly + label.
  const otherPipeline = isUpstream ? edge.from_pipeline : edge.to_pipeline;
  const isCrossPipeline = Boolean(otherPipeline);

  // Linking rules:
  //  - upstream + via_table set → link to the producer's table (via_table
  //    is exactly that, server-formatted to dodge sanitization in the UI).
  //  - downstream + consumer is a transform → link to the consumer's
  //    auto-table, composed from the current database + the consumer
  //    node's sanitized id with the implicit "default" output key. The
  //    server doesn't surface the consumer's table id today; deriving it
  //    here keeps the response shape minimal.
  //  - source upstream or destination downstream → non-link chip with
  //    the node id.
  let linkTarget: string | null = null;
  if (isUpstream && edge.via_table) {
    linkTarget = toTableRoute(edge.via_table);
  } else if (!isUpstream && edge.to_type === "transform") {
    // Cross-pipeline downstream rows carry the consumer's own
    // table id on `to_table` — the consumer lives in another
    // pipeline and uses a different Glue DB than the one we're
    // viewing. Intra-pipeline rows derive locally from `database`.
    if (edge.to_table) {
      linkTarget = toTableRoute(edge.to_table);
    } else {
      const consumerTable = `${sanitize(otherNode)}__default`;
      linkTarget = buildTableRoute(database, consumerTable);
    }
  }

  // External (unresolved) cross-pipeline edges are tagged
  // FromPipeline="(external)" by the server — render as a non-link chip
  // since we can't navigate to a producer we can't find.
  const isExternal = otherPipeline === "(external)";

  if (linkTarget && !isExternal) {
    return (
      <NavLink
        to={linkTarget}
        className={
          "group flex items-center gap-2 rounded-md border border-transparent px-2 py-1 hover:border-border hover:bg-muted/50" +
          (isCrossPipeline ? " border-l-2 border-l-indigo-500/60 pl-2.5" : "")
        }
      >
        <Database className="h-3.5 w-3.5 flex-shrink-0 text-muted-foreground group-hover:text-primary" />
        <code className="truncate font-mono text-xs">
          {isUpstream
            ? edge.via_table
            : edge.to_table || `${database}.${sanitize(otherNode)}__default`}
        </code>
        <span className="ml-auto flex flex-shrink-0 items-center gap-1.5 whitespace-nowrap text-[10px] text-muted-foreground">
          {isCrossPipeline && (
            <span className="rounded bg-indigo-500/10 px-1 py-0.5 font-mono text-indigo-700 dark:text-indigo-300">
              {otherPipeline}
            </span>
          )}
          <span>via {otherNode}</span>
        </span>
      </NavLink>
    );
  }

  // Source upstream, destination downstream, or external (unresolved)
  // cross-pipeline upstream — non-link chip.
  return (
    <div
      className={
        "flex items-center gap-2 rounded-md px-2 py-1" +
        (isCrossPipeline ? " border-l-2 border-l-indigo-500/60 pl-2.5" : "")
      }
    >
      <Database className="h-3.5 w-3.5 flex-shrink-0 text-muted-foreground" />
      <code className="truncate font-mono text-xs">{otherNode}</code>
      <span className="ml-auto flex flex-shrink-0 items-center gap-1.5 whitespace-nowrap text-[10px] uppercase text-muted-foreground">
        {isCrossPipeline && (
          <span className="rounded bg-indigo-500/10 px-1 py-0.5 font-mono normal-case text-indigo-700 dark:text-indigo-300">
            {otherPipeline}
          </span>
        )}
        {otherType}
      </span>
    </div>
  );
}

// toTableRoute splits "clavesa_demo__schema.xform__default" into the
// ADR-016 three-level URL /tables/<catalog>/<schema>/<table>. Catalog
// DBs can in principle contain dots in exotic pipelines (rare, but
// legal in HCL), so we split on the LAST dot to separate db from
// table.
function toTableRoute(viaTable: string): string {
  const lastDot = viaTable.lastIndexOf(".");
  if (lastDot < 0) return "/";
  const db = viaTable.slice(0, lastDot);
  const table = viaTable.slice(lastDot + 1);
  return buildTableRoute(db, table);
}

// buildTableRoute turns an encoded Glue DB (`<catalog>__<schema>`) plus
// a table name into the three-level ADR-016 URL. Legacy DBs without
// the `__` boundary marker get a `(default)` schema segment so the
// router still has three pieces to match against — TableDetail then
// reconstructs the original DB name from catalog+schema.
function buildTableRoute(db: string, table: string): string {
  const i = db.indexOf("__");
  const catalog = i >= 0 ? db.slice(0, i) : db;
  const schema = i >= 0 ? db.slice(i + 2) : "";
  return `/tables/${encodeURIComponent(catalog)}/${encodeURIComponent(schema)}/${encodeURIComponent(table)}`;
}

// sanitize mirrors the runner's pipeline_safe / table_safe transformation:
// dashes → underscores, so `<sanitize(node)>__default` matches the auto-
// table name the runner produces. Keep in sync with
// internal/service/lineage.go:sanitizeNodeForTable.
function sanitize(s: string): string {
  return s.replace(/-/g, "_");
}
