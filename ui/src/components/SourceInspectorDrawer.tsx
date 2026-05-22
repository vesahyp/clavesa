/**
 * SourceInspectorDrawer — read-only inspector for the synthetic source
 * and external-table nodes that appear in the pipeline DAG.
 *
 * The DAG synthesises one node per `sources.<name>` reference and per
 * `<schema>.<table>` external reference; they aren't editable like
 * transforms (their configuration lives in the workspace source registry
 * or in another pipeline). Clicking one previously did nothing — this
 * drawer answers the obvious question: "what does this source produce?"
 *
 * For a registered source it shows: kind, location, format, and the
 * columns inferred from a one-row preview. For an external table it
 * resolves the `<schema>.<table>` ref against the workspace catalog and
 * surfaces the schema + a deep link to the table detail page.
 */

import { useEffect, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ArrowUpRight, Database, X } from "lucide-react";

import { getRegistrySourcePreview } from "@/api/data";
import { useCatalogTables, useSources } from "@/lib/queries";
import { Badge } from "@/components/ui/badge";

export interface SourceInspectorDrawerProps {
  /** Synthetic node id from the DAG: `source:<name>` or `external:<schema>.<table>`. */
  nodeId: string;
  onClose: () => void;
}

export function SourceInspectorDrawer({
  nodeId,
  onClose,
}: SourceInspectorDrawerProps) {
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const kind = nodeId.startsWith("source:")
    ? "source"
    : nodeId.startsWith("external:")
      ? "external"
      : "unknown";
  const name = nodeId.split(":").slice(1).join(":");

  return (
    <aside className="fixed bottom-0 right-0 top-14 z-40 flex w-[400px] flex-col border-l border-border bg-card shadow-xl">
      <div className="flex items-start justify-between gap-3 border-b border-border px-4 py-3">
        <div className="min-w-0">
          <div className="truncate font-mono text-sm font-semibold">
            {name}
          </div>
          <div className="mt-0.5 flex items-center gap-2">
            <Badge variant="outline" className="text-[9px] uppercase">
              {kind === "source" ? "source" : "external table"}
            </Badge>
          </div>
        </div>
        <button
          onClick={onClose}
          aria-label="Close"
          className="shrink-0 rounded-md p-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {kind === "source" ? (
          <RegistrySourceBody name={name} />
        ) : kind === "external" ? (
          <ExternalTableBody ref={name} />
        ) : (
          <div className="px-4 py-3 text-xs text-muted-foreground">
            Unknown node kind.
          </div>
        )}
      </div>
    </aside>
  );
}

function Section({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="border-b border-border px-4 py-3">
      <div className="mb-1.5 text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </div>
      {children}
    </div>
  );
}

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-0.5 text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="text-right font-mono text-foreground">{value}</span>
    </div>
  );
}

function RegistrySourceBody({ name }: { name: string }) {
  const sources = useSources();
  const spec = sources.data?.sources.find((s) => s.name === name);

  // One-row preview to infer columns. Independent of pipeline state — the
  // registry source is global to the workspace. Skipped on http sources
  // that reference credentials (preview path doesn't carry those yet —
  // matches CLI `source preview` behavior).
  const preview = useQuery({
    queryKey: ["source-preview", name],
    queryFn: () => getRegistrySourcePreview(name, 0, 1),
    enabled: !!name && !!spec && !spec.credentials,
    staleTime: 60_000,
    retry: 0,
  });

  const columns = useMemo(() => {
    const items = preview.data?.items ?? [];
    if (items.length === 0) return [];
    return Object.keys(items[0] as Record<string, unknown>).sort();
  }, [preview.data]);

  if (sources.isLoading) {
    return (
      <div className="px-4 py-3 text-xs text-muted-foreground">Loading…</div>
    );
  }
  if (!spec) {
    return (
      <div className="px-4 py-3 text-xs text-muted-foreground">
        Source <code className="font-mono">{name}</code> is not registered in
        this workspace. It may have been deleted; the reference in the
        pipeline .tf is dangling.
      </div>
    );
  }

  const location = spec.kind === "s3"
    ? `s3://${spec.bucket ?? ""}/${spec.prefix ?? ""}`
    : (spec.url ?? "—");

  return (
    <>
      <Section label="Source">
        <Field label="Kind" value={spec.kind} />
        <Field label="Format" value={spec.format || "—"} />
        <Field label="Location" value={<span className="truncate">{location}</span>} />
        {spec.credentials && (
          <Field label="Credential" value={spec.credentials} />
        )}
        {spec.partitions && spec.partitions.length > 0 && (
          <Field label="Partitions" value={spec.partitions.join(", ")} />
        )}
      </Section>

      <Section label={`Columns${columns.length > 0 ? ` · ${columns.length}` : ""}`}>
        {spec.credentials ? (
          <span className="text-xs text-muted-foreground">
            Preview is unavailable for credential-backed sources. Columns
            resolve at run time.
          </span>
        ) : preview.isLoading ? (
          <span className="text-xs text-muted-foreground">Sampling…</span>
        ) : preview.error ? (
          <span className="text-xs text-muted-foreground">
            Could not sample: {preview.error instanceof Error ? preview.error.message : String(preview.error)}
          </span>
        ) : columns.length === 0 ? (
          <span className="text-xs text-muted-foreground">
            No rows returned — column list unavailable.
          </span>
        ) : (
          <ul className="space-y-0.5">
            {columns.map((c) => (
              <li
                key={c}
                className="flex items-center gap-2 font-mono text-xs"
              >
                <Database className="h-3 w-3 text-muted-foreground" />
                {c}
              </li>
            ))}
          </ul>
        )}
      </Section>

      <Section label="Use it">
        <p className="text-xs text-muted-foreground">
          Click a transform node and use the <strong>+ Add input</strong>
          {" "}button to attach this source.
        </p>
      </Section>
    </>
  );
}

function ExternalTableBody({ ref }: { ref: string }) {
  const catalog = useCatalogTables();

  // Resolve the schema.table reference against the workspace catalog so
  // we can show the producing pipeline + a deep link.
  const match = useMemo(() => {
    const dot = ref.indexOf(".");
    if (dot <= 0) return null;
    const schema = ref.slice(0, dot);
    const table = ref.slice(dot + 1);
    for (const t of catalog.data?.tables ?? []) {
      if (!t.database.includes("__")) continue;
      const sch = t.database.slice(t.database.indexOf("__") + 2);
      if (sch === schema && t.name === table) return t;
    }
    return null;
  }, [catalog.data, ref]);

  return (
    <>
      <Section label="Reference">
        <Field label="Schema.table" value={ref} />
        {match?.owning_pipeline && (
          <Field label="Produced by" value={match.owning_pipeline} />
        )}
      </Section>

      <Section label={`Columns${match?.columns?.length ? ` · ${match.columns.length}` : ""}`}>
        {catalog.isLoading ? (
          <span className="text-xs text-muted-foreground">Loading…</span>
        ) : !match ? (
          <span className="text-xs text-muted-foreground">
            Table <code className="font-mono">{ref}</code> is not in the
            workspace catalog yet. It will resolve once the producing
            pipeline runs.
          </span>
        ) : !match.columns || match.columns.length === 0 ? (
          <span className="text-xs text-muted-foreground">
            No columns recorded.
          </span>
        ) : (
          <ul className="space-y-0.5">
            {match.columns.map((c) => (
              <li
                key={c.name}
                className="flex items-center gap-2 font-mono text-xs"
              >
                <Database className="h-3 w-3 text-muted-foreground" />
                <span>{c.name}</span>
                <span className="text-muted-foreground">{c.type}</span>
              </li>
            ))}
          </ul>
        )}
      </Section>

      {match && (
        <div className="border-t border-border p-3">
          <Link
            to={tableHref(match.database, match.name)}
            className="flex items-center justify-center gap-1.5 rounded-md border border-border py-2 text-xs font-medium text-foreground transition-colors hover:bg-muted"
          >
            Open table
            <ArrowUpRight className="h-3.5 w-3.5" />
          </Link>
        </div>
      )}
    </>
  );
}

// Catalog tables are addressed by the synthetic `<catalog>__<schema>`
// database segment (Glue flat namespace). Split it back into the
// three-level path the TableDetail route expects.
function tableHref(database: string, name: string): string {
  const i = database.indexOf("__");
  const catalog = i >= 0 ? database.slice(0, i) : database;
  const schema = i >= 0 ? database.slice(i + 2) : "";
  return `/tables/${encodeURIComponent(catalog)}/${encodeURIComponent(schema)}/${encodeURIComponent(name)}`;
}

export default SourceInspectorDrawer;
