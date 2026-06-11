/**
 * PipelinesList — workspace's pipelines, secondary navigation now that
 * the data catalog is the front door. Authoring entry point.
 *
 * Replaces the old pages/Home.tsx. Same data (GET /api/pipelines), new
 * tech stack (Tailwind + shadcn + TanStack Query).
 */

import { useNavigate } from "react-router-dom";
import { useMemo, useState } from "react";
import { ArrowUpCircle, FileWarning, Loader2, Plus, Trash2, Workflow } from "lucide-react";
import { toast } from "sonner";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useChrome } from "@/components/PageChrome";
import { Highlight } from "@/components/Highlight";
import { ListSearch } from "@/components/ListSearch";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { usePipelines, useRuns } from "@/lib/queries";
import { createPipeline, deletePipeline, upgradeWorkspace } from "@/api/workspace";
import { formatRelative } from "@/lib/format";
import { cn } from "@/lib/utils";

// How many recent runs to show as status bars per pipeline row.
const RUN_BARS = 6;

// SFN-style run status → bar colour. Local pipelines write the same
// uppercase statuses as cloud (ADR-014), so one mapping covers both.
function runBarColor(status: string): string {
  switch (status) {
    case "SUCCEEDED":
      return "bg-status-success";
    case "FAILED":
    case "TIMED_OUT":
    case "ABORTED":
      return "bg-status-failed";
    case "RUNNING":
      return "bg-status-running";
    default:
      return "bg-muted-foreground/40";
  }
}

/**
 * PipelineRuns — recent run health for one pipeline row.
 *
 * Fetches lazily and per-row so the Pipelines list renders instantly from
 * the cheap /api/pipelines payload; the run bars fill in once the per-
 * pipeline runs query (Spark for local, Athena for cloud) returns.
 */
function PipelineRuns({ name, dir }: { name: string; dir: string }) {
  // Pipeline names land in node_runs/runs as the literal pipeline_name
  // var.tf value — what `pipeline create` writes, dashes preserved.
  // (Confirmed: runs_writer sidecar uses CLAVESA_PIPELINE verbatim, and
  // local recordLocalRun uses filepath.Base(dir).) PipelineDashboard
  // uses the same convention.
  const runs = useRuns(name, { dir, limit: RUN_BARS });

  if (runs.isLoading) {
    return <Skeleton className="h-4 w-28" />;
  }
  const rows = [...(runs.data?.rows ?? [])].sort((a, b) =>
    b.started_at.localeCompare(a.started_at),
  );
  if (runs.error || rows.length === 0) {
    return <span className="text-xs text-muted-foreground">no runs</span>;
  }
  const latest = rows[0];
  // Oldest → newest, left → right.
  const bars = rows.slice(0, RUN_BARS).reverse();
  return (
    <div className="flex items-center gap-2">
      <div className="flex items-center gap-1">
        {bars.map((r) => (
          <span
            key={r.run_id}
            title={`${r.status}${r.started_at ? ` · ${formatRelative(r.started_at)}` : ""}`}
            className={cn("h-4 w-1.5 rounded-sm", runBarColor(r.status))}
          />
        ))}
      </div>
      {latest.started_at && (
        <span className="whitespace-nowrap text-xs text-muted-foreground">
          {formatRelative(latest.started_at)}
        </span>
      )}
    </div>
  );
}

export function PipelinesList() {
  const navigate = useNavigate();
  const qc = useQueryClient();
  const { data, isLoading, error } = usePipelines();
  const [showNew, setShowNew] = useState(false);
  const [busyDir, setBusyDir] = useState<string | null>(null);

  // Free-text filter — matches case-insensitively against pipeline name,
  // directory, schema, and the registered sources each pipeline consumes.
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const filtered = useMemo(() => {
    if (!data) return [];
    if (!q) return data;
    return data.filter((p) =>
      [p.name, p.dir, p.schema, ...p.sources].some((f) =>
        f.toLowerCase().includes(q),
      ),
    );
  }, [data, q]);

  // "Upgrade all" — one-shot workspace upgrade (shell + every pipeline).
  // Empty version => the running CLI's embedded module version.
  const upgradeAll = useMutation({
    mutationFn: () => upgradeWorkspace(),
    onSuccess: (r) => {
      const changed = r.pipelines.filter(
        (p) => p.updated > 0 || p.migrated > 0,
      ).length;
      const unchanged = r.pipelines.length - changed;
      toast.success(
        `Upgraded workspace ${r.prev_version} -> ${r.target_version} · ` +
          `${changed} pipeline${changed === 1 ? "" : "s"} updated, ${unchanged} unchanged`,
      );
      const failed = r.pipelines.filter((p) => p.err);
      if (failed.length > 0) {
        toast.error(
          `Failed: ${failed.map((p) => p.name).join(", ")}`,
        );
      }
      qc.invalidateQueries({ queryKey: ["pipelines"] });
      qc.invalidateQueries({ queryKey: ["module-version"] });
    },
    onError: (e) => toast.error(e instanceof Error ? e.message : String(e)),
  });

  async function handleDelete(dir: string, name: string) {
    if (
      !window.confirm(
        `Delete pipeline "${name}"?\n\n` +
          `This permanently removes ${dir}/ from the workspace, including its terraform state. ` +
          `If the pipeline has been deployed to AWS, run \`clavesa pipeline destroy\` first ` +
          `to tear down cloud resources — otherwise they'll be orphaned.`,
      )
    ) {
      return;
    }
    setBusyDir(dir);
    try {
      await deletePipeline(dir);
      toast.success(`Deleted ${name}`);
      qc.invalidateQueries({ queryKey: ["pipelines"] });
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusyDir(null);
    }
  }

  useChrome(
    useMemo(
      () => ({
        breadcrumbs: [{ label: "Pipelines", to: "/pipelines" }],
        trailing: (
          <div className="flex items-center gap-2">
            <Button
              onClick={() => upgradeAll.mutate()}
              size="sm"
              variant="outline"
              disabled={upgradeAll.isPending}
              title="Upgrade the workspace shell and every pipeline to the running CLI's module version"
            >
              {upgradeAll.isPending ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <ArrowUpCircle className="h-4 w-4" />
              )}
              Upgrade all
            </Button>
            <Button onClick={() => setShowNew(true)} size="sm">
              <Plus className="h-4 w-4" />
              New pipeline
            </Button>
          </div>
        ),
      }),
      [upgradeAll.isPending, upgradeAll],
    ),
  );

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
        <div className="mb-6">
          <h1 className="text-2xl font-semibold tracking-tight">Pipelines</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Authoring view. Open one to edit its DAG, transforms, and config.
          </p>
        </div>

        {data && data.length > 0 && (
          <div className="mb-4 flex items-center gap-3">
            <ListSearch
              value={query}
              onChange={setQuery}
              placeholder="Filter pipelines…"
            />
            {q && (
              <span className="text-xs text-muted-foreground">
                <span className="font-semibold text-foreground">
                  {filtered.length}
                </span>{" "}
                of {data.length}
              </span>
            )}
          </div>
        )}

        {error && (
          <Card className="border-destructive/40 bg-destructive/5">
            <CardHeader className="flex-row items-start gap-3">
              <FileWarning className="mt-0.5 h-5 w-5 text-destructive" />
              <div>
                <CardTitle className="text-destructive">
                  Failed to list pipelines
                </CardTitle>
                <p className="mt-1 text-xs text-muted-foreground">
                  {error instanceof Error ? error.message : String(error)}
                </p>
              </div>
            </CardHeader>
          </Card>
        )}

        {isLoading && (
          <div className="grid gap-3">
            <Skeleton className="h-16 w-full" />
            <Skeleton className="h-16 w-full" />
            <Skeleton className="h-16 w-full" />
          </div>
        )}

        {data && data.length === 0 && (
          <Card className="border-dashed">
            <CardContent className="flex flex-col items-center gap-3 py-12 text-center">
              <span className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
                <Workflow className="h-6 w-6" />
              </span>
              <div className="text-base font-semibold">No pipelines yet</div>
              <p className="max-w-md text-sm text-muted-foreground">
                Click <strong>New pipeline</strong> to scaffold one. Each pipeline
                is a directory of <code>.tf</code> files clavesa manages.
              </p>
              <Button onClick={() => setShowNew(true)} size="sm" variant="outline">
                <Plus className="h-4 w-4" />
                New pipeline
              </Button>
            </CardContent>
          </Card>
        )}

        {data && data.length > 0 && filtered.length === 0 && (
          <Card className="border-dashed">
            <CardContent className="py-10 text-center text-sm text-muted-foreground">
              No pipelines match{" "}
              <span className="font-mono text-foreground">{query}</span>.
            </CardContent>
          </Card>
        )}

        {data && data.length > 0 && filtered.length > 0 && (
          <div className="grid gap-3">
            {filtered.map((p) => (
              <Card
                key={p.dir}
                onClick={() =>
                  // Lands on the per-pipeline dashboard — that's where
                  // the Run pipeline button + run history + table list
                  // live. From there one click takes the user to the
                  // editor when they want to author. README's flow
                  // ("click demo → dashboard, then Open editor")
                  // depends on this routing.
                  navigate(`/pipelines/dashboard?dir=${encodeURIComponent(p.dir)}`)
                }
                className="group cursor-pointer transition-colors hover:border-primary/50"
                data-testid="pipeline-card"
                data-pipeline={p.dir}
              >
                <CardContent className="flex items-center gap-3 p-4">
                  <Workflow className="h-4 w-4 flex-shrink-0 text-muted-foreground" />
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-mono font-medium">
                        <Highlight text={p.name} query={q} />
                      </span>
                      {p.schema && (
                        <span
                          className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground"
                          title="ADR-016 schema this pipeline writes into — one schema per pipeline"
                        >
                          schema: <Highlight text={p.schema} query={q} />
                        </span>
                      )}
                      {p.compute && (
                        <Badge variant="outline" className="font-mono text-[10px]">
                          {p.compute}
                        </Badge>
                      )}
                      <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground">
                        {p.node_count} node{p.node_count === 1 ? "" : "s"}
                      </span>
                      {p.sources.map((s) => (
                        <span
                          key={s}
                          className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground"
                        >
                          <Highlight text={s} query={q} />
                        </span>
                      ))}
                    </div>
                    <code className="break-all font-mono text-xs text-muted-foreground">
                      <Highlight text={p.dir} query={q} />
                    </code>
                  </div>
                  <PipelineRuns name={p.name} dir={p.dir} />
                  <Button
                    variant="ghost"
                    size="icon"
                    className="opacity-0 transition-opacity group-hover:opacity-100 hover:bg-destructive/10 hover:text-destructive"
                    disabled={busyDir === p.dir}
                    aria-label={`Delete pipeline ${p.name}`}
                    data-testid={`delete-pipeline-${p.name}`}
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDelete(p.dir, p.name);
                    }}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </CardContent>
              </Card>
            ))}
          </div>
        )}

      {showNew && <NewPipelineDialog onClose={() => setShowNew(false)} />}
    </div>
  );
}

interface NewPipelineDialogProps {
  onClose: () => void;
}

function NewPipelineDialog({ onClose }: NewPipelineDialogProps) {
  const navigate = useNavigate();
  const qc = useQueryClient();
  const [name, setName] = useState("");
  const [schema, setSchema] = useState("");
  // Schema defaults to the sanitized pipeline name (one schema per
  // pipeline, ADR-016). It is an advanced override — collapsed by
  // default so the common case is just "type a name".
  const [showAdvanced, setShowAdvanced] = useState(false);
  const mut = useMutation({
    mutationFn: async (args: { name: string; schema: string }) =>
      createPipeline(args.name, args.schema),
    onSuccess: (resp) => {
      toast.success(`Created ${resp.dir}`);
      qc.invalidateQueries({ queryKey: ["pipelines"] });
      navigate(`/pipelines/dashboard?dir=${encodeURIComponent(resp.dir)}`);
    },
    onError: (err) => {
      toast.error(err instanceof Error ? err.message : String(err));
    },
  });

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    mut.mutate({ name: trimmed, schema: schema.trim() });
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 backdrop-blur-sm"
      onClick={onClose}
    >
      <Card
        className="w-full max-w-md"
        onClick={(e) => e.stopPropagation()}
      >
        <CardHeader>
          <CardTitle>New pipeline</CardTitle>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="flex flex-col gap-4">
            <label className="flex flex-col gap-1.5 text-sm">
              <span className="font-medium">Name</span>
              <input
                autoFocus
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="my_pipeline"
                pattern="[a-z][a-z0-9_\-]*"
                title="Lowercase letters, digits, _ or -; must start with a letter."
                required
                data-testid="new-pipeline-name"
                className="rounded-md border border-input bg-background px-3 py-2 text-sm font-mono text-foreground outline-none ring-offset-background placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring"
              />
              <span className="text-xs text-muted-foreground">
                Lowercase letters, digits, <code>_</code>, or <code>-</code>.
                Becomes the directory name.
              </span>
            </label>
            {showAdvanced ? (
              <label className="flex flex-col gap-1.5 text-sm">
                <span className="font-medium">Schema</span>
                <input
                  value={schema}
                  onChange={(e) => setSchema(e.target.value)}
                  placeholder="(default: sanitized pipeline name)"
                  pattern="[a-z][a-z0-9_\-]*"
                  title="Lowercase letters, digits, _ or -; must start with a letter."
                  data-testid="new-pipeline-schema"
                  className="rounded-md border border-input bg-background px-3 py-2 text-sm font-mono text-foreground outline-none ring-offset-background placeholder:text-muted-foreground focus-visible:ring-2 focus-visible:ring-ring"
                />
                <span className="text-xs text-muted-foreground">
                  ADR-016 schema (middle level of{" "}
                  <code>catalog.schema.table</code>). Defaults to the
                  sanitized pipeline name — one schema per pipeline. Override
                  only to group several pipelines under one domain.
                </span>
              </label>
            ) : (
              <button
                type="button"
                onClick={() => setShowAdvanced(true)}
                data-testid="new-pipeline-advanced"
                className="self-start text-xs text-muted-foreground hover:text-foreground hover:underline"
              >
                Advanced — set a custom schema
              </button>
            )}
            <div className="flex justify-end gap-2">
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={onClose}
                disabled={mut.isPending}
              >
                Cancel
              </Button>
              <Button
                type="submit"
                size="sm"
                disabled={mut.isPending || !name.trim()}
              >
                {mut.isPending ? "Creating…" : "Create"}
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
