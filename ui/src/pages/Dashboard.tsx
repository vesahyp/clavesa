/**
 * Dashboard — detail/render page, with an in-UI edit mode.
 *
 * Render mode loads one dashboard by slug and lays its widgets out on a
 * 12-column grid; each widget binds to a named dataset, resolved here to
 * {sql, dir} and handed down (two widgets on one dataset share a single
 * query — the hook caches on `[dir, sql]`). Edit mode swaps in the
 * DashboardEditor. `?new=1` opens the editor on a blank dashboard.
 */

import { useMemo, useState } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";
import { Pencil } from "lucide-react";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import {
  ControlStrip,
  useDashboardParams,
} from "@/components/dashboards/ControlStrip";
import { DashboardEditor } from "@/components/dashboards/DashboardEditor";
import { Widget } from "@/components/dashboards/Widget";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  useDashboard,
  type Dashboard as DashboardSpec,
  type DashboardDataset,
} from "@/lib/queries";

// pipelineScopeLabel summarizes the distinct pipeline dirs a dashboard's
// datasets read from — one dir reads as `demo`, several as `3 pipelines`.
function pipelineScopeLabel(datasets: DashboardDataset[]): string {
  const dirs = Array.from(new Set(datasets.map((d) => d.dir).filter(Boolean)));
  if (dirs.length === 0) return "no pipeline";
  if (dirs.length === 1) return dirs[0];
  return `${dirs.length} pipelines`;
}

export function Dashboard() {
  const params = useParams<{ slug: string }>();
  const slug = decodeURIComponent(params.slug ?? "");
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();

  // `?new=1` opens the editor on a blank dashboard; the fetch is skipped
  // since the slug does not exist on the server yet.
  const isNew = searchParams.get("new") === "1";
  const newTitle = searchParams.get("title") || slug;
  const dashboard = useDashboard(isNew ? "" : slug);
  const [editing, setEditing] = useState(isNew);
  // The editor's starting spec, captured when the editor opens so a later
  // re-fetch (e.g. after Save strips `?new=1`) can't yank it away.
  const [editorInitial, setEditorInitial] = useState<DashboardSpec | null>(
    isNew
      ? {
          slug,
          title: newTitle,
          datasets: [],
          widgets: [],
          controls: [],
          updated_at: "",
        }
      : null,
  );
  // Resolved param map (URL state + declared control defaults).
  // Computed on every render so the first paint of a widget already
  // has the right values — no useEffect race that fires a query with
  // an empty param map and 400s on the missing placeholder.
  const controlParams = useDashboardParams(dashboard.data?.controls ?? []);

  // Resolve widget.dataset → {sql, dir} once per spec change.
  const datasetMap = useMemo(() => {
    const m = new Map<string, DashboardDataset>();
    for (const ds of dashboard.data?.datasets ?? []) {
      m.set(ds.name, ds);
    }
    return m;
  }, [dashboard.data?.datasets]);

  useChrome(
    useMemo<PageChrome>(
      () => ({
        breadcrumbs: [
          { label: "Dashboards", to: "/dashboards" },
          {
            label: dashboard.data?.title || slug,
            to: `/dashboards/${encodeURIComponent(slug)}`,
          },
        ],
      }),
      [slug, dashboard.data?.title],
    ),
  );

  return (
    <div className="mx-auto w-full max-w-7xl px-6 py-8">
      {editing && editorInitial ? (
        <DashboardEditor
          isNew={isNew}
          initial={editorInitial}
          onSaved={() => {
            // Strip `?new=1` so a reload re-fetches the saved spec — but
            // stay in the editor; saving shouldn't kick the user out.
            if (isNew) {
              navigate(`/dashboards/${encodeURIComponent(slug)}`, {
                replace: true,
              });
            }
          }}
          onCancel={() => {
            if (isNew) {
              navigate("/dashboards", { replace: true });
            } else {
              setEditing(false);
            }
          }}
        />
      ) : (
        <>
          {dashboard.isLoading && (
            <div className="mt-2 space-y-3">
              <Skeleton className="h-8 w-1/3" />
              <Skeleton className="h-64 w-full" />
            </div>
          )}

          {dashboard.error && (
            <Card>
              <CardContent className="p-6 text-sm text-destructive">
                {dashboard.error instanceof Error
                  ? dashboard.error.message
                  : String(dashboard.error)}
              </CardContent>
            </Card>
          )}

          {dashboard.data && (
            <>
              <div className="mb-6 flex items-start justify-between gap-4">
                <div>
                  <h1 className="font-mono text-2xl font-semibold tracking-tight">
                    {dashboard.data.title}
                  </h1>
                  {dashboard.data.datasets.length > 0 && (
                    <p className="mt-1 text-sm text-muted-foreground">
                      {dashboard.data.datasets.length} dataset
                      {dashboard.data.datasets.length === 1 ? "" : "s"} over{" "}
                      {pipelineScopeLabel(dashboard.data.datasets)}
                    </p>
                  )}
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    if (dashboard.data) setEditorInitial(dashboard.data);
                    setEditing(true);
                  }}
                >
                  <Pencil className="mr-1 h-3.5 w-3.5" /> Edit
                </Button>
              </div>

              {dashboard.data.controls.length > 0 && (
                <ControlStrip
                  controls={dashboard.data.controls}
                  params={controlParams}
                />
              )}

              {dashboard.data.widgets.length === 0 ? (
                <Card>
                  <CardContent className="p-6 text-sm text-muted-foreground">
                    This dashboard has no widgets yet — click Edit to add some.
                  </CardContent>
                </Card>
              ) : (
                <div
                  className="grid gap-4"
                  style={{
                    gridTemplateColumns: "repeat(12, minmax(0, 1fr))",
                    gridAutoRows: "minmax(80px, auto)",
                  }}
                >
                  {dashboard.data.widgets.map((w) => {
                    const ds = datasetMap.get(w.dataset);
                    return (
                      <Widget
                        key={w.id}
                        widget={w}
                        sql={ds?.sql ?? ""}
                        dir={ds?.dir ?? ""}
                        params={
                          dashboard.data?.controls.length
                            ? controlParams
                            : undefined
                        }
                      />
                    );
                  })}
                </div>
              )}
            </>
          )}
        </>
      )}
    </div>
  );
}
