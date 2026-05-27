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
import { useLocation, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { Pencil } from "lucide-react";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import {
  ControlStrip,
  useDashboardParams,
} from "@/components/dashboards/ControlStrip";
import { DashboardEditor } from "@/components/dashboards/DashboardEditor";
import { TemplatePicker } from "@/components/dashboards/TemplatePicker";
import {
  buildTemplate,
  type TemplateId,
} from "@/components/dashboards/templates";
import { Widget } from "@/components/dashboards/Widget";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  useDashboard,
  usePipelines,
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
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  // `?new=1` opens the editor on a blank dashboard; the fetch is skipped
  // since the slug does not exist on the server yet.
  const isNew = searchParams.get("new") === "1";
  const newTitle = searchParams.get("title") || slug;
  const templateParam = searchParams.get("template") as TemplateId | null;
  const dashboard = useDashboard(isNew ? "" : slug);
  const pipelines = usePipelines();
  // Optional prefilled spec from upstream callers — e.g. TableDetail's
  // "Create dashboard" button seeds an explore-this-table widget.
  // Router state survives until the user navigates away; capturing it
  // into editor state once at open keeps it stable through re-renders.
  const location = useLocation();
  const prefilled =
    (location.state as { prefilled?: DashboardSpec } | null)?.prefilled ?? null;
  // The editor's starting spec, captured when the editor opens so a later
  // re-fetch (e.g. after Save strips `?new=1`) can't yank it away.
  //
  // For new dashboards there are three intent signals, in priority order:
  //   - router state.prefilled (TableDetail's "Create dashboard" path)
  //   - ?template=<id> on the URL (after the picker writes it)
  //   - none of the above → editor stays closed until the picker fires
  //     (handled below via `editorInitial === null` while isNew).
  const initialForNew = useMemo<DashboardSpec | null>(() => {
    if (!isNew) return null;
    if (prefilled) return prefilled;
    if (templateParam) {
      const dir = pipelines.data?.[0]?.dir ?? "";
      return buildTemplate(templateParam, slug, newTitle, dir);
    }
    return null;
    // pipelines.data isn't a dep deliberately — the template is computed
    // once at open; the user can pick a dir from the drawer afterwards.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isNew, prefilled, templateParam, slug, newTitle]);
  const [editorInitial, setEditorInitial] = useState<DashboardSpec | null>(
    initialForNew,
  );
  const [editing, setEditing] = useState(isNew && initialForNew !== null);
  // Picker shows when we have a fresh-new dashboard with no signal yet
  // about what shape to use. Picking writes ?template=<id>, builds the
  // spec, and opens the editor in one step.
  const showPicker = isNew && !editorInitial;

  function onTemplatePick(id: TemplateId) {
    const dir = pipelines.data?.[0]?.dir ?? "";
    const spec = buildTemplate(id, slug, newTitle, dir);
    setEditorInitial(spec);
    setEditing(true);
    // Stamp the choice on the URL so a reload during editing rebuilds
    // the same spec rather than re-prompting the picker.
    const sp = new URLSearchParams(searchParams);
    sp.set("template", id);
    setSearchParams(sp, { replace: true });
  }
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
      <TemplatePicker
        open={showPicker}
        onOpenChange={(o) => {
          // Picker dismissal (X / Escape / outside-click) on a fresh
          // ?new=1 entry isn't a real "cancel" choice — there's no
          // editor to fall back to. Route home so the user isn't stuck
          // looking at a blank page.
          if (!o && showPicker) navigate("/dashboards", { replace: true });
        }}
        onPick={onTemplatePick}
      />
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
      ) : showPicker ? (
        // Picker holds the foreground; the viewer underneath is empty
        // (no dashboard exists yet) so we render nothing rather than a
        // 404 hint that would flash through the dialog backdrop.
        null
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
