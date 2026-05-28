/**
 * DashboardEditor — chart-first authoring surface.
 *
 * Holds a draft copy of the spec and lays out one page rather than three
 * tabs: title row + Save, a live ControlStrip (the same component the
 * viewer renders), a datasets sidebar listing user-named datasets, and
 * the interactive EditorGrid. Clicking a widget opens the WidgetDrawer
 * with that widget's full configuration (data binding, field mapping,
 * layout) in one place. Selection is URL-synced via `?widget=<id>` so
 * deep links work and the back button does the intuitive thing.
 *
 * Editor-only concepts (the inline-dataset prefix, selection chrome,
 * draft state) never leak into the viewer's components: `Widget.tsx`
 * and `ControlStrip.tsx` are imported and rendered, not modified.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { AlertCircle, CheckCircle2, Loader2, Plus, Settings2 } from "lucide-react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  ControlStrip,
  useDashboardParams,
} from "@/components/dashboards/ControlStrip";
import {
  createDashboard,
  resolveControlDefaults,
  saveDashboard,
  usePipelines,
  type Dashboard,
  type DashboardControl,
  type DashboardDataset,
  type DashboardWidget,
} from "@/lib/queries";

import { useDatasetColumns } from "@/hooks/useDatasetColumns";

import { ControlsPanel } from "./ControlsPanel";
import { DatasetsSidebar } from "./DatasetsSidebar";
import {
  clearDraft,
  loadDraft,
  saveDraft,
  shouldOfferRestore,
} from "./draftStorage";
import { EditorGrid } from "./EditorGrid";
import { validateDraft, type ValidationError } from "./validateDraft";
import {
  WidgetDrawer,
  inlineDatasetName,
  isInlineDataset,
  makeWidget,
} from "./WidgetDrawer";
import { WidgetTypePicker, type WidgetType } from "./WidgetTypePicker";

interface DashboardEditorProps {
  initial: Dashboard;
  /** New dashboards POST (and 409 on a slug clash); existing ones PUT. */
  isNew: boolean;
  /** Called after a successful save — the editor stays open. */
  onSaved: () => void;
  onCancel: () => void;
}

export function DashboardEditor({
  initial,
  isNew,
  onSaved,
  onCancel,
}: DashboardEditorProps) {
  const qc = useQueryClient();
  const pipelines = usePipelines();
  const [searchParams, setSearchParams] = useSearchParams();

  const [title, setTitle] = useState(initial.title);
  const [datasets, setDatasets] = useState<DashboardDataset[]>(initial.datasets);
  const [widgets, setWidgets] = useState<DashboardWidget[]>(initial.widgets);
  const [controls, setControls] = useState<DashboardControl[]>(initial.controls);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [controlsEditorOpen, setControlsEditorOpen] = useState(false);
  // A new dashboard POSTs on its first save; every save after that (and
  // every save of an existing dashboard) is a PUT — the editor stays
  // open across saves, so this can't stay tied to the `isNew` prop.
  const [creating, setCreating] = useState(isNew);

  // Autosave / restore state (Slice D).
  // `lastSavedAt` flips between null and a timestamp to drive the
  // "saving · saved · error" pill in the header.
  const [saveState, setSaveState] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [validationErrors, setValidationErrors] = useState<ValidationError[]>([]);
  // Restore banner: a draft newer than the server's `updated_at`
  // surfaces a Restore / Discard choice on mount. Captured once;
  // dismissing either way clears the offer.
  const [restoreOffer, setRestoreOffer] = useState<Dashboard | null>(null);
  const restoreCheckedRef = useRef(false);
  useEffect(() => {
    if (restoreCheckedRef.current) return;
    restoreCheckedRef.current = true;
    const draft = loadDraft(initial.slug);
    if (!draft) return;
    if (!shouldOfferRestore(draft, initial.updated_at)) {
      // Server is newer — silently discard the stale draft.
      clearDraft(initial.slug);
      return;
    }
    setRestoreOffer(draft.spec);
  }, [initial.slug, initial.updated_at]);

  function acceptRestore() {
    if (!restoreOffer) return;
    setTitle(restoreOffer.title);
    setDatasets(restoreOffer.datasets);
    setWidgets(restoreOffer.widgets);
    setControls(restoreOffer.controls);
    setRestoreOffer(null);
  }
  function discardRestore() {
    clearDraft(initial.slug);
    setRestoreOffer(null);
  }

  // URL-driven selection: `?widget=<id>` opens the drawer for that
  // widget. Form blur can't deselect (only an explicit close action
  // does), and deep links / back-button work the way users expect.
  const selectedWidgetId = searchParams.get("widget");
  const selectedWidget =
    widgets.find((w) => w.id === selectedWidgetId) ?? null;

  function selectWidget(id: string | null) {
    const next = new URLSearchParams(searchParams);
    if (id) next.set("widget", id);
    else next.delete("widget");
    setSearchParams(next, { replace: true });
  }

  // If the selected widget gets removed by some other action, clear the
  // URL so the drawer isn't stuck open on a phantom id.
  useEffect(() => {
    if (selectedWidgetId && !selectedWidget) selectWidget(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedWidgetId, selectedWidget]);

  // Datasets that reference `{{name}}` placeholders need substituted
  // values for the column probe; synthesize them from the current
  // controls' defaults so the editor's field pickers populate without
  // a 400 from /api/dashboards/query.
  const datasetProbeParams = useMemo(
    () => resolveControlDefaults(controls),
    [controls],
  );
  const columnsByDataset = useDatasetColumns(datasets, datasetProbeParams);

  // Same control-resolution the viewer uses, so the strip in the editor
  // behaves identically (URL state + declared defaults).
  const controlParams = useDashboardParams(controls);

  const namedDatasets = useMemo(
    () => datasets.filter((d) => !isInlineDataset(d.name)),
    [datasets],
  );

  // Debounced autosave: every state mutation kicks off a timer; the
  // last edit within the window wins. Restore-offer state means the
  // user hasn't decided yet — don't overwrite the stored draft until
  // they pick Restore or Discard.
  useEffect(() => {
    if (restoreOffer) return;
    const id = window.setTimeout(() => {
      const spec: Dashboard = {
        slug: initial.slug,
        title,
        datasets,
        widgets,
        controls,
        updated_at: initial.updated_at,
      };
      saveDraft(initial.slug, spec);
    }, 1000);
    return () => window.clearTimeout(id);
  }, [
    initial.slug,
    initial.updated_at,
    title,
    datasets,
    widgets,
    controls,
    restoreOffer,
  ]);

  async function save() {
    setSaving(true);
    setSaveState("saving");
    setError(null);
    // Publish-time validate. If the spec has any structural issues,
    // block the POST, surface the list inline, and open the first
    // offending widget's drawer so the user lands on the fix.
    const draftSpec: Dashboard = {
      slug: initial.slug,
      title,
      datasets,
      widgets,
      controls,
      updated_at: initial.updated_at,
    };
    const errors = validateDraft(draftSpec, columnsByDataset);
    if (errors.length > 0) {
      setValidationErrors(errors);
      setSaveState("error");
      setSaving(false);
      // Open the drawer on the first widget that's in trouble so the
      // user sees the section their fix belongs in.
      const first = errors[0];
      if (first.widgetId && first.widgetId !== selectedWidgetId) {
        selectWidget(first.widgetId);
      }
      return;
    }
    setValidationErrors([]);
    const input = { slug: initial.slug, title, datasets, widgets, controls };
    try {
      if (creating) {
        await createDashboard(input);
        setCreating(false);
      } else {
        await saveDashboard(initial.slug, input);
      }
      await qc.invalidateQueries({ queryKey: ["dashboards"] });
      // Clear the autosave draft now that the canonical store has it.
      clearDraft(initial.slug);
      toast.success("Dashboard saved");
      setSaveState("saved");
      onSaved();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setSaveState("error");
    } finally {
      setSaving(false);
    }
  }

  // Clear validation errors when the user edits anything — the
  // outdated complaint shouldn't sit there while they're fixing it.
  useEffect(() => {
    if (validationErrors.length === 0) return;
    setValidationErrors([]);
    if (saveState === "error") setSaveState("idle");
    // intentionally minimal deps — only react to spec edits
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasets, widgets, controls]);

  function addWidget(type: WidgetType) {
    setPickerOpen(false);
    const w = makeWidget(type, widgets);
    setWidgets([...widgets, w]);
    selectWidget(w.id);
  }

  function patchWidget(updated: DashboardWidget) {
    setWidgets(widgets.map((w) => (w.id === updated.id ? updated : w)));
  }

  function duplicateWidget(id: string) {
    const src = widgets.find((w) => w.id === id);
    if (!src) return;
    // Carry the same dataset binding. Inline-bound widgets get a fresh
    // inline dataset so each one is independent.
    const seed = makeWidget(src.type, widgets);
    const copy: DashboardWidget = {
      ...src,
      id: seed.id,
      layout: {
        x: src.layout.x,
        y: src.layout.y + src.layout.h,
        w: src.layout.w,
        h: src.layout.h,
      },
    };
    if (isInlineDataset(src.dataset)) {
      const srcDs = datasets.find((d) => d.name === src.dataset);
      const newInline = inlineDatasetName(copy.id);
      copy.dataset = newInline;
      if (srcDs) {
        setDatasets([
          ...datasets,
          { name: newInline, dir: srcDs.dir, sql: srcDs.sql },
        ]);
      }
    }
    setWidgets([...widgets, copy]);
    selectWidget(copy.id);
  }

  function deleteWidget(id: string) {
    setWidgets(widgets.filter((w) => w.id !== id));
    // Clean up the inline dataset that no other widget references.
    const inline = inlineDatasetName(id);
    setDatasets(datasets.filter((d) => d.name !== inline));
    if (selectedWidgetId === id) selectWidget(null);
  }

  return (
    <div className="space-y-4">
      <div className="flex items-end justify-between gap-4">
        <div className="flex-1 space-y-1">
          <Label className="text-xs">Dashboard title</Label>
          <Input
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            className="max-w-md font-mono text-lg"
          />
          <p className="text-xs text-muted-foreground">
            slug <code className="font-mono">{initial.slug}</code>
          </p>
        </div>
        <div className="flex items-center gap-2">
          <SavePill state={saveState} />
          <Button variant="ghost" onClick={onCancel} disabled={saving}>
            Cancel
          </Button>
          <Button onClick={save} disabled={saving}>
            {saving && <Loader2 className="mr-1 h-4 w-4 animate-spin" />}
            Save
          </Button>
        </div>
      </div>

      {restoreOffer && (
        <Card>
          <CardContent className="flex items-center justify-between gap-3 p-3 text-sm">
            <div>
              <span className="font-medium">Unsaved changes available.</span>
              <span className="ml-2 text-muted-foreground">
                Restore the local autosave (newer than the saved version)?
              </span>
            </div>
            <div className="flex gap-2">
              <Button size="sm" variant="ghost" onClick={discardRestore}>
                Discard
              </Button>
              <Button size="sm" onClick={acceptRestore}>
                Restore
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {validationErrors.length > 0 && (
        <Card>
          <CardContent className="p-3 text-sm">
            <div className="flex items-center gap-2 font-medium text-destructive">
              <AlertCircle className="h-4 w-4" />
              Can't publish — fix {validationErrors.length} issue
              {validationErrors.length === 1 ? "" : "s"}:
            </div>
            <ul className="mt-2 space-y-1 text-xs text-muted-foreground">
              {validationErrors.slice(0, 5).map((e, i) => (
                <li key={i}>
                  <button
                    type="button"
                    className="text-left hover:text-foreground"
                    onClick={() => selectWidget(e.widgetId)}
                  >
                    <code className="font-mono">{e.widgetId}</code> · {e.message}
                  </button>
                </li>
              ))}
              {validationErrors.length > 5 && (
                <li className="text-muted-foreground/70">
                  …{validationErrors.length - 5} more
                </li>
              )}
            </ul>
          </CardContent>
        </Card>
      )}

      {error && (
        <Card>
          <CardContent className="p-4 text-sm text-destructive">
            {error}
          </CardContent>
        </Card>
      )}

      {/* Live controls strip — same component the viewer renders, so
          the editor preview matches the published view. The "Manage
          controls" toggle reveals the per-control editor inline. */}
      <div className="space-y-2">
        <div className="flex items-start gap-2">
          <div className="flex-1">
            {controls.length > 0 ? (
              <ControlStrip controls={controls} params={controlParams} />
            ) : (
              <p className="text-xs text-muted-foreground">
                No controls yet. Add one to let viewers filter the dashboard.
              </p>
            )}
          </div>
          <Button
            size="sm"
            variant="outline"
            onClick={() => setControlsEditorOpen((v) => !v)}
          >
            <Settings2 className="mr-1 h-3.5 w-3.5" />
            {controlsEditorOpen ? "Hide controls" : "Manage controls"}
          </Button>
        </div>
        {controlsEditorOpen && (
          <div className="rounded-md border border-border bg-card p-3">
            <ControlsPanel
              controls={controls}
              pipelines={pipelines.data ?? []}
              onChange={setControls}
            />
          </div>
        )}
      </div>

      {/* Body: sidebar of named datasets + interactive grid. */}
      <div className="grid grid-cols-[14rem_1fr] gap-4">
        <DatasetsSidebar
          datasets={namedDatasets}
          widgets={widgets}
          onAddWidget={() => setPickerOpen(true)}
          onPickDataset={(name) => {
            // Find the first widget bound to this dataset; if none,
            // create a fresh `table` widget that binds to it.
            const bound = widgets.find((w) => w.dataset === name);
            if (bound) {
              selectWidget(bound.id);
              return;
            }
            const w = makeWidget("table", widgets);
            w.dataset = name;
            setWidgets([...widgets, w]);
            selectWidget(w.id);
          }}
        />
        <div className="min-w-0">
          {widgets.length === 0 ? (
            <Card>
              <CardContent className="space-y-3 p-6 text-sm text-muted-foreground">
                <p>No widgets yet. Add one to start.</p>
                <Button size="sm" onClick={() => setPickerOpen(true)}>
                  <Plus className="mr-1 h-3.5 w-3.5" /> Add widget
                </Button>
              </CardContent>
            </Card>
          ) : (
            <EditorGrid
              widgets={widgets}
              datasets={datasets}
              onChange={setWidgets}
              scrollToId={selectedWidgetId ?? undefined}
              selectedId={selectedWidgetId}
              onSelect={selectWidget}
              params={controlParams}
            />
          )}
        </div>
      </div>

      <WidgetTypePicker
        open={pickerOpen}
        onOpenChange={setPickerOpen}
        onPick={addWidget}
      />

      <WidgetDrawer
        widget={selectedWidget}
        datasets={datasets}
        pipelines={pipelines.data ?? []}
        controls={controls}
        columnsByDataset={columnsByDataset}
        onChangeWidget={patchWidget}
        onChangeDatasets={setDatasets}
        onDuplicate={duplicateWidget}
        onDelete={deleteWidget}
        onClose={() => selectWidget(null)}
      />
    </div>
  );
}

/**
 * SavePill — tiny status indicator next to the Save button. Reflects
 * the most recent save attempt: nothing while idle, a spinner during
 * a publish, a check after success, an alert after a failure or a
 * validation-blocked save. Idle state is silent so the chrome stays
 * clean for users who never see the "saved" feedback.
 */
function SavePill({
  state,
}: {
  state: "idle" | "saving" | "saved" | "error";
}) {
  if (state === "idle") return null;
  if (state === "saving") {
    return (
      <span className="flex items-center gap-1 text-xs text-muted-foreground">
        <Loader2 className="h-3 w-3 animate-spin" /> saving…
      </span>
    );
  }
  if (state === "saved") {
    return (
      <span className="flex items-center gap-1 text-xs text-muted-foreground">
        <CheckCircle2 className="h-3 w-3 text-emerald-600" /> saved
      </span>
    );
  }
  return (
    <span className="flex items-center gap-1 text-xs text-destructive">
      <AlertCircle className="h-3 w-3" /> blocked
    </span>
  );
}
