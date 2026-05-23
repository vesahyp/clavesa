/**
 * DashboardEditor — in-UI authoring for a dashboard.
 *
 * Holds a draft copy of the spec, edits datasets and widgets through the
 * side panels, and shows a live preview grid. Save writes the draft to
 * the `dashboards` system table (POST for a new dashboard, PUT to
 * replace) so it is immediately shared with everyone on the workspace.
 */

import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
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
import { DatasetPanel } from "./DatasetPanel";
import { EditorGrid } from "./EditorGrid";
import { WidgetEditor } from "./WidgetEditor";

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

  const [title, setTitle] = useState(initial.title);
  const [datasets, setDatasets] = useState<DashboardDataset[]>(initial.datasets);
  const [widgets, setWidgets] = useState<DashboardWidget[]>(initial.widgets);
  const [controls, setControls] = useState<DashboardControl[]>(initial.controls);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scrollToId, setScrollToId] = useState<string | undefined>();
  // A new dashboard POSTs on its first save; every save after that (and
  // every save of an existing dashboard) is a PUT — the editor stays
  // open across saves, so this can't stay tied to the `isNew` prop.
  const [creating, setCreating] = useState(isNew);

  // Datasets that reference `{{name}}` placeholders need substituted
  // values for the column probe; synthesize them from the current
  // controls' defaults so the editor's field pickers populate without
  // a 400 from /api/dashboards/query.
  const datasetProbeParams = useMemo(
    () => resolveControlDefaults(controls),
    [controls],
  );
  const columnsByDataset = useDatasetColumns(datasets, datasetProbeParams);

  async function save() {
    setSaving(true);
    setError(null);
    const input = { slug: initial.slug, title, datasets, widgets, controls };
    try {
      if (creating) {
        await createDashboard(input);
        setCreating(false);
      } else {
        await saveDashboard(initial.slug, input);
      }
      await qc.invalidateQueries({ queryKey: ["dashboards"] });
      toast.success("Dashboard saved");
      onSaved();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-6">
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
        <div className="flex gap-2">
          <Button variant="ghost" onClick={onCancel} disabled={saving}>
            Cancel
          </Button>
          <Button onClick={save} disabled={saving}>
            {saving && <Loader2 className="mr-1 h-4 w-4 animate-spin" />}
            Save
          </Button>
        </div>
      </div>

      {error && (
        <Card>
          <CardContent className="p-4 text-sm text-destructive">
            {error}
          </CardContent>
        </Card>
      )}

      {/* Datasets, controls, and widgets are separate tabs — a
          dashboard with many widgets is a long scroll otherwise. */}
      <Tabs defaultValue="datasets">
        <TabsList>
          <TabsTrigger value="datasets">
            Datasets · {datasets.length}
          </TabsTrigger>
          <TabsTrigger value="controls">
            Controls · {controls.length}
          </TabsTrigger>
          <TabsTrigger value="widgets">
            Widgets · {widgets.length}
          </TabsTrigger>
        </TabsList>

        <TabsContent value="datasets" className="mt-4">
          <DatasetPanel
            datasets={datasets}
            pipelines={pipelines.data ?? []}
            onChange={setDatasets}
          />
        </TabsContent>

        <TabsContent value="controls" className="mt-4">
          <ControlsPanel
            controls={controls}
            pipelines={pipelines.data ?? []}
            onChange={setControls}
          />
        </TabsContent>

        <TabsContent value="widgets" className="mt-4 space-y-6">
          <WidgetEditor
            widgets={widgets}
            datasets={datasets}
            columnsByDataset={columnsByDataset}
            onChange={setWidgets}
            onWidgetAdded={setScrollToId}
          />

          <section className="space-y-3">
            <h2 className="font-mono text-sm font-semibold uppercase tracking-wide text-muted-foreground">
              Layout
            </h2>
            {widgets.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                Add a widget to see the layout.
              </p>
            ) : (
              <p className="text-xs text-muted-foreground">
                Drag a widget by its handle to move it; drag the bottom-right
                corner to resize.
              </p>
            )}
            {widgets.length > 0 && (
              <EditorGrid
                widgets={widgets}
                datasets={datasets}
                onChange={setWidgets}
                scrollToId={scrollToId}
              />
            )}
          </section>
        </TabsContent>
      </Tabs>
    </div>
  );
}
