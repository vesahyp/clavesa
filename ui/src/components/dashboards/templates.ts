/**
 * Dashboard templates — starting specs shown by the `?new=1` picker.
 *
 * Three shapes proven useful enough on first encounter that the picker
 * earns its keep: a scoreboard of headline numbers, a line + bar combo
 * for trends-and-categories, a top-N table for "what are the biggest
 * things." Each is intentionally small — placeholder SQL the author
 * fills in immediately. The widgets pre-bind to a shared dataset
 * named `default` so renaming or editing the SQL once updates every
 * widget, which is the point of starting from a template.
 *
 * `Blank` is the escape hatch the picker also exposes — it just
 * returns the same empty Dashboard the editor used to default to,
 * so users who don't want a template aren't slowed down.
 */

import type { Dashboard, DashboardWidget } from "@/lib/queries";

export type TemplateId = "blank" | "scoreboard" | "line_bar" | "top_n";

export interface TemplateInfo {
  id: TemplateId;
  label: string;
  hint: string;
}

export const TEMPLATES: TemplateInfo[] = [
  {
    id: "blank",
    label: "Blank",
    hint: "Start empty. Add widgets one by one.",
  },
  {
    id: "scoreboard",
    label: "Scoreboard",
    hint: "Four headline metrics in a row.",
  },
  {
    id: "line_bar",
    label: "Line + bar",
    hint: "A trend and a category breakdown, side by side.",
  },
  {
    id: "top_n",
    label: "Top-N table",
    hint: "A single ranked table, full width.",
  },
];

function emptyWidget(
  id: string,
  type: DashboardWidget["type"],
  layout: DashboardWidget["layout"],
  title: string,
): DashboardWidget {
  return {
    id,
    type,
    title,
    dataset: "default",
    value_field: "",
    x_field: "",
    y_field: "",
    series_fields: [],
    line_field: "",
    region_field: "",
    tooltip_field: "",
    layout,
  };
}

/**
 * buildTemplate — materialise a Dashboard spec from a template id.
 *
 * `dir` is the pipeline the dataset dispatches against. If the workspace
 * has any pipelines, the caller passes the first; otherwise empty and
 * the author picks via the drawer's pipeline selector. The shared
 * dataset is named `default` so the sidebar shows one entry the user
 * can edit; every widget binds to it.
 */
export function buildTemplate(
  id: TemplateId,
  slug: string,
  title: string,
  dir: string,
): Dashboard {
  const datasets =
    id === "blank"
      ? []
      : [{ name: "default", dir, sql: "SELECT 1 AS n" }];
  const widgets: DashboardWidget[] = (() => {
    switch (id) {
      case "blank":
        return [];
      case "scoreboard":
        return [0, 1, 2, 3].map((i) =>
          emptyWidget(
            `metric${i + 1}`,
            "big_number",
            { x: i * 3, y: 0, w: 3, h: 2 },
            `Metric ${i + 1}`,
          ),
        );
      case "line_bar":
        return [
          emptyWidget(
            "trend",
            "line",
            { x: 0, y: 0, w: 6, h: 5 },
            "Trend",
          ),
          emptyWidget(
            "by_category",
            "bar",
            { x: 6, y: 0, w: 6, h: 5 },
            "By category",
          ),
        ];
      case "top_n":
        return [
          emptyWidget(
            "ranked",
            "table",
            { x: 0, y: 0, w: 12, h: 8 },
            "Top results",
          ),
        ];
    }
  })();
  return {
    slug,
    title,
    datasets,
    widgets,
    controls: [],
    updated_at: "",
  };
}
