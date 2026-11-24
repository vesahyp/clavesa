/**
 * workspaceNav — the sections of one workspace in the hosted and
 * multi-workspace UI (docs/ideas/hosted-navigation.md). The desktop
 * sidebar and the phone's bottom bar show the same four, in this order,
 * on every page of the workspace; only the active one changes.
 *
 * Every page has one parent: the second-to-last crumb it declares with
 * useChrome. Back on the phone goes there, never further.
 */

import { Activity, Database, LayoutDashboard, Workflow } from "lucide-react";
import type { LucideIcon } from "lucide-react";

export type SectionKey = "overview" | "pipelines" | "catalog" | "dashboards";

export interface Section {
  key: SectionKey;
  label: string;
  to: string;
  icon: LucideIcon;
  match: (pathname: string) => boolean;
}

export const SECTIONS: Section[] = [
  { key: "overview", label: "Overview", to: "/", icon: Activity, match: (p) => p === "/" || p === "/overview" },
  {
    key: "pipelines",
    label: "Pipelines",
    to: "/pipelines",
    icon: Workflow,
    match: (p) => p.startsWith("/pipelines") || p.startsWith("/backfills"),
  },
  {
    key: "catalog",
    label: "Catalog",
    to: "/catalog",
    icon: Database,
    match: (p) => p.startsWith("/catalog") || p.startsWith("/tables"),
  },
  { key: "dashboards", label: "Dashboards", to: "/dashboards", icon: LayoutDashboard, match: (p) => p.startsWith("/dashboards") },
];

export function activeSection(pathname: string): SectionKey | null {
  return SECTIONS.find((s) => s.match(pathname))?.key ?? null;
}
