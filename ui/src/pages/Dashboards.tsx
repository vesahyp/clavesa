/**
 * Dashboards — list page.
 *
 * Workspace's saved dashboards. Each entry links to /dashboards/:slug
 * for the detail/render view. Dashboards live in the `dashboards` system
 * Delta table, shared across everyone with workspace access. "New
 * dashboard" opens the editor on a blank spec.
 */

import { useMemo, useState } from "react";
import { NavLink, useNavigate } from "react-router-dom";
import { LayoutDashboard, ChevronRight, Plus } from "lucide-react";

import { useChrome, type PageChrome } from "@/components/PageChrome";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { slugify } from "@/lib/format";
import { useDashboards } from "@/lib/queries";
import { RegistryList } from "@/pages/RegistryList";

const DASHBOARDS_CHROME: PageChrome = {
  breadcrumbs: [{ label: "Dashboards", to: "/dashboards" }],
};

export function Dashboards() {
  const list = useDashboards();
  useChrome(DASHBOARDS_CHROME);

  // Free-text filter — case-insensitive over title and slug.
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const all = list.data?.dashboards ?? [];
  const filtered = useMemo(() => {
    if (!q) return all;
    return all.filter((d) =>
      [d.title, d.slug].some((f) => f.toLowerCase().includes(q)),
    );
  }, [all, q]);

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
      <div className="mb-6 flex items-start justify-between gap-4">
        <div>
          <h1 className="font-mono text-2xl font-semibold tracking-tight">
            Dashboards
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Saved SQL widgets over your workspace's catalog — stored in the
            shared{" "}
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
              dashboards
            </code>{" "}
            system table.
          </p>
        </div>
        <NewDashboardButton existing={list.data?.dashboards.map((d) => d.slug) ?? []} />
      </div>

      <RegistryList
        query={list}
        items={all}
        filtered={filtered}
        search={query}
        onSearchChange={setQuery}
        searchPlaceholder="Filter dashboards…"
        noun="dashboards"
        skeletonClassName="h-12 w-full"
        empty={<EmptyState />}
        renderItem={(d) => (
          <li key={d.slug}>
            <NavLink
              to={`/dashboards/${encodeURIComponent(d.slug)}`}
              className="group flex items-center gap-3 rounded-md border border-border bg-card px-4 py-3 hover:border-primary/40 hover:bg-muted/30"
            >
              <LayoutDashboard className="h-4 w-4 flex-shrink-0 text-muted-foreground group-hover:text-primary" />
              <div className="min-w-0 flex-1">
                <div className="font-medium">{d.title}</div>
                <code className="font-mono text-xs text-muted-foreground">
                  {d.slug}
                </code>
              </div>
              <ChevronRight className="h-4 w-4 flex-shrink-0 text-muted-foreground" />
            </NavLink>
          </li>
        )}
      />
    </div>
  );
}

function NewDashboardButton({ existing }: { existing: string[] }) {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");

  const slug = slugify(name);
  const clash = existing.includes(slug);
  const valid = slug.length > 0 && !clash;

  function create() {
    if (!valid) return;
    setOpen(false);
    setName("");
    navigate(
      `/dashboards/${encodeURIComponent(slug)}?new=1&title=${encodeURIComponent(name.trim())}`,
    );
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <Button size="sm" onClick={() => setOpen(true)}>
        <Plus className="mr-1 h-3.5 w-3.5" /> New dashboard
      </Button>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>New dashboard</DialogTitle>
        </DialogHeader>
        <div className="space-y-2">
          <Label htmlFor="new-dashboard-name" className="text-xs">
            Name
          </Label>
          <Input
            id="new-dashboard-name"
            value={name}
            autoFocus
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") create();
            }}
            placeholder="Revenue overview"
          />
          {slug.length > 0 && (
            <p className="text-xs text-muted-foreground">
              slug <code className="font-mono">{slug}</code>
              {clash && (
                <span className="text-destructive"> — already exists</span>
              )}
            </p>
          )}
        </div>
        <DialogFooter>
          <Button variant="ghost" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={create} disabled={!valid}>
            Create
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EmptyState() {
  return (
    <Card>
      <CardContent className="flex flex-col items-start gap-3 p-6 text-sm">
        <div className="flex items-center gap-2">
          <LayoutDashboard className="h-5 w-5 text-muted-foreground" />
          <span className="font-medium">No dashboards yet</span>
        </div>
        <p className="text-muted-foreground">
          Click <span className="font-medium text-foreground">New dashboard</span>{" "}
          to build one, or create a pipeline (the{" "}
          <NavLink to="/pipelines" className="text-foreground underline-offset-2 hover:underline">
            Pipelines
          </NavLink>{" "}
          tab) — clavesa seeds a "Pipeline runs" dashboard the first time
          you do.
        </p>
      </CardContent>
    </Card>
  );
}
