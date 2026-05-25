/**
 * Notebooks — workspace registry of multi-cell SQL + PySpark .ipynb files.
 *
 * Slice 1 of the notebooks feature. Mirrors the Sources / Credentials page
 * shape (list + inline create + delete) since notebooks are a workspace-level
 * registry like sources are; the editor itself lives at /notebooks/:name.
 */

import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { BookOpen, Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";

import { useChrome } from "@/components/PageChrome";
import { ListSearch } from "@/components/ListSearch";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  createNotebook,
  deleteNotebook,
  useNotebooks,
  type NotebookSummary,
} from "@/lib/queries";

export function Notebooks() {
  const list = useNotebooks();
  const qc = useQueryClient();
  const [showForm, setShowForm] = useState(false);

  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const allNotebooks = list.data?.notebooks ?? [];
  const filtered = useMemo(() => {
    if (!q) return allNotebooks;
    return allNotebooks.filter((nb) => nb.name.toLowerCase().includes(q));
  }, [allNotebooks, q]);

  useChrome(
    useMemo(
      () => ({
        breadcrumbs: [{ label: "Notebooks", to: "/notebooks" }],
        trailing: (
          <Button
            size="sm"
            variant={showForm ? "secondary" : "default"}
            onClick={() => setShowForm((v) => !v)}
          >
            <Plus className="mr-1 h-4 w-4" />
            {showForm ? "Cancel" : "New notebook"}
          </Button>
        ),
      }),
      [showForm],
    ),
  );

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
      <div className="mb-6">
        <h1 className="font-mono text-2xl font-semibold tracking-tight">
          Notebooks
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Multi-cell SQL + PySpark notebooks (Jupyter <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">.ipynb</code>),
          stored in <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">notebooks/</code>{" "}
          so GitHub renders them natively. Cells run against the warm Spark
          Connect container with per-notebook session isolation — temp views
          and Python globals persist across cells in the same notebook.
        </p>
      </div>

      {showForm && (
        <CreateForm
          onDone={() => {
            setShowForm(false);
            void qc.invalidateQueries({ queryKey: ["notebooks"] });
          }}
        />
      )}

      {list.isLoading && (
        <div className="space-y-3">
          <Skeleton className="h-16 w-full" />
          <Skeleton className="h-16 w-full" />
        </div>
      )}

      {list.error && (
        <Card>
          <CardContent className="p-6 text-sm text-destructive">
            Couldn't load notebooks —{" "}
            {list.error instanceof Error
              ? list.error.message
              : "unknown error"}
          </CardContent>
        </Card>
      )}

      {list.data && list.data.notebooks.length === 0 && !showForm && (
        <EmptyState onAdd={() => setShowForm(true)} />
      )}

      {list.data && list.data.notebooks.length > 0 && (
        <div className="mb-4 flex items-center gap-3">
          <ListSearch
            value={query}
            onChange={setQuery}
            placeholder="Filter notebooks…"
          />
          {q && (
            <span className="text-xs text-muted-foreground">
              <span className="font-semibold text-foreground">
                {filtered.length}
              </span>{" "}
              of {allNotebooks.length}
            </span>
          )}
        </div>
      )}

      {list.data && list.data.notebooks.length > 0 && filtered.length === 0 && (
        <Card>
          <CardContent className="py-10 text-center text-sm text-muted-foreground">
            No notebooks match{" "}
            <span className="font-mono text-foreground">{query}</span>.
          </CardContent>
        </Card>
      )}

      {filtered.length > 0 && (
        <ul className="grid gap-3">
          {filtered.map((nb) => (
            <NotebookRow
              key={nb.name}
              summary={nb}
              onDeleted={() =>
                qc.invalidateQueries({ queryKey: ["notebooks"] })
              }
            />
          ))}
        </ul>
      )}
    </div>
  );
}

function CreateForm({ onDone }: { onDone: () => void }) {
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit() {
    if (!name) return;
    setBusy(true);
    try {
      const nb = await createNotebook(name);
      toast.success(`Created notebook ${nb.name}`);
      onDone();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card className="mb-6">
      <CardContent className="space-y-3 p-6">
        <Input
          placeholder="name (a-z, 0-9, - _)"
          value={name}
          onChange={(e) => setName(e.target.value)}
          disabled={busy}
          onKeyDown={(e) => {
            if (e.key === "Enter") void submit();
          }}
        />
        <div className="flex justify-end gap-2">
          <Button variant="ghost" onClick={onDone} disabled={busy}>
            Cancel
          </Button>
          <Button onClick={submit} disabled={!name || busy}>
            Create
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function NotebookRow({
  summary,
  onDeleted,
}: {
  summary: NotebookSummary;
  onDeleted: () => void;
}) {
  const [confirming, setConfirming] = useState(false);

  async function doDelete() {
    try {
      await deleteNotebook(summary.name);
      toast.success(`Deleted ${summary.name}`);
      onDeleted();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <li>
      <Card>
        <CardContent className="flex items-center gap-4 p-4">
          <BookOpen className="h-5 w-5 shrink-0 text-muted-foreground" />
          <div className="min-w-0 flex-1">
            <Link
              to={`/notebooks/${encodeURIComponent(summary.name)}`}
              className="font-mono text-sm font-semibold hover:underline"
            >
              {summary.name}
            </Link>
            <p className="mt-0.5 text-xs text-muted-foreground">
              {summary.cell_count} cell{summary.cell_count === 1 ? "" : "s"}
              {" · updated "}
              {summary.updated_at}
            </p>
          </div>
          {confirming ? (
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                variant="destructive"
                onClick={doDelete}
              >
                Confirm delete
              </Button>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => setConfirming(false)}
              >
                Cancel
              </Button>
            </div>
          ) : (
            <Button
              size="icon"
              variant="ghost"
              onClick={() => setConfirming(true)}
              title="Delete notebook"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          )}
        </CardContent>
      </Card>
    </li>
  );
}

function EmptyState({ onAdd }: { onAdd: () => void }) {
  return (
    <Card>
      <CardContent className="py-10 text-center">
        <BookOpen className="mx-auto mb-3 h-10 w-10 text-muted-foreground" />
        <h3 className="font-mono text-sm font-semibold">No notebooks yet</h3>
        <p className="mx-auto mt-1 max-w-md text-sm text-muted-foreground">
          A notebook is a multi-cell scratchpad — SQL and PySpark cells share
          one SparkSession with persistent Python globals, like Databricks.
        </p>
        <Button className="mt-4" onClick={onAdd}>
          <Plus className="mr-1 h-4 w-4" />
          New notebook
        </Button>
      </CardContent>
    </Card>
  );
}
