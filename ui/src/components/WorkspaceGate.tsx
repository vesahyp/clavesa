/**
 * WorkspaceGate — first-launch gate around the whole app.
 *
 * `clavesa ui` started in a directory with no clavesa.json serves
 * a valid HTTP API rooted at that directory, but every page (Catalog,
 * Pipelines, Sources…) assumes a workspace exists. This gate queries
 * GET /api/workspace once: if no workspace is there, it renders the
 * create-workspace screen instead of the routes; the "Create workspace"
 * action POSTs to /api/workspace/init — the same code path as
 * `clavesa workspace init`.
 *
 * The server's root path doesn't change on create (the directory just
 * gains a manifest), so the running server's handlers pick the new
 * workspace up with no restart — invalidating the query is enough.
 *
 * Switching to a *different* workspace directory is deliberately out of
 * scope here: the server bakes its root into every handler at startup,
 * so a cross-directory switch needs a restart or a live-root refactor.
 */

import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, FolderPlus } from "lucide-react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { getWorkspace, initWorkspace } from "@/api/workspace";

export function WorkspaceGate({ children }: { children: React.ReactNode }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["workspace"],
    queryFn: getWorkspace,
    staleTime: 60_000,
  });

  if (isLoading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
      </div>
    );
  }

  // A failed /api/workspace probe shouldn't strand the user on a blank
  // screen — fall through to the app, where the per-page error states
  // give a more specific diagnosis.
  if (error || data?.exists) {
    return <>{children}</>;
  }

  return <CreateWorkspaceScreen root={data?.root ?? ""} />;
}

function CreateWorkspaceScreen({ root }: { root: string }) {
  const qc = useQueryClient();
  const [name, setName] = useState("");
  const [catalog, setCatalog] = useState("");
  const [advanced, setAdvanced] = useState(false);
  const [busy, setBusy] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || busy) return;
    setBusy(true);
    try {
      await initWorkspace(trimmed, catalog.trim());
      toast.success(`Workspace "${trimmed}" created`);
      // Root path is unchanged — re-fetching /api/workspace flips
      // `exists` to true and the gate renders the app.
      await qc.invalidateQueries({ queryKey: ["workspace"] });
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-6">
      <Card className="w-full max-w-lg">
        <CardContent className="space-y-6 py-8">
          <div className="flex flex-col items-center gap-3 text-center">
            <span className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
              <FolderPlus className="h-6 w-6" />
            </span>
            <div>
              <div className="text-lg font-semibold">Create a workspace</div>
              <p className="mt-1 text-sm text-muted-foreground">
                This directory has no workspace yet. Clavesa will write{" "}
                <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
                  clavesa.json
                </code>{" "}
                and the workspace Terraform here, then build the local
                runner image for previews.
              </p>
            </div>
          </div>

          <form onSubmit={submit} className="flex flex-col gap-4">
            <label className="flex flex-col gap-1.5 text-sm">
              <span className="font-medium">Workspace name</span>
              <Input
                autoFocus
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="my_project"
                pattern="[a-zA-Z][a-zA-Z0-9_\-]*"
                title="Letters, digits, _ or -; must start with a letter."
                required
                disabled={busy}
                data-testid="workspace-name"
                className="font-mono"
              />
            </label>

            <button
              type="button"
              onClick={() => setAdvanced((v) => !v)}
              disabled={busy}
              className="self-start text-xs text-muted-foreground hover:text-foreground"
            >
              {advanced ? "▾ Advanced" : "▸ Advanced (catalog identifier)"}
            </button>
            {advanced && (
              <label className="flex flex-col gap-1.5 text-sm">
                <span className="font-medium">
                  Catalog{" "}
                  <span className="font-normal text-muted-foreground">
                    (optional)
                  </span>
                </span>
                <Input
                  value={catalog}
                  onChange={(e) => setCatalog(e.target.value)}
                  placeholder="(default: clavesa_<name>)"
                  disabled={busy}
                  className="font-mono"
                />
                <span className="text-xs text-muted-foreground">
                  Top level of the <code>catalog.schema.table</code>{" "}
                  namespace (ADR-016). One catalog per workspace.
                </span>
              </label>
            )}

            {root && (
              <p className="font-mono text-[11px] text-muted-foreground">
                Location: {root}
              </p>
            )}

            <Button
              type="submit"
              disabled={busy || !name.trim()}
              data-testid="create-workspace"
            >
              {busy ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Creating… (building runner image, up to a minute)
                </>
              ) : (
                "Create workspace"
              )}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}

export default WorkspaceGate;
