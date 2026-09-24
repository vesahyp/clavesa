/**
 * TerraformBackendControl — workspace-wide Terraform state backend
 * (ADR-025). Lives in the persistent AppShell header next to the AWS
 * profile switcher and the warehouse toggle.
 *
 * Shows "local" or `s3://bucket/prefix`. Opens a dialog with three
 * things the CLI's `workspace set-backend` / `--clear` /
 * `migrate-state` also do, through the same service calls (ADR-015):
 *   - set the backend (bucket + region, optional key prefix)
 *   - clear it (refused once any stack has migrated — the server 409s)
 *   - migrate state onto it, behind an explicit confirm step, then show
 *     the per-stack result rows exactly like the CLI's table
 *
 * Configuring the backend never touches Terraform state by itself —
 * only "Migrate state" does, which is why it gets its own confirm step
 * that says so before running.
 */

import { useState } from "react";
import { Database, Loader2 } from "lucide-react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import {
  clearBackend,
  migrateBackend,
  setBackend,
  useBackend,
  type MigrateResult,
} from "@/lib/queries";

type View = "summary" | "edit" | "confirm-migrate" | "migrate-result";

function backendLabel(bucket: string, keyPrefix: string): string {
  return `s3://${bucket}/${keyPrefix}`;
}

export function TerraformBackendControl() {
  const [open, setOpen] = useState(false);
  const [view, setView] = useState<View>("summary");
  const [bucket, setBucketInput] = useState("");
  const [region, setRegionInput] = useState("");
  const [keyPrefix, setKeyPrefixInput] = useState("");
  const [migrateResult, setMigrateResult] = useState<MigrateResult | null>(
    null,
  );

  const qc = useQueryClient();
  const backend = useBackend();

  const setMutation = useMutation({
    mutationFn: setBackend,
    onSuccess: (res) => {
      qc.setQueryData(["workspace", "backend"], res);
      toast.success(`Recorded ${backendLabel(res.bucket, res.key_prefix)}`);
      setView("summary");
    },
    onError: (err) => {
      toast.error(err instanceof Error ? err.message : "Failed to set backend");
    },
  });

  const clearMutation = useMutation({
    mutationFn: clearBackend,
    onSuccess: (res) => {
      qc.setQueryData(["workspace", "backend"], res);
      toast.success("Cleared — state is local again");
    },
    onError: (err) => {
      toast.error(
        err instanceof Error ? err.message : "Failed to clear backend",
      );
    },
  });

  const migrateMutation = useMutation({
    mutationFn: migrateBackend,
    onSuccess: (res) => {
      setMigrateResult(res);
      setView("migrate-result");
      if (!res.error) {
        toast.success("Migration complete");
      } else {
        toast.error(res.error);
      }
    },
    onError: (err) => {
      toast.error(
        err instanceof Error ? err.message : "Failed to run migrate-state",
      );
      setView("summary");
    },
  });

  const info = backend.data;
  const label = info?.configured
    ? backendLabel(info.bucket, info.key_prefix)
    : "local";

  function openDialog() {
    setView("summary");
    setMigrateResult(null);
    setOpen(true);
  }

  function startEdit() {
    setBucketInput(info?.bucket ?? "");
    setRegionInput(info?.region ?? "");
    setKeyPrefixInput(info?.configured ? info.key_prefix : "");
    setView("edit");
  }

  const busy = setMutation.isPending || clearMutation.isPending || migrateMutation.isPending;

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        if (!busy) setOpen(o);
      }}
    >
      <button
        type="button"
        data-testid="terraform-backend-control"
        onClick={openDialog}
        className={cn(
          "flex h-7 items-center gap-1.5 rounded-md px-2 text-xs text-muted-foreground",
          "hover:bg-accent hover:text-foreground",
        )}
        title="Terraform state backend"
      >
        <Database className="h-3.5 w-3.5 shrink-0" />
        <span className="font-mono" data-testid="terraform-backend-status">
          {label}
        </span>
      </button>

      <DialogContent aria-label="Terraform state backend">
        <DialogHeader>
          <DialogTitle>Terraform state</DialogTitle>
          <DialogDescription>
            Where this workspace&apos;s Terraform state lives (ADR-025). Local
            state is a file per stack in the workspace directory; a remote
            backend is a versioned, locked S3 bucket every developer shares.
          </DialogDescription>
        </DialogHeader>

        {view === "summary" && (
          <div className="grid gap-4">
            <div className="rounded-md border border-border bg-muted/40 p-3">
              <p className="text-xs font-medium text-muted-foreground">
                Current backend
              </p>
              <p
                className="mt-1 font-mono text-sm"
                data-testid="terraform-backend-summary"
              >
                {label}
              </p>
              {info?.configured && (
                <p className="mt-1 text-xs text-muted-foreground">
                  region {info.region}
                </p>
              )}
            </div>

            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                size="sm"
                data-testid="terraform-backend-edit-button"
                onClick={startEdit}
              >
                {info?.configured ? "Change" : "Set backend"}
              </Button>
              {info?.configured && (
                <>
                  <Button
                    variant="outline"
                    size="sm"
                    data-testid="terraform-backend-clear-button"
                    disabled={clearMutation.isPending}
                    onClick={() => clearMutation.mutate()}
                  >
                    {clearMutation.isPending && (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    )}
                    Clear
                  </Button>
                  <Button
                    size="sm"
                    data-testid="terraform-backend-migrate-button"
                    onClick={() => setView("confirm-migrate")}
                  >
                    Migrate state
                  </Button>
                </>
              )}
            </div>
          </div>
        )}

        {view === "edit" && (
          <div className="grid gap-3.5">
            <div className="space-y-1.5">
              <Label htmlFor="tf-backend-bucket">S3 bucket</Label>
              <Input
                id="tf-backend-bucket"
                data-testid="terraform-backend-bucket-input"
                value={bucket}
                onChange={(e) => setBucketInput(e.target.value)}
                placeholder="my-workspace-tfstate"
                className="font-mono text-xs"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="tf-backend-region">AWS region</Label>
              <Input
                id="tf-backend-region"
                data-testid="terraform-backend-region-input"
                value={region}
                onChange={(e) => setRegionInput(e.target.value)}
                placeholder="eu-north-1"
                className="font-mono text-xs"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="tf-backend-key-prefix">
                Key prefix (optional)
              </Label>
              <Input
                id="tf-backend-key-prefix"
                data-testid="terraform-backend-key-prefix-input"
                value={keyPrefix}
                onChange={(e) => setKeyPrefixInput(e.target.value)}
                placeholder="clavesa/"
                className="font-mono text-xs"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              The bucket must already exist, with versioning and default
              encryption enabled. This only records the backend — nothing
              deploys and no state moves until you run Migrate state.
            </p>
          </div>
        )}

        {view === "confirm-migrate" && (
          <div className="grid gap-3">
            <p
              className="text-sm"
              data-testid="terraform-backend-migrate-warning"
            >
              This moves the workspace&apos;s live Terraform state — and every
              pipeline&apos;s — onto{" "}
              <span className="font-mono">
                {info?.configured ? backendLabel(info.bucket, info.key_prefix) : ""}
              </span>
              . Nothing is destroyed or recreated, and the pre-migration
              local state is kept on disk as{" "}
              <span className="font-mono">terraform.tfstate.pre-migrate</span>{" "}
              so a botched run can be recovered by hand. Every stack is
              planned (never applied) afterward to confirm nothing changed.
            </p>
          </div>
        )}

        {view === "migrate-result" && migrateResult && (
          <div className="grid gap-3">
            {migrateResult.error && (
              <p
                role="alert"
                className="rounded-md border border-status-failed/40 bg-status-failed/10 p-2 text-xs text-status-failed"
              >
                {migrateResult.error}
              </p>
            )}
            <div
              className="max-h-64 overflow-y-auto rounded-md border border-border"
              data-testid="terraform-backend-migrate-result"
            >
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Dir</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Plan</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {migrateResult.stacks.map((s) => (
                    <TableRow
                      key={s.dir}
                      data-testid={`terraform-backend-migrate-row-${s.dir === "." ? "root" : s.dir}`}
                    >
                      <TableCell className="font-mono text-xs">
                        {s.dir}
                      </TableCell>
                      <TableCell className="text-xs">{s.status}</TableCell>
                      <TableCell className="text-xs">
                        {s.plan || "—"}
                        {s.err && (
                          <span className="block text-status-failed">
                            {s.err}
                          </span>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        )}

        <DialogFooter>
          {view === "summary" && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => setOpen(false)}
            >
              Close
            </Button>
          )}
          {view === "edit" && (
            <>
              <Button
                variant="outline"
                size="sm"
                data-testid="terraform-backend-cancel-button"
                onClick={() => setView("summary")}
                disabled={setMutation.isPending}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                data-testid="terraform-backend-save-button"
                disabled={!bucket || !region || setMutation.isPending}
                onClick={() =>
                  setMutation.mutate({
                    bucket,
                    region,
                    keyPrefix: keyPrefix || undefined,
                  })
                }
              >
                {setMutation.isPending && (
                  <Loader2 className="h-3 w-3 animate-spin" />
                )}
                Save
              </Button>
            </>
          )}
          {view === "confirm-migrate" && (
            <>
              <Button
                variant="outline"
                size="sm"
                data-testid="terraform-backend-migrate-cancel-button"
                onClick={() => setView("summary")}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                data-testid="terraform-backend-migrate-confirm-button"
                disabled={migrateMutation.isPending}
                onClick={() => migrateMutation.mutate()}
              >
                {migrateMutation.isPending && (
                  <Loader2 className="h-3 w-3 animate-spin" />
                )}
                {migrateMutation.isPending ? "Migrating…" : "Migrate now"}
              </Button>
            </>
          )}
          {view === "migrate-result" && (
            <Button
              variant="outline"
              size="sm"
              data-testid="terraform-backend-migrate-close-button"
              onClick={() => setOpen(false)}
            >
              Close
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
