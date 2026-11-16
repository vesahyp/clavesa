/**
 * HostedSettings — the hosted read-only UI block of clavesa.json
 * (ADR-032). Twin of `clavesa workspace hosted enable|disable|show`
 * (ADR-015). Lives in the AppShell header beside the Terraform backend
 * control and opens a dialog. Read-only mode shows the status only.
 *
 * Changing the block writes clavesa.json and hosted.tf; the reader role
 * itself is created or removed by the next `clavesa deploy`.
 */

import { useState } from "react";
import { Globe, Loader2 } from "lucide-react";
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
import { useReadOnly } from "@/hooks/useReadOnly";
import { cn } from "@/lib/utils";
import { disableHosted, enableHosted, useHosted } from "@/lib/queries";

export function HostedSettings() {
  const readOnly = useReadOnly();
  const qc = useQueryClient();
  const hosted = useHosted();
  const [open, setOpen] = useState(false);
  const [editing, setEditing] = useState(false);
  const [url, setUrl] = useState("");
  // "" lets the server pick the kind (ADR-032): cloud for a workspace
  // with a remote backend or a deployed stack, else the warehouse setting.
  const [kind, setKind] = useState<"" | "cloud" | "local">("");
  const [principals, setPrincipals] = useState("");
  const [busArn, setBusArn] = useState("");
  const [changed, setChanged] = useState<"" | "enabled" | "disabled">("");

  const enableMutation = useMutation({
    mutationFn: enableHosted,
    onSuccess: (res) => {
      qc.setQueryData(["workspace", "hosted"], res);
      toast.success("Hosted UI recorded");
      setChanged("enabled");
      setEditing(false);
    },
    onError: (err) =>
      toast.error(err instanceof Error ? err.message : "Failed to enable"),
  });
  const disableMutation = useMutation({
    mutationFn: disableHosted,
    onSuccess: (res) => {
      qc.setQueryData(["workspace", "hosted"], res);
      toast.success("Hosted UI removed");
      setChanged("disabled");
    },
    onError: (err) =>
      toast.error(err instanceof Error ? err.message : "Failed to disable"),
  });

  const info = hosted.data;
  const block = info?.hosted ?? null;
  const busy = enableMutation.isPending || disableMutation.isPending;
  const principalList = principals
    .split(/[\s,]+/)
    .map((p) => p.trim())
    .filter(Boolean);

  // Read-only with nothing recorded: nothing to show.
  if (readOnly && !block) return null;

  function startEdit() {
    setUrl(block?.url ?? "");
    setKind(block?.kind === "cloud" || block?.kind === "local" ? block.kind : "");
    // The first principal is the hosted UI's own (from its well-known
    // document); the field holds only the extras.
    setPrincipals((block?.reader_principals ?? []).slice(1).join("\n"));
    setBusArn(block?.event_bus_arn ?? "");
    setEditing(true);
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        if (!busy) {
          setOpen(o);
          if (!o) setEditing(false);
        }
      }}
    >
      <button
        type="button"
        data-testid="hosted-settings"
        onClick={() => {
          setChanged("");
          setOpen(true);
        }}
        className={cn(
          "flex h-7 items-center gap-1.5 rounded-md px-2 text-xs text-muted-foreground",
          "hover:bg-accent hover:text-foreground",
        )}
        title="Hosted read-only UI"
      >
        <Globe className="h-3.5 w-3.5 shrink-0" />
        <span className="font-mono" data-testid="hosted-status">
          {block ? "hosted" : "not hosted"}
        </span>
      </button>

      <DialogContent aria-label="Hosted UI">
        <DialogHeader>
          <DialogTitle>Hosted UI</DialogTitle>
          <DialogDescription>
            A read-only copy of this workspace served at a hosted URL
            (ADR-032).
          </DialogDescription>
        </DialogHeader>

        {!editing && (
          <div className="grid gap-3">
            <div className="rounded-md border border-border bg-muted/40 p-3 text-xs">
              {block ? (
                <dl className="grid gap-1.5" data-testid="hosted-summary">
                  <div>
                    <dt className="text-muted-foreground">URL</dt>
                    <dd className="font-mono">{block.url}</dd>
                  </div>
                  {info?.kind === "local" ? (
                    <>
                      <div>
                        <dt className="text-muted-foreground">Kind</dt>
                        <dd>Local warehouse: every local pipeline run publishes the summary</dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Publisher role</dt>
                        <dd className="break-all font-mono">{block.publisher_role_arn}</dd>
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Summary</dt>
                        <dd className="break-all font-mono">{info.summary_uri}</dd>
                      </div>
                    </>
                  ) : (
                    <>
                      <div>
                        <dt className="text-muted-foreground">Reader principals</dt>
                        {block.reader_principals.map((p) => (
                          <dd key={p} className="break-all font-mono">
                            {p}
                          </dd>
                        ))}
                      </div>
                      <div>
                        <dt className="text-muted-foreground">Event bus</dt>
                        <dd className="break-all font-mono">
                          {block.event_bus_arn || "none"}
                        </dd>
                      </div>
                    </>
                  )}
                </dl>
              ) : (
                <p data-testid="hosted-summary">Not hosted.</p>
              )}
            </div>
            {changed && (
              <p className="text-xs text-muted-foreground" data-testid="hosted-note">
                {changed === "enabled"
                  ? info?.kind === "local"
                    ? "The next local pipeline run publishes the summary."
                    : "The next `clavesa deploy` creates the reader role."
                  : "The next `clavesa deploy` removes the reader role."}
              </p>
            )}
            {!readOnly && (
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  data-testid="hosted-enable"
                  onClick={startEdit}
                >
                  {block ? "Change" : "Enable"}
                </Button>
                {block && (
                  <Button
                    variant="outline"
                    size="sm"
                    data-testid="hosted-disable"
                    disabled={busy}
                    onClick={() => disableMutation.mutate()}
                  >
                    {disableMutation.isPending && (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    )}
                    Disable
                  </Button>
                )}
              </div>
            )}
          </div>
        )}

        {editing && !readOnly && (
          <div className="grid gap-3.5">
            <div className="space-y-1.5">
              <Label htmlFor="hosted-url">Hosted URL</Label>
              <Input
                id="hosted-url"
                data-testid="hosted-url-input"
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="https://ui.example.com"
                className="font-mono text-xs"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="hosted-kind">Kind</Label>
              <select
                id="hosted-kind"
                data-testid="hosted-kind-input"
                value={kind}
                onChange={(e) => setKind(e.target.value as "" | "cloud" | "local")}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-xs"
              >
                <option value="">
                  Automatic (cloud when the workspace has a remote backend or is deployed)
                </option>
                <option value="cloud">Cloud: the hosted UI reads through a reader role</option>
                <option value="local">Local: each local run publishes the summary</option>
              </select>
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="hosted-principal">
                Extra reader principal ARNs (optional, one per line)
              </Label>
              <textarea
                id="hosted-principal"
                data-testid="hosted-principal-input"
                value={principals}
                onChange={(e) => setPrincipals(e.target.value)}
                rows={3}
                placeholder="arn:aws:iam::123456789012:user/you"
                className="w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="hosted-bus">
                Event bus ARN (optional; default: the hosted UI's)
              </Label>
              <Input
                id="hosted-bus"
                data-testid="hosted-bus-input"
                value={busArn}
                onChange={(e) => setBusArn(e.target.value)}
                placeholder="arn:aws:events:eu-north-1:123456789012:event-bus/clavesa-ui"
                className="font-mono text-xs"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              The hosted UI's own principal and event bus are read from
              its /.well-known/clavesa.json. This records the block only.
              The next `clavesa deploy` creates the reader role.
            </p>
          </div>
        )}

        <DialogFooter>
          {editing ? (
            <>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setEditing(false)}
                disabled={busy}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                data-testid="hosted-save"
                disabled={!url || busy}
                onClick={() =>
                  enableMutation.mutate({
                    url,
                    kind: kind || undefined,
                    readerPrincipals: principalList,
                    eventBusArn: busArn.trim() || undefined,
                  })
                }
              >
                {enableMutation.isPending && (
                  <Loader2 className="h-3 w-3 animate-spin" />
                )}
                Save
              </Button>
            </>
          ) : (
            <Button variant="outline" size="sm" onClick={() => setOpen(false)}>
              Close
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
