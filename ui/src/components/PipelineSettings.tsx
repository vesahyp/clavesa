/**
 * PipelineSettings — modal for editing pipeline-level Terraform variables.
 *
 * Reads variable declarations from variables.tf and current values from
 * terraform.tfvars via GET /pipeline/vars, lets the user edit them, and
 * writes back via PUT /pipeline/vars.
 */

import { useEffect, useState } from "react";
import { Loader2 } from "lucide-react";

import { BASE_URL } from "../api/client";
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

interface PipelineSettingsProps {
  dir: string;
  onClose: () => void;
}

export function PipelineSettings({ dir, onClose }: PipelineSettingsProps) {
  const [vars, setVars] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`${BASE_URL}/pipeline/vars?dir=${encodeURIComponent(dir)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.json() as Promise<Record<string, string>>;
      })
      .then((data) => {
        setVars(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : String(err));
        setLoading(false);
      });
  }, [dir]);

  async function handleSave() {
    setSaving(true);
    setError(null);
    try {
      const res = await fetch(`${BASE_URL}/pipeline/vars`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dir, vars }),
      });
      if (!res.ok) {
        const text = await res.text().catch(() => res.statusText);
        throw new Error(text);
      }
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open onOpenChange={(open) => { if (!open) onClose(); }}>
      <DialogContent
        className="max-h-[80vh] w-[440px] max-w-[90vw] overflow-hidden p-0"
        aria-label="Pipeline settings"
      >
        <DialogHeader className="border-b border-border px-5 py-4">
          <DialogTitle>Pipeline Settings</DialogTitle>
          <DialogDescription className="font-mono text-xs">{dir}</DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          {loading && (
            <div className="flex items-center justify-center gap-2 py-5 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading variables…
            </div>
          )}

          {!loading && Object.keys(vars).length === 0 && (
            <div className="text-sm text-muted-foreground">
              No variable declarations found in variables.tf.
            </div>
          )}

          {!loading &&
            Object.entries(vars).map(([key, value]) => (
              <div key={key} className="mb-3.5 space-y-1.5">
                <Label
                  htmlFor={`var-${key}`}
                  className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground"
                >
                  {key}
                </Label>
                <Input
                  id={`var-${key}`}
                  value={value}
                  onChange={(e) =>
                    setVars((prev) => ({ ...prev, [key]: e.target.value }))
                  }
                  className="font-mono text-xs"
                />
              </div>
            ))}

          {error && (
            <div
              role="alert"
              className="mt-2 rounded-md border border-status-failed/40 bg-status-failed/10 p-2 text-xs text-status-failed"
            >
              {error}
            </div>
          )}
        </div>

        <DialogFooter className="border-t border-border px-5 py-3">
          <Button onClick={onClose} variant="outline" size="sm">
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={saving || loading} size="sm">
            {saving ? (
              <>
                <Loader2 className="h-3 w-3 animate-spin" />
                Saving…
              </>
            ) : (
              "Save"
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
