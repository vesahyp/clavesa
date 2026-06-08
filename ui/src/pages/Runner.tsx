/**
 * Runner — extra Python pip dependencies for this workspace's transform
 * runner image.
 *
 * Edits a single requirements.txt baked into the runner image at build time,
 * for use in UDFs. Standard pip format; `#` comments allowed. The parsed
 * preview shows which lines are recognized as packages vs comments/blanks.
 */

import { useEffect, useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Package } from "lucide-react";
import { toast } from "sonner";

import { useChrome } from "@/components/PageChrome";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import {
  setRunnerRequirements,
  useRunnerRequirements,
} from "@/lib/queries";

export function Runner() {
  const reqs = useRunnerRequirements();
  const qc = useQueryClient();

  const [content, setContent] = useState("");
  const [saving, setSaving] = useState(false);

  // Seed the editor from the server once data lands. Re-seed whenever the
  // server-side content changes (e.g. after a successful save invalidation).
  const serverContent = reqs.data?.content;
  useEffect(() => {
    if (serverContent !== undefined) setContent(serverContent);
  }, [serverContent]);

  const dirty = serverContent !== undefined && content !== serverContent;
  const packages = reqs.data?.requirements ?? [];

  useChrome(
    useMemo(
      () => ({
        breadcrumbs: [{ label: "Runner", to: "/runner" }],
      }),
      [],
    ),
  );

  async function save() {
    setSaving(true);
    try {
      const stored = await setRunnerRequirements(content);
      void qc.invalidateQueries({ queryKey: ["runner-requirements"] });
      toast.success(
        stored.requirements.length === 1
          ? "Saved 1 package"
          : `Saved ${stored.requirements.length} packages`,
      );
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8">
      <div className="mb-6">
        <h1 className="font-mono text-2xl font-semibold tracking-tight">
          Runner
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Extra Python pip dependencies installed into this workspace's
          transform runner image, for use in UDFs.
        </p>
      </div>

      {reqs.isLoading && (
        <div className="space-y-3">
          <Skeleton className="h-48 w-full" />
        </div>
      )}

      {reqs.error && (
        <Card>
          <CardContent className="p-6 text-sm text-destructive">
            Couldn't load runner requirements —{" "}
            {reqs.error instanceof Error
              ? reqs.error.message
              : "unknown error"}
          </CardContent>
        </Card>
      )}

      {reqs.data && (
        <Card>
          <CardContent className="space-y-4 p-6">
            <div className="space-y-2">
              <label
                htmlFor="runner-requirements"
                className="text-sm font-medium"
              >
                requirements.txt
              </label>
              <Textarea
                id="runner-requirements"
                value={content}
                onChange={(e) => setContent(e.target.value)}
                disabled={saving}
                spellCheck={false}
                rows={12}
                className="min-h-48 font-mono text-xs"
                placeholder={"# one package per line, pip format\npyasn>=1.6\ncrawlerdetect>=0.3"}
              />
              <p className="text-xs text-muted-foreground">
                Standard pip requirements.txt format — one per line,{" "}
                <code className="rounded bg-muted px-1 py-0.5 font-mono">
                  #
                </code>{" "}
                comments allowed.
              </p>
            </div>

            <div className="space-y-2">
              <div className="flex items-center gap-2 text-sm font-medium">
                <Package className="h-4 w-4 text-muted-foreground" />
                {packages.length === 1
                  ? "1 package"
                  : `${packages.length} packages`}
              </div>
              {packages.length > 0 ? (
                <div className="flex flex-wrap gap-1.5">
                  {packages.map((p) => (
                    <Badge key={p} variant="secondary" className="font-mono">
                      {p}
                    </Badge>
                  ))}
                </div>
              ) : (
                <p className="text-xs text-muted-foreground">
                  No packages recognized yet. Comment-only and blank lines
                  are ignored.
                </p>
              )}
            </div>

            <div className="flex items-center justify-between border-t border-border pt-4">
              <p className="text-xs text-muted-foreground">
                Changes apply on the next runner build —{" "}
                <code className="rounded bg-muted px-1 py-0.5 font-mono">
                  clavesa pipeline run
                </code>{" "}
                locally, or{" "}
                <code className="rounded bg-muted px-1 py-0.5 font-mono">
                  clavesa workspace deploy
                </code>{" "}
                for cloud.
              </p>
              <Button onClick={save} disabled={saving || !dirty}>
                {saving ? "Saving…" : "Save"}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
