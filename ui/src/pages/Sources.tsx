/**
 * Sources — workspace input source registry (ADR-017 slice 1).
 *
 * Lists registered sources with kind/format/url. Inline form to register
 * a new http source by URL. Delete button uses the API's deletion guard
 * (refuses with the consumer list when the source is attached to any
 * pipeline; the UI surfaces the dependency list inline).
 */

import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Database, Eye, Pencil, Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";

import { useChrome } from "@/components/PageChrome";
import { Highlight } from "@/components/Highlight";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { NativeSelect } from "@/components/ui/native-select";
import { Skeleton } from "@/components/ui/skeleton";
import { RegistryList } from "@/pages/RegistryList";
import {
  deleteSource,
  registerSource,
  updateSource,
  useCredentials,
  useSources,
  type SourceSpec,
} from "@/lib/queries";
import { getRegistrySourcePreview, type PreviewResult } from "@/api/data";

export function Sources() {
  const list = useSources();
  const qc = useQueryClient();
  const [showForm, setShowForm] = useState(false);

  // Free-text filter — matches case-insensitively against the source
  // name, kind, format, and its location (url or bucket/prefix).
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const allSources = list.data?.sources ?? [];
  const filtered = useMemo(() => {
    if (!q) return allSources;
    return allSources.filter((s) =>
      [s.name, s.kind, s.format, s.url, s.bucket, s.prefix].some((f) =>
        f.toLowerCase().includes(q),
      ),
    );
  }, [allSources, q]);

  useChrome(
    useMemo(
      () => ({
        breadcrumbs: [{ label: "Sources", to: "/sources" }],
        trailing: (
          <Button
            size="sm"
            variant={showForm ? "secondary" : "default"}
            onClick={() => setShowForm((v) => !v)}
          >
            <Plus className="mr-1 h-4 w-4" />
            {showForm ? "Cancel" : "Register source"}
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
            Sources
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Workspace input registry — every URL or bucket+prefix clavesa
            reads is named here. Pipelines reference sources by name from
            transform inputs (
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
              inputs = {"{ x = \"sources.<name>\" }"}
            </code>
            ).
          </p>
        </div>

        {showForm && (
          <SourceForm
            onDone={() => {
              setShowForm(false);
              void qc.invalidateQueries({ queryKey: ["sources"] });
            }}
            onCancel={() => setShowForm(false)}
          />
        )}

        <RegistryList
          query={list}
          items={allSources}
          filtered={filtered}
          search={query}
          onSearchChange={setQuery}
          searchPlaceholder="Filter sources…"
          noun="sources"
          empty={<EmptyState onAdd={() => setShowForm(true)} />}
          showEmpty={!showForm}
          renderItem={(s) => (
            <SourceRow
              key={s.name}
              spec={s}
              query={q}
              onChanged={() =>
                qc.invalidateQueries({ queryKey: ["sources"] })
              }
            />
          )}
        />
    </div>
  );
}

// inferFromURL mirrors the Go service's register-time inference
// (internal/service/source.go AddSource + inferFormatFromFilename) so
// the form can show what will be stored *before* submit. The Go side
// stays authoritative — this is a display hint only; if the rules ever
// drift, the registered spec still reflects the server, not this.
function inferFromURL(
  url: string,
): { kind: string; bucket?: string; prefix?: string; format: string } | null {
  const u = url.trim();
  if (!u) return null;
  const fmtFromName = (name: string): string => {
    const l = name.toLowerCase();
    if (l.endsWith(".parquet")) return "parquet";
    if (l.endsWith(".csv") || l.endsWith(".csv.gz")) return "csv";
    if (l.endsWith(".json") || l.endsWith(".ndjson")) return "json";
    return "";
  };
  const basename = (s: string): string => {
    const i = s.lastIndexOf("/");
    return i < 0 ? s : s.slice(i + 1);
  };
  if (u.startsWith("s3://")) {
    const rest = u.slice("s3://".length);
    const slash = rest.indexOf("/");
    const bucket = slash < 0 ? rest : rest.slice(0, slash);
    const key = slash < 0 ? "" : rest.slice(slash + 1);
    return { kind: "s3", bucket, prefix: key, format: key ? fmtFromName(basename(key)) : "" };
  }
  if (u.startsWith("http://") || u.startsWith("https://")) {
    return { kind: "http", format: fmtFromName(basename(u)) };
  }
  return null;
}

// SourceForm registers a new source or edits an existing one. In edit
// mode (`editing` set) the name field is locked — it is the registry key
// pipelines reference — and submit routes to updateSource instead.
function SourceForm({
  editing,
  onDone,
  onCancel,
}: {
  editing?: SourceSpec;
  onDone: () => void;
  onCancel?: () => void;
}) {
  const isEdit = !!editing;
  const initialUrl = editing
    ? editing.kind === "s3"
      ? `s3://${editing.bucket}/${editing.prefix}`
      : editing.url
    : "";
  const [name, setName] = useState(editing?.name ?? "");
  const [url, setUrl] = useState(initialUrl);
  const [credentials, setCredentials] = useState(editing?.credentials ?? "");
  const [advanced, setAdvanced] = useState(
    !!(
      editing &&
      (editing.format ||
        (editing.partitions && editing.partitions.length > 0) ||
        editing.start_from ||
        editing.manage_bucket_notifications)
    ),
  );
  const [format, setFormat] = useState(editing?.format ?? "");
  const [partitions, setPartitions] = useState(
    (editing?.partitions ?? []).join(", "),
  );
  const [startFrom, setStartFrom] = useState(editing?.start_from ?? "");
  const [manageNotifications, setManageNotifications] = useState(
    editing?.manage_bucket_notifications ?? false,
  );
  const [busy, setBusy] = useState(false);
  const credList = useCredentials();

  async function submit() {
    if (!name || !url) return;
    setBusy(true);
    try {
      const parts = partitions
        .split(",")
        .map((p) => p.trim())
        .filter((p) => p.length > 0);
      // The Go service auto-promotes s3://… URLs to kind=s3 with
      // bucket+prefix derived; https://… stay kind=http. UI keeps a
      // single URL field that handles both — no kind selector needed
      // for the happy path. Explicit kind/bucket/prefix remain
      // available via the CLI for power users.
      //
      // partitions / start_from / managed notifications are kind=s3
      // only — omit them when the URL resolves to http so editing a
      // source from s3 to http doesn't ship leftover s3 fields the
      // server would reject.
      const isS3 = inferFromURL(url)?.kind === "s3";
      const payload = {
        name,
        url,
        credentials: credentials || undefined,
        format: format || undefined,
        partitions: isS3 && parts.length ? parts : undefined,
        start_from: isS3 ? startFrom || undefined : undefined,
        manage_bucket_notifications:
          isS3 ? manageNotifications || undefined : undefined,
      };
      const stored = isEdit
        ? await updateSource(name, payload)
        : await registerSource(payload);
      toast.success(
        `${isEdit ? "Updated" : "Registered"} ${stored.name} (${stored.kind}/${stored.format})`,
      );
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
        <div className="grid gap-3 sm:grid-cols-[180px_1fr_auto]">
          <Input
            placeholder="name (e.g. trips)"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={busy || isEdit}
            data-testid="source-name-input"
          />
          <Input
            placeholder="https://example.com/data.parquet  or  s3://bucket/prefix/"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            disabled={busy}
            data-testid="source-url-input"
          />
          <div className="flex gap-2">
            <Button
              onClick={submit}
              disabled={busy || !name || !url}
              data-testid="register-source-submit"
            >
              {isEdit ? "Save changes" : "Register"}
            </Button>
            {onCancel && (
              <Button variant="ghost" onClick={onCancel} disabled={busy}>
                Cancel
              </Button>
            )}
          </div>
        </div>
        {isEdit && (
          <p className="text-[11px] text-muted-foreground">
            The source name is fixed — pipelines reference it by name.
            Editing a kind=s3 source doesn&apos;t re-sync pipelines already
            attached to it; re-attach to propagate.
          </p>
        )}
        {(() => {
          const inf = inferFromURL(url);
          if (!inf) return null;
          const effectiveFormat = format || inf.format;
          return (
            <p
              className="font-mono text-[11px] text-muted-foreground"
              data-testid="register-inference-hint"
            >
              →{" "}
              <span className="text-foreground">kind={inf.kind}</span>
              {inf.kind === "s3" && (
                <>
                  , bucket=<span className="text-foreground">{inf.bucket || "?"}</span>
                  , prefix=<span className="text-foreground">{inf.prefix || "(whole bucket)"}</span>
                </>
              )}
              , format=
              {effectiveFormat ? (
                <span className="text-foreground">{effectiveFormat}</span>
              ) : (
                <span className="italic">no extension — set it under Advanced</span>
              )}
              {format && (
                <span className="ml-1 text-muted-foreground">(format overridden)</span>
              )}
            </p>
          );
        })()}
        <div className="flex items-center gap-3">
          <label className="text-xs text-muted-foreground">
            Credentials (optional):
          </label>
          {/* Native select keeps the slice tight — switch to a custom
              combobox if/when the registry gets bigger than a dozen
              entries. Empty option = anonymous fetch. */}
          <NativeSelect
            value={credentials}
            onChange={(e) => setCredentials(e.target.value)}
            disabled={busy}
            className="h-auto w-auto px-2 shadow-none"
          >
            <option value="">(none — public URL / same-account S3)</option>
            {credList.data?.credentials.map((c) => (
              <option key={c.name} value={c.name}>
                {c.name} ({c.backend})
              </option>
            ))}
          </NativeSelect>
        </div>
        <button
          type="button"
          onClick={() => setAdvanced((v) => !v)}
          className="text-xs text-muted-foreground hover:text-foreground"
          disabled={busy}
        >
          {advanced ? "▾ Advanced" : "▸ Advanced (format, partitions, notifications)"}
        </button>
        {advanced && (
          <div className="grid gap-3 rounded-md border border-border p-3 sm:grid-cols-2">
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">Format</label>
              <NativeSelect
                value={format}
                onChange={(e) => setFormat(e.target.value)}
                disabled={busy}
                className="h-auto px-2 shadow-none"
              >
                <option value="">(infer from filename)</option>
                <option value="parquet">parquet</option>
                <option value="csv">csv</option>
                <option value="json">json</option>
              </NativeSelect>
            </div>
            <div className="space-y-1">
              <label className="text-xs text-muted-foreground">
                Start from (kind=s3 + partitions)
              </label>
              <Input
                placeholder='"all" | "now" | "2024-01-01"'
                value={startFrom}
                onChange={(e) => setStartFrom(e.target.value)}
                disabled={busy}
                className="font-mono text-xs"
              />
            </div>
            <div className="space-y-1 sm:col-span-2">
              <label className="text-xs text-muted-foreground">
                Partitions (kind=s3, Hive-style)
              </label>
              <Input
                placeholder="year, month, day"
                value={partitions}
                onChange={(e) => setPartitions(e.target.value)}
                disabled={busy}
                className="font-mono text-xs"
              />
              <p className="text-[11px] text-muted-foreground">
                Comma-separated. Declaring partitions switches the bucket
                to incremental reads — each run advances a watermark and
                only reads new partitions. Requires{" "}
                <code>format=parquet</code>.
              </p>
            </div>
            <label className="flex items-start gap-2 sm:col-span-2">
              <input
                type="checkbox"
                checked={manageNotifications}
                onChange={(e) => setManageNotifications(e.target.checked)}
                disabled={busy}
                className="mt-0.5"
              />
              <span className="space-y-0.5">
                <span className="block text-xs text-foreground">
                  Manage bucket EventBridge notifications (kind=s3)
                </span>
                <span className="block text-[11px] text-muted-foreground">
                  Terraform will own <code>aws_s3_bucket_notification</code>{" "}
                  on the source bucket, so S3 object-create events reach
                  the pipeline without an out-of-band{" "}
                  <code>aws s3api put-bucket-notification-configuration</code>{" "}
                  step. Authoritative; replaces any other notification
                  config. Leave off when clavesa doesn't own the
                  bucket.
                </span>
              </span>
            </label>
          </div>
        )}
        <p className="text-xs text-muted-foreground">
          Paste an https:// URL for kind=http or an s3:// URL for kind=s3
          (same-account). Format is inferred from the trailing filename;
          for directory URLs (no filename), pick the format under Advanced.
        </p>
      </CardContent>
    </Card>
  );
}

function SourceRow({
  spec,
  onChanged,
  query,
}: {
  spec: SourceSpec;
  onChanged: () => void;
  query: string;
}) {
  const [busy, setBusy] = useState(false);
  const [editing, setEditing] = useState(false);
  const [preview, setPreview] = useState<
    | { kind: "loading" }
    | { kind: "error"; message: string }
    | { kind: "ok"; result: PreviewResult }
    | null
  >(null);

  async function togglePreview() {
    if (preview) {
      setPreview(null);
      return;
    }
    setPreview({ kind: "loading" });
    try {
      const result = await getRegistrySourcePreview(spec.name, 0, 50);
      setPreview({ kind: "ok", result });
    } catch (e) {
      setPreview({
        kind: "error",
        message: e instanceof Error ? e.message : String(e),
      });
    }
  }

  async function remove(force = false) {
    setBusy(true);
    try {
      const res = await deleteSource(spec.name, { force });
      if (res?.usages?.length) {
        const usedBy = res.usages
          .map((u) => `${u.pipeline_dir} (${u.node_ids.join(", ")})`)
          .join("; ");
        if (window.confirm(`In use by: ${usedBy}\n\nDelete anyway?`)) {
          await deleteSource(spec.name, { force: true });
          toast.success(`Deleted ${spec.name}`);
          onChanged();
        }
      } else {
        toast.success(`Deleted ${spec.name}`);
        onChanged();
      }
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
    void force;
  }

  if (editing) {
    return (
      <li className="min-w-0">
        <SourceForm
          editing={spec}
          onDone={() => {
            setEditing(false);
            onChanged();
          }}
          onCancel={() => setEditing(false)}
        />
      </li>
    );
  }

  return (
    <li className="min-w-0" data-testid="source-row" data-source={spec.name}>
      <Card>
        <CardContent className="flex items-center gap-3 p-4">
          <Database className="h-4 w-4 flex-shrink-0 text-muted-foreground" />
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <span className="font-mono font-medium">
                <Highlight text={spec.name} query={query} />
              </span>
              <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground">
                <Highlight text={spec.kind} query={query} />
              </span>
              {spec.format && (
                <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground">
                  <Highlight text={spec.format} query={query} />
                </span>
              )}
              {spec.credentials && (
                <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground">
                  cred: {spec.credentials}
                </span>
              )}
              {spec.partitions && spec.partitions.length > 0 && (
                <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground">
                  parts: {spec.partitions.join("/")}
                  {spec.start_from && spec.start_from !== "all" ? ` from=${spec.start_from}` : ""}
                </span>
              )}
            </div>
            <code className="break-all font-mono text-xs text-muted-foreground">
              <Highlight
                text={
                  spec.kind === "s3"
                    ? `s3://${spec.bucket}/${spec.prefix}`
                    : spec.url
                }
                query={query}
              />
            </code>
          </div>
          <Button
            variant="ghost"
            size="icon"
            onClick={togglePreview}
            disabled={busy}
            aria-label={`Preview source ${spec.name}`}
          >
            <Eye className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setEditing(true)}
            disabled={busy}
            aria-label={`Edit source ${spec.name}`}
          >
            <Pencil className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            onClick={() => remove(false)}
            disabled={busy}
            aria-label={`Delete source ${spec.name}`}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </CardContent>
        {preview && <SourcePreviewPanel state={preview} />}
      </Card>
    </li>
  );
}

function SourcePreviewPanel({
  state,
}: {
  state:
    | { kind: "loading" }
    | { kind: "error"; message: string }
    | { kind: "ok"; result: PreviewResult };
}) {
  if (state.kind === "loading") {
    return (
      <div className="border-t border-border p-4">
        <Skeleton className="h-20 w-full" />
      </div>
    );
  }
  if (state.kind === "error") {
    return (
      <div className="border-t border-border p-4 text-sm text-destructive">
        Preview failed — {state.message}
      </div>
    );
  }
  const { result } = state;
  const cols =
    result.schema.length > 0
      ? result.schema.map((c) => c.name)
      : result.items.length > 0
        ? Object.keys(result.items[0])
        : [];
  if (result.items.length === 0) {
    return (
      <div className="border-t border-border p-4 text-sm text-muted-foreground">
        Source returned no rows.
      </div>
    );
  }
  return (
    <div className="border-t border-border">
      <div className="max-h-72 overflow-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-card">
            <tr className="border-b border-border">
              {cols.map((c) => (
                <th
                  key={c}
                  className="px-3 py-1.5 text-left font-mono font-medium text-muted-foreground"
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.items.map((row, i) => (
              <tr key={i} className="border-b border-border/50">
                {cols.map((c) => (
                  <td
                    key={c}
                    className="whitespace-nowrap px-3 py-1.5 font-mono"
                  >
                    {formatPreviewCell(row[c])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="px-3 py-1.5 text-[11px] text-muted-foreground">
        {result.items.length} row{result.items.length === 1 ? "" : "s"}
        {result.truncated ? " · truncated" : ""}
      </div>
    </div>
  );
}

function formatPreviewCell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function EmptyState({ onAdd }: { onAdd: () => void }) {
  return (
    <Card>
      <CardContent className="flex flex-col items-start gap-3 p-6 text-sm">
        <div className="flex items-center gap-2">
          <Database className="h-5 w-5 text-muted-foreground" />
          <span className="font-medium">No sources registered yet</span>
        </div>
        <p className="text-muted-foreground">
          Register a source to declare where raw data lives. Pipelines then
          reference it by name from transform inputs. The CLI equivalent is{" "}
          <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
            clavesa source register &lt;name&gt; --from &lt;url&gt;
          </code>
          .
        </p>
        <Button size="sm" onClick={onAdd}>
          <Plus className="mr-1 h-4 w-4" />
          Register source
        </Button>
      </CardContent>
    </Card>
  );
}
