/**
 * Credentials — workspace credentials registry (ADR-017 slice 2).
 *
 * Lists registered credentials with kind, header, and the secret-backend
 * discriminator. Inline form to register a new header credential. Detail
 * never shows the secret material itself, only the reference (per ADR).
 */

import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { KeyRound, Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";

import { useChrome } from "@/components/PageChrome";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { RegistryList } from "@/pages/RegistryList";
import {
  deleteCredential,
  registerCredential,
  useCredentials,
  type CredentialSpec,
} from "@/lib/queries";

export function Credentials() {
  const list = useCredentials();
  const qc = useQueryClient();
  const [showForm, setShowForm] = useState(false);

  // Free-text filter — case-insensitive over name, kind, secret backend,
  // header, and the secret reference.
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();
  const allCredentials = list.data?.credentials ?? [];
  const filtered = useMemo(() => {
    if (!q) return allCredentials;
    return allCredentials.filter((c) =>
      [c.name, c.kind, c.backend, c.header_name, c.secret].some((f) =>
        (f ?? "").toLowerCase().includes(q),
      ),
    );
  }, [allCredentials, q]);

  useChrome(
    useMemo(
      () => ({
        breadcrumbs: [{ label: "Credentials", to: "/credentials" }],
        trailing: (
          <Button
            size="sm"
            variant={showForm ? "secondary" : "default"}
            onClick={() => setShowForm((v) => !v)}
          >
            <Plus className="mr-1 h-4 w-4" />
            {showForm ? "Cancel" : "Register credential"}
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
            Credentials
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Named secrets for outbound auth. Sources reference credentials
            by name; the secret material itself is never stored here, only
            the reference (
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
              env:VAR
            </code>
            ,{" "}
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
              file:&lt;rel&gt;
            </code>
            , or{" "}
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
              arn:aws:secretsmanager:…
            </code>
            ).
          </p>
        </div>

        {showForm && (
          <RegisterForm
            onDone={() => {
              setShowForm(false);
              void qc.invalidateQueries({ queryKey: ["credentials"] });
            }}
          />
        )}

        <RegistryList
          query={list}
          items={allCredentials}
          filtered={filtered}
          search={query}
          onSearchChange={setQuery}
          searchPlaceholder="Filter credentials…"
          noun="credentials"
          empty={<EmptyState onAdd={() => setShowForm(true)} />}
          showEmpty={!showForm}
          renderItem={(c) => (
            <CredentialRow
              key={c.name}
              spec={c}
              onDeleted={() =>
                qc.invalidateQueries({ queryKey: ["credentials"] })
              }
            />
          )}
        />
    </div>
  );
}

function RegisterForm({ onDone }: { onDone: () => void }) {
  const [name, setName] = useState("");
  const [headerName, setHeaderName] = useState("Authorization");
  const [valuePrefix, setValuePrefix] = useState("Bearer ");
  const [secret, setSecret] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit() {
    if (!name || !headerName || !secret) return;
    setBusy(true);
    try {
      const stored = await registerCredential({
        name,
        kind: "header",
        header_name: headerName,
        value_prefix: valuePrefix,
        secret,
      });
      toast.success(`Registered ${stored.name} (${stored.backend})`);
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
        <div className="grid gap-3 sm:grid-cols-2">
          <Input
            placeholder="name (e.g. stripe)"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={busy}
          />
          <Input
            placeholder="header (e.g. Authorization)"
            value={headerName}
            onChange={(e) => setHeaderName(e.target.value)}
            disabled={busy}
          />
        </div>
        <div className="grid gap-3 sm:grid-cols-2">
          <Input
            placeholder='value prefix (e.g. "Bearer ")'
            value={valuePrefix}
            onChange={(e) => setValuePrefix(e.target.value)}
            disabled={busy}
          />
          <Input
            placeholder="secret reference (env:STRIPE_KEY)"
            value={secret}
            onChange={(e) => setSecret(e.target.value)}
            disabled={busy}
          />
        </div>
        <div className="flex items-center justify-between">
          <p className="text-xs text-muted-foreground">
            Backends: <code className="font-mono">env:VAR</code>,{" "}
            <code className="font-mono">file:&lt;rel&gt;</code>, or{" "}
            <code className="font-mono">arn:aws:secretsmanager:…</code>.
            Local-only backends fail at orchestration emit for cloud
            deploys.
          </p>
          <Button
            onClick={submit}
            disabled={busy || !name || !headerName || !secret}
          >
            Register
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function CredentialRow({
  spec,
  onDeleted,
}: {
  spec: CredentialSpec;
  onDeleted: () => void;
}) {
  const [busy, setBusy] = useState(false);

  async function remove() {
    setBusy(true);
    try {
      const res = await deleteCredential(spec.name);
      if (res?.usages?.length) {
        const usedBy = res.usages.map((u) => u.source_name).join(", ");
        if (window.confirm(`In use by sources: ${usedBy}\n\nDelete anyway?`)) {
          await deleteCredential(spec.name, { force: true });
          toast.success(`Deleted ${spec.name}`);
          onDeleted();
        }
      } else {
        toast.success(`Deleted ${spec.name}`);
        onDeleted();
      }
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <li>
      <Card>
        <CardContent className="flex items-center gap-3 p-4">
          <KeyRound className="h-4 w-4 flex-shrink-0 text-muted-foreground" />
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <span className="font-mono font-medium">{spec.name}</span>
              <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground">
                {spec.kind}
              </span>
              {spec.backend && (
                <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground">
                  {spec.backend}
                </span>
              )}
            </div>
            <code className="break-all font-mono text-xs text-muted-foreground">
              {spec.header_name}
              {spec.value_prefix ? `: ${spec.value_prefix}…` : ""} → {spec.secret}
            </code>
          </div>
          <Button
            variant="ghost"
            size="icon"
            onClick={remove}
            disabled={busy}
            aria-label={`Delete credential ${spec.name}`}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </CardContent>
      </Card>
    </li>
  );
}

function EmptyState({ onAdd }: { onAdd: () => void }) {
  return (
    <Card>
      <CardContent className="flex flex-col items-start gap-3 p-6 text-sm">
        <div className="flex items-center gap-2">
          <KeyRound className="h-5 w-5 text-muted-foreground" />
          <span className="font-medium">No credentials registered yet</span>
        </div>
        <p className="text-muted-foreground">
          Register a credential to authenticate outbound source fetches —
          API keys, Bearer tokens. The CLI equivalent is{" "}
          <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
            clavesa credential register &lt;name&gt; --header
            Authorization --value-prefix "Bearer " --secret env:KEY
          </code>
          .
        </p>
        <Button size="sm" onClick={onAdd}>
          <Plus className="mr-1 h-4 w-4" />
          Register credential
        </Button>
      </CardContent>
    </Card>
  );
}
