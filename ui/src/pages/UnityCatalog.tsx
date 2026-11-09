/**
 * UnityCatalog — the workspace's Databricks connection (ADR-027).
 *
 * The twin of `clavesa workspace connect databricks` / `workspace
 * databricks [--check]` / `workspace disconnect databricks` (ADR-015):
 * all three call the same service methods, write the same `databricks`
 * block in clavesa.json, and show the same admin checks with the same
 * fixes. Clavesa never changes anything in Databricks from here; the
 * next deploy does that.
 */

import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  CheckCircle2,
  CircleDashed,
  CircleHelp,
  Loader2,
  Share2,
  XCircle,
} from "lucide-react";
import { toast } from "sonner";

import { useChrome } from "@/components/PageChrome";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { NativeSelect } from "@/components/ui/native-select";
import { cn } from "@/lib/utils";
import {
  checkDatabricks,
  connectDatabricks,
  disconnectDatabricks,
  setDatabricksJobs,
  compareDatabricksTables,
  runPreflight,
  type PreflightResult,
  type DatabricksCompareResult,
  useCredentials,
  useDatabricks,
  useDatabricksTables,
  type DatabricksCheck,
  type DatabricksStatus,
} from "@/lib/queries";

export function UnityCatalog() {
  const status = useDatabricks();
  const qc = useQueryClient();
  const [editing, setEditing] = useState(false);
  const [checks, setChecks] = useState<DatabricksCheck[] | null>(null);
  const [busy, setBusy] = useState<"check" | "disconnect" | null>(null);

  useChrome(
    useMemo(
      () => ({ breadcrumbs: [{ label: "Unity Catalog", to: "/unity-catalog" }] }),
      [],
    ),
  );

  const info = status.data;
  const conn = info?.connection;

  function stored(next: DatabricksStatus) {
    qc.setQueryData(["workspace", "databricks"], next);
  }

  async function recheck() {
    setBusy("check");
    try {
      const res = await checkDatabricks();
      setChecks(res.checks);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  async function disconnect() {
    if (!window.confirm("Remove the Databricks connection from clavesa.json? Tables already in Unity Catalog stay there. A catalog made with --create-catalog stays too, and clavesa stops managing it.")) return;
    setBusy("disconnect");
    try {
      const st = await disconnectDatabricks();
      stored(st);
      setChecks(null);
      toast.success(
        st.released.length > 0
          ? `Disconnected. ${st.released.length} Databricks and AWS objects stay, no longer managed by clavesa. Deploy the workspace and each pipeline next.`
          : "Disconnected. Deploy the workspace and each pipeline next.",
      );
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="mx-auto w-full max-w-4xl px-6 py-8" data-testid="unity-catalog-page">
      <div className="mb-6">
        <h1 className="font-mono text-2xl font-semibold tracking-tight">
          Unity Catalog
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Publish this workspace&apos;s tables and lineage to Databricks Unity
          Catalog. Pipelines still run on clavesa compute; Databricks jobs
          downstream read the tables as{" "}
          <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
            &lt;catalog&gt;.&lt;pipeline schema&gt;.&lt;table&gt;
          </code>
          .
        </p>
      </div>

      {status.isLoading && (
        <p className="text-sm text-muted-foreground">Loading…</p>
      )}
      {status.error && (
        <p role="alert" className="text-sm text-status-failed">
          {status.error instanceof Error ? status.error.message : String(status.error)}
        </p>
      )}

      {info && !editing && (
        <Card className="mb-6">
          <CardContent className="space-y-4 p-6">
            {conn ? (
              <dl
                className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1.5 text-sm"
                data-testid="databricks-connection"
              >
                <dt className="text-muted-foreground">Host</dt>
                <dd className="font-mono text-xs" data-testid="databricks-host">{conn.host}</dd>
                <dt className="text-muted-foreground">Catalog</dt>
                <dd className="font-mono text-xs" data-testid="databricks-catalog">
                  {conn.catalog}
                  <span className="text-muted-foreground">
                    {conn.create_catalog ? " (created by clavesa)" : " (existing)"}
                  </span>
                </dd>
                <dt className="text-muted-foreground">Credential</dt>
                <dd className="font-mono text-xs">
                  {conn.credential}
                  {info.client_id && (
                    <span className="text-muted-foreground"> (client {info.client_id})</span>
                  )}
                </dd>
                <dt className="text-muted-foreground">External location</dt>
                <dd className="font-mono text-xs">
                  {conn.external_location ||
                    (conn.create_catalog
                      ? "created by clavesa at deploy"
                      : "none (existing external tables only)")}
                </dd>
                <dt className="text-muted-foreground">Publish tables</dt>
                <dd className="text-xs">{conn.publish_tables ? "on" : "off"}</dd>
                <dt className="text-muted-foreground">Push lineage</dt>
                <dd className="text-xs">{conn.push_lineage ? "on" : "off"}</dd>
                <dt className="text-muted-foreground">Readers</dt>
                <dd className="font-mono text-xs" data-testid="databricks-readers">
                  {conn.readers.length > 0 ? conn.readers.join(", ") : "none"}
                </dd>
              </dl>
            ) : (
              <div className="flex items-center gap-2 text-sm" data-testid="databricks-not-connected">
                <Share2 className="h-4 w-4 text-muted-foreground" />
                Not connected. The CLI equivalent is{" "}
                <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
                  clavesa workspace connect databricks --host … --catalog … --credential …
                </code>
              </div>
            )}
            <div className="flex flex-wrap gap-2">
              <Button
                size="sm"
                variant={conn ? "outline" : "default"}
                data-testid="databricks-edit-button"
                onClick={() => {
                  setChecks(null);
                  setEditing(true);
                }}
              >
                {conn ? "Change" : "Connect"}
              </Button>
              {conn && (
                <>
                  <Button
                    size="sm"
                    variant="outline"
                    data-testid="databricks-check-button"
                    disabled={busy !== null}
                    onClick={recheck}
                  >
                    {busy === "check" && <Loader2 className="h-3 w-3 animate-spin" />}
                    Check again
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    data-testid="databricks-disconnect-button"
                    disabled={busy !== null}
                    onClick={disconnect}
                  >
                    Disconnect
                  </Button>
                </>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {info && editing && (
        <ConnectForm
          initial={info}
          onCancel={() => setEditing(false)}
          onResult={(res) => {
            setChecks(res.checks);
            if (res.saved) {
              stored(res);
              setEditing(false);
              toast.success(`Connected to ${res.connection?.catalog}`);
            } else {
              toast.error("Not connected: a Databricks admin must do the failed steps first");
            }
          }}
        />
      )}

      {checks && <CheckList checks={checks} />}

      {conn && !editing && <PipelineTables />}

      <PreflightCard defaultHost={conn?.host ?? ""} />
    </div>
  );
}

/**
 * Read-only preflight for a step to replace (CLI twin: `clavesa workspace
 * databricks preflight`). Needs no connection: it is for a work
 * workspace before anything is set up.
 */
function PreflightCard({ defaultHost }: { defaultHost: string }) {
  // host starts empty and falls back to the connection's host, which may
  // load after the first render (a useState default would miss it).
  const [v, setV] = useState({
    host: "",
    profile: "",
    principal: "",
    table: "",
    shadow_location: "",
    job: "",
    inputs: "",
    readers: "",
    subnets: "",
  });
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState<PreflightResult | null>(null);
  const host = v.host || defaultHost;
  const list = (s: string) =>
    s
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

  async function run() {
    setBusy(true);
    setRes(null);
    try {
      setRes(
        await runPreflight({
          host,
          profile: v.profile,
          principal: v.principal,
          table: v.table,
          shadow_location: v.shadow_location,
          job: v.job ? Number(v.job) : 0,
          inputs: list(v.inputs),
          readers: list(v.readers),
          subnets: list(v.subnets),
        }),
      );
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const fields: [keyof typeof v, string, string][] = [
    ["host", "Databricks host", "https://dbc-….cloud.databricks.com"],
    ["profile", "Databricks CLI profile (your login)", "work"],
    ["principal", "Service principal clavesa runs as", "application ID"],
    ["table", "Table the step writes", "catalog.schema.table"],
    ["shadow_location", "Shadow table folder (optional)", "s3://team-bucket/clavesa/table/"],
    ["job", "Job that runs the step (optional)", "123456789"],
    ["inputs", "Input tables (optional)", "catalog.schema.table, …"],
    ["readers", "SQL warehouse readers (optional)", "analysts, …"],
    ["subnets", "Fargate subnets (optional; default VPC)", "subnet-…, …"],
  ];
  return (
    <details className="mt-6 rounded-md border p-4 text-sm" data-testid="databricks-preflight">
      <summary className="cursor-pointer font-medium">Preflight a step to replace (read-only)</summary>
      <p className="mt-2 text-xs text-muted-foreground">
        Checks every right and setting clavesa needs and lists exactly what an admin must grant.
        Nothing in Databricks or AWS changes.
      </p>
      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        {fields.map(([k, label, ph]) => (
          <div key={k} className="space-y-1">
            <Label htmlFor={`preflight-${k}`} className="text-xs text-muted-foreground">
              {label}
            </Label>
            <Input
              id={`preflight-${k}`}
              data-testid={`databricks-preflight-${k.replace("_", "-")}`}
              value={v[k]}
              onChange={(e) => setV({ ...v, [k]: e.target.value })}
              placeholder={k === "host" && defaultHost ? defaultHost : ph}
              className="h-8 font-mono text-xs"
            />
          </div>
        ))}
      </div>
      <Button
        size="sm"
        className="mt-3"
        disabled={busy || !host || !v.profile}
        onClick={run}
        data-testid="databricks-preflight-run"
      >
        {busy && <Loader2 className="h-3 w-3 animate-spin" />}
        Run preflight
      </Button>
      {res && (
        <div className="mt-3 space-y-2">
          <p
            className={res.ready ? "font-semibold text-status-success" : "font-semibold text-status-failed"}
            data-testid="databricks-preflight-verdict"
            data-ready={res.ready ? "true" : "false"}
          >
            {res.ready ? "Ready: nothing is missing" : "Not ready: an admin must make the changes below"}
          </p>
          <CheckList checks={res.checks} />
        </div>
      )}
    </details>
  );
}

/**
 * Each pipeline's schema in Unity Catalog, the tables Databricks holds
 * there (read from Databricks, not from clavesa's intent), and the jobs
 * the pipeline starts after a successful run. CLI twins: `workspace
 * databricks tables` and `pipeline databricks-jobs`.
 */
function PipelineTables() {
  const tables = useDatabricksTables(true);
  return (
    <div className="mt-8" data-testid="databricks-pipelines">
      <h2 className="mb-3 text-lg font-semibold">Pipelines in Unity Catalog</h2>
      {tables.isLoading && <p className="text-sm text-muted-foreground">Reading Unity Catalog…</p>}
      {tables.error && (
        <p role="alert" className="text-sm text-status-failed">
          {tables.error instanceof Error ? tables.error.message : String(tables.error)}
        </p>
      )}
      <div className="space-y-4">
        {tables.data?.pipelines.map((p) => (
          <Card key={p.pipeline} data-testid={`databricks-pipeline-${p.pipeline}`}>
            <CardContent className="space-y-3 p-5">
              <div className="flex flex-wrap items-baseline justify-between gap-2">
                <span className="font-mono font-medium">{p.pipeline}</span>
                <a className="font-mono text-xs underline" href={p.schema_url} target="_blank" rel="noreferrer">
                  {p.schema}
                </a>
              </div>
              {p.schema_error && <p className="text-xs text-muted-foreground">{p.schema_error}</p>}
              {p.tables.length > 0 && (
                <ul className="space-y-1.5">
                  {p.tables.map((t) => (
                    <li key={t.full_name} className="flex flex-wrap items-center gap-2 text-sm" data-testid="databricks-table-row">
                      {t.by_clavesa && (
                        <span
                          className="rounded bg-status-success/15 px-1.5 py-0.5 text-xs text-status-success"
                          data-testid="databricks-published-badge"
                        >
                          Published to Unity Catalog
                        </span>
                      )}
                      <span className="font-mono text-xs">{t.full_name}</span>
                      <span className="text-xs text-muted-foreground">{t.type}</span>
                      <a className="text-xs underline" href={t.url} target="_blank" rel="noreferrer">
                        Open in Catalog Explorer
                      </a>
                    </li>
                  ))}
                </ul>
              )}
              <JobsEditor pipeline={p.dir} jobs={p.jobs} />
              <CompareForm pipeline={p.dir} />
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

function JobsEditor({ pipeline, jobs }: { pipeline: string; jobs: number[] }) {
  const qc = useQueryClient();
  const [value, setValue] = useState(jobs.join(", "));
  const [busy, setBusy] = useState(false);

  async function save() {
    const parsed = value
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => Number(s));
    if (parsed.some((n) => !Number.isInteger(n) || n <= 0)) {
      toast.error("Job IDs are positive whole numbers, comma-separated");
      return;
    }
    setBusy(true);
    try {
      const res = await setDatabricksJobs(pipeline, parsed);
      void qc.invalidateQueries({ queryKey: ["workspace", "databricks", "tables"] });
      toast.success(
        res.jobs.length
          ? `Starts jobs ${res.jobs.join(", ")} after each successful run. Deploy the pipeline to apply.`
          : "Starts no jobs. Deploy the pipeline to apply.",
      );
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      <Label htmlFor={`jobs-${pipeline}`} className="text-xs text-muted-foreground">
        Databricks jobs to start after a successful run
      </Label>
      <Input
        id={`jobs-${pipeline}`}
        data-testid={`databricks-jobs-input-${pipeline}`}
        value={value}
        onChange={(e) => setValue(e.target.value)}
        placeholder="job IDs, e.g. 263121218945262"
        className="h-8 w-64 font-mono text-xs"
      />
      <Button size="sm" variant="outline" disabled={busy} onClick={save} data-testid={`databricks-jobs-save-${pipeline}`}>
        Save
      </Button>
    </div>
  );
}

/**
 * The shadow-mode check (docs/cookbook/replace-a-databricks-step.md): the
 * replaced Databricks step's table against clavesa's, per partition. CLI
 * twin: `clavesa pipeline databricks-compare`.
 */
function CompareForm({ pipeline }: { pipeline: string }) {
  const [existing, setExisting] = useState("");
  const [shadow, setShadow] = useState("");
  const [partition, setPartition] = useState("");
  const [sums, setSums] = useState("");
  const [where, setWhere] = useState("");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<DatabricksCompareResult | null>(null);

  async function run() {
    setBusy(true);
    setResult(null);
    try {
      setResult(
        await compareDatabricksTables({
          pipeline,
          existing,
          shadow,
          partition,
          sums: sums
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
          where: where || undefined,
        }),
      );
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  const field = (id: string, label: string, value: string, set: (v: string) => void, ph: string) => (
    <div className="space-y-1">
      <Label htmlFor={`${id}-${pipeline}`} className="text-xs text-muted-foreground">
        {label}
      </Label>
      <Input
        id={`${id}-${pipeline}`}
        data-testid={`databricks-compare-${id}-${pipeline}`}
        value={value}
        onChange={(e) => set(e.target.value)}
        placeholder={ph}
        className="h-8 font-mono text-xs"
      />
    </div>
  );

  return (
    <details className="rounded-md border p-3 text-sm" data-testid={`databricks-compare-${pipeline}`}>
      <summary className="cursor-pointer text-xs text-muted-foreground">
        Shadow mode: compare with the Databricks step&apos;s table
      </summary>
      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        {field("existing", "Existing table (the Databricks step's)", existing, setExisting, "catalog.schema.table")}
        {field("shadow", "Shadow table (clavesa's)", shadow, setShadow, "catalog.schema.table_clavesa")}
        {field("partition", "Partition column", partition, setPartition, "event_date")}
        {field("sums", "Key columns to sum", sums, setSums, "amount, quantity")}
        {field("where", "Where (optional)", where, setWhere, "event_date >= date_sub(current_date(), 7)")}
      </div>
      <Button
        size="sm"
        variant="outline"
        className="mt-3"
        disabled={busy || !existing || !shadow || !partition}
        onClick={run}
        data-testid={`databricks-compare-run-${pipeline}`}
      >
        {busy ? "Comparing…" : "Compare"}
      </Button>
      {result && (
        <div className="mt-3 space-y-2" data-testid={`databricks-compare-result-${pipeline}`}>
          <div
            className={result.pass ? "font-semibold text-emerald-600" : "font-semibold text-destructive"}
            data-testid={`databricks-compare-verdict-${pipeline}`}
            data-pass={result.pass ? "true" : "false"}
          >
            {result.pass ? `PASS: ${result.partitions.length} partitions match` : "FAIL"}
          </div>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-left text-muted-foreground">
                <th className="pr-3">{result.partition}</th>
                <th className="pr-3">rows (existing / shadow)</th>
                <th className="pr-3">sums</th>
                <th>status</th>
              </tr>
            </thead>
            <tbody>
              {result.partitions.map((p) => (
                <tr key={p.partition} className={p.status === "match" ? "" : "text-destructive"}>
                  <td className="pr-3 font-mono">{p.partition}</td>
                  <td className="pr-3 font-mono">
                    {p.rows[0] ?? "-"} / {p.rows[1] ?? "-"}
                  </td>
                  <td className="pr-3 font-mono">
                    {Object.entries(p.sums)
                      .map(([k, v]) => `${k} ${String(v[0] ?? "-")} / ${String(v[1] ?? "-")}`)
                      .join("; ")}
                  </td>
                  <td>{p.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {result.schema_diff.map((d) => (
            <div key={d} className="text-xs text-destructive">
              column differs: {d}
            </div>
          ))}
        </div>
      )}
    </details>
  );
}

function ConnectForm({
  initial,
  onCancel,
  onResult,
}: {
  initial: DatabricksStatus;
  onCancel: () => void;
  onResult: (res: DatabricksStatus) => void;
}) {
  const creds = useCredentials();
  const dbxCreds = (creds.data?.credentials ?? []).filter(
    (c) => c.kind === "databricks-oauth",
  );
  const c = initial.connection;
  const [host, setHost] = useState(c?.host ?? "");
  const [catalog, setCatalog] = useState(c?.catalog ?? "");
  const [credential, setCredential] = useState(c?.credential ?? "");
  const [externalLocation, setExternalLocation] = useState(c?.external_location ?? "");
  const [publishTables, setPublishTables] = useState(c?.publish_tables ?? true);
  const [pushLineage, setPushLineage] = useState(c?.push_lineage ?? true);
  const [createCatalog, setCreateCatalog] = useState(c?.create_catalog ?? false);
  const [readers, setReaders] = useState((c?.readers ?? []).join(", "));
  const [busy, setBusy] = useState(false);

  const cred = credential || dbxCreds[0]?.name || "";

  async function submit() {
    setBusy(true);
    try {
      onResult(
        await connectDatabricks({
          host,
          catalog,
          credential: cred,
          external_location: externalLocation || undefined,
          create_catalog: createCatalog,
          publish_tables: publishTables,
          push_lineage: pushLineage,
          readers: readers
            .split(",")
            .map((r) => r.trim())
            .filter(Boolean),
        }),
      );
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card className="mb-6">
      <CardContent className="grid gap-3.5 p-6">
        <div className="space-y-1.5">
          <Label htmlFor="dbx-host">Host</Label>
          <Input
            id="dbx-host"
            data-testid="databricks-host-input"
            value={host}
            onChange={(e) => setHost(e.target.value)}
            placeholder="https://dbc-1234abcd-5678.cloud.databricks.com"
            className="font-mono text-xs"
          />
        </div>
        <div className="grid gap-3 sm:grid-cols-2">
          <div className="space-y-1.5">
            <Label htmlFor="dbx-catalog">Catalog</Label>
            <Input
              id="dbx-catalog"
              data-testid="databricks-catalog-input"
              value={catalog}
              onChange={(e) => setCatalog(e.target.value)}
              placeholder="analytics"
              className="font-mono text-xs"
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="dbx-credential">Credential</Label>
            <NativeSelect
              id="dbx-credential"
              data-testid="databricks-credential-select"
              value={cred}
              onChange={(e) => setCredential(e.target.value)}
              className="font-mono text-xs"
            >
              {dbxCreds.length === 0 && (
                <option value="">no databricks-oauth credential: register one under Credentials</option>
              )}
              {dbxCreds.map((d) => (
                <option key={d.name} value={d.name}>
                  {d.name} (client {d.client_id})
                </option>
              ))}
            </NativeSelect>
          </div>
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="dbx-external-location">External location (optional)</Label>
          <Input
            id="dbx-external-location"
            data-testid="databricks-external-location-input"
            value={externalLocation}
            onChange={(e) => setExternalLocation(e.target.value)}
            placeholder="admin-created location over the workspace bucket, needed for new tables"
            className="font-mono text-xs"
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="dbx-readers">Readers (optional)</Label>
          <Input
            id="dbx-readers"
            data-testid="databricks-readers-input"
            value={readers}
            onChange={(e) => setReaders(e.target.value)}
            placeholder="users or groups that read the tables from a SQL warehouse, comma-separated"
            className="font-mono text-xs"
          />
        </div>
        <label className="flex items-start gap-2 text-sm">
          <input
            type="checkbox"
            className="mt-1"
            data-testid="databricks-create-catalog-input"
            checked={createCatalog}
            onChange={(e) => setCreateCatalog(e.target.checked)}
          />
          <span>
            Create this catalog
            <span className="block text-xs text-muted-foreground">
              Only for a Databricks workspace with no real catalog. Off: the
              catalog must exist, and clavesa creates no catalog or schema.
            </span>
          </span>
        </label>
        <div className="flex flex-wrap gap-6 text-sm">
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              data-testid="databricks-publish-tables-input"
              checked={publishTables}
              onChange={(e) => setPublishTables(e.target.checked)}
            />
            Publish tables
          </label>
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              data-testid="databricks-push-lineage-input"
              checked={pushLineage}
              onChange={(e) => setPushLineage(e.target.checked)}
            />
            Push lineage
          </label>
        </div>
        <p className="text-xs text-muted-foreground">
          Connect checks each step a Databricks admin must do first and
          records the connection only when none fails. Nothing changes in
          Databricks until the next deploy.
        </p>
        <div className="flex justify-end gap-2">
          <Button size="sm" variant="outline" onClick={onCancel} disabled={busy}>
            Cancel
          </Button>
          <Button
            size="sm"
            data-testid="databricks-connect-button"
            disabled={busy || !host || !catalog || !cred}
            onClick={submit}
          >
            {busy && <Loader2 className="h-3 w-3 animate-spin" />}
            {busy ? "Checking…" : "Check and connect"}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

const STATUS_ICON = {
  pass: CheckCircle2,
  fail: XCircle,
  unknown: CircleHelp,
  skipped: CircleDashed,
} as const;

function CheckList({ checks }: { checks: DatabricksCheck[] }) {
  return (
    <Card>
      <CardContent className="p-0">
        <ul className="divide-y divide-border" data-testid="databricks-checks">
          {checks.map((c, i) => {
            const Icon = STATUS_ICON[c.status];
            return (
              <li
                key={`${c.id}-${i}`}
                className="flex gap-3 p-4"
                data-testid={`databricks-check-${c.id}`}
                data-status={c.status}
              >
                <Icon
                  className={cn(
                    "mt-0.5 h-4 w-4 flex-shrink-0",
                    c.status === "pass" && "text-status-success",
                    c.status === "fail" && "text-status-failed",
                    (c.status === "unknown" || c.status === "skipped") && "text-muted-foreground",
                  )}
                />
                <div className="min-w-0 flex-1 text-sm">
                  <p className="font-medium">{c.title}</p>
                  {c.detail && (
                    <p className="break-words text-xs text-muted-foreground">{c.detail}</p>
                  )}
                  {c.fix && (
                    <p className="mt-1.5 break-words rounded bg-muted px-2 py-1.5 font-mono text-xs">
                      {c.fix}
                    </p>
                  )}
                </div>
              </li>
            );
          })}
        </ul>
      </CardContent>
    </Card>
  );
}
