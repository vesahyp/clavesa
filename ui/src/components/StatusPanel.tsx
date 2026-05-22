/**
 * StatusPanel — shows Step Functions execution history for the pipeline.
 *
 * Fetches GET /pipeline/status?dir=<dir> on mount and renders:
 *   - "Not deployed" when deployed=false
 *   - Execution list when deployed=true
 *   - Inline error if the request fails
 *
 * For FAILED/TIMED_OUT executions a "Details" button fetches
 * GET /pipeline/execution?arn=<arn> to show the failed step and error cause.
 */

import { useEffect, useState } from "react";
import { ExternalLink, Loader2, Play } from "lucide-react";

import { BASE_URL } from "../api/client";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { useExecutionLogs } from "@/lib/queries";

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------

interface ExecutionInfo {
  name: string;
  status: string;
  started_at: string;
  stopped_at?: string;
  console_url: string;
  execution_arn: string;
}

interface ExecutionDetail {
  status: string;
  error?: string;
  cause?: string;
  failed_step?: string;
  step_error?: string;
  step_cause?: string;
}

interface PipelineStatus {
  deployed: boolean;
  cloud?: string;
  state_machine_arn?: string;
  executions: ExecutionInfo[];
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const API_BASE = BASE_URL;

async function fetchStatus(dir: string): Promise<PipelineStatus> {
  const res = await fetch(`${API_BASE}/pipeline/status?dir=${encodeURIComponent(dir)}`);
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`GET /pipeline/status -> ${res.status}: ${text}`);
  }
  return res.json() as Promise<PipelineStatus>;
}

async function triggerRun(dir: string): Promise<void> {
  const res = await fetch(`${API_BASE}/pipeline/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ dir }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`POST /pipeline/run -> ${res.status}: ${text}`);
  }
}

async function fetchExecutionDetail(arn: string): Promise<ExecutionDetail> {
  const res = await fetch(`${API_BASE}/pipeline/execution?arn=${encodeURIComponent(arn)}`);
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`GET /pipeline/execution -> ${res.status}: ${text}`);
  }
  return res.json() as Promise<ExecutionDetail>;
}

function relativeTime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function durationLabel(startIso: string, stopIso: string): string {
  const ms = new Date(stopIso).getTime() - new Date(startIso).getTime();
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ${secs % 60}s`;
  return `${Math.floor(mins / 60)}h ${mins % 60}m`;
}

function statusBadgeVariant(status: string) {
  switch (status) {
    case "SUCCEEDED":
      return "success" as const;
    case "FAILED":
      return "failed" as const;
    case "RUNNING":
      return "running" as const;
    case "TIMED_OUT":
    case "ABORTED":
      return "secondary" as const;
    default:
      return "outline" as const;
  }
}

// ---------------------------------------------------------------------------
// ExecutionRow — handles expand/collapse for failure details
// ---------------------------------------------------------------------------

function ExecutionRow({ e }: { e: ExecutionInfo }) {
  const canExpand = e.status === "FAILED" || e.status === "TIMED_OUT";
  const [expanded, setExpanded] = useState(false);
  const [detail, setDetail] = useState<ExecutionDetail | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  function toggle() {
    if (!canExpand) return;
    if (!expanded && detail === null && !detailLoading) {
      setDetailLoading(true);
      fetchExecutionDetail(e.execution_arn)
        .then((d) => { setDetail(d); setDetailLoading(false); })
        .catch((err) => {
          setDetailError(err instanceof Error ? err.message : String(err));
          setDetailLoading(false);
        });
    }
    setExpanded((v) => !v);
  }

  function formatCause(cause: string): string {
    try {
      const parsed = JSON.parse(cause) as Record<string, unknown>;
      const msg = parsed.errorMessage ?? parsed.ErrorMessage;
      if (typeof msg === "string") return msg;
    } catch {
      // not JSON
    }
    return cause;
  }

  return (
    <Card className="overflow-hidden">
      <div className="flex items-center gap-3 px-3 py-2.5">
        <Badge variant={statusBadgeVariant(e.status)} className="font-mono text-[10px]">
          {e.status}
        </Badge>

        <div className="min-w-0 flex-1">
          <div className="truncate font-mono text-xs text-foreground">
            {e.name}
          </div>
          <div className="mt-0.5 text-[11px] text-muted-foreground">
            {relativeTime(e.started_at)}
            {e.stopped_at && ` · ${durationLabel(e.started_at, e.stopped_at)}`}
          </div>
        </div>

        {canExpand && (
          <button
            onClick={toggle}
            className="text-[11px] text-status-failed hover:underline"
          >
            {expanded ? "Hide details" : "Open details"}
          </button>
        )}

        <a
          href={e.console_url}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-[11px] text-primary hover:underline"
        >
          Console
          <ExternalLink className="h-3 w-3" />
        </a>
      </div>

      {expanded && (
        <div className="border-t border-border bg-background p-3">
          {detailLoading && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" />
              Loading…
            </div>
          )}
          {detailError && (
            <div className="text-xs text-status-failed">{detailError}</div>
          )}
          {detail && (
            <div className="flex flex-col gap-2.5">
              {detail.failed_step && (
                <div>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                    Failed step
                  </div>
                  <div className="font-mono text-xs text-foreground">
                    {detail.failed_step}
                  </div>
                </div>
              )}
              {(detail.step_error || detail.error) && (
                <div>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                    Error
                  </div>
                  <div className="font-mono text-xs text-foreground">
                    {detail.step_error || detail.error}
                  </div>
                </div>
              )}
              {(detail.step_cause || detail.cause) && (
                <div>
                  <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                    Cause
                  </div>
                  <pre className="m-0 whitespace-pre-wrap break-words rounded-md border border-status-failed/40 bg-status-failed/5 p-2 font-mono text-[11px] leading-relaxed text-status-failed">
                    {formatCause(detail.step_cause || detail.cause || "")}
                  </pre>
                </div>
              )}
              {detail.failed_step && (
                <ExecutionLogsBlock
                  arn={e.execution_arn}
                  step={detail.failed_step}
                />
              )}
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

// ---------------------------------------------------------------------------
// ExecutionLogsBlock — fetches & renders runner Lambda log lines for a
// failed step. Toggle is collapsed by default to avoid hitting CloudWatch
// for users who only want SFN-level cause.
// ---------------------------------------------------------------------------

function ExecutionLogsBlock({ arn, step }: { arn: string; step: string }) {
  const [show, setShow] = useState(false);
  const logs = useExecutionLogs({
    arn: show ? arn : "",
    step: show ? step : "",
  });

  return (
    <div>
      <div className="mb-1 flex items-center justify-between">
        <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          Runner logs
        </div>
        <button
          onClick={() => setShow((v) => !v)}
          className="text-[11px] text-primary hover:underline"
        >
          {show ? "Hide logs" : "Show logs"}
        </button>
      </div>
      {show && logs.isLoading && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Loader2 className="h-3 w-3 animate-spin" />
          Fetching logs…
        </div>
      )}
      {show && logs.error && (
        <div className="rounded-md border border-status-failed/40 bg-status-failed/5 p-2 text-xs text-status-failed">
          {logs.error instanceof Error ? logs.error.message : String(logs.error)}
        </div>
      )}
      {show && logs.data && logs.data.events.length === 0 && (
        <div className="text-[11px] text-muted-foreground">
          No log events found in <code className="font-mono">{logs.data.log_group}</code> for the
          step's time window.
          {logs.data.source === "local"
            ? " The runner may not have written to this file yet, or the step never ran."
            : " The Lambda may not have started, or logs are still ingesting (CloudWatch can lag a few seconds)."}
        </div>
      )}
      {show && logs.data && logs.data.events.length > 0 && (
        <pre className="m-0 max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-md border border-border bg-background p-2 font-mono text-[11px] leading-relaxed text-foreground">
          {logs.data.events.map((ev, i) => (
            <div key={i} className="flex gap-3">
              <span className="flex-shrink-0 text-muted-foreground">
                {ev.timestamp.replace("T", " ").replace(/\.\d+Z$/, "Z")}
              </span>
              <span className="min-w-0">{ev.message}</span>
            </div>
          ))}
          {logs.data.truncated && (
            <div className="mt-1 text-muted-foreground">
              {logs.data.source === "local"
                ? `… more lines available; tail ${logs.data.log_group} for full output.`
                : "… more lines available; query CloudWatch directly for full output."}
            </div>
          )}
        </pre>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// StatusPanel
// ---------------------------------------------------------------------------

export interface StatusPanelProps {
  dir: string;
}

export function StatusPanel({ dir }: StatusPanelProps) {
  const [status, setStatus] = useState<PipelineStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);

  function reload() {
    setLoading(true);
    fetchStatus(dir)
      .then((s) => { setStatus(s); setLoading(false); })
      .catch((err) => {
        setError(err instanceof Error ? err.message : String(err));
        setLoading(false);
      });
  }

  useEffect(() => { reload(); }, [dir]);

  function handleRun() {
    setRunning(true);
    setRunError(null);
    triggerRun(dir)
      .then(() => { setRunning(false); reload(); })
      .catch((err) => {
        setRunError(err instanceof Error ? err.message : String(err));
        setRunning(false);
      });
  }

  if (loading) {
    return (
      <div className="flex h-full items-center gap-2 p-4 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading status…
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4">
        <div className="rounded-md border border-status-failed/40 bg-status-failed/10 px-3 py-2 text-xs text-status-failed">
          {error}
        </div>
      </div>
    );
  }

  if (!status?.deployed) {
    return (
      <div className="p-4">
        <Card className="border-dashed">
          <CardContent className="flex flex-col items-center gap-2 px-6 py-8 text-center">
            <div className="text-base font-semibold text-muted-foreground">
              Not deployed
            </div>
            <div className="text-xs leading-relaxed text-muted-foreground">
              No <code className="rounded bg-muted px-1 py-0.5 font-mono text-foreground">terraform.tfstate</code> found.
              Run <code className="rounded bg-muted px-1 py-0.5 font-mono text-foreground">terraform apply</code> in the
              pipeline directory to deploy.
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="h-full overflow-y-auto p-4 text-foreground">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          {status.state_machine_arn && (
            <>
              <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                State Machine
              </div>
              <div className="break-all font-mono text-[11px] text-muted-foreground">
                {status.state_machine_arn}
              </div>
            </>
          )}
        </div>
        <Button
          onClick={handleRun}
          disabled={running}
          size="sm"
          className={cn("flex-shrink-0", running && "opacity-70")}
        >
          {running ? (
            <>
              <Loader2 className="h-3 w-3 animate-spin" />
              Starting…
            </>
          ) : (
            <>
              <Play className="h-3 w-3" />
              Run now
            </>
          )}
        </Button>
      </div>

      {runError && (
        <div className="mb-3 rounded-md border border-status-failed/40 bg-status-failed/10 px-3 py-2 text-xs text-status-failed">
          {runError}
        </div>
      )}

      <div className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        Recent Executions ({status.executions.length})
      </div>

      {status.executions.length === 0 && (
        <div className="text-xs text-muted-foreground">No executions found.</div>
      )}

      <div className="flex flex-col gap-1.5">
        {status.executions.map((e) => (
          <ExecutionRow key={e.name} e={e} />
        ))}
      </div>
    </div>
  );
}

export default StatusPanel;
