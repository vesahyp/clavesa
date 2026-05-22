/**
 * PipelineHealthHeader — the pinned "is this pipeline healthy?" banner at
 * the top of the per-pipeline dashboard.
 *
 * Purely presentational: the dashboard computes the verdict and passes it
 * in. Sticks to the top of the scroll area so the health summary stays
 * visible while the user scrolls the runs grid.
 */

import type { ReactNode } from "react";

import { formatDuration, formatRelative } from "@/lib/format";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { Health } from "@/lib/runStatus";
import type { Run } from "@/lib/queries";

const HEALTH: Record<Health, { label: string; dot: string; text: string }> = {
  healthy: { label: "Healthy", dot: "bg-status-success", text: "text-status-success" },
  failed: { label: "Last run failed", dot: "bg-status-failed", text: "text-status-failed" },
  running: {
    label: "Running",
    dot: "bg-status-running animate-pulse",
    text: "text-status-running",
  },
  "never-run": {
    label: "Never run",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
  unknown: {
    label: "Status unknown",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
};

export interface PipelineHealthHeaderProps {
  pipelineName: string;
  dir: string;
  nodeCount: number;
  state: Health;
  /** Fraction 0..1 of successful runs over the recent window, or null. */
  successRate: number | null;
  successWindow: number;
  lastRun: Run | null;
  cloud?: string;
  moduleSlot?: ReactNode;
  /**
   * Right-side action slot — pipeline-specific buttons that belong here
   * rather than in the global app header (Run, Settings, ValidationBadge).
   * The AppShell top bar holds workspace-level affordances only.
   */
  actionsSlot?: ReactNode;
}

function Stat({
  label,
  value,
  sub,
}: {
  label: string;
  value: string;
  sub?: string;
}) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </span>
      <span className="text-sm">
        {value}
        {sub && <span className="ml-1 text-xs text-muted-foreground">{sub}</span>}
      </span>
    </div>
  );
}

export function PipelineHealthHeader({
  pipelineName,
  dir,
  nodeCount,
  state,
  successRate,
  successWindow,
  lastRun,
  cloud,
  moduleSlot,
  actionsSlot,
}: PipelineHealthHeaderProps) {
  const h = HEALTH[state];
  return (
    <div className="sticky top-0 z-30 -mx-6 -mt-8 mb-6 border-b border-border bg-background px-6 py-4">
      <div className="flex flex-wrap items-center justify-between gap-x-8 gap-y-3">
        <div className="flex items-center gap-3">
          <span className={cn("h-2.5 w-2.5 shrink-0 rounded-full", h.dot)} />
          <div className="min-w-0">
            <div className="flex flex-wrap items-baseline gap-x-2.5">
              <h1 className="font-mono text-xl font-semibold tracking-tight">
                {pipelineName || "(unknown pipeline)"}
              </h1>
              <span className={cn("text-sm font-medium", h.text)}>
                {h.label}
              </span>
            </div>
            <p className="truncate text-xs text-muted-foreground">
              <code className="font-mono">{dir}</code>
              <span className="mx-1.5">·</span>
              {nodeCount} node{nodeCount === 1 ? "" : "s"}
            </p>
          </div>
        </div>

        <div className="flex items-center gap-6">
          {lastRun && (
            <Stat
              label="Last run"
              value={lastRun.started_at ? formatRelative(lastRun.started_at) : "—"}
              sub={
                lastRun.duration_ms != null
                  ? formatDuration(lastRun.duration_ms)
                  : undefined
              }
            />
          )}
          {successRate != null && (
            <Stat
              label="Success rate"
              value={`${Math.round(successRate * 100)}%`}
              sub={`last ${successWindow}`}
            />
          )}
          <div className="flex items-center gap-2">
            {moduleSlot}
            {cloud === "aws" && (
              <Badge variant="outline" className="font-mono">
                AWS
              </Badge>
            )}
          </div>
          {actionsSlot && (
            <div className="flex items-center gap-2 border-l border-border pl-4">
              {actionsSlot}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
