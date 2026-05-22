/**
 * ValidationBadge — header badge showing pipeline validation status.
 *
 * Red if there are errors, yellow if warnings only, hidden if clean.
 * Clicking opens an inline dropdown listing all messages.
 */

import { useState } from "react";
import { AlertTriangle } from "lucide-react";

import type { ValidationMessage } from "../types/pipeline";
import { cn } from "@/lib/utils";

interface ValidationBadgeProps {
  errors: ValidationMessage[];
  warnings: ValidationMessage[];
}

export function ValidationBadge({ errors, warnings }: ValidationBadgeProps) {
  const [open, setOpen] = useState(false);

  if (errors.length === 0 && warnings.length === 0) return null;

  const isError = errors.length > 0;
  const label = isError
    ? `${errors.length} error${errors.length !== 1 ? "s" : ""}`
    : `${warnings.length} warning${warnings.length !== 1 ? "s" : ""}`;

  const allMessages = [
    ...errors.map((m) => ({ ...m, level: "error" as const })),
    ...warnings.map((m) => ({ ...m, level: "warning" as const })),
  ];

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setOpen((o) => !o)}
        className={cn(
          "inline-flex items-center gap-1.5 rounded-full border px-3 py-0.5 text-xs font-semibold transition-colors",
          isError
            ? "border-status-failed/40 bg-status-failed/10 text-status-failed hover:bg-status-failed/20"
            : "border-status-running/40 bg-status-running/10 text-status-running hover:bg-status-running/20"
        )}
        aria-label={`${label}, click to see details`}
      >
        <AlertTriangle className="h-3 w-3" />
        {label}
      </button>

      {open && (
        <>
          <div
            className="fixed inset-0 z-40"
            onClick={() => setOpen(false)}
          />
          <div
            className="absolute right-0 top-[calc(100%+6px)] z-50 max-h-80 w-80 max-w-md overflow-y-auto rounded-md border border-border bg-popover p-2 shadow-lg"
            role="region"
            aria-label="Validation messages"
          >
            {allMessages.map((msg, i) => (
              <div
                key={i}
                className={cn(
                  "rounded-sm border p-2",
                  i < allMessages.length - 1 && "mb-1",
                  msg.level === "error"
                    ? "border-status-failed/40 bg-status-failed/10"
                    : "border-status-running/40 bg-status-running/10"
                )}
              >
                <div
                  className={cn(
                    "mb-0.5 text-[10px] font-bold uppercase tracking-wider",
                    msg.level === "error"
                      ? "text-status-failed"
                      : "text-status-running"
                  )}
                >
                  {msg.code}
                </div>
                <div className="text-xs text-foreground">{msg.message}</div>
                {msg.nodes && msg.nodes.length > 0 && (
                  <div className="mt-0.5 text-[11px] text-muted-foreground">
                    Nodes: {msg.nodes.join(", ")}
                  </div>
                )}
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
