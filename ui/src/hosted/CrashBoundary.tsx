/**
 * CrashBoundary — the hosted UI's top-level React error boundary (ADR-032
 * section 4, "Tracking and errors"; jeeves practices/web-tracking.md,
 * "Minimum error reporting"). Mounted from main.tsx in hosted mode only.
 *
 * A throw in a render used to leave a white page. The boundary reports it
 * as an `error` beacon with type "boundary" (tracker.js already reports
 * uncaught errors and rejections itself, but a boundary swallows the
 * throw, so it is reported here by hand) and shows a plain crash screen
 * with a reload button. The tracker's sanitize (hosted/static/
 * tracker-config.js) runs over msg like over every value.
 *
 * ThrowProbe is the proof the rule asks for: `?__throw=1` throws once in a
 * render, in hosted mode only (this file never loads in the local UI), so
 * `make hosted-tracking-check` can see the beacon leave the page. The
 * message carries a run id and an execution ARN on purpose: the check
 * asserts that sanitize replaced both.
 */

import { Component, type ErrorInfo, type ReactNode } from "react";

import { Button } from "@/components/ui/button";

declare global {
  interface Window {
    __clvtracker?: { track: (event: string, data?: Record<string, unknown>) => void };
  }
}

interface State {
  message: string | null;
}

export class CrashBoundary extends Component<{ children: ReactNode }, State> {
  state: State = { message: null };

  static getDerivedStateFromError(error: unknown): State {
    return { message: error instanceof Error ? error.message : String(error) };
  }

  componentDidCatch(error: unknown, _info: ErrorInfo): void {
    try {
      const msg = error instanceof Error ? error.message : String(error);
      window.__clvtracker?.track("error", { msg: msg.slice(0, 200), type: "boundary" });
    } catch {
      // A lost report must not hide the crash screen.
    }
  }

  render() {
    if (this.state.message === null) return this.props.children;
    return (
      <div
        data-testid="crash-screen"
        className="flex min-h-screen items-start justify-center bg-background px-4 pt-[max(4rem,env(safe-area-inset-top))] text-foreground"
      >
        <div className="flex w-full max-w-sm flex-col gap-4 rounded-lg border border-border bg-card p-6">
          <div>
            <div className="text-lg font-semibold tracking-tight">Something broke</div>
            <div className="text-sm text-muted-foreground">
              The page stopped with an error. It has been reported. Reload to try again.
            </div>
          </div>
          <Button
            type="button"
            data-testid="crash-reload"
            className="h-11"
            onClick={() => window.location.reload()}
          >
            Reload
          </Button>
          <a href="/" className="text-center text-sm text-muted-foreground underline-offset-4 hover:underline">
            All workspaces
          </a>
          <p className="break-words text-xs text-muted-foreground">{this.state.message}</p>
        </div>
      </div>
    );
  }
}

/** Throws in its render when the page URL has `?__throw=1`. */
export function ThrowProbe() {
  if (new URLSearchParams(window.location.search).get("__throw") === "1") {
    throw new Error(
      "clavesa test throw (__throw=1) for run 3f2b8c1e-9d4a-4e7b-8c6f-1a2b3c4d5e6f, " +
        "execution arn:aws:states:eu-north-1:699166197771:execution:clavesa-smoke2-taxi:3f2b8c1e-9d4a-4e7b-8c6f-1a2b3c4d5e6f",
    );
  }
  return null;
}
