/**
 * useIsPhone — true below Tailwind's md breakpoint (768px), where the
 * pipeline and run pages swap to their phone layouts (ADR-032) and the
 * dashboard widgets flow in reading order (WidgetShell). A media
 * query hook rather than CSS hiding, so the desktop-only parts (the DAG
 * canvas, the node grid) do not mount on a phone at all.
 */

import { useEffect, useState } from "react";

const QUERY = "(max-width: 767.98px)";
// Below 640px two widgets side by side leave a chart about 165px wide,
// too narrow for its axis labels (dashboard shots of 2026-11-18).
const NARROW_QUERY = "(max-width: 639.98px)";

function matches(query: string): boolean {
  return typeof window !== "undefined" && window.matchMedia(query).matches;
}

function useMediaQuery(query: string): boolean {
  const [on, setOn] = useState(() => matches(query));
  useEffect(() => {
    const mq = window.matchMedia(query);
    const change = () => setOn(mq.matches);
    mq.addEventListener("change", change);
    return () => mq.removeEventListener("change", change);
  }, [query]);
  return on;
}

export function useIsPhone(): boolean {
  return useMediaQuery(QUERY);
}

/** True below 640px, where a dashboard shows one widget per row. */
export function useIsNarrow(): boolean {
  return useMediaQuery(NARROW_QUERY);
}
