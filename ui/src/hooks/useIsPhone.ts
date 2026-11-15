/**
 * useIsPhone — true below Tailwind's md breakpoint (768px), where the
 * pipeline and run pages swap to their phone layouts (ADR-032). A media
 * query hook rather than CSS hiding, so the desktop-only parts (the DAG
 * canvas, the node grid) do not mount on a phone at all.
 */

import { useEffect, useState } from "react";

const QUERY = "(max-width: 767.98px)";

function matches(): boolean {
  return typeof window !== "undefined" && window.matchMedia(QUERY).matches;
}

export function useIsPhone(): boolean {
  const [phone, setPhone] = useState(matches);
  useEffect(() => {
    const mq = window.matchMedia(QUERY);
    const on = () => setPhone(mq.matches);
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, []);
  return phone;
}
