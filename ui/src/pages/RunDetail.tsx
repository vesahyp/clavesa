/**
 * RunDetail — kept as a redirect shell for backwards compatibility with
 * bookmarks / notifications that pointed at `/pipelines/run?dir=…&run=…`.
 *
 * The drill-down content moved into a right-side Sheet on the pipeline
 * dashboard (`/pipelines/dashboard?dir=…&run=…`); the dashboard owns
 * grid + sheet + URL state. This file rewrites the URL on mount and
 * lets the dashboard take over.
 */

import { useEffect } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

export function RunDetail() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const dir = searchParams.get("dir") ?? "";
  const runId = searchParams.get("run") ?? "";

  useEffect(() => {
    if (!dir) {
      navigate("/pipelines", { replace: true });
      return;
    }
    const params = new URLSearchParams();
    params.set("dir", dir);
    if (runId) params.set("run", runId);
    navigate(`/pipelines/dashboard?${params.toString()}`, { replace: true });
  }, [dir, runId, navigate]);

  return null;
}
