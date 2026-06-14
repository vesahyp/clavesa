/**
 * EngineBadge — "what runs this / what served this" chip (ADR-024).
 *
 * The warehouse/compute split made engine identity legible: SQL-running
 * endpoints stamp a `served` object on their response describing which
 * engine answered (Spark vs Athena), against which warehouse, and whether
 * the SQL was transpiled. This badge renders that.
 *
 * Because the dialect differs by engine (Spark vs Athena/Trino), the most
 * useful moment to know the engine is BEFORE you author or run — not after.
 * So when `served` is absent (an empty editor, a cell you haven't run), the
 * badge predicts from the workspace warehouse + the surface kind. The
 * prediction is deterministic and matches exactly what the response stamps,
 * so it never has to walk anything back — the response only adds the
 * `(transpiled)` qualifier and drops the "predicted" dimming. A predicted
 * badge is rendered at reduced opacity and carries `data-predicted`.
 *
 * Surface kind:
 *   - "serving"   — /query, dashboard widgets, table sample rows. Cloud →
 *                   Athena; local → Spark.
 *   - "authoring" — notebooks, editor preview. Always Spark (these can never
 *                   run on Athena), against whichever warehouse.
 *
 * Copy:
 *   - Spark always runs in the local docker worker → "Spark · local docker
 *     · <wh> warehouse".
 *   - Athena is the cloud serving engine, no docker qualifier → "Athena
 *     (transpiled) · cloud warehouse".
 */

import type { ServedInfo } from "@/lib/queries";
import { useWarehouse } from "@/lib/queries";

type Surface = "serving" | "authoring";

/** Deterministic engine prediction from warehouse + surface — the same
 * mapping the backend providers stamp, so prediction == confirmation for
 * engine + warehouse (only `transpiled` is unknown until the run). */
function predict(
  warehouse: "local" | "cloud" | undefined,
  surface: Surface | undefined,
): ServedInfo | undefined {
  if (!warehouse || !surface) return undefined;
  const engine = warehouse === "cloud" && surface === "serving" ? "athena" : "spark";
  return { engine, warehouse };
}

export function EngineBadge({
  served,
  surface,
  testid,
}: {
  /** Per-response metadata once the request has run. */
  served?: ServedInfo;
  /** Surface kind, used to predict the engine before `served` arrives. Omit
   * (with no `served`) to render nothing — the back-compat default. */
  surface?: Surface;
  testid: string;
}) {
  // The hook is cheap (cached, deduped) and only feeds the prediction
  // fallback; it must run unconditionally to keep hook order stable.
  const warehouseQuery = useWarehouse();
  const warehouse = warehouseQuery.data?.warehouse as
    | "local"
    | "cloud"
    | undefined;

  const info = served ?? predict(warehouse, surface);
  if (!info) return null;
  const predicted = !served;

  const engine = info.engine.charAt(0).toUpperCase() + info.engine.slice(1);
  const parts = [
    engine + (info.transpiled ? " (transpiled)" : ""),
    ...(info.engine === "spark" ? ["local docker"] : []),
    `${info.warehouse} warehouse`,
  ];
  return (
    <span
      data-testid={testid}
      data-predicted={predicted ? "" : undefined}
      title={
        predicted
          ? "Predicted engine and warehouse for this surface — confirmed once it runs"
          : "Engine and warehouse that served this result"
      }
      className={
        "inline-flex items-center whitespace-nowrap rounded border border-border bg-muted/40 px-1.5 py-0.5 font-mono text-[11px] text-muted-foreground" +
        (predicted ? " opacity-60" : "")
      }
    >
      {parts.join(" · ")}
    </span>
  );
}
