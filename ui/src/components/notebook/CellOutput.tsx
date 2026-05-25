/**
 * CellOutput — render one nbformat output entry (stream / error / execute_result).
 *
 * Switches on output_type. For execute_result with the clavesa MIME bundle
 * we render the DataFrame as a compact grid; for everything else we fall
 * back to text. Markdown cells render through a sibling component because
 * they're cell-level not output-level (nbformat puts the content in the
 * cell's source, not in outputs[]).
 */

import { useMemo } from "react";
import type { NotebookOutput } from "@/lib/queries";

const CLAVESA_DF_MIME = "application/vnd.clavesa.dataframe+json";

interface DataFrameBundle {
  columns: string[];
  column_types?: string[];
  rows: unknown[][];
}

export function CellOutput({ output }: { output: NotebookOutput }) {
  switch (output.output_type) {
    case "stream":
      return <Stream name={output.name ?? "stdout"} text={(output.text ?? []).join("")} />;
    case "error":
      return (
        <ErrorOutput
          ename={output.ename ?? ""}
          evalue={output.evalue ?? ""}
          traceback={output.traceback ?? []}
        />
      );
    case "execute_result":
    case "display_data":
      return <ExecuteResult data={output.data ?? {}} truncated={!!(output.metadata as { clavesa?: { truncated?: boolean } } | undefined)?.clavesa?.truncated} />;
    default:
      return null;
  }
}

function Stream({ name, text }: { name: string; text: string }) {
  if (!text) return null;
  const isErr = name === "stderr";
  return (
    <pre
      className={
        "max-h-64 overflow-auto whitespace-pre-wrap rounded border bg-muted/40 p-3 font-mono text-xs " +
        (isErr ? "text-destructive" : "text-foreground")
      }
    >
      {text}
    </pre>
  );
}

function ErrorOutput({
  ename,
  evalue,
  traceback,
}: {
  ename: string;
  evalue: string;
  traceback: string[];
}) {
  return (
    <pre className="max-h-96 overflow-auto whitespace-pre-wrap rounded border border-destructive/40 bg-destructive/5 p-3 font-mono text-xs text-destructive">
      <div className="mb-2 font-semibold">
        {ename}: {evalue}
      </div>
      {traceback.join("")}
    </pre>
  );
}

function ExecuteResult({
  data,
  truncated,
}: {
  data: Record<string, unknown>;
  truncated?: boolean;
}) {
  const df = useMemo<DataFrameBundle | null>(() => {
    const raw = data[CLAVESA_DF_MIME];
    if (!raw || typeof raw !== "object") return null;
    const obj = raw as DataFrameBundle;
    if (!Array.isArray(obj.columns) || !Array.isArray(obj.rows)) return null;
    return obj;
  }, [data]);

  if (df) {
    return <DataFrameGrid df={df} truncated={!!truncated} />;
  }

  // Fallback: text/plain repr.
  const plain = typeof data["text/plain"] === "string" ? (data["text/plain"] as string) : "";
  if (plain) {
    return (
      <pre className="max-h-64 overflow-auto whitespace-pre-wrap rounded border bg-muted/40 p-3 font-mono text-xs">
        {plain}
      </pre>
    );
  }
  return null;
}

function DataFrameGrid({ df, truncated }: { df: DataFrameBundle; truncated: boolean }) {
  return (
    <div className="overflow-auto rounded border">
      <table className="w-full border-collapse text-xs">
        <thead className="sticky top-0 bg-muted">
          <tr>
            {df.columns.map((c, i) => (
              <th
                key={c}
                className="border-b px-2 py-1.5 text-left font-mono font-semibold"
                title={df.column_types?.[i]}
              >
                {c}
                {df.column_types?.[i] && (
                  <span className="ml-1 text-muted-foreground/70">
                    ({df.column_types[i]})
                  </span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {df.rows.slice(0, 200).map((row, i) => (
            <tr key={i} className={i % 2 ? "bg-muted/20" : undefined}>
              {row.map((cell, j) => (
                <td
                  key={j}
                  className="border-b px-2 py-1 font-mono align-top"
                >
                  {cell === null || cell === undefined
                    ? <span className="text-muted-foreground italic">null</span>
                    : typeof cell === "object"
                      ? <pre className="m-0 whitespace-pre-wrap text-[10px]">{JSON.stringify(cell, null, 2)}</pre>
                      : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="border-t bg-muted/30 px-2 py-1 text-[11px] text-muted-foreground">
        {df.rows.length} row{df.rows.length === 1 ? "" : "s"}
        {df.rows.length > 200 && <> · showing first 200</>}
        {truncated && <> · output truncated</>}
      </div>
    </div>
  );
}
