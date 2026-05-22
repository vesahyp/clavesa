/**
 * SqlPreview — run a dataset's SQL and show the result without saving.
 *
 * Lets the author check a query while editing it. Runs the *current draft*
 * SQL string, so unsaved edits are previewed. Goes through `fetchQuery`
 * with the shared `["dashboards","query",dir,sql]` key, so a preview run
 * also warms the column dropdowns (and the live widget grid) for free.
 */

import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Loader2, Play } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { runDashboardQuery, type DashboardQueryResult } from "@/lib/queries";

const PREVIEW_ROW_CAP = 10;

interface SqlPreviewProps {
  dir: string;
  sql: string;
}

export function SqlPreview({ dir, sql }: SqlPreviewProps) {
  const qc = useQueryClient();
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<DashboardQueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const canRun = Boolean(dir) && Boolean(sql.trim());

  async function run() {
    setRunning(true);
    setError(null);
    try {
      const r = await qc.fetchQuery({
        queryKey: ["dashboards", "query", dir, sql],
        queryFn: () => runDashboardQuery(dir, sql),
        staleTime: 5 * 60_000,
      });
      setResult(r);
    } catch (e) {
      setResult(null);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <Button
          size="sm"
          variant="outline"
          onClick={run}
          disabled={!canRun || running}
        >
          {running ? (
            <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
          ) : (
            <Play className="mr-1 h-3.5 w-3.5" />
          )}
          Run
        </Button>
        {!canRun && (
          <span className="text-xs text-muted-foreground">
            Needs a pipeline and a query.
          </span>
        )}
        {result && (
          <span className="text-xs text-muted-foreground">
            {result.row_count} row{result.row_count === 1 ? "" : "s"}
            {result.truncated && " (truncated)"}
          </span>
        )}
      </div>

      {error && (
        <div className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/5 p-3 text-xs">
          <div className="min-w-0">
            <div className="font-medium text-destructive">Query failed</div>
            <p className="mt-0.5 break-words text-muted-foreground">{error}</p>
          </div>
        </div>
      )}

      {result && !error && (
        <div className="max-h-64 overflow-auto rounded-md border border-border">
          {result.rows.length === 0 ? (
            <p className="p-3 text-xs text-muted-foreground">No rows.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  {result.columns.map((c) => (
                    <TableHead key={c.name} className="whitespace-nowrap">
                      {c.name}
                    </TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {result.rows.slice(0, PREVIEW_ROW_CAP).map((row, i) => (
                  <TableRow key={i}>
                    {row.map((cell, j) => (
                      <TableCell
                        key={j}
                        className="whitespace-nowrap font-mono text-xs"
                      >
                        {cell === "" ? (
                          <span className="text-muted-foreground">—</span>
                        ) : (
                          cell
                        )}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
          {result.rows.length > PREVIEW_ROW_CAP && (
            <p className="border-t border-border p-2 text-xs text-muted-foreground">
              Showing first {PREVIEW_ROW_CAP} of {result.rows.length} rows.
            </p>
          )}
        </div>
      )}
    </div>
  );
}
