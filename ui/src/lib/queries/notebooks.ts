/**
 * notebooks.ts — workspace .ipynb registry + cell execution (Slice 1):
 * summaries, the full-notebook read/write pair, cell run/cancel, session
 * teardown, and graduating a cell into a pipeline transform.
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { BASE_URL, request } from "@/api/client";
import { ServedInfo, requestParsed, requestVoid } from "./core";

const NotebookSummary = z.object({
  name: z.string(),
  cell_count: z.number(),
  updated_at: z.string(),
});
export type NotebookSummary = z.infer<typeof NotebookSummary>;

const NotebookCellClavesaMeta = z.object({
  last_run_at: z.string().optional().default(""),
  last_duration_ms: z.number().optional().default(0),
  last_status: z.string().optional().default(""),
});

const NotebookCellMetadata = z.object({
  clavesa: NotebookCellClavesaMeta.optional(),
});

const NotebookOutput = z.object({
  output_type: z.string(),
  name: z.string().optional(),
  text: z.array(z.string()).optional(),
  ename: z.string().optional(),
  evalue: z.string().optional(),
  traceback: z.array(z.string()).optional(),
  execution_count: z.number().nullable().optional(),
  data: z.record(z.string(), z.unknown()).optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});
export type NotebookOutput = z.infer<typeof NotebookOutput>;

const NotebookCell = z.object({
  cell_type: z.enum(["code", "markdown"]),
  id: z.string(),
  source: z.array(z.string()),
  metadata: NotebookCellMetadata.optional().default({}),
  execution_count: z.number().nullable().optional(),
  outputs: z.array(NotebookOutput).optional().default([]),
});
export type NotebookCell = z.infer<typeof NotebookCell>;

const Notebook = z.object({
  // nbformat-required scalars
  nbformat: z.number(),
  nbformat_minor: z.number(),
  metadata: z.object({
    kernelspec: z.object({ name: z.string(), display_name: z.string() }),
    language_info: z.object({ name: z.string() }),
    clavesa: z.object({ format_version: z.number() }),
  }),
  cells: z.array(NotebookCell),
  // clavesa convenience — service layer surfaces the filename name here
  // even though nbformat itself doesn't carry it. Optional for safety on
  // older notebooks.
  name: z.string().optional().default(""),
});
export type Notebook = z.infer<typeof Notebook>;

const NotebooksListResponse = z.object({
  notebooks: z.array(NotebookSummary).default([]),
});

const CellDisplay = z.object({
  type: z.enum(["table", "text", "none"]),
  columns: z.array(z.string()).optional(),
  column_types: z.array(z.string()).optional(),
  rows: z.array(z.array(z.unknown())).optional(),
  truncated: z.boolean().optional(),
  text_repr: z.string(),
});

const CellError = z.object({
  ename: z.string(),
  evalue: z.string(),
  traceback: z.array(z.string()),
});

const CellResult = z.object({
  status: z.enum(["ok", "error", "cancelled"]),
  duration_ms: z.number(),
  stdout: z.string(),
  stderr: z.string(),
  display: CellDisplay.optional(),
  error: CellError.optional(),
  served: ServedInfo.optional(),
});
export type CellResult = z.infer<typeof CellResult>;

const CellRunResult = z.object({
  cell: NotebookCell,
  result: CellResult,
  // ADR-024 engine identity. The backend stamps `served` next to `result`;
  // CellResult.served above tolerates the sibling-of-status placement too.
  served: ServedInfo.optional(),
});
export type CellRunResult = z.infer<typeof CellRunResult>;

/** GET /api/notebooks — lightweight summaries of every workspace notebook. */
export function useNotebooks() {
  return useQuery({
    queryKey: ["notebooks"],
    queryFn: async () => {
      const raw = await request<unknown>("/notebooks");
      return NotebooksListResponse.parse(raw);
    },
  });
}

/** GET /api/notebooks/{name} — full notebook (cells + outputs). */
export function useNotebook(name: string | null | undefined) {
  return useQuery({
    queryKey: ["notebook", name ?? ""],
    enabled: !!name,
    queryFn: async () => {
      const raw = await request<unknown>(`/notebooks/${encodeURIComponent(name!)}`);
      return Notebook.parse(raw);
    },
  });
}

/** POST /api/notebooks — create empty notebook. */
export async function createNotebook(name: string): Promise<Notebook> {
  return requestParsed("/notebooks", Notebook, {
    method: "POST",
    body: JSON.stringify({ name }),
    errorLabel: "POST /notebooks",
  });
}

/** PATCH /api/notebooks/{name} — save full notebook (cells + metadata). */
export async function saveNotebook(name: string, nb: Notebook): Promise<Notebook> {
  return requestParsed(`/notebooks/${encodeURIComponent(name)}`, Notebook, {
    method: "PATCH",
    body: JSON.stringify(nb),
    errorLabel: `PATCH /notebooks/${name}`,
  });
}

/** DELETE /api/notebooks/{name}. */
export async function deleteNotebook(name: string): Promise<void> {
  return requestVoid(`/notebooks/${encodeURIComponent(name)}`, {
    method: "DELETE",
    errorLabel: `DELETE /notebooks/${name}`,
    require204: true,
  });
}

/** POST /api/notebooks/{name}/clear-outputs. */
export async function clearNotebookOutputs(name: string): Promise<Notebook> {
  return requestParsed(
    `/notebooks/${encodeURIComponent(name)}/clear-outputs`,
    Notebook,
    { method: "POST", errorLabel: `POST /notebooks/${name}/clear-outputs` },
  );
}

/** POST /api/notebooks/{name}/cells/{cellId}/run — blocks until cell finishes. */
export async function runNotebookCell(
  name: string,
  cellId: string,
): Promise<CellRunResult> {
  return requestParsed(
    `/notebooks/${encodeURIComponent(name)}/cells/${encodeURIComponent(cellId)}/run`,
    CellRunResult,
    { method: "POST", errorLabel: "POST run" },
  );
}

/** POST /api/notebooks/{name}/cells/{cellRunId}/cancel.
 *
 * Fire-and-forget by design (no status check) — cancellation is a best-
 * effort interrupt and the run call's own result reports the outcome.
 */
export async function cancelNotebookCell(
  name: string,
  cellRunId: string,
): Promise<void> {
  await fetch(
    `${BASE_URL}/notebooks/${encodeURIComponent(name)}/cells/${encodeURIComponent(cellRunId)}/cancel`,
    { method: "POST" },
  );
}

/** DELETE /api/notebooks/{name}/session — kill the REPL subprocess.
 * Fire-and-forget, like cancelNotebookCell. */
export async function stopNotebookSession(name: string): Promise<void> {
  await fetch(
    `${BASE_URL}/notebooks/${encodeURIComponent(name)}/session`,
    { method: "DELETE" },
  );
}

/** POST /api/notebooks/{name}/cells/{cellId}/graduate — turn cell into transform. */
export async function graduateNotebookCell(
  name: string,
  cellId: string,
  body: { pipeline: string; transform_name: string },
): Promise<void> {
  return requestVoid(
    `/notebooks/${encodeURIComponent(name)}/cells/${encodeURIComponent(cellId)}/graduate`,
    {
      method: "POST",
      body: JSON.stringify(body),
      errorLabel: "POST graduate",
    },
  );
}
