/**
 * useReadOnly — true when the server runs read-only (`clavesa ui
 * --read-only`, and always on the hosted UI; ADR-032). Components use it
 * to HIDE the controls that change state: run, deploy, edit, delete,
 * create, the DAG editor's edit mode and settings writes. Hidden, not
 * disabled: a read-only page shows only what can be done. The server
 * refuses those requests too (405), so this is the UI half of the guard.
 *
 * The value comes from the runtime config and is fixed for the life of
 * the page, so this is a plain read; it is a hook so a later per-viewer
 * permission can replace it without touching the call sites.
 */

import { runtimeConfig } from "@/lib/runtimeConfig";

export function useReadOnly(): boolean {
  return runtimeConfig.readOnly;
}
