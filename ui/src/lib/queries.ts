/**
 * queries.ts — TanStack Query hooks for the data-first UI.
 *
 * Wraps the API client with React Query so pages get caching,
 * invalidation, polling, and consistent loading/error UX for free.
 *
 * Split by domain under lib/queries/ (G P2-3); this barrel re-exports
 * everything so import sites keep using "@/lib/queries".
 */

export * from "./queries/core";
export * from "./queries/catalog";
export * from "./queries/pipelines";
export * from "./queries/runs";
export * from "./queries/dashboards";
export * from "./queries/sources";
export * from "./queries/backfills";
export * from "./queries/workspace";
export * from "./queries/notebooks";
