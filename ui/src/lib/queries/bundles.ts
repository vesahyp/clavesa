/**
 * bundles.ts — the read-only bundle page (ADR-031 section 11): the plan
 * report of a Databricks bundle target, and the pipelines generated from
 * bundle jobs. Same service calls as `clavesa bundle plan --json` and
 * `clavesa bundle list --json` (ADR-015).
 */

import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { request } from "@/api/client";

const Finding = z.object({
  severity: z.string(),
  task: z.string().optional(),
  file: z.string().optional(),
  line: z.number().optional(),
  message: z.string(),
});

const TaskPlan = z.object({
  task_key: z.string(),
  type: z.string(),
  place: z.string(),
  status: z.string(),
  disabled: z.boolean().optional(),
  reads: z.array(z.string()).nullish(),
  writes: z.array(z.string()).nullish(),
  findings: z.array(Finding).nullish(),
});

const JobPlan = z.object({
  job: z.string(),
  pipeline: z.string(),
  schedule: z.string().optional(),
  trigger: z.string().optional(),
  segments: z.number(),
  compute: z.object({ kind: z.string(), vcpu: z.number(), memory_gb: z.number() }).nullish(),
  tasks: z.array(TaskPlan),
  findings: z.array(Finding).nullish(),
});

export const BundleReport = z.object({
  bundle: z.string(),
  target: z.string(),
  catalog: z.string(),
  shadow: z.boolean(),
  jobs: z.array(JobPlan),
  findings: z.array(Finding).nullish(),
  libraries: z.array(z.string()).nullish(),
  secrets: z
    .array(z.object({ name: z.string(), tasks: z.array(z.string()), secrets_manager: z.string() }))
    .nullish(),
  refuse: z.number(),
  warn: z.number(),
  ready: z.boolean(),
});
export type BundleReport = z.infer<typeof BundleReport>;

export const BundlePipeline = z.object({
  pipeline: z.string(),
  bundle: z.string(),
  target: z.string(),
  job: z.string(),
  catalog: z.string(),
  shadow: z.boolean(),
  tasks: z.number(),
  source: z.string().optional(),
});
export type BundlePipeline = z.infer<typeof BundlePipeline>;

/** GET /api/bundle/plan — plan a bundle folder for a target. */
export function useBundlePlan(bundle: string, target: string) {
  return useQuery({
    queryKey: ["bundle-plan", bundle, target],
    enabled: bundle !== "",
    retry: false,
    queryFn: async () => {
      const q = new URLSearchParams({ bundle, target });
      // A bundle that does not plan comes back as { error } (HTTP 200).
      const res = z
        .object({ report: BundleReport.optional(), error: z.string().optional() })
        .parse(await request<unknown>(`/bundle/plan?${q}`));
      if (res.error) throw new Error(res.error);
      return res.report as BundleReport;
    },
  });
}

/** GET /api/bundle/pipelines — pipelines generated from bundle jobs. */
export function useBundlePipelines() {
  return useQuery({
    queryKey: ["bundle-pipelines"],
    queryFn: async () => z.array(BundlePipeline).parse(await request<unknown>("/bundle/pipelines")),
  });
}
