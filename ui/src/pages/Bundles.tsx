/**
 * Bundles — the read-only view of Databricks bundles (ADR-031 section 11).
 *
 * The plan report of a bundle target (the same report as `clavesa bundle
 * plan`): every task of every job, where it runs, and ok / warn / refuse
 * with the reason. And the pipelines generated from bundle jobs (as
 * `clavesa bundle list`). The bundle is the source: nothing is edited here.
 */

import { useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { Package2 } from "lucide-react";

import { useChrome } from "@/components/PageChrome";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useBundlePipelines, useBundlePlan } from "@/lib/queries";

function statusVariant(s: string): "default" | "secondary" | "destructive" | "outline" {
  if (s === "refuse") return "destructive";
  if (s === "warn") return "secondary";
  return "outline";
}

export function Bundles() {
  const [params, setParams] = useSearchParams();
  const bundle = params.get("bundle") ?? "";
  const target = params.get("target") ?? "";
  const [pathInput, setPathInput] = useState(bundle);
  const [targetInput, setTargetInput] = useState(target);
  const plan = useBundlePlan(bundle, target);
  const pipelines = useBundlePipelines();

  useChrome(useMemo(() => ({ breadcrumbs: [{ label: "Bundles", to: "/bundles" }] }), []));

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-8" data-testid="bundles-page">
      <div className="mb-6">
        <h1 className="font-mono text-2xl font-semibold tracking-tight">Bundles</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Databricks bundle jobs that run on clavesa. The bundle (databricks.yml) is the source: change a job
          there, then run <code>clavesa bundle deploy</code>.
        </p>
      </div>

      <Card className="mb-6">
        <CardContent className="pt-6">
          <form
            className="flex flex-wrap items-end gap-3"
            onSubmit={(e) => {
              e.preventDefault();
              setParams({ bundle: pathInput.trim(), target: targetInput.trim() });
            }}
          >
            <label className="flex min-w-[24rem] flex-1 flex-col gap-1 text-sm">
              Bundle folder (with databricks.yml)
              <Input
                data-testid="bundle-path-input"
                value={pathInput}
                placeholder="/path/to/bundle"
                onChange={(e) => setPathInput(e.target.value)}
              />
            </label>
            <label className="flex w-48 flex-col gap-1 text-sm">
              Target
              <Input
                data-testid="bundle-target-input"
                value={targetInput}
                placeholder="default target"
                onChange={(e) => setTargetInput(e.target.value)}
              />
            </label>
            <Button type="submit" data-testid="bundle-plan-button" disabled={pathInput.trim() === ""}>
              Plan
            </Button>
          </form>
        </CardContent>
      </Card>

      {bundle !== "" && plan.isLoading && <Skeleton className="h-40 w-full" />}
      {plan.error && (
        <Card className="mb-6 border-destructive" data-testid="bundle-plan-error">
          <CardContent className="pt-6 text-sm text-destructive">{String(plan.error.message ?? plan.error)}</CardContent>
        </Card>
      )}
      {plan.data && (
        <div className="mb-8 space-y-4" data-testid="bundle-plan">
          <div className="flex flex-wrap items-center gap-3 text-sm">
            <span className="font-mono">
              {plan.data.bundle} · target {plan.data.target} · catalog {plan.data.catalog}
              {plan.data.shadow ? " · shadow" : ""}
            </span>
            <Badge data-testid="bundle-plan-ready" variant={plan.data.ready ? "default" : "destructive"}>
              {plan.data.ready ? "Ready" : "Not ready"}: {plan.data.refuse} refuse, {plan.data.warn} warn
            </Badge>
          </div>
          {(plan.data.findings ?? []).map((f, i) => (
            <p key={i} className="text-sm">
              <Badge variant={statusVariant(f.severity)}>{f.severity}</Badge> {f.message}
            </p>
          ))}
          {plan.data.jobs.map((j) => (
            <Card key={j.job} data-testid={`bundle-job-${j.job}`}>
              <CardContent className="pt-6">
                <div className="mb-3 flex flex-wrap items-baseline gap-x-4 gap-y-1">
                  <h2 className="font-mono text-lg font-semibold">{j.job}</h2>
                  <span className="text-xs text-muted-foreground">
                    pipeline {j.pipeline}
                    {j.schedule ? ` · schedule ${j.schedule}` : ""}
                    {j.trigger ? ` · trigger ${j.trigger}` : ""} · {j.segments}{" "}
                    {j.segments === 1 ? "segment" : "segments"}
                    {j.compute
                      ? j.compute.kind === "lambda"
                        ? ` · Lambda ${j.compute.memory_gb} GB`
                        : ` · Fargate ${j.compute.vcpu} vCPU / ${j.compute.memory_gb} GB`
                      : ""}
                  </span>
                </div>
                {(j.findings ?? []).map((f, i) => (
                  <p key={i} className="mb-2 text-sm">
                    <Badge variant={statusVariant(f.severity)}>{f.severity}</Badge> {f.message}
                  </p>
                ))}
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Task</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Runs on</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>What the plan found</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {j.tasks.map((t) => (
                      <TableRow key={t.task_key} data-testid="bundle-task-row" data-status={t.status}>
                        <TableCell className="font-mono">{t.task_key}</TableCell>
                        <TableCell>{t.type}</TableCell>
                        <TableCell>{t.place}</TableCell>
                        <TableCell>
                          <Badge variant={statusVariant(t.status)}>{t.status}</Badge>
                        </TableCell>
                        <TableCell className="text-sm">
                          {(t.findings ?? []).map((f, i) => (
                            <div key={i}>
                              {f.file ? (
                                <span className="font-mono text-xs text-muted-foreground">
                                  {f.file}
                                  {f.line ? `:${f.line}` : ""}{" "}
                                </span>
                              ) : null}
                              {f.message}
                            </div>
                          ))}
                          {(t.writes ?? []).length > 0 && (
                            <div className="text-xs text-muted-foreground">writes {(t.writes ?? []).join(", ")}</div>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          ))}
          {(plan.data.secrets ?? []).length > 0 && (
            <Card>
              <CardContent className="pt-6 text-sm">
                <h3 className="mb-2 font-semibold">Secrets (put each in AWS Secrets Manager once)</h3>
                {(plan.data.secrets ?? []).map((s) => (
                  <div key={s.name} className="font-mono text-xs">
                    {s.name} → {s.secrets_manager}
                  </div>
                ))}
              </CardContent>
            </Card>
          )}
        </div>
      )}

      <h2 className="mb-3 flex items-center gap-2 text-lg font-semibold">
        <Package2 className="h-4 w-4" /> Pipelines from bundle jobs
      </h2>
      {pipelines.isLoading && <Skeleton className="h-20 w-full" />}
      {pipelines.data && pipelines.data.length === 0 && (
        <p className="text-sm text-muted-foreground" data-testid="bundle-pipelines-empty">
          None yet: <code>clavesa bundle run</code> or <code>clavesa bundle deploy</code> generates them.
        </p>
      )}
      {pipelines.data && pipelines.data.length > 0 && (
        <Table data-testid="bundle-pipelines-table">
          <TableHeader>
            <TableRow>
              <TableHead>Pipeline</TableHead>
              <TableHead>Bundle</TableHead>
              <TableHead>Target</TableHead>
              <TableHead>Job</TableHead>
              <TableHead>Catalog</TableHead>
              <TableHead>Tasks</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pipelines.data.map((p) => (
              <TableRow key={p.pipeline} data-testid="bundle-pipeline-row">
                <TableCell className="font-mono">
                  <Link className="underline" to={`/pipelines/dashboard?dir=${encodeURIComponent(p.pipeline)}`}>
                    {p.pipeline}
                  </Link>
                </TableCell>
                <TableCell>{p.bundle}</TableCell>
                <TableCell>{p.target}</TableCell>
                <TableCell>{p.job}</TableCell>
                <TableCell>
                  {p.catalog}
                  {p.shadow ? " (shadow)" : ""}
                </TableCell>
                <TableCell>{p.tasks}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </div>
  );
}
