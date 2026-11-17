import "modern-normalize";
import "./index.css";
import { StrictMode, Suspense, lazy, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { runtimeConfig as rc } from "@/lib/runtimeConfig";
import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  useLocation,
} from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { TooltipProvider } from "@/components/ui/tooltip";
import { Toaster } from "@/components/ui/sonner";
import { WorkspaceGate } from "@/components/WorkspaceGate";
import { AppShell } from "@/components/AppShell";
import { SidebarProvider } from "@/components/SidebarContext";
import { Catalog } from "@/pages/Catalog";
import { TableDetail, LegacyTableDetailRedirect } from "@/pages/TableDetail";
import { PipelinesList } from "@/pages/PipelinesList";
import { PipelineDashboard } from "@/pages/PipelineDashboard";
import { RunDetail } from "@/pages/RunDetail";
import { BackfillDetail } from "@/pages/BackfillDetail";
import { Sources } from "@/pages/Sources";
import { Credentials } from "@/pages/Credentials";
import { UnityCatalog } from "@/pages/UnityCatalog";
import { Runner } from "@/pages/Runner";
import { Notebooks } from "@/pages/Notebooks";
import { Notebook } from "@/pages/Notebook";
import { Query } from "@/pages/Query";
import { Bundles } from "@/pages/Bundles";
import { Workspaces } from "@/pages/Workspaces";
import { NOT_HOSTED, NotHosted, OnTheMac } from "@/pages/NotHosted";
import { WorkspaceHome } from "@/pages/WorkspaceHome";
import { atWorkspacesRoot, runtimeConfig } from "@/lib/runtimeConfig";

// Dashboard pages pull recharts (~110 KB gzipped). Lazy so /, /tables/*,
// and /pipelines/* don't pay for charts they don't render.
// Read-only (ADR-033): Query, Credentials and Runner cannot work here.
// Their entries stay in the navigation and open a page that says what
// to do instead.
function readOnlyAlt(el: ReactNode, title: string, line: string): ReactNode {
  return rc.readOnly ? <NotHosted title={title} line={line} /> : el;
}

const Dashboards = lazy(() =>
  import("@/pages/Dashboards").then((m) => ({ default: m.Dashboards })),
);
const Dashboard = lazy(() =>
  import("@/pages/Dashboard").then((m) => ({ default: m.Dashboard })),
);
// Hosted mode (ADR-032) only: the sign-in gate and its Cognito client.
// Lazy, so the local and multi-workspace UIs never download it.
const HostedGate = lazy(() => import("@/hosted/HostedGate"));
// Hosted mode only: the crash boundary that reports to the tracker, and
// the Stats page (recharts, so lazy like the dashboards).
const HostedCrash = lazy(() =>
  import("@/hosted/CrashBoundary").then((m) => ({
    default: ({ children }: { children: ReactNode }) => (
      <m.CrashBoundary>
        <m.ThrowProbe />
        {children}
      </m.CrashBoundary>
    ),
  })),
);
const Stats = lazy(() =>
  import("@/pages/Stats").then((m) => ({ default: m.Stats })),
);

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Server data is cheap to refetch; staleness is OK for catalog/table
      // tiles. Per-query overrides for live polling.
      staleTime: 30_000,
      refetchOnWindowFocus: true,
      retry: 1,
    },
  },
});

/**
 * EditorRedirect — preserves bookmarked /editor?dir=<p> URLs. The editor
 * has folded into the pipeline dashboard, so /editor now redirects there
 * carrying its query string verbatim. Old bookmarks land on the new
 * surface without a manual rewrite.
 */
function EditorRedirect() {
  const location = useLocation();
  return (
    <Navigate
      to={{ pathname: "/pipelines/dashboard", search: location.search }}
      replace
    />
  );
}

const root = document.getElementById("root");
if (root === null) {
  throw new Error("Root element not found");
}

// basename is "" in the local UI and "/w/<name>" inside one workspace of a
// multi-workspace or hosted server (ADR-032), so every route below and
// every <Link to="/…"> stays inside the current workspace.
const app = (
  <BrowserRouter basename={runtimeConfig.basename || undefined}>
    {atWorkspacesRoot ? (
      <Routes>
        <Route path="/" element={<Workspaces />} />
        {runtimeConfig.mode === "hosted" && (
          <Route
            path="/stats"
            element={
              <Suspense fallback={null}>
                <Stats />
              </Suspense>
            }
          />
        )}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    ) : (
      <WorkspaceGate>
        <Routes>
          {/* Every chrome-bearing page renders inside the AppShell
              layout route: persistent sidebar + breadcrumb header. */}
          <Route element={<AppShell />}>
            {/* Local UI: "/" is the Catalog (the README walks
                it). Inside a workspace of a multi-workspace or
                hosted server "/" is the workspace home and the
                Catalog is "/catalog" (ADR-032). */}
            <Route
              path="/"
              element={
                runtimeConfig.mode === "local" ? (
                  <Catalog />
                ) : (
                  <WorkspaceHome />
                )
              }
            />
            {runtimeConfig.mode !== "local" && (
              <Route path="/catalog" element={<OnTheMac title="Catalog" snapshot><Catalog /></OnTheMac>} />
            )}
            <Route path="/overview" element={<WorkspaceHome />} />
            <Route
              path="/tables/:catalog/:schema/:table"
              element={<OnTheMac title="Table"><TableDetail /></OnTheMac>}
            />
            <Route path="/sources" element={<OnTheMac title="Sources"><Sources /></OnTheMac>} />
            <Route path="/credentials" element={readOnlyAlt(<Credentials />, "Credentials", NOT_HOSTED.credentials)} />
            <Route path="/unity-catalog" element={<OnTheMac title="Unity Catalog"><UnityCatalog /></OnTheMac>} />
            <Route path="/runner" element={<OnTheMac title="Runner"><Runner /></OnTheMac>} />
            <Route path="/notebooks" element={<OnTheMac title="Notebooks"><Notebooks /></OnTheMac>} />
            <Route path="/notebooks/:name" element={<OnTheMac title="Notebook"><Notebook /></OnTheMac>} />
            <Route path="/query" element={readOnlyAlt(<Query />, "Query", NOT_HOSTED.query)} />
            <Route path="/pipelines" element={<PipelinesList />} />
            <Route path="/bundles" element={<OnTheMac title="Bundles"><Bundles /></OnTheMac>} />
            <Route
              path="/pipelines/dashboard"
              element={<PipelineDashboard />}
            />
            <Route path="/pipelines/run" element={<RunDetail />} />
            <Route path="/backfills" element={<OnTheMac title="Backfill"><BackfillDetail /></OnTheMac>} />
            <Route path="/editor" element={<EditorRedirect />} />
            <Route
              path="/dashboards"
              element={
                <OnTheMac title="Dashboards" snapshot>
                  <Suspense fallback={null}>
                    <Dashboards />
                  </Suspense>
                </OnTheMac>
              }
            />
            <Route
              path="/dashboards/:slug"
              element={
                <OnTheMac title="Dashboard" snapshot>
                  <Suspense fallback={null}>
                    <Dashboard />
                  </Suspense>
                </OnTheMac>
              }
            />
          </Route>

          {/* Pre-ADR-016 URL form — :database is `<catalog>__<schema>`.
             Redirect to the three-level form so old bookmarks work.
             No chrome needed: it only renders <Navigate>. */}
          <Route
            path="/tables/:database/:table"
            element={<LegacyTableDetailRedirect />}
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </WorkspaceGate>
    )}
  </BrowserRouter>
);

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={200}>
        <SidebarProvider>
          {/* Hosted (ADR-032): no page renders before sign-in, and a
              crash reports to the tracker (tracker.js, loaded by the
              hosted index.html only) and shows a reload screen. */}
          {runtimeConfig.mode === "hosted" ? (
            <Suspense fallback={null}>
              <HostedCrash>
                <HostedGate>{app}</HostedGate>
              </HostedCrash>
            </Suspense>
          ) : (
            app
          )}
        </SidebarProvider>
        <Toaster />
      </TooltipProvider>
    </QueryClientProvider>
  </StrictMode>,
);
