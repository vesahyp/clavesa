import "modern-normalize";
import "./index.css";
import { StrictMode, Suspense, lazy } from "react";
import { createRoot } from "react-dom/client";
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
import { Runner } from "@/pages/Runner";
import { Notebooks } from "@/pages/Notebooks";
import { Notebook } from "@/pages/Notebook";
import { Query } from "@/pages/Query";

// Dashboard pages pull recharts (~110 KB gzipped). Lazy so /, /tables/*,
// and /pipelines/* don't pay for charts they don't render.
const Dashboards = lazy(() =>
  import("@/pages/Dashboards").then((m) => ({ default: m.Dashboards })),
);
const Dashboard = lazy(() =>
  import("@/pages/Dashboard").then((m) => ({ default: m.Dashboard })),
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

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={200}>
        <SidebarProvider>
          <BrowserRouter>
            <WorkspaceGate>
              <Routes>
                {/* Every chrome-bearing page renders inside the AppShell
                    layout route: persistent sidebar + breadcrumb header. */}
                <Route element={<AppShell />}>
                  <Route path="/" element={<Catalog />} />
                  <Route
                    path="/tables/:catalog/:schema/:table"
                    element={<TableDetail />}
                  />
                  <Route path="/sources" element={<Sources />} />
                  <Route path="/credentials" element={<Credentials />} />
                  <Route path="/runner" element={<Runner />} />
                  <Route path="/notebooks" element={<Notebooks />} />
                  <Route path="/notebooks/:name" element={<Notebook />} />
                  <Route path="/query" element={<Query />} />
                  <Route path="/pipelines" element={<PipelinesList />} />
                  <Route
                    path="/pipelines/dashboard"
                    element={<PipelineDashboard />}
                  />
                  <Route path="/pipelines/run" element={<RunDetail />} />
                  <Route path="/backfills" element={<BackfillDetail />} />
                  <Route path="/editor" element={<EditorRedirect />} />
                  <Route
                    path="/dashboards"
                    element={
                      <Suspense fallback={null}>
                        <Dashboards />
                      </Suspense>
                    }
                  />
                  <Route
                    path="/dashboards/:slug"
                    element={
                      <Suspense fallback={null}>
                        <Dashboard />
                      </Suspense>
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
          </BrowserRouter>
        </SidebarProvider>
        <Toaster />
      </TooltipProvider>
    </QueryClientProvider>
  </StrictMode>,
);
