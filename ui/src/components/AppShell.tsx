/**
 * AppShell — the persistent application frame.
 *
 * Rendered once as a react-router layout route: collapsible sidebar + a
 * breadcrumb header + the routed page in <Outlet/>. Because it sits above
 * the page routes it does not remount on navigation, so the sidebar's
 * collapse state and animation stay continuous.
 *
 * The page declares its breadcrumbs / trailing actions / layout mode via
 * `useChrome` (see PageChrome).
 */

import { Outlet } from "react-router-dom";

import { useReadOnly } from "@/hooks/useReadOnly";
import { cn } from "@/lib/utils";
import { AppSidebar } from "./AppSidebar";
import { AwsIdentityChip } from "./AwsIdentityChip";
import { Breadcrumbs } from "./Breadcrumbs";
import { HostedSettings } from "./HostedSettings";
import { TerraformBackendControl } from "./TerraformBackendControl";
import { WarehouseToggle } from "./WarehouseToggle";
import { PageChromeProvider, usePageChrome } from "./PageChrome";
import { PhoneNav } from "./PhoneNav";
import { RuntimeStatus } from "./RuntimeStatus";

function ShellFrame() {
  const { breadcrumbs, trailing, fullBleed } = usePageChrome();
  // Read-only (ADR-032): the backend switch is hidden; the warehouse and
  // AWS chips render as plain labels (they read readOnly themselves).
  const readOnly = useReadOnly();
  return (
    <div className="flex h-screen w-screen overflow-hidden bg-background text-foreground">
      <AppSidebar />
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center gap-4 border-b border-border bg-background px-4 md:px-6">
          <Breadcrumbs items={breadcrumbs} />
          <div className="ml-auto flex shrink-0 items-center gap-3">
            {/* Phone (below md): the workspace chips do not fit; the
                read-only badge and the page's own actions stay. */}
            <div className="hidden items-center gap-3 md:flex">
              <RuntimeStatus />
              <AwsIdentityChip />
              {!readOnly && <TerraformBackendControl />}
              <HostedSettings />
              <WarehouseToggle />
            </div>
            {readOnly && (
              <span
                data-testid="read-only-badge"
                className="rounded-md border border-border px-2 py-0.5 text-[11px] text-muted-foreground"
              >
                Read-only
              </span>
            )}
            {trailing}
          </div>
        </header>
        <main
          className={cn(
            "relative flex-1",
            fullBleed ? "min-h-0 overflow-hidden" : "overflow-y-auto",
            // Room for PhoneNav below md.
            "pb-[calc(3.75rem+env(safe-area-inset-bottom))] md:pb-0",
          )}
        >
          <Outlet />
        </main>
      </div>
      <PhoneNav />
    </div>
  );
}

export function AppShell() {
  return (
    <PageChromeProvider>
      <ShellFrame />
    </PageChromeProvider>
  );
}
