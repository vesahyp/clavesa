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

import { cn } from "@/lib/utils";
import { AppSidebar } from "./AppSidebar";
import { AwsIdentityChip } from "./AwsIdentityChip";
import { Breadcrumbs } from "./Breadcrumbs";
import { EnvModeToggle } from "./EnvModeToggle";
import { PageChromeProvider, usePageChrome } from "./PageChrome";
import { RuntimeStatus } from "./RuntimeStatus";

function ShellFrame() {
  const { breadcrumbs, trailing, fullBleed } = usePageChrome();
  return (
    <div className="flex h-screen w-screen overflow-hidden bg-background text-foreground">
      <AppSidebar />
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center gap-4 border-b border-border bg-background px-6">
          <Breadcrumbs items={breadcrumbs} />
          <div className="ml-auto flex shrink-0 items-center gap-3">
            <RuntimeStatus />
            <AwsIdentityChip />
            <EnvModeToggle />
            {trailing}
          </div>
        </header>
        <main
          className={cn(
            "relative flex-1",
            fullBleed ? "min-h-0 overflow-hidden" : "overflow-y-auto",
          )}
        >
          <Outlet />
        </main>
      </div>
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
