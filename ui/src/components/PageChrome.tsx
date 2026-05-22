/**
 * PageChrome — lets a routed page publish its header chrome (breadcrumbs,
 * trailing actions, layout mode) up to the persistent AppShell.
 *
 * AppShell is a layout route rendered once; the page inside <Outlet/> calls
 * `useChrome(...)` to declare what the header should show. Writing happens in
 * a layout effect so there is no empty-header frame on navigation.
 *
 * Two contexts on purpose: the *setter* is stable and never changes, so a
 * page reading it via `useChrome` does not re-render when the chrome value
 * updates — that would otherwise risk a render loop. AppShell reads the
 * *value* context. Pages should still pass a memoized object to `useChrome`
 * so the shell only re-renders on real changes.
 */

import {
  createContext,
  useContext,
  useLayoutEffect,
  useState,
  type ReactNode,
} from "react";

import type { Crumb } from "./Breadcrumbs";

export interface PageChrome {
  breadcrumbs: Crumb[];
  /** Page-specific actions, right-aligned in the header. */
  trailing?: ReactNode;
  /** True for the editor: header stays, but <main> is full-bleed (no scroll). */
  fullBleed?: boolean;
}

const EMPTY_CHROME: PageChrome = { breadcrumbs: [] };

const ChromeValueContext = createContext<PageChrome>(EMPTY_CHROME);
const ChromeSetterContext = createContext<((c: PageChrome) => void) | null>(null);

export function PageChromeProvider({ children }: { children: ReactNode }) {
  const [chrome, setChrome] = useState<PageChrome>(EMPTY_CHROME);
  return (
    <ChromeSetterContext.Provider value={setChrome}>
      <ChromeValueContext.Provider value={chrome}>
        {children}
      </ChromeValueContext.Provider>
    </ChromeSetterContext.Provider>
  );
}

/** Read the current chrome — used by AppShell to render the header. */
export function usePageChrome(): PageChrome {
  return useContext(ChromeValueContext);
}

/** Declare this page's header chrome. Pass a memoized object. */
export function useChrome(chrome: PageChrome): void {
  const setChrome = useContext(ChromeSetterContext);
  if (!setChrome) {
    throw new Error("useChrome must be used within a PageChromeProvider");
  }
  useLayoutEffect(() => {
    setChrome(chrome);
  }, [setChrome, chrome]);
}
