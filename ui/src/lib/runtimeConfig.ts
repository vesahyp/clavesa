/**
 * runtimeConfig.ts — the frontend's runtime configuration (ADR-032).
 *
 * The Go server writes `window.__CLAVESA__ = { mode, basename, apiBase,
 * readOnly }` into index.html when it serves it, so one ui/ build serves
 * the local UI (`clavesa ui`), the multi-workspace UI
 * (`clavesa ui --workspaces`) and the hosted UI. This replaces the old
 * build-time VITE_API_BASE_URL.
 *
 * Multi-workspace and hosted servers scope a page to one workspace by its
 * path, /w/<name>/…: the server sets basename "/w/<name>" and apiBase
 * "/w/<name>/api" for that page. Moving to another workspace is a full
 * page load, so the config is fixed for the life of the page and every
 * existing `<Link to="/pipelines">` resolves inside the current workspace.
 * A static index.html (hosted, S3) may leave basename/apiBase out; they
 * are then derived from the path the same way.
 *
 * With no config at all (Vite dev server, `make dev`) the defaults are the
 * local UI's: basename "", apiBase "/api" (Vite proxies /api to :8080).
 */

export type ClavesaMode = "local" | "multi" | "hosted";

/** Sign-in settings of the hosted UI, written into index.html by
 * hosted/scripts/render-index.mjs from the hosted/infra outputs. */
export interface HostedCognitoConfig {
  region: string;
  userPoolId: string;
  /** The SPA app client (EMAIL_OTP through USER_AUTH, no password). */
  clientId: string;
  /** Cognito prefix domain host, for the Google code flow. */
  domain: string;
  googleEnabled: boolean;
}

export interface RuntimeConfig {
  mode: ClavesaMode;
  /** React Router basename: "" or "/w/<name>". */
  basename: string;
  /** Prefix of every API call: "/api" or "/w/<name>/api". */
  apiBase: string;
  /** Hide the controls that change state (the server refuses them too). */
  readOnly: boolean;
  /** Path-safe workspace name inside a multi/hosted page; "" at the root. */
  workspace: string;
  /** Hosted mode only: how to sign in. */
  cognito?: HostedCognitoConfig;
  /** IANA zone every day and time is shown in (clock.ts). The server
   * writes it; undefined means the browser's zone. */
  timeZone?: string;
}

declare global {
  interface Window {
    __CLAVESA__?: Partial<RuntimeConfig>;
  }
}

const WORKSPACE_PATH = /^\/w\/([^/]+)/;

export function resolveRuntimeConfig(
  raw: Partial<RuntimeConfig> | undefined,
  pathname: string,
): RuntimeConfig {
  const mode: ClavesaMode =
    raw?.mode === "multi" || raw?.mode === "hosted" ? raw.mode : "local";
  const readOnly = raw?.readOnly === true;
  const cognito = mode === "hosted" ? raw?.cognito : undefined;
  const timeZone = typeof raw?.timeZone === "string" && raw.timeZone ? raw.timeZone : undefined;
  if (mode === "local") {
    return {
      mode,
      basename: raw?.basename ?? "",
      apiBase: raw?.apiBase ?? "/api",
      readOnly,
      workspace: "",
      timeZone,
    };
  }
  if (raw?.basename !== undefined && raw?.apiBase !== undefined) {
    return {
      mode,
      basename: raw.basename,
      apiBase: raw.apiBase,
      readOnly,
      workspace: raw.workspace ?? "",
      cognito,
      timeZone,
    };
  }
  const m = WORKSPACE_PATH.exec(pathname);
  if (m) {
    const ws = decodeURIComponent(m[1]);
    return {
      mode,
      basename: `/w/${m[1]}`,
      apiBase: `/w/${m[1]}/api`,
      readOnly,
      workspace: ws,
      cognito,
      timeZone,
    };
  }
  return { mode, basename: "", apiBase: "/api", readOnly, workspace: "", cognito, timeZone };
}

export const runtimeConfig: RuntimeConfig = resolveRuntimeConfig(
  typeof window === "undefined" ? undefined : window.__CLAVESA__,
  typeof window === "undefined" ? "/" : window.location.pathname,
);

/** True in a multi-workspace or hosted page outside any workspace (the
 * Workspaces list at /). */
export const atWorkspacesRoot: boolean =
  runtimeConfig.mode !== "local" && runtimeConfig.workspace === "";

/**
 * Inside one workspace of a multi-workspace or hosted server the
 * workspace home (the Workspace page, ADR-032 section 1, page 2) is "/"
 * and the Catalog moves to "/catalog". The local UI keeps "/" as the
 * Catalog (the README walkthrough asserts it) and has the Workspace page
 * at "/overview".
 */
export const homePath: string = runtimeConfig.mode === "local" ? "/overview" : "/";
export const catalogPath: string = runtimeConfig.mode === "local" ? "/" : "/catalog";
