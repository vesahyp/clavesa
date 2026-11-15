/**
 * auth.ts — the hosted UI's session (ADR-032 section 4). Loaded only in
 * hosted mode (dynamic import from main.tsx and api/client.ts), so the
 * local UI never downloads it.
 *
 * Sign-in is Cognito's USER_AUTH flow with the EMAIL_OTP challenge, plain
 * fetch to the Cognito JSON API (no AWS SDK: two calls do not justify
 * ~60 KB). Google, when the config enables it, is the Cognito prefix
 * domain's code flow with PKCE.
 *
 * Where tokens live:
 *   - id and access token: in memory, mirrored in sessionStorage
 *     (SESSION_KEY) so a reload or a full page load into another
 *     workspace (/w/<name>/…) does not refresh. They live 60 minutes.
 *   - refresh token: localStorage (REFRESH_KEY), so the phone stays signed
 *     in across app restarts for its 30 days. Sign-out revokes it.
 * Every API call sends `Authorization: Bearer <id token>`: the id token
 * carries the email, which the per-workspace viewers list keys on.
 */

import { runtimeConfig, type HostedCognitoConfig } from "@/lib/runtimeConfig";

export const SESSION_KEY = "clavesa.hosted.session";
export const REFRESH_KEY = "clavesa.hosted.refresh";
const PKCE_KEY = "clavesa.hosted.pkce";

export interface Tokens {
  idToken: string;
  accessToken: string;
  /** Epoch ms. */
  expiresAt: number;
}

function cfg(): HostedCognitoConfig {
  const c = runtimeConfig.cognito;
  if (!c) throw new Error("hosted mode without cognito settings in window.__CLAVESA__");
  return c;
}

// ---- storage (every access guarded: private mode can throw) ----

function read(store: Storage | undefined, key: string): string | null {
  try {
    return store?.getItem(key) ?? null;
  } catch {
    return null;
  }
}
function write(store: Storage | undefined, key: string, value: string | null) {
  try {
    if (value === null) store?.removeItem(key);
    else store?.setItem(key, value);
  } catch {
    // storage unavailable: the session lasts as long as the page
  }
}
const local = typeof window === "undefined" ? undefined : window.localStorage;
const session = typeof window === "undefined" ? undefined : window.sessionStorage;

let tokens: Tokens | null = (() => {
  const raw = read(session, SESSION_KEY);
  if (!raw) return null;
  try {
    const t = JSON.parse(raw) as Tokens;
    return t.idToken && t.expiresAt ? t : null;
  } catch {
    return null;
  }
})();

function store(t: Tokens, refreshToken?: string) {
  tokens = t;
  write(session, SESSION_KEY, JSON.stringify(t));
  if (refreshToken) write(local, REFRESH_KEY, refreshToken);
}

function clear() {
  tokens = null;
  write(session, SESSION_KEY, null);
  write(local, REFRESH_KEY, null);
}

// ---- signed-out notification (a 401 from the API, a failed refresh) ----

const listeners = new Set<() => void>();
export function onSignedOut(fn: () => void): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}
export function sessionLost() {
  clear();
  listeners.forEach((fn) => fn());
}

// ---- Cognito JSON API ----

export class AuthError extends Error {
  constructor(
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

async function cognito<T>(action: string, body: object): Promise<T> {
  const res = await fetch(`https://cognito-idp.${cfg().region}.amazonaws.com/`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-amz-json-1.1",
      "X-Amz-Target": `AWSCognitoIdentityProviderService.${action}`,
    },
    body: JSON.stringify(body),
  });
  const json = (await res.json().catch(() => ({}))) as Record<string, unknown>;
  if (!res.ok) {
    const type = String(json.__type ?? "Error").split("#").pop() ?? "Error";
    throw new AuthError(type, String(json.message ?? json.Message ?? res.statusText));
  }
  return json as T;
}

interface AuthResult {
  AuthenticationResult?: {
    IdToken: string;
    AccessToken: string;
    RefreshToken?: string;
    ExpiresIn: number;
  };
  ChallengeName?: string;
  Session?: string;
}

function fromResult(r: AuthResult): Tokens {
  const a = r.AuthenticationResult;
  if (!a) throw new AuthError("NoTokens", "Cognito returned no tokens");
  const t = {
    idToken: a.IdToken,
    accessToken: a.AccessToken,
    expiresAt: Date.now() + a.ExpiresIn * 1000,
  };
  store(t, a.RefreshToken);
  return t;
}

/** Step 1: ask Cognito to mail a code. Returns the challenge session. */
export async function startEmailSignIn(email: string): Promise<string> {
  const username = email.trim().toLowerCase();
  let r = await cognito<AuthResult>("InitiateAuth", {
    AuthFlow: "USER_AUTH",
    ClientId: cfg().clientId,
    AuthParameters: { USERNAME: username, PREFERRED_CHALLENGE: "EMAIL_OTP" },
  });
  if (r.ChallengeName === "SELECT_CHALLENGE") {
    r = await cognito<AuthResult>("RespondToAuthChallenge", {
      ChallengeName: "SELECT_CHALLENGE",
      ClientId: cfg().clientId,
      Session: r.Session,
      ChallengeResponses: { USERNAME: username, ANSWER: "EMAIL_OTP" },
    });
  }
  if (r.ChallengeName !== "EMAIL_OTP" || !r.Session) {
    throw new AuthError("UnexpectedChallenge", `Unexpected sign-in step ${r.ChallengeName ?? "(none)"}`);
  }
  return r.Session;
}

/** Step 2: answer with the mailed code. Throws AuthError
 * (CodeMismatchException for a wrong code). */
export async function answerEmailCode(
  email: string,
  code: string,
  sessionToken: string,
): Promise<void> {
  const r = await cognito<AuthResult>("RespondToAuthChallenge", {
    ChallengeName: "EMAIL_OTP",
    ClientId: cfg().clientId,
    Session: sessionToken,
    ChallengeResponses: {
      USERNAME: email.trim().toLowerCase(),
      EMAIL_OTP_CODE: code.trim(),
    },
  });
  fromResult(r);
}

let refreshing: Promise<Tokens | null> | null = null;

async function refresh(): Promise<Tokens | null> {
  const rt = read(local, REFRESH_KEY);
  if (!rt) return null;
  try {
    const r = await cognito<AuthResult>("InitiateAuth", {
      AuthFlow: "REFRESH_TOKEN_AUTH",
      ClientId: cfg().clientId,
      AuthParameters: { REFRESH_TOKEN: rt },
    });
    return fromResult(r);
  } catch {
    clear();
    return null;
  }
}

/** The id token, refreshed silently when it has under a minute left.
 * null when there is no session. */
export async function getIdToken(): Promise<string | null> {
  if (tokens && tokens.expiresAt - 60_000 > Date.now()) return tokens.idToken;
  refreshing ??= refresh().finally(() => {
    refreshing = null;
  });
  const t = await refreshing;
  return t?.idToken ?? null;
}

export async function authHeaders(): Promise<Record<string, string>> {
  const t = await getIdToken();
  return t ? { Authorization: `Bearer ${t}` } : {};
}

export async function signOut(): Promise<void> {
  const rt = read(local, REFRESH_KEY);
  clear();
  if (rt) {
    await cognito("RevokeToken", { Token: rt, ClientId: cfg().clientId }).catch(() => {});
  }
  window.location.assign("/");
}

// ---- Google: Cognito prefix domain, code flow with PKCE ----

function b64url(bytes: Uint8Array): string {
  let s = "";
  bytes.forEach((b) => (s += String.fromCharCode(b)));
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

const redirectUri = () => `${window.location.origin}/`;

export async function startGoogleSignIn(): Promise<void> {
  const verifier = b64url(crypto.getRandomValues(new Uint8Array(32)));
  const state = b64url(crypto.getRandomValues(new Uint8Array(16)));
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
  write(session, PKCE_KEY, JSON.stringify({ verifier, state }));
  const q = new URLSearchParams({
    response_type: "code",
    client_id: cfg().clientId,
    redirect_uri: redirectUri(),
    identity_provider: "Google",
    scope: "openid email profile",
    state,
    code_challenge: b64url(new Uint8Array(digest)),
    code_challenge_method: "S256",
  });
  window.location.assign(`https://${cfg().domain}/oauth2/authorize?${q}`);
}

/** On load: finish a Google redirect (?code=&state=). Returns an error
 * message to show, or null. Clears the query either way. */
export async function finishRedirect(): Promise<string | null> {
  const q = new URLSearchParams(window.location.search);
  const code = q.get("code");
  const err = q.get("error_description") ?? q.get("error");
  if (!code && !err) return null;
  window.history.replaceState(null, "", window.location.pathname);
  if (err) return err;
  const saved = JSON.parse(read(session, PKCE_KEY) ?? "{}") as { verifier?: string; state?: string };
  write(session, PKCE_KEY, null);
  if (!saved.verifier || saved.state !== q.get("state")) return "Sign-in expired. Try again.";
  const res = await fetch(`https://${cfg().domain}/oauth2/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "authorization_code",
      client_id: cfg().clientId,
      code: code!,
      redirect_uri: redirectUri(),
      code_verifier: saved.verifier,
    }),
  });
  if (!res.ok) return "Google sign-in failed. Try again.";
  const j = (await res.json()) as {
    id_token: string;
    access_token: string;
    refresh_token?: string;
    expires_in: number;
  };
  store(
    { idToken: j.id_token, accessToken: j.access_token, expiresAt: Date.now() + j.expires_in * 1000 },
    j.refresh_token,
  );
  return null;
}
