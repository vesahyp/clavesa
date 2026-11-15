/**
 * HostedGate — the hosted UI's door (ADR-032 section 4). Lazy-loaded from
 * main.tsx in hosted mode only. Renders the app when there is a session
 * (refreshed silently), the sign-in screen when there is none.
 *
 * Sign-in: email, then the code Cognito mails (EMAIL_OTP). "Continue with
 * Google" only when the config enables it. There is no password field.
 */

import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import { Loader2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { runtimeConfig } from "@/lib/runtimeConfig";
import {
  AuthError,
  answerEmailCode,
  finishRedirect,
  getIdToken,
  onSignedOut,
  startEmailSignIn,
  startGoogleSignIn,
} from "./auth";

type State = "checking" | "signedOut" | "signedIn";

export default function HostedGate({ children }: { children: ReactNode }) {
  const [state, setState] = useState<State>("checking");
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    (async () => {
      const err = await finishRedirect().catch(() => "Sign-in failed. Try again.");
      const token = await getIdToken();
      if (!live) return;
      setNotice(err);
      setState(token ? "signedIn" : "signedOut");
    })();
    const off = onSignedOut(() => setState("signedOut"));
    return () => {
      live = false;
      off();
    };
  }, []);

  if (state === "checking") {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
      </div>
    );
  }
  if (state === "signedOut") {
    return <SignIn notice={notice} onSignedIn={() => setState("signedIn")} />;
  }
  return <>{children}</>;
}

function message(e: unknown): string {
  if (e instanceof AuthError) {
    switch (e.code) {
      case "CodeMismatchException":
        return "That code is not right. Check the mail and try again.";
      case "ExpiredCodeException":
      case "NotAuthorizedException":
        return "The code expired or was used up. Ask for a new one.";
      case "LimitExceededException":
      case "TooManyRequestsException":
        return "Too many tries. Wait a few minutes and try again.";
      case "UserNotFoundException":
      case "UserNotConfirmedException":
        return "This address cannot sign in here.";
    }
    return e.message;
  }
  return e instanceof Error ? e.message : String(e);
}

function SignIn({
  notice,
  onSignedIn,
}: {
  notice: string | null;
  onSignedIn: () => void;
}) {
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [session, setSession] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(notice);
  const google = runtimeConfig.cognito?.googleEnabled === true;

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      if (session === null) {
        setSession(await startEmailSignIn(email));
      } else {
        await answerEmailCode(email, code, session);
        onSignedIn();
        return;
      }
    } catch (err) {
      setError(message(err));
      if (err instanceof AuthError && err.code !== "CodeMismatchException") {
        setSession(null);
        setCode("");
      }
    }
    setBusy(false);
  }

  return (
    <div className="flex min-h-screen items-start justify-center bg-background px-4 pt-[max(4rem,env(safe-area-inset-top))] text-foreground">
      <form
        onSubmit={submit}
        data-testid="signin-form"
        className="flex w-full max-w-sm flex-col gap-4 rounded-lg border border-border bg-card p-6"
      >
        <div>
          <div className="text-lg font-semibold tracking-tight">Clavesλ</div>
          <div className="text-sm text-muted-foreground">
            {session === null
              ? "Sign in with a code sent to your email."
              : `We sent a code to ${email}.`}
          </div>
        </div>

        {session === null ? (
          <div className="flex flex-col gap-2">
            <Label htmlFor="signin-email">Email</Label>
            <Input
              id="signin-email"
              data-testid="signin-email"
              type="email"
              name="email"
              autoComplete="email"
              inputMode="email"
              required
              autoFocus
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="h-11 text-base"
            />
          </div>
        ) : (
          <div className="flex flex-col gap-2">
            <Label htmlFor="signin-code">Code</Label>
            <Input
              id="signin-code"
              data-testid="signin-code"
              type="text"
              name="code"
              autoComplete="one-time-code"
              inputMode="numeric"
              pattern="[0-9]*"
              maxLength={8}
              required
              autoFocus
              value={code}
              onChange={(e) => setCode(e.target.value.replace(/\D/g, ""))}
              className="h-11 text-base tracking-widest"
            />
          </div>
        )}

        {error && (
          <div data-testid="signin-error" role="alert" className="text-sm text-status-failed">
            {error}
          </div>
        )}

        <Button type="submit" data-testid="signin-submit" disabled={busy} className="h-11">
          {busy && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          {session === null ? "Send code" : "Sign in"}
        </Button>

        {session !== null && (
          <button
            type="button"
            className="text-sm text-muted-foreground underline-offset-4 hover:underline"
            onClick={() => {
              setSession(null);
              setCode("");
              setError(null);
            }}
          >
            Use another address
          </button>
        )}

        {google && session === null && (
          <Button
            type="button"
            variant="outline"
            data-testid="signin-google"
            className="h-11"
            onClick={() => void startGoogleSignIn()}
          >
            Continue with Google
          </Button>
        )}
      </form>
    </div>
  );
}
