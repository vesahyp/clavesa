// A signed-in session on the deployed hosted UI for a Playwright script, as
// the scripted check user (/clavesa-ui/check-user in SSM). The same steps as
// live-phone-check.mjs: the password goes through a 0600 file and is never
// printed; the tokens are planted the way ui/src/hosted/auth.ts keeps them.
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const REGION = "eu-north-1";

export function hostedSignIn({ repo, profile = "personal", parameter = "/clavesa-ui/check-user" }) {
  const env = { ...process.env, AWS_PROFILE: profile, AWS_REGION: REGION, AWS_DEFAULT_REGION: REGION };
  const run = (cmd, args) => execFileSync(cmd, args, { encoding: "utf8", env, stdio: ["ignore", "pipe", "pipe"] });
  const first = (e) => String(e.stderr ?? e.message).split("\n")[0];

  let user;
  try {
    user = JSON.parse(run("aws", ["ssm", "get-parameter", "--name", parameter, "--with-decryption", "--query", "Parameter.Value", "--output", "text"]));
  } catch (e) {
    throw new Error(`could not read ${parameter} (profile ${profile}): ${first(e)}`);
  }
  let tf;
  try {
    tf = JSON.parse(run("terraform", [`-chdir=${repo}hosted/infra`, "output", "-json"]));
  } catch (e) {
    throw new Error(`terraform output of hosted/infra failed (make hosted-init first?): ${first(e)}`);
  }
  const dir = mkdtempSync(join(tmpdir(), "clavesa-nav-"));
  const file = join(dir, "auth.json");
  try {
    writeFileSync(file, JSON.stringify({
      UserPoolId: tf.user_pool_id.value,
      ClientId: tf.check_client_id.value,
      AuthFlow: "ADMIN_USER_PASSWORD_AUTH",
      AuthParameters: { USERNAME: user.username, PASSWORD: user.password },
    }), { mode: 0o600 });
    const a = JSON.parse(run("aws", ["cognito-idp", "admin-initiate-auth", "--cli-input-json", `file://${file}`])).AuthenticationResult;
    if (!a?.IdToken) throw new Error("Cognito returned no tokens");
    return { idToken: a.IdToken, accessToken: a.AccessToken, refreshToken: a.RefreshToken, expiresAt: Date.now() + a.ExpiresIn * 1000 };
  } catch (e) {
    throw new Error(`admin-initiate-auth failed: ${first(e)}`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

/** Plant the session in a browser context before the app loads. */
export async function plantSession(ctx, tok) {
  await ctx.addInitScript((t) => {
    try {
      sessionStorage.setItem("clavesa.hosted.session", JSON.stringify({ idToken: t.idToken, accessToken: t.accessToken, expiresAt: t.expiresAt }));
      if (t.refreshToken) localStorage.setItem("clavesa.hosted.refresh", t.refreshToken);
    } catch {}
  }, tok);
}
