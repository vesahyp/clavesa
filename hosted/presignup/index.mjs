/**
 * Cognito Pre-SignUp and Pre-Authentication trigger of the hosted clavesa
 * UI (ADR-032 section 4). Copied from keitos/src/presignup; three jobs:
 *
 * 1. Allowlist. Unless REGISTRATION_OPEN is "1", a self-service sign-up
 *    (native or Google) is refused unless its email is in ALLOWED_EMAILS.
 *    Fails shut: an empty or missing list admits nobody. Accounts an
 *    operator creates (`make hosted-add-user`, the Terraform check user)
 *    are admitted by definition. The list is never logged.
 *
 * 2. Linking. A Google sign-in whose email matches an existing native
 *    user is linked to it (AdminLinkProviderForUser), so both doors open
 *    the same account. Cognito needs the federated sign-up aborted after
 *    the link, so the very first Google attempt shows "sign in again" and
 *    the second walks in.
 */

import {
  AdminLinkProviderForUserCommand,
  CognitoIdentityProviderClient,
  DescribeUserPoolClientCommand,
  ListUsersCommand,
} from "@aws-sdk/client-cognito-identity-provider";

const idp = new CognitoIdentityProviderClient({});

export async function handler(event) {
  if (event.triggerSource === "PreAuthentication_Authentication") {
    return checkUserOnlyOnCheckClient(event);
  }
  if (event.triggerSource === "PreSignUp_AdminCreateUser") return event;

  const allowed = new Set(
    (process.env.ALLOWED_EMAILS ?? "")
      .toLowerCase()
      .split(",")
      .map((e) => e.trim())
      .filter(Boolean),
  );
  const email = (event.request?.userAttributes?.email ?? "")
    .trim()
    .toLowerCase();

  if (process.env.REGISTRATION_OPEN !== "1" && (!email || !allowed.has(email))) {
    throw new Error("This clavesa UI is not open to this address.");
  }

  if (event.triggerSource === "PreSignUp_ExternalProvider") {
    // userName arrives as "google_<providerUserId>".
    const [providerRaw, ...idParts] = String(event.userName).split("_");
    const providerUserId = idParts.join("_");
    const providerName =
      providerRaw.charAt(0).toUpperCase() + providerRaw.slice(1);

    const existing = await idp.send(
      new ListUsersCommand({
        UserPoolId: event.userPoolId,
        Filter: `email = "${email.replace(/"/g, "")}"`,
      }),
    );
    const native = (existing.Users ?? []).find(
      (u) => !String(u.Username).includes("_"),
    );
    if (native) {
      await idp.send(
        new AdminLinkProviderForUserCommand({
          UserPoolId: event.userPoolId,
          DestinationUser: {
            ProviderName: "Cognito",
            ProviderAttributeValue: native.Username,
          },
          SourceUser: {
            ProviderName: providerName,
            ProviderAttributeName: "Cognito_Subject",
            ProviderAttributeValue: providerUserId,
          },
        }),
      );
      throw new Error("LINKED_RETRY: linked to your account, sign in again.");
    }
  }

  return event;
}

/**
 * 3. (Pre-authentication.) The check user has a password, kept in SSM for
 *    the scripted phone check. Cognito's USER_AUTH flow on the SPA client
 *    would also accept a password (PASSWORD must stay a pool factor), so
 *    the check user may sign in only through the check client, whose one
 *    password flow is the admin flow that needs IAM credentials. Other
 *    users' passwords are random and kept nowhere.
 */
async function checkUserOnlyOnCheckClient(event) {
  const checkUser = (process.env.CHECK_USER ?? "").toLowerCase();
  const email = (event.request?.userAttributes?.email ?? "").toLowerCase();
  if (!checkUser || email !== checkUser) return event;
  const out = await idp.send(
    new DescribeUserPoolClientCommand({
      UserPoolId: event.userPoolId,
      ClientId: event.callerContext?.clientId,
    }),
  );
  if (out.UserPoolClient?.ClientName !== process.env.CHECK_CLIENT_NAME) {
    throw new Error("This account signs in only through the check client.");
  }
  return event;
}
