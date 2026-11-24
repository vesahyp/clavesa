---
adr: 35
title: A hosted UI user sees only the workspaces they are a viewer of, by Cognito group; a workspace they may not see does not exist for them
date: 2026-11-17
status: Accepted
deciders: Vesa
---

# ADR 035: Per-workspace viewers for the hosted UI

**Status**: Accepted (2026-11-17, Vesa: go on with the parity plan;
parity slice 6). Replaces ADR-032 section 2's "every signed-in user sees
every workspace".

## Context

ADR-032 let every signed-in user see every registered workspace. The
sign-in allowlist (SSM `/clavesa-ui/config`) admits Vesa and the check
user only, so that was the same as "Vesa sees everything". The parity
plan opens more of each workspace (dashboards, notebooks, later ad-hoc
query), and webbaa's web-traffic is shared work data. Before anyone else
signs in, access must be per workspace.

Vesa, 2026-11-17: build the mechanism, add nobody; webbaa collaborators
get no access until he says so.

## Decision

1. **Viewers are Cognito groups.** The hosted stack creates one group per
   registered workspace, `ws-<name>`, and one group `all`. A user sees a
   workspace when they are in `all` or in its group. No address is in the
   repository: membership is set with `make hosted-viewer EMAIL=...
   WS=<name>|all` (an admin call, as `hosted-add-user` is).
2. **The Lambda enforces it** from the id token's `cognito:groups`, which
   the API's JWT authorizer has already checked:
   - `GET /api/workspaces` lists only the workspaces the user may see;
   - `/w/<name>/...` of a workspace the user may not see answers 404,
     the same answer as a workspace that does not exist;
   - a request with no groups sees nothing (fail closed).
   `/api/stats` (the hosted UI's own traffic) stays for `all` only.
3. **Sign-in is unchanged.** The SSM allowlist still decides who may sign
   in at all; a group decides what they see.
4. **Migration.** The deploy adds the users who can sign in today (Vesa)
   to `all`, so nothing changes for him. The check user gets the groups
   of the workspaces the live check walks (smoke2, site-traffic,
   web-traffic) and nothing else, so the live check proves the 404 on
   another workspace.

## Consequences

- A second viewer gets exactly the workspaces they are added to; adding
  someone is one command and leaves no trace in the repository.
- Group membership is in the Cognito id token, so a change applies at the
  user's next sign-in or token refresh (up to an hour).
- The Lambda trusts the claims because the API Gateway JWT authorizer
  verified the token; the execute-api endpoint has the same authorizer.
- The registry grows a group per workspace (free in Cognito).

## Update 2026-11-17 (slice 6 review)

- The check user is a viewer of smoke2 and site-traffic only, the
  workspaces the live check walks; it is not a viewer of web-traffic
  (webbaa work data, personal_data). web-traffic is covered by Vesa's own
  use, not by the scripted check.
- Memberships are hand steps (`make hosted-viewer`), not done by the
  deploy: Terraform creates the groups only. A check user made again by
  Terraform has no groups and fails closed.
- No user can change their own attributes: both app clients may write
  only `locale` (an empty list would mean every attribute to Cognito),
  and the pool requires a verified email before an email change applies.
  `make hosted-viewer` grants by user id, for exactly one user with that
  verified email, and a removal also signs the user out everywhere.
- A workspace may not be named `all`.

## Update 2026-11-22 (column tags)

The `personal_data` parts of this ADR are superseded by
`docs/ideas/column-tags.md`, slice 1b: the flag is removed from
`clavesa.json` and from the hosted registry. Lake Formation column tags
decide what a viewer reads of a cloud workspace.
