// render-index.mjs <in index.html> <out index.html> <region> <userPoolId>
//                  <clientId> <cognitoDomain> <googleEnabled> <timeZone>
//
// Writes the hosted index.html: the same script tag the Go server injects
// (internal/ui/spa.go InjectRuntimeConfig), plus the Cognito settings and
// a noindex meta. basename and apiBase are left out on purpose: one
// static index.html serves every path, and the SPA derives them from the
// path (ui/src/lib/runtimeConfig.ts).
//
// Then the tracker (ADR-032 section 4, "Tracking and errors"):
// /tracker-config.js (hosted/static: spa, sanitize) and /tracker.js (the
// canonical web-tracker/tracker.js). Plain blocking scripts after the
// config, so the tracker is up before the app bundle (a deferred module)
// runs and sees its errors too. Only this hosted index.html has them; the
// local `clavesa ui` never loads the tracker.
import { readFileSync, writeFileSync } from "node:fs";

const [input, output, region, userPoolId, clientId, domain, google, timeZone] =
  process.argv.slice(2);
if (!output || !userPoolId || !clientId || !timeZone) {
  console.error("usage: render-index.mjs <in> <out> <region> <poolId> <clientId> <domain> <googleEnabled> <timeZone>");
  process.exit(2);
}

const config = {
  mode: "hosted",
  readOnly: true,
  cognito: {
    region,
    userPoolId,
    clientId,
    domain,
    googleEnabled: google === "true",
  },
  // hosted/infra var.time_zone: the Lambda counts the summary days in it
  // too (CLAVESA_TIME_ZONE), so the cards and the CLI agree.
  timeZone,
};
// Escape < so a value can never close the script tag (json.Marshal does
// the same on the Go side).
const json = JSON.stringify(config).replace(/</g, "\\u003c");
const tags =
  `<meta name="robots" content="noindex, nofollow" />` +
  `<script>window.__CLAVESA__=${json};</script>` +
  `<script src="/tracker-config.js"></script>` +
  `<script src="/tracker.js"></script>`;

const html = readFileSync(input, "utf8");
if (!html.includes("</head>")) {
  console.error(`${input} has no </head>`);
  process.exit(1);
}
writeFileSync(output, html.replace("</head>", `${tags}</head>`));
