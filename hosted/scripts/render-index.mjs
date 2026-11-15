// render-index.mjs <in index.html> <out index.html> <region> <userPoolId>
//                  <clientId> <cognitoDomain> <googleEnabled>
//
// Writes the hosted index.html: the same script tag the Go server injects
// (internal/ui/spa.go InjectRuntimeConfig), plus the Cognito settings and
// a noindex meta. basename and apiBase are left out on purpose: one
// static index.html serves every path, and the SPA derives them from the
// path (ui/src/lib/runtimeConfig.ts).
import { readFileSync, writeFileSync } from "node:fs";

const [input, output, region, userPoolId, clientId, domain, google] =
  process.argv.slice(2);
if (!output || !userPoolId || !clientId) {
  console.error("usage: render-index.mjs <in> <out> <region> <poolId> <clientId> <domain> <googleEnabled>");
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
};
// Escape < so a value can never close the script tag (json.Marshal does
// the same on the Go side).
const json = JSON.stringify(config).replace(/</g, "\\u003c");
const tags =
  `<meta name="robots" content="noindex, nofollow" />` +
  `<script>window.__CLAVESA__=${json};</script>`;

const html = readFileSync(input, "utf8");
if (!html.includes("</head>")) {
  console.error(`${input} has no </head>`);
  process.exit(1);
}
writeFileSync(output, html.replace("</head>", `${tags}</head>`));
