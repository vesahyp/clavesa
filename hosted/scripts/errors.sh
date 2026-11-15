#!/usr/bin/env bash
# make errors: the hosted UI's client errors of the last N days (default 7),
# read from s3://<data bucket>/stats/ui.json, the per-day rollup that the
# site-traffic workspace's traffic pipeline writes (node ui_json). One line
# per msg + path + type, most frequent first:
#
#   <count>  <type>  <path>  <msg>   (last <day>)
#
# Usage: errors.sh <bucket> [days]. AWS profile: $AWS_PROFILE, default
# "personal". Days are Helsinki days, the pipeline's day boundary.
set -euo pipefail

BUCKET="${1:?usage: errors.sh <bucket> [days]}"
DAYS="${2:-7}"
export AWS_PROFILE="${AWS_PROFILE:-personal}"

JSON="$(aws s3 cp "s3://$BUCKET/stats/ui.json" - --region eu-north-1)"

node -e '
const rows = JSON.parse(process.argv[1]);
const days = Number(process.argv[2]);
const fmt = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Helsinki" });
const since = fmt.format(new Date(Date.now() - (days - 1) * 86400000));
const m = new Map();
let total = 0;
for (const r of rows) {
  if (r.date < since) continue;
  for (const e of r.errorList || []) {
    const k = [e.msg, e.path, e.type].join("\u0000");
    const cur = m.get(k) || { msg: e.msg, path: e.path, type: e.type, n: 0, last: r.date };
    cur.n += e.n;
    if (r.date > cur.last) cur.last = r.date;
    m.set(k, cur);
    total += e.n;
  }
}
const last = rows.length ? rows[rows.length - 1].date : "none";
console.log(`hosted UI errors since ${since} (${days} days; newest day in the rollup: ${last}): ${total}`);
for (const e of [...m.values()].sort((a, b) => b.n - a.n)) {
  console.log(`${String(e.n).padStart(5)}  ${(e.type || "-").padEnd(8)}  ${e.path || "-"}  ${e.msg || "(no message)"}   (last ${e.last})`);
}
' "$JSON" "$DAYS"
