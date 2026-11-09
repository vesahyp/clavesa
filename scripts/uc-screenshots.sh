#!/usr/bin/env bash
# Screenshots of the Unity Catalog surfaces (ADR-027, slice 7), taken with
# playwright-cli against a running `clavesa ui` on a connected workspace.
#
#   CLAVESA_URL   the UI (default http://localhost:8080)
#   UC_PIPELINE   a pipeline that published (default uc_orders)
#   UC_RUN        one of its run IDs that started a Databricks job
#   OUT           where the PNGs go (default ./screenshots/unity-catalog)
#   PLAYWRIGHT_CLI_CONFIG  optional playwright-cli --config file (e.g. to
#                 point at a cached Chromium where Chrome will not start)
#
# Fails when the page lacks what slice 7 promises: the "Published to Unity
# Catalog" badge with a Catalog Explorer link, and the run page's
# Databricks card. Run via `make screenshots-unity-catalog`.
set -euo pipefail

url=${CLAVESA_URL:-http://localhost:8080}
pipeline=${UC_PIPELINE:-uc_orders}
run=${UC_RUN:?set UC_RUN to a run ID of $pipeline that started a Databricks job}
out=${OUT:-screenshots/unity-catalog}
mkdir -p "$out"
out=$(cd "$out" && pwd)

cfg=()
if [[ -n "${PLAYWRIGHT_CLI_CONFIG:-}" ]]; then
  cfg=(--config "$PLAYWRIGHT_CLI_CONFIG")
fi

count() {
  # Number of elements matching a data-testid, polled for up to 30 s
  # because the tables come from Databricks, not from local state.
  local testid=$1 n=0
  for _ in $(seq 1 30); do
    n=$(playwright-cli eval "document.querySelectorAll('[data-testid=\"$testid\"]').length" | grep -Eo '^[0-9]+' | tail -1 || true)
    [[ "${n:-0}" -gt 0 ]] && break
    sleep 1
  done
  echo "${n:-0}"
}

playwright-cli open "${cfg[@]}" "$url/unity-catalog" >/dev/null
trap 'playwright-cli close-all >/dev/null 2>&1 || true' EXIT
playwright-cli resize 1440 1000 >/dev/null

n=$(count databricks-published-badge)
[[ "$n" -gt 0 ]] || { echo "✗ /unity-catalog: no 'Published to Unity Catalog' badge"; exit 1; }
echo "✓ /unity-catalog: $n published tables"
playwright-cli screenshot --full-page --filename "$out/unity-catalog.png" >/dev/null

playwright-cli goto "$url/pipelines/dashboard?dir=$pipeline&run=$run" >/dev/null
n=$(count run-databricks-card)
[[ "$n" -gt 0 ]] || { echo "✗ run page: no Databricks card for run $run"; exit 1; }
jobs=$(count run-databricks-job)
echo "✓ run page: Databricks card, $jobs job link(s)"
playwright-cli screenshot --filename "$out/run-databricks.png" >/dev/null

errors=$(playwright-cli console error | grep -Eo 'Errors: [0-9]+' | grep -Eo '[0-9]+' || echo 0)
[[ "$errors" -eq 0 ]] || { echo "✗ $errors console errors"; playwright-cli console error; exit 1; }
echo "✓ 0 console errors"
echo "Screenshots: $out/unity-catalog.png $out/run-databricks.png"
