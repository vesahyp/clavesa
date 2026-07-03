#!/usr/bin/env bash
set -euo pipefail

cleanup() {
    echo ""
    echo "Stopping..."
    kill $(jobs -p) 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup INT TERM EXIT

# Kill any existing processes on our ports
for port in 8080 5173 5174 5175; do
    lsof -ti tcp:$port | xargs kill -9 2>/dev/null || true
done

WORKSPACE="${WORKSPACE:-$(pwd)}"

echo "  Backend:  http://localhost:8080"
echo "  Frontend: http://localhost:5173"
echo "  Workspace: $WORKSPACE"
echo ""

go run ./cmd/clavesa/... ui --no-browser --workspace "$WORKSPACE" &
(cd ui && npm run dev) &

wait
