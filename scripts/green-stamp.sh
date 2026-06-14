#!/usr/bin/env bash
# green-stamp.sh — tree-exact green stamps for the local release gates.
#
#   write <name>   write .<name>-green.json recording the working-tree hash,
#                  HEAD commit, ModuleVersion, and a UTC timestamp.
#   check <name>   recompute the working-tree hash and compare it to the
#                  stamp; exit 0 (✓) on match, 1 (✗) on mismatch or missing.
#
# The tree hash is computed with `git stash create` — it hashes the *current
# content* of all tracked files (staged or not) into a throwaway commit
# without touching the working tree, and falls back to HEAD's tree when the
# tree is clean. CAVEAT: `git stash create` ignores untracked files, so a
# brand-new file that was never `git add`ed does not change the hash. That is
# acceptable for the release flow (everything ships staged in the release
# commit), but it means "check" cannot detect an untracked-only change.
#
# Used by the Makefile: `make test` and `make verify-readme` write their
# stamps on green; `make release-check` checks them so a release can prove
# the local gates ran on the exact bytes being released (the cloud smoke
# stamp, keyed by version, already covers the cloud gate).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

usage() {
  echo "usage: $0 {write|check} <name>" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
cmd="$1"
name="$2"
stamp=".$name-green.json"

tree_hash() {
  local s
  s="$(git stash create)"
  git rev-parse "${s:-HEAD}^{tree}"
}

case "$cmd" in
  write)
    tree="$(tree_hash)"
    commit="$(git rev-parse HEAD)"
    version="$(grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' internal/version/version.go | head -1)"
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '{\n  "gate": "%s",\n  "tree": "%s",\n  "commit": "%s",\n  "version": "%s",\n  "timestamp": "%s"\n}\n' \
      "$name" "$tree" "$commit" "$version" "$timestamp" >"$stamp"
    echo "✓ wrote $stamp (tree $tree)"
    ;;
  check)
    if [[ ! -f "$stamp" ]]; then
      echo "✗ $stamp is missing — the '$name' gate has not passed on this tree." >&2
      exit 1
    fi
    want="$(tree_hash)"
    have="$(sed -n 's/.*"tree"[[:space:]]*:[[:space:]]*"\([0-9a-f]*\)".*/\1/p' "$stamp" | head -1)"
    ts="$(sed -n 's/.*"timestamp"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$stamp" | head -1)"
    if [[ "$have" == "$want" ]]; then
      echo "✓ $name stamp matches the working tree (stamped $ts, tree $want)"
    else
      echo "✗ $name stamp is for tree ${have:-<unparsable>}, but the working tree is $want — the tree changed after the gate ran." >&2
      exit 1
    fi
    ;;
  *)
    usage
    ;;
esac
