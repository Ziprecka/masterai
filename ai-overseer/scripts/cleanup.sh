#!/usr/bin/env bash
# Remove all worktrees under .worktrees/ and prune. Use after a crash.
set -euo pipefail

REPO="${TARGET_REPO:?TARGET_REPO must be set}"
ROOT="${WORKTREE_ROOT:-$(pwd)/.worktrees}"

cd "$REPO"
for wt in "$ROOT"/*; do
  [ -d "$wt" ] || continue
  echo "Removing worktree: $wt"
  git worktree remove --force "$wt" 2>/dev/null || rm -rf "$wt"
done

git worktree prune
echo "Done."
