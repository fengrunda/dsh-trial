#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GUARD="$ROOT/bin/dsh-trial-git-guard"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
git init -q -b main "$TMP/repo"
cd "$TMP/repo"
git config user.email t@example.com
git config user.name t
echo a > f && git add f && git commit -q -m init

# Without guard: push would need remote; commit on main works via real git through guard passthrough
DSH_TRIAL_GUARD=0 "$GUARD" rev-parse --abbrev-ref HEAD | grep -qx main

# With guard: commit on main refused
if DSH_TRIAL_GUARD=1 "$GUARD" commit --allow-empty -m x 2>"$TMP/err"; then
  echo "expected commit on main to fail" >&2
  exit 1
fi
grep -q "protected branch" "$TMP/err"

# Feature branch commit OK
DSH_TRIAL_GUARD=1 "$GUARD" checkout -q -b feat/x
DSH_TRIAL_GUARD=1 "$GUARD" commit --allow-empty -q -m ok

# Push main refused
DSH_TRIAL_GUARD=1 "$GUARD" checkout -q main
if DSH_TRIAL_GUARD=1 "$GUARD" push origin main 2>"$TMP/err2"; then
  echo "expected push main to fail" >&2
  exit 1
fi
grep -q "protected" "$TMP/err2"
echo "git-guard ok"
