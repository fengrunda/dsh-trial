#!/usr/bin/env bash
# Start isolated broker-dsh-trial (thin poller). Never touches broker-khub-prod.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
DSH_HOME="${DSH_HOME:-$HOME/.dsh}"
STATE_DIR="${TRIAL_BROKER_DIR:-$DSH_HOME/broker-dsh-trial}"
PIDFILE="$STATE_DIR/trial-broker.pid"
LOG="$STATE_DIR/trial-broker.log"
BROKER_PY="$ROOT/trial-broker.py"
POLL="${TRIAL_BROKER_POLL:-20}"
ARTIFACT_ROOT="${TRIAL_ARTIFACT_ROOT:-${TMPDIR:-/tmp}/trial-broker}"

mkdir -p "$STATE_DIR" \
  "$DSH_HOME/supervisor/trial/"{inbox,outbox,processing,failed} \
  "$ARTIFACT_ROOT"

# shellcheck disable=SC1091
if [[ -f "$DSH_HOME/load-env.sh" ]]; then
  source "$DSH_HOME/load-env.sh"
fi
export PATH="$DSH_HOME/bin:${HOME}/.local/bin:${PATH}"
export DSH_HOME
export TRIAL_BROKER_JOB_TIMEOUT="${TRIAL_BROKER_JOB_TIMEOUT:-2400}"
export DSH_ACP_PROMPT_TIMEOUT="${DSH_ACP_PROMPT_TIMEOUT:-1800}"
export DSH_PERMISSION_MODE="${DSH_PERMISSION_MODE:-danger-full-access}"
# Prefer official DeepSeek env from load-env; never echo secrets
unset NEW_API_KEY 2>/dev/null || true

if [[ -f "$PIDFILE" ]]; then
  old="$(cat "$PIDFILE" 2>/dev/null || true)"
  if [[ -n "${old}" ]] && kill -0 "$old" 2>/dev/null; then
    echo "already running pid=$old (pidfile=$PIDFILE)" >&2
    exit 1
  fi
  rm -f "$PIDFILE"
fi

# Coexistence: warn if prod socket exists, but do NOT stop/start prod
PROD_SOCK="$DSH_HOME/broker-khub-prod/broker.sock"
if [[ -S "$PROD_SOCK" ]] || [[ -e "$PROD_SOCK" ]]; then
  echo "note: prod broker sock present at $PROD_SOCK — coexistence OK; this start never calls khub-broker-restart.sh" >&2
fi

ONCE=0
ARGS=()
for a in "$@"; do
  case "$a" in
    --once) ONCE=1 ;;
    *) ARGS+=("$a") ;;
  esac
done

if [[ "$ONCE" -eq 1 ]]; then
  echo "trial-broker --once (foreground)"
  exec python3 "$BROKER_PY" --once "${ARGS[@]+"${ARGS[@]}"}"
fi

nohup python3 "$BROKER_PY" --poll "$POLL" "${ARGS[@]+"${ARGS[@]}"}" >>"$LOG" 2>&1 &
echo $! >"$PIDFILE"
sleep 0.3
if kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "started broker-dsh-trial pid=$(cat "$PIDFILE") poll=${POLL}s log=$LOG"
  echo "inbox=$DSH_HOME/supervisor/trial/inbox"
  python3 "$BROKER_PY" --status || true
  exit 0
fi
echo "failed to start; see $LOG" >&2
exit 1
