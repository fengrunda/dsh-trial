#!/usr/bin/env bash
# Start the standalone trial watchdog. Independent of trial-broker.py so it can
# still alert when the broker is dead or frozen. Never touches broker-khub-prod.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
DSH_HOME="${DSH_HOME:-$HOME/.dsh}"
STATE_DIR="${TRIAL_BROKER_DIR:-$DSH_HOME/broker-dsh-trial}"
PIDFILE="$STATE_DIR/trial-watchdog.pid"
LOG="$STATE_DIR/trial-watchdog.log"
WATCHDOG_PY="$ROOT/trial-watchdog.py"
INTERVAL="${TRIAL_WATCHDOG_INTERVAL:-60}"

mkdir -p "$STATE_DIR"

# shellcheck disable=SC1091
if [[ -f "$DSH_HOME/load-env.sh" ]]; then
  source "$DSH_HOME/load-env.sh"
fi
export DSH_HOME
unset NEW_API_KEY 2>/dev/null || true

if [[ ! -f "$WATCHDOG_PY" ]]; then
  echo "missing $WATCHDOG_PY" >&2
  exit 1
fi

ONCE=0
for a in "$@"; do
  case "$a" in
    --once) ONCE=1 ;;
  esac
done

if [[ "$ONCE" -eq 1 ]]; then
  echo "trial-watchdog --once (foreground)"
  exec python3 "$WATCHDOG_PY" --once
fi

if [[ -f "$PIDFILE" ]]; then
  old="$(cat "$PIDFILE" 2>/dev/null || true)"
  if [[ -n "${old}" ]] && kill -0 "$old" 2>/dev/null; then
    echo "watchdog already running pid=$old (pidfile=$PIDFILE)" >&2
    exit 1
  fi
  rm -f "$PIDFILE"
fi

nohup python3 "$WATCHDOG_PY" --loop --interval "$INTERVAL" >>"$LOG" 2>&1 &
echo $! >"$PIDFILE"
sleep 0.3
if kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "started trial-watchdog pid=$(cat "$PIDFILE") interval=${INTERVAL}s log=$LOG"
  exit 0
fi
echo "failed to start watchdog; see $LOG" >&2
exit 1
