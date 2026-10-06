#!/usr/bin/env bash
# Stop only the standalone trial watchdog. Never touches broker-khub-prod.
set -euo pipefail

DSH_HOME="${DSH_HOME:-$HOME/.dsh}"
STATE_DIR="${TRIAL_BROKER_DIR:-$DSH_HOME/broker-dsh-trial}"
PIDFILE="$STATE_DIR/trial-watchdog.pid"

if [[ ! -f "$PIDFILE" ]]; then
  echo "no watchdog pidfile at $PIDFILE (already stopped?)"
  exit 0
fi
pid="$(cat "$PIDFILE" 2>/dev/null || true)"
if [[ -z "${pid}" ]]; then
  rm -f "$PIDFILE"
  echo "empty watchdog pidfile removed"
  exit 0
fi
if kill -0 "$pid" 2>/dev/null; then
  kill "$pid" 2>/dev/null || true
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "pid $pid still alive; sending TERM again" >&2
    kill -TERM "$pid" 2>/dev/null || true
    sleep 1
  fi
  if kill -0 "$pid" 2>/dev/null; then
    echo "refusing SIGKILL by default; pid=$pid still up" >&2
    exit 1
  fi
  echo "stopped trial-watchdog pid=$pid"
else
  echo "stale watchdog pidfile (pid $pid not running)"
fi
rm -f "$PIDFILE"
