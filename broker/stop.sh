#!/usr/bin/env bash
# Stop only broker-dsh-trial. Never touches broker-khub-prod / product tickets.
#
# T2 drain: the first SIGTERM puts the broker into drain — it stops dispatching
# new jobs and spawning new open-slices, lets the in-flight tickets reach their
# boundary, then exits 0. That is why this script never sends a second TERM
# (a second TERM/SIGINT means "exit now" to the broker) and never SIGKILLs.
#
#   stop.sh          TERM, wait up to TRIAL_BROKER_STOP_WAIT_SEC (default 15s);
#                    if it is still draining, say so and exit 0 (pidfile kept).
#   stop.sh --wait   TERM, then block until the broker really exits.
set -euo pipefail

WAIT=0
for arg in "$@"; do
  case "$arg" in
    --wait) WAIT=1 ;;
    -h|--help)
      echo "usage: stop.sh [--wait]"
      exit 0
      ;;
    *)
      echo "unknown option: $arg" >&2
      exit 2
      ;;
  esac
done

DSH_HOME="${DSH_HOME:-$HOME/.dsh}"
STATE_DIR="${TRIAL_BROKER_DIR:-$DSH_HOME/broker-dsh-trial}"
PIDFILE="$STATE_DIR/trial-broker.pid"
WAIT_SEC="${TRIAL_BROKER_STOP_WAIT_SEC:-15}"

_stop_watchdog() {
  # Stop the standalone watchdog too (its own pidfile; never touches prod).
  local _root
  _root="$(cd "$(dirname "$0")" && pwd)"
  if [[ -x "$_root/watchdog-stop.sh" ]]; then
    "$_root/watchdog-stop.sh" || echo "warning: watchdog stop failed" >&2
  fi
}

if [[ ! -f "$PIDFILE" ]]; then
  echo "no pidfile at $PIDFILE (already stopped?)"
  _stop_watchdog
  exit 0
fi
pid="$(cat "$PIDFILE" 2>/dev/null || true)"
if [[ -z "${pid}" ]]; then
  rm -f "$PIDFILE"
  echo "empty pidfile removed"
  _stop_watchdog
  exit 0
fi
if ! kill -0 "$pid" 2>/dev/null; then
  echo "stale pidfile (pid $pid not running)"
  rm -f "$PIDFILE"
  _stop_watchdog
  exit 0
fi

kill -TERM "$pid" 2>/dev/null || true

deadline=$((SECONDS + WAIT_SEC))
while kill -0 "$pid" 2>/dev/null; do
  if (( SECONDS >= deadline )); then
    break
  fi
  sleep 0.5
done

if kill -0 "$pid" 2>/dev/null; then
  if (( WAIT )); then
    echo "waiting for pid=$pid to finish in-flight tickets (drain)..."
    while kill -0 "$pid" 2>/dev/null; do
      sleep 1
    done
  else
    # Still draining: keep the pidfile so a rerun (or --wait) can find it.
    echo "draining pid=$pid (in-flight tickets will finish; rerun stop.sh --wait to block)"
    exit 0
  fi
fi

echo "stopped broker-dsh-trial pid=$pid"
rm -f "$PIDFILE"
_stop_watchdog
