/**
 * Shared thin-state mailbox helpers for ask_supervisor / submit_for_review.
 */
import fsp from 'node:fs/promises'
import path from 'node:path'
import os from 'node:os'
import { execFile } from 'node:child_process'

// Fallback gate budget (seconds) when the broker's interim ack omits
// gate_timeout_sec: 600 (ask_supervisor) + 3600 (prompt hard cap) + 120.
export const DEFAULT_GATE_BUDGET_SEC = 4320
// Extra grace after the gate budget before the client gives up (final answer may
// land just after the gate exits). Overridable for tests via ackGraceMs.
export const DEFAULT_GATE_ACK_GRACE_MS = 30000

export function isInterimAnswer(body) {
  if (!body || typeof body !== 'object') return false
  if (body.status === 'gate_running') return true
  // A progress marker without a verdict is still in-flight; old clients that
  // only looked for ok:true would otherwise treat it as the final answer.
  return body.progress === true && body.verdict == null
}

export function isFinalAnswer(body) {
  if (!body || typeof body !== 'object') return false
  if (isInterimAnswer(body)) return false
  if (body.verdict != null) return true
  if (body.status === 'done') return true
  if (typeof body.answer === 'string' && body.answer.trim()) return true
  // structured broker failure (e.g. no route) — final even without a verdict
  if (body.ok === false && body.error) return true
  // bare ok:true with nothing else is NOT final (keep polling)
  return false
}

export async function fileExists(filePath) {
  try {
    await fsp.access(filePath)
    return true
  } catch {
    return false
  }
}

function execFileP(bin, argv) {
  return new Promise((resolve) => {
    execFile(bin, argv, (error, _stdout, _stderr) => {
      if (!error) return resolve({ code: 0 })
      if (typeof error.code === 'number') return resolve({ code: error.code })
      return resolve({ code: null, errno: error.code || error.code })
    })
  })
}

// Scan /proc/*/cmdline for the gate ticket string when pgrep is unavailable.
async function scanProcForTicket(ticket) {
  let entries
  try {
    entries = await fsp.readdir('/proc')
  } catch {
    return 'unknown'
  }
  let sawPid = false
  for (const ent of entries) {
    if (!/^\d+$/.test(ent)) continue
    sawPid = true
    try {
      const buf = await fsp.readFile(path.join('/proc', ent, 'cmdline'))
      if (buf.includes(ticket)) return 'running'
    } catch {
      /* process vanished mid-scan */
    }
  }
  return sawPid ? 'exited' : 'unknown'
}

/**
 * Portable "is the gate still running?" probe for a gate_running ack.
 *
 * Preferred: `pgrep -f -- <gate_ticket>` (exit 0 running, exit 1 exited).
 * If pgrep is missing, fall back to scanning every PID's cmdline under /proc.
 * If neither is available, return 'unknown' (the caller reports it like running).
 * `pgrep` is injectable so tests don't depend on the OS.
 */
export async function probeGateProcess(ack, { pgrep } = {}) {
  const ticket = String(ack?.gate_ticket || '').trim()
  if (!ticket) return 'unknown'
  const bin = pgrep || 'pgrep'
  const r = await execFileP(bin, ['-f', '--', ticket])
  if (r.code === 0) return 'running'
  if (r.code === 1) return 'exited'
  // pgrep unavailable (ENOENT) or failed — fall back to /proc
  return scanProcForTicket(ticket)
}

export function resolveDshHome(env = process.env, dshHome, home) {
  if (dshHome) return dshHome
  if (env.DSH_HOME) return env.DSH_HOME
  return path.join(home || os.homedir(), '.dsh')
}

// Trial-broker's MAILBOX is a fixed global root under $DSH_HOME (not role home);
// role home (DSH_HOME → ~/.dsh-homes/<role>/...) must never take priority,
// or pending messages land where no watcher reads them.
export function resolveMailboxRoots(config = {}) {
  const env = config.env || process.env
  // Global thin-state must NOT follow role-home DSH_HOME (config.dshHome).
  // Prefer DSH_TRIAL_THIN_STATE / DSH_TRIAL_MAILBOX; else $DSH_GLOBAL_HOME or $HOME/.dsh.
  const hostHome = config.hostHome || env.HOME || os.homedir()
  const globalDsh = env.DSH_GLOBAL_HOME || path.join(hostHome, '.dsh')
  const GLOBAL_THIN =
    env.DSH_TRIAL_THIN_STATE || path.join(globalDsh, 'supervisor', 'thin-state')

  let root
  if (config.mailboxRoot) {
    root = path.isAbsolute(config.mailboxRoot)
      ? config.mailboxRoot
      : path.join(GLOBAL_THIN, config.mailboxRoot)
  } else if (env.DSH_TRIAL_MAILBOX) {
    root = env.DSH_TRIAL_MAILBOX
  } else {
    root = path.join(GLOBAL_THIN, 'mailbox')
  }
  const thin = path.dirname(root)
  return {
    thin,
    root,
    pending: path.join(root, 'pending'),
    answers: path.join(root, 'answers'),
    archive: path.join(root, 'archive'),
  }
}

export async function ensureMailboxDirs(dirs) {
  for (const d of [dirs.root, dirs.pending, dirs.answers, dirs.archive]) {
    await fsp.mkdir(d, { recursive: true })
  }
}

export function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      const err = new Error('aborted')
      err.name = 'AbortError'
      reject(err)
      return
    }
    const t = setTimeout(resolve, ms)
    const onAbort = () => {
      clearTimeout(t)
      const err = new Error('aborted')
      err.name = 'AbortError'
      reject(err)
    }
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}

export async function pollAnswer(answerPath, {
  deadline,
  pollMs,
  signal,
  started,
  gateBudgetSec = DEFAULT_GATE_BUDGET_SEC,
  gateProbe = probeGateProcess,
  ackGraceMs = DEFAULT_GATE_ACK_GRACE_MS,
} = {}) {
  let ack = null
  while (Date.now() < deadline) {
    if (signal?.aborted) {
      return {
        ok: false,
        timed_out: false,
        aborted: true,
        waited_ms: Date.now() - started,
        error: 'aborted while waiting',
        degrade: 'question',
        gate_ack: ack,
      }
    }
    try {
      const raw = await fsp.readFile(answerPath, 'utf8')
      const ans = JSON.parse(raw)
      if (isInterimAnswer(ans)) {
        if (!ack) {
          ack = {
            gate_ticket: ans.gate_ticket || null,
            gate_timeout_sec: Number.isFinite(Number(ans.gate_timeout_sec))
              ? Number(ans.gate_timeout_sec)
              : null,
            started_at: ans.started_at || null,
          }
          // Use the CLIENT clock (when we observed the ack), never the broker's
          // started_at, so skew between the two hosts cannot shrink our wait.
          const ackSeenAt = Date.now()
          const budgetSec = ack.gate_timeout_sec > 0 ? ack.gate_timeout_sec : gateBudgetSec
          const extended = ackSeenAt + budgetSec * 1000 + ackGraceMs
          if (extended > deadline) deadline = extended
        }
      } else if (isFinalAnswer(ans)) {
        return {
          ok: true,
          body: ans,
          waited_ms: Date.now() - started,
          timed_out: false,
          gate_ack: ack,
        }
      }
    } catch (e) {
      if (e && e.code !== 'ENOENT' && !(e instanceof SyntaxError)) {
        /* ignore transient */
      }
    }
    await sleep(pollMs, signal)
  }

  if (!ack) {
    return {
      ok: false,
      timed_out: true,
      waited_ms: Date.now() - started,
      error: 'timed out waiting for mailbox answer (no gate_running ack)',
      gate_status: 'no_ack',
      gate_ack: null,
      degrade: 'question',
    }
  }

  let status = 'unknown'
  try {
    const probed = await gateProbe(ack)
    status = probed === 'running' ? 'running' : probed === 'unknown' ? 'unknown' : 'exited_no_result'
  } catch {
    status = 'unknown'
  }
  const ticket = ack.gate_ticket
  if (status === 'running') {
    return {
      ok: false,
      timed_out: true,
      waited_ms: Date.now() - started,
      error: `gate still running (ticket=${ticket}); wait longer or check broker`,
      gate_status: 'running',
      gate_ticket: ticket,
      gate_ack: ack,
      degrade: 'retry_wait',
    }
  }
  if (status === 'unknown') {
    return {
      ok: false,
      timed_out: true,
      waited_ms: Date.now() - started,
      error: `gate status unknown (ticket=${ticket})`,
      gate_status: 'unknown',
      gate_ticket: ticket,
      gate_ack: ack,
      degrade: 'retry_wait',
    }
  }
  return {
    ok: false,
    timed_out: true,
    waited_ms: Date.now() - started,
    error: `gate exited without result (ticket=${ticket})`,
    gate_status: 'exited_no_result',
    gate_ticket: ticket,
    gate_ack: ack,
    degrade: 'close_ticket',
  }
}
