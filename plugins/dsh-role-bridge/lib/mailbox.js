/**
 * Shared thin-state mailbox helpers for ask_supervisor / submit_for_review.
 */
import fsp from 'node:fs/promises'
import path from 'node:path'
import os from 'node:os'

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

export async function pollAnswer(answerPath, { deadline, pollMs, signal, started }) {
  while (Date.now() < deadline) {
    if (signal?.aborted) {
      return {
        ok: false,
        timed_out: false,
        aborted: true,
        waited_ms: Date.now() - started,
        error: 'aborted while waiting',
        degrade: 'question',
      }
    }
    try {
      const raw = await fsp.readFile(answerPath, 'utf8')
      const ans = JSON.parse(raw)
      if (ans && (ans.answer || ans.verdict || ans.ok === true || ans.ok === false)) {
        return {
          ok: true,
          body: ans,
          waited_ms: Date.now() - started,
          timed_out: false,
        }
      }
    } catch (e) {
      if (e && e.code !== 'ENOENT' && !(e instanceof SyntaxError)) {
        /* ignore transient */
      }
    }
    await sleep(pollMs, signal)
  }
  return {
    ok: false,
    timed_out: true,
    waited_ms: Date.now() - started,
    error: 'timed out waiting for mailbox answer',
    degrade: 'question',
  }
}
