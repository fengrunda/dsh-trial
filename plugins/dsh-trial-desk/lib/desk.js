/**
 * dsh-trial-desk — pure helpers (no Cordis).
 *
 * Human/supervisor session writes Goal / chain / chain-reply JSON into
 * `$DSH_HOME/supervisor/trial/inbox/` and reads thin-state + broker pid.
 * Does **not** open-slice, join rooms, or call Hub webhooks.
 *
 * Job shapes match `broker/trial-broker.py` `load_job` + `broker/examples/job-*.json`.
 *
 * @module dsh-trial-desk/desk
 */

import crypto from 'node:crypto'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

/** Profiles the broker accepts on job.profile / gate_profile / supervisor_profile. */
export const JOB_PROFILES = Object.freeze([
  'acp',
  'acp-lite',
  'acp-trial',
  'acp-lite-trial',
])

export const JOB_TYPES = Object.freeze([
  'goal',
  'chain',
  'chain-reply',
  'goal-update',
  'ticket',
])

export const DROP_JOB_TOOL = 'trial_drop_job'
export const CHAIN_REPLY_TOOL = 'trial_chain_reply'
export const STATUS_TOOL = 'trial_status'
export const VALIDATE_JOB_TOOL = 'trial_validate_job'

const DEFAULTS = {
  recentLimit: 8,
}

const ROOM_KEY_RE = /^(room|rooms|room_id|roomid|join_room)$/i

/**
 * @param {{ dshHome?: string, home?: string, env?: NodeJS.ProcessEnv }} [opts]
 * @returns {string}
 */
export function resolveDshHome(opts = {}) {
  const env = opts.env ?? process.env
  if (typeof opts.dshHome === 'string' && opts.dshHome.trim()) {
    return path.resolve(opts.dshHome.trim())
  }
  if (typeof env.DSH_HOME === 'string' && env.DSH_HOME.trim()) {
    return path.resolve(env.DSH_HOME.trim())
  }
  if (typeof env.DSH_GLOBAL_HOME === 'string' && env.DSH_GLOBAL_HOME.trim()) {
    return path.resolve(env.DSH_GLOBAL_HOME.trim())
  }
  return path.resolve(opts.home ?? env.HOME ?? os.homedir(), '.dsh')
}

/**
 * @param {unknown} raw
 * @param {string} anchor
 * @param {string} fallbackRel
 * @returns {string}
 */
function resolveUnder(raw, anchor, fallbackRel) {
  const value = typeof raw === 'string' && raw.trim() ? raw.trim() : fallbackRel
  return path.isAbsolute(value) ? path.normalize(value) : path.resolve(anchor, value)
}

/**
 * @param {unknown} n
 * @param {string} label
 * @param {number} fallback
 * @returns {number}
 */
function requirePositiveInt(n, label, fallback) {
  if (n === undefined || n === null) return fallback
  if (typeof n !== 'number' || !Number.isInteger(n) || n < 1) {
    throw new Error(`trial-desk: ${label} must be a positive integer (got ${n})`)
  }
  return n
}

/**
 * Validate and normalise plugin config. No /home/box or /workspace defaults.
 *
 * @param {Record<string, unknown>} [config]
 * @param {{ dshHome?: string, home?: string, env?: NodeJS.ProcessEnv }} [opts]
 */
export function normalizeConfig(config, opts = {}) {
  config ??= {}
  const env = opts.env ?? config.env ?? process.env
  const dshHome = resolveDshHome({
    dshHome: config.dshHome ?? opts.dshHome,
    home: config.home ?? opts.home,
    env,
  })
  const inboxRoot = env.DSH_TRIAL_INBOX
    ? path.resolve(env.DSH_TRIAL_INBOX)
    : resolveUnder(config.inboxRoot, dshHome, path.join('supervisor', 'trial', 'inbox'))
  const thinStateRoot = env.DSH_TRIAL_THIN_STATE
    ? path.resolve(env.DSH_TRIAL_THIN_STATE)
    : resolveUnder(config.thinStateRoot, dshHome, path.join('supervisor', 'thin-state'))
  const mailboxRoot = env.DSH_TRIAL_MAILBOX
    ? path.resolve(env.DSH_TRIAL_MAILBOX)
    : resolveUnder(config.mailboxRoot, thinStateRoot, 'mailbox')
  const brokerDir = env.TRIAL_BROKER_DIR
    ? path.resolve(env.TRIAL_BROKER_DIR)
    : resolveUnder(config.brokerDir, dshHome, 'broker-dsh-trial')
  return {
    dshHome,
    inboxRoot,
    thinStateRoot,
    mailboxRoot,
    brokerDir,
    recentLimit: requirePositiveInt(config.recentLimit, 'recentLimit', DEFAULTS.recentLimit),
    env,
    home: opts.home ?? config.home,
  }
}

export function trialPaths(cfg) {
  const thin = cfg.thinStateRoot
  const mailbox = cfg.mailboxRoot
  return {
    dshHome: cfg.dshHome,
    inbox: cfg.inboxRoot,
    outbox: path.join(path.dirname(cfg.inboxRoot), 'outbox'),
    failed: path.join(path.dirname(cfg.inboxRoot), 'failed'),
    processing: path.join(path.dirname(cfg.inboxRoot), 'processing'),
    thin,
    packs: path.join(thin, 'packs'),
    summaries: path.join(thin, 'summaries'),
    chains: path.join(thin, 'chains'),
    goals: path.join(thin, 'goals'),
    mailbox,
    mailboxPending: path.join(mailbox, 'pending'),
    mailboxAnswers: path.join(mailbox, 'answers'),
    brokerDir: cfg.brokerDir,
    pidfile: path.join(cfg.brokerDir, 'trial-broker.pid'),
    prodBrokerDir: path.join(cfg.dshHome, 'broker-khub-prod'),
    prodSock: path.join(cfg.dshHome, 'broker-khub-prod', 'broker.sock'),
  }
}

/**
 * Parse job argument: object or JSON string. Does not invent fields.
 * @param {unknown} raw
 * @returns {{ ok: true, job: Record<string, unknown> } | { ok: false, error: string }}
 */
export function parseJobArg(raw) {
  if (raw == null) return { ok: false, error: 'job required' }
  if (typeof raw === 'string') {
    const text = raw.trim()
    if (!text) return { ok: false, error: 'job JSON string is empty' }
    try {
      const parsed = JSON.parse(text)
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        return { ok: false, error: 'job must be a JSON object' }
      }
      return { ok: true, job: parsed }
    } catch (e) {
      return { ok: false, error: `job is not valid JSON: ${e.message}` }
    }
  }
  if (typeof raw !== 'object' || Array.isArray(raw)) {
    return { ok: false, error: 'job must be a JSON object' }
  }
  return { ok: true, job: raw }
}

function jobTypeOf(data) {
  const t = String(data?.type || 'ticket').trim().toLowerCase()
  return t || 'ticket'
}

function findRoomKey(value, trail = []) {
  if (value == null) return null
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      const hit = findRoomKey(value[i], trail.concat(String(i)))
      if (hit) return hit
    }
    return null
  }
  if (typeof value !== 'object') return null
  for (const [k, v] of Object.entries(value)) {
    if (ROOM_KEY_RE.test(k) || /^room_/i.test(k)) {
      return trail.concat(k).join('.')
    }
    const hit = findRoomKey(v, trail.concat(k))
    if (hit) return hit
  }
  return null
}

function profileError(label, value) {
  if (!JOB_PROFILES.includes(String(value))) {
    return `${label} must be acp|acp-lite|acp-trial|acp-lite-trial, got ${JSON.stringify(value)}`
  }
  return null
}

function asInt(value, fallback) {
  // Match Python `int(data.get(k) or fallback)`: 0 / '' / null → fallback.
  if (value === undefined || value === null || value === '' || value === 0 || value === false) {
    return fallback
  }
  const n = Number(value)
  if (!Number.isFinite(n) || !Number.isInteger(n)) return NaN
  return n
}

/** Cwd values that are never acceptable as a real workdir. */
const PLACEHOLDER_CWDS = new Set(['/path/to/your/workdir', '/workspace'])

/**
 * Enforce a non-empty real cwd for goal / chain / single-ticket jobs.
 * Returns an error string, or null when cwd is acceptable.
 */
function cwdError(cwd) {
  const s = cwd == null ? '' : String(cwd).trim()
  if (!s) {
    return 'cwd required (non-empty absolute path). Mac/本机请写绝对路径 — do not omit cwd'
  }
  if (PLACEHOLDER_CWDS.has(s)) {
    return `cwd rejects placeholder or /workspace (${JSON.stringify(s)}). Mac/本机请写绝对路径，例如 /Users/you/proj 或 /tmp/dsh-trial-example-workdir`
  }
  return null
}

/**
 * Validate a job against broker `load_job` rules. Extra keys are kept (pass-through).
 * Rejects missing / placeholder / `/workspace` cwd for goal, chain and single-ticket
 * jobs (never silently defaults cwd). `chain-reply` / `goal-update` are exempt.
 *
 * @param {Record<string, unknown>} data
 * @returns {{ ok: true, type: string, errors: [] } | { ok: false, type: string, errors: string[] }}
 */
export function validateJob(data) {
  const errors = []
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    return { ok: false, type: 'invalid', errors: ['job must be a JSON object'] }
  }
  const roomKey = findRoomKey(data)
  if (roomKey) {
    errors.push(`desk never writes room_* fields (found ${roomKey})`)
  }

  const jtype = jobTypeOf(data)

  if (jtype === 'chain') {
    if (!data.slice) errors.push('chain job missing slice')
    if (!data.pack) errors.push('chain job missing pack')
    const profile = data.profile == null || data.profile === '' ? 'acp' : data.profile
    const pe = profileError('profile', profile)
    if (pe) errors.push(pe)
    const gate = data.gate_profile == null || data.gate_profile === '' ? profile : data.gate_profile
    const ge = profileError('gate_profile', gate)
    if (ge) errors.push(ge)
    const maxRounds = asInt(data.max_rounds, 2)
    if (!Number.isInteger(maxRounds) || maxRounds < 1 || maxRounds > 8) {
      errors.push(`max_rounds out of range: ${data.max_rounds}`)
    }
  } else if (jtype === 'chain-reply') {
    if (!data.slice) errors.push('chain-reply missing slice')
    if (data.answer == null || String(data.answer).trim() === '') {
      errors.push('chain-reply missing slice/answer')
    }
  } else if (jtype === 'goal-update') {
    const goalId = data.goal || data.id
    if (!goalId) errors.push('goal-update missing goal')
  } else if (jtype === 'goal') {
    const goalId = data.goal || data.id
    const brief = data.brief || data.goal_text || data.prompt
    if (!goalId || !brief) errors.push('goal job missing goal/brief')
    const profile = data.profile == null || data.profile === '' ? 'acp-lite' : data.profile
    const pe = profileError('profile', profile)
    if (pe) errors.push(pe)
    const sup = data.supervisor_profile == null || data.supervisor_profile === ''
      ? 'acp-lite'
      : data.supervisor_profile
    const se = profileError('supervisor_profile', sup)
    if (se) errors.push(se)
    if (data.gate_profile) {
      const ge = profileError('gate_profile', data.gate_profile)
      if (ge) errors.push(ge)
    }
    const maxSlices = asInt(data.max_slices, 1)
    if (!Number.isInteger(maxSlices) || maxSlices < 1) {
      errors.push(`max_slices must be a positive integer, got ${data.max_slices}`)
    }
    const maxSup = asInt(data.max_supervisor_tickets, 8)
    if (!Number.isInteger(maxSup) || maxSup < 1) {
      errors.push(`max_supervisor_tickets must be a positive integer, got ${data.max_supervisor_tickets}`)
    }
    const maxRounds = asInt(data.max_rounds, 2)
    if (!Number.isInteger(maxRounds) || maxRounds < 1 || maxRounds > 8) {
      errors.push(`max_rounds out of range: ${data.max_rounds}`)
    }
  } else {
    // broker load_job: any other type is a single ticket
    if (!data.ticket || !data.pack) errors.push('job missing ticket/pack')
    const profile = data.profile == null || data.profile === '' ? 'acp' : data.profile
    const pe = profileError('profile', profile)
    if (pe) errors.push(pe)
  }

  // Real cwd is required for goal / chain / single-ticket jobs.
  // Skip for chain-reply and goal-update (no cwd needed).
  if (jtype !== 'chain-reply' && jtype !== 'goal-update') {
    const ce = cwdError(data.cwd)
    if (ce) errors.push(ce)
  }

  return errors.length
    ? { ok: false, type: jtype, errors }
    : { ok: true, type: jtype, errors: [] }
}

export function safeJobId(raw, fallback) {
  const s = String(raw || fallback || '').trim()
  if (!s) return ''
  if (s.includes('..') || s.includes('/') || s.includes('\\') || s.includes('\0')) return ''
  if (!/^[A-Za-z0-9._@+-]+$/.test(s)) return ''
  return s.slice(0, 120)
}

function newJobId(type) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const rand = crypto.randomBytes(3).toString('hex')
  return `desk-${type}-${stamp}-${rand}`
}

export async function ensureInbox(inboxRoot) {
  await fsp.mkdir(inboxRoot, { recursive: true })
}

/**
 * Write job JSON into inbox. Never overwrites an existing file.
 *
 * @param {Record<string, unknown>} job
 * @param {{ dryRun?: boolean, filename?: string }} [opts]
 * @param {ReturnType<typeof normalizeConfig>} cfg
 */
export async function dropJob(job, opts = {}, cfg) {
  const parsed = parseJobArg(job)
  if (!parsed.ok) return { ok: false, error: parsed.error, errors: [parsed.error] }
  const data = { ...parsed.job }
  const check = validateJob(data)
  if (!check.ok) {
    return {
      ok: false,
      error: check.errors.join('; '),
      errors: check.errors,
      type: check.type,
      dry_run: !!opts.dryRun,
    }
  }

  const type = check.type
  if (type !== 'ticket' && (data.type == null || data.type === '')) {
    data.type = type
  }

  let id
  if (data.id != null && String(data.id).trim() !== '') {
    id = safeJobId(data.id, '')
    if (!id) {
      return {
        ok: false,
        error: 'invalid id (use [A-Za-z0-9._@+-], no path separators)',
        errors: ['invalid id'],
        type,
      }
    }
    data.id = id
  } else {
    id = newJobId(type)
    data.id = id
  }

  const filenameRaw = opts.filename ? safeJobId(opts.filename.replace(/\.json$/i, ''), '') : id
  if (!filenameRaw) {
    return { ok: false, error: 'invalid filename/id (use [A-Za-z0-9._@+-])', errors: ['invalid filename/id'] }
  }
  const filename = `${filenameRaw}.json`
  const dest = path.join(cfg.inboxRoot, filename)
  const body = `${JSON.stringify(data, null, 2)}\n`

  if (opts.dryRun) {
    return {
      ok: true,
      dry_run: true,
      type,
      id: data.id,
      would_write: dest,
      inbox: cfg.inboxRoot,
      job: data,
    }
  }

  await ensureInbox(cfg.inboxRoot)
  try {
    await fsp.access(dest)
    return {
      ok: false,
      error: `inbox file already exists: ${dest}`,
      errors: [`inbox file already exists: ${dest}`],
      path: dest,
      type,
      id: data.id,
    }
  } catch (e) {
    if (e && e.code !== 'ENOENT') throw e
  }

  const tmp = path.join(cfg.inboxRoot, `.${filenameRaw}.${crypto.randomBytes(4).toString('hex')}.tmp`)
  await fsp.writeFile(tmp, body, { encoding: 'utf8', mode: 0o600 })
  await fsp.rename(tmp, dest)
  return {
    ok: true,
    dry_run: false,
    type,
    id: data.id,
    path: dest,
    inbox: cfg.inboxRoot,
    hint: 'broker daemon consumes this file; desk does not open-slice. Ensure `dsh-trial start` is running.',
  }
}

export async function dropChainReply(args = {}, opts = {}, cfg) {
  const slice = String(args.slice || '').trim()
  const answer = args.answer == null ? '' : String(args.answer)
  const job = {
    type: 'chain-reply',
    ...(args.id ? { id: args.id } : {}),
    slice,
    answer,
  }
  return dropJob(job, opts, cfg)
}

async function listJsonFiles(dir, limit) {
  let names
  try {
    names = await fsp.readdir(dir)
  } catch (e) {
    if (e && e.code === 'ENOENT') return { exists: false, entries: [], total: 0 }
    throw e
  }
  const rows = []
  for (const name of names) {
    if (name.startsWith('.') || name === 'README.md') continue
    const full = path.join(dir, name)
    try {
      const st = await fsp.stat(full)
      if (!st.isFile()) continue
      rows.push({ name, path: full, mtime_ms: st.mtimeMs, bytes: st.size })
    } catch {
      /* skip */
    }
  }
  rows.sort((a, b) => b.mtime_ms - a.mtime_ms)
  return { exists: true, entries: rows.slice(0, limit), total: rows.length }
}

async function readJsonThin(filePath) {
  try {
    const raw = await fsp.readFile(filePath, 'utf8')
    return JSON.parse(raw)
  } catch {
    return null
  }
}

export async function readBrokerAlive(pidfile) {
  try {
    const raw = await fsp.readFile(pidfile, 'utf8')
    const pid = Number.parseInt(String(raw).trim(), 10)
    if (!Number.isInteger(pid) || pid <= 0) {
      return { alive: false, pid: null, pidfile, stale: true }
    }
    try {
      process.kill(pid, 0)
      return { alive: true, pid, pidfile }
    } catch {
      return { alive: false, pid, pidfile, stale: true }
    }
  } catch (e) {
    if (e && e.code === 'ENOENT') return { alive: false, pid: null, pidfile }
    throw e
  }
}

/**
 * Read-only snapshot: broker pid, inbox, chains, summaries, mailbox pending, goals.
 */
export async function readTrialStatus(cfg) {
  const paths = trialPaths(cfg)
  const limit = cfg.recentLimit
  const broker = await readBrokerAlive(paths.pidfile)
  let prodSockExists = false
  try {
    await fsp.access(paths.prodSock)
    prodSockExists = true
  } catch {
    prodSockExists = false
  }

  const [inbox, outbox, failed, chainsDir, summariesDir, pendingDir, goalsDir] = await Promise.all([
    listJsonFiles(paths.inbox, limit),
    listJsonFiles(paths.outbox, limit),
    listJsonFiles(paths.failed, limit),
    listJsonFiles(paths.chains, limit),
    listJsonFiles(paths.summaries, limit),
    listJsonFiles(paths.mailboxPending, limit),
    listJsonFiles(paths.goals, limit),
  ])

  const chains = []
  for (const e of chainsDir.entries) {
    const body = await readJsonThin(e.path)
    chains.push({
      file: e.name,
      slice: body?.slice || e.name.replace(/\.json$/i, ''),
      state: body?.state || null,
      goal: body?.goal || null,
      updated_at: body?.updated_at || null,
    })
  }

  const goals = []
  for (const e of goalsDir.entries) {
    const body = await readJsonThin(e.path)
    goals.push({
      file: e.name,
      goal: body?.goal || e.name.replace(/\.json$/i, ''),
      status: body?.status || null,
      slices: Array.isArray(body?.slices) ? body.slices.length : null,
      updated_at: body?.updated_at || body?.last_update_at || null,
    })
  }

  const mailboxPending = []
  for (const e of pendingDir.entries) {
    const body = await readJsonThin(e.path)
    mailboxPending.push({
      file: e.name,
      id: body?.id || body?.ask_id || e.name.replace(/\.json$/i, ''),
      kind: body?.kind || null,
      to_role: body?.to_role || null,
      status: body?.status || 'pending',
    })
  }

  return {
    ok: true,
    desk: 'dsh-trial-desk',
    note: 'desk reads state only; broker daemon owns open-slice --final. Never rooms / never broker-khub-prod.',
    dsh_home: paths.dshHome,
    broker: {
      alive: broker.alive,
      pid: broker.pid,
      pidfile: broker.pidfile,
      state_dir: paths.brokerDir,
      stale: broker.stale || false,
      hint: broker.alive ? null : 'broker not running — `dsh-trial start` (wraps broker/start.sh)',
    },
    prod_broker: {
      dir: paths.prodBrokerDir,
      sock_exists: prodSockExists,
      note: 'coexist OK; desk/trial never start/stop/resume broker-khub-prod',
    },
    inbox: {
      path: paths.inbox,
      pending: inbox.entries.map((e) => e.name),
      count: inbox.total ?? inbox.entries.length,
    },
    outbox: { path: paths.outbox, recent: outbox.entries.map((e) => e.name) },
    failed: { path: paths.failed, recent: failed.entries.map((e) => e.name) },
    chains: { path: paths.chains, recent: chains },
    summaries: {
      path: paths.summaries,
      recent: summariesDir.entries.map((e) => e.name),
    },
    mailbox: {
      path: paths.mailbox,
      pending_count: pendingDir.total ?? mailboxPending.length,
      pending: mailboxPending,
    },
    goals: { path: paths.goals, recent: goals },
  }
}

export function renderDropResult(value) {
  const r = value && typeof value === 'object' ? value : {}
  if (r.ok && r.dry_run) {
    return [{
      type: 'text',
      text: `trial_drop_job dry-run ok type=${r.type} id=${r.id}\nwould_write=${r.would_write}\n${JSON.stringify(r.job, null, 2)}`,
    }]
  }
  if (r.ok) {
    return [{
      type: 'text',
      text: `trial_drop_job wrote type=${r.type} id=${r.id}\npath=${r.path}\n${r.hint || ''}`,
    }]
  }
  return [{
    type: 'text',
    text: `trial_drop_job failed: ${r.error || 'unknown'}\n${(r.errors || []).join('\n')}`,
  }]
}

export function renderStatusResult(value) {
  const r = value && typeof value === 'object' ? value : {}
  if (!r.ok) {
    return [{ type: 'text', text: `trial_status failed: ${r.error || 'unknown'}` }]
  }
  const b = r.broker || {}
  const lines = [
    `trial_status broker_alive=${b.alive} pid=${b.pid}`,
    `inbox pending=${(r.inbox?.pending || []).length} ${(r.inbox?.pending || []).join(',')}`,
    `chains=${(r.chains?.recent || []).map((c) => `${c.slice}:${c.state}`).join(',') || '(none)'}`,
    `goals=${(r.goals?.recent || []).map((g) => `${g.goal}:${g.status}`).join(',') || '(none)'}`,
    `summaries=${(r.summaries?.recent || []).join(',') || '(none)'}`,
    `mailbox_pending=${r.mailbox?.pending_count ?? 0}`,
    b.hint ? `hint=${b.hint}` : '',
    JSON.stringify({
      broker: r.broker,
      inbox: r.inbox,
      chains: r.chains,
      goals: r.goals,
      mailbox: r.mailbox,
      summaries: r.summaries,
      prod_broker: r.prod_broker,
    }, null, 2),
  ]
  return [{ type: 'text', text: lines.filter(Boolean).join('\n') }]
}

function jobParameters() {
  return {
    type: 'object',
    additionalProperties: false,
    properties: {
      job: {
        description:
          'Inbox JSON object (or JSON string) matching broker/examples/job-goal-*.json / job-chain-*.json. Types: goal | chain | chain-reply | goal-update | ticket.',
        oneOf: [
          { type: 'object', additionalProperties: true },
          { type: 'string' },
        ],
      },
      dry_run: {
        type: 'boolean',
        description: 'Validate and show the file that would be written; do not touch inbox',
      },
      filename: {
        type: 'string',
        description: 'Optional inbox filename stem (default job.id)',
      },
    },
    required: ['job'],
  }
}

export function createDropJobToolOptions(config = {}) {
  const cfg = normalizeConfig(config)
  return {
    name: DROP_JOB_TOOL,
    description:
      'Drop a dsh-trial Goal or chain job JSON into $DSH_HOME/supervisor/trial/inbox/ '
      + '(same shapes as broker/examples/job-goal-*.json and job-chain-*.json). '
      + 'Desk is UX only: it never open-slices, never joins rooms, never calls Hub. '
      + 'The trial broker daemon must already be running (`dsh-trial start`). '
      + 'Set dry_run=true to validate without writing.',
    parameters: jobParameters(),
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: (_a, v) => renderDropResult(v),
    },
    timeoutMs: 15_000,
    isConcurrencySafe: () => true,
    execute: async (args) => dropJob(args?.job, { dryRun: !!args?.dry_run, filename: args?.filename }, cfg),
  }
}

export function createValidateJobToolOptions(config = {}) {
  const cfg = normalizeConfig(config)
  return {
    name: VALIDATE_JOB_TOOL,
    description:
      'Validate a dsh-trial inbox job JSON against broker load_job rules without writing. '
      + 'Same as trial_drop_job with dry_run=true.',
    parameters: jobParameters(),
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: (_a, v) => renderDropResult({ ...v, dry_run: true }),
    },
    timeoutMs: 10_000,
    isConcurrencySafe: () => true,
    execute: async (args) => dropJob(args?.job, { dryRun: true, filename: args?.filename }, cfg),
  }
}

export function createChainReplyToolOptions(config = {}) {
  const cfg = normalizeConfig(config)
  return {
    name: CHAIN_REPLY_TOOL,
    description:
      'Write type:chain-reply into the trial inbox (same shape as broker/examples/job-chain-reply.json). '
      + 'Use when a chain is awaiting_supervisor. Does not open-slice.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      required: ['slice', 'answer'],
      properties: {
        slice: { type: 'string', description: 'Chain slice id (thin-state/chains/<slice>.json)' },
        answer: { type: 'string', description: 'Supervisor answer text' },
        id: { type: 'string', description: 'Optional job id / filename stem' },
        dry_run: { type: 'boolean' },
      },
    },
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: (_a, v) => renderDropResult(v),
    },
    timeoutMs: 15_000,
    isConcurrencySafe: () => true,
    execute: async (args) => dropChainReply(args || {}, { dryRun: !!args?.dry_run }, cfg),
  }
}

export function createStatusToolOptions(config = {}) {
  const cfg = normalizeConfig(config)
  return {
    name: STATUS_TOOL,
    description:
      'Read dsh-trial thin-state + broker pid (alive?). Reports recent chains, summaries, '
      + 'mailbox pending, inbox, goals. Read-only — does not start/stop broker or prod.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      properties: {},
    },
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: (_a, v) => renderStatusResult(v),
    },
    timeoutMs: 10_000,
    isConcurrencySafe: () => true,
    execute: async () => readTrialStatus(cfg),
  }
}
