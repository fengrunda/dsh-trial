/**
 * send_to_role primitive + thin wrappers ask_supervisor / submit_for_review.
 */
import fsp from 'node:fs/promises'
import path from 'node:path'
import crypto from 'node:crypto'
import {
  resolveMailboxRoots,
  ensureMailboxDirs,
  pollAnswer,
} from './mailbox.js'

export { resolveMailboxRoots, ensureMailboxDirs } from './mailbox.js'

export const SEND_TO_ROLE_TOOL = 'send_to_role'
export const ASK_SUPERVISOR_TOOL = 'ask_supervisor'
export const SUBMIT_FOR_REVIEW_TOOL = 'submit_for_review'

const DEFAULTS = {
  mailboxRoot: 'mailbox',
  timeoutSec: 600,
  pollMs: 2000,
  wrappers: ['ask_supervisor', 'submit_for_review'],
  enableSendToRole: true,
  inplaceReworkMaxPrompt: 20000,
  inplaceReworkMaxFindings: 3,
}

export function normalizeConfig(config = {}) {
  const timeoutSec = Number(config.timeoutSec ?? DEFAULTS.timeoutSec)
  const pollMs = Number(config.pollMs ?? DEFAULTS.pollMs)
  if (!Number.isFinite(timeoutSec) || timeoutSec < 1) {
    throw new Error('role-bridge: timeoutSec must be >= 1')
  }
  if (!Number.isFinite(pollMs) || pollMs < 50) {
    throw new Error('role-bridge: pollMs must be >= 50')
  }
  let wrappers = config.wrappers ?? DEFAULTS.wrappers
  if (typeof wrappers === 'string') wrappers = wrappers.split(',').map((s) => s.trim()).filter(Boolean)
  if (!Array.isArray(wrappers)) wrappers = [...DEFAULTS.wrappers]
  return {
    mailboxRoot: config.mailboxRoot || DEFAULTS.mailboxRoot,
    timeoutSec,
    pollMs,
    wrappers,
    enableSendToRole: config.enableSendToRole !== false,
    inplaceReworkMaxPrompt: Number(config.inplaceReworkMaxPrompt ?? DEFAULTS.inplaceReworkMaxPrompt),
    inplaceReworkMaxFindings: Number(config.inplaceReworkMaxFindings ?? DEFAULTS.inplaceReworkMaxFindings),
    env: config.env,
    dshHome: config.dshHome,
    home: config.home,
  }
}

function newMsgId(prefix = 'msg') {
  return `${prefix}-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`
}

/**
 * Core: write pending message, optionally wait for answer.
 * Message schema (thin-state/mailbox/pending/<id>.json):
 *   id, kind, to_role, from{ticket,role,goal,slice}, payload, reply_to, wait, timeout_sec, status, created_at
 */
export async function sendToRole(args, exec = {}, config = {}) {
  const cfg = normalizeConfig(config)
  const dirs = resolveMailboxRoots(cfg)
  await ensureMailboxDirs(dirs)

  const toRole = String(args?.to_role || args?.toRole || '').trim()
  const kind = String(args?.kind || '').trim()
  if (!toRole) return { ok: false, error: 'to_role required' }
  if (!kind) return { ok: false, error: 'kind required' }

  const payload = args?.payload
  if (payload == null || typeof payload !== 'object' || Array.isArray(payload)) {
    return { ok: false, error: 'payload must be a JSON object' }
  }

  const wait = args?.wait !== false && args?.wait !== 'false'
  const timeoutSec = Number(args?.timeout ?? args?.timeout_sec ?? cfg.timeoutSec)
  const msgId =
    (typeof args?.message_id === 'string' && args.message_id.trim()) ||
    (typeof args?.ask_id === 'string' && args.ask_id.trim()) ||
    newMsgId(kind.replace(/[^a-z0-9]+/gi, '-').slice(0, 24) || 'msg')

  const pendingPath = path.join(dirs.pending, `${msgId}.json`)
  const answerPath = path.join(dirs.answers, `${msgId}.json`)

  const message = {
    id: msgId,
    ask_id: msgId, // back-compat with earlier mailbox handlers
    kind,
    to_role: toRole,
    from: {
      ticket: args?.ticket || process.env.DSH_TICKET || null,
      role: args?.from_role || process.env.DSH_ROLE || null,
      goal: args?.goal || null,
      slice: args?.slice || null,
    },
    payload,
    reply_to: args?.reply_to || null,
    wait,
    timeout_sec: timeoutSec,
    status: 'pending',
    created_at: new Date().toISOString(),
    // flat mirrors for older broker fields
    goal: args?.goal || null,
    slice: args?.slice || null,
    ticket: args?.ticket || process.env.DSH_TICKET || null,
    ...flattenKnownPayload(kind, payload),
  }

  await fsp.writeFile(pendingPath, JSON.stringify(message, null, 2) + '\n', {
    encoding: 'utf8',
    mode: 0o600,
  })

  if (!wait) {
    return {
      ok: true,
      message_id: msgId,
      waiting: false,
      kind,
      to_role: toRole,
    }
  }

  const signal = exec?.signal
  const started = Date.now()
  let polled
  try {
    polled = await pollAnswer(answerPath, {
      deadline: started + timeoutSec * 1000,
      pollMs: cfg.pollMs,
      signal,
      started,
    })
  } catch (e) {
    if (e?.name === 'AbortError') {
      return {
        ok: false,
        message_id: msgId,
        kind,
        to_role: toRole,
        aborted: true,
        waited_ms: Date.now() - started,
        error: 'aborted while waiting for role reply',
        degrade: degradeForKind(kind),
      }
    }
    throw e
  }

  if (!polled.ok) {
    return {
      ok: false,
      message_id: msgId,
      kind,
      to_role: toRole,
      timed_out: !!polled.timed_out,
      waited_ms: polled.waited_ms,
      error: polled.error || `role reply timed out after ${timeoutSec}s`,
      degrade: degradeForKind(kind),
    }
  }

  const body = polled.body || {}
  return {
    ok: true,
    message_id: msgId,
    kind,
    to_role: toRole,
    waited_ms: polled.waited_ms,
    timed_out: false,
    reply: body,
    // convenience flatten for wrappers
    ...presentReply(kind, body),
  }
}

function flattenKnownPayload(kind, payload) {
  if (kind === 'ask_supervisor') {
    return {
      questions: payload.questions,
      context: payload.context || '',
    }
  }
  if (kind === 'submit_for_review') {
    return {
      summary: payload.summary || '',
      changed_files: payload.changed_files || [],
      base: payload.base || '',
      commit: payload.commit || '',
      usage_prompt: payload.usage_prompt,
      round: payload.round,
    }
  }
  return {}
}

function degradeForKind(kind) {
  if (kind === 'ask_supervisor') return 'question'
  if (kind === 'submit_for_review') return 'close_ticket'
  return 'retry_or_close'
}

function presentReply(kind, body) {
  if (kind === 'ask_supervisor') {
    return {
      answer: String(body.answer || '').trim(),
      supervisor_ticket: body.supervisor_ticket || null,
    }
  }
  if (kind === 'submit_for_review') {
    return {
      verdict: body.verdict,
      rework_mode: body.rework_mode || body.mode,
      instruction: body.instruction,
      findings: body.findings || [],
      unmet_acceptance: body.unmet_acceptance || [],
      gate_ticket: body.gate_ticket || null,
    }
  }
  return {}
}

export function renderSendResult(value) {
  const r = value && typeof value === 'object' ? value : {}
  if (r.ok && r.reply) {
    return [{
      type: 'text',
      text: `send_to_role ok id=${r.message_id} kind=${r.kind} to=${r.to_role} waited_ms=${r.waited_ms}\n`
        + JSON.stringify(r.reply, null, 2),
    }]
  }
  if (r.ok && !r.waiting) {
    return [{ type: 'text', text: `send_to_role queued id=${r.message_id} kind=${r.kind}` }]
  }
  return [{
    type: 'text',
    text: `send_to_role failed: ${r.error || 'unknown'}\ndegrade=${r.degrade || ''}`,
  }]
}

export function createSendToRoleToolOptions(config = {}) {
  const cfg = normalizeConfig(config)
  const timeoutMs = (cfg.timeoutSec + 60) * 1000
  return {
    name: SEND_TO_ROLE_TOOL,
    description:
      'Send a structured mid-ticket message to another dsh role via thin-state mailbox. '
      + 'Broker (broker-dsh-trial) must be running to wake the peer short-ticket and write the answer. '
      + 'Set wait=true (default) to block until reply or timeout. Do NOT use room_*.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      required: ['to_role', 'kind', 'payload'],
      properties: {
        to_role: { type: 'string', description: 'Target role: supervisor | gate | impl' },
        kind: { type: 'string', description: 'Message kind routed by broker routes.json' },
        payload: { type: 'object', additionalProperties: true, description: 'Kind-specific structured payload' },
        wait: { type: 'boolean', description: 'Block until answer (default true)' },
        timeout: { type: 'number', description: 'Seconds to wait (default from plugin config)' },
        goal: { type: 'string' },
        slice: { type: 'string' },
        ticket: { type: 'string' },
        from_role: { type: 'string' },
        message_id: { type: 'string' },
        reply_to: { type: 'string' },
      },
    },
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: (_a, v) => renderSendResult(v),
    },
    timeoutMs,
    isConcurrencySafe: () => false,
    execute: async (args, exec) => sendToRole(args, exec || {}, cfg),
  }
}

/** Thin wrapper: ask_supervisor → send_to_role(supervisor, ask_supervisor, …) */
export async function askSupervisor(args, exec = {}, config = {}) {
  const questions = args?.questions
  const qList = Array.isArray(questions)
    ? questions.map((q) => String(q || '').trim()).filter(Boolean)
    : typeof questions === 'string' && questions.trim()
      ? [questions.trim()]
      : []
  if (!qList.length) return { ok: false, error: 'questions required' }
  const result = await sendToRole(
    {
      to_role: 'supervisor',
      kind: 'ask_supervisor',
      payload: { questions: qList, context: args?.context || '' },
      wait: true,
      timeout: args?.timeout,
      goal: args?.goal,
      slice: args?.slice,
      ticket: args?.ticket,
      message_id: args?.ask_id,
    },
    exec,
    config,
  )
  if (result.ok) {
    return {
      ok: true,
      ask_id: result.message_id,
      answer: result.answer,
      waited_ms: result.waited_ms,
      supervisor_ticket: result.supervisor_ticket,
      timed_out: false,
    }
  }
  return {
    ok: false,
    ask_id: result.message_id,
    timed_out: !!result.timed_out,
    aborted: !!result.aborted,
    waited_ms: result.waited_ms,
    error: result.error,
    degrade: result.degrade || 'question',
    hint: 'Set summary status=question with the same questions for broker fallback.',
  }
}

export function createAskSupervisorToolOptions(config = {}) {
  const cfg = normalizeConfig(config)
  const timeoutMs = (cfg.timeoutSec + 60) * 1000
  return {
    name: ASK_SUPERVISOR_TOOL,
    description:
      'Ask the dsh trial supervisor mid-ticket (thin wrapper over send_to_role). '
      + 'Broker opens a supervisor short ticket. On timeout, set summary status=question.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      required: ['questions'],
      properties: {
        questions: {
          oneOf: [
            { type: 'string' },
            { type: 'array', items: { type: 'string' }, minItems: 1 },
          ],
        },
        context: { type: 'string' },
        goal: { type: 'string' },
        slice: { type: 'string' },
        ticket: { type: 'string' },
        ask_id: { type: 'string' },
        timeout: { type: 'number' },
      },
    },
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: (_a, v) => {
        if (v?.ok) {
          return [{ type: 'text', text: `ask_supervisor ok\nwaited_ms=${v.waited_ms}\nanswer:\n${v.answer}` }]
        }
        return [{ type: 'text', text: `ask_supervisor failed: ${v?.error}\ndegrade=${v?.degrade || ''}` }]
      },
    },
    timeoutMs,
    isConcurrencySafe: () => false,
    execute: async (args, exec) => askSupervisor(args, exec || {}, cfg),
  }
}

/** Thin wrapper: submit_for_review → send_to_role(gate, submit_for_review, …) */
export async function submitForReview(args, exec = {}, config = {}) {
  const usagePrompt = Number(args?.usage_prompt ?? args?.current_prompt_tokens ?? NaN)
  if (!Number.isFinite(usagePrompt) || usagePrompt < 0) {
    return { ok: false, error: 'usage_prompt required (non-negative number)' }
  }
  const summary = String(args?.summary || args?.notes || '').trim()
  if (!summary) return { ok: false, error: 'summary required' }
  const changed = args?.changed_files
  const changedFiles = Array.isArray(changed)
    ? changed.map(String)
    : typeof changed === 'string' && changed
      ? [changed]
      : []

  const result = await sendToRole(
    {
      to_role: 'gate',
      kind: 'submit_for_review',
      payload: {
        summary,
        changed_files: changedFiles,
        base: args?.base || '',
        commit: args?.commit || '',
        usage_prompt: usagePrompt,
        round: args?.round,
      },
      wait: true,
      timeout: args?.timeout,
      goal: args?.goal,
      slice: args?.slice,
      ticket: args?.ticket,
      message_id: args?.ask_id,
    },
    exec,
    config,
  )

  if (!result.ok) {
    return {
      ok: false,
      ask_id: result.message_id,
      timed_out: !!result.timed_out,
      waited_ms: result.waited_ms,
      error: result.error,
      degrade: result.degrade || 'close_ticket',
      hint: 'Fall back to summary status=done for broker gate.',
    }
  }

  const verdict = String(result.verdict || result.reply?.verdict || '').toUpperCase()
  if (verdict === 'PASS') {
    return {
      ok: true,
      ask_id: result.message_id,
      verdict: 'PASS',
      waited_ms: result.waited_ms,
      gate_ticket: result.gate_ticket,
      findings: result.findings || [],
      unmet_acceptance: result.unmet_acceptance || [],
      instruction: result.instruction || 'PASS — write final summary status=done and stop.',
      timed_out: false,
    }
  }
  return {
    ok: true,
    ask_id: result.message_id,
    verdict: 'HOLD',
    rework_mode: result.rework_mode || 'fresh',
    waited_ms: result.waited_ms,
    gate_ticket: result.gate_ticket,
    findings: result.findings || [],
    unmet_acceptance: result.unmet_acceptance || [],
    instruction: result.instruction || 'HOLD — follow rework_mode instruction',
    timed_out: false,
  }
}

export function createSubmitForReviewToolOptions(config = {}) {
  const cfg = normalizeConfig(config)
  const timeoutMs = (cfg.timeoutSec + 60) * 1000
  return {
    name: SUBMIT_FOR_REVIEW_TOOL,
    description:
      'Submit current slice for design-gate review mid-ticket (thin wrapper over send_to_role→gate). '
      + 'Pass usage_prompt for inplace vs fresh threshold. On PASS finish ticket; on HOLD follow instruction.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      required: ['usage_prompt', 'summary'],
      properties: {
        usage_prompt: { type: 'number' },
        summary: { type: 'string' },
        changed_files: {
          oneOf: [{ type: 'string' }, { type: 'array', items: { type: 'string' } }],
        },
        base: { type: 'string' },
        commit: { type: 'string' },
        goal: { type: 'string' },
        slice: { type: 'string' },
        ticket: { type: 'string' },
        round: { type: 'number' },
        ask_id: { type: 'string' },
        timeout: { type: 'number' },
        current_prompt_tokens: { type: 'number' },
      },
    },
    output: {
      schema: { type: 'object', additionalProperties: true },
      render: (_a, v) => {
        if (v?.ok && v.verdict === 'PASS') {
          return [{ type: 'text', text: `submit_for_review PASS\n${v.instruction}` }]
        }
        if (v?.ok && v.verdict === 'HOLD') {
          return [{
            type: 'text',
            text: `submit_for_review HOLD mode=${v.rework_mode}\n${v.instruction}\n`
              + `findings=${JSON.stringify(v.findings || [])}`,
          }]
        }
        return [{ type: 'text', text: `submit_for_review failed: ${v?.error}` }]
      },
    },
    timeoutMs,
    isConcurrencySafe: () => false,
    execute: async (args, exec) => submitForReview(args, exec || {}, cfg),
  }
}
