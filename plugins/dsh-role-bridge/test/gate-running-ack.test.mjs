/**
 * Regression tests for the gate_running ack + submit_for_review wait budget.
 *
 * Incident: the broker only wrote answers/<ask_id>.json AFTER a long gate
 * finished, so submit_for_review timed out at ~180s while the gate was still
 * running, and the caller wrongly concluded the gate was absent / set blocked.
 */
import test from 'node:test'
import assert from 'node:assert/strict'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import {
  pollAnswer,
  isInterimAnswer,
  isFinalAnswer,
} from '../lib/mailbox.js'
import {
  sendToRole,
  submitForReview,
  createSubmitForReviewToolOptions,
  normalizeConfig,
  resolveMailboxRoots,
  ensureMailboxDirs,
} from '../lib/bridge.js'

async function scratchCfg(extra = {}) {
  const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), 'gate-ack-'))
  await fsp.mkdir(path.join(tmp, 'supervisor', 'thin-state'), { recursive: true })
  const cfg = {
    dshHome: tmp,
    mailboxRoot: path.join(tmp, 'mailbox'),
    timeoutSec: 2,
    gateTimeoutSec: 2,
    gateAckGraceMs: 30,
    pollMs: 50,
    ...extra,
  }
  const dirs = resolveMailboxRoots(cfg)
  await ensureMailboxDirs(dirs)
  return { tmp, cfg, dirs }
}

test('interim/final answer classification', () => {
  assert.equal(isInterimAnswer({ status: 'gate_running', ok: true }), true)
  assert.equal(isInterimAnswer({ progress: true, gate_ticket: 'g' }), true)
  assert.equal(isInterimAnswer({ progress: true, verdict: 'PASS' }), false)
  // bare ok:true alone is NOT final
  assert.equal(isFinalAnswer({ ok: true }), false)
  // ok:true + status gate_running is interim, not final
  assert.equal(isFinalAnswer({ ok: true, status: 'gate_running' }), false)
  assert.equal(isFinalAnswer({ verdict: 'PASS' }), true)
  assert.equal(isFinalAnswer({ status: 'done', verdict: 'HOLD' }), true)
  assert.equal(isFinalAnswer({ answer: 'hi' }), true)
  assert.equal(isFinalAnswer({ ok: false, error: 'no route' }), true)
})

test('pollAnswer ignores gate_running, extends deadline, accepts late final', async () => {
  const { cfg } = await scratchCfg()
  const dirs = resolveMailboxRoots(cfg)
  const answerPath = path.join(dirs.answers, 'rev-1.json')
  const started = Date.now()
  await fsp.writeFile(answerPath, JSON.stringify({
    ask_id: 'rev-1',
    status: 'gate_running',
    gate_ticket: 'gate-trial-x-rev1',
    gate_timeout_sec: 2,
    started_at: new Date().toISOString(),
    progress: true,
  }) + '\n')

  // Final lands at ~800ms; the initial 300ms deadline alone would have expired.
  const timer = setTimeout(() => {
    fsp.writeFile(answerPath, JSON.stringify({
      ask_id: 'rev-1',
      status: 'done',
      verdict: 'PASS',
      gate_ticket: 'gate-trial-x-rev1',
    }) + '\n')
  }, 800)

  const res = await pollAnswer(answerPath, {
    deadline: started + 300,
    pollMs: 50,
    started,
    gateBudgetSec: 2,
    ackGraceMs: 30,
  })
  clearTimeout(timer)
  assert.equal(res.ok, true)
  assert.equal(res.timed_out, false)
  assert.equal(res.body.verdict, 'PASS')
  assert.equal(res.gate_ack.gate_ticket, 'gate-trial-x-rev1')
  assert.ok(res.waited_ms >= 700, `expected to wait past the initial deadline, got ${res.waited_ms}`)
})

test('ok:true with status gate_running is not accepted as final', async () => {
  const { cfg } = await scratchCfg()
  const dirs = resolveMailboxRoots(cfg)
  const answerPath = path.join(dirs.answers, 'rev-2.json')
  const started = Date.now()
  await fsp.writeFile(answerPath, JSON.stringify({
    ok: true,
    status: 'gate_running',
    progress: true,
    gate_ticket: 'gate-trial-x-rev2',
  }) + '\n')
  const res = await pollAnswer(answerPath, {
    deadline: started + 150,
    pollMs: 50,
    started,
    gateBudgetSec: 0.05,
    ackGraceMs: 30,
    gateProbe: async () => 'running',
  })
  assert.equal(res.ok, false)
  assert.equal(res.timed_out, true)
  assert.equal(res.gate_status, 'running')
})

test('timeout after ack: running / exited / unknown / no_ack', async () => {
  const { cfg } = await scratchCfg()

  async function ackTimeout(gateProbe, { writeAck = true } = {}) {
    const tmpDirs = resolveMailboxRoots(cfg)
    const answerPath = path.join(tmpDirs.answers, `t-${Math.random().toString(36).slice(2)}.json`)
    if (writeAck) {
      await fsp.writeFile(answerPath, JSON.stringify({
        status: 'gate_running',
        gate_ticket: 'gate-trial-z-rev1',
        progress: true,
      }) + '\n')
    }
    const started = Date.now()
    return pollAnswer(answerPath, {
      deadline: started + 100,
      pollMs: 30,
      started,
      gateBudgetSec: 0.05,
      ackGraceMs: 30,
      gateProbe,
    })
  }

  const running = await ackTimeout(async () => 'running')
  assert.equal(running.gate_status, 'running')
  assert.equal(running.degrade, 'retry_wait')
  assert.match(running.error, /gate-trial-z-rev1/)
  assert.equal(running.gate_ticket, 'gate-trial-z-rev1')

  const exited = await ackTimeout(async () => 'exited')
  assert.equal(exited.gate_status, 'exited_no_result')
  assert.equal(exited.degrade, 'close_ticket')

  const unknown = await ackTimeout(async () => 'unknown')
  assert.equal(unknown.gate_status, 'unknown')

  const noAck = await ackTimeout(async () => 'exited', { writeAck: false })
  assert.equal(noAck.gate_status, 'no_ack')
  assert.match(noAck.error, /no gate_running ack/)
  assert.equal(noAck.gate_ack, null)
})

test('submit_for_review timeout arg cannot shrink below cfg.timeoutSec', async () => {
  const { cfg, dirs } = await scratchCfg({ timeoutSec: 600 })
  const r = await sendToRole(
    { to_role: 'gate', kind: 'submit_for_review', payload: {}, wait: false, timeout: 1, message_id: 'floor-1' },
    {},
    cfg,
  )
  assert.equal(r.ok, true)
  const msg = JSON.parse(await fsp.readFile(path.join(dirs.pending, 'floor-1.json'), 'utf8'))
  assert.equal(msg.timeout_sec, 600)

  // non-gate kinds keep the caller's shorter timeout
  await sendToRole(
    { to_role: 'supervisor', kind: 'ask_supervisor', payload: {}, wait: false, timeout: 1, message_id: 'floor-2' },
    {},
    cfg,
  )
  const msg2 = JSON.parse(await fsp.readFile(path.join(dirs.pending, 'floor-2.json'), 'utf8'))
  assert.equal(msg2.timeout_sec, 1)
})

test('submitForReview failure never claims gate absent; running → retry_wait', async () => {
  const { cfg, dirs } = await scratchCfg({ timeoutSec: 1, gateTimeoutSec: 1, gateAckGraceMs: 30 })
  const answerPath = path.join(dirs.answers, 'rev-run-1.json')
  await fsp.writeFile(answerPath, JSON.stringify({
    ask_id: 'rev-run-1',
    status: 'gate_running',
    gate_ticket: 'gate-trial-run-rev1',
    gate_timeout_sec: 1,
    progress: true,
  }) + '\n')

  const r = await submitForReview(
    { usage_prompt: 100, summary: 'wip', ask_id: 'rev-run-1' },
    {},
    { ...cfg, gateProbe: async () => 'running' },
  )
  assert.equal(r.ok, false)
  assert.equal(r.gate_status, 'running')
  assert.equal(r.gate_ticket, 'gate-trial-run-rev1')
  assert.equal(r.degrade, 'retry_wait')
  assert.doesNotMatch(r.hint, /缺席/)
  assert.doesNotMatch(r.hint, /status=blocked/)
  assert.match(r.hint, /do NOT set blocked/)
  // resume: an existing answer file must not be clobbered by a new pending
  assert.equal(await fsp.access(path.join(dirs.pending, 'rev-run-1.json')).then(() => true).catch(() => false), false)
})

test('submitForReview exited_no_result / no_ack hints are close_ticket', async () => {
  const { cfg, dirs } = await scratchCfg({ timeoutSec: 1, gateTimeoutSec: 1, gateAckGraceMs: 30 })

  await fsp.writeFile(path.join(dirs.answers, 'rev-exit-1.json'), JSON.stringify({
    status: 'gate_running', gate_ticket: 'gate-trial-exit-rev1', gate_timeout_sec: 1, progress: true,
  }) + '\n')
  const exited = await submitForReview(
    { usage_prompt: 100, summary: 'wip', ask_id: 'rev-exit-1' },
    {},
    { ...cfg, gateProbe: async () => 'exited' },
  )
  assert.equal(exited.gate_status, 'exited_no_result')
  assert.equal(exited.degrade, 'close_ticket')
  assert.doesNotMatch(exited.hint, /缺席/)
  assert.doesNotMatch(exited.hint, /status=blocked/)

  const noAck = await submitForReview(
    { usage_prompt: 100, summary: 'wip', ask_id: 'rev-noack-1' },
    {},
    cfg,
  )
  assert.equal(noAck.gate_status, 'no_ack')
  assert.equal(noAck.degrade, 'close_ticket')
  assert.doesNotMatch(noAck.hint, /缺席/)
  assert.doesNotMatch(noAck.hint, /status=blocked/)
})

test('resume: existing final answer for ask_id is returned without new pending', async () => {
  const { cfg, dirs } = await scratchCfg({ timeoutSec: 1 })
  const answerPath = path.join(dirs.answers, 'rev-resume-1.json')
  await fsp.writeFile(answerPath, JSON.stringify({
    ask_id: 'rev-resume-1',
    status: 'done',
    verdict: 'HOLD',
    rework_mode: 'fresh',
    gate_ticket: 'gate-trial-resume-rev1',
    instruction: 'HOLD fresh',
  }) + '\n')
  const r = await submitForReview(
    { usage_prompt: 100, summary: 'wip', ask_id: 'rev-resume-1' },
    {},
    cfg,
  )
  assert.equal(r.ok, true)
  assert.equal(r.verdict, 'HOLD')
  const pendingExists = await fsp.access(path.join(dirs.pending, 'rev-resume-1.json'))
    .then(() => true).catch(() => false)
  assert.equal(pendingExists, false)
})

test('createSubmitForReviewToolOptions covers the gate budget', () => {
  const def = createSubmitForReviewToolOptions({ timeoutSec: 600, gateTimeoutSec: 4320 })
  assert.equal(def.timeoutMs, (4320 + 120) * 1000)
  assert.ok(def.timeoutMs >= (4320 + 120) * 1000)
  // default gate budget = 600 + 3600 + 120
  const dflt = normalizeConfig({})
  assert.equal(dflt.gateTimeoutSec, 4320)
  // env override
  const envCfg = normalizeConfig({ env: { DSH_TRIAL_GATE_TIMEOUT_SEC: '900' } })
  assert.equal(envCfg.gateTimeoutSec, 900)
})
