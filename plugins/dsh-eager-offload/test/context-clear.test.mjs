/**
 * contextClear (T2) tests.
 *
 * The session/token-meter packages are resolved from the box's installed dsh
 * tree at runtime (the plugin itself is symlinked without a node_modules).
 * When they are unavailable the session-backed tests skip with a reason so the
 * suite stays CI-safe.
 */

import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { test } from 'node:test'

import {
  CONTEXT_CLEAR_MARK,
  buildContextClearPlaceholder,
  clearAfterCompaction,
  isFailedCommandText,
  liveCompactionCheckpoint,
} from '../lib/context-clear.js'
import { apply } from '../lib/index.js'
import { normalizeConfig } from '../lib/offload.js'

/* ------------------------------------------------------------------ *
 * Real dsh runtime resolution (skippable)                            *
 * ------------------------------------------------------------------ */

const DSH_PACKAGE_JSON = '/home/box/.local/lib/node_modules/@deepseek-ai/dsh/package.json'

async function loadRealDeps() {
  try {
    const require = createRequire(DSH_PACKAGE_JSON)
    const [sessionMod, meterMod] = await Promise.all([
      import(pathToFileURL(require.resolve('@deepseek-ai/dsh-session')).href),
      import(pathToFileURL(require.resolve('@deepseek-ai/dsh-token-meter')).href),
    ])
    return { Session: sessionMod.Session, TokenMeter: meterMod.TokenMeter }
  } catch {
    return undefined
  }
}

const deps = await loadRealDeps()
const sessionSkip = deps === undefined ? 'dsh-session / dsh-token-meter not resolvable in this environment' : false

/** Minimal fake Cordis context accepted by the TokenMeter Service base class. */
function fakeMeterContext() {
  return {
    reflect: { provide() {} },
    sessionProjections: { register() {} },
    on() {},
    get() {
      return undefined
    },
  }
}

/* ------------------------------------------------------------------ *
 * Realistic session fixtures                                         *
 * ------------------------------------------------------------------ */

function systemMessage(id, text) {
  return { id, role: 'system', content: [{ type: 'text', text }], source: { kind: 'plugin', plugin: 'test' } }
}

function userMessage(id, text) {
  return { id, role: 'user', content: [{ type: 'text', text }], source: { kind: 'user' } }
}

function assistantMessage(id, callId, name) {
  return {
    id,
    role: 'assistant',
    content: [{ type: 'tool-call', id: callId, name, arguments: '{}' }],
    source: { kind: 'model', provider: 'test-provider', model: 'test-model' },
  }
}

function toolResultMessage(callId, text, isError = false) {
  return {
    id: `result-${callId}`,
    role: 'user',
    content: [{ type: 'tool-result', toolCallId: callId, content: [{ type: 'text', text }], isError }],
    source: { kind: 'tool', callId },
  }
}

/**
 * Build a multi-step session: system head, one user message, then `steps` each
 * with step/start → assistant tool-call → tool/call → tool/result → step/end.
 * @returns {{ session: any, userSeq: number, resultSeqs: number[] }}
 */
function buildSession(Session, steps) {
  const session = Session.create('sess-1')
  session.append('system/message', { message: systemMessage('sys', 'SYSTEM HEAD') }, { surfaceOp: 'append' })
  const userSeq = session.append('user/message', userMessage('u1', 'please run the tools'), { surfaceOp: 'append' }).seq
  const resultSeqs = []
  for (const spec of steps) {
    const { step } = spec
    session.append('step/start', { turn: 1, step })
    session.append(
      'assistant/message',
      {
        turn: 1,
        step,
        stream: [],
        message: assistantMessage(`assistant-${step}`, spec.callId, spec.tool),
      },
      { surfaceOp: 'append' },
    )
    const callSeq = session.append('tool/call', {
      turn: 1,
      step,
      callId: spec.callId,
      name: spec.tool,
      arguments: '{}',
    }).seq
    const resultSeq = session.append(
      'tool/result',
      { turn: 1, step, message: toolResultMessage(spec.callId, spec.text, spec.isError === true) },
      { surfaceOp: 'append', sourceEventSeqs: [callSeq] },
    ).seq
    resultSeqs.push(resultSeq)
    session.append('step/end', { turn: 1, step })
  }
  return { session, userSeq, resultSeqs }
}

/**
 * Simulate `commitCompactionBody`: append `compaction/summary` then a
 * checkpoint `user/message` replacing the whole span `[startSeq, endSeq]`.
 */
function commitCompaction(session, startSeq, endSeq) {
  const shadowedSeqs = session.surface.nodes.filter((seq) => seq >= startSeq && seq <= endSeq)
  const summary = session.append('compaction/summary', {
    compactionId: 'compaction-1',
    summary: 'compacted summary text',
    shadowedRange: { start: startSeq, end: endSeq },
    shadowedSeqs,
    shadowedTokenCount: 123,
    provider: 'test-provider',
    model: 'test-model',
  })
  const checkpoint = {
    id: 'checkpoint-1',
    role: 'user',
    content: [{ type: 'text', text: '[compacted summary]' }],
    source: { kind: 'plugin', plugin: 'compact', compactionId: 'compaction-1' },
  }
  session.append('user/message', checkpoint, {
    surfaceOp: { op: 'replace', startSeq, endSeq },
    sourceEventSeqs: [...new Set([...shadowedSeqs, summary.seq])],
  })
}

function largeText(step) {
  return `result-${step}:` + `${step}`.repeat(5000)
}

/**
 * Canonical fixture: 8 steps, compaction of the user + steps 1-2, so the
 * retained tail holds results 3-8. keepRecentResults=2 keeps 7-8; result 3 is
 * small, result 4 is an error, results 5-6 are large and eligible.
 */
async function buildFixture() {
  const { Session, TokenMeter } = deps
  const steps = [
    { step: 1, callId: 'c1', tool: 'bash', text: largeText(1) },
    { step: 2, callId: 'c2', tool: 'read', text: largeText(2) },
    { step: 3, callId: 'c3', tool: 'bash', text: 'tiny' },
    { step: 4, callId: 'c4', tool: 'bash', text: largeText(4), isError: true },
    { step: 5, callId: 'c5', tool: 'bash', text: largeText(5) },
    { step: 6, callId: 'c6', tool: 'read', text: largeText(6) },
    { step: 7, callId: 'c7', tool: 'bash', text: largeText(7) },
    { step: 8, callId: 'c8', tool: 'bash', text: largeText(8) },
  ]
  const { session, userSeq, resultSeqs } = buildSession(Session, steps)
  commitCompaction(session, userSeq, resultSeqs[1])

  const offloadRoot = await mkdtemp(join(tmpdir(), 'ctxclear-'))
  const cfg = normalizeConfig(
    {
      offloadRoot,
      contextClear: { enabled: true, keepRecentResults: 2, minResultBytes: 1200 },
    },
    { dshHome: offloadRoot },
  )
  const tokenMeter = new TokenMeter(fakeMeterContext())
  return {
    session,
    cfg,
    tokenMeter,
    offloadRoot,
    sessionId: 'sess-1',
    texts: Object.fromEntries(steps.map((s) => [s.callId, s.text])),
  }
}

/** Extract the re-readable path cited by a context-clear placeholder notice. */
function pathFromPlaceholder(text) {
  const match = /full text at (\S+)/.exec(text)
  return match ? match[1] : undefined
}

/** Collect derived tool-result text keyed by callId. */
function derivedResults(messages) {
  const byCallId = new Map()
  for (const message of messages) {
    for (const block of message.content ?? []) {
      if (block.type !== 'tool-result') continue
      const text = block.content.map((inner) => inner.text).join('')
      byCallId.set(block.toolCallId, { text, isError: block.isError, message })
    }
  }
  return byCallId
}

/** Every tool-result must still follow the assistant tool-call that produced it. */
function assertToolPairing(messages) {
  const calls = new Set()
  for (const message of messages) {
    for (const block of message.content ?? []) {
      if (block.type === 'tool-call') calls.add(block.id)
      if (block.type === 'tool-result') {
        assert.ok(calls.has(block.toolCallId), `tool-result ${block.toolCallId} has no preceding tool-call`)
      }
    }
  }
}

const FAKE_METER = { estimateMessage: () => 7 }

/* ------------------------------------------------------------------ *
 * Pure helpers (no runtime dependency)                               *
 * ------------------------------------------------------------------ */

test('context-clear failure heuristic reads the shell exit marker', () => {
  assert.equal(isFailedCommandText('ok\n[exit code: 0]'), false)
  assert.equal(isFailedCommandText('boom\n[exit code: 1]'), true)
  assert.equal(isFailedCommandText('boom\n[exit code: 127]'), true)
  assert.equal(isFailedCommandText('boom\n[exit code: 1]\n'), true)
  assert.equal(isFailedCommandText('mentions [exit code: 1] mid-text'), false)
  assert.equal(isFailedCommandText(undefined), false)
})

test('buildContextClearPlaceholder is deterministic and keeps a bigger failed tail', () => {
  const success = buildContextClearPlaceholder('A'.repeat(4000), {
    toolName: 'bash',
    bytes: 4000,
    path: '/tmp/x.txt',
    headBytes: 100,
    tailBytes: 100,
    failTailBytes: 900,
  })
  const failed = buildContextClearPlaceholder('A'.repeat(4000) + '\n[exit code: 1]', {
    toolName: 'bash',
    bytes: 4017,
    path: '/tmp/x.txt',
    headBytes: 100,
    tailBytes: 100,
    failTailBytes: 900,
  })
  assert.ok(success.startsWith(CONTEXT_CLEAR_MARK))
  assert.ok(success.includes('full text at /tmp/x.txt'))
  assert.ok(failed.includes('[exit code: 1]'))
  assert.ok(failed.length > success.length)
  const again = buildContextClearPlaceholder('A'.repeat(4000), {
    toolName: 'bash',
    bytes: 4000,
    path: '/tmp/x.txt',
    headBytes: 100,
    tailBytes: 100,
    failTailBytes: 900,
  })
  assert.equal(success, again)
})

/* ------------------------------------------------------------------ *
 * Trigger guard / no-op paths                                        *
 * ------------------------------------------------------------------ */

test('no compaction → no trigger and zero appends', { skip: sessionSkip }, async () => {
  const { Session, TokenMeter } = deps
  const { session } = buildSession(Session, [{ step: 1, callId: 'c1', tool: 'bash', text: largeText(1) }])
  const cfg = normalizeConfig(
    { contextClear: { enabled: true, keepRecentResults: 0, minResultBytes: 10 } },
    { dshHome: '/tmp/x' },
  )
  const seqBefore = session.seq
  const result = await clearAfterCompaction(session, {
    cfg,
    tokenMeter: new TokenMeter(fakeMeterContext()),
    sessionId: 'sess-1',
  })
  assert.equal(result.triggered, false)
  assert.equal(result.cleared, 0)
  assert.equal(session.seq, seqBefore)
})

test('step/start after the summary disables the trigger (restart-safe)', { skip: sessionSkip }, async () => {
  const fixture = await buildFixture()
  const { session } = fixture
  session.append('step/start', { turn: 1, step: 9 })
  const seqBefore = session.seq
  const result = await clearAfterCompaction(session, {
    cfg: fixture.cfg,
    tokenMeter: fixture.tokenMeter,
    sessionId: fixture.sessionId,
  })
  assert.equal(result.triggered, false)
  assert.equal(result.cleared, 0)
  assert.equal(session.seq, seqBefore)
  await rm(fixture.offloadRoot, { recursive: true, force: true })
})

/* ------------------------------------------------------------------ *
 * Full clear behaviour                                               *
 * ------------------------------------------------------------------ */

test('clears only old big post-checkpoint results; prefix and pairing intact', { skip: sessionSkip }, async () => {
  const fixture = await buildFixture()
  const { session, cfg, tokenMeter, sessionId } = fixture
  try {
    const before = session.deriveMessages()
    const checkpointIdx = session.surface.nodes.indexOf(liveCompactionCheckpoint(session).checkpointSeq)
    const prefixBefore = JSON.stringify(session.surface.nodes.slice(0, checkpointIdx + 1))

    const result = await clearAfterCompaction(session, { cfg, tokenMeter, sessionId })
    assert.equal(result.triggered, true)
    assert.equal(result.cleared, 2)
    assert.ok(result.bytesBefore > result.bytesAfter)
    assert.deepEqual(result.skipped, { tooSmall: 1, isError: 1, keepRecent: 2 })

    const after = session.deriveMessages()
    // b. system + checkpoint projection is byte-identical.
    const checkpointAfterIdx = session.surface.nodes.indexOf(liveCompactionCheckpoint(session).checkpointSeq)
    assert.equal(JSON.stringify(session.surface.nodes.slice(0, checkpointAfterIdx + 1)), prefixBefore)
    assert.equal(JSON.stringify(after.slice(0, 2)), JSON.stringify(before.slice(0, 2)))

    const byCall = derivedResults(after)
    assert.equal(byCall.get('c3').text, 'tiny') // small untouched
    assert.equal(byCall.get('c4').text, fixture.texts.c4) // error untouched
    assert.equal(byCall.get('c7').text, fixture.texts.c7) // newest K untouched
    assert.equal(byCall.get('c8').text, fixture.texts.c8) // newest K untouched
    for (const callId of ['c5', 'c6']) {
      const cleared = byCall.get(callId).text
      assert.ok(cleared.startsWith(CONTEXT_CLEAR_MARK), `${callId} must carry the marker`)
      assert.ok(cleared.includes('full text at '), `${callId} must cite a re-readable path`)
      assert.ok(cleared.length < fixture.texts[callId].length)
      assert.ok(cleared.includes(fixture.texts[callId].slice(0, 64)))
    }
    assertToolPairing(after)
    assert.ok(!byCall.get('c5').isError)
    assert.equal(byCall.get('c5').message.source.callId, 'c5')
  } finally {
    await rm(fixture.offloadRoot, { recursive: true, force: true })
  }
})

test('token meter measures a lower total after clearing', { skip: sessionSkip }, async () => {
  const fixture = await buildFixture()
  const { session, cfg, tokenMeter, sessionId } = fixture
  try {
    const before = tokenMeter.measure(session).totalTokens
    assert.ok(Number.isFinite(before))
    await clearAfterCompaction(session, { cfg, tokenMeter, sessionId })
    const after = tokenMeter.measure(session).totalTokens
    assert.ok(Number.isFinite(after))
    assert.ok(after < before, `expected ${after} < ${before}`)
  } finally {
    await rm(fixture.offloadRoot, { recursive: true, force: true })
  }
})

test('saved offload file hashes equal the original cleared text', { skip: sessionSkip }, async () => {
  const fixture = await buildFixture()
  const { session, cfg, tokenMeter, sessionId } = fixture
  try {
    await clearAfterCompaction(session, { cfg, tokenMeter, sessionId })
    const byCall = derivedResults(session.deriveMessages())
    const cleared = byCall.get('c5').text
    const onDiskPath = pathFromPlaceholder(cleared)
    assert.ok(onDiskPath, 'placeholder must cite an offload path')
    const onDisk = await readFile(onDiskPath, 'utf8')
    const sha = (v) => createHash('sha256').update(v, 'utf8').digest('hex')
    assert.equal(sha(onDisk), sha(fixture.texts.c5))
  } finally {
    await rm(fixture.offloadRoot, { recursive: true, force: true })
  }
})

test('an existing offload path is reused, not re-saved', { skip: sessionSkip }, async () => {
  const { Session, TokenMeter } = deps
  const offloadRoot = await mkdtemp(join(tmpdir(), 'ctxclear-reuse-'))
  try {
    const existingPath = join(offloadRoot, 'already-there.txt')
    await writeFile(existingPath, 'irrelevant', 'utf8')
    const text =
      largeText(5) +
      `\n\n(dsh-eager-offload: tool=bash callId=c5 bytes=5000 path=${existingPath} — full output saved; use read.)`
    const { session, userSeq, resultSeqs } = buildSession(Session, [
      { step: 1, callId: 'c1', tool: 'bash', text: largeText(1) },
      { step: 2, callId: 'c5', tool: 'bash', text },
    ])
    commitCompaction(session, userSeq, resultSeqs[0])
    const cfg = normalizeConfig(
      { offloadRoot, contextClear: { enabled: true, keepRecentResults: 0, minResultBytes: 100 } },
      { dshHome: offloadRoot },
    )
    const result = await clearAfterCompaction(session, {
      cfg,
      tokenMeter: new TokenMeter(fakeMeterContext()),
      sessionId: 'sess-1',
    })
    assert.equal(result.cleared, 1)
    const cleared = derivedResults(session.deriveMessages()).get('c5').text
    assert.equal(pathFromPlaceholder(cleared), existingPath)
  } finally {
    await rm(offloadRoot, { recursive: true, force: true })
  }
})

test('running twice is idempotent', { skip: sessionSkip }, async () => {
  const fixture = await buildFixture()
  const { session, cfg, tokenMeter, sessionId } = fixture
  try {
    const first = await clearAfterCompaction(session, { cfg, tokenMeter, sessionId })
    assert.equal(first.cleared, 2)
    const seqAfterFirst = session.seq
    const derivedAfterFirst = JSON.stringify(session.deriveMessages())
    const second = await clearAfterCompaction(session, { cfg, tokenMeter, sessionId })
    assert.equal(second.cleared, 0)
    assert.equal(session.seq, seqAfterFirst)
    assert.equal(JSON.stringify(session.deriveMessages()), derivedAfterFirst)
    assert.equal(second.skipped.alreadyCleared, 2)
  } finally {
    await rm(fixture.offloadRoot, { recursive: true, force: true })
  }
})

test('a fresh session replayed from the log derives identical messages', { skip: sessionSkip }, async () => {
  const { Session } = deps
  const fixture = await buildFixture()
  const { session, cfg, tokenMeter, sessionId } = fixture
  try {
    await clearAfterCompaction(session, { cfg, tokenMeter, sessionId })
    const expected = JSON.stringify(session.deriveMessages())
    const replay = Session.create('replay-1', session.snapshotEvents())
    assert.equal(JSON.stringify(replay.deriveMessages()), expected)
  } finally {
    await rm(fixture.offloadRoot, { recursive: true, force: true })
  }
})

/* ------------------------------------------------------------------ *
 * Plugin wiring                                                      *
 * ------------------------------------------------------------------ */

test('apply registers agent/pre-step only when contextClear is enabled', async () => {
  const off = []
  apply(
    { on: (event, handler, opts) => off.push({ event, handler, opts }), logger: { warn() {}, info() {} } },
    { contextClear: { enabled: false } },
  )
  assert.deepEqual(off.map((entry) => entry.event), ['tools/post-execute'])

  const on = []
  apply(
    {
      on: (event, handler, opts) => on.push({ event, handler, opts }),
      get: () => FAKE_METER,
      logger: { warn() {}, info() {} },
    },
    { contextClear: { enabled: true, mode: 'compaction-coupled' } },
  )
  const pre = on.filter((entry) => entry.event === 'agent/pre-step')
  assert.equal(pre.length, 1)
  assert.deepEqual(pre[0].opts, { prepend: true })

  const decision = { kind: 'enter', messages: [] }
  const session = { seq: 0, eventAt: () => undefined, surface: { nodes: [] }, header: { id: 'sess-1' } }
  const returned = await pre[0].handler({ agent: { session } }, async () => decision)
  assert.equal(returned, decision)
})

test('mounted listener clears on enter and passes the decision through', { skip: sessionSkip }, async () => {
  const fixture = await buildFixture()
  const infos = []
  const handlers = []
  apply(
    {
      on: (event, handler) => handlers.push({ event, handler }),
      get: () => fixture.tokenMeter,
      logger: { warn() {}, info: (m) => infos.push(m) },
    },
    {
      offloadRoot: fixture.offloadRoot,
      contextClear: { enabled: true, keepRecentResults: 2, minResultBytes: 1200 },
    },
  )
  const pre = handlers.find((entry) => entry.event === 'agent/pre-step')
  assert.ok(pre)
  const decision = { kind: 'enter', messages: [] }
  try {
    const returned = await pre.handler({ agent: { session: fixture.session } }, async () => decision)
    assert.equal(returned, decision)
    assert.ok(
      infos.some((m) => /^context-clear: cleared=2 bytes \d+->\d+$/.test(m)),
      `expected a context-clear info log, got ${JSON.stringify(infos)}`,
    )
  } finally {
    await rm(fixture.offloadRoot, { recursive: true, force: true })
  }
})

test('missing tokenMeter warns once and appends nothing', async () => {
  const warns = []
  const handlers = []
  apply(
    {
      on: (event, handler) => handlers.push({ event, handler }),
      get: () => undefined,
      logger: { warn: (m) => warns.push(m), info() {} },
    },
    { contextClear: { enabled: true } },
  )
  const pre = handlers.find((entry) => entry.event === 'agent/pre-step')
  assert.ok(pre, 'pre-step listener must be registered')

  let appended = 0
  const session = {
    seq: 0,
    eventAt: () => undefined,
    surface: { nodes: [] },
    header: { id: 'sess-1' },
    append: () => {
      appended += 1
      return { seq: appended }
    },
  }
  const next = async () => ({ kind: 'enter', messages: [] })
  await pre.handler({ agent: { session } }, next)
  await pre.handler({ agent: { session } }, next)

  assert.equal(warns.filter((m) => /tokenMeter service is unavailable/.test(m)).length, 1)
  assert.equal(appended, 0)
})
