import test from 'node:test'
import assert from 'node:assert/strict'

import {
  applyContextClear,
  normalizeClearConfig,
  placeholderTokens,
  isClearableResult,
} from '../lib/contextclear.mjs'
import { runSession, sumWindows } from '../lib/sim.mjs'
import { DEFAULT_PRICES } from '../lib/prices.mjs'

const result = (callId, tool, textChars, extra = {}) => ({
  kind: 'result',
  callId,
  tool,
  textChars,
  chars: textChars,
  plain: true,
  isError: false,
  tok: Math.round(textChars / 3),
  ...extra,
})

const asst = (args, extra = {}) => ({
  kind: 'asst',
  step: 0,
  reason: 5,
  text: 5,
  args: args.map(([tool, chars, id]) => ({ tool, chars, id, tok: Math.round(chars / 3) })),
  ...extra,
})

function postSummarySurface() {
  return [
    { kind: 'sys', tok: 96 },
    { kind: 'summary', tok: 1560 },
    asst([['write', 2000, 'c1']]),
    result('c1', 'write', 2000),
    result('b1', 'bash', 5000),
    result('b2', 'bash', 5000),
  ]
}

test('contextClear only rewrites the retained tail (after sys + summary)', () => {
  const surface = postSummarySurface()
  const cfg = normalizeClearConfig({ keep: 1 })
  const out = applyContextClear(surface, cfg)

  // Nothing before the summary changes.
  assert.equal(out.surface[0], surface[0])
  assert.equal(out.surface[0].tok, 96)
  assert.equal(out.surface[1], surface[1])
  assert.equal(out.surface[1].tok, 1560)
  assert.equal(out.changedFrom, 2)

  // T3 folded the write-only step (assistant + its result) into one user note.
  assert.equal(out.t3, 1)
  assert.equal(out.surface[2].kind, 'user')
  assert.equal(out.surface[2].collapsed, true)
  assert.equal(out.surface[2].tok, 80)

  // T2 cleared the older bash result but kept the newest one.
  assert.equal(out.t2, 1)
  assert.equal(out.cleared, 2)
  assert.equal(out.surface[3].masked, true)
  assert.equal(out.surface[3].tok, placeholderTokens(surface[4], cfg))
  assert.equal(out.surface[4].masked, undefined)
})

test('the newest `keep` results are protected from T2 and block T3', () => {
  // The write step's result is itself the newest result, so it is protected:
  // T3 must not fold that step, while the older bash result is still cleared.
  const surface = [
    { kind: 'sys', tok: 96 },
    { kind: 'summary', tok: 1560 },
    asst([['bash', 300, 'b0']]),
    result('b0', 'bash', 5000),
    asst([['write', 2000, 'c1']]),
    result('c1', 'write', 2000),
  ]
  const out = applyContextClear(surface, normalizeClearConfig({ keep: 1 }))
  assert.equal(out.t3, 0)
  assert.equal(out.surface.some((it) => it.collapsed), false)
  assert.equal(out.t2, 1)
  assert.equal(out.cleared, 1)
  assert.equal(out.surface[3].masked, true) // b0 cleared
  assert.equal(out.surface[5].masked, undefined) // c1 protected
})

test('mixed-tool steps are never collapsed', () => {
  const surface = [
    { kind: 'sys', tok: 96 },
    { kind: 'summary', tok: 1560 },
    asst([['bash', 10, 'x1'], ['write', 2000, 'c1']]),
    result('x1', 'bash', 200),
    result('c1', 'write', 2000),
    result('b1', 'bash', 5000),
    result('b2', 'bash', 5000),
  ]
  const out = applyContextClear(surface, normalizeClearConfig({ keep: 1 }))
  assert.equal(out.t3, 0)
  assert.equal(out.surface.some((it) => it.collapsed), false)
})

test('T3 requires an argument of at least minArgChars', () => {
  const surface = [
    { kind: 'sys', tok: 96 },
    { kind: 'summary', tok: 1560 },
    asst([['write', 500, 'c1']]),
    result('c1', 'write', 500),
    result('b1', 'bash', 5000),
  ]
  const out = applyContextClear(surface, normalizeClearConfig({ keep: 0 }))
  assert.equal(out.t3, 0)
})

test('failure-flavoured placeholders keep a longer tail', () => {
  const cfg = normalizeClearConfig()
  assert.equal(placeholderTokens({ fail: false }, cfg), 130)
  assert.equal(placeholderTokens({ fail: true }, cfg), 220)
  assert.equal(isClearableResult(result('r', 'bash', 1199), cfg), false)
  assert.equal(isClearableResult(result('r', 'bash', 1200), cfg), true)
  assert.equal(isClearableResult(result('r', 'bash', 5000, { isError: true }), cfg), false)
  assert.equal(isClearableResult(result('r', 'bash', 5000, { plain: false }), cfg), false)
})

// ---------------------------------------------------------------------------
// Synthetic session: with compaction disabled, contextClear must be a no-op.
// ---------------------------------------------------------------------------

function syntheticSession() {
  const step = (t, P, pre) => ({
    t: Date.parse(t),
    miss: P - 100,
    hit: 100,
    out: 100,
    reasoningTokens: 0,
    P,
    pre,
    asst: { rc: 0, tc: 0, args: [] },
  })
  return {
    file: '<synthetic>',
    home: 'test',
    sysChars: 416,
    toolChars: 0,
    preUserChars: 0,
    compactions: [],
    steps: [
      step('2026-10-08T01:10:00Z', 1000, []),
      step('2026-10-08T01:11:00Z', 2000, [result('c1', 'read', 400)]),
      step('2026-10-08T01:12:00Z', 3000, [result('c2', 'read', 800)]),
    ],
  }
}

test('BASE and contextClear are identical when no compaction fires', () => {
  const session = syntheticSession()
  const windowOf = () => 0
  const noClear = runSession(session, { T: 1e12, R: 20000, clear: null }, windowOf, 1, DEFAULT_PRICES)
  const withClear = runSession(
    session,
    { T: 1e12, R: 20000, clear: normalizeClearConfig({ keep: 0 }) },
    windowOf,
    1,
    DEFAULT_PRICES,
  )
  const base = sumWindows(noClear)
  const cleared = sumWindows(withClear)
  assert.equal(base.comp, 0)
  assert.equal(base.cleared, 0)
  assert.deepEqual(cleared, base)
})
