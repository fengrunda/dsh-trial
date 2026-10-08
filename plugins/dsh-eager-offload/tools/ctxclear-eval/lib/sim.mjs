// Offline replay simulator: contextClear x threshold/retain compaction x
// DeepSeek prefix cache. Ported from /tmp/tokaudit/agemask-sim/sim.js with
// per-request peak/off-peak pricing and the plugin-faithful T2/T3 clear.

import { build, sumTokens, tokOf } from './model.mjs'
import { PrefixCache, floorTo128 } from './cache.mjs'
import { requestCost } from './prices.mjs'
import { applyContextClear, normalizeClearConfig } from './contextclear.mjs'

/** Built-in tool-result pruner: >4096 chars -> head 2048 + tail 512 chars. */
export const PRUNER_CHARS = 4096

/** Measured fixed miss overhead of a compaction summary call (tokens). */
export const COMPACTION_MISS_OVERHEAD = 553

/** Summary output ratio / clamp observed on real compactions: 7035/68766. */
export const SUMMARY_RATIO = 0.102
export const SUMMARY_MIN = 1500
export const SUMMARY_MAX = 8192

/** Empty per-window accumulator. */
export function newAccumulator() {
  return { P: 0, hit: 0, miss: 0, out: 0, req: 0, cost: 0, comp: 0, cleared: 0, t2: 0, t3: 0 }
}

function addTo(acc, win, key, value) {
  acc.all[key] += value
  if (win >= 0 && win < acc.w.length) acc.w[win][key] += value
}

/**
 * Replay a whole session.
 *
 * @param {any} session extracted session
 * @param {{T?:number,R?:number,clear?:object|null}} cfg
 * @param {(t:number)=>number} windowOf request timestamp -> window index (or -1)
 * @param {number} windows number of windows
 * @param {object} prices price table
 */
export function runSession(session, cfg, windowOf, windows, prices) {
  const { items, reqs } = build(session)
  const acc = {
    w: Array.from({ length: windows }, () => newAccumulator()),
    all: newAccumulator(),
  }
  const cache = new PrefixCache()
  let surface = []
  let ptr = 0

  const compact = (win, t) => {
    if (!cfg.T) return
    let measured = sumTokens(surface)
    if (measured < cfg.T) return
    // Built-in tool-result pruner runs before any summarisation.
    for (let i = 0; i < surface.length; i++) {
      const item = surface[i]
      if (item.kind === 'result' && item.textChars > PRUNER_CHARS && !item.pruned && !item.masked) {
        const next = Math.max(1, Math.round((item.tok * 2600) / item.textChars))
        if (next < item.tok) {
          item.tok = next
          item.pruned = true
          cache.invalidateFrom(i)
        }
      }
    }
    measured = sumTokens(surface)
    if (measured < cfg.T) return

    for (let attempt = 0; attempt < 3 && measured >= cfg.T; attempt++) {
      // Retained tail: minimal suffix starting at an assistant item with >= R tokens.
      let tail = surface.length
      let tailTokens = 0
      for (let i = surface.length - 1; i >= 2; i--) {
        tailTokens += tokOf(surface[i])
        if (tailTokens >= cfg.R && surface[i].kind === 'asst') {
          tail = i
          break
        }
      }
      if (tail <= 2) return
      const regionTokens = sumTokens(surface, 1, tail)
      const prefixTokens = sumTokens(surface, 0, tail)
      const baseline = tokOf(surface[0])
      const cHit = floorTo128(Math.min(cache.best(tail, baseline), prefixTokens))
      const cMiss = prefixTokens - cHit + COMPACTION_MISS_OVERHEAD
      const summaryOut = Math.min(
        SUMMARY_MAX,
        Math.max(SUMMARY_MIN, Math.round(regionTokens * SUMMARY_RATIO)),
      )
      addTo(acc, win, 'hit', cHit)
      addTo(acc, win, 'miss', cMiss)
      addTo(acc, win, 'P', cHit + cMiss)
      addTo(acc, win, 'out', summaryOut)
      addTo(acc, win, 'comp', 1)
      addTo(acc, win, 'cost', requestCost({ hit: cHit, miss: cMiss, out: summaryOut }, t, prices))

      surface = [surface[0], { kind: 'summary', tok: summaryOut + 60 }, ...surface.slice(tail)]
      cache.reset([{ idx: 1, tok: tokOf(surface[0]) }])

      // contextClear runs in this same pre-step, only over the retained tail.
      if (cfg.clear) {
        const cleared = applyContextClear(surface, cfg.clear)
        surface = cleared.surface
        addTo(acc, win, 'cleared', cleared.cleared)
        addTo(acc, win, 't2', cleared.t2)
        addTo(acc, win, 't3', cleared.t3)
        if (cleared.changedFrom >= 0) cache.invalidateFrom(cleared.changedFrom)
      }
      measured = sumTokens(surface)
    }
  }

  for (let r = 0; r < reqs.length; r++) {
    const req = reqs[r]
    while (ptr < req.nItems) {
      surface.push(items[ptr])
      ptr++
    }
    const win = windowOf(req.t)
    if (r === 0) cache.reset([{ idx: 1, tok: Math.min(req.actual.hit, tokOf(surface[0])) }])

    compact(win, req.t)

    const P = sumTokens(surface)
    const baseline = r > 0 ? tokOf(surface[0]) : 0
    const hit = r === 0 ? Math.min(req.actual.hit, P) : cache.hit(surface.length, P, baseline)
    addTo(acc, win, 'P', P)
    addTo(acc, win, 'hit', hit)
    addTo(acc, win, 'miss', P - hit)
    addTo(acc, win, 'out', req.actual.out)
    addTo(acc, win, 'req', 1)
    addTo(acc, win, 'cost', requestCost({ hit, miss: P - hit, out: req.actual.out }, req.t, prices))

    cache.add(surface.length, P)
    const next = items[ptr]
    if (next && next.kind === 'asst') cache.add(surface.length + 1, P + tokOf(next))
  }
  return acc
}

/** Sum the per-window accumulators (ignores requests outside every window). */
export function sumWindows(acc) {
  const total = newAccumulator()
  for (const w of acc.w) for (const key of Object.keys(total)) total[key] += w[key]
  return total
}

/** Replay every session and sum the requested windows. */
export function runAll(sessions, cfg, windowOf, windows, prices) {
  const total = {
    w: Array.from({ length: windows }, () => newAccumulator()),
    all: newAccumulator(),
  }
  for (const session of sessions) {
    const acc = runSession(session, cfg, windowOf, windows, prices)
    for (let i = 0; i < windows; i++) {
      for (const key of Object.keys(total.w[i])) total.w[i][key] += acc.w[i][key]
    }
    for (const key of Object.keys(total.all)) total.all[key] += acc.all[key]
  }
  return total
}

/** Aggregate real logged usage per window (no simulation). */
export function actualTotals(sessions, windowOf, windows, prices) {
  const acc = {
    w: Array.from({ length: windows }, () => newAccumulator()),
    all: newAccumulator(),
  }
  for (const session of sessions) {
    for (const step of session.steps) {
      const win = windowOf(step.t)
      const usage = { P: step.P, hit: step.hit, miss: step.miss, out: step.out }
      addTo(acc, win, 'P', usage.P)
      addTo(acc, win, 'hit', usage.hit)
      addTo(acc, win, 'miss', usage.miss)
      addTo(acc, win, 'out', usage.out)
      addTo(acc, win, 'req', 1)
      addTo(acc, win, 'cost', requestCost(usage, step.t, prices))
    }
  }
  return acc
}

export { normalizeClearConfig }
