// contextClear model (T2 + T3), matching the dsh-eager-offload plugin semantics.
//
// contextClear is compaction-coupled: it only runs in the pre-step where a
// compaction just produced a summary, and only over the summary's retained tail
// (surface index >= 2: [sys, summary, ...tail]). Nothing before the summary is
// ever touched.
//
//   T2 — replace plain-text, non-error tool results larger than
//        `minResultBytes`, except the newest `keep` of them, with a compact
//        placeholder (head + tail + a path line; failure-flavoured results keep
//        a longer tail).
//   T3 — collapse a whole step whose tool calls are *all* write/edit and where
//        at least one argument is >= `minArgChars`, when all of that step's
//        results sit before the newest `keep` results, into one ~80-token user
//        note. Mixed-tool steps (e.g. bash+write) are never collapsed.
//        Optional `clearReasoning` (default off) additionally collapses a step
//        of any tools whose assistant thinking text is >= `minReasoningChars`
//        characters (chars are read from the real reasoning blocks).

import { tokOf } from './model.mjs'

/** Tools that qualify a step for T3 collapse (plugin default). */
export const DEFAULT_COLLAPSE_TOOLS = Object.freeze(['write', 'edit'])

/** Default contextClear knobs, mirroring `DEFAULT_CONTEXT_CLEAR` in the plugin. */
export const DEFAULT_CONTEXT_CLEAR_CFG = Object.freeze({
  keep: 8,
  minResultBytes: 1200,
  collapse: true,
  collapseTools: DEFAULT_COLLAPSE_TOOLS,
  collapseMinArgChars: 1200,
  collapseTok: 80,
  clearReasoning: false,
  minReasoningChars: 600,
  placeholder: Object.freeze({
    headBytes: 160,
    tailBytes: 240,
    failTailBytes: 600,
    pathLineBytes: 120,
    charsPerToken: 4,
  }),
})

/** Merge a partial config over the defaults. */
export function normalizeClearConfig(raw = {}) {
  const placeholder = { ...DEFAULT_CONTEXT_CLEAR_CFG.placeholder, ...(raw.placeholder ?? {}) }
  return { ...DEFAULT_CONTEXT_CLEAR_CFG, ...raw, placeholder }
}

/** Estimated placeholder token count for a cleared result. */
export function placeholderTokens(item, cfg = DEFAULT_CONTEXT_CLEAR_CFG) {
  const ph = cfg.placeholder
  const tail = item.fail ? ph.failTailBytes : ph.tailBytes
  const bytes = ph.headBytes + tail + ph.pathLineBytes
  return Math.max(1, Math.round(bytes / ph.charsPerToken))
}

/** Whether a tool result is eligible for T2 clearing. */
export function isClearableResult(item, cfg = DEFAULT_CONTEXT_CLEAR_CFG) {
  if (item.kind !== 'result' || item.masked) return false
  if (item.isError) return false
  if (item.plain === false) return false
  const bytes = item.textChars ?? item.chars ?? 0
  return bytes >= cfg.minResultBytes
}

/**
 * Apply T2 + T3 to a post-summary surface, in place where possible, and return
 * a rebuilt surface plus counters.
 *
 * @param {any[]} surface `[sys, summary, ...tail]` (or a bare tail)
 * @param {object} [cfg]
 * @returns {{surface:any[], t2:number, t3:number, t3write:number, t3reason:number, cleared:number, changedFrom:number}}
 */
export function applyContextClear(surface, cfg = DEFAULT_CONTEXT_CLEAR_CFG) {
  const tailStart =
    surface[0]?.kind === 'sys' && surface[1]?.kind === 'summary' ? 2 : 0
  const result = {
    surface,
    t2: 0,
    t3: 0,
    t3write: 0,
    t3reason: 0,
    cleared: 0,
    changedFrom: Number.POSITIVE_INFINITY,
  }
  const origIndex = new Map()
  surface.forEach((item, i) => origIndex.set(item, i))

  // ------------------------------------------------------------------ T3
  const writeTools = new Set(cfg.collapseTools ?? DEFAULT_COLLAPSE_TOOLS)
  const collapseWrite = cfg.collapse !== false
  const clearReasoning = cfg.clearReasoning === true
  const minReasoningChars = cfg.minReasoningChars ?? DEFAULT_CONTEXT_CLEAR_CFG.minReasoningChars
  const collapses = []
  if (collapseWrite || clearReasoning) {
    const allResults = []
    for (let i = tailStart; i < surface.length; i++) if (surface[i].kind === 'result') allResults.push(i)
    // Plugin parity: the newest `keep` results are protected (all of them when
    // the tail holds fewer than `keep`).
    const cutIdx =
      cfg.keep > 0 && allResults.length > 0
        ? allResults[Math.max(0, allResults.length - cfg.keep)]
        : Number.POSITIVE_INFINITY
    for (let i = tailStart; i < surface.length; i++) {
      const item = surface[i]
      if (item.kind !== 'asst') continue
      const calls = item.args ?? []
      if (!calls.length) continue
      // Plugin parity: the assistant must be immediately followed by exactly
      // its results, one per call, in call order, nothing interleaved.
      let end = i
      let contiguous = true
      for (let ci = 0; ci < calls.length; ci++) {
        const next = surface[i + 1 + ci]
        if (next?.kind !== 'result' || next.callId !== calls[ci].id) {
          contiguous = false
          break
        }
        end = i + 1 + ci
      }
      if (!contiguous) continue
      const stepResults = surface.slice(i + 1, end + 1)
      const start = i
      i = end
      if (stepResults.some((it) => it.isError || it.plain === false)) continue
      if (stepResults.some((_it, ci) => start + 1 + ci >= cutIdx)) continue
      const byWrite =
        collapseWrite &&
        calls.every((a) => writeTools.has(a.tool)) &&
        calls.some((a) => a.chars >= cfg.collapseMinArgChars)
      const byReasoning = !byWrite && clearReasoning && (item.reasonChars ?? 0) >= minReasoningChars
      const reason = byWrite ? 'write' : byReasoning ? 'reasoning' : null
      if (reason === null) continue
      collapses.push({ start, end, step: item.step, reason })
    }
  }

  let rebuilt = surface
  if (collapses.length) {
    const drop = new Set()
    for (const c of collapses) {
      for (let i = c.start; i <= c.end; i++) drop.add(i)
      result.changedFrom = Math.min(result.changedFrom, c.start)
    }
    rebuilt = []
    for (let i = 0; i < surface.length; i++) {
      const collapse = collapses.find((c) => c.start === i)
      if (collapse) {
        rebuilt.push({
          kind: 'user',
          tok: cfg.collapseTok,
          collapsed: true,
          step: collapse.step,
          reason: collapse.reason,
        })
        result.t3 += 1
        if (collapse.reason === 'write') result.t3write += 1
        else result.t3reason += 1
        continue
      }
      if (drop.has(i)) continue
      rebuilt.push(surface[i])
    }
  }

  // ------------------------------------------------------------------ T2
  const tailResults = []
  for (let i = tailStart; i < rebuilt.length; i++) if (rebuilt[i].kind === 'result') tailResults.push(i)
  // Plugin parity: newest `keep` protected, all when fewer than `keep` exist.
  const cutIdx =
    cfg.keep > 0 && tailResults.length > 0
      ? tailResults[Math.max(0, tailResults.length - cfg.keep)]
      : Number.POSITIVE_INFINITY
  for (const i of tailResults) {
    if (i >= cutIdx) continue
    const item = rebuilt[i]
    if (!isClearableResult(item, cfg)) continue
    item.tok = placeholderTokens(item, cfg)
    item.masked = true
    result.t2 += 1
    const oi = origIndex.get(item)
    if (oi !== undefined) result.changedFrom = Math.min(result.changedFrom, oi)
  }

  result.surface = rebuilt
  result.cleared = result.t2 + result.t3
  if (!Number.isFinite(result.changedFrom)) result.changedFrom = -1
  return result
}

/** Convenience: total tokens of a rebuilt surface (assistant getters included). */
export function surfaceTokens(surface) {
  let total = 0
  for (const item of surface) total += tokOf(item)
  return total
}
