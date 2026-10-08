// Build the token-annotated item lineage for one session from the extracted
// per-step usage deltas. Ported from /tmp/tokaudit/agemask-sim/model.js.

/** Calibrated via b0f6964e post-compaction hit 8960 tok for 37256 chars sys+tools. */
export const SYS_CHARS_PER_TOK = 4.16

/** Fallback chars-per-token used when a step's delta is dominated by compaction. */
export const REST_CHARS_PER_TOK = 3.2

/** Reasoning chars-per-token fallback when `reasoningTokens` is absent. */
export const REASON_CHARS_PER_TOK = 4.1

/** Token count of a lineage item (assistant items compute from their parts). */
export function tokOf(item) {
  if (item.kind === 'asst') {
    let t = item.reason + item.text
    for (const arg of item.args) t += arg.tok
    return t
  }
  return item.tok
}

/** Sum the token counts of a surface (optionally a slice `[a, b)`). */
export function sumTokens(surface, a = 0, b = surface.length) {
  let total = 0
  for (let i = a; i < b; i++) total += tokOf(surface[i])
  return total
}

/**
 * @param {any} session output of `extractSession`
 * @returns {{items:any[], reqs:any[], sysTools:number}}
 */
export function build(session) {
  const items = []
  let id = 0
  const first = session.steps[0]
  const sysTools = Math.min(
    Math.round((session.sysChars + session.toolChars) / SYS_CHARS_PER_TOK),
    first.P - 200,
  )
  items.push({ id: id++, kind: 'sys', tok: Math.max(0, sysTools), step: -1 })
  items.push({ id: id++, kind: 'user0', tok: Math.max(0, first.P - sysTools), step: -1 })

  const reqs = []
  const compBefore = new Set(session.compactions.map((c) => c.beforeStep))
  for (let i = 0; i < session.steps.length; i++) {
    const step = session.steps[i]
    if (i > 0) {
      const prev = session.steps[i - 1]
      const delta = step.P - prev.P
      const pre = step.pre
      const chars = pre.reduce((a, x) => a + x.chars, 0)
      let asstTok = prev.out
      let rest
      if (compBefore.has(i) || delta <= 0) {
        rest = Math.round(chars / REST_CHARS_PER_TOK)
      } else {
        if (asstTok > delta * 0.95) asstTok = Math.round(delta * 0.95)
        rest = delta - asstTok
      }
      const a = prev.asst
      const reason = Math.min(
        prev.reasoningTokens || Math.round(a.rc / REASON_CHARS_PER_TOK),
        Math.round(asstTok * 0.98),
      )
      const nonReason = asstTok - reason
      const argChars = a.args.reduce((x, y) => x + y.chars, 0)
      const denominator = argChars + a.tc + 1
      const args = a.args.map((g) => ({
        tool: g.tool,
        chars: g.chars,
        id: g.id,
        tok: Math.round((nonReason * g.chars) / denominator),
        masked: false,
      }))
      const textTok = Math.max(0, nonReason - args.reduce((x, y) => x + y.tok, 0))
      items.push({
        id: id++,
        kind: 'asst',
        step: i - 1,
        reason,
        reasonChars: a.rc,
        args,
        text: textTok,
      })
      for (const x of pre) {
        const tok = chars > 0 ? Math.max(1, Math.round((rest * x.chars) / chars)) : 1
        items.push({
          id: id++,
          kind: x.kind,
          step: i,
          tok,
          orig: tok,
          chars: x.chars,
          textChars: x.textChars,
          tool: x.tool,
          callId: x.callId,
          isError: x.isError,
          plain: x.plain,
          offloaded: x.offloaded,
          fail: x.exitNonZero || x.failHint,
        })
      }
    }
    reqs.push({
      i,
      t: step.t,
      nItems: items.length,
      actual: { P: step.P, hit: step.hit, miss: step.miss, out: step.out },
    })
  }
  return { items, reqs, sysTools }
}
