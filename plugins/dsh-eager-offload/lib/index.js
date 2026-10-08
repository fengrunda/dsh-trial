/**
 * dsh-eager-offload — Cordis plugin entry.
 *
 * Hooks `tools/post-execute` (same waterfall as official spill-policy): after
 * `next()`, if the accepted plain-text projection exceeds `inlineMaxBytes`,
 * write the FULL text under `$DSH_HOME/offload/<session>/<id>.txt` and replace
 * the model-facing content with a head/tail preview + path + byte count.
 *
 * Unlike spill-policy this plugin **does** cover `read`. Reads of paths under
 * the offload root are truncated in place (no second file) to break
 * read→offload→read loops.
 *
 * Age-based clearing (`ageMaskEnabled`) is DEPRECATED and a no-op on this dsh:
 * `agent/pre-step` only receives this step's newly claimed user messages, never
 * the full history, so nothing was ever masked. The config keys are still
 * accepted (and a one-time warning is logged) so existing profiles load. The
 * replacement is `contextClear` (compaction-coupled, persistent surfaceOp
 * `replace`): T2 wires the tool-result clearing; collapsing oversized
 * write/edit steps (T3) is still pending.
 *
 * Named exports only (`apply` / `inject` / `name`) — `export default apply`
 * breaks Cordis inject metadata (see dsh-design-pack).
 *
 * @module dsh-eager-offload
 */

import { clearAfterCompaction } from './context-clear.js'
import { flattenPlainText, maybeOffload, normalizeConfig } from './offload.js'

/** Cordis row / logger channel id. */
export const name = 'eager-offload'

/** Hard dependency: the tool registry exposes `tools/post-execute`. */
export const inject = ['tools']

/**
 * Owning session id, or `undefined` for a direct/test call with no agent.
 * @param {{ agent?: { session?: { header?: { id?: string } } } }} exec
 * @returns {string | undefined}
 */
function ownerSessionId(exec) {
  return exec.agent?.session?.header?.id
}

/**
 * Mount the plugin.
 *
 * @param {import('@deepseek-ai/cordis').Context} ctx
 * @param {Record<string, unknown>} [config]
 */
export function apply(ctx, config = {}) {
  const cfg = normalizeConfig(config)

  ctx.on(
    'tools/post-execute',
    async (exec, result, next) => {
      const decision = await next()
      // Mirror spill-policy guards: only reshape accepted content projections
      // (not value replacements), and skip nested PTC sub-calls.
      if (
        decision.kind !== 'accept' ||
        Object.hasOwn(decision, 'value') ||
        exec.parent !== undefined
      ) {
        return decision
      }

      const text = flattenPlainText(decision.content ?? result.content)
      if (text === undefined) return decision

      let replaced
      try {
        replaced = await maybeOffload({
          toolName: exec.name,
          callId: exec.callId,
          arguments: exec.arguments,
          text,
          sessionId: ownerSessionId(exec),
          cfg,
        })
      } catch (error) {
        ctx.logger?.warn?.(
          `eager-offload: failed for ${exec.name}: ${String(error)}; keeping inline content`,
        )
        return decision
      }
      if (replaced === undefined) return decision

      return {
        kind: 'accept',
        content: [{ type: 'text', text: replaced }],
        ...(decision.additionalContexts
          ? { additionalContexts: decision.additionalContexts }
          : {}),
      }
    },
    { prepend: true },
  )

  // contextClear (T2): compaction-coupled clearing of old tool results. The
  // listener is registered only when enabled, and prepended so it is outermost
  // in the `agent/pre-step` waterfall: after `next()` returns, compaction-basic
  // (and any inner listener) has already committed its summary + checkpoint, so
  // the fresh surface can be cleared before the step starts. The returned
  // decision is always passed through unchanged.
  if (cfg.contextClear.enabled && cfg.contextClear.mode === 'compaction-coupled') {
    let warnedMissingMeter = false
    ctx.on(
      'agent/pre-step',
      async ({ agent }, next) => {
        const decision = await next()
        if (decision?.kind !== 'enter') return decision
        try {
          const tokenMeter = ctx.get?.('tokenMeter')
          if (tokenMeter === undefined) {
            if (!warnedMissingMeter) {
              warnedMissingMeter = true
              ctx.logger?.warn?.(
                'dsh-eager-offload: contextClear is enabled but the tokenMeter service is ' +
                  'unavailable; skipping tool-result clearing',
              )
            }
            return decision
          }
          const session = agent?.session
          const result = await clearAfterCompaction(session, {
            cfg,
            tokenMeter,
            logger: ctx.logger,
            sessionId: session?.header?.id,
          })
          if (result.triggered) {
            ctx.logger?.info?.(
              `context-clear: cleared=${result.cleared} bytes ${result.bytesBefore}->${result.bytesAfter}`,
            )
          }
        } catch (error) {
          ctx.logger?.warn?.(
            `dsh-eager-offload: context-clear failed: ${String(error)}; continuing the turn`,
          )
        }
        return decision
      },
      { prepend: true },
    )
  }

  // ageMask is deprecated and intentionally NOT registered: `agent/pre-step`
  // only ever receives this step's newly claimed user messages, never the full
  // history (the request is derived from `session.deriveMessages()`), so the
  // old listener could never mask anything. The keys are still accepted by
  // normalizeConfig for backward compatibility; warn once if a profile sets it.
  if (cfg.ageMaskEnabled === true) {
    ctx.logger?.warn?.(
      'dsh-eager-offload: ageMaskEnabled is deprecated and a no-op on this dsh ' +
        '(agent/pre-step never sees the full history); use contextClear',
    )
  }

  ctx.logger?.info?.(
    `dsh-eager-offload mounted: offloadRoot=${cfg.offloadRoot} inlineMaxBytes=${cfg.inlineMaxBytes} ` +
      `previewHead=${cfg.previewHeadBytes} previewTail=${cfg.previewTailBytes} ` +
      `offloadReadMaxInline=${cfg.offloadReadMaxInlineBytes} ` +
      `contextClear=${
        cfg.contextClear.enabled
          ? `on(${cfg.contextClear.mode},keep=${cfg.contextClear.keepRecentResults})`
          : 'off'
      }`,
  )
}
