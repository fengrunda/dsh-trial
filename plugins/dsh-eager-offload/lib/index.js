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
 * Named exports only (`apply` / `inject` / `name`) — `export default apply`
 * breaks Cordis inject metadata (see dsh-design-pack).
 *
 * @module dsh-eager-offload
 */

import {
  flattenPlainText,
  maybeOffload,
  normalizeConfig,
} from './offload.js'

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

  ctx.logger?.info?.(
    `dsh-eager-offload mounted: offloadRoot=${cfg.offloadRoot} inlineMaxBytes=${cfg.inlineMaxBytes} ` +
      `previewHead=${cfg.previewHeadBytes} previewTail=${cfg.previewTailBytes} ` +
      `offloadReadMaxInline=${cfg.offloadReadMaxInlineBytes}`,
  )
}
