/**
 * dsh-trial-desk — Cordis plugin entry.
 *
 * Tools for a human/supervisor `dsh web` session:
 *   trial_drop_job / trial_validate_job / trial_chain_reply / trial_status
 *
 * Desk writes trial inbox and reads thin-state. It is **not** the broker:
 * no open-slice, no rooms, no Hub webhooks, never broker-khub-prod.
 *
 * Named exports only (`apply` / `inject` / `name`) — `export default apply`
 * breaks Cordis inject metadata (see dsh-eager-offload).
 *
 * @module dsh-trial-desk
 */

import {
  createChainReplyToolOptions,
  createDropJobToolOptions,
  createStatusToolOptions,
  createValidateJobToolOptions,
  normalizeConfig,
} from './desk.js'

/** Cordis row / logger channel id. */
export const name = 'trial-desk'

/** Hard dependency: tool registry. */
export const inject = ['tools']

/**
 * @param {import('@deepseek-ai/cordis').Context} ctx
 * @param {Record<string, unknown>} [config]
 */
export function apply(ctx, config) {
  // `config:` with only commented keys in cordis.patch.yml composes to null.
  const cfg = normalizeConfig(config ?? {})
  const tools = [
    createDropJobToolOptions(cfg),
    createValidateJobToolOptions(cfg),
    createChainReplyToolOptions(cfg),
    createStatusToolOptions(cfg),
  ]
  for (const t of tools) ctx.tools.register(t)
  ctx.logger?.info?.(
    `dsh-trial-desk mounted: tools=${tools.map((t) => t.name).join(',')} `
      + `inbox=${cfg.inboxRoot} thin=${cfg.thinStateRoot} brokerDir=${cfg.brokerDir}`,
  )
}
