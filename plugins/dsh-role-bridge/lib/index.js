import {
  normalizeConfig,
  createSendToRoleToolOptions,
  createAskSupervisorToolOptions,
  createSubmitForReviewToolOptions,
} from './bridge.js'

export const name = 'role-bridge'
export const inject = ['tools']

export function apply(ctx, config = {}) {
  const cfg = normalizeConfig(config)
  const registered = []
  if (cfg.enableSendToRole) {
    const t = createSendToRoleToolOptions(cfg)
    ctx.tools.register(t)
    registered.push(t.name)
  }
  if (cfg.wrappers.includes('ask_supervisor')) {
    const t = createAskSupervisorToolOptions(cfg)
    ctx.tools.register(t)
    registered.push(t.name)
  }
  if (cfg.wrappers.includes('submit_for_review')) {
    const t = createSubmitForReviewToolOptions(cfg)
    ctx.tools.register(t)
    registered.push(t.name)
  }
  ctx.logger?.info?.(
    `dsh-role-bridge mounted: tools=${registered.join(',')} timeoutSec=${cfg.timeoutSec}`,
  )
}
