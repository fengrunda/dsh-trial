/**
 * dsh-design-pack — Cordis plugin entry (dev scaffold, `0.1.0-dev`).
 *
 * Registers the read-only `design_pack_read` tool on `ctx.tools`. It does
 * **not** inject pack text as a `user/message` (the opening broker/bridge still
 * owns prompt assembly), so installing this bundle cannot create a new sticky
 * transcript by itself.
 *
 * Behaviour and path/size rules live in `./pack.js`; this file only wires the
 * definition onto the tool registry. Configuration (see `cordis.patch.yml`):
 *
 * - `packRoot`     — absolute path, or path relative to
 *                    `$DSH_HOME/supervisor/thin-state` (default `packs`).
 * - `maxPackBytes` — positive UTF-8 byte cap (default `12288`); a larger pack
 *                    is rejected with its byte count, never truncated.
 *
 * @module dsh-design-pack
 */

import { createDesignPackToolOptions, normalizeConfig } from './pack.js'

/** Cordis row / logger channel id. */
export const name = 'design-pack'

/** Hard dependency: the tool registry must exist before `apply`. */
/** Named exports preserve loader injection metadata (no `export default`). */
export const inject = ['tools']

/**
 * Mount the plugin.
 *
 * @param {import('@deepseek-ai/cordis').Context} ctx
 * @param {Record<string, unknown>} [config]
 */
export function apply(ctx, config = {}) {
  const { packRoot, maxPackBytes } = normalizeConfig(config)
  const definition = createDesignPackToolOptions({ packRoot, maxPackBytes })
  ctx.tools.register(definition)
  ctx.logger?.info?.(
    `dsh-design-pack mounted: tool=${definition.name} packRoot=${packRoot} maxPackBytes=${maxPackBytes}`,
  )
}

