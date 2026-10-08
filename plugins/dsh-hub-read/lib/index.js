/**
 * dsh-hub-read — Cordis plugin entry (`0.1.0-dev`).
 *
 * Registers read-only Hub tools on `ctx.tools`:
 *
 * - `hub_browse`              — always on;
 * - `hub_resolve`             — always on;
 * - `hub_neighborhood_read`   — only when profile config sets
 *                               `neighborhoodEnabled: true` (wire default-off).
 *
 * Behaviour and the subprocess bridge live in `./wire.js`; this file only
 * normalizes config and wires definitions onto the tool registry.
 * Configuration (see `cordis.patch.yml`):
 *
 * - `hubRepoRoot`        — absolute path to the knowledge-hub checkout
 *                          (default `/workspace/hermes-work/knowledge-hub`);
 * - `pythonBin`          — interpreter (default `python3`);
 * - `neighborhoodEnabled`— mount `hub_neighborhood_read` (default off).
 * - `engineEnvFile`     — KEY=VALUE file merged into the python subprocess
 *                          env only (never logged). Empty if unset.
 *
 * @module dsh-hub-read
 */

import {
  createHubBrowseToolOptions,
  createHubNeighborhoodToolOptions,
  createHubResolveToolOptions,
  normalizeConfig,
} from './wire.js'

/** Cordis row / logger channel id. */
export const name = 'hub-read'

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
  const { hubRepoRoot, pythonBin, neighborhoodEnabled, engineEnvFile } = normalizeConfig(config)
  const options = { hubRepoRoot, pythonBin, engineEnvFile }

  const definitions = [
    createHubBrowseToolOptions(options),
    createHubResolveToolOptions(options),
  ]
  if (neighborhoodEnabled) {
    definitions.push(createHubNeighborhoodToolOptions(options))
  }
  for (const definition of definitions) {
    ctx.tools.register(definition)
  }

  ctx.logger?.info?.(
    `dsh-hub-read mounted: tools=${definitions.map((d) => d.name).join(',')} hubRepoRoot=${hubRepoRoot} neighborhoodEnabled=${neighborhoodEnabled} engineEnvFile=${engineEnvFile ? 'set' : 'unset'}`,
  )
}
