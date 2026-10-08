/**
 * dsh-design-pack — bounded design-pack reader (pure core).
 *
 * This module holds every piece of behaviour the `design_pack_read` tool
 * exposes, with no dependency on `@deepseek-ai/dsh-tools`:
 *
 * - `resolvePackRoot` / `resolvePackPath` — confine reads to `packRoot`
 *   (absolute paths, `..` traversal and symlink escapes are rejected);
 * - `readDesignPack` — return UTF-8 text for a pack at or below
 *   `maxPackBytes`, otherwise reject with the byte count and **no content**
 *   (so an oversized pack can never be truncated into history);
 * - `createDesignPackToolOptions` — the raw {@link ToolDefinition} registered
 *   on `ctx.tools`.
 *
 * The plugin deliberately registers a plain definition instead of importing
 * `defineTool`. A `file:`/`link:` dev install keeps the package's real path
 * outside the profile's `node_modules`, where a bare peer import would not
 * resolve; the registry accepts a plain object and only needs
 * name/description/parameters/output/execute. Parameter validation is done
 * here, so the tool still fails soft instead of throwing on a bad argument.
 *
 * @module dsh-design-pack/pack
 */

import { existsSync, readFileSync, realpathSync, statSync } from 'node:fs'
import { homedir } from 'node:os'
import { isAbsolute, join, normalize, relative, resolve, sep } from 'node:path'

/** Default pack directory, relative to the thin-state root. */
export const DEFAULT_PACK_ROOT = 'packs'

/** Soft reject threshold in UTF-8 bytes (12 KiB, matching the box spill cap). */
export const DEFAULT_MAX_PACK_BYTES = 12288

/** Public tool name registered by this plugin. */
export const DESIGN_PACK_READ_TOOL = 'design_pack_read'

/**
 * Absolute thin-state root: `$DSH_HOME/supervisor/thin-state`.
 *
 * @param {{ dshHome?: string, home?: string, env?: NodeJS.ProcessEnv }} [opts]
 * @returns {string}
 */
export function resolveThinStateRoot(opts = {}) {
  const env = opts.env ?? process.env
  const home = opts.home ?? homedir()
  const dshHome = typeof opts.dshHome === 'string' && opts.dshHome.trim()
    ? opts.dshHome.trim()
    : (typeof env.DSH_HOME === 'string' && env.DSH_HOME.trim()
      ? env.DSH_HOME.trim()
      : join(home, '.dsh'))
  return resolve(dshHome, 'supervisor', 'thin-state')
}

/**
 * Absolute pack root. A relative `packRoot` is anchored under thin-state.
 *
 * @param {unknown} packRoot
 * @param {{ dshHome?: string, home?: string, env?: NodeJS.ProcessEnv }} [opts]
 * @returns {string}
 */
export function resolvePackRoot(packRoot, opts = {}) {
  const raw = typeof packRoot === 'string' && packRoot.trim() ? packRoot.trim() : DEFAULT_PACK_ROOT
  return isAbsolute(raw) ? normalize(raw) : resolve(resolveThinStateRoot(opts), raw)
}

/**
 * Validate and normalise plugin config. Throws on a bad `maxPackBytes`
 * (fail loud at mount rather than at the first read).
 *
 * @param {Record<string, unknown>} [config]
 * @returns {{ packRoot: string, maxPackBytes: number }}
 */
export function normalizeConfig(config = {}) {
  const source = config && typeof config === 'object' ? config : {}
  const packRoot = typeof source.packRoot === 'string' && source.packRoot.trim()
    ? source.packRoot.trim()
    : DEFAULT_PACK_ROOT
  const rawMax = source.maxPackBytes
  // Strict: YAML gives a number; a string here is a config mistake, not a value to coerce.
  const maxPackBytes = rawMax === undefined || rawMax === null
    ? DEFAULT_MAX_PACK_BYTES
    : (typeof rawMax === 'number' ? rawMax : Number.NaN)
  if (!Number.isInteger(maxPackBytes) || maxPackBytes <= 0) {
    throw new Error('dsh-design-pack: maxPackBytes must be a positive integer')
  }
  return { packRoot, maxPackBytes }
}

/**
 * Resolve one pack path under `root`, rejecting anything that escapes it.
 *
 * @param {string} root - absolute, already-resolved pack root
 * @param {unknown} rel - caller-supplied relative path
 * @returns {string} absolute path inside `root`
 */
export function resolvePackPath(root, rel) {
  if (typeof rel !== 'string' || rel.trim() === '') {
    throw new Error('path required (relative to packRoot)')
  }
  const cleaned = rel.trim()
  if (cleaned.includes('\0')) throw new Error('path must not contain NUL')
  if (isAbsolute(cleaned)) throw new Error(`path must be relative to packRoot (got absolute: ${cleaned})`)
  const full = resolve(root, cleaned)
  const relToRoot = relative(root, full)
  if (relToRoot === '') throw new Error('path must name a pack file, not packRoot itself')
  if (relToRoot === '..' || relToRoot.startsWith(`..${sep}`) || isAbsolute(relToRoot)) {
    throw new Error(`path escapes packRoot (${cleaned})`)
  }
  return full
}

/**
 * Reject a resolved file whose real path (after following symlinks) is still
 * outside the real pack root. Only called for a path that already exists.
 *
 * @param {string} root
 * @param {string} full
 */
function assertNoSymlinkEscape(root, full) {
  const realRoot = existsSync(root) ? realpathSync(root) : root
  const realFull = realpathSync(full)
  const rel = relative(realRoot, realFull)
  if (rel === '..' || rel.startsWith(`..${sep}`) || isAbsolute(rel)) {
    throw new Error('resolved pack path escapes packRoot via symlink')
  }
}

/**
 * @typedef {object} DesignPackResult
 * @property {boolean} ok
 * @property {string} [path]
 * @property {number} [bytes]
 * @property {number} maxPackBytes
 * @property {string} [text]
 * @property {string} [error]
 */

/**
 * Read one bounded design pack.
 *
 * The success value carries the pack text; every rejection carries only an
 * error string plus byte counts, never partial content. That is the property
 * the medium-ticket path relies on: an oversized pack is refused instead of
 * being silently truncated into the session history.
 *
 * @param {{ packRoot?: string, maxPackBytes?: number, path?: unknown, env?: NodeJS.ProcessEnv, dshHome?: string, home?: string }} options
 * @returns {DesignPackResult}
 */
export function readDesignPack(options = {}) {
  const { maxPackBytes } = normalizeConfig({ maxPackBytes: options.maxPackBytes })
  const root = resolvePackRoot(options.packRoot, options)
  const requested = typeof options.path === 'string' ? options.path.trim() : ''

  let full
  try {
    full = resolvePackPath(root, requested)
  } catch (error) {
    return { ok: false, error: String(error?.message ?? error), maxPackBytes }
  }

  let stats
  try {
    stats = statSync(full)
  } catch {
    return { ok: false, error: `pack not found: ${requested}`, maxPackBytes }
  }
  if (!stats.isFile()) {
    return { ok: false, error: `not a regular file: ${requested}`, maxPackBytes }
  }
  try {
    assertNoSymlinkEscape(root, full)
  } catch (error) {
    return { ok: false, error: String(error?.message ?? error), maxPackBytes }
  }
  if (stats.size > maxPackBytes) {
    return {
      ok: false,
      error: `pack is ${stats.size} bytes and exceeds maxPackBytes=${maxPackBytes}; split the slice (content not loaded)`,
      bytes: stats.size,
      maxPackBytes,
    }
  }

  const buffer = readFileSync(full)
  if (buffer.byteLength > maxPackBytes) {
    // Defensive: the file grew between stat and read.
    return {
      ok: false,
      error: `pack is ${buffer.byteLength} bytes and exceeds maxPackBytes=${maxPackBytes}; split the slice (content not loaded)`,
      bytes: buffer.byteLength,
      maxPackBytes,
    }
  }
  return { ok: true, path: requested, bytes: buffer.byteLength, maxPackBytes, text: buffer.toString('utf8') }
}

/**
 * Render one canonical value to model-facing content. Never throws: replay of
 * an older value must not break session rendering.
 *
 * @param {unknown} value
 * @returns {{ type: 'text', text: string }[]}
 */
export function renderDesignPackResult(value) {
  const record = value && typeof value === 'object' ? /** @type {Record<string, unknown>} */ (value) : {}
  if (record.ok === true) return [{ type: 'text', text: typeof record.text === 'string' ? record.text : '' }]
  const error = typeof record.error === 'string' && record.error ? record.error : 'unknown error'
  return [{ type: 'text', text: `design_pack_read failed: ${error}` }]
}

/**
 * Build the registry-ready definition for `design_pack_read`.
 *
 * @param {{ packRoot?: unknown, maxPackBytes?: unknown, env?: NodeJS.ProcessEnv, dshHome?: string, home?: string }} [config]
 * @returns {import('@deepseek-ai/dsh-tools').ToolDefinition}
 */
export function createDesignPackToolOptions(config = {}) {
  const { packRoot, maxPackBytes } = normalizeConfig(config)
  const readOptions = {
    packRoot,
    maxPackBytes,
    ...(config.env ? { env: config.env } : {}),
    ...(config.dshHome ? { dshHome: config.dshHome } : {}),
    ...(config.home ? { home: config.home } : {}),
  }
  return {
    name: DESIGN_PACK_READ_TOOL,
    description:
      'Read one bounded design pack (Markdown) for the current medium ticket slice. '
      + 'Pass a path relative to the configured packRoot. A pack larger than maxPackBytes is '
      + 'rejected outright — content is never truncated into history; split the slice instead.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      required: ['path'],
      properties: {
        path: {
          type: 'string',
          description: `Relative pack path under packRoot, e.g. "slice-design-pack-mvp.pack.md".`,
        },
      },
    },
    output: {
      schema: {
        type: 'object',
        additionalProperties: true,
        properties: {
          ok: { type: 'boolean' },
          path: { type: 'string' },
          bytes: { type: 'integer' },
          maxPackBytes: { type: 'integer' },
          text: { type: 'string' },
          error: { type: 'string' },
        },
      },
      render: (_args, value) => renderDesignPackResult(value),
    },
    isConcurrencySafe: () => true,
    execute: async (args) => {
      const path = args && typeof args === 'object' ? /** @type {Record<string, unknown>} */ (args).path : undefined
      return readDesignPack({ ...readOptions, path })
    },
  }
}
