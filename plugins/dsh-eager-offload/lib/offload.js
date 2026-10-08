/**
 * dsh-eager-offload — pure helpers (no Cordis).
 *
 * Offload oversized plain-text tool results to session-scoped files under
 * `$DSH_HOME/offload/<session>/<id>.txt` and leave a short head/tail preview
 * plus path + byte count inline. Covers `read` (unlike official spill-policy)
 * while avoiding a read→offload→read loop for paths under the offload root.
 *
 * @module dsh-eager-offload/offload
 */

import { createHash, randomBytes } from 'node:crypto'
import { mkdir, open, writeFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { isAbsolute, join, normalize, relative, resolve, sep } from 'node:path'

/** Default model-facing inline budget (UTF-8 bytes). Below official spill 12288. */
export const DEFAULT_INLINE_MAX_BYTES = 4096

/** Default head/tail preview ends when not otherwise derived from the budget. */
export const DEFAULT_PREVIEW_HEAD_BYTES = 1536
export const DEFAULT_PREVIEW_TAIL_BYTES = 1024

/**
 * When `read` targets a path under the offload root, allow this much inline
 * before truncating in place (never writes another offload file).
 */
export const DEFAULT_OFFLOAD_READ_MAX_INLINE_BYTES = 16384

/**
 * Age-based clearing is opt-in: product profiles keep every tool_result in full.
 * @deprecated ageMask is a no-op on dsh 0.1.5-rc.2 / 0.2.x — `agent/pre-step`
 * never sees the full history. Kept only so existing profiles still load.
 */
export const DEFAULT_AGE_MASK_ENABLED = false

/**
 * Newest N tool_result messages kept verbatim once age-mask is enabled.
 * @deprecated Kept only so existing profiles still load; see `maskOldToolResults`.
 */
export const DEFAULT_AGE_MASK_KEEP_RECENT_N = 8

/** Marker embedded in every replacement notice (loop / composition detection). */
export const OFFLOAD_MARK = 'dsh-eager-offload:'

/**
 * `contextClear` defaults (T1 skeleton; wiring lands in T2/T3). Compaction-coupled
 * clearing only: right after `compaction-basic` summarises, the old tool results
 * kept in its retained tail are cleared via a persistent surfaceOp `replace`.
 * Opt-in per profile: acp / acp-lite leave it unconfigured, i.e. disabled.
 */
export const DEFAULT_CONTEXT_CLEAR = Object.freeze({
  enabled: false,
  mode: 'compaction-coupled',
  keepRecentResults: 8,
  minResultBytes: 1200,
  placeholder: Object.freeze({ headBytes: 160, tailBytes: 240, failTailBytes: 600 }),
  collapseWriteSteps: Object.freeze({
    enabled: true,
    tools: Object.freeze(['write', 'edit']),
    minArgChars: 1200,
  }),
})

/** Allowed `contextClear.mode` values; anything else is rejected at mount. */
export const CONTEXT_CLEAR_MODES = Object.freeze(['off', 'compaction-coupled'])

/** Official spill-policy notice fragment — already-bounded results may still be shrunk. */
export const SPILL_LOCATION_MARK = ' Full formatted result stored at: '

/**
 * Resolve `$DSH_HOME` (env wins, else `~/.dsh`).
 * @param {{ dshHome?: string, home?: string, env?: NodeJS.ProcessEnv }} [opts]
 * @returns {string}
 */
export function resolveDshHome(opts = {}) {
  const env = opts.env ?? process.env
  if (typeof opts.dshHome === 'string' && opts.dshHome.trim()) return resolve(opts.dshHome.trim())
  if (typeof env.DSH_HOME === 'string' && env.DSH_HOME.trim()) return resolve(env.DSH_HOME.trim())
  return resolve(opts.home ?? homedir(), '.dsh')
}

/**
 * Absolute offload root. Relative values anchor under `$DSH_HOME`.
 * @param {unknown} offloadRoot
 * @param {{ dshHome?: string, home?: string, env?: NodeJS.ProcessEnv }} [opts]
 * @returns {string}
 */
export function resolveOffloadRoot(offloadRoot, opts = {}) {
  const raw = typeof offloadRoot === 'string' && offloadRoot.trim() ? offloadRoot.trim() : 'offload'
  return isAbsolute(raw) ? normalize(raw) : resolve(resolveDshHome(opts), raw)
}

/**
 * @param {unknown} n
 * @param {string} label
 * @param {number} fallback
 * @returns {number}
 */
function requireNonNegInt(n, label, fallback) {
  if (n === undefined || n === null) return fallback
  if (typeof n !== 'number' || !Number.isInteger(n) || n < 0) {
    throw new Error(`eager-offload: ${label} must be a non-negative integer (got ${n})`)
  }
  return n
}

/**
 * @param {unknown} value
 * @param {string} label
 * @param {boolean} fallback
 * @returns {boolean}
 */
function requireBoolean(value, label, fallback) {
  if (value === undefined || value === null) return fallback
  if (value !== true && value !== false) {
    throw new TypeError(`dsh-eager-offload: ${label} must be a boolean`)
  }
  return value
}

/**
 * Reject unknown keys so typos surface at mount instead of being silently ignored.
 * @param {Record<string, unknown>} object
 * @param {ReadonlySet<string>} allowed
 * @param {string} label
 */
function rejectUnknownKeys(object, allowed, label) {
  for (const key of Object.keys(object)) {
    if (!allowed.has(key)) {
      throw new TypeError(`dsh-eager-offload: ${label}.${key} is not a known option`)
    }
  }
}

/**
 * Coerce an optional plain object; `undefined` / `null` become `{}`.
 * @param {unknown} value
 * @param {string} label
 * @returns {Record<string, unknown>}
 */
function optionalObject(value, label) {
  if (value === undefined || value === null) return {}
  if (typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`dsh-eager-offload: ${label} must be an object`)
  }
  return /** @type {Record<string, unknown>} */ (value)
}

const CONTEXT_CLEAR_KEYS = new Set([
  'enabled',
  'mode',
  'keepRecentResults',
  'minResultBytes',
  'placeholder',
  'collapseWriteSteps',
])
const CONTEXT_CLEAR_PLACEHOLDER_KEYS = new Set(['headBytes', 'tailBytes', 'failTailBytes'])
const CONTEXT_CLEAR_COLLAPSE_KEYS = new Set(['enabled', 'tools', 'minArgChars'])

/**
 * Validate and normalise the `contextClear` block, merging partial input over
 * `DEFAULT_CONTEXT_CLEAR`. Unknown keys, non-objects, bad enum values and bad
 * integers throw at mount. The returned object is deeply frozen.
 *
 * @param {unknown} raw
 * @returns {{
 *   enabled: boolean,
 *   mode: 'off' | 'compaction-coupled',
 *   keepRecentResults: number,
 *   minResultBytes: number,
 *   placeholder: { headBytes: number, tailBytes: number, failTailBytes: number },
 *   collapseWriteSteps: { enabled: boolean, tools: readonly string[], minArgChars: number },
 * }}
 */
export function normalizeContextClear(raw) {
  const source = optionalObject(raw, 'contextClear')
  rejectUnknownKeys(source, CONTEXT_CLEAR_KEYS, 'contextClear')

  const enabled = requireBoolean(source.enabled, 'contextClear.enabled', DEFAULT_CONTEXT_CLEAR.enabled)

  let mode = DEFAULT_CONTEXT_CLEAR.mode
  if (source.mode !== undefined && source.mode !== null) {
    if (!CONTEXT_CLEAR_MODES.includes(/** @type {string} */ (source.mode))) {
      throw new TypeError(
        `dsh-eager-offload: contextClear.mode must be one of ${CONTEXT_CLEAR_MODES.join(' | ')} ` +
          `(got ${String(source.mode)})`,
      )
    }
    mode = /** @type {'off' | 'compaction-coupled'} */ (source.mode)
  }

  const keepRecentResults = requireNonNegInt(
    source.keepRecentResults,
    'contextClear.keepRecentResults',
    DEFAULT_CONTEXT_CLEAR.keepRecentResults,
  )
  const minResultBytes = requireNonNegInt(
    source.minResultBytes,
    'contextClear.minResultBytes',
    DEFAULT_CONTEXT_CLEAR.minResultBytes,
  )

  const placeholderSource = optionalObject(source.placeholder, 'contextClear.placeholder')
  rejectUnknownKeys(placeholderSource, CONTEXT_CLEAR_PLACEHOLDER_KEYS, 'contextClear.placeholder')
  const placeholder = Object.freeze({
    headBytes: requireNonNegInt(
      placeholderSource.headBytes,
      'contextClear.placeholder.headBytes',
      DEFAULT_CONTEXT_CLEAR.placeholder.headBytes,
    ),
    tailBytes: requireNonNegInt(
      placeholderSource.tailBytes,
      'contextClear.placeholder.tailBytes',
      DEFAULT_CONTEXT_CLEAR.placeholder.tailBytes,
    ),
    failTailBytes: requireNonNegInt(
      placeholderSource.failTailBytes,
      'contextClear.placeholder.failTailBytes',
      DEFAULT_CONTEXT_CLEAR.placeholder.failTailBytes,
    ),
  })

  const collapseSource = optionalObject(source.collapseWriteSteps, 'contextClear.collapseWriteSteps')
  rejectUnknownKeys(collapseSource, CONTEXT_CLEAR_COLLAPSE_KEYS, 'contextClear.collapseWriteSteps')
  let tools = [...DEFAULT_CONTEXT_CLEAR.collapseWriteSteps.tools]
  if (collapseSource.tools !== undefined && collapseSource.tools !== null) {
    if (!Array.isArray(collapseSource.tools)) {
      throw new TypeError('dsh-eager-offload: contextClear.collapseWriteSteps.tools must be an array')
    }
    tools = collapseSource.tools.map((tool) => {
      if (typeof tool !== 'string' || !tool.trim()) {
        throw new TypeError(
          'dsh-eager-offload: contextClear.collapseWriteSteps.tools entries must be non-empty strings',
        )
      }
      return tool.trim()
    })
  }
  const collapseWriteSteps = Object.freeze({
    enabled: requireBoolean(
      collapseSource.enabled,
      'contextClear.collapseWriteSteps.enabled',
      DEFAULT_CONTEXT_CLEAR.collapseWriteSteps.enabled,
    ),
    tools: Object.freeze(tools),
    minArgChars: requireNonNegInt(
      collapseSource.minArgChars,
      'contextClear.collapseWriteSteps.minArgChars',
      DEFAULT_CONTEXT_CLEAR.collapseWriteSteps.minArgChars,
    ),
  })

  return Object.freeze({ enabled, mode, keepRecentResults, minResultBytes, placeholder, collapseWriteSteps })
}

/**
 * Validate and normalise plugin config. Throws on bad numbers at mount.
 *
 * @param {Record<string, unknown>} [config]
 * @param {{ dshHome?: string, home?: string, env?: NodeJS.ProcessEnv }} [opts]
 * @returns {{
 *   offloadRoot: string,
 *   inlineMaxBytes: number,
 *   previewHeadBytes: number,
 *   previewTailBytes: number,
 *   offloadReadMaxInlineBytes: number,
 *   ageMaskEnabled: boolean,
 *   ageMaskKeepRecentN: number,
 *   contextClear: {
 *     enabled: boolean,
 *     mode: 'off' | 'compaction-coupled',
 *     keepRecentResults: number,
 *     minResultBytes: number,
 *     placeholder: { headBytes: number, tailBytes: number, failTailBytes: number },
 *     collapseWriteSteps: { enabled: boolean, tools: readonly string[], minArgChars: number },
 *   },
 *   excludeTools: Set<string>,
 *   toolOverrides: Map<string, { inlineMaxBytes?: number }>,
 * }}
 *
 * `ageMaskEnabled` / `ageMaskKeepRecentN` are accepted and validated only so
 * existing profiles keep loading: ageMask is a no-op on this dsh and is
 * deprecated in favour of `contextClear`.
 */
export function normalizeConfig(config = {}, opts = {}) {
  const source = config && typeof config === 'object' ? config : {}
  const offloadRoot = resolveOffloadRoot(source.offloadRoot, opts)
  const inlineMaxBytes = requireNonNegInt(source.inlineMaxBytes, 'inlineMaxBytes', DEFAULT_INLINE_MAX_BYTES)
  const previewHeadBytes = requireNonNegInt(
    source.previewHeadBytes,
    'previewHeadBytes',
    DEFAULT_PREVIEW_HEAD_BYTES,
  )
  const previewTailBytes = requireNonNegInt(
    source.previewTailBytes,
    'previewTailBytes',
    DEFAULT_PREVIEW_TAIL_BYTES,
  )
  const offloadReadMaxInlineBytes = requireNonNegInt(
    source.offloadReadMaxInlineBytes,
    'offloadReadMaxInlineBytes',
    DEFAULT_OFFLOAD_READ_MAX_INLINE_BYTES,
  )
  // ageMask* is accepted and validated only for backward compatibility with
  // existing profiles (e.g. ageMaskEnabled: true); it is deprecated and a no-op.
  const ageMaskEnabled =
    source.ageMaskEnabled === undefined || source.ageMaskEnabled === null
      ? DEFAULT_AGE_MASK_ENABLED
      : source.ageMaskEnabled === true || source.ageMaskEnabled === false
        ? source.ageMaskEnabled
        : (() => {
            throw new TypeError('dsh-eager-offload: ageMaskEnabled must be a boolean')
          })()
  const ageMaskKeepRecentN = requireNonNegInt(
    source.ageMaskKeepRecentN,
    'ageMaskKeepRecentN',
    DEFAULT_AGE_MASK_KEEP_RECENT_N,
  )

  const contextClear = normalizeContextClear(source.contextClear)

  const excludeTools = new Set()
  if (Array.isArray(source.excludeTools)) {
    for (const name of source.excludeTools) {
      if (typeof name === 'string' && name.trim()) excludeTools.add(name.trim())
    }
  }

  /** @type {Map<string, { inlineMaxBytes?: number }>} */
  const toolOverrides = new Map()
  const rawOverrides = source.toolOverrides
  if (rawOverrides && typeof rawOverrides === 'object' && !Array.isArray(rawOverrides)) {
    for (const [tool, ov] of Object.entries(rawOverrides)) {
      if (!tool || !ov || typeof ov !== 'object') continue
      const entry = {}
      if (ov.inlineMaxBytes !== undefined) {
        entry.inlineMaxBytes = requireNonNegInt(ov.inlineMaxBytes, `toolOverrides.${tool}.inlineMaxBytes`, inlineMaxBytes)
      }
      toolOverrides.set(tool, entry)
    }
  }

  return {
    offloadRoot,
    inlineMaxBytes,
    previewHeadBytes,
    previewTailBytes,
    offloadReadMaxInlineBytes,
    ageMaskEnabled,
    ageMaskKeepRecentN,
    contextClear,
    excludeTools,
    toolOverrides,
  }
}

/**
 * All-text content flattened to one UTF-8 string, or `undefined` if any block is non-text.
 * @param {Array<{ type?: string, text?: string }> | undefined} content
 * @returns {string | undefined}
 */
export function flattenPlainText(content) {
  if (!Array.isArray(content)) return undefined
  let text = ''
  for (const block of content) {
    if (!block || block.type !== 'text' || typeof block.text !== 'string') return undefined
    text += block.text
  }
  return text
}

/**
 * Stable session directory name (same shape idea as spill-local: short hash).
 * @param {string} sessionId
 * @returns {string}
 */
export function sessionDirName(sessionId) {
  const digest = createHash('sha256').update(String(sessionId)).digest('hex').slice(0, 12)
  return digest
}

/**
 * Encode one filesystem-safe path segment (injective over common tool names).
 * @param {string} raw
 * @returns {string}
 */
export function safeSegment(raw) {
  const s = String(raw || 'tool')
  if (s.length === 0) return 'tool'
  return s.replace(/[^A-Za-z0-9._-]+/g, '_').slice(0, 64) || 'tool'
}

/**
 * Whether `filePath` resolves under `offloadRoot` (after normalize; no must-exist).
 * @param {string} filePath
 * @param {string} offloadRoot
 * @returns {boolean}
 */
export function isUnderOffloadRoot(filePath, offloadRoot) {
  if (typeof filePath !== 'string' || !filePath.trim()) return false
  const root = resolve(offloadRoot)
  const target = isAbsolute(filePath) ? resolve(filePath) : resolve(filePath)
  const rel = relative(root, target)
  return rel === '' || (!rel.startsWith(`..${sep}`) && rel !== '..' && !isAbsolute(rel))
}

/**
 * Extract `file_path` from a `read` tool's arguments when present.
 * @param {unknown} args
 * @returns {string | undefined}
 */
export function readFilePathFromArgs(args) {
  if (!args || typeof args !== 'object') return undefined
  const fp = /** @type {Record<string, unknown>} */ (args).file_path
  return typeof fp === 'string' && fp.trim() ? fp.trim() : undefined
}

/**
 * Split `text` into a UTF-8-safe head + tail within the given byte budgets.
 * When the whole text fits in head+tail, returns it unchanged (omitted=0).
 *
 * @param {string} text
 * @param {number} headBytes
 * @param {number} tailBytes
 * @returns {{ text: string, omittedBytes: number, totalBytes: number }}
 */
export function headTailPreview(text, headBytes, tailBytes) {
  const totalBytes = Buffer.byteLength(text, 'utf8')
  const head = Math.max(0, headBytes | 0)
  const tail = Math.max(0, tailBytes | 0)
  if (totalBytes <= head + tail) {
    return { text, omittedBytes: 0, totalBytes }
  }

  const buf = Buffer.from(text, 'utf8')
  let headEnd = Math.min(head, buf.length)
  while (headEnd > 0 && (buf[headEnd - 1] & 0xc0) === 0x80) headEnd--
  let tailStart = Math.max(headEnd, buf.length - tail)
  while (tailStart < buf.length && (buf[tailStart] & 0xc0) === 0x80) tailStart++

  const headStr = buf.subarray(0, headEnd).toString('utf8')
  const tailStr = buf.subarray(tailStart).toString('utf8')
  const omittedBytes = Math.max(0, tailStart - headEnd)
  const marker = `\n\n… (${omittedBytes} bytes omitted) …\n\n`
  return {
    text: `${headStr}${marker}${tailStr}`,
    omittedBytes,
    totalBytes,
  }
}

/**
 * Build the model-facing notice (no leading separator).
 * @param {{ path: string, bytes: number, toolName: string, callId?: string }} info
 * @returns {string}
 */
export function formatOffloadNotice(info) {
  const call = info.callId ? ` callId=${info.callId}` : ''
  return (
    `(${OFFLOAD_MARK} tool=${info.toolName}${call} bytes=${info.bytes}` +
    ` path=${info.path}` +
    ` — full output saved; use read with offset/limit on that path if you need more.)`
  )
}

/**
 * Compose preview + notice, shrinking head/tail until the whole replacement
 * fits in `inlineMaxBytes` (best-effort; may return notice-only).
 *
 * @param {string} fullText
 * @param {{
 *   inlineMaxBytes: number,
 *   previewHeadBytes: number,
 *   previewTailBytes: number,
 *   path: string,
 *   toolName: string,
 *   callId?: string,
 * }} opts
 * @returns {string}
 */
export function composeReplacement(fullText, opts) {
  const totalBytes = Buffer.byteLength(fullText, 'utf8')
  let notice = formatOffloadNotice({
    path: opts.path,
    bytes: totalBytes,
    toolName: opts.toolName,
    callId: opts.callId,
  })
  // Tiny caps: shrink notice until it alone fits (path is the critical retrieval handle).
  if (Buffer.byteLength(notice, 'utf8') > opts.inlineMaxBytes) {
    notice = `(${OFFLOAD_MARK} bytes=${totalBytes} path=${opts.path})`
  }
  if (Buffer.byteLength(notice, 'utf8') > opts.inlineMaxBytes) {
    const prefix = `(${OFFLOAD_MARK} bytes=${totalBytes} path=`
    const suffix = ')'
    const room = Math.max(0, opts.inlineMaxBytes - Buffer.byteLength(prefix + suffix, 'utf8'))
    const pathBuf = Buffer.from(opts.path, 'utf8')
    notice = `${prefix}${pathBuf.subarray(0, room).toString('utf8')}${suffix}`
  }
  const noticeBytes = Buffer.byteLength(notice, 'utf8')
  const budget = Math.max(0, opts.inlineMaxBytes - noticeBytes - 2)

  let head = Math.min(opts.previewHeadBytes, Math.ceil(budget * 0.6))
  let tail = Math.min(opts.previewTailBytes, Math.floor(budget * 0.4))
  if (head + tail > budget) {
    head = Math.ceil(budget / 2)
    tail = Math.floor(budget / 2)
  }

  for (let attempt = 0; attempt < 6; attempt++) {
    const { text: preview } = headTailPreview(fullText, head, tail)
    const replaced = preview.length > 0 ? `${preview}\n\n${notice}` : notice
    if (Buffer.byteLength(replaced, 'utf8') <= opts.inlineMaxBytes) return replaced
    head = Math.floor(head / 2)
    tail = Math.floor(tail / 2)
  }
  return notice
}

/**
 * In-place truncate for `read` of an already-offloaded file (no second write).
 * @param {string} fullText
 * @param {{ maxInlineBytes: number, previewHeadBytes: number, previewTailBytes: number }} opts
 * @returns {string}
 */
export function composeInPlaceTruncate(fullText, opts) {
  const totalBytes = Buffer.byteLength(fullText, 'utf8')
  const hint =
    `(${OFFLOAD_MARK} in-place truncate of offload-file read; ` +
    `bytes=${totalBytes} — use read offset/limit for another range; no new offload file.)`
  const hintBytes = Buffer.byteLength(hint, 'utf8')
  const budget = Math.max(0, opts.maxInlineBytes - hintBytes - 2)
  let head = Math.min(opts.previewHeadBytes, Math.ceil(budget * 0.6))
  let tail = Math.min(opts.previewTailBytes, Math.floor(budget * 0.4))
  if (head + tail > budget) {
    head = Math.ceil(budget / 2)
    tail = Math.floor(budget / 2)
  }
  for (let attempt = 0; attempt < 6; attempt++) {
    const { text: preview } = headTailPreview(fullText, head, tail)
    const replaced = preview.length > 0 ? `${preview}\n\n${hint}` : hint
    if (Buffer.byteLength(replaced, 'utf8') <= opts.maxInlineBytes) return replaced
    head = Math.floor(head / 2)
    tail = Math.floor(tail / 2)
  }
  return hint
}

/**
 * Persist full text under `<offloadRoot>/<sessionHash>/<id>-<tool>.txt`.
 *
 * @param {{
 *   offloadRoot: string,
 *   sessionId: string,
 *   toolName: string,
 *   callId?: string,
 *   content: string,
 * }} req
 * @returns {Promise<{ path: string, bytes: number }>}
 */
export async function saveOffloadFile(req) {
  const dir = join(req.offloadRoot, sessionDirName(req.sessionId))
  await mkdir(dir, { recursive: true, mode: 0o700 })
  const id = randomBytes(6).toString('hex')
  const name = `${id}-${safeSegment(req.toolName)}.txt`
  const path = join(dir, name)
  // Exclusive create — collide → retry once with new id (extremely unlikely).
  let handle
  try {
    handle = await open(path, 'wx', 0o600)
  } catch (err) {
    if (/** @type {NodeJS.ErrnoException} */ (err).code === 'EEXIST') {
      const path2 = join(dir, `${randomBytes(6).toString('hex')}-${safeSegment(req.toolName)}.txt`)
      await writeFile(path2, req.content, { encoding: 'utf8', mode: 0o600, flag: 'wx' })
      return { path: path2, bytes: Buffer.byteLength(req.content, 'utf8') }
    }
    throw err
  }
  try {
    await handle.writeFile(req.content, 'utf8')
  } finally {
    await handle.close()
  }
  return { path, bytes: Buffer.byteLength(req.content, 'utf8') }
}

/**
 * Effective inline cap for a tool (per-tool override wins).
 * @param {ReturnType<typeof normalizeConfig>} cfg
 * @param {string} toolName
 * @returns {number}
 */
export function effectiveInlineMax(cfg, toolName) {
  const ov = cfg.toolOverrides.get(toolName)
  if (ov && ov.inlineMaxBytes !== undefined) return ov.inlineMaxBytes
  return cfg.inlineMaxBytes
}

/**
 * Decide whether / how to transform a plain-text tool result.
 * Pure decision + optional async save — returns replacement text or undefined.
 *
 * @param {{
 *   toolName: string,
 *   callId?: string,
 *   arguments?: unknown,
 *   text: string,
 *   sessionId: string | undefined,
 *   cfg: ReturnType<typeof normalizeConfig>,
 *   save?: typeof saveOffloadFile,
 * }} input
 * @returns {Promise<string | undefined>}
 */
export async function maybeOffload(input) {
  const { toolName, text, sessionId, cfg } = input
  if (cfg.excludeTools.has(toolName)) return undefined

  const totalBytes = Buffer.byteLength(text, 'utf8')
  const readingOffload =
    toolName === 'read' && isUnderOffloadRoot(readFilePathFromArgs(input.arguments) ?? '', cfg.offloadRoot)

  if (readingOffload) {
    // Never write a second offload file for an offload-path read (loop break).
    if (totalBytes <= cfg.offloadReadMaxInlineBytes) return undefined
    return composeInPlaceTruncate(text, {
      maxInlineBytes: Math.min(cfg.offloadReadMaxInlineBytes, cfg.inlineMaxBytes),
      previewHeadBytes: cfg.previewHeadBytes,
      previewTailBytes: cfg.previewTailBytes,
    })
  }

  const cap = effectiveInlineMax(cfg, toolName)
  if (totalBytes <= cap) return undefined
  if (sessionId === undefined) return undefined

  const save = input.save ?? saveOffloadFile
  const saved = await save({
    offloadRoot: cfg.offloadRoot,
    sessionId,
    toolName,
    callId: input.callId,
    content: text,
  })

  return composeReplacement(text, {
    inlineMaxBytes: cap,
    previewHeadBytes: cfg.previewHeadBytes,
    previewTailBytes: cfg.previewTailBytes,
    path: saved.path,
    toolName,
    callId: input.callId,
  })
}

/* ------------------------------------------------------------------ *
 * Age-based clearing (history-wide, applies to tool_result messages) *
 * ------------------------------------------------------------------ */

/** Extract an already-offloaded path from a replacement notice, if present. */
const OFFLOAD_PATH_RE = /dsh-eager-offload:[^\n]*?path=([^\s)]+)/

/**
 * Recover the offload file path cited by an earlier byte-offload replacement.
 * Returns undefined for untouched text (no marker) — callers then spill lazily.
 *
 * @param {string} text
 * @returns {string | undefined}
 */
export function offloadedPathFromText(text) {
  if (typeof text !== 'string' || !text.includes(OFFLOAD_MARK)) return undefined
  const m = OFFLOAD_PATH_RE.exec(text)
  return m ? m[1] : undefined
}

/**
 * Whether a message is a tool_result carrying only plain-text blocks.
 * Non-text blocks (images/files) make the message ineligible: masking would
 * lose content the plugin cannot re-materialise.
 *
 * @param {any} message
 * @returns {boolean}
 */
export function isPlainTextToolResult(message) {
  if (!message || message.source?.kind !== 'tool') return false
  // Error results carry actionable diagnostics; never age them out.
  if (message.content?.some?.((b) => b?.type === 'tool-result' && b.isError)) return false
  const blocks = message.content
  if (!Array.isArray(blocks) || blocks.length === 0) return false
  for (const block of blocks) {
    if (block?.type !== 'tool-result') return false
    if (!Array.isArray(block.content)) return false
    for (const inner of block.content) {
      if (inner?.type !== 'text' || typeof inner.text !== 'string') return false
    }
  }
  return true
}

/**
 * Collect the plain text of a tool_result message (concatenated inner texts).
 * @param {any} message
 * @returns {string | undefined}
 */
export function toolResultText(message) {
  if (!isPlainTextToolResult(message)) return undefined
  let text = ''
  for (const block of message.content) for (const inner of block.content) text += inner.text
  return text
}

/**
 * Build the placeholder that replaces an aged-out tool_result.
 * Always cites a re-readable path; never re-embeds the full text.
 *
 * @param {{ path?: string, bytes: number, reason: 'offloaded' | 'dropped' }} info
 * @returns {string}
 */
export function formatAgeMaskPlaceholder(info) {
  const bytes = info.bytes
  if (info.path) {
    return `(${OFFLOAD_MARK} age-masked bytes=${bytes} path=${info.path} — old tool result cleared; read that path for the full text)`
  }
  return `(${OFFLOAD_MARK} age-masked bytes=${bytes} — old tool result cleared; re-run the tool or read the session log for the full text)`
}

/**
 * Rewrite one tool_result message's text, rebuilding the frozen chain.
 * @param {any} message
 * @param {string} replacement
 * @returns {any}
 */
function withReplacedText(message, replacement) {
  return {
    ...message,
    content: message.content.map((block) => ({
      ...block,
      content: [{ type: 'text', text: replacement }],
    })),
  }
}

/**
 * Clear text from all but the newest `keepRecentN` tool_result messages.
 *
 * Pure with respect to `messages`: returns a new array (the originals are
 * deep-frozen). Only plain-text, non-error tool results are eligible. Entries
 * already carrying an offload path reuse it; untouched entries are spilled once
 * via `save` (injectable for tests) so the full text stays re-readable.
 *
 * @deprecated ageMask is a no-op on dsh 0.1.5-rc.2 / 0.2.x: `agent/pre-step`
 * only receives this step's newly claimed user messages, never the full history
 * (the request is derived from `session.deriveMessages()`). Retained (and still
 * unit-tested) for reference only; `contextClear` is the replacement and it
 * reuses `offloadedPathFromText` / `isPlainTextToolResult` / `toolResultText`.
 *
 * @param {any[]} messages
 * @param {{ ageMaskEnabled?: boolean, ageMaskKeepRecentN?: number, offloadRoot?: string }} cfg
 * @param {{
 *   sessionId?: string,
 *   save?: (req: { offloadRoot: string, sessionId: string, toolName: string, callId?: string, content: string }) => Promise<{ path: string, bytes: number }>,
 * }} [opts]
 * @returns {Promise<{ messages: any[], masked: number, spilled: number, pathReused: number, placeholderOnly: number }>}
 */
export async function maskOldToolResults(messages, cfg = {}, opts = {}) {
  const unchanged = { messages, masked: 0, spilled: 0, pathReused: 0, placeholderOnly: 0 }
  if (cfg.ageMaskEnabled !== true) return unchanged
  if (!Array.isArray(messages) || messages.length === 0) return unchanged

  const keepRecentN = requireNonNegInt(cfg.ageMaskKeepRecentN, 'ageMaskKeepRecentN', DEFAULT_AGE_MASK_KEEP_RECENT_N)

  const eligible = []
  for (let i = 0; i < messages.length; i++) {
    const text = toolResultText(messages[i])
    if (text !== undefined) eligible.push({ index: i, text })
  }

  const cut = eligible.length - keepRecentN
  if (cut <= 0) return unchanged

  const save = opts.save ?? saveOffloadFile
  const next = messages.slice()
  let masked = 0
  let spilled = 0
  let pathReused = 0
  let placeholderOnly = 0

  for (let e = 0; e < cut; e++) {
    const { index, text } = eligible[e]
    const bytes = Buffer.byteLength(text, 'utf8')
    let path = offloadedPathFromText(text)

    if (path) {
      pathReused++
    } else if (opts.sessionId !== undefined) {
      try {
        const saved = await save({
          offloadRoot: cfg.offloadRoot,
          sessionId: opts.sessionId,
          toolName: messages[index]?.source?.toolName ?? 'tool',
          callId: messages[index]?.source?.callId,
          content: text,
        })
        path = saved.path
        spilled++
      } catch {
        // Offload failure must never block the step: degrade to a bare placeholder.
        path = undefined
        placeholderOnly++
      }
    } else {
      placeholderOnly++
    }

    next[index] = withReplacedText(
      messages[index],
      formatAgeMaskPlaceholder({ path, bytes }),
    )
    masked++
  }

  return { messages: next, masked, spilled, pathReused, placeholderOnly }
}
