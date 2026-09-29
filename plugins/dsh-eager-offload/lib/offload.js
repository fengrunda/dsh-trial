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

/** Marker embedded in every replacement notice (loop / composition detection). */
export const OFFLOAD_MARK = 'dsh-eager-offload:'

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
 *   excludeTools: Set<string>,
 *   toolOverrides: Map<string, { inlineMaxBytes?: number }>,
 * }}
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
