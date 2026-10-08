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

/** Age-based clearing is opt-in: product profiles keep every tool_result in full. */
export const DEFAULT_AGE_MASK_ENABLED = false

/** Newest N tool_result messages kept verbatim once age-mask is enabled. */
export const DEFAULT_AGE_MASK_KEEP_RECENT_N = 8

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
 *   ageMaskEnabled: boolean,
 *   ageMaskKeepRecentN: number,
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
