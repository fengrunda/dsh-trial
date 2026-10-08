/**
 * dsh-eager-offload — compaction-coupled clearing of old tool results (T2).
 *
 * Compaction-basic replaces a span of old history with one summary checkpoint,
 * but the results retained *after* the checkpoint (and any result that survived
 * in the retained tail) still ride along on every later request. This module
 * clears those already-superseded results once, right after a pressure
 * compaction commits inside the same `agent/pre-step` waterfall, by appending
 * the same durable `compaction/prune` + `tool/result` replacement pair the
 * official `dsh-compaction-tool-result-pruner` uses:
 *
 *   - `compaction/prune` prices the shadowed node through the token meter;
 *   - `tool/result` with `surfaceOp: replace` over exactly that node swaps the
 *     content for a short head/tail preview + re-readable offload path.
 *
 * The trigger is derived from the durable log, never from in-memory state, so it
 * is restart-safe: the latest `compaction/summary` must have no `step/start`
 * after it (i.e. the compaction happened in this very pre-step) and its
 * checkpoint `user/message` must still be on the surface. Compactions that
 * commit outside the pre-step (overflow retry / manual command) are out of
 * scope by design.
 *
 * No `@deepseek-ai/*` imports: the plugin is symlinked without a node_modules,
 * so every session interaction is duck-typed and every event payload is a plain
 * JSON object (the session deep-freezes it on append).
 *
 * @module dsh-eager-offload/context-clear
 */

import {
  headTailPreview,
  isPlainTextToolResult,
  offloadedPathFromText,
  resolveOffloadRoot,
  safeSegment,
  saveOffloadFile,
  toolResultText,
} from './offload.js'

/** Marker carried by every context-clear placeholder (idempotence / detection). */
export const CONTEXT_CLEAR_MARK = '[dsh-eager-offload:context-cleared]'

/**
 * Documented failure heuristic for the shell result format: both the bash and
 * pwsh tools terminate a failed result with a bare `[exit code: N]` marker on
 * the last line, N != 0. Anything else (including a zero exit) is treated as a
 * success, which selects the shorter default tail rather than `failTailBytes`.
 */
const FAIL_EXIT_MARKER_RE = /\n\[exit code: (?:[1-9]\d*)\]\s*$/

/**
 * Whether a tool-result text ends with a non-zero shell exit marker.
 * @param {string} text
 * @returns {boolean}
 */
export function isFailedCommandText(text) {
  return typeof text === 'string' && FAIL_EXIT_MARKER_RE.test(text)
}

/**
 * Build the deterministic first line of a context-clear placeholder.
 * @param {{ toolName: string, bytes: number, path: string }} info
 * @returns {string}
 */
export function contextClearNotice(info) {
  return (
    `${CONTEXT_CLEAR_MARK} ${info.toolName} result (${info.bytes} bytes) cleared after compaction; ` +
    `full text at ${info.path} — re-read it with the read tool (use offset/limit for large files).`
  )
}

/**
 * Compose the replacement text: one notice line, then a UTF-8-safe head/tail
 * preview. Failed commands keep a larger tail so the diagnostic stays visible.
 * Deterministic — no timestamps, no randomness.
 *
 * @param {string} text - full original text.
 * @param {{
 *   toolName: string,
 *   bytes: number,
 *   path: string,
 *   headBytes: number,
 *   tailBytes: number,
 *   failTailBytes: number,
 * }} opts
 * @returns {string}
 */
export function buildContextClearPlaceholder(text, opts) {
  const failed = isFailedCommandText(text)
  const tail = failed ? opts.failTailBytes : opts.tailBytes
  const { text: preview } = headTailPreview(text, opts.headBytes, tail)
  const notice = contextClearNotice({ toolName: opts.toolName, bytes: opts.bytes, path: opts.path })
  return preview.length > 0 ? `${notice}\n${preview}` : notice
}

/**
 * Find the latest compaction whose checkpoint is still the live surface tail.
 *
 * Returns `undefined` unless the newest `compaction/summary` has (a) no
 * `step/start` at a later seq and (b) an immediately following checkpoint
 * `user/message` (the compaction plugin's shape) that is present on the
 * current surface.
 *
 * @param {any} session
 * @returns {{ summarySeq: number, checkpointSeq: number } | undefined}
 */
export function liveCompactionCheckpoint(session) {
  if (!session || typeof session.eventAt !== 'function' || session.seq === undefined) return undefined
  const total = Number(session.seq)
  if (!Number.isInteger(total) || total <= 0) return undefined

  let summarySeq = -1
  for (let seq = 0; seq < total; seq++) {
    if (session.eventAt(seq)?.type === 'compaction/summary') summarySeq = seq
  }
  if (summarySeq < 0) return undefined

  for (let seq = summarySeq + 1; seq < total; seq++) {
    if (session.eventAt(seq)?.type === 'step/start') return undefined
  }

  const checkpointSeq = summarySeq + 1
  const checkpoint = session.eventAt(checkpointSeq)
  if (checkpoint?.type !== 'user/message') return undefined
  if (checkpoint.data?.source?.plugin !== 'compact') return undefined
  if (typeof session.surface?.nodes?.includes !== 'function') return undefined
  if (!session.surface.nodes.includes(checkpointSeq)) return undefined
  return { summarySeq, checkpointSeq }
}

/**
 * Resolve the tool name for a result: prefer an explicit `source.toolName`,
 * else the matching earlier `tool/call` event, else `'tool'`.
 *
 * @param {any} session
 * @param {any} event - the `tool/result` event.
 * @returns {string}
 */
function resolveToolName(session, event) {
  const direct = event?.data?.message?.source?.toolName
  if (typeof direct === 'string' && direct.length > 0) return direct
  const callId = event?.data?.message?.source?.callId
  if (callId !== undefined) {
    for (let seq = Number(event.seq) - 1; seq >= 0; seq--) {
      const candidate = session.eventAt(seq)
      if (candidate?.type === 'tool/call' && candidate.data?.callId === callId) {
        const name = candidate.data?.name
        return typeof name === 'string' && name.length > 0 ? name : 'tool'
      }
    }
  }
  return 'tool'
}

/**
 * Clear old tool results that survived the compaction checkpoint.
 *
 * @param {any} session - live `Session` (duck-typed).
 * @param {{
 *   cfg?: any,
 *   tokenMeter?: { estimateMessage: (message: any) => number },
 *   logger?: { warn?: (m: string) => void },
 *   sessionId?: string,
 * }} options
 *   `cfg` is the full normalized config (or, for direct callers, the
 *   `contextClear` block itself); `offloadRoot` is read from it when present.
 * @returns {Promise<{
 *   triggered: boolean,
 *   cleared: number,
 *   bytesBefore: number,
 *   bytesAfter: number,
 *   skipped: Record<string, number>,
 * }>}
 */
export async function clearAfterCompaction(session, { cfg, tokenMeter, logger, sessionId } = {}) {
  /** @type {Record<string, number>} */
  const skipped = {}
  /** @param {string} reason */
  const skip = (reason) => {
    skipped[reason] = (skipped[reason] ?? 0) + 1
  }
  const result = { triggered: false, cleared: 0, bytesBefore: 0, bytesAfter: 0, skipped }

  if (!session || typeof session.surface?.nodes?.includes !== 'function') return result

  const full = cfg && typeof cfg === 'object' ? cfg : {}
  const cc = full.contextClear && typeof full.contextClear === 'object' ? full.contextClear : full
  if (cc.enabled !== true) return result
  if (cc.mode !== undefined && cc.mode !== 'compaction-coupled') return result

  const checkpoint = liveCompactionCheckpoint(session)
  if (checkpoint === undefined) return result
  result.triggered = true

  if (!tokenMeter || typeof tokenMeter.estimateMessage !== 'function') {
    skip('tokenMeterMissing')
    return result
  }

  const offloadRoot = typeof full.offloadRoot === 'string' ? full.offloadRoot : resolveOffloadRoot(undefined)
  const keepRecent = Number.isInteger(cc.keepRecentResults) ? cc.keepRecentResults : 0
  const minResultBytes = Number.isInteger(cc.minResultBytes) ? cc.minResultBytes : 0
  const placeholder = cc.placeholder ?? {}
  const headBytes = Number.isInteger(placeholder.headBytes) ? placeholder.headBytes : 0
  const tailBytes = Number.isInteger(placeholder.tailBytes) ? placeholder.tailBytes : 0
  const failTailBytes = Number.isInteger(placeholder.failTailBytes) ? placeholder.failTailBytes : tailBytes

  // Snapshot the surface once: every replacement below targets an original seq.
  const surface = [...session.surface.nodes]
  const checkpointIdx = surface.indexOf(checkpoint.checkpointSeq)

  const allResults = surface.filter((seq) => session.eventAt(seq)?.type === 'tool/result')
  const kept = new Set(allResults.slice(Math.max(0, allResults.length - keepRecent)))

  for (const seq of surface.slice(checkpointIdx + 1)) {
    const event = session.eventAt(seq)
    if (event?.type !== 'tool/result') continue
    if (kept.has(seq)) {
      skip('keepRecent')
      continue
    }
    const message = event.data?.message
    if (message?.content?.some?.((block) => block?.type === 'tool-result' && block.isError === true)) {
      skip('isError')
      continue
    }
    if (!isPlainTextToolResult(message)) {
      skip('notPlainText')
      continue
    }
    const text = toolResultText(message)
    if (typeof text !== 'string') {
      skip('notPlainText')
      continue
    }
    const bytes = Buffer.byteLength(text, 'utf8')
    if (text.includes(CONTEXT_CLEAR_MARK)) {
      skip('alreadyCleared')
      continue
    }
    if (bytes < minResultBytes) {
      skip('tooSmall')
      continue
    }

    const toolName = resolveToolName(session, event)
    const callId = message?.source?.callId
    let path = offloadedPathFromText(text)
    if (path === undefined) {
      if (sessionId === undefined) {
        skip('noSessionForSave')
        continue
      }
      try {
        const saved = await saveOffloadFile({
          offloadRoot,
          sessionId,
          toolName,
          callId,
          content: text,
          name: callId !== undefined ? safeSegment(callId) : undefined,
        })
        path = saved.path
      } catch (error) {
        logger?.warn?.(
          `dsh-eager-offload: context-clear could not persist seq ${seq}: ${String(error)}; keeping inline`,
        )
        skip('saveFailed')
        continue
      }
    }

    const replacement = buildContextClearPlaceholder(text, {
      toolName,
      bytes,
      path,
      headBytes,
      tailBytes,
      failTailBytes,
    })
    const afterBytes = Buffer.byteLength(replacement, 'utf8')
    if (afterBytes >= bytes) {
      skip('notSmaller')
      continue
    }

    const originalResult = message.content[0]
    const replacedMessage = {
      ...message,
      content: [{ ...originalResult, content: [{ type: 'text', text: replacement }] }],
    }
    session.append('compaction/prune', {
      shadowedRange: { start: seq, end: seq },
      shadowedSeqs: [seq],
      shadowedTokenCount: tokenMeter.estimateMessage(message),
    })
    session.append(
      'tool/result',
      { ...event.data, message: replacedMessage },
      {
        surfaceOp: { op: 'replace', startSeq: seq, endSeq: seq },
        sourceEventSeqs: [seq],
      },
    )

    result.cleared += 1
    result.bytesBefore += bytes
    result.bytesAfter += afterBytes
  }

  return result
}
