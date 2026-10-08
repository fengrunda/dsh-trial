/**
 * dsh-eager-offload — compaction-coupled clearing of old tool results (T2) and
 * collapsing of eligible old steps (T3).
 *
 * Compaction-basic replaces a span of old history with one summary checkpoint,
 * but the results retained *after* the checkpoint (and any step that survived in
 * the retained tail) still ride along on every later request. This module clears
 * those already-superseded results once, right after a pressure compaction
 * commits inside the same `agent/pre-step` waterfall:
 *
 *   - T2 appends the same durable `compaction/prune` + `tool/result`
 *     replacement pair the official `dsh-compaction-tool-result-pruner` uses, so
 *     the content becomes a short head/tail preview + re-readable offload path;
 *   - T3 (this ticket) collapses an eligible *whole step* — an
 *     `assistant/message` with tool calls immediately followed by exactly its
 *     `tool/result` nodes — into one plugin `user/message` via a
 *     `compaction/prune` shadow price + a range `surfaceOp: replace`, dropping
 *     the step's old reasoning (and its tool arguments/results). A step is
 *     eligible when its results are all older than the newest
 *     `keepRecentResults`, none is an error, and either every tool call is one
 *     of `collapseWriteSteps.tools` with a large argument, or (opt-in,
 *     default off) `clearReasoning` is on and the step's thinking text is large
 *     enough.
 *
 * Both passes price the shadowed range through the token meter immediately
 * before the replacement, so the surface-token fold stays exact. The trigger is
 * derived from the durable log, never from in-memory state, so it is
 * restart-safe: the latest `compaction/summary` must have no `step/start` after
 * it (i.e. the compaction happened in this very pre-step) and its checkpoint
 * `user/message` must still be on the surface. Compactions that commit outside
 * the pre-step (overflow retry / manual command) are out of scope by design.
 *
 * No `@deepseek-ai/*` imports: the plugin is symlinked without a node_modules,
 * so every session interaction is duck-typed and every event payload is a plain
 * JSON object (the session deep-freezes it on append).
 *
 * @module dsh-eager-offload/context-clear
 */

import { createHash, randomUUID } from 'node:crypto'

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

/** Marker carried by every collapsed-step notice (idempotence / detection). */
export const CONTEXT_COLLAPSE_MARK = '[dsh-eager-offload:context-collapsed]'

/** Visible assistant text kept when a step is collapsed (JS chars). */
const COLLAPSE_VISIBLE_HEAD_CHARS = 300

/** Raw non-write/edit tool argument preview kept when a step is collapsed (JS chars). */
const COLLAPSE_ARG_PREVIEW_CHARS = 300

/** Per-line preview kept for write/edit argument heads (JS chars). */
const COLLAPSE_LINE_PREVIEW_CHARS = 40

/** Lines kept from the head/tail of a write body. */
const COLLAPSE_WRITE_LINES = 3

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

  // Scan backward from the newest seq so an active tail never walks the whole
  // log: a `step/start` before any summary means the latest compaction is
  // already consumed (restart-safe no-op); the first `compaction/summary` met
  // is the latest one and, by construction, has no `step/start` after it.
  let summarySeq = -1
  for (let seq = total - 1; seq >= 0; seq--) {
    const type = session.eventAt(seq)?.type
    if (type === 'step/start') return undefined
    if (type === 'compaction/summary') {
      summarySeq = seq
      break
    }
  }
  if (summarySeq < 0) return undefined

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

/** Concatenate the assistant message's visible text blocks. */
function assistantVisibleText(message) {
  if (!Array.isArray(message?.content)) return ''
  let text = ''
  for (const block of message.content) {
    if (block?.type === 'text' && typeof block.text === 'string') text += block.text
  }
  return text
}

/** Total JS character length of the assistant message's reasoning/thinking blocks. */
function assistantReasoningChars(message) {
  if (!Array.isArray(message?.content)) return 0
  let chars = 0
  for (const block of message.content) {
    if (block?.type === 'reasoning' && typeof block.text === 'string') chars += block.text.length
  }
  return chars
}

/** Raw character clip (no whitespace collapsing) with an ellipsis marker. */
function clipChars(text, max) {
  if (typeof text !== 'string') return ''
  return text.length <= max ? text : `${text.slice(0, max)}…`
}

/** One-line preview of a line: whitespace collapsed, clipped, deterministic. */
function linePreview(line, max) {
  const collapsed = String(line ?? '').replace(/\s+/g, ' ').trim()
  return collapsed.length <= max ? collapsed : `${collapsed.slice(0, max)}…`
}

/** First/last `n` lines of a write body, each collapsed + clipped. */
function firstLastLines(text, n, lineChars) {
  const lines = String(text).split('\n')
  const head = lines.slice(0, n).map((line) => linePreview(line, lineChars))
  const tail = lines.slice(Math.max(n, lines.length - n)).map((line) => linePreview(line, lineChars))
  return { head, tail }
}

/** Parse a tool call's raw `arguments` JSON string; `{}` on any failure. */
function parsedArguments(raw) {
  if (typeof raw !== 'string' || raw.length === 0) return {}
  try {
    const value = JSON.parse(raw)
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {}
  } catch {
    return {}
  }
}

/** Deterministic summary line for one collapsed tool call. */
function callSummaryLine(call) {
  const name = typeof call?.name === 'string' && call.name.length > 0 ? call.name : 'tool'
  const args = parsedArguments(call?.arguments)
  const path =
    typeof args.file_path === 'string'
      ? args.file_path
      : typeof args.path === 'string'
        ? args.path
        : undefined
  if (name === 'write') {
    const content = typeof args.content === 'string' ? args.content : ''
    const sha = createHash('sha256').update(content, 'utf8').digest('hex').slice(0, 12)
    const { head, tail } = firstLastLines(content, COLLAPSE_WRITE_LINES, COLLAPSE_LINE_PREVIEW_CHARS)
    return (
      `write ${path ?? '?'}: ${content.length} chars, sha256 ${sha}, ` +
      `first ${COLLAPSE_WRITE_LINES} / last ${COLLAPSE_WRITE_LINES} lines ` +
      `${head.join(' | ')} … ${tail.join(' | ')}; file is on disk, re-read by path`
    )
  }
  if (name === 'edit') {
    const oldString = typeof args.old_string === 'string' ? args.old_string : ''
    const newString = typeof args.new_string === 'string' ? args.new_string : ''
    return (
      `edit ${path ?? '?'}: replaced ${oldString.length} chars with ${newString.length} chars; ` +
      `old head: ${linePreview(oldString, COLLAPSE_LINE_PREVIEW_CHARS)}; ` +
      `new head: ${linePreview(newString, COLLAPSE_LINE_PREVIEW_CHARS)}`
    )
  }
  return `${name}(${clipChars(call?.arguments ?? '', COLLAPSE_ARG_PREVIEW_CHARS)})`
}

/**
 * Match one collapsible step at `surface[index]`: an `assistant/message` with
 * tool calls immediately followed, contiguously, by exactly its tool/result
 * nodes (one per call, in call order, nothing interleaved).
 *
 * @returns {{ assistantSeq:number, assistantMessage:any, calls:any[],
 *   resultSeqs:number[], endSeq:number, endIndex:number } | undefined}
 */
function matchCollapsibleStep(session, surface, index) {
  const assistantSeq = surface[index]
  const event = session.eventAt(assistantSeq)
  if (event?.type !== 'assistant/message') return undefined
  const message = event.data?.message
  const blocks = Array.isArray(message?.content) ? message.content : []
  const calls = blocks.filter((block) => block?.type === 'tool-call')
  if (calls.length === 0) return undefined
  const callIds = calls.map((call) => (typeof call.id === 'string' ? call.id : String(call.id)))
  if (callIds.some((id) => id.length === 0)) return undefined

  const resultSeqs = []
  let cursor = index + 1
  for (const callId of callIds) {
    const seq = surface[cursor]
    if (seq === undefined) return undefined
    const resultEvent = session.eventAt(seq)
    if (resultEvent?.type !== 'tool/result') return undefined
    const resultCallId = resultEvent.data?.message?.source?.callId
    if (String(resultCallId) !== callId) return undefined
    resultSeqs.push(seq)
    cursor += 1
  }
  return {
    assistantSeq,
    assistantMessage: message,
    calls,
    resultSeqs,
    endSeq: resultSeqs[resultSeqs.length - 1],
    endIndex: cursor - 1,
  }
}

/** The assistant step's `turn`/`step` identity, or the failed lookup marker. */
function stepIdentity(session, assistantSeq) {
  const event = session.eventAt(assistantSeq)
  const turn = event?.data?.turn
  const step = event?.data?.step
  return `${turn ?? '?'}/${step ?? '?'}`
}

/**
 * Build the replacement text for one matched step. Returns `undefined` when a
 * needed offload file cannot be persisted (the caller then skips the whole
 * step). Deterministic apart from nothing — no timestamps, no randomness.
 */
async function buildCollapsedText(session, step, ctx) {
  const { minResultBytes, offloadRoot, sessionId, placeholderBytes, logger } = ctx
  const parts = [
    `${CONTEXT_COLLAPSE_MARK} earlier step ${stepIdentity(session, step.assistantSeq)} ` +
      `collapsed after compaction (reasoning omitted).`,
  ]
  const visible = assistantVisibleText(step.assistantMessage)
  if (visible.trim().length > 0) parts.push(clipChars(visible, COLLAPSE_VISIBLE_HEAD_CHARS))
  for (const call of step.calls) parts.push(callSummaryLine(call))

  for (const seq of step.resultSeqs) {
    const event = session.eventAt(seq)
    const message = event?.data?.message
    const text = toolResultText(message)
    if (typeof text !== 'string') return undefined
    const bytes = Buffer.byteLength(text, 'utf8')
    if (bytes < minResultBytes) {
      parts.push(text)
      continue
    }
    const toolName = resolveToolName(session, event)
    const callId = message?.source?.callId
    let path = offloadedPathFromText(text)
    if (path === undefined) {
      if (sessionId === undefined) {
        logger?.warn?.(
          `dsh-eager-offload: context-collapse cannot persist seq ${seq} without a session id; keeping step`,
        )
        return undefined
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
          `dsh-eager-offload: context-collapse could not persist seq ${seq}: ${String(error)}; keeping step`,
        )
        return undefined
      }
    }
    parts.push(
      buildContextClearPlaceholder(text, {
        toolName,
        bytes,
        path,
        headBytes: placeholderBytes.headBytes,
        tailBytes: placeholderBytes.tailBytes,
        failTailBytes: placeholderBytes.failTailBytes,
      }),
    )
  }
  return parts.join('\n')
}

/** Estimate the covered nodes (assistant + each result) through the token meter. */
function estimateCovered(session, tokenMeter, assistantSeq, resultSeqs) {
  let total = 0
  for (const seq of [assistantSeq, ...resultSeqs]) {
    const event = session.eventAt(seq)
    const message = session.deriveEventMessage ? session.deriveEventMessage(event) : event?.data?.message
    if (message) total += tokenMeter.estimateMessage(message)
  }
  return total
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
 *   collapsedSteps: number,
 *   collapsedByReason: { write: number, reasoning: number },
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
  const result = {
    triggered: false,
    cleared: 0,
    collapsedSteps: 0,
    collapsedByReason: { write: 0, reasoning: 0 },
    bytesBefore: 0,
    bytesAfter: 0,
    skipped,
  }

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
  const placeholderBytes = { headBytes, tailBytes, failTailBytes }

  const collapseWriteSteps = cc.collapseWriteSteps ?? {}
  const clearReasoning = cc.clearReasoning ?? {}
  const writeTools = new Set(Array.isArray(collapseWriteSteps.tools) ? collapseWriteSteps.tools : [])
  const collapseWriteEnabled = collapseWriteSteps.enabled !== false && writeTools.size > 0
  const minArgChars = Number.isInteger(collapseWriteSteps.minArgChars) ? collapseWriteSteps.minArgChars : 0
  const clearReasoningEnabled = clearReasoning.enabled === true
  const minReasoningChars = Number.isInteger(clearReasoning.minReasoningChars)
    ? clearReasoning.minReasoningChars
    : 0

  // Snapshot the surface once: every replacement below targets an original seq.
  const surface = [...session.surface.nodes]
  const checkpointIdx = surface.indexOf(checkpoint.checkpointSeq)
  const allResults = surface.filter((seq) => session.eventAt(seq)?.type === 'tool/result')
  const kept = new Set(allResults.slice(Math.max(0, allResults.length - keepRecent)))

  // --- T3: collapse eligible steps, before the T2 tool-result pass --------
  if (collapseWriteEnabled || clearReasoningEnabled) {
    for (let index = checkpointIdx + 1; index < surface.length; index++) {
      const step = matchCollapsibleStep(session, surface, index)
      if (step === undefined) continue
      index = step.endIndex
      const resultEvents = step.resultSeqs.map((seq) => session.eventAt(seq))
      const isError = resultEvents.some((event) =>
        event?.data?.message?.content?.some?.((block) => block?.type === 'tool-result' && block.isError === true),
      )
      if (isError || step.resultSeqs.some((seq) => kept.has(seq))) continue
      if (!resultEvents.every((event) => isPlainTextToolResult(event?.data?.message))) continue

      const byWrite =
        collapseWriteEnabled &&
        step.calls.every((call) => writeTools.has(call?.name)) &&
        step.calls.some(
          (call) => typeof call?.arguments === 'string' && call.arguments.length >= minArgChars,
        )
      const byReasoning =
        !byWrite &&
        clearReasoningEnabled &&
        assistantReasoningChars(step.assistantMessage) >= minReasoningChars
      const reason = byWrite ? 'write' : byReasoning ? 'reasoning' : undefined
      if (reason === undefined) continue

      const text = await buildCollapsedText(session, step, {
        minResultBytes,
        offloadRoot,
        sessionId,
        placeholderBytes,
        logger,
      })
      if (text === undefined) continue

      const collapsedMessage = {
        id: randomUUID(),
        role: 'user',
        content: [{ type: 'text', text }],
        source: {
          kind: 'plugin',
          plugin: 'dsh-eager-offload',
          form: 'notice',
          summary: clipChars(
            `context-collapsed step ${stepIdentity(session, step.assistantSeq)} (${reason})`,
            120,
          ),
        },
      }
      const coveredEstimate = estimateCovered(session, tokenMeter, step.assistantSeq, step.resultSeqs)
      if (tokenMeter.estimateMessage(collapsedMessage) >= coveredEstimate) continue

      let coveredBytes = 0
      for (const seq of [step.assistantSeq, ...step.resultSeqs]) {
        coveredBytes += Buffer.byteLength(JSON.stringify(session.eventAt(seq)?.data?.message ?? ''), 'utf8')
      }
      session.append('compaction/prune', {
        shadowedRange: { start: step.assistantSeq, end: step.endSeq },
        shadowedSeqs: [step.assistantSeq, ...step.resultSeqs],
        shadowedTokenCount: coveredEstimate,
      })
      session.append('user/message', collapsedMessage, {
        surfaceOp: { op: 'replace', startSeq: step.assistantSeq, endSeq: step.endSeq },
        sourceEventSeqs: [step.assistantSeq, ...step.resultSeqs],
      })
      result.collapsedSteps += 1
      result.collapsedByReason[reason] += 1
      result.bytesBefore += coveredBytes
      result.bytesAfter += Buffer.byteLength(text, 'utf8')
    }
  }

  // --- T2: clear old tool results on a FRESH surface snapshot -------------
  const surfaceAfter = [...session.surface.nodes]
  const checkpointIdxAfter = surfaceAfter.indexOf(checkpoint.checkpointSeq)
  const allResultsAfter = surfaceAfter.filter((seq) => session.eventAt(seq)?.type === 'tool/result')
  const keptAfter = new Set(allResultsAfter.slice(Math.max(0, allResultsAfter.length - keepRecent)))

  for (const seq of surfaceAfter.slice(checkpointIdxAfter + 1)) {
    const event = session.eventAt(seq)
    if (event?.type !== 'tool/result') continue
    if (keptAfter.has(seq)) {
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
