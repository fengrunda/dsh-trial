// Extract per-session timelines from real dsh session logs (read-only).
//
// Mirrors /tmp/tokaudit/agemask-sim/extract.js: only `provider=deepseek-official`
// assistant steps become requests; everything appended between two requests is
// attributed to the later request as `pre` items (tool results + user messages).

import { jsonLen } from './util.mjs'

/**
 * @param {string} file session.v3.jsonl.zstd path
 * @param {any[]} events parsed JSONL events
 * @returns {null | {file:string,home:string,sysChars:number,toolChars:number,
 *   preUserChars:number,compactions:any[],steps:any[]}}
 */
export function extractSession(file, events) {
  const items = []
  const steps = []
  const compactions = []
  const callTool = new Map()
  let sysChars = 0
  let toolChars = 0
  let preUserChars = 0
  let pending = []

  for (const event of events) {
    const type = event.type
    if (type === 'system/message' && event.surfaceOp === 'append') {
      sysChars += jsonLen(event.data?.message?.content)
    }
    if (type === 'request/header' && event.data?.header?.tools) {
      toolChars = jsonLen(event.data.header.tools)
    }
    if (type === 'user/message' && event.surfaceOp === 'append') {
      const chars = jsonLen(event.data?.content)
      if (steps.length === 0) preUserChars += chars
      else pending.push({ kind: 'user', chars, src: event.data?.source?.kind })
    }
    if (type === 'tool/call') {
      callTool.set(event.data?.callId, event.data?.name)
    }
    if (type === 'tool/result' && event.surfaceOp === 'append') {
      const message = event.data?.message
      const block = message?.content?.[0]
      const inner = block?.content ?? []
      const text = inner.map((b) => b.text || '').join('')
      const plain = inner.every((b) => b.type === 'text')
      const callId = message?.source?.callId
      pending.push({
        kind: 'result',
        chars: jsonLen(block?.content),
        textChars: text.length,
        tool: callTool.get(callId) || '?',
        callId,
        isError: !!block?.isError,
        plain,
        offloaded: text.includes('dsh-eager-offload:'),
        exitNonZero: /\[exit code: [1-9]/.test(text),
        failHint: /(FAIL|Error|Traceback|error:|failed)/.test(text),
      })
    }
    if (typeof type === 'string' && type.startsWith('compaction/summary')) {
      compactions.push({
        beforeStep: steps.length,
        shadowed: event.data?.shadowedTokenCount,
        usage: event.data?.usage,
      })
    }
    if (type === 'assistant/message' && event.data?.usage && !event.data.interrupted) {
      const source = event.data.message?.source || {}
      if (source.provider !== 'deepseek-official') continue
      const usage = event.data.usage
      const blocks = event.data.message?.content ?? []
      let rc = 0
      let tc = 0
      const args = []
      for (const block of blocks) {
        if (block.type === 'reasoning') rc += jsonLen(block.text)
        else if (block.type === 'text') tc += jsonLen(block.text)
        else if (block.type === 'tool-call') args.push({ tool: block.name, chars: jsonLen(block.arguments), id: block.id })
      }
      steps.push({
        t: event.time,
        miss: usage.inputTokens || 0,
        hit: usage.cacheReadTokens || 0,
        out: usage.outputTokens || 0,
        reasoningTokens: usage.reasoningTokens || 0,
        P: (usage.inputTokens || 0) + (usage.cacheReadTokens || 0),
        pre: pending,
        asst: { rc, tc, args },
      })
      pending = []
    }
  }

  if (!steps.length) return null
  const parts = file.split('/')
  const home = parts.includes('.dsh-homes') ? parts[parts.indexOf('.dsh-homes') + 1] : (parts[4] ?? '?')
  return { file, home, sysChars, toolChars, preUserChars, compactions, steps }
}
