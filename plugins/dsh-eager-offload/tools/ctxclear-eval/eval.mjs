#!/usr/bin/env node
// ctxclear-eval — offline replay of contextClear against real dsh session logs.
//
// Pure Node (>=22), no third-party dependencies, no network / LLM calls.
//
//   node tools/ctxclear-eval/eval.mjs \
//     --date 2026-10-08 --window 09:00-10:00 --window 14:00-15:00 --tz +08:00
//
// See tools/ctxclear-eval/README.md for the model and its assumptions.

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import {
  globFiles,
  readSessionEvents,
  buildWindowRange,
  parseTime,
  parseTzOffset,
  parseArgs,
  commaList,
} from './lib/util.mjs'
import { extractSession } from './lib/extract.mjs'
import { DEFAULT_PRICES, normalizePrices } from './lib/prices.mjs'
import { runAll, actualTotals, normalizeClearConfig } from './lib/sim.mjs'
import { buildRows, formatTable, toJSON } from './lib/report.mjs'

const HERE = path.dirname(fileURLToPath(import.meta.url))
const DEFAULT_GLOBS = [
  '/home/box/.dsh-homes/{gate,supervisor,impl}/sessions/**/session.v3.jsonl.zstd',
]

const SPEC = {
  'sessions-glob': { repeat: true, default: [] },
  files: { default: '' },
  since: { default: undefined },
  until: { default: undefined },
  window: { repeat: true, default: [] },
  date: { default: undefined },
  tz: { default: '+08:00' },
  threshold: { default: 60000, parse: Number },
  retain: { default: 20000, parse: Number },
  keep: { default: 8, parse: Number },
  minResultBytes: { flag: '--min-result-bytes', default: 1200, parse: Number },
  noCollapse: { flag: '--no-collapse', boolean: true },
  clearReasoning: { flag: '--clear-reasoning', boolean: true },
  minReasoningChars: { flag: '--min-reasoning-chars', default: 600, parse: Number },
  prices: { default: undefined },
  json: { boolean: true },
  help: { flag: '--help', boolean: true },
}

const HELP = `ctxclear-eval — offline replay of contextClear (T2/T3) vs 60k/20k compaction.

Usage:
  node tools/ctxclear-eval/eval.mjs [options]

Session selection:
  --sessions-glob <glob>   repeatable; default ~/.dsh-homes/{gate,supervisor,impl}/sessions/**/session.v3.jsonl.zstd
  --files <a,b,...>        explicit session.v3.jsonl.zstd paths

Window selection:
  --since <ISO> --until <ISO>       absolute range
  --window 09:00-10:00 --window 14:00-15:00 --date 2026-10-08 --tz +08:00
                                    relative windows (repeatable)
  (no window flags => every request in every session)

Simulation:
  --threshold <tokens>     compaction trigger T (default 60000)
  --retain <tokens>        retained tail R (default 20000)
  --keep <n>               newest tool results protected by contextClear (default 8)
  --min-result-bytes <n>   T2 eligibility floor in bytes (default 1200)
  --no-collapse            disable T3 write/edit step collapse
  --clear-reasoning        also collapse reasoning-heavy steps (any tools; default off)
  --min-reasoning-chars <n> reasoning-text floor for --clear-reasoning (default 600)
  --prices '<json>'        {hit,miss,out} peak rates (off-peak = half) or {peak,offpeak}

Output:
  --json                   machine-readable result
  --help                   this message
`

function fail(message) {
  process.stderr.write(`ctxclear-eval: ${message}\n`)
  process.exit(2)
}

function readPrices(value) {
  if (!value) return DEFAULT_PRICES
  const trimmed = value.trim()
  let text
  if (trimmed.startsWith('{')) text = value
  else if (fs.existsSync(trimmed)) text = fs.readFileSync(trimmed, 'utf8')
  else throw new Error('--prices must be a JSON object or a path to a JSON file')
  return normalizePrices(JSON.parse(text))
}

function resolveRangeEnds(ranges) {
  let maxEnd = -Infinity
  for (const range of ranges) if (Number.isFinite(range.end)) maxEnd = Math.max(maxEnd, range.end)
  return maxEnd
}

function main(argv) {
  let args
  try {
    args = parseArgs(argv, SPEC)
  } catch (error) {
    fail(error.message)
  }
  if (args.help) {
    process.stdout.write(HELP)
    return
  }

  for (const key of ['threshold', 'retain', 'keep', 'minResultBytes', 'minReasoningChars']) {
    const value = args[key]
    if (!Number.isInteger(value) || value < 0) fail(`--${key} must be a non-negative integer`)
  }

  const tzOffset = parseTzOffset(args.tz)
  const ranges = []
  if (args.since !== undefined || args.until !== undefined) {
    ranges.push({
      start: parseTime(args.since) ?? -Infinity,
      end: parseTime(args.until) ?? Infinity,
      label: `${args.since ?? '-inf'} .. ${args.until ?? '+inf'}`,
    })
  }
  for (const window of args.window) {
    ranges.push(buildWindowRange(window, args.date, tzOffset))
  }
  if (!ranges.length) ranges.push({ start: -Infinity, end: Infinity, label: 'all' })
  const maxEnd = resolveRangeEnds(ranges)
  const windowOf = (t) => {
    for (let i = 0; i < ranges.length; i++) if (t >= ranges[i].start && t < ranges[i].end) return i
    return -1
  }

  const files = commaList(args.files)
  const patterns = args['sessions-glob'].length ? args['sessions-glob'] : DEFAULT_GLOBS
  const discovered = files.length ? files : patterns.flatMap((p) => globFiles(p))
  const uniqueFiles = [...new Set(discovered)].sort()
  if (!uniqueFiles.length) fail('no session files matched')

  const sessions = []
  let skipped = 0
  let parseErrors = 0
  for (const file of uniqueFiles) {
    let sawWindow = false
    let seen = 0
    const shouldStop = (events) => {
      for (; seen < events.length; seen++) {
        const event = events[seen]
        if (event.type !== 'assistant/message' || !event.data?.usage || event.data.interrupted) continue
        if (event.data.message?.source?.provider !== 'deepseek-official') continue
        const t = event.time
        if (windowOf(t) >= 0) sawWindow = true
        else if (sawWindow === false && Number.isFinite(maxEnd) && t >= maxEnd) return true
      }
      return false
    }
    let events
    try {
      events = readSessionEvents(file, shouldStop)
    } catch (error) {
      parseErrors++
      process.stderr.write(`ctxclear-eval: skipped ${file}: ${error.message}\n`)
      continue
    }
    const session = extractSession(file, events)
    if (!session || !session.steps.some((step) => windowOf(step.t) >= 0)) {
      skipped++
      continue
    }
    sessions.push(session)
  }

  const windows = ranges.length
  const prices = readPrices(args.prices)
  const requests = sessions.reduce(
    (sum, session) => sum + session.steps.filter((step) => windowOf(step.t) >= 0).length,
    0,
  )

  const baseCfg = { T: args.threshold, R: args.retain, clear: null }
  const clearCfg = normalizeClearConfig({
    keep: args.keep,
    minResultBytes: args.minResultBytes,
    collapse: !args.noCollapse,
    clearReasoning: args.clearReasoning === true,
    minReasoningChars: args.minReasoningChars,
  })

  const actual = actualTotals(sessions, windowOf, windows, prices)
  const calibration = runAll(sessions, { T: 0, R: args.retain, clear: null }, windowOf, windows, prices)
  const base = runAll(sessions, baseCfg, windowOf, windows, prices)
  const contextClear = runAll(sessions, { ...baseCfg, clear: clearCfg }, windowOf, windows, prices)

  const rows = buildRows({
    actual,
    calibration,
    base,
    contextClear,
    config: { threshold: args.threshold, retain: args.retain },
  })
  const meta = {
    generatedAt: new Date().toISOString(),
    windows: ranges.map((r) => r.label),
    tz: args.tz,
    threshold: args.threshold,
    retain: args.retain,
    keep: args.keep,
    minResultBytes: args.minResultBytes,
    collapse: !args.noCollapse,
    clearReasoning: args.clearReasoning === true,
    minReasoningChars: args.minReasoningChars,
    prices,
    sessionsScanned: uniqueFiles.length,
    sessionsWithWindowRequests: sessions.length,
    sessionsSkipped: skipped,
    parseErrors,
    windowRequests: requests,
  }

  if (args.json) process.stdout.write(`${JSON.stringify(toJSON(rows, meta), null, 2)}\n`)
  else {
    process.stdout.write(`# ctxclear-eval ${meta.generatedAt}\n`)
    process.stdout.write(
      `# windows: ${meta.windows.join(', ')} | sessions: ${sessions.length}/${uniqueFiles.length} | requests: ${requests}\n`,
    )
    process.stdout.write(`${formatTable(rows)}\n`)
  }
}

try {
  main(process.argv.slice(2))
} catch (error) {
  fail(error instanceof Error ? error.message : String(error))
}
