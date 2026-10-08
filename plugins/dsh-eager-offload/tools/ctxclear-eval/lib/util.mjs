// Shared utilities for the offline contextClear replay tool.
// Pure Node (>=22), no third-party dependencies.

import fs from 'node:fs'
import path from 'node:path'
import zlib from 'node:zlib'

/** JSON-ish string length used by the prototype for token attribution. */
export function jsonLen(value) {
  return typeof value === 'string' ? value.length : JSON.stringify(value ?? '').length
}

// ---------------------------------------------------------------------------
// Minimal glob (supports braces, `**`, `*`, `?`) — no dependency, no warnings.
// ---------------------------------------------------------------------------

function expandBraces(pattern) {
  const open = pattern.indexOf('{')
  if (open < 0) return [pattern]
  const close = pattern.indexOf('}', open)
  if (close < 0) return [pattern]
  const head = pattern.slice(0, open)
  const body = pattern.slice(open + 1, close)
  const tail = pattern.slice(close + 1)
  const out = []
  for (const option of body.split(',')) {
    for (const rest of expandBraces(tail)) out.push(head + option + rest)
  }
  return out
}

function segmentToRegExp(segment) {
  let re = ''
  for (const ch of segment) {
    if (ch === '*') re += '[^/]*'
    else if (ch === '?') re += '[^/]'
    else re += ch.replace(/[.+^${}()|[\]\\]/g, '\\$&')
  }
  return new RegExp(`^${re}$`)
}

function hasGlobChars(segment) {
  return segment.includes('*') || segment.includes('?')
}

function readdirSafe(dir) {
  try {
    return fs.readdirSync(dir, { withFileTypes: true })
  } catch {
    return []
  }
}

function walkGlob(parts, index, dir, out) {
  if (index >= parts.length) {
    if (isFile(dir)) out.push(dir)
    return
  }
  const segment = parts[index]
  const last = index === parts.length - 1
  if (segment === '**') {
    walkGlob(parts, index + 1, dir, out)
    for (const entry of readdirSafe(dir)) {
      if (entry.isDirectory()) walkGlob(parts, index, path.join(dir, entry.name), out)
    }
    return
  }
  if (!hasGlobChars(segment)) {
    const next = path.join(dir, segment)
    if (last) {
      if (isFile(next)) out.push(next)
    } else if (isDir(next)) {
      walkGlob(parts, index + 1, next, out)
    }
    return
  }
  const re = segmentToRegExp(segment)
  for (const entry of readdirSafe(dir)) {
    if (!re.test(entry.name)) continue
    const next = path.join(dir, entry.name)
    if (last) {
      if (entry.isFile()) out.push(next)
    } else if (entry.isDirectory()) {
      walkGlob(parts, index + 1, next, out)
    }
  }
}

function isFile(p) {
  try {
    return fs.statSync(p).isFile()
  } catch {
    return false
  }
}

function isDir(p) {
  try {
    return fs.statSync(p).isDirectory()
  } catch {
    return false
  }
}

/**
 * Expand a glob (absolute or relative to `cwd`) into a sorted, unique file list.
 * Supports `{a,b}` braces, `**`, `*` and `?`.
 */
export function globFiles(pattern, cwd = process.cwd()) {
  const out = []
  for (const expanded of expandBraces(pattern)) {
    const absolute = path.isAbsolute(expanded) ? expanded : path.resolve(cwd, expanded)
    const parts = absolute.split(path.sep).filter(Boolean)
    walkGlob(parts, 0, path.parse(absolute).root, out)
  }
  return [...new Set(out)].sort()
}

// ---------------------------------------------------------------------------
// Multi-frame zstd session logs.
//
// dsh appends one zstd frame per write, so `zlib.zstdDecompressSync` on the
// whole file only decodes the first frame. We walk the frames explicitly using
// the zstd frame header + block headers, then decompress each frame.
// ---------------------------------------------------------------------------

const ZSTD_MAGIC = 0xfd2fb528

/** Byte length of the zstd frame starting at `offset`, or -1 if malformed. */
export function zstdFrameLength(raw, offset = 0) {
  let p = offset
  if (p + 4 > raw.length || raw.readUInt32LE(p) !== ZSTD_MAGIC) return -1
  p += 4
  const descriptor = raw[p]
  p += 1
  const fcsFlag = (descriptor >> 6) & 3
  const singleSegment = (descriptor >> 5) & 1
  const checksum = (descriptor >> 2) & 1
  const dictFlag = descriptor & 3
  if (!singleSegment) p += 1
  p += [0, 1, 2, 4][dictFlag]
  p += fcsFlag === 0 ? (singleSegment ? 1 : 0) : [0, 2, 4, 8][fcsFlag]
  for (;;) {
    if (p + 3 > raw.length) return -1
    const header = raw[p] | (raw[p + 1] << 8) | (raw[p + 2] << 16)
    p += 3
    const last = header & 1
    const blockType = (header >> 1) & 3
    const blockSize = header >> 3
    if (blockType === 0) p += blockSize
    else if (blockType === 1) p += 1
    else if (blockType === 2) p += blockSize
    else return -1
    if (p > raw.length) return -1
    if (last) break
  }
  if (checksum) p += 4
  return p > raw.length ? -1 : p - offset
}

/** Split a raw multi-frame zstd buffer into individual frame buffers. */
export function splitZstdFrames(raw) {
  const frames = []
  let p = 0
  while (p < raw.length) {
    const len = zstdFrameLength(raw, p)
    if (len <= 0) throw new Error(`malformed zstd frame at byte ${p}`)
    frames.push(raw.subarray(p, p + len))
    p += len
  }
  return frames
}

/**
 * Read a (multi-frame) session.v3.jsonl.zstd into parsed JSONL events.
 *
 * When `shouldStop` is supplied it is called with the events parsed so far and
 * may return true to stop early (used to skip sessions that never enter the
 * requested window without decompressing the rest of the file).
 */
export function readSessionEvents(file, shouldStop) {
  const raw = fs.readFileSync(file)
  const events = []
  let pending = ''
  let p = 0
  while (p < raw.length) {
    const len = zstdFrameLength(raw, p)
    if (len <= 0) throw new Error(`malformed zstd frame at byte ${p} in ${file}`)
    const text = zlib.zstdDecompressSync(raw.subarray(p, p + len)).toString()
    p += len
    pending += text
    let nl
    while ((nl = pending.indexOf('\n')) >= 0) {
      const line = pending.slice(0, nl).trim()
      pending = pending.slice(nl + 1)
      if (line) events.push(JSON.parse(line))
    }
    if (shouldStop && shouldStop(events)) return events
  }
  const tail = pending.trim()
  if (tail) events.push(JSON.parse(tail))
  return events
}

// ---------------------------------------------------------------------------
// CLI / time helpers
// ---------------------------------------------------------------------------

/** Parse a timezone token (`Z`, `+08:00`, `-0530`, `+8`) into minutes. */
export function parseTzOffset(token) {
  if (token === undefined || token === null || token === '') return 0
  const t = String(token).trim()
  if (t === 'Z' || t === 'z' || t === 'UTC') return 0
  const m = /^([+-])(\d{1,2})(?::?(\d{2}))?$/.exec(t)
  if (!m) throw new Error(`invalid --tz offset: ${token}`)
  const sign = m[1] === '-' ? -1 : 1
  return sign * (Number(m[2]) * 60 + Number(m[3] ?? 0))
}

function formatOffset(minutes) {
  const sign = minutes < 0 ? '-' : '+'
  const abs = Math.abs(minutes)
  return `${sign}${String(Math.floor(abs / 60)).padStart(2, '0')}:${String(abs % 60).padStart(2, '0')}`
}

/** Build an absolute [start, end) range from `--date`, `HH:MM-HH:MM` and `--tz`. */
export function buildWindowRange(window, date, tzOffsetMinutes) {
  if (!date) throw new Error('--window requires --date (YYYY-MM-DD)')
  const m = /^(\d{2}):(\d{2})-(\d{2}):(\d{2})$/.exec(window)
  if (!m) throw new Error(`invalid --window (expected HH:MM-HH:MM): ${window}`)
  const off = formatOffset(tzOffsetMinutes)
  const start = Date.parse(`${date}T${m[1]}:${m[2]}:00${off}`)
  let end = Date.parse(`${date}T${m[3]}:${m[4]}:00${off}`)
  if (Number.isNaN(start) || Number.isNaN(end)) throw new Error(`invalid --date/--window: ${date} ${window}`)
  if (end <= start) end += 24 * 60 * 60 * 1000 // wrap past midnight
  return { start, end, label: `${date} ${window} ${off}` }
}

/** Parse a `--since`/`--until`/ISO value into epoch ms. */
export function parseTime(value) {
  if (value === undefined || value === null) return undefined
  if (typeof value === 'number') return value
  const s = String(value).trim()
  if (/^\d+$/.test(s)) return Number(s)
  const t = Date.parse(s)
  if (Number.isNaN(t)) throw new Error(`invalid timestamp: ${value}`)
  return t
}

/**
 * Tiny argv parser: `--flag value`, `--flag=value`, repeatable flags, and
 * boolean flags (`--no-collapse`, `--json`). Unknown flags are rejected.
 */
export function parseArgs(argv, spec) {
  const out = {}
  for (const [key, def] of Object.entries(spec)) {
    if (def.repeat) out[key] = []
    else if (def.boolean) out[key] = def.default ?? false
    else out[key] = def.default
  }
  const byFlag = new Map()
  for (const [key, def] of Object.entries(spec)) byFlag.set(def.flag ?? `--${key}`, { key, def })

  for (let i = 0; i < argv.length; i++) {
    const raw = argv[i]
    if (!raw.startsWith('--')) throw new Error(`unexpected argument: ${raw}`)
    const eq = raw.indexOf('=')
    const flag = eq >= 0 ? raw.slice(0, eq) : raw
    const entry = byFlag.get(flag)
    if (!entry) throw new Error(`unknown argument: ${raw}`)
    const { key, def } = entry
    if (def.boolean) {
      const value = eq >= 0 ? raw.slice(eq + 1) !== 'false' : true
      out[key] = value
      continue
    }
    let value
    if (eq >= 0) value = raw.slice(eq + 1)
    else {
      value = argv[++i]
      if (value === undefined) throw new Error(`${flag} requires a value`)
    }
    if (def.repeat) out[key].push(def.parse ? def.parse(value) : value)
    else out[key] = def.parse ? def.parse(value) : value
  }
  return out
}

export function commaList(value) {
  if (!value) return []
  return String(value)
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
}
