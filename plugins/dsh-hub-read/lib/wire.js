/**
 * dsh-hub-read — thin read-only bridge into knowledge-hub `dsh_read_wire`.
 *
 * Pure core (no `@deepseek-ai/dsh-tools` dependency): every Hub call is a
 * `python3 -c` subprocess with `PYTHONPATH=<repo>/src` and cwd = the Hub
 * checkout, invoking the real merged Python entrypoints
 * (`knowledge_hub.dsh_read_wire`). No HTTP, no invented surface.
 *
 * Hard rules (contract: docs/specs/2026-09-30-dsh-hub-read-contract.md):
 *
 * - Bank is pinned to `dsh-dev` (DEV_SCOPE_PLACEHOLDER). Tools never accept a
 *   bank argument; the wire itself rejects any other bank.
 * - Read-only: only `hub_browse` / `hub_resolve` / `hub_neighborhood_read`
 *   are reachable. No write / promote / apply / CSV path exists here.
 * - `hub_neighborhood_read` is default-off at the wire (requires
 *   `enabled=True`); this plugin only registers the tool when the profile
 *   config sets `neighborhoodEnabled: true`, and then always passes
 *   `enabled=True` explicitly.
 * - Misses are structured empties from the wire (`items: []` / `found:false`),
 *   never retried, never faked.
 *
 * @module dsh-hub-read/wire
 */

import { spawn } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { isAbsolute, join } from 'node:path'

/** Pinned Hub scope. The wire hard-rejects anything else. */
export const HUB_BANK = 'dsh-dev'

/** Default Hub checkout (profile overrides via `hubRepoRoot`). */
export const DEFAULT_HUB_REPO_ROOT = '/workspace/hermes-work/knowledge-hub'

/** Default Python interpreter. */
export const DEFAULT_PYTHON_BIN = 'python3'

/** Stable tool names exposed by the wire contract. */
export const TOOL_NAMES = Object.freeze({
  browse: 'hub_browse',
  resolve: 'hub_resolve',
  neighborhood: 'hub_neighborhood_read',
})

/**
 * One-shot Python driver: reads one JSON request `{fn, args, kwargs}` from
 * stdin, calls the wire function, prints one JSON line
 * `{ok:true, result}` / `{ok:false, error, code?}`. Kept tiny so the plugin
 * never drifts from the merged Python signatures.
 */
const WIRE_DRIVER = [
  'import json, sys',
  'from knowledge_hub import dsh_read_wire as w',
  'req = json.load(sys.stdin)',
  'fn = getattr(w, req["fn"])',
  'try:',
  '    result = fn(*req["args"], **req["kwargs"])',
  'except w.DshReadWireError as exc:',
  '    print(json.dumps({"ok": False, "error": str(exc), "code": getattr(exc, "code", None)}, ensure_ascii=False))',
  '    sys.exit(0)',
  'print(json.dumps({"ok": True, "result": result}, ensure_ascii=False))',
].join('\n')

/**
 * Normalize plugin config.
 *
 * @param {Record<string, unknown>} [config]
 * @returns {{ hubRepoRoot: string, pythonBin: string, neighborhoodEnabled: boolean, engineEnvFile: string }}
 */
export function normalizeConfig(config = {}) {
  const record = config && typeof config === 'object' ? config : {}
  const hubRepoRoot =
    typeof record.hubRepoRoot === 'string' && record.hubRepoRoot.trim()
      ? record.hubRepoRoot.trim()
      : DEFAULT_HUB_REPO_ROOT
  const pythonBin =
    typeof record.pythonBin === 'string' && record.pythonBin.trim()
      ? record.pythonBin.trim()
      : DEFAULT_PYTHON_BIN
  const neighborhoodEnabled = record.neighborhoodEnabled === true
  const engineEnvFile =
    typeof record.engineEnvFile === 'string' && record.engineEnvFile.trim()
      ? record.engineEnvFile.trim()
      : ''
  return { hubRepoRoot, pythonBin, neighborhoodEnabled, engineEnvFile }
}

/**
 * Invoke one wire function in a subprocess. Never throws: failures come back
 * as `{ ok: false, error }` so a tool call renders an honest error instead of
 * crashing the registry.
 *
 * @param {{ hubRepoRoot: string, pythonBin: string, fn: string, args?: unknown[], kwargs?: Record<string, unknown>, spawnImpl?: typeof spawn }} options
 * @returns {Promise<{ ok: boolean, result?: unknown, error?: string, code?: string|null }>}
 */

/**
 * KEY=VALUE lines for the Hub python subprocess only. Missing file is empty.
 * Values are never logged.
 * @param {string} filePath
 * @returns {Record<string, string>}
 */
export function readEngineEnvFile(filePath) {
  if (!filePath) return {}
  let text
  try {
    text = readFileSync(filePath, 'utf8')
  } catch {
    return {}
  }
  /** @type {Record<string, string>} */
  const out = {}
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const eq = line.indexOf('=')
    if (eq <= 0) continue
    const key = line.slice(0, eq).trim()
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) continue
    let value = line.slice(eq + 1).trim()
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1)
    }
    out[key] = value
  }
  return out
}

export async function callHubWire(options) {
  const { hubRepoRoot, pythonBin, fn } = options
  const args = Array.isArray(options.args) ? options.args : []
  const kwargs = options.kwargs && typeof options.kwargs === 'object' ? options.kwargs : {}
  const spawnImpl = options.spawnImpl ?? spawn

  const srcPath = join(hubRepoRoot, 'src')
  const env = { ...process.env, ...readEngineEnvFile(options.engineEnvFile) }
  // PYTHONPATH must point at the Hub `src` dir (contract: PYTHONPATH=src with
  // cwd=hubRepoRoot). Preserve a pre-existing PYTHONPATH after it.
  env.PYTHONPATH = env.PYTHONPATH ? `${srcPath}:${env.PYTHONPATH}` : srcPath

  /** @type {import('node:child_process').ChildProcess} */
  let child
  try {
    child = spawnImpl(pythonBin, ['-u', '-c', WIRE_DRIVER], {
      cwd: hubRepoRoot,
      env,
      stdio: ['pipe', 'pipe', 'pipe'],
      shell: false,
    })
  } catch (error) {
    return { ok: false, error: `failed to spawn ${pythonBin}: ${String(error?.message ?? error)}` }
  }

  const stdout = []
  const stderr = []
  child.stdout?.on('data', (chunk) => stdout.push(String(chunk)))
  child.stderr?.on('data', (chunk) => stderr.push(String(chunk)))

  const done = new Promise((resolve) => {
    child.once('error', (error) => resolve({ exitCode: null, spawnError: error }))
    child.once('close', (exitCode) => resolve({ exitCode, spawnError: null }))
  })

  child.stdin?.write(JSON.stringify({ fn, args, kwargs }))
  child.stdin?.end()

  const { exitCode, spawnError } = await done
  if (spawnError) {
    return { ok: false, error: `failed to run ${pythonBin}: ${String(spawnError.message ?? spawnError)}` }
  }
  const text = stdout.join('').trim()
  if (exitCode !== 0) {
    const errTail = stderr.join('').trim().slice(-800)
    return { ok: false, error: `${pythonBin} exited ${exitCode}${errTail ? `: ${errTail}` : ''}` }
  }
  if (!text) {
    const errTail = stderr.join('').trim().slice(-800)
    return { ok: false, error: `empty wire response${errTail ? ` (stderr: ${errTail})` : ''}` }
  }
  try {
    const parsed = JSON.parse(text)
    if (parsed && typeof parsed === 'object' && typeof parsed.ok === 'boolean') {
      return parsed
    }
    return { ok: false, error: `unexpected wire response shape: ${text.slice(0, 200)}` }
  } catch {
    return { ok: false, error: `wire response is not JSON: ${text.slice(0, 200)}` }
  }
}

/**
 * Render one canonical value to model-facing content. Never throws.
 *
 * @param {unknown} value
 * @returns {{ type: 'text', text: string }[]}
 */
export function renderHubResult(value) {
  const record = value && typeof value === 'object' ? /** @type {Record<string, unknown>} */ (value) : {}
  if (record.ok === true) {
    return [{ type: 'text', text: JSON.stringify(record.result ?? null, null, 2) }]
  }
  const error = typeof record.error === 'string' && record.error ? record.error : 'unknown error'
  return [{ type: 'text', text: `hub read failed: ${error}` }]
}

/**
 * Shared output descriptor: structured result, rendered as JSON text.
 */
const OUTPUT = {
  schema: {
    type: 'object',
    additionalProperties: true,
    properties: {
      ok: { type: 'boolean' },
      result: {},
      error: { type: 'string' },
      code: { type: 'string' },
    },
  },
  render: (_args, value) => renderHubResult(value),
}

/**
 * Build the registry-ready definition for `hub_browse` (always on).
 *
 * @param {{ hubRepoRoot: string, pythonBin: string }} options
 * @returns {import('@deepseek-ai/dsh-tools').ToolDefinition}
 */
export function createHubBrowseToolOptions(options) {
  return {
    name: TOOL_NAMES.browse,
    description:
      'Browse the knowledge-hub ontology catalog (read-only, draft catalog). '
      + 'Returns a structured page {count, items, next_cursor, reason}; pass next_cursor back for the next page. '
      + 'Misses return items:[] — not an error. No write/promote/apply exists.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      properties: {
        kind: {
          type: 'string',
          description: 'Optional kind filter (e.g. DomainOntologyPack).',
        },
        cursor: {
          type: 'string',
          description: 'Opaque cursor from a previous response next_cursor.',
        },
        limit: {
          type: 'integer',
          description: 'Page size (wire clamps to its max).',
        },
      },
    },
    output: OUTPUT,
    isConcurrencySafe: () => true,
    execute: async (args) => {
      const record = args && typeof args === 'object' ? /** @type {Record<string, unknown>} */ (args) : {}
      const kwargs = {}
      if (typeof record.kind === 'string' && record.kind) kwargs.kind = record.kind
      if (typeof record.cursor === 'string' && record.cursor) kwargs.cursor = record.cursor
      if (Number.isInteger(record.limit)) kwargs.limit = record.limit
      return callHubWire({ ...options, fn: TOOL_NAMES.browse, args: [HUB_BANK], kwargs })
    },
  }
}

/**
 * Build the registry-ready definition for `hub_resolve` (always on).
 *
 * @param {{ hubRepoRoot: string, pythonBin: string }} options
 * @returns {import('@deepseek-ai/dsh-tools').ToolDefinition}
 */
export function createHubResolveToolOptions(options) {
  return {
    name: TOOL_NAMES.resolve,
    description:
      'Resolve one knowledge-hub ontology id to its full read-only record '
      + '(kind, name, is_a, relations, constraints, evidence, …). '
      + 'A miss returns {found:false} — not an error.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      required: ['id'],
      properties: {
        id: {
          type: 'string',
          description: 'Ontology id to resolve, e.g. "Customer".',
        },
      },
    },
    output: OUTPUT,
    isConcurrencySafe: () => true,
    execute: async (args) => {
      const record = args && typeof args === 'object' ? /** @type {Record<string, unknown>} */ (args) : {}
      const id = typeof record.id === 'string' ? record.id : ''
      if (!id) return { ok: false, error: 'id is required' }
      return callHubWire({ ...options, fn: TOOL_NAMES.resolve, args: [HUB_BANK, id], kwargs: {} })
    },
  }
}

/**
 * Build the registry-ready definition for `hub_neighborhood_read`.
 * Only registered when the profile sets `neighborhoodEnabled: true`; the
 * execute path then passes `enabled=True` explicitly (wire default-off).
 *
 * @param {{ hubRepoRoot: string, pythonBin: string }} options
 * @returns {import('@deepseek-ai/dsh-tools').ToolDefinition}
 */
export function createHubNeighborhoodToolOptions(options) {
  return {
    name: TOOL_NAMES.neighborhood,
    description:
      'Read the one-hop neighborhood of one ontology id (nodes + edges). '
      + 'Default-off tool: only mounted when the profile explicitly enables it. '
      + 'Center miss returns empty nodes/edges with found:false.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      required: ['id'],
      properties: {
        id: {
          type: 'string',
          description: 'Center ontology id.',
        },
        depth: {
          type: 'integer',
          description: 'Only depth 1 is supported by the wire.',
        },
      },
    },
    output: OUTPUT,
    isConcurrencySafe: () => true,
    execute: async (args) => {
      const record = args && typeof args === 'object' ? /** @type {Record<string, unknown>} */ (args) : {}
      const id = typeof record.id === 'string' ? record.id : ''
      if (!id) return { ok: false, error: 'id is required' }
      const kwargs = { enabled: true }
      if (Number.isInteger(record.depth)) kwargs.depth = record.depth
      return callHubWire({ ...options, fn: TOOL_NAMES.neighborhood, args: [HUB_BANK, id], kwargs })
    },
  }
}
