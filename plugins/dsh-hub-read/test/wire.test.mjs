/**
 * dsh-hub-read unit tests (no Hub checkout required): the Python subprocess is
 * mocked via `spawnImpl`, so these run anywhere node runs.
 */
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test from 'node:test'

import {
  HUB_BANK,
  TOOL_NAMES,
  callHubWire,
  createHubBrowseToolOptions,
  readEngineEnvFile,
  createHubNeighborhoodToolOptions,
  createHubResolveToolOptions,
  normalizeConfig,
  renderHubResult,
} from '../lib/wire.js'

/** Fake spawn: feeds the request through and replies with `reply` on stdout. */
function fakeSpawn(reply, { exitCode = 0, stderr = '' } = {}) {
  return (cmd, args, opts) => {
    const child = new EventEmitter()
    child.stdout = new EventEmitter()
    child.stderr = new EventEmitter()
    child.stdin = {
      written: '',
      write(chunk) { this.written += String(chunk) },
      end() {
        queueMicrotask(() => {
          if (stderr) child.stderr.emit('data', stderr)
          if (reply !== null) child.stdout.emit('data', `${typeof reply === 'string' ? reply : JSON.stringify(reply)}\n`)
          child.emit('close', exitCode)
        })
      },
    }
    return child
  }
}

test('normalizeConfig fills defaults', () => {
  const cfg = normalizeConfig({})
  assert.equal(cfg.hubRepoRoot, '/workspace/hermes-work/knowledge-hub')
  assert.equal(cfg.pythonBin, 'python3')
  assert.equal(cfg.neighborhoodEnabled, false)
  assert.equal(normalizeConfig({ neighborhoodEnabled: true }).neighborhoodEnabled, true)
})

test('callHubWire sends pinned bank and parses wire envelope', async () => {
  let seen = null
  const spawnImpl = (cmd, args, opts) => {
    const child = new EventEmitter()
    child.stdout = new EventEmitter()
    child.stderr = new EventEmitter()
    child.stdin = {
      write(chunk) { seen = { cmd, opts, request: JSON.parse(String(chunk)) } },
      end() {
        queueMicrotask(() => {
          child.stdout.emit('data', JSON.stringify({ ok: true, result: { count: 1, items: [], next_cursor: null, reason: null } }) + '\n')
          child.emit('close', 0)
        })
      },
    }
    return child
  }
  const out = await callHubWire({
    hubRepoRoot: '/x', pythonBin: 'python3', fn: TOOL_NAMES.browse,
    args: [HUB_BANK], kwargs: { limit: 3 }, spawnImpl,
  })
  assert.equal(out.ok, true)
  assert.equal(seen.request.fn, 'hub_browse')
  assert.deepEqual(seen.request.args, ['dsh-dev'])
  assert.deepEqual(seen.request.kwargs, { limit: 3 })
  // PYTHONPATH must contain the repo src dir; cwd must be the repo root.
  assert.equal(seen.opts.cwd, '/x')
  assert.match(seen.opts.env.PYTHONPATH, /^\/x\/src/)
  assert.equal(seen.opts.shell, false)
})

test('callHubWire surfaces wire errors without throwing', async () => {
  const out = await callHubWire({
    hubRepoRoot: '/x', pythonBin: 'python3', fn: TOOL_NAMES.neighborhood,
    args: [HUB_BANK, 'Customer'], kwargs: {},
    spawnImpl: fakeSpawn({ ok: false, error: 'hub_neighborhood_read is default-off', code: 'default_off' }),
  })
  assert.equal(out.ok, false)
  assert.equal(out.code, 'default_off')
})

test('callHubWire reports non-zero exit with stderr tail', async () => {
  const out = await callHubWire({
    hubRepoRoot: '/x', pythonBin: 'python3', fn: TOOL_NAMES.browse,
    args: [HUB_BANK], kwargs: {},
    spawnImpl: fakeSpawn(null, { exitCode: 1, stderr: 'ModuleNotFoundError: no module named knowledge_hub\n' }),
  })
  assert.equal(out.ok, false)
  assert.match(out.error, /exited 1/)
  assert.match(out.error, /knowledge_hub/)
})

test('browse tool: no bank parameter, forwards optional args', async () => {
  let seen = null
  const spawnImpl = (cmd, args, opts) => {
    const child = new EventEmitter()
    child.stdout = new EventEmitter()
    child.stderr = new EventEmitter()
    child.stdin = {
      write(chunk) { seen = JSON.parse(String(chunk)) },
      end() {
        queueMicrotask(() => {
          child.stdout.emit('data', '{"ok":true,"result":{}}\n')
          child.emit('close', 0)
        })
      },
    }
    return child
  }
  const def = createHubBrowseToolOptions({ hubRepoRoot: '/x', pythonBin: 'python3', spawnImpl })
  assert.equal(def.name, 'hub_browse')
  // Bank is pinned; the schema must not expose it.
  assert.ok(!('bank' in def.parameters.properties))
  await def.execute({ kind: 'DomainOntologyPack', cursor: 'c1', limit: 5 })
  assert.deepEqual(seen.args, ['dsh-dev'])
  assert.deepEqual(seen.kwargs, { kind: 'DomainOntologyPack', cursor: 'c1', limit: 5 })
  await def.execute({ limit: 'not-an-int' })
  assert.deepEqual(seen.kwargs, {})
})

test('resolve tool requires id and pins bank positionally', async () => {
  let seen = null
  const spawnImpl = (cmd, args, opts) => {
    const child = new EventEmitter()
    child.stdout = new EventEmitter()
    child.stderr = new EventEmitter()
    child.stdin = {
      write(chunk) { seen = JSON.parse(String(chunk)) },
      end() {
        queueMicrotask(() => {
          child.stdout.emit('data', '{"ok":true,"result":{"found":false}}\n')
          child.emit('close', 0)
        })
      },
    }
    return child
  }
  const def = createHubResolveToolOptions({ hubRepoRoot: '/x', pythonBin: 'python3', spawnImpl })
  assert.deepEqual(def.parameters.required, ['id'])
  const missing = await def.execute({})
  assert.equal(missing.ok, false)
  await def.execute({ id: 'Customer' })
  assert.deepEqual(seen.args, ['dsh-dev', 'Customer'])
  assert.deepEqual(seen.kwargs, {})
})

test('neighborhood tool always passes enabled=True (explicit opt-in)', async () => {
  let seen = null
  const spawnImpl = (cmd, args, opts) => {
    const child = new EventEmitter()
    child.stdout = new EventEmitter()
    child.stderr = new EventEmitter()
    child.stdin = {
      write(chunk) { seen = JSON.parse(String(chunk)) },
      end() {
        queueMicrotask(() => {
          child.stdout.emit('data', '{"ok":true,"result":{"found":true,"nodes":[],"edges":[]}}\n')
          child.emit('close', 0)
        })
      },
    }
    return child
  }
  const def = createHubNeighborhoodToolOptions({ hubRepoRoot: '/x', pythonBin: 'python3', spawnImpl })
  await def.execute({ id: 'Customer' })
  assert.deepEqual(seen.args, ['dsh-dev', 'Customer'])
  assert.deepEqual(seen.kwargs, { enabled: true })
  await def.execute({ id: 'Customer', depth: 1 })
  assert.deepEqual(seen.kwargs, { enabled: true, depth: 1 })
})

test('renderHubResult: ok renders JSON, error renders message, never throws', () => {
  const ok = renderHubResult({ ok: true, result: { found: false } })
  assert.equal(ok[0].type, 'text')
  assert.match(ok[0].text, /"found": false/)
  const err = renderHubResult({ ok: false, error: 'boom' })
  assert.equal(err[0].text, 'hub read failed: boom')
  assert.doesNotThrow(() => renderHubResult(null))
  assert.doesNotThrow(() => renderHubResult('junk'))
})

test('engine env file is merged into the python subprocess and not echoed', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'hub-read-env-'))
  const envFile = join(dir, 'engine.env')
  writeFileSync(envFile, [
    'KNOWLEDGE_HUB_ENGINE_BASE_URL=http://127.0.0.1:8000',
    'KNOWLEDGE_HUB_ENGINE_TENANT_ID=dsh-dev',
    'KNOWLEDGE_HUB_ENGINE_USER_ID=dsh-dev',
    'KNOWLEDGE_HUB_ENGINE_TOKEN=platform-test-example',
    '# comment',
  ].join('\n'))
  const loaded = readEngineEnvFile(envFile)
  assert.equal(loaded.KNOWLEDGE_HUB_ENGINE_TENANT_ID, 'dsh-dev')
  assert.equal(loaded.KNOWLEDGE_HUB_ENGINE_TOKEN, 'platform-test-example')
  let seenEnv = null
  const spawnImpl = (cmd, args, opts) => {
    seenEnv = opts.env
    const child = new EventEmitter()
    child.stdout = new EventEmitter()
    child.stderr = new EventEmitter()
    child.stdin = {
      write() {},
      end() {
        queueMicrotask(() => {
          child.stdout.emit('data', '{"ok":true,"result":{"items":[],"count":0}}\n')
          child.emit('close', 0)
        })
      },
    }
    return child
  }
  const result = await callHubWire({
    hubRepoRoot: '/x',
    pythonBin: 'python3',
    fn: 'hub_browse',
    args: ['dsh-dev'],
    engineEnvFile: envFile,
    spawnImpl,
  })
  assert.equal(result.ok, true)
  assert.equal(seenEnv.KNOWLEDGE_HUB_ENGINE_BASE_URL, 'http://127.0.0.1:8000')
  assert.equal(seenEnv.KNOWLEDGE_HUB_ENGINE_TENANT_ID, 'dsh-dev')
  assert.equal(seenEnv.KNOWLEDGE_HUB_ENGINE_USER_ID, 'dsh-dev')
  assert.equal(seenEnv.KNOWLEDGE_HUB_ENGINE_TOKEN, 'platform-test-example')
  assert.match(seenEnv.PYTHONPATH, /^\/x\/src/)
})
