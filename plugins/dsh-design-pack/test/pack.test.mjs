/**
 * Self-test for dsh-design-pack.
 *
 * Runs with plain Node (no profile, no model, no network):
 *
 *   node --test test/
 *
 * It exercises the real tool definition the registry receives — path
 * confinement, the `maxPackBytes` reject (never a truncated body), render
 * shape, and the `apply` wiring. When the box's `@deepseek-ai/dsh-tools` is
 * resolvable it also asserts the registered schema is one the real registry
 * accepts; otherwise those assertions are skipped.
 */

import assert from 'node:assert/strict'
import { existsSync, mkdirSync, mkdtempSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { test } from 'node:test'

import {
  DEFAULT_MAX_PACK_BYTES,
  DESIGN_PACK_READ_TOOL,
  createDesignPackToolOptions,
  normalizeConfig,
  readDesignPack,
  renderDesignPackResult,
  resolvePackPath,
  resolvePackRoot,
} from '../lib/pack.js'
import { apply } from '../lib/index.js'

const LIMIT = 64
const SMALL_TEXT = '# Small pack\n\nhello pack\n'

/** @returns {{ root: string, packs: string, small: string, big: string }} */
function makeWorkspace() {
  const root = mkdtempSync(join(tmpdir(), 'design-pack-test-'))
  const packs = join(root, 'packs')
  mkdirSync(packs, { recursive: true })
  writeFileSync(join(packs, 'small.pack.md'), SMALL_TEXT)
  writeFileSync(join(packs, 'big.pack.md'), 'x'.repeat(LIMIT + 1))
  writeFileSync(join(root, 'outside.md'), 'outside\n')
  mkdirSync(join(packs, 'nested'), { recursive: true })
  writeFileSync(join(packs, 'nested', 'inner.pack.md'), 'inner\n')
  return { root, packs, small: SMALL_TEXT, big: 'x'.repeat(LIMIT + 1) }
}

function cleanup(root) {
  rmSync(root, { recursive: true, force: true })
}

const read = (packs, path, maxPackBytes = LIMIT) => readDesignPack({ packRoot: packs, maxPackBytes, path })

test('resolvePackPath keeps nested relative paths inside packRoot', () => {
  const { root, packs } = makeWorkspace()
  try {
    assert.equal(resolvePackPath(packs, 'nested/inner.pack.md'), join(packs, 'nested', 'inner.pack.md'))
  } finally {
    cleanup(root)
  }
})

test('readDesignPack returns the exact text and byte count for a pack', () => {
  const { root, packs } = makeWorkspace()
  try {
    const result = read(packs, 'small.pack.md')
    assert.equal(result.ok, true)
    assert.equal(result.text, SMALL_TEXT)
    assert.equal(result.bytes, Buffer.byteLength(SMALL_TEXT))
    assert.equal(result.maxPackBytes, LIMIT)
  } finally {
    cleanup(root)
  }
})

test('readDesignPack accepts a pack exactly at maxPackBytes (boundary)', () => {
  const { root, packs } = makeWorkspace()
  try {
    writeFileSync(join(packs, 'exact.pack.md'), 'y'.repeat(LIMIT))
    const result = read(packs, 'exact.pack.md')
    assert.equal(result.ok, true)
    assert.equal(result.bytes, LIMIT)
  } finally {
    cleanup(root)
  }
})

test('readDesignPack rejects an oversized pack without loading its content', () => {
  const { root, packs, big } = makeWorkspace()
  try {
    const result = read(packs, 'big.pack.md')
    assert.equal(result.ok, false)
    assert.equal(result.text, undefined)
    assert.equal(result.bytes, Buffer.byteLength(big))
    assert.equal(result.maxPackBytes, LIMIT)
    assert.match(result.error, /exceeds maxPackBytes=64/)
    assert.match(result.error, /content not loaded/)
  } finally {
    cleanup(root)
  }
})

test('readDesignPack rejects missing files, directories, absolute paths, traversal and empty input', () => {
  const { root, packs } = makeWorkspace()
  try {
    const cases = [
      ['missing.pack.md', /not found/],
      ['nested', /not a regular file/],
      ['/etc/passwd', /must be relative/],
      ['../outside.md', /escapes packRoot/],
      ['nested/../../outside.md', /escapes packRoot/],
      ['', /path required/],
    ]
    for (const [path, expected] of cases) {
      const result = read(packs, path)
      assert.equal(result.ok, false, `expected reject for ${JSON.stringify(path)}`)
      assert.equal(result.text, undefined)
      assert.match(result.error, expected, `error for ${JSON.stringify(path)}`)
    }
  } finally {
    cleanup(root)
  }
})

test('readDesignPack rejects a symlink that escapes packRoot', (t) => {
  const { root, packs } = makeWorkspace()
  try {
    const link = join(packs, 'escape.md')
    try {
      symlinkSync(join(root, 'outside.md'), link)
    } catch {
      t.skip('symlinks unavailable in this environment')
      return
    }
    const result = read(packs, 'escape.md')
    assert.equal(result.ok, false)
    assert.match(result.error, /symlink/)
  } finally {
    cleanup(root)
  }
})

test('normalizeConfig defaults and rejects a non-positive maxPackBytes', () => {
  assert.deepEqual(normalizeConfig({}), { packRoot: 'packs', maxPackBytes: DEFAULT_MAX_PACK_BYTES })
  assert.throws(() => normalizeConfig({ maxPackBytes: 0 }), /positive integer/)
  assert.throws(() => normalizeConfig({ maxPackBytes: 1.5 }), /positive integer/)
  assert.throws(() => normalizeConfig({ maxPackBytes: '12' }), /positive integer/)
})

test('resolvePackRoot anchors a relative packRoot under DSH_HOME thin-state', () => {
  const home = join(tmpdir(), 'design-pack-home-test')
  assert.equal(resolvePackRoot('packs', { dshHome: home }), join(home, 'supervisor', 'thin-state', 'packs'))
  assert.equal(resolvePackRoot('/absolute/packs', { dshHome: home }), '/absolute/packs')
})

test('the registered definition is accepted by the real dsh-tools schema checker', async (t) => {
  const tools = await loadDshTools()
  if (!tools) {
    t.skip('@deepseek-ai/dsh-tools not resolvable here')
    return
  }
  const { root, packs } = makeWorkspace()
  try {
    const definition = createDesignPackToolOptions({ packRoot: packs, maxPackBytes: LIMIT })
    assert.doesNotThrow(() => tools.assertObjectJsonSchema(definition.parameters))
    assert.doesNotThrow(() => tools.assertSupportedJsonSchema(definition.output.schema))
  } finally {
    cleanup(root)
  }
})

test('design_pack_read execute + render never leak an oversized body', async () => {
  const { root, packs, big } = makeWorkspace()
  try {
    const definition = createDesignPackToolOptions({ packRoot: packs, maxPackBytes: LIMIT })
    assert.equal(definition.name, DESIGN_PACK_READ_TOOL)

    const okValue = await definition.execute({ path: 'small.pack.md' })
    assert.equal(okValue.ok, true)
    assert.deepEqual(definition.output.render({ path: 'small.pack.md' }, okValue), [{ type: 'text', text: SMALL_TEXT }])

    const bigValue = await definition.execute({ path: 'big.pack.md' })
    assert.equal(bigValue.ok, false)
    const blocks = definition.output.render({ path: 'big.pack.md' }, bigValue)
    assert.equal(blocks.length, 1)
    assert.match(blocks[0].text, /^design_pack_read failed: /)
    assert.ok(!blocks[0].text.includes(big), 'rejection text must not contain pack content')

    const badArg = await definition.execute({})
    assert.equal(badArg.ok, false)
    assert.match(String(badArg.error), /path required/)
  } finally {
    cleanup(root)
  }
})

test('renderDesignPackResult is total and never throws on odd values', () => {
  assert.deepEqual(renderDesignPackResult({ ok: true, text: 'body' }), [{ type: 'text', text: 'body' }])
  assert.match(renderDesignPackResult(undefined)[0].text, /unknown error/)
  assert.match(renderDesignPackResult({ ok: false, error: 'nope' })[0].text, /nope/)
})

test('apply registers design_pack_read on ctx.tools and fails loud on bad config', async () => {
  const { root, packs } = makeWorkspace()
  try {
    let captured
    const ctx = { tools: { register: (definition) => { captured = definition } }, logger: { info: () => {} } }
    apply(ctx, { packRoot: packs, maxPackBytes: LIMIT })
    assert.equal(captured.name, DESIGN_PACK_READ_TOOL)
    const value = await captured.execute({ path: 'small.pack.md' })
    assert.equal(value.ok, true)
    assert.equal(value.text, SMALL_TEXT)

    assert.throws(() => apply(ctx, { packRoot: packs, maxPackBytes: -1 }), /positive integer/)
  } finally {
    cleanup(root)
  }
})

test('reads the box thin-state example pack when present', (t) => {
  const packs = join(process.env.DSH_HOME || join(homedir(), '.dsh'), 'supervisor', 'thin-state', 'packs')
  const example = join(packs, 'slice-design-pack-mvp.pack.md')
  if (!existsSync(example)) {
    t.skip('no box thin-state example pack here')
    return
  }
  const result = readDesignPack({ packRoot: packs, path: 'slice-design-pack-mvp.pack.md' })
  assert.equal(result.ok, true)
  assert.equal(result.bytes, statSync(example).size)
  assert.ok(result.bytes <= DEFAULT_MAX_PACK_BYTES)
  assert.match(result.text, /slice_id:/)
})

/**
 * Resolve the box's real `@deepseek-ai/dsh-tools` without requiring it to be
 * installed next to this dev checkout.
 *
 * @returns {Promise<{ assertSupportedJsonSchema: Function, assertObjectJsonSchema: Function } | undefined>}
 */
async function loadDshTools() {
  const home = process.env.DSH_HOME || join(homedir(), '.dsh')
  const candidates = [
    process.env.DSH_DESIGN_PACK_DSH_TOOLS,
    join(home, 'profiles', 'node_modules', '@deepseek-ai', 'dsh-tools', 'lib', 'index.js'),
  ].filter((candidate) => typeof candidate === 'string' && existsSync(candidate))
  for (const candidate of candidates) {
    try {
      return await import(pathToFileURL(candidate).href)
    } catch {
      // try the next candidate
    }
  }
  return undefined
}
