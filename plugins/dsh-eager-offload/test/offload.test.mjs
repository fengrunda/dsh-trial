import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'

import {
  OFFLOAD_MARK,
  composeInPlaceTruncate,
  composeReplacement,
  flattenPlainText,
  headTailPreview,
  isUnderOffloadRoot,
  maybeOffload,
  normalizeConfig,
  readFilePathFromArgs,
  saveOffloadFile,
  sessionDirName,
} from '../lib/offload.js'

test('normalizeConfig defaults', () => {
  const cfg = normalizeConfig({}, { dshHome: '/tmp/fake-dsh' })
  assert.equal(cfg.offloadRoot, '/tmp/fake-dsh/offload')
  assert.equal(cfg.inlineMaxBytes, 4096)
  assert.equal(cfg.previewHeadBytes, 1536)
  assert.equal(cfg.previewTailBytes, 1024)
  assert.equal(cfg.offloadReadMaxInlineBytes, 16384)
  assert.equal(cfg.excludeTools.size, 0)
})

test('normalizeConfig rejects bad inlineMaxBytes', () => {
  assert.throws(() => normalizeConfig({ inlineMaxBytes: -1 }), /inlineMaxBytes/)
  assert.throws(() => normalizeConfig({ inlineMaxBytes: 1.5 }), /inlineMaxBytes/)
})

test('flattenPlainText requires all-text blocks', () => {
  assert.equal(flattenPlainText([{ type: 'text', text: 'a' }, { type: 'text', text: 'b' }]), 'ab')
  assert.equal(flattenPlainText([{ type: 'text', text: 'a' }, { type: 'image' }]), undefined)
  assert.equal(flattenPlainText(undefined), undefined)
})

test('headTailPreview keeps short text', () => {
  const r = headTailPreview('hello', 10, 10)
  assert.equal(r.text, 'hello')
  assert.equal(r.omittedBytes, 0)
})

test('headTailPreview omits middle and stays UTF-8 safe', () => {
  const text = 'A'.repeat(100) + '中文' + 'B'.repeat(100)
  const r = headTailPreview(text, 40, 40)
  assert.ok(r.omittedBytes > 0)
  assert.ok(r.text.includes('bytes omitted'))
  assert.doesNotThrow(() => Buffer.from(r.text, 'utf8'))
})

test('isUnderOffloadRoot', () => {
  const root = '/home/box/.dsh/offload'
  assert.equal(isUnderOffloadRoot('/home/box/.dsh/offload/abc/x.txt', root), true)
  assert.equal(isUnderOffloadRoot('/home/box/.dsh/offload', root), true)
  assert.equal(isUnderOffloadRoot('/home/box/.dsh/other/x.txt', root), false)
  assert.equal(isUnderOffloadRoot('/tmp/x.txt', root), false)
})

test('readFilePathFromArgs', () => {
  assert.equal(readFilePathFromArgs({ file_path: '/a/b' }), '/a/b')
  assert.equal(readFilePathFromArgs({}), undefined)
  assert.equal(readFilePathFromArgs(null), undefined)
})

test('saveOffloadFile writes full content under session dir', async () => {
  const root = await mkdtemp(join(tmpdir(), 'eager-offload-'))
  try {
    const saved = await saveOffloadFile({
      offloadRoot: root,
      sessionId: 'sess-test-1',
      toolName: 'bash',
      content: 'FULL_OUTPUT_LINE\n'.repeat(50),
    })
    assert.ok(saved.path.startsWith(join(root, sessionDirName('sess-test-1'))))
    assert.equal(await readFile(saved.path, 'utf8'), 'FULL_OUTPUT_LINE\n'.repeat(50))
    assert.equal(saved.bytes, Buffer.byteLength('FULL_OUTPUT_LINE\n'.repeat(50), 'utf8'))
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('composeReplacement stays within inlineMaxBytes and carries mark', () => {
  const full = 'Z'.repeat(20000)
  const out = composeReplacement(full, {
    inlineMaxBytes: 4096,
    previewHeadBytes: 1536,
    previewTailBytes: 1024,
    path: '/tmp/offload/x.txt',
    toolName: 'bash',
    callId: 'c1',
  })
  assert.ok(Buffer.byteLength(out, 'utf8') <= 4096)
  assert.ok(out.includes(OFFLOAD_MARK))
  assert.ok(out.includes('/tmp/offload/x.txt'))
  assert.ok(out.includes('bytes=20000'))
})

test('maybeOffload skips under cap', async () => {
  const cfg = normalizeConfig({ inlineMaxBytes: 4096 }, { dshHome: '/tmp/x' })
  const r = await maybeOffload({
    toolName: 'bash',
    text: 'short',
    sessionId: 's',
    cfg,
  })
  assert.equal(r, undefined)
})

test('maybeOffload excludes listed tools', async () => {
  const cfg = normalizeConfig({ inlineMaxBytes: 10, excludeTools: ['bash'] }, { dshHome: '/tmp/x' })
  const r = await maybeOffload({
    toolName: 'bash',
    text: 'x'.repeat(100),
    sessionId: 's',
    cfg,
  })
  assert.equal(r, undefined)
})

test('maybeOffload covers read and saves file', async () => {
  const root = await mkdtemp(join(tmpdir(), 'eager-offload-'))
  try {
    const cfg = normalizeConfig(
      { offloadRoot: root, inlineMaxBytes: 200, previewHeadBytes: 60, previewTailBytes: 40 },
      { dshHome: '/tmp/x' },
    )
    const body = 'line\n'.repeat(200)
    const r = await maybeOffload({
      toolName: 'read',
      callId: 'r1',
      arguments: { file_path: '/workspace/dsh-trial/scratch/eager-offload-fixtures/large.txt' },
      text: body,
      sessionId: 'sess-read',
      cfg,
    })
    assert.ok(r)
    assert.ok(r.includes(OFFLOAD_MARK))
    assert.ok(Buffer.byteLength(r, 'utf8') <= 200)
    const m = /path=(\S+)/.exec(r)
    assert.ok(m)
    assert.equal(await readFile(m[1], 'utf8'), body)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('maybeOffload breaks loop for read under offload root (in-place, no new file)', async () => {
  const root = await mkdtemp(join(tmpdir(), 'eager-offload-'))
  try {
    const cfg = normalizeConfig(
      {
        offloadRoot: root,
        inlineMaxBytes: 200,
        offloadReadMaxInlineBytes: 300,
        previewHeadBytes: 60,
        previewTailBytes: 40,
      },
      { dshHome: '/tmp/x' },
    )
    const offPath = join(root, 'sess', 'already.txt')
    // Pretend the model is reading an existing offload file with huge content.
    const body = 'Q'.repeat(5000)
    let saves = 0
    const r = await maybeOffload({
      toolName: 'read',
      arguments: { file_path: join(root, 'abc', 'x.txt') },
      text: body,
      sessionId: 'sess-loop',
      cfg,
      save: async () => {
        saves++
        return { path: offPath, bytes: body.length }
      },
    })
    assert.equal(saves, 0)
    assert.ok(r)
    assert.ok(r.includes('in-place truncate'))
    assert.ok(Buffer.byteLength(r, 'utf8') <= 200)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('composeInPlaceTruncate carries mark', () => {
  const out = composeInPlaceTruncate('Y'.repeat(10000), {
    maxInlineBytes: 500,
    previewHeadBytes: 100,
    previewTailBytes: 80,
  })
  assert.ok(out.includes(OFFLOAD_MARK))
  assert.ok(Buffer.byteLength(out, 'utf8') <= 500)
})

test('toolOverrides tighten bash only', async () => {
  const root = await mkdtemp(join(tmpdir(), 'eager-offload-'))
  try {
    const cfg = normalizeConfig(
      {
        offloadRoot: root,
        inlineMaxBytes: 5000,
        toolOverrides: { bash: { inlineMaxBytes: 320 } },
        previewHeadBytes: 40,
        previewTailBytes: 20,
      },
      { dshHome: '/tmp/x' },
    )
    const mid = 'm'.repeat(1000)
    const skip = await maybeOffload({ toolName: 'read', text: mid, sessionId: 's', cfg, arguments: { file_path: '/tmp/a' } })
    assert.equal(skip, undefined) // under 5000
    const hit = await maybeOffload({ toolName: 'bash', text: mid, sessionId: 's', cfg })
    assert.ok(hit)
    assert.ok(Buffer.byteLength(hit, 'utf8') <= 320)
    assert.ok(hit.includes(OFFLOAD_MARK))
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
