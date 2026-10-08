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
  formatAgeMaskPlaceholder,
  headTailPreview,
  isUnderOffloadRoot,
  maskOldToolResults,
  maybeOffload,
  normalizeConfig,
  offloadedPathFromText,
  readFilePathFromArgs,
  saveOffloadFile,
  sessionDirName,
} from '../lib/offload.js'
import { apply } from '../lib/index.js'

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

test('normalizeConfig contextClear defaults: disabled and deeply frozen', () => {
  const cc = normalizeConfig({}, { dshHome: '/tmp/x' }).contextClear
  assert.deepEqual(cc, {
    enabled: false,
    mode: 'compaction-coupled',
    keepRecentResults: 8,
    minResultBytes: 1200,
    placeholder: { headBytes: 160, tailBytes: 240, failTailBytes: 600 },
    collapseWriteSteps: { enabled: true, tools: ['write', 'edit'], minArgChars: 1200 },
  })
  assert.ok(Object.isFrozen(cc))
  assert.ok(Object.isFrozen(cc.placeholder))
  assert.ok(Object.isFrozen(cc.collapseWriteSteps))
  assert.ok(Object.isFrozen(cc.collapseWriteSteps.tools))
})

test('normalizeConfig contextClear merges partial input over defaults', () => {
  const cc = normalizeConfig(
    {
      contextClear: {
        enabled: true,
        mode: 'off',
        keepRecentResults: 2,
        placeholder: { tailBytes: 10 },
        collapseWriteSteps: { tools: ['write'] },
      },
    },
    { dshHome: '/tmp/x' },
  ).contextClear
  assert.equal(cc.enabled, true)
  assert.equal(cc.mode, 'off')
  assert.equal(cc.keepRecentResults, 2)
  assert.equal(cc.minResultBytes, 1200) // untouched default survives
  assert.deepEqual(cc.placeholder, { headBytes: 160, tailBytes: 10, failTailBytes: 600 })
  assert.deepEqual(cc.collapseWriteSteps, { enabled: true, tools: ['write'], minArgChars: 1200 })
})

test('normalizeConfig contextClear null/undefined yield full defaults', () => {
  const base = normalizeConfig({}, { dshHome: '/tmp/x' }).contextClear
  assert.deepEqual(normalizeConfig({ contextClear: null }, { dshHome: '/tmp/x' }).contextClear, base)
  assert.deepEqual(normalizeConfig({ contextClear: undefined }, { dshHome: '/tmp/x' }).contextClear, base)
})

test('normalizeConfig contextClear rejects unknown keys, bad enums and bad values', () => {
  assert.throws(() => normalizeConfig({ contextClear: { nope: 1 } }), /contextClear\.nope/)
  assert.throws(() => normalizeConfig({ contextClear: { placeholder: { nope: 1 } } }), /placeholder\.nope/)
  assert.throws(
    () => normalizeConfig({ contextClear: { collapseWriteSteps: { nope: 1 } } }),
    /collapseWriteSteps\.nope/,
  )
  assert.throws(() => normalizeConfig({ contextClear: { mode: 'sometimes' } }), /contextClear\.mode/)
  assert.throws(() => normalizeConfig({ contextClear: { enabled: 'yes' } }), /contextClear\.enabled/)
  assert.throws(() => normalizeConfig({ contextClear: { keepRecentResults: -1 } }), /keepRecentResults/)
  assert.throws(() => normalizeConfig({ contextClear: { keepRecentResults: 1.5 } }), /keepRecentResults/)
  assert.throws(() => normalizeConfig({ contextClear: { minResultBytes: -1 } }), /minResultBytes/)
  assert.throws(() => normalizeConfig({ contextClear: { placeholder: { headBytes: -1 } } }), /headBytes/)
  assert.throws(
    () => normalizeConfig({ contextClear: { collapseWriteSteps: { minArgChars: -1 } } }),
    /minArgChars/,
  )
  assert.throws(
    () =>
      normalizeConfig({
        contextClear: { collapseWriteSteps: { enabled: 'yes' } },
      }),
    /collapseWriteSteps\.enabled/,
  )
  assert.throws(
    () => normalizeConfig({ contextClear: { collapseWriteSteps: { tools: 'write' } } }),
    /tools must be an array/,
  )
  assert.throws(
    () => normalizeConfig({ contextClear: { collapseWriteSteps: { tools: [''] } } }),
    /non-empty strings/,
  )
  assert.throws(() => normalizeConfig({ contextClear: 'on' }), /contextClear must be an object/)
  assert.throws(() => normalizeConfig({ contextClear: { placeholder: [] } }), /placeholder must be an object/)
})

test('normalizeConfig still loads legacy ageMaskEnabled:true (deprecated, retained)', () => {
  const cfg = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 3 }, { dshHome: '/tmp/x' })
  assert.equal(cfg.ageMaskEnabled, true)
  assert.equal(cfg.ageMaskKeepRecentN, 3)
  assert.equal(cfg.contextClear.enabled, false)
})

test('apply warns once when deprecated ageMaskEnabled is set and keeps contextClear off', () => {
  const events = []
  const warns = []
  const infos = []
  apply(
    {
      on: (event, handler, opts) => events.push({ event, handler, opts }),
      logger: { warn: (m) => warns.push(m), info: (m) => infos.push(m) },
    },
    { ageMaskEnabled: true },
  )

  assert.deepEqual(events.map((e) => e.event), ['tools/post-execute'])
  assert.equal(warns.filter((w) => /ageMaskEnabled is deprecated/.test(w)).length, 1)
  assert.ok(warns[0].includes('use contextClear'))
  assert.ok(infos.some((m) => m.includes('contextClear=off')))

  const warnsOff = []
  const infosOff = []
  apply(
    {
      on: () => {},
      logger: { warn: (m) => warnsOff.push(m), info: (m) => infosOff.push(m) },
    },
    { contextClear: {} },
  )
  assert.equal(warnsOff.length, 0)
  assert.ok(infosOff.some((m) => m.includes('contextClear=off')))
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

/* ------------------------- age-based clearing ------------------------- */

/** Build a frozen-ish tool_result message like dsh-session produces. */
function toolMsg(text, { toolName = 'bash', callId = 'c1', isError = false } = {}) {
  return {
    id: `m-${callId}`,
    role: 'user',
    source: { kind: 'tool', callId, toolName },
    content: [
      {
        type: 'tool-result',
        toolCallId: callId,
        isError,
        content: [{ type: 'text', text }],
      },
    ],
  }
}

/** 10 tool results, each uniquely identifiable. */
function history(n = 10) {
  return Array.from({ length: n }, (_, i) => toolMsg(`RESULT_${i}_BODY`, { callId: `c${i}` }))
}

function textOf(msg) {
  return msg.content[0].content[0].text
}

test('age mask keeps newest N full and replaces older ones', async () => {
  const cfg = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 3 }, { dshHome: '/tmp/x' })
  const msgs = history(10)
  const r = await maskOldToolResults(msgs, cfg, { sessionId: 's', save: async () => ({ path: '/p/x.txt', bytes: 1 }) })

  assert.equal(r.masked, 7)
  // newest 3 untouched (identity preserved)
  for (const i of [7, 8, 9]) {
    assert.equal(r.messages[i], msgs[i])
    assert.equal(textOf(r.messages[i]), `RESULT_${i}_BODY`)
  }
  // older 7 -> placeholder carrying a path; byte count reflects the dropped text
  for (const i of [0, 1, 2, 3, 4, 5, 6]) {
    const body = `RESULT_${i}_BODY`
    assert.equal(
      textOf(r.messages[i]),
      formatAgeMaskPlaceholder({ path: '/p/x.txt', bytes: Buffer.byteLength(body, 'utf8') }),
    )
    assert.ok(textOf(r.messages[i]).includes('path=/p/x.txt'))
  }
})

test('age mask defaults: disabled is a no-op, default N is 8', async () => {
  const off = normalizeConfig({}, { dshHome: '/tmp/x' })
  assert.equal(off.ageMaskEnabled, false)
  assert.equal(off.ageMaskKeepRecentN, 8)

  const msgs = history(10)
  let saves = 0
  const r = await maskOldToolResults(msgs, off, { sessionId: 's', save: async () => (saves++, { path: '/p', bytes: 1 }) })
  assert.equal(r.messages, msgs) // same reference: nothing ran
  assert.equal(r.masked, 0)
  assert.equal(saves, 0)

  const on = normalizeConfig({ ageMaskEnabled: true }, { dshHome: '/tmp/x' })
  const r2 = await maskOldToolResults(history(10), on, { sessionId: 's', save: async () => (saves++, { path: '/p/x.txt', bytes: 1 }) })
  assert.equal(r2.masked, 2) // 10 - 8
  assert.equal(saves, 2)
})

test('age mask N=0 clears every tool result; N>=count is a no-op', async () => {
  const zero = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 0 }, { dshHome: '/tmp/x' })
  const r0 = await maskOldToolResults(history(4), zero, { sessionId: 's', save: async () => ({ path: '/p/x.txt', bytes: 1 }) })
  assert.equal(r0.masked, 4)

  const big = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 99 }, { dshHome: '/tmp/x' })
  const msgs = history(4)
  const r1 = await maskOldToolResults(msgs, big, { sessionId: 's', save: async () => ({ path: '/p', bytes: 1 }) })
  assert.equal(r1.messages, msgs)
  assert.equal(r1.masked, 0)
})

test('age mask rejects non-boolean ageMaskEnabled and bad N', () => {
  assert.throws(() => normalizeConfig({ ageMaskEnabled: 'yes' }), /ageMaskEnabled/)
  assert.throws(() => normalizeConfig({ ageMaskKeepRecentN: -1 }), /ageMaskKeepRecentN/)
})

test('age mask reuses existing offload path and never re-spills', async () => {
  const cfg = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 1 }, { dshHome: '/tmp/x' })
  const priorNotice = composeReplacement('Z'.repeat(9000), {
    inlineMaxBytes: 400,
    previewHeadBytes: 100,
    previewTailBytes: 60,
    path: '/already/offloaded.txt',
    toolName: 'bash',
    callId: 'old',
  })
  const msgs = [toolMsg(priorNotice, { callId: 'old' }), toolMsg('NEWEST', { callId: 'new' })]

  let saves = 0
  const r = await maskOldToolResults(msgs, cfg, { sessionId: 's', save: async () => (saves++, { path: '/should/not/be/used', bytes: 1 }) })

  assert.equal(r.pathReused, 1)
  assert.equal(r.spilled, 0)
  assert.equal(saves, 0) // no second file
  const out = textOf(r.messages[0])
  assert.ok(out.includes('/already/offloaded.txt')) // original path preserved
  assert.ok(!out.includes('/should/not/be/used'))
  assert.ok(!out.includes('Z'.repeat(50))) // full text not re-embedded
})

test('age mask spills untouched old entries once and stays re-readable', async () => {
  const root = await mkdtemp(join(tmpdir(), 'eager-offload-age-'))
  try {
    const cfg = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 1 }, { dshHome: '/tmp/x', offloadRoot: root })
    const body = 'FULL_OLD_BODY\n'.repeat(20)
    const r = await maskOldToolResults([toolMsg(body, { callId: 'old' }), toolMsg('NEW')], cfg, { sessionId: 'sess-age' })

    assert.equal(r.spilled, 1)
    const out = textOf(r.messages[0])
    const path = offloadedPathFromText(out)
    assert.ok(path, 'placeholder must carry a re-readable path')
    assert.equal(await readFile(path, 'utf8'), body) // recoverable
    assert.ok(Buffer.byteLength(out, 'utf8') < Buffer.byteLength(body, 'utf8'))
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('age mask spill failure degrades to bare placeholder without throwing', async () => {
  const cfg = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 1 }, { dshHome: '/tmp/x' })
  const r = await maskOldToolResults([toolMsg('BODY', { callId: 'old' }), toolMsg('NEW')], cfg, {
    sessionId: 's',
    save: async () => { throw new Error('disk full') },
  })
  assert.equal(r.placeholderOnly, 1)
  assert.equal(r.spilled, 0)
  assert.match(textOf(r.messages[0]), /age-masked/)
  assert.equal(offloadedPathFromText(textOf(r.messages[0])), undefined)
})

test('age mask never clears isError results or non-text blocks', async () => {
  const cfg = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 1 }, { dshHome: '/tmp/x' })
  const errMsg = toolMsg('BOOM', { callId: 'e', isError: true })
  const imgMsg = {
    id: 'm-img',
    role: 'user',
    source: { kind: 'tool', callId: 'i', toolName: 'read' },
    content: [{ type: 'tool-result', toolCallId: 'i', content: [{ type: 'image', data: 'x' }] }],
  }
  const msgs = [errMsg, imgMsg, toolMsg('NEWEST', { callId: 'n' })]
  let saves = 0
  const r = await maskOldToolResults(msgs, cfg, { sessionId: 's', save: async () => (saves++, { path: '/p', bytes: 1 }) })
  assert.equal(r.masked, 0)
  assert.equal(saves, 0)
  assert.equal(textOf(r.messages[0]), 'BOOM')
})

test('age mask ignores non-tool messages and rebuilds (does not mutate) frozen input', async () => {
  const cfg = normalizeConfig({ ageMaskEnabled: true, ageMaskKeepRecentN: 1 }, { dshHome: '/tmp/x' })
  const user = { id: 'u1', role: 'user', content: [{ type: 'text', text: 'hi' }] }
  const old = Object.freeze(toolMsg('OLD_BODY', { callId: 'o' }))
  const msgs = [user, old, toolMsg('NEW')]
  const r = await maskOldToolResults(msgs, cfg, { sessionId: 's', save: async () => ({ path: '/p/x.txt', bytes: 1 }) })

  assert.equal(r.messages[0], user) // untouched, same ref
  assert.notEqual(r.messages[1], old) // rebuilt
  assert.equal(textOf(old), 'OLD_BODY') // original not mutated
  assert.equal(r.messages.length, 3)
})
