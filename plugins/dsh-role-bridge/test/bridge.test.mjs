import test from 'node:test'
import assert from 'node:assert/strict'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import {
  sendToRole,
  askSupervisor,
  submitForReview,
  createSendToRoleToolOptions,
  normalizeConfig,
  resolveMailboxRoots,
  ensureMailboxDirs,
} from '../lib/bridge.js'

test('normalizeConfig', () => {
  assert.throws(() => normalizeConfig({ timeoutSec: 0 }))
  const c = normalizeConfig({ wrappers: 'ask_supervisor' })
  assert.deepEqual(c.wrappers, ['ask_supervisor'])
})

test('send_to_role sync + ask wrapper', async () => {
  const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), 'rb-'))
  await fsp.mkdir(path.join(tmp, 'supervisor', 'thin-state'), { recursive: true })
  const cfg = {
    dshHome: tmp,
    mailboxRoot: path.join(tmp, 'mailbox'),
    timeoutSec: 5,
    pollMs: 100,
  }
  const dirs = resolveMailboxRoots(cfg)
  await ensureMailboxDirs(dirs)
  const writer = (async () => {
    for (let i = 0; i < 50; i++) {
      const pending = await fsp.readdir(dirs.pending)
      if (pending.length) {
        const id = pending[0].replace(/\.json$/, '')
        const raw = JSON.parse(await fsp.readFile(path.join(dirs.pending, pending[0]), 'utf8'))
        assert.equal(raw.kind, 'ask_supervisor')
        assert.equal(raw.to_role, 'supervisor')
        await fsp.writeFile(
          path.join(dirs.answers, `${id}.json`),
          JSON.stringify({ ask_id: id, answer: 'use Bye', supervisor_ticket: 's1' }) + '\n',
        )
        return
      }
      await new Promise((r) => setTimeout(r, 50))
    }
    throw new Error('no pending')
  })()
  const r = await askSupervisor({ questions: ['tmpl?'], ask_id: 'ask-rb-1' }, {}, cfg)
  await writer
  assert.equal(r.ok, true)
  assert.match(r.answer, /Bye/)
})

test('submit_for_review wrapper PASS', async () => {
  const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), 'rb-'))
  await fsp.mkdir(path.join(tmp, 'supervisor', 'thin-state'), { recursive: true })
  const cfg = {
    dshHome: tmp,
    mailboxRoot: path.join(tmp, 'mailbox'),
    timeoutSec: 5,
    pollMs: 100,
  }
  const dirs = resolveMailboxRoots(cfg)
  await ensureMailboxDirs(dirs)
  const writer = (async () => {
    for (let i = 0; i < 50; i++) {
      const pending = await fsp.readdir(dirs.pending)
      if (pending.length) {
        const id = pending[0].replace(/\.json$/, '')
        await fsp.writeFile(
          path.join(dirs.answers, `${id}.json`),
          JSON.stringify({ verdict: 'PASS', gate_ticket: 'g1', findings: [] }) + '\n',
        )
        return
      }
      await new Promise((r) => setTimeout(r, 50))
    }
    throw new Error('no pending')
  })()
  const r = await submitForReview({ usage_prompt: 9000, summary: 'done', ask_id: 'rev-rb-1' }, {}, cfg)
  await writer
  assert.equal(r.verdict, 'PASS')
})

test('send_to_role tool timeoutMs', () => {
  const def = createSendToRoleToolOptions({ timeoutSec: 600 })
  assert.equal(def.name, 'send_to_role')
  assert.equal(def.timeoutMs, 660000)
})

test('role-home DSH_HOME does not steal mailbox; env DSH_TRIAL_MAILBOX wins', async () => {
  const tmp = await fsp.mkdtemp(path.join(os.tmpdir(), 'rb-'))
  const roleHome = path.join(tmp, 'role-home')
  // role home already has its own supervisor/thin-state (packs/summaries symlinks)
  await fsp.mkdir(path.join(roleHome, 'supervisor', 'thin-state'), { recursive: true })
  const envMailbox = path.join(tmp, 'env-mailbox')
  const cfg = {
    dshHome: roleHome,
    home: roleHome,
    env: { ...process.env, DSH_HOME: roleHome, DSH_TRIAL_MAILBOX: envMailbox },
    timeoutSec: 5,
    pollMs: 100,
  }
  const dirs = resolveMailboxRoots(cfg)
  assert.equal(dirs.root, envMailbox)
  await ensureMailboxDirs(dirs)
  await fsp.writeFile(
    path.join(dirs.pending, 'probe.json'),
    JSON.stringify({ kind: 'probe' }) + '\n',
  )
  // pending must land in the env mailbox, not in the role home
  const rolePending = path.join(roleHome, 'supervisor', 'thin-state', 'mailbox', 'pending')
  assert.deepEqual(await fsp.readdir(rolePending).catch(() => []), [])
  const got = JSON.parse(await fsp.readFile(path.join(dirs.pending, 'probe.json'), 'utf8'))
  assert.equal(got.kind, 'probe')
  await fsp.rm(path.join(dirs.pending, 'probe.json'))
})

test('default root is the global absolute mailbox regardless of DSH_HOME', () => {
  const tmp = '/tmp/rb-role-home'
  const dirs = resolveMailboxRoots({
    dshHome: tmp,
    home: tmp,
    env: { ...process.env, DSH_HOME: tmp },
  })
  const expected = path.join(os.homedir(), '.dsh', 'supervisor', 'thin-state', 'mailbox')
  assert.equal(dirs.root, expected)
})
