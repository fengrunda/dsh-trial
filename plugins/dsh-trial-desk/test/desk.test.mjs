import assert from 'node:assert/strict'
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'

import {
  DROP_JOB_TOOL,
  createDropJobToolOptions,
  createStatusToolOptions,
  dropChainReply,
  dropJob,
  normalizeConfig,
  parseJobArg,
  readTrialStatus,
  resolveDshHome,
  safeJobId,
  trialPaths,
  validateJob,
} from '../lib/desk.js'
import { apply, inject, name as pluginName } from '../lib/index.js'

const HERE = dirname(fileURLToPath(import.meta.url))
const EXAMPLES = join(HERE, '..', '..', '..', 'broker', 'examples')

const ISOLATED_ENV = {
  HOME: '/tmp/dsh-trial-desk-home',
  PATH: process.env.PATH,
}

function cfgFor(tmp, extra = {}) {
  return normalizeConfig(
    { dshHome: tmp, recentLimit: 8, ...extra },
    { env: { ...ISOLATED_ENV } },
  )
}

test('resolveDshHome uses $DSH_HOME then ~/.dsh — never /home/box or /workspace defaults', () => {
  const fromEnv = resolveDshHome({ env: { DSH_HOME: '/tmp/my-dsh' } })
  assert.equal(fromEnv, '/tmp/my-dsh')
  const fromHome = resolveDshHome({ home: '/tmp/host', env: {} })
  assert.equal(fromHome, '/tmp/host/.dsh')
  assert.ok(!fromHome.includes('/home/box'))
  assert.ok(!fromHome.includes('/workspace'))
})

test('normalizeConfig anchors inbox/thin/mailbox/broker under dshHome', () => {
  const cfg = cfgFor('/tmp/fake-dsh')
  assert.equal(cfg.dshHome, '/tmp/fake-dsh')
  assert.equal(cfg.inboxRoot, '/tmp/fake-dsh/supervisor/trial/inbox')
  assert.equal(cfg.thinStateRoot, '/tmp/fake-dsh/supervisor/thin-state')
  assert.equal(cfg.mailboxRoot, '/tmp/fake-dsh/supervisor/thin-state/mailbox')
  assert.equal(cfg.brokerDir, '/tmp/fake-dsh/broker-dsh-trial')
  assert.ok(!cfg.inboxRoot.includes('/workspace'))
  assert.ok(!cfg.inboxRoot.includes('/home/box'))
})

test('DSH_TRIAL_MAILBOX / DSH_TRIAL_INBOX env win over dshHome-relative defaults', () => {
  const cfg = normalizeConfig(
    { dshHome: '/tmp/role-home' },
    {
      env: {
        ...ISOLATED_ENV,
        DSH_TRIAL_INBOX: '/tmp/global-inbox',
        DSH_TRIAL_THIN_STATE: '/tmp/global-thin',
        DSH_TRIAL_MAILBOX: '/tmp/global-mailbox',
        TRIAL_BROKER_DIR: '/tmp/global-broker',
      },
    },
  )
  assert.equal(cfg.inboxRoot, '/tmp/global-inbox')
  assert.equal(cfg.thinStateRoot, '/tmp/global-thin')
  assert.equal(cfg.mailboxRoot, '/tmp/global-mailbox')
  assert.equal(cfg.brokerDir, '/tmp/global-broker')
})

test('broker example jobs validate', async () => {
  const files = [
    'job-goal-intra-comms.json',
    'job-goal-medium-bridge.json',
    'job-chain-scratch.json',
    'job-chain-acp-accept.json',
    'job-chain-reply.json',
    'job-baseline.json',
  ]
  for (const name of files) {
    const raw = JSON.parse(await readFile(join(EXAMPLES, name), 'utf8'))
    const v = validateJob(raw)
    assert.equal(v.ok, true, `${name}: ${v.errors?.join('; ')}`)
  }
})

test('validateJob rejects missing fields, rooms, bad profile', () => {
  assert.equal(validateJob({ type: 'goal', goal: 'g' }).ok, false)
  assert.equal(validateJob({ type: 'chain', slice: 's' }).ok, false)
  assert.equal(validateJob({ type: 'chain-reply', slice: 's', answer: '  ' }).ok, false)
  assert.equal(validateJob({ type: 'goal', goal: 'g', brief: 'b', profile: 'web' }).ok, false)
  const room = validateJob({
    type: 'goal',
    goal: 'g',
    brief: 'b',
    room_join: true,
  })
  assert.equal(room.ok, false)
  assert.match(room.errors.join(' '), /room/)
  assert.equal(validateJob({ ticket: 't1' }).ok, false)
})

test('validateJob rejects bad cwd for goal / chain / ticket', () => {
  const goalBase = { type: 'goal', goal: 'g', brief: 'b', profile: 'acp-lite' }
  const chainBase = { type: 'chain', slice: 's', pack: 'p.pack.md', profile: 'acp-lite' }
  const ticketBase = { ticket: 't1', pack: 'p.pack.md' }

  for (const [name, base] of [
    ['goal', goalBase],
    ['chain', chainBase],
    ['ticket', ticketBase],
  ]) {
    const missing = validateJob({ ...base })
    assert.equal(missing.ok, false, `${name} missing cwd should fail`)
    assert.match(missing.errors.join(' '), /绝对路径|absolute|Mac/i)

    const empty = validateJob({ ...base, cwd: '' })
    assert.equal(empty.ok, false, `${name} empty cwd should fail`)
    assert.match(empty.errors.join(' '), /绝对路径|absolute|Mac/i)

    const blank = validateJob({ ...base, cwd: '   ' })
    assert.equal(blank.ok, false, `${name} whitespace cwd should fail`)

    const placeholder = validateJob({ ...base, cwd: '/path/to/your/workdir' })
    assert.equal(placeholder.ok, false, `${name} placeholder cwd should fail`)
    assert.match(placeholder.errors.join(' '), /绝对路径|absolute|Mac/i)

    const ws = validateJob({ ...base, cwd: '/workspace' })
    assert.equal(ws.ok, false, `${name} /workspace cwd should fail`)
    assert.match(ws.errors.join(' '), /绝对路径|absolute|Mac/i)

    const okCwd = validateJob({ ...base, cwd: '/tmp/dsh-trial-example-workdir' })
    assert.equal(okCwd.ok, true, `${name} legal cwd should pass: ${okCwd.errors?.join('; ')}`)
  }

  // chain-reply / goal-update are exempt from cwd checks
  assert.equal(
    validateJob({ type: 'chain-reply', slice: 's', answer: 'ok' }).ok,
    true,
  )
  assert.equal(validateJob({ type: 'goal-update', goal: 'g' }).ok, true)
})

test('safeJobId rejects path traversal', () => {
  assert.equal(safeJobId('../etc'), '')
  assert.equal(safeJobId('a/b'), '')
  assert.equal(safeJobId('job-goal-ok'), 'job-goal-ok')
})

test('dropJob writes inbox; dry_run does not; no overwrite', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'dsh-trial-desk-'))
  try {
    const cfg = cfgFor(tmp)
    const job = {
      type: 'goal',
      id: 'job-desk-unit-goal',
      goal: 'desk-unit',
      brief: 'add farewell next to greet',
      cwd: '/tmp/dsh-trial-example-workdir',
      profile: 'acp-lite',
      supervisor_profile: 'acp-lite',
      max_slices: 1,
    }
    const dry = await dropJob(job, { dryRun: true }, cfg)
    assert.equal(dry.ok, true)
    assert.equal(dry.dry_run, true)
    assert.equal(dry.would_write, join(tmp, 'supervisor/trial/inbox/job-desk-unit-goal.json'))
    assert.deepEqual(await readdir(join(tmp, 'supervisor/trial/inbox')).catch(() => []), [])

    const wrote = await dropJob(job, {}, cfg)
    assert.equal(wrote.ok, true)
    const dest = join(tmp, 'supervisor/trial/inbox/job-desk-unit-goal.json')
    const saved = JSON.parse(await readFile(dest, 'utf8'))
    assert.equal(saved.type, 'goal')
    assert.equal(saved.goal, 'desk-unit')
    assert.equal(saved.cwd, '/tmp/dsh-trial-example-workdir')
    assert.ok(!JSON.stringify(saved).includes('/workspace'))

    const again = await dropJob(job, {}, cfg)
    assert.equal(again.ok, false)
    assert.match(again.error, /already exists/)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('dropChainReply writes type chain-reply', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'dsh-trial-desk-'))
  try {
    const cfg = cfgFor(tmp)
    const r = await dropChainReply(
      { slice: 'scratch-hello', answer: 'Use def greet(name=None).', id: 'job-chain-reply-unit' },
      {},
      cfg,
    )
    assert.equal(r.ok, true)
    const saved = JSON.parse(await readFile(r.path, 'utf8'))
    assert.equal(saved.type, 'chain-reply')
    assert.equal(saved.slice, 'scratch-hello')
    assert.match(saved.answer, /greet/)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('readTrialStatus reports broker alive, chains, summaries, mailbox pending', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'dsh-trial-desk-'))
  try {
    const cfg = cfgFor(tmp)
    const p = trialPaths(cfg)
    await mkdir(p.inbox, { recursive: true })
    await mkdir(p.chains, { recursive: true })
    await mkdir(p.summaries, { recursive: true })
    await mkdir(p.mailboxPending, { recursive: true })
    await mkdir(p.goals, { recursive: true })
    await mkdir(p.brokerDir, { recursive: true })
    await writeFile(p.pidfile, `${process.pid}\n`)
    await writeFile(
      join(p.chains, 'scratch-hello.json'),
      JSON.stringify({ slice: 'scratch-hello', state: 'awaiting_supervisor', updated_at: 't' }) + '\n',
    )
    await writeFile(join(p.summaries, 'scratch-hello-r1.md'), '# summary\n')
    await writeFile(
      join(p.mailboxPending, 'ask-1.json'),
      JSON.stringify({ id: 'ask-1', kind: 'ask_supervisor', to_role: 'supervisor', status: 'pending' }) + '\n',
    )
    await writeFile(
      join(p.goals, 'desk-unit.json'),
      JSON.stringify({ goal: 'desk-unit', status: 'running', slices: ['s1'] }) + '\n',
    )
    await writeFile(
      join(p.inbox, 'waiting.json'),
      JSON.stringify({ type: 'goal', goal: 'x', brief: 'y' }) + '\n',
    )

    const st = await readTrialStatus(cfg)
    assert.equal(st.ok, true)
    assert.equal(st.broker.alive, true)
    assert.equal(st.broker.pid, process.pid)
    assert.equal(st.prod_broker.sock_exists, false)
    assert.ok(st.inbox.pending.includes('waiting.json'))
    assert.equal(st.chains.recent[0].state, 'awaiting_supervisor')
    assert.ok(st.summaries.recent.includes('scratch-hello-r1.md'))
    assert.equal(st.mailbox.pending_count, 1)
    assert.equal(st.mailbox.pending[0].kind, 'ask_supervisor')
    assert.equal(st.goals.recent[0].status, 'running')
    assert.match(st.note, /never rooms/i)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('stale pidfile is not alive', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'dsh-trial-desk-'))
  try {
    const cfg = cfgFor(tmp)
    const p = trialPaths(cfg)
    await mkdir(p.brokerDir, { recursive: true })
    await writeFile(p.pidfile, '99999999\n')
    const st = await readTrialStatus(cfg)
    assert.equal(st.broker.alive, false)
    assert.equal(st.broker.stale, true)
    assert.match(st.broker.hint, /dsh-trial start/)
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('cordis named exports and bundle metadata', async () => {
  assert.equal(pluginName, 'trial-desk')
  assert.deepEqual(inject, ['tools'])
  assert.equal(typeof apply, 'function')
  const pkg = JSON.parse(await readFile(join(HERE, '..', 'package.json'), 'utf8'))
  assert.equal(pkg.peerDependencies['@deepseek-ai/cordis'], '^4.0.2')
  assert.equal(pkg.dsh.bundle.patch, './cordis.patch.yml')
  const registered = []
  apply(
    {
      tools: { register: (t) => registered.push(t.name) },
      logger: { info() {} },
    },
    { dshHome: '/tmp/desk-apply', env: { ...ISOLATED_ENV } },
  )
  assert.deepEqual(registered, [
    'trial_drop_job',
    'trial_validate_job',
    'trial_chain_reply',
    'trial_status',
  ])
})

test('parseJobArg accepts JSON string', () => {
  const r = parseJobArg('{"type":"chain-reply","slice":"s","answer":"ok"}')
  assert.equal(r.ok, true)
  assert.equal(r.job.slice, 's')
})

test('dropJob without cwd fails validate and writes nothing', async () => {
  const tmp = await mkdtemp(join(tmpdir(), 'dsh-trial-desk-'))
  try {
    const cfg = cfgFor(tmp)
    const r = await dropJob(
      {
        type: 'chain',
        id: 'job-chain-no-cwd',
        slice: 's1',
        pack: 's1.pack.md',
        profile: 'acp-lite',
        acceptance: ['no room_*'],
      },
      {},
      cfg,
    )
    assert.equal(r.ok, false)
    assert.match(r.errors.join(' '), /绝对路径|absolute|Mac/i)
    assert.equal(r.path, undefined)
    assert.deepEqual(await readdir(join(tmp, 'supervisor/trial/inbox')).catch(() => []), [])
    assert.ok(!JSON.stringify(r).includes('/workspace'))

    const withCwd = await dropJob(
      {
        type: 'chain',
        id: 'job-chain-with-cwd',
        slice: 's1',
        pack: 's1.pack.md',
        profile: 'acp-lite',
        cwd: '/tmp/dsh-trial-example-workdir',
        acceptance: ['no room_*'],
      },
      {},
      cfg,
    )
    assert.equal(withCwd.ok, true)
    const saved = JSON.parse(await readFile(withCwd.path, 'utf8'))
    assert.equal(saved.cwd, '/tmp/dsh-trial-example-workdir')
    assert.ok(!JSON.stringify(saved).includes('/workspace'))
    assert.ok(!JSON.stringify(saved).includes('/home/box'))
  } finally {
    await rm(tmp, { recursive: true, force: true })
  }
})

test('apply tolerates null config (cordis.patch.yml `config:` with only comments)', () => {
  const registered = []
  apply(
    { tools: { register: (t) => registered.push(t.name) }, logger: { info() {} } },
    null,
  )
  assert.equal(registered.length, 4)
  const cfg = normalizeConfig(null, { dshHome: '/tmp/desk-null', env: { ...ISOLATED_ENV } })
  assert.equal(cfg.inboxRoot, join('/tmp/desk-null', 'supervisor', 'trial', 'inbox'))
})

test('bundle cordis.patch.yml gives trial-desk an object config', async () => {
  const yml = await readFile(join(HERE, '..', 'cordis.patch.yml'), 'utf8')
  assert.match(yml, /^\s+config: \{\}\s*$/m)
})
