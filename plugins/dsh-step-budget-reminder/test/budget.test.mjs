import assert from 'node:assert/strict'
import test from 'node:test'

import { StepCounter, parseBudget, thresholds } from '../lib/budget.js'
import { apply } from '../lib/index.js'

/** Minimal fake ctx: records the handlers `apply` registers. */
function fakeCtx() {
  const handlers = new Map()
  return {
    handlers,
    on(event, handler) {
      handlers.set(event, handler)
    },
  }
}

/** A root agent: no live parent Agent. */
function rootAgent(id = 'root') {
  return { id, sessionId: id }
}

/** A delegated agent: has a live parent Agent. */
function childAgent(parent, id = 'child') {
  return { id, sessionId: id, parentAgent: parent }
}

/** Drive one step: pre-step then post-execute with a downstream allow result. */
async function runStep(ctx, agent, downstream = { kind: 'allow' }) {
  const preStep = ctx.handlers.get('agent/pre-step')
  await preStep({ agent, messages: [] }, () => undefined)
  const postExecute = ctx.handlers.get('tools/post-execute')
  return postExecute({ agent, name: 'bash', arguments: {} }, undefined, () =>
    downstream,
  )
}

test('parseBudget: env "30" → 30', () => {
  assert.equal(parseBudget({ DSH_STEP_BUDGET: '30' }, {}), 30)
})

test('parseBudget: "0" / "abc" / missing → 0 (no budget)', () => {
  assert.equal(parseBudget({ DSH_STEP_BUDGET: '0' }, {}), 0)
  assert.equal(parseBudget({ DSH_STEP_BUDGET: 'abc' }, {}), 0)
  assert.equal(parseBudget({}, {}), 0)
  assert.equal(parseBudget({ DSH_STEP_BUDGET: '-5' }, {}), 0)
  assert.equal(parseBudget({ DSH_STEP_BUDGET: '30' }, { budget: 0 }), 30)
})

test('parseBudget: config wins over env', () => {
  assert.equal(parseBudget({ DSH_STEP_BUDGET: '30' }, { budget: 12 }), 12)
  assert.equal(parseBudget({ DSH_STEP_BUDGET: '30' }, { budget: 'abc' }), 30)
})

test('thresholds: [30,45] for (30, 1.5) and [7,11] for (7, 1.5)', () => {
  assert.deepEqual(thresholds(30, 1.5), [30, 45])
  assert.deepEqual(thresholds(7, 1.5), [7, 11])
})

test('thresholds: no budget → no thresholds', () => {
  assert.deepEqual(thresholds(0, 1.5), [])
})

test('StepCounter: fires at 30 and 45 only, once each, across 50 steps', () => {
  const counter = new StepCounter(30, 1.5)
  const fired = []
  for (let step = 1; step <= 50; step += 1) {
    const kind = counter.step()
    if (kind !== null) fired.push([step, kind])
  }
  assert.deepEqual(fired, [
    [30, 'first'],
    [45, 'second'],
  ])
  assert.equal(counter.steps, 50)
})

test('StepCounter: budget 0 never fires', () => {
  const counter = new StepCounter(0, 1.5)
  for (let step = 1; step <= 50; step += 1) {
    assert.equal(counter.step(), null)
  }
})

test('apply: first reminder lands at the END of the 30th post-execute', async () => {
  const ctx = fakeCtx()
  apply(ctx, { budget: 30 })
  const agent = rootAgent()

  const results = []
  for (let step = 1; step <= 44; step += 1) {
    results.push(await runStep(ctx, agent, { kind: 'allow', additionalContexts: [{ id: 'theirs' }] }))
  }

  assert.equal(results.length, 44)
  // 29 steps clean, then the budget step carries exactly one reminder.
  for (let step = 1; step <= 29; step += 1) {
    assert.deepEqual(results[step - 1], { kind: 'allow', additionalContexts: [{ id: 'theirs' }] })
  }
  const atBudget = results[29]
  assert.equal(atBudget.additionalContexts.length, 2)
  assert.deepEqual(atBudget.additionalContexts[0], { id: 'theirs' })
  const first = atBudget.additionalContexts[1]
  assert.equal(first.role, 'user')
  assert.equal(first.source.kind, 'plugin')
  assert.equal(first.source.plugin, 'step-budget-reminder')
  assert.equal(first.source.form, 'notice')
  assert.equal(
    first.content[0].text,
    '[step-budget] 已到步数预算（30/30）。请收尾：跑相关测试、写 summary、提交；做不完就在 summary 里写清剩余工作。',
  )
  assert.ok(first.id)
  // steps 31..44 carry no further reminder
  for (let step = 31; step <= 44; step += 1) {
    assert.equal(results[step - 1].additionalContexts.length, 1)
  }
})

test('apply: second reminder at 45 only, and never repeated after', async () => {
  const ctx = fakeCtx()
  apply(ctx, { budget: 30, secondFactor: 1.5 })
  const agent = rootAgent()

  const withReminder = []
  for (let step = 1; step <= 60; step += 1) {
    const result = await runStep(ctx, agent, { kind: 'allow' })
    if (result.additionalContexts) {
      withReminder.push([step, result.additionalContexts.at(-1).content[0].text])
    }
  }

  assert.equal(withReminder.length, 2)
  assert.deepEqual(withReminder[0], [
    30,
    '[step-budget] 已到步数预算（30/30）。请收尾：跑相关测试、写 summary、提交；做不完就在 summary 里写清剩余工作。',
  ])
  assert.deepEqual(withReminder[1], [
    45,
    '[step-budget] 已超预算 1.5 倍（45/30）。请立即收尾：跑相关测试、写 summary、提交；做不完就写清剩余工作。',
  ])
})

test('apply: block branch keeps the reminder and downstream feedback', async () => {
  const ctx = fakeCtx()
  apply(ctx, { budget: 3 })
  const agent = rootAgent()

  await runStep(ctx, agent, { kind: 'allow' })
  await runStep(ctx, agent, { kind: 'allow' })
  const blocked = await runStep(ctx, agent, {
    kind: 'block',
    feedback: 'denied by sandbox',
    additionalContexts: [{ id: 'theirs' }],
  })

  assert.equal(blocked.kind, 'block')
  assert.equal(blocked.feedback, 'denied by sandbox')
  assert.deepEqual(blocked.additionalContexts[0], { id: 'theirs' })
  assert.equal(
    blocked.additionalContexts[1].content[0].text,
    '[step-budget] 已到步数预算（3/3）。请收尾：跑相关测试、写 summary、提交；做不完就在 summary 里写清剩余工作。',
  )
})

test('apply: with no budget the handlers pass the downstream result through untouched', async () => {
  const ctx = fakeCtx()
  apply(ctx, {})
  const agent = rootAgent()

  const downstream = { kind: 'allow', additionalContexts: [{ id: 'theirs' }] }
  let result
  for (let step = 1; step <= 50; step += 1) {
    result = await runStep(ctx, agent, downstream)
  }
  assert.equal(result, downstream)
})

test('apply: env budget is used when config has none', async () => {
  const previous = process.env.DSH_STEP_BUDGET
  process.env.DSH_STEP_BUDGET = '4'
  try {
    const ctx = fakeCtx()
    apply(ctx, { secondFactor: 2 })
    const agent = rootAgent()
    const withReminder = []
    for (let step = 1; step <= 10; step += 1) {
      const result = await runStep(ctx, agent)
      if (result.additionalContexts) {
        withReminder.push([step, result.additionalContexts.at(-1).content[0].text])
      }
    }
    assert.deepEqual(
      withReminder.map(([step]) => step),
      [4, 8],
    )
    assert.match(withReminder[1][1], /已超预算 1\.5 倍（8\/4）/)
  } finally {
    if (previous === undefined) delete process.env.DSH_STEP_BUDGET
    else process.env.DSH_STEP_BUDGET = previous
  }
})

test('apply: delegated agents (parentAgent) are not counted', async () => {
  const ctx = fakeCtx()
  apply(ctx, { budget: 3 })
  const parent = rootAgent('parent')
  const child = childAgent(parent)

  for (let step = 1; step <= 10; step += 1) {
    const result = await runStep(ctx, child)
    assert.equal(result.additionalContexts, undefined)
  }
})
