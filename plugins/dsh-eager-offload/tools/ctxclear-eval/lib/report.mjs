// Table / JSON rendering for the ctxclear-eval comparison.

import { sumWindows } from './sim.mjs'

const MILLION = 1e6

/** Turn one accumulator into a display row, with deltas relative to BASE. */
export function rowFromAcc(name, acc, base, extra = {}) {
  const total = sumWindows(acc)
  const totalTokens = total.P + total.out
  const baseTokens = base.P + base.out
  return {
    name,
    P: total.P,
    hit: total.hit,
    miss: total.miss,
    out: total.out,
    totalTokens,
    cost: total.cost,
    costWindows: acc.w.map((w) => w.cost),
    comp: total.comp,
    cleared: total.cleared,
    t2: total.t2,
    t3: total.t3,
    t3write: total.t3write,
    t3reason: total.t3reason,
    req: total.req,
    dTotal: baseTokens ? totalTokens / baseTokens - 1 : 0,
    dCost: base.cost ? total.cost / base.cost - 1 : 0,
    ...extra,
  }
}

/** Build the four required rows in display order. */
export function buildRows({ actual, calibration, base, contextClear, config }) {
  const baseTotal = sumWindows(base)
  const baseRow = rowFromAcc('BASE 不清+' + describeCompaction(config), base, baseTotal)
  const rows = [
    rowFromAcc('实际日志', actual, baseTotal),
    rowFromAcc('模拟:不清+不压缩(校准)', calibration, baseTotal),
    baseRow,
    rowFromAcc('contextClear(压缩联动)', contextClear, baseTotal),
  ]
  return rows
}

function describeCompaction(config) {
  const t = Math.round(config.threshold / 1000)
  const r = Math.round(config.retain / 1000)
  return `${t}k/${r}k压缩`
}

const m = (x) => (x / MILLION).toFixed(2)
const pct = (x) => `${x >= 0 ? '+' : ''}${(100 * x).toFixed(1)}%`

/** Markdown / plain-text table. */
export function formatTable(rows) {
  const lines = []
  lines.push(
    '| 策略 | prompt合计(M) | 未命中(M) | 输出(M) | 总token(M) | 费用$ 合计 (分窗口) | 压缩次数 | 清理条数 | Δ总token | Δ费用 |',
  )
  lines.push('|---|---|---|---|---|---|---|---|---|---|')
  for (const row of rows) {
    const split = row.costWindows.map((c) => c.toFixed(3)).join(' / ')
    lines.push(
      `| ${row.name} | ${m(row.P)} | ${m(row.miss)} | ${m(row.out)} | ${m(row.totalTokens)} | ${row.cost.toFixed(3)} (${split}) | ${row.comp} | ${row.cleared}${row.t2 || row.t3 ? ` (T2=${row.t2}, T3=${row.t3})` : ''} | ${pct(row.dTotal)} | ${pct(row.dCost)} |`,
    )
  }
  return lines.join('\n')
}

/** Structured JSON output. */
export function toJSON(rows, meta) {
  return {
    meta,
    rows: rows.map((row) => ({
      name: row.name,
      promptTokens: row.P,
      missTokens: row.miss,
      hitTokens: row.hit,
      outputTokens: row.out,
      totalTokens: row.totalTokens,
      costUsd: row.cost,
      costByWindowUsd: row.costWindows,
      compactions: row.comp,
      clearedEntries: row.cleared,
      clearedT2: row.t2,
      clearedT3: row.t3,
      clearedT3Write: row.t3write,
      clearedT3Reasoning: row.t3reason,
      requests: row.req,
      deltaTotalTokens: row.dTotal,
      deltaCost: row.dCost,
    })),
  }
}
