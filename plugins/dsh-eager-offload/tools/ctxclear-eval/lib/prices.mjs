// deepseek-flash official pricing with per-request peak / off-peak selection.
//
// Peak (USD per 1M tokens): hit 0.006 / miss 0.30 / out 1.20
// Peak window: UTC Mon–Fri 01:00–04:00 and 06:00–10:00 (Chinese public holidays
// are deliberately ignored — no holiday calendar is consulted).
// Off-peak = every other instant, at exactly half the peak rates.

/** Official deepseek-flash peak prices (USD per 1M tokens). */
export const DEFAULT_PEAK_PRICES = Object.freeze({ hit: 0.006, miss: 0.3, out: 1.2 })

/** Full price table: peak plus the derived half-price off-peak rates. */
export const DEFAULT_PRICES = Object.freeze({
  peak: DEFAULT_PEAK_PRICES,
  offpeak: Object.freeze({
    hit: DEFAULT_PEAK_PRICES.hit / 2,
    miss: DEFAULT_PEAK_PRICES.miss / 2,
    out: DEFAULT_PEAK_PRICES.out / 2,
  }),
})

/**
 * True when `timestamp` (epoch ms) falls in a UTC peak window:
 * Monday–Friday, 01:00–04:00 or 06:00–10:00 UTC. Boundary instants belong to
 * the interval they start (01:00 peak, 04:00 not, 06:00 peak, 10:00 not).
 */
export function isPeak(timestamp) {
  const d = new Date(timestamp)
  const day = d.getUTCDay()
  if (day === 0 || day === 6) return false
  const minutes = d.getUTCHours() * 60 + d.getUTCMinutes()
  return (minutes >= 60 && minutes < 240) || (minutes >= 360 && minutes < 600)
}

/** Price tuple for a request at `timestamp`. */
export function pricesFor(timestamp, prices = DEFAULT_PRICES) {
  return isPeak(timestamp) ? prices.peak : prices.offpeak
}

/** USD cost of one request's usage `{hit, miss, out}` at `timestamp`. */
export function requestCost(usage, timestamp, prices = DEFAULT_PRICES) {
  const p = pricesFor(timestamp, prices)
  return (usage.hit * p.hit + usage.miss * p.miss + usage.out * p.out) / 1e6
}

function requirePrice(value, label) {
  const n = Number(value)
  if (!Number.isFinite(n) || n < 0) throw new Error(`invalid price for ${label}: ${value}`)
  return n
}

/**
 * Normalise a user-supplied `--prices` value.
 *
 * Accepted:
 *   {hit,miss,out}                  -> peak rates, off-peak = half
 *   {peak:{...}, offpeak:{...}}     -> explicit both
 *   {peak:{...}}                    -> off-peak = half of the given peak
 */
export function normalizePrices(raw) {
  if (!raw || typeof raw !== 'object') throw new Error('--prices must be a JSON object')
  if (raw.peak || raw.offpeak) {
    const peak = raw.peak ?? DEFAULT_PEAK_PRICES
    const peakNorm = {
      hit: requirePrice(peak.hit, 'peak.hit'),
      miss: requirePrice(peak.miss, 'peak.miss'),
      out: requirePrice(peak.out, 'peak.out'),
    }
    const off = raw.offpeak ?? { hit: peakNorm.hit / 2, miss: peakNorm.miss / 2, out: peakNorm.out / 2 }
    return {
      peak: peakNorm,
      offpeak: {
        hit: requirePrice(off.hit, 'offpeak.hit'),
        miss: requirePrice(off.miss, 'offpeak.miss'),
        out: requirePrice(off.out, 'offpeak.out'),
      },
    }
  }
  const peak = {
    hit: requirePrice(raw.hit, 'hit'),
    miss: requirePrice(raw.miss, 'miss'),
    out: requirePrice(raw.out, 'out'),
  }
  return { peak, offpeak: { hit: peak.hit / 2, miss: peak.miss / 2, out: peak.out / 2 } }
}
