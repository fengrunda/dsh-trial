import test from 'node:test'
import assert from 'node:assert/strict'

import {
  isPeak,
  pricesFor,
  requestCost,
  normalizePrices,
  DEFAULT_PRICES,
} from '../lib/prices.mjs'

const at = (iso) => Date.parse(iso)

test('weekday fixtures are what the boundary tests assume', () => {
  assert.equal(new Date(at('2026-10-05T00:00:00Z')).getUTCDay(), 1) // Monday
  assert.equal(new Date(at('2026-10-08T00:00:00Z')).getUTCDay(), 4) // Thursday
  assert.equal(new Date(at('2026-10-10T00:00:00Z')).getUTCDay(), 6) // Saturday
  assert.equal(new Date(at('2026-10-11T00:00:00Z')).getUTCDay(), 0) // Sunday
})

test('peak windows are UTC Mon-Fri 01:00-04:00 and 06:00-10:00', () => {
  const mon = '2026-10-05'
  assert.equal(isPeak(at(`${mon}T00:59:59Z`)), false)
  assert.equal(isPeak(at(`${mon}T01:00:00Z`)), true) // start inclusive
  assert.equal(isPeak(at(`${mon}T03:59:59Z`)), true)
  assert.equal(isPeak(at(`${mon}T04:00:00Z`)), false) // end exclusive
  assert.equal(isPeak(at(`${mon}T05:00:00Z`)), false) // gap between windows
  assert.equal(isPeak(at(`${mon}T06:00:00Z`)), true)
  assert.equal(isPeak(at(`${mon}T09:59:59Z`)), true)
  assert.equal(isPeak(at(`${mon}T10:00:00Z`)), false)
  assert.equal(isPeak(at(`${mon}T23:30:00Z`)), false)
})

test('weekends are off-peak even inside the clock windows', () => {
  assert.equal(isPeak(at('2026-10-10T02:00:00Z')), false) // Saturday
  assert.equal(isPeak(at('2026-10-11T07:00:00Z')), false) // Sunday
  assert.equal(isPeak(at('2026-10-09T02:00:00Z')), true) // Friday still peak
})

test('off-peak is exactly half the peak rate', () => {
  const peak = pricesFor(at('2026-10-05T02:00:00Z'))
  const off = pricesFor(at('2026-10-05T05:00:00Z'))
  assert.deepEqual(peak, DEFAULT_PRICES.peak)
  assert.equal(off.hit, peak.hit / 2)
  assert.equal(off.miss, peak.miss / 2)
  assert.equal(off.out, peak.out / 2)
})

test('requestCost applies the timestamp-appropriate rate', () => {
  const usage = { hit: 1_000_000, miss: 1_000_000, out: 1_000_000 }
  const peakCost = requestCost(usage, at('2026-10-08T01:00:00Z'))
  const offCost = requestCost(usage, at('2026-10-08T05:00:00Z'))
  assert.equal(peakCost, 0.006 + 0.3 + 1.2)
  assert.equal(offCost, peakCost / 2)
})

test('normalizePrices derives off-peak and rejects junk', () => {
  const half = normalizePrices({ hit: 0.01, miss: 0.4, out: 2 })
  assert.deepEqual(half.offpeak, { hit: 0.005, miss: 0.2, out: 1 })
  const explicit = normalizePrices({ peak: { hit: 1, miss: 2, out: 3 }, offpeak: { hit: 4, miss: 5, out: 6 } })
  assert.deepEqual(explicit.offpeak, { hit: 4, miss: 5, out: 6 })
  assert.throws(() => normalizePrices({ hit: -1, miss: 1, out: 1 }), /invalid price/)
  assert.throws(() => normalizePrices('nope'), /JSON object/)
})
