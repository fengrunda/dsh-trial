import test from 'node:test'
import assert from 'node:assert/strict'

import { PrefixCache, floorTo128, BLOCK } from '../lib/cache.mjs'

test('floorTo128 rounds down to the 128-token cache block', () => {
  assert.equal(BLOCK, 128)
  assert.equal(floorTo128(0), 0)
  assert.equal(floorTo128(127), 0)
  assert.equal(floorTo128(128), 128)
  assert.equal(floorTo128(129), 128)
  assert.equal(floorTo128(1000), 896)
  assert.equal(floorTo128(12_345), 12_288)
})

test('prefix units match only while their prefix survives', () => {
  const cache = new PrefixCache()
  cache.add(2, 1000) // persisted at input end of request 1
  cache.add(3, 1200) // persisted at output end of request 1
  cache.add(5, 3000)

  // Anything ending at or before the current surface length can match.
  assert.equal(cache.best(1), 0)
  assert.equal(cache.best(2), 1000)
  assert.equal(cache.best(4), 1200)
  assert.equal(cache.best(9), 3000)
})

test('editing item i invalidates every unit ending after i', () => {
  const cache = new PrefixCache()
  cache.add(2, 1000)
  cache.add(4, 2000)
  cache.add(6, 3000)
  cache.invalidateFrom(4)
  assert.deepEqual(cache.units.map((u) => u.idx), [2, 4])
  assert.equal(cache.best(6), 2000)

  cache.invalidateFrom(1)
  assert.deepEqual(cache.units, [])
  assert.equal(cache.best(6), 0)
})

test('hit is floored to a block, capped at the prompt, and honours the sys baseline', () => {
  const cache = new PrefixCache()
  cache.add(10, 2000)
  assert.equal(cache.hit(10, 1000), 896) // min(2000,1000) -> 896
  assert.equal(cache.hit(10, 5000), 1920) // min(2000,5000) -> 1920
  assert.equal(cache.hit(3, 5000, 640), 640) // unit does not prefix, baseline does
  assert.equal(cache.hit(3, 500, 640), 384) // still capped at prompt tokens
})

test('reset replaces the unit set (post-compaction sys+tools only)', () => {
  const cache = new PrefixCache()
  cache.add(4, 4000)
  cache.add(9, 9000)
  cache.reset([{ idx: 1, tok: 96 }])
  assert.equal(cache.best(9), 96)
  assert.equal(cache.hit(9, 9000), 0) // 96 floors to 0
})
