// DeepSeek prefix-cache model (ported from /tmp/tokaudit/agemask-sim/sim.js).
//
// Each request persists one prefix unit at the end of its input and one at the
// end of its output (i.e. after the assistant item it produced). A later
// request can only hit a unit whose prefix is a prefix of its own input; editing
// item i invalidates every unit that ends after i. Hits are reported rounded
// down to a 128-token block (real logs only ever contain multiples of 128).

/** Cache block granularity in tokens. */
export const BLOCK = 128

/** Round down to the cache block size. */
export function floorTo128(tokens) {
  return Math.floor(tokens / BLOCK) * BLOCK
}

/** Ordered set of persisted prefix units: `{idx, tok}`. */
export class PrefixCache {
  constructor() {
    /** @type {{idx:number,tok:number}[]} */
    this.units = []
  }

  /** Replace all units (e.g. right after a compaction: only sys+tools survive). */
  reset(units = []) {
    this.units = units
  }

  /** Persist a prefix unit ending after `idx` surface items with `tok` tokens. */
  add(idx, tok) {
    this.units.push({ idx, tok })
  }

  /** Drop units that end after item `m` (item `m` was edited). */
  invalidateFrom(m) {
    this.units = this.units.filter((u) => u.idx <= m)
  }

  /**
   * Token count of the longest persisted unit that still prefixes a surface of
   * `surfaceLen` items. `baseline` (sys+tools) is always considered available.
   */
  best(surfaceLen, baseline = 0) {
    let best = baseline
    for (const u of this.units) if (u.idx <= surfaceLen && u.tok > best) best = u.tok
    return best
  }

  /** Cache-read tokens for a request of `promptTokens` over `surfaceLen` items. */
  hit(surfaceLen, promptTokens, baseline = 0) {
    return floorTo128(Math.min(this.best(surfaceLen, baseline), promptTokens))
  }
}
