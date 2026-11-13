// @ts-check
/**
 * Splits test files into parts that take about the same time, for the parallel CI runners.
 *
 * Both runners shard whole files: Vitest by a hash of the path into equal counts, Playwright by
 * equal counts in file order. A handful of files here take minutes and most take milliseconds, so
 * equal counts gave one runner more than twice another's work. These parts are chosen by measured
 * duration instead (longest first, each to the part with the least work so far).
 *
 * The weights only steer the split. A missing, stale, or wrong weight makes a part slower; it can
 * never drop a file or run one twice, because every part computes the same deterministic
 * assignment from the same file list and keeps only its own share.
 */

/**
 * Assigns every key to one of `count` parts and returns, for each input position, its zero-based
 * part. `reserved[p]` is work part `p` already has (an extra suite it also runs). Deterministic for
 * a given set of distinct keys and weights, whatever order they arrive in.
 *
 * @param {readonly string[]} keys
 * @param {readonly number[]} weights
 * @param {number} count
 * @param {readonly number[]} [reserved]
 * @returns {number[]}
 */
export function assignParts(keys, weights, count, reserved = []) {
  if (!Number.isInteger(count) || count < 1)
    throw new RangeError(`bad part count ${String(count)}`);
  if (new Set(keys).size !== keys.length) throw new Error("part keys must be distinct");
  const order = keys
    .map((key, index) => ({ key, index, weight: weights[index] ?? 0 }))
    .sort((a, b) => b.weight - a.weight || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  const load = Array.from({ length: count }, (_, p) => reserved[p] ?? 0);
  /** @type {number[]} */
  const part = new Array(keys.length).fill(0);
  for (const item of order) {
    let best = 0;
    let least = Infinity;
    load.forEach((sum, p) => {
      if (sum < least) [best, least] = [p, sum];
    });
    part[item.index] = best;
    load[best] = least + item.weight;
  }
  return part;
}

/**
 * The weight for a file a table has not measured yet: the median of the ones it has.
 *
 * @param {Readonly<Record<string, number>>} profile
 * @returns {number}
 */
export function defaultWeight(profile) {
  const known = Object.values(profile).sort((a, b) => a - b);
  return known[Math.floor(known.length / 2)] ?? 1;
}

/**
 * Parses a one-based `index/count` part (`"2/4"`), or returns null for an unset value.
 *
 * @param {string | undefined} value
 * @returns {{ index: number; count: number } | null}
 */
export function parsePart(value) {
  if (value === undefined || value.trim() === "") return null;
  const match = /^(\d+)\/(\d+)$/u.exec(value.trim());
  const index = Number(match?.[1]),
    count = Number(match?.[2]);
  if (!match || count < 1 || index < 1 || index > count)
    throw new Error(`expected a part like 2/4, got ${JSON.stringify(value)}`);
  return { index, count };
}
