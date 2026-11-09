import { wideRecord } from "./lib/wide-layout-kernel.mts";
/** Fixed-workload baseline validation: byte equality, local dictionary codes and PostgreSQL's
 * ordered float8 transitions. Independent Map/Set models, not timing assertions. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach } from "../packages/core/src/testing/reach.js";
import {
  highcard,
  distinct,
  wide,
  wideSplit,
  wideNative,
  type Bytes,
  type LightInput,
  type LightResult,
} from "./lib/aggregation-light-kernels.mts";

const collision = ["sol-collision-64004", "sol-collision-217575"] as const;
const encoder = new TextEncoder();
function at<T>(values: ArrayLike<T>, i: number): T {
  const v = values[i];
  if (v === undefined) throw new Error("fixture index");
  return v;
}
function bytes(values: readonly string[]): Bytes {
  const encoded = values.map((v) => encoder.encode(v)),
    offsets = new Int32Array(values.length + 1);
  encoded.forEach((v, i) => {
    offsets[i + 1] = at(offsets, i) + v.length;
  });
  const raw = new Uint8Array(at(offsets, values.length));
  encoded.forEach((v, i) => raw.set(v, at(offsets, i)));
  return { bytes: raw, offsets };
}
function order(rows: unknown[][]): unknown[][] {
  return rows.sort((a, b) => {
    for (let k = 0; k < 3; k++) {
      if (String(a[k]) < String(b[k])) return -1;
      if (String(a[k]) > String(b[k])) return 1;
    }
    return 0;
  });
}
function sum(values: Float64Array, indices: readonly number[], initial = -0): number {
  let total = initial;
  for (const i of indices) total += at(values, i);
  return total;
}
function extreme(values: Float64Array, indices: readonly number[], max: boolean): number {
  let best = max ? -Infinity : Infinity;
  for (const i of indices) {
    const v = at(values, i);
    if (max ? v >= best : v <= best) best = v;
  }
  return best;
}
function reference(input: LightInput, labels: ReadonlyArray<readonly string[]>): LightResult {
  const groups = new Map<string, number[]>();
  for (let i = 0; i < input.rows; i++) {
    const key = JSON.stringify(labels.slice(0, 3).map((c) => at(c, i)));
    const rows = groups.get(key) ?? [];
    rows.push(i);
    groups.set(key, rows);
  }
  const rows: unknown[][] = [],
    sxx = Array.from({ length: 5 }, () => new Float64Array(1000));
  let gid = 0;
  for (const indices of groups.values()) {
    const first = at(indices, 0),
      values: unknown[] = [
        at(at(labels, 0), first),
        at(at(labels, 1), first),
        at(at(labels, 2), first),
        indices.length,
      ];
    for (let c = 0; c < 5; c++) values.push(sum(at(input.n, c), indices, 0));
    for (let c = 5; c < 10; c++) {
      let n = 0,
        sx = 0,
        q = 0;
      for (const i of indices) {
        const old = n;
        n++;
        sx += at(at(input.n, c), i);
        if (old > 0) {
          const tmp = at(at(input.n, c), i) * n - sx;
          q += (tmp * tmp) / (n * old);
        }
      }
      at(sxx, c - 5)[gid] = q;
      values.push(sx / n);
    }
    for (let c = 10; c < 14; c++) values.push(extreme(at(input.n, c), indices, c % 2 === 1));
    values.push(
      sum(at(input.n, 14), indices),
      sum(at(input.n, 15), indices),
      sum(at(input.n, 16), indices, 0) / indices.length,
      indices.reduce((n, i) => n + at(input.valid, i), 0),
    );
    values.push(
      indices.map((i) => at(at(labels, 3), i)).sort()[0],
      extreme(at(input.n, 19), indices, true),
    );
    rows.push(values);
    gid++;
  }
  return { rows: order(rows), sxx };
}

it.each(seedsFor("aggregation-light-baselines", [0xb401, 0xb402]))(
  "matches independent models across collisions, local code changes, sums and top100 (seed %i)",
  (seed) => {
    const reach = createReach("aggregation light baseline", [
      "hash collision",
      "byte tails",
      "signed int keys",
      "top100",
      "dictionary changes",
      "duplicate codes",
      "positive Sxx",
      "float negative zero",
    ] as const);
    fc.assert(
      fc.property(
        fc.array(fc.integer(), { minLength: 256, maxLength: 256 }),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 2, max: 33 }),
        (data, long, zeros, chunk) => {
          reach.beginHistory();
          const N = long ? 256 : 32,
            keys = Float64Array.from({ length: N }, (_, i) =>
              i === 0 ? -2147483648 : i === 1 ? 2147483647 : at(data, i),
            ),
            values = Float64Array.from({ length: N }, (_, i) => (at(data, i) % 100) / 4);
          const grouped = new Map<number, { n: number; sum: number }>();
          for (let i = 0; i < N; i++) {
            const key = at(keys, i),
              old = grouped.get(key) ?? { n: 0, sum: -0 };
            old.n++;
            old.sum += at(values, i);
            grouped.set(key, old);
          }
          const highExpected = [...grouped]
            .map(([key, g]) => [key, g.n, g.sum])
            .sort((a, b) => at(b, 1) - at(a, 1) || at(a, 0) - at(b, 0))
            .slice(0, 100);
          expect(
            highcard({ rows: N, n: [keys, values], b: [], valid: new Uint8Array(0) }).rows,
          ).toEqual(highExpected);
          reach.reach("signed int keys");
          if (grouped.size > 100) reach.reach("top100");
          const strings = Array.from({ length: N }, (_, i) =>
              i === 0 || i === 2
                ? collision[0]
                : i === 1
                  ? collision[1]
                  : i % 7 === 0
                    ? ""
                    : i % 7 === 1
                      ? "α\u0000"
                      : `acct-${String(at(data, i) % 19)}`,
            ),
            other = [...strings].reverse();
          expect(
            distinct({
              rows: N,
              n: [],
              b: [bytes(strings), bytes(other)],
              valid: new Uint8Array(0),
            }).rows,
          ).toEqual([[new Set(strings).size, new Set(other).size]]);
          reach.reach("hash collision");
          reach.reach("byte tails");
          const labels = [2, 3, 4, 12].map((card, c) =>
            Array.from(
              { length: N },
              (_, i) =>
                `d${String(c)}_${String(i < 2 ? 0 : (at(data, (i + c * 11) % 256) >>> 0) % card)}`,
            ),
          );
          const numbers = Array.from({ length: 20 }, (_, c) =>
            Float64Array.from({ length: N }, (_, i) =>
              zeros && (c === 9 || c === 14 || c === 15)
                ? -0
                : i < 2
                  ? (c + 1) * (i + 1)
                  : c === 19
                    ? 1704067200000000 + i * 1000
                    : c < 5 || c === 12 || c === 13 || c === 16
                      ? (at(data, (i + c * 17) % 256) % 100) | 0
                      : (at(data, (i + c * 17) % 256) % 100) / 4,
            ),
          );
          const input: LightInput = {
            rows: N,
            n: numbers,
            b: labels.map(bytes),
            valid: Uint8Array.from({ length: N }, (_, i) => (i % 3 === 1 ? 0 : 1)),
          };
          let duplicate = false;
          input.dictionaries = labels.map((column, c) => {
            const domain = Array.from(
                { length: [2, 3, 4, 12][c] ?? 0 },
                (_, i) => `d${String(c)}_${String(i)}`,
              ),
              segments = [];
            for (let start = 0; start < N; start += chunk) {
              let entries = [...domain, at(domain, 0)];
              if ((start / chunk) % 2 >= 1) entries = entries.reverse();
              const length = Math.min(chunk, N - start),
                codes = Int32Array.from({ length }, (_, j) => {
                  const matches = entries
                    .map((v, id) => (v === at(column, start + j) ? id : -1))
                    .filter((id) => id >= 0);
                  return at(matches, j % matches.length);
                });
              if (start === 0 && at(codes, 0) !== at(codes, 1) && at(column, 0) === at(column, 1))
                duplicate = true;
              segments.push({ start, codes, entries: bytes(entries) });
            }
            return segments;
          });
          const expected = reference(input, labels);
          if (zeros) {
            for (const row of expected.rows) {
              expect(Object.is(row[18], -0)).toBe(true);
              expect(Object.is(row[19], -0)).toBe(true);
              expect(Object.is(row[13], 0)).toBe(true);
            }
            reach.reach("float negative zero");
          }
          expect(wide(input)).toEqual(expected);
          expect(wideSplit(input)).toEqual(expected);
          expect(wideNative(input)).toEqual(expected);
          expect(wideRecord(input)).toEqual(expected);
          expect(at(expected.sxx ?? [], 0).some((v) => v > 0)).toBe(true);
          reach.reach("positive Sxx");
          reach.reach("dictionary changes");
          if (duplicate) reach.reach("duplicate codes");
        },
      ),
      { seed, numRuns: 60 },
    );
    reach.assertReached();
  },
);
