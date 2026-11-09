/** COUNT(DISTINCT) pairs use SQL equality and ignore NULL; storage strategy cannot change it. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach } from "../../../testing/reach.js";
import type { Vector } from "../../vector/vector.js";
import {
  I32,
  F64,
  TEXT,
  decimal,
  decimals,
  numbers,
  texts,
  dictionary,
} from "../kernels/test-support.js";
import { DistinctSet } from "./distinct.js";
const D = decimal(null, null);
function budget(failAt = 0) {
  let allocations = 0;
  return {
    used: 0,
    peak: 0,
    failed: false,
    allocate(n: number) {
      if (++allocations === failAt) {
        this.failed = true;
        return false;
      }
      this.used += n;
      this.peak = Math.max(this.peak, this.used);
      return true;
    },
    free(n: number) {
      this.used -= n;
    },
  };
}

it.each(seedsFor("next-distinct-bitmap", [0xe241, 0xe242]))(
  "preserves first pair selection across forms and group growth (seed %i)",
  (seed) => {
    const reach = createReach("distinct bitmap", [
      "scalar",
      "grouped",
      "late-groups",
      "many-first",
      "selected",
      "null",
      "dictionary",
      "constant",
      "decimal",
      "float",
      "word-boundary",
      "memory-saving",
      "released",
    ]);
    fc.assert(
      fc.property(
        fc.constantFrom(1, 10, 63, 64, 65, 100),
        fc.constantFrom(31, 32, 33, 63, 64, 65, 257),
        fc.constantFrom(31, 128, 2048),
        fc.constantFrom("integer", "text", "dictionary", "constant", "decimal", "float"),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 1000 }),
        (groups, domain, chunk, form, scalar, selected, salt) => {
          reach.beginHistory();
          const left = budget(),
            right = budget(),
            type =
              form === "decimal" ? D : form === "float" ? F64 : form === "integer" ? I32 : TEXT;
          const a = new DistinctSet(type, left, 2048),
            b = new DistinctSet(type, right, 2048, true),
            seen = new Set<string>(),
            valuesSeen = new Set<string>();
          let observedHigh = false;
          const ao = { rows: new Uint16Array(2048), count: 0 },
            bo = { rows: new Uint16Array(2048), count: 0 };
          try {
            for (let from = 0; from < 4097; from += chunk) {
              const n = Math.min(chunk, 4097 - from),
                raw = Array.from({ length: n }, (_, j) => {
                  const i = from + j,
                    k = (i * 17 + salt) % domain;
                  if (form === "constant") return salt % 3 === 0 ? null : "fixed";
                  if (i % 29 === 0) return null;
                  if (form === "float")
                    return k % 11 === 0
                      ? NaN
                      : k % 11 === 1
                        ? -0
                        : k % 11 === 2
                          ? 0
                          : k % 11 === 3
                            ? Infinity
                            : k % 11 === 4
                              ? -Infinity
                              : k;
                  return form === "text" || form === "dictionary" ? `k${String(k)}-α-O'Brien` : k;
                }),
                gids = Int32Array.from({ length: n }, (_, j) =>
                  from + j < 512 ? (from + j) % Math.min(groups, 10) : ((from + j) * 131) % groups,
                );
              const rows = selected
                ? {
                    rows: Uint16Array.from(
                      Array.from({ length: n }, (_, j) => j).filter((j) => (from + j) % 7 !== 0),
                    ),
                    count: 0,
                  }
                : null;
              if (rows) rows.count = rows.rows.length;
              let vector: Vector;
              if (form === "dictionary")
                vector = dictionary(
                  texts(Array.from({ length: domain }, (_, k) => `k${String(k)}-α-O'Brien`)),
                  raw.map((v) => (v === null ? null : Number(String(v).split("-")[0]?.slice(1)))),
                );
              else if (form === "constant")
                vector = {
                  form: "constant",
                  type: TEXT,
                  length: n,
                  value: texts([raw[0] === null ? null : "fixed"]),
                };
              else if (form === "decimal")
                vector = decimals(
                  D,
                  raw.map((v, j) =>
                    v === null
                      ? null
                      : ([
                          BigInt(Number(v)) * ((from + j) % 2 ? 100n : 10n),
                          (from + j) % 2 ? 2 : 1,
                        ] as const),
                  ),
                );
              else if (form === "integer" || form === "float")
                vector = numbers(
                  type,
                  raw.map((v) => (v === null ? null : Number(v))),
                );
              else vector = texts(raw.map((v) => (v === null ? null : String(v))));
              const expected: number[] = [];
              for (const row of rows === null
                ? Array.from({ length: n }, (_, j) => j)
                : rows.rows) {
                const v = raw[row];
                if (v === null) {
                  reach.reach("null");
                  continue;
                }
                const normalized =
                  typeof v === "number"
                    ? Number.isNaN(v)
                      ? "NaN"
                      : v === 0
                        ? "0"
                        : String(v)
                    : String(v);
                valuesSeen.add(normalized);
                if (!scalar && from + row >= 512 && (gids[row] ?? 0) >= 64) observedHigh = true;
                const key = `${String(scalar ? 0 : gids[row])}|${normalized}`;
                if (!seen.has(key)) {
                  seen.add(key);
                  expected.push(row);
                }
              }
              a.filter(vector, gids, n, rows, scalar, ao);
              b.filter(vector, gids, n, rows, scalar, bo);
              expect(Array.from(ao.rows.subarray(0, ao.count))).toEqual(expected);
              expect(Array.from(bo.rows.subarray(0, bo.count))).toEqual(expected);
            }
            if (!scalar && groups === 10 && domain === 257 && form === "text" && !selected) {
              expect(right.used).toBeLessThan(left.used);
              reach.reach("memory-saving");
            }
            if (!scalar && groups > 64 && observedHigh)
              reach.reach(chunk < 512 ? "late-groups" : "many-first");
            reach.reach(scalar ? "scalar" : "grouped");
            if (selected) reach.reach("selected");
            if (valuesSeen.size > 32 && (scalar || groups <= 64 || chunk < 512))
              reach.reach("word-boundary");
            if (
              form === "dictionary" ||
              form === "constant" ||
              form === "decimal" ||
              form === "float"
            )
              reach.reach(form);
          } finally {
            a.close();
            b.close();
            expect(left.used).toBe(0);
            expect(right.used).toBe(0);
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [10, 257, 128, "text", false, false, 1],
          [65, 65, 31, "dictionary", false, true, 1],
          [100, 257, 2048, "integer", false, false, 1],
          [1, 33, 128, "constant", true, false, 0],
          [63, 64, 128, "decimal", false, true, 1],
          [64, 257, 128, "float", false, false, 1],
        ],
      },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("next-distinct-bitmap-refusal", [0xe243, 0xe244]))(
  "returns every charge after generated constructor/growth refusals (seed %i)",
  (seed) => {
    const reach = createReach("distinct bitmap refusal", [
      "constructor",
      "growth",
      "complete",
      "released",
    ]);
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 128 }), (failAt) => {
        reach.beginHistory();
        const charge = budget(failAt);
        let set: DistinctSet | null = null;
        try {
          set = new DistinctSet(TEXT, charge, 2048, true);
          const out = { rows: new Uint16Array(2048), count: 0 };
          for (let batch = 0; batch < 8; batch++) {
            const vector = texts(
                Array.from({ length: 2048 }, (_, i) => `key-${String(batch * 2048 + i)}`),
              ),
              gids = Int32Array.from({ length: 2048 }, (_, i) => i % 10);
            set.filter(vector, gids, 2048, null, false, out);
          }
          reach.reach("complete");
        } catch (error) {
          expect(error).toBeInstanceOf(RangeError);
          expect(charge.failed).toBe(true);
          reach.reach(set === null ? "constructor" : "growth");
        } finally {
          set?.close();
          expect(charge.used).toBe(0);
          reach.reach("released");
        }
      }),
      { seed, numRuns: 32, examples: [[0], [1], [64]] },
    );
    reach.assertReached();
  },
);
