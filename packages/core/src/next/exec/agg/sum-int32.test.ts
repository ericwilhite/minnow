/**
 * SUM over int2/int4 into exact int8 state (states.ts `addFlat32`: no per-row check while a
 * batch cannot push a part past ±2^52, a rescan when it might, the checked loop and the carry
 * near the limit) against exact BigInt sums. Law: PostgreSQL's SUM(int4) is the exact sum, an
 * int8 (int4_sum), whatever order or batching the rows arrive in.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { createReach } from "../../../testing/reach.js";
import { mulberry32, seedsFor } from "../../../testing/seeds.js";
import type { Vector } from "../../vector/vector.js";
import {
  I32,
  I64,
  UNCHARGED,
  constant,
  dictionary,
  numbers,
  valueAt,
} from "../kernels/test-support.js";
import { IDENTITY } from "../kernels/vectors.js";
import { accumulatorFor } from "./states.js";

const SEEDS = seedsFor("next-agg-sum-int32", [0x51, 0x52]);
const MAX = 2 ** 31 - 1;

describe("SUM(int4) is exact across the unchecked, rescanned and checked loops (generated)", () => {
  it.each(SEEDS)(
    "flat, NULL-bearing, dictionary and constant batches, toward ±2^52 per group (seed %i)",
    (seed) => {
      const reach = createReach("next-agg-sum-int32", [
        "unchecked batches only",
        "past 2^52 − 2^42 (rescans, checked loop)",
        "past 2^52 (carry)",
        "past 2^53",
        "past 2^53 in flat batches only",
        "past 2^53 after converted batches",
        "other int32 forms between flat batches",
      ] as const);
      fc.assert(
        fc.property(
          fc.constantFrom(1, 2, 50),
          // Rows: a few batches, or enough of ±(2^31 − 1) to pass 2^52 in one group, and 2^53
          // (where a float64 part would stop being exact without the limit and the carry).
          fc.constantFrom(5_000, 300_000, 2_300_000, 4_600_000),
          fc.constantFrom(1, -1),
          // Flat batches only (long unchecked runs); other int32 forms among them (the
          // converted, checked path, which also moves the bound); or converted batches for the
          // first half, then flat ones (a part the converted path took near 2^52 before any
          // flat batch ran unchecked).
          fc.constantFrom("flat", "mixed", "converted then flat"),
          fc.integer(),
          (someGroups, rows, sign, forms, inner) => {
            // Only one group's 4.6M rows pass 2^53 (rows spread over groups would not).
            const groups = rows > 4_000_000 ? 1 : someGroups;
            const flatOnly = forms === "flat";
            reach.beginHistory();
            const random = mulberry32(inner);
            const sum = accumulatorFor("sum", I64, I32, UNCHARGED);
            sum.grow(groups);
            const expected = Array.from({ length: groups }, () => 0n);
            const seen = Array.from({ length: groups }, () => false);
            let largest = 0n;
            const gids = new Int32Array(2048);
            let mixed = false;
            // "converted then flat": converted batches until a part is just below 2^52.
            let converting = forms === "converted then flat";
            for (let done = 0; done < rows;) {
              const count = Math.min(rows - done, 1 + Math.floor(random() * 2048));
              done += count;
              // Mostly the largest values (one sign), so sums climb toward the limit.
              const extreme = rows > 1_000_000 || random() < 0.5;
              const values: Array<number | null> = [];
              for (let r = 0; r < count; r++) {
                // Few groups when climbing: one group takes most rows.
                gids[r] = groups === 1 || random() < 0.8 ? 0 : Math.floor(random() * groups);
                if (random() < 0.02) values.push(null);
                else if (extreme) values.push(sign * MAX);
                else values.push(Math.floor((random() - 0.5) * 2 * MAX));
              }
              const form =
                forms === "flat" ? 0 : forms === "mixed" ? random() : converting ? 0.9 : 0;
              let vector: Vector;
              if (form < 0.85) vector = numbers(I32, values);
              else if (form < 0.95) {
                // A dictionary of int32 values (the converted path).
                const entries = [...new Set(values.filter((v) => v !== null))];
                vector = dictionary(
                  numbers(I32, entries.length === 0 ? [0] : entries),
                  values.map((v) => (v === null ? null : entries.indexOf(v))),
                );
                mixed = true;
              } else {
                const one = values.find((v) => v !== null) ?? 0;
                values.fill(one);
                vector = constant(numbers(I32, [one]), count);
                mixed = true;
              }
              sum.update(vector, gids, IDENTITY, count, false);
              // The batch's per-group sums are exact in a double (below 2^42): one BigInt add per
              // group per batch.
              const batch = new Float64Array(groups);
              for (let r = 0; r < count; r++) {
                const v = values[r];
                if (v === null || v === undefined) continue;
                const g = gids[r] ?? 0;
                batch[g] = (batch[g] ?? 0) + v;
                seen[g] = true;
              }
              for (let g = 0; g < groups; g++) {
                const total = (expected[g] ?? 0n) + BigInt(batch[g] ?? 0);
                expected[g] = total;
                const size = total < 0n ? -total : total;
                if (size > largest) largest = size;
              }
              if (largest >= 2n ** 52n - 2n ** 43n) converting = false;
            }
            const out = sum.emit(0, groups);
            for (let g = 0; g < groups; g++) {
              const value = valueAt(out, g);
              if (!(seen[g] ?? false)) expect(value).toBeNull();
              else expect(BigInt(value as number | bigint), `group ${String(g)}`).toBe(expected[g]);
            }
            sum.close();
            if (largest > 2n ** 52n) reach.reach("past 2^52 (carry)");
            if (largest > 2n ** 53n) reach.reach("past 2^53");
            if (largest > 2n ** 53n && flatOnly) reach.reach("past 2^53 in flat batches only");
            if (largest > 2n ** 53n && forms === "converted then flat")
              reach.reach("past 2^53 after converted batches");
            if (largest > 2n ** 52n - 2n ** 42n)
              reach.reach("past 2^52 − 2^42 (rescans, checked loop)");
            else reach.reach("unchecked batches only");
            if (mixed) reach.reach("other int32 forms between flat batches");
          },
        ),
        { seed, numRuns: 36 },
      );
      reach.assertReached();
    },
    300_000,
  );
});
