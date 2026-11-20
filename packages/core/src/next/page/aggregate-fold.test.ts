import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, GROUP_AGGREGATE_FOLD_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { setInt128 } from "../vector/int128.js";
import { AggregateFold } from "./aggregate-fold.js";
import type { ByteBudget } from "../storage/extents.js";

function funding(cap = Number.MAX_SAFE_INTEGER): ByteBudget & { held: number } {
  return {
    held: 0,
    allocate(bytes) {
      if (this.held + bytes > cap) return false;
      this.held += bytes;
      return true;
    },
    free(bytes) {
      this.held -= bytes;
      expect(this.held).toBeGreaterThanOrEqual(0);
    },
  };
}
function fold(budget: ByteBudget): AggregateFold {
  const value = AggregateFold.create({ kind: "decimal", precision: null, scale: null }, budget);
  if (value === null) throw new Error("Missing fold funding");
  return value;
}
const arbitrary = fc.array(
  fc.record({
    value: fc.bigInt({ min: -(1n << 127n), max: (1n << 127n) - 1n }),
    scale: fc.integer({ min: 0, max: 30 }),
    valid: fc.boolean(),
    special: fc.integer({ min: 0, max: 3 }),
  }),
  { minLength: 1, maxLength: 256 },
);
for (const seed of seedsFor("next-group-aggregate-fold", [0xb2e3, 0xb2e4]))
  it(`folds actual bounded word lanes exactly seed ${String(seed)}`, () => {
    const reach = createReach("A1 raw lane fold", GROUP_AGGREGATE_FOLD_REACH);
    fc.assert(
      fc.property(arbitrary, (input) => {
        reach.beginHistory();
        const validity = new Uint32Array(Math.ceil(input.length / 32));
        input.forEach((v, i) => {
          if (v.valid) validity[i >>> 5] = (validity[i >>> 5] ?? 0) | (1 << (i & 31));
        });
        const budgets = [funding(), funding(), funding()] as const,
          folds = [fold(budgets[0]), fold(budgets[1]), fold(budgets[2])] as const;
        const i32 = new Int32Array(input.length),
          words = new Int32Array(2 * input.length),
          limbs = new Uint32Array(4 * input.length);
        const scales = new Uint16Array(input.length),
          specials = new Uint8Array(input.length);
        let expected32 = 0n,
          expected64 = 0n,
          expectedDecimal = 0n,
          count = 0,
          nan = 0,
          pos = 0,
          neg = 0,
          maxScale = 0;
        for (const value of input)
          if (value.valid && value.special === 0) maxScale = Math.max(maxScale, value.scale);
        input.forEach((value, row) => {
          i32[row] = Number(BigInt.asIntN(32, value.value));
          const int64 = BigInt.asIntN(64, value.value);
          words[row * 2] = Number(BigInt.asIntN(32, int64));
          words[row * 2 + 1] = Number(int64 >> 32n);
          setInt128(limbs, row, value.value);
          scales[row] = value.scale;
          specials[row] = value.special;
          if (!value.valid) return;
          count++;
          expected32 += BigInt(i32[row] ?? 0);
          expected64 += int64;
          if (value.special === 1) nan++;
          else if (value.special === 2) pos++;
          else if (value.special === 3) neg++;
          else expectedDecimal += value.value * 10n ** BigInt(maxScale - value.scale);
        });
        folds[0].int32(i32, validity, input.length);
        folds[1].int64(words, validity, input.length);
        folds[2].decimal(limbs, specials, scales, 0, validity, input.length);
        const a = folds[0].finish(),
          b = folds[1].finish(),
          c = folds[2].finish();
        expect(a?.sum?.coefficient).toBe(expected32);
        expect(b?.sum?.coefficient).toBe(expected64);
        expect(c?.nonNullCount).toBe(count);
        expect(c?.sum).toMatchObject({
          resultScale: maxScale,
          nanCount: nan,
          positiveInfinityCount: pos,
          negativeInfinityCount: neg,
        });
        if (c?.sum === null || c?.sum === undefined) throw new Error("Missing exact decimal fold");
        expect(c.sum.coefficient * 10n ** BigInt(maxScale - c.sum.coefficientScale)).toBe(
          expectedDecimal,
        );
        for (const value of [a, b, c]) expect(value?.nonNullCount).toBe(count);
        folds.forEach((value, i) => {
          value.close();
          value.close();
          expect(budgets[i]?.held).toBe(0);
        });
        for (const label of ["int32 lanes", "int64 lanes", "int128 lanes", "released"] as const)
          reach.reach(label);
        if (input.some((v) => !v.valid)) reach.reach("NULL payload");
        if (nan + pos + neg > 0) reach.reach("special counts");
        if (new Set(input.filter((v) => v.valid && v.special === 0).map((v) => v.scale)).size > 1)
          reach.reach("mixed scales");
        // Independent cap/oversize dimensions occur in EVERY generated history.
        const narrow = funding(1024),
          denied = fold(narrow);
        const two = new Uint32Array(8);
        setInt128(two, 0, 1n);
        setInt128(two, 1, 1n);
        denied.decimal(two, new Uint8Array(2), new Uint16Array([0, 1]), 0, null, 2);
        expect(denied.finish()).toBeNull();
        denied.close();
        expect(narrow.held).toBe(0);
        reach.reach("workspace refusal");
        const bounded = funding(),
          tooWide = fold(bounded);
        tooWide.decimal(two, new Uint8Array(2), new Uint16Array([0, 16383]), 0, null, 2);
        expect(tooWide.finish()).toBeNull();
        tooWide.close();
        expect(bounded.held).toBe(0);
        reach.reach("bounded omission");
        const limit = funding(),
          overflow = fold(limit);
        overflow.int32(new Int32Array(1), null, 65537);
        expect(overflow.finish()).toBeNull();
        overflow.close();
        expect(limit.held).toBe(0);
        reach.reach("whole group bound");
        const carryBudget = funding(),
          carry = fold(carryBudget),
          repeated = new Int32Array(2 * 65536);
        for (let i = 0; i < 65536; i++) {
          repeated[i * 2] = -1;
          repeated[i * 2 + 1] = 0x7fffffff;
        }
        carry.int64(repeated, null, 65536);
        expect(carry.finish()?.sum?.coefficient).toBe(((1n << 63n) - 1n) * 65536n);
        carry.close();
        expect(carryBudget.held).toBe(0);
        reach.reach("exact lane carry");
      }),
      {
        seed,
        numRuns: 32,
        examples: [
          [
            [
              { value: 1n, scale: 0, valid: true, special: 0 },
              { value: -1n, scale: 1, valid: true, special: 0 },
              { value: 0n, scale: 1, valid: false, special: 255 },
              { value: 0n, scale: 0, valid: true, special: 1 },
            ],
          ],
        ],
      },
    );
    reach.assertReached();
  });
