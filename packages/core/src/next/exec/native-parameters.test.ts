/** Law: TriggerBodyHost borrows exact native parameters until its producer retires; only
 * internal clock/NULL slots allocate. No caller payload is copied, coerced or freed. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_PARAMETER_REACH,
  NATIVE_PARAMETER_VALIDATION_REACH,
} from "../../testing/reach.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { Governor } from "../memory/governor.js";
import type { Snapshot } from "../access/snapshot.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { int64At } from "../vector/int64.js";
import type { FlatVector } from "../vector/vector.js";
import { NativeParameterBinding, nativeParameterMinimumBytes } from "./native-parameters.js";
import { ParameterBindError } from "./errors.js";
import { flatBytes } from "./agg/values.js";
import {
  I32,
  I64,
  TS,
  TEXT,
  BOOL,
  numbers,
  bigints,
  decimals,
  decimal,
  texts,
  evaluate,
} from "./kernels/test-support.js";
import { QueryRuntime } from "./run.js";
import type { ChunkReads } from "./scan.js";
function plan(types: readonly PhysicalType[], clock: boolean, fts: boolean): PhysicalPlan {
  return {
    root: { id: 1, op: "values", rows: [], output: [] },
    columns: [],
    parameters: [...types, ...(clock ? [TS] : []), I32, ...(fts ? [TEXT] : [])],
    placeholderCount: types.length,
    clockSlot: clock ? types.length : null,
    ...(fts ? { ftsParameters: [{ source: 0, slot: types.length + (clock ? 2 : 1) }] } : {}),
    initPlans: [],
    catalogVersion: 0,
    volatile: false,
  };
}
function account(limit = Infinity) {
  let held = 0;
  return {
    allocate(n: number) {
      if (held + n > limit) return false;
      held += n;
      return true;
    },
    free(n: number) {
      held -= n;
      expect(held).toBeGreaterThanOrEqual(0);
    },
    get held() {
      return held;
    },
  };
}
function sample(kind: number, n: number, width: number): FlatVector {
  switch (kind) {
    case 0:
      return numbers(I32, [n]);
    case 1:
      return bigints(I64, [2n ** 60n + BigInt(n)]);
    case 2:
      return bigints(TS, [BigInt(n) * 1000000n + 123456n]);
    case 3:
      return decimals(decimal(null, null), [[BigInt(n) * 10n ** 50n + 123n, 7]], "wide");
    case 4:
      return decimals(decimal(null, null), [[BigInt(n) * 10n ** 25n + 123n, 7]], "int128");
    case 5:
      return texts([`雪${String(n)}${"x".repeat(width)}`]);
    case 6:
      return numbers(BOOL, [n & 1]);
    case 7:
      return texts([`{"n":${String(n)}}`], { kind: "json", binary: true });
    default:
      return numbers(I32, [null]);
  }
}
for (const seed of seedsFor("next-native-parameters", [0xb651, 0xb652]))
  it(`borrows typed scalars and retires producer references seed ${String(seed)}`, async () => {
    const reach = createReach("native parameters", NATIVE_PARAMETER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 8 }),
        fc.integer({ min: -999, max: 999 }),
        fc.integer({ min: 0, max: 131073 }),
        fc.boolean(),
        async (kind, n, width, clock) => {
          reach.beginHistory();
          const value = Object.freeze(sample(kind, n, width)),
            p = plan([value.type], clock, kind === 5),
            metadata = account(),
            owned = account(),
            signal = new AbortController(),
            binding = new NativeParameterBinding(p, { slots: [value] }, metadata, signal.signal),
            source = structuredClone(value),
            result = binding.bind(owned, 123456789012345n);
          expect(metadata.held).toBe(nativeParameterMinimumBytes(p));
          expect(binding.slotBytes[0]).toBe(flatBytes(value));
          expect(binding.logicalBytes).toBeLessThanOrEqual(24);
          expect(result.slots[0]).toBe(value);
          const output = evaluate(
            { op: "param", slot: 0, type: value.type },
            { length: 1, columns: [], selection: null },
            result,
          );
          expect(output).toEqual(
            evaluate(
              { op: "literal", value, type: value.type },
              { length: 1, columns: [], selection: null },
            ),
          );
          if (clock) {
            const v = result.slots[1];
            if (v?.data.layout !== "int64") throw new Error("clock");
            expect(int64At(v.data.values, 0)).toBe(123456789012345n);
            reach.reach("clock");
          } else reach.reach("no clock");
          const nullSlot = result.slots[clock ? 2 : 1];
          expect(nullSlot?.validity?.[0]).toBe(0);
          if (kind === 5) {
            expect(result.slots.at(-1)?.data).toBe(value.data);
            reach.reach("FTS alias");
          }
          reach.reach(
            kind === 8
              ? "NULL"
              : kind === 1
                ? "int64 exact"
                : kind === 2
                  ? "timestamp exact"
                  : kind === 3 || kind === 4
                    ? "numeric exact"
                    : kind === 5
                      ? "payload"
                      : "fixed",
          );
          const allocated = owned.held;
          expect(allocated).toBe(binding.bytes);
          expect(() => binding.bind(owned)).toThrow("already bound");
          binding.retire();
          binding.retire();
          expect(result.slots.length).toBe(0);
          expect(metadata.held).toBe(0);
          expect(owned.held).toBe(allocated);
          owned.free(allocated);
          expect(value).toEqual(source);
          expect(() => binding.bind(owned)).toThrow("retired");
          reach.reach("zero ownership");
        },
      ),
      { seed, numRuns: 100 },
    );
    reach.assertReached();
  }, 120000);
for (const seed of seedsFor("next-native-parameter-validation", [0xb653, 0xb654]))
  it(`validates before retaining and reads native bounds seed ${String(seed)}`, async () => {
    const reach = createReach("native parameter validation", NATIVE_PARAMETER_VALIDATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -1000, max: 1000 }),
        fc.boolean(),
        fc.integer({ min: 0, max: 6 }),
        async (n, wide, invalid) => {
          reach.beginHistory();
          const value = wide ? bigints(I64, [BigInt(n)]) : numbers(I32, [n]),
            p = plan([value.type], false, false),
            metadata = account(),
            controller = new AbortController();
          const bad =
            invalid === 0
              ? { ...p, placeholderCount: 2 }
              : invalid === 1
                ? { ...p, clockSlot: 0 }
                : invalid === 2
                  ? { ...p, ftsParameters: [{ source: 0, slot: 0 }] }
                  : p;
          const slots =
            invalid === 3
              ? []
              : invalid === 4
                ? [{ ...value, length: 2 }]
                : invalid === 5
                  ? [texts(["wrong"])]
                  : [value];
          if (invalid < 6) {
            reach.reach(
              invalid === 0 || invalid === 3
                ? "count"
                : invalid === 1
                  ? "clock slot"
                  : invalid === 2
                    ? "FTS slot"
                    : invalid === 4
                      ? "shape"
                      : "type",
            );
            expect(
              () => new NativeParameterBinding(bad, { slots }, metadata, controller.signal),
            ).toThrow(ParameterBindError);
            expect(metadata.held).toBe(0);
            return;
          }
          reach.reach(wide ? "int64 bound" : "int32 bound");
          const binding = new NativeParameterBinding(
              p,
              { slots: [value] },
              metadata,
              controller.signal,
            ),
            gov = new Governor(8 << 20),
            view = { catalog: { version: 0 } } as unknown as Snapshot,
            runtime = new QueryRuntime(
              p,
              {
                snapshot: view,
                statement: null,
                parameters: [],
                statementTime: 0n,
                reads: {} as ChunkReads,
                governor: gov,
                spill: new MemorySpillService(),
                queryId: 1,
                sequences: null,
                sequenceSession: null,
                returning: "atExecuted",
              },
              view,
              controller.signal,
              binding,
            );
          try {
            expect(runtime.boundParameter(0)).toBe(wide ? BigInt(n) : n);
            expect(
              runtime.bounds({
                id: 2,
                op: "limit",
                input: p.root,
                output: [],
                limit: { kind: "param", slot: 0 },
                offset: null,
              }),
            ).toEqual(n < 0 ? undefined : { limit: n, offset: 0 });
          } catch (error) {
            if (n >= 0) throw error;
            expect(error).toMatchObject({ sqlState: "2201W" });
          }
          await runtime.reserve([]);
          expect(runtime.params.slots[0]).toBe(value);
          await runtime.close();
          binding.retire();
          expect(metadata.held).toBe(0);
          expect(gov.usedBytes).toBe(0);
          controller.abort(new Error("cancel native binding"));
          expect(
            () => new NativeParameterBinding(p, { slots: [value] }, metadata, controller.signal),
          ).toThrow("cancel native binding");
          expect(metadata.held).toBe(0);
        },
      ),
      { seed, numRuns: 80 },
    );
    reach.assertReached();
  });
