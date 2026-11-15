/** PC2: forwarding structured bytes never makes an invalid value a complete snapshot. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, STRUCTURED_PARTS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import { encodeArrayValue, type ArrayElement } from "../vector/array-value.js";
import { validateStructuredParts, type ValidationAccount } from "./structured-value.js";

const utf8 = new TextEncoder();
function account(cap = 8 << 20) {
  let used = 0,
    peak = 0,
    pauses = 0;
  const value: ValidationAccount = {
    take(bytes) {
      if (used + bytes > cap) throw new Error("test funding refused");
      used += bytes;
      peak = Math.max(peak, used);
    },
    free(bytes) {
      used -= bytes;
      expect(used).toBeGreaterThanOrEqual(0);
    },
    async pace() {
      pauses++;
    },
  };
  return {
    value,
    get used() {
      return used;
    },
    get peak() {
      return peak;
    },
    get pauses() {
      return pauses;
    },
  };
}
function cases(salt: number): Array<{ element: PhysicalType; value: ArrayElement }> {
  return [
    { element: { kind: "bool" }, value: true },
    { element: { kind: "int32", bits: 16 }, value: -123 },
    { element: { kind: "int32", bits: 32 }, value: 2147483647 },
    { element: { kind: "int64" }, value: -(1n << 63n) },
    { element: { kind: "date" }, value: -2147483648 },
    { element: { kind: "time" }, value: 86400000000n },
    { element: { kind: "timestamp", zoned: false }, value: (1n << 63n) - 1n },
    { element: { kind: "timestamp", zoned: true }, value: BigInt(salt) },
    { element: { kind: "float32" }, value: NaN },
    { element: { kind: "float64" }, value: -0 },
    { element: { kind: "interval" }, value: { months: 1, days: -30, micros: -(1n << 63n) } },
    { element: { kind: "uuid" }, value: new Uint8Array(16).fill(salt) },
    {
      element: { kind: "decimal", precision: null, scale: null },
      value: { coefficient: 123450n, scale: 3 },
    },
    {
      element: { kind: "decimal", precision: null, scale: null },
      value: { coefficient: 0n, scale: 0, special: "NaN" },
    },
    { element: { kind: "text" }, value: utf8.encode("label😀" + String(salt)) },
    { element: { kind: "bytes" }, value: Uint8Array.of(0, 255, salt) },
    { element: { kind: "enum", name: "e", labels: ["label😀"] }, value: utf8.encode("label😀") },
  ];
}
async function validate(type: PhysicalType, bytes: Uint8Array, width: number, cap?: number) {
  const funded = account(cap),
    output: number[] = [];
  let closed = false;
  const parts = async function* () {
    expect(funded.used).toBeGreaterThanOrEqual(131072);
    try {
      yield new Uint8Array(0);
      for (let at = 0; at < bytes.length; at += width) {
        const part = bytes.slice(at, at + width);
        yield part;
        // The producer is allowed to overwrite a borrowed part after the next stream step.
        part.fill(0xee);
      }
    } finally {
      closed = true;
    }
  };
  let error: unknown;
  try {
    await validateStructuredParts(type, parts, bytes.length, funded.value, (part) => {
      output.push(...part);
    });
  } catch (failure) {
    error = failure;
  }
  expect(funded.used).toBe(0);
  if (funded.peak !== 0) expect(closed).toBe(true);
  return { error, output: Uint8Array.from(output), peak: funded.peak, pauses: funded.pauses };
}

for (const seed of seedsFor("next-structured-parts", [0xb2b1, 0xb2b2]))
  it(`validates borrowed fragments with bounded charged workspace seed ${String(seed)}`, async () => {
    const reach = createReach("structured forwarding", STRUCTURED_PARTS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 31 }),
        fc.integer({ min: 1, max: 255 }),
        async (width, salt) => {
          reach.beginHistory();
          for (const { element, value } of cases(salt)) {
            const type: PhysicalType = { kind: "array", element };
            for (const nulls of [false, true]) {
              const bytes = encodeArrayValue(element, {
                dimensions: [9],
                lowerBounds: [-3],
                elements: Array.from({ length: 9 }, (_, i) =>
                  nulls && i % 3 === 0 ? null : value,
                ),
              });
              const result = await validate(type, bytes, width);
              expect(result.error).toBeUndefined();
              expect(result.output).toEqual(bytes);
              expect(result.peak).toBeLessThanOrEqual((4 << 20) + 131072 + 512);
              reach.reach(nulls ? "array NULL bitmap" : "all array scalar kinds");
            }
          }
          for (const binary of [false, true]) {
            const bytes = utf8.encode('{"a":[1.5e2,true,null,"😀","\\uD83D\\uDE00"]}');
            const result = await validate({ kind: "json", binary }, bytes, width);
            expect(result.error).toBeUndefined();
            expect(result.output).toEqual(bytes);
            reach.reach(binary ? "JSONB" : "JSON");
            const bad = await validate({ kind: "json", binary }, utf8.encode('{"a":[1,]}'), width);
            expect(bad.error).toMatchObject({ code: "MINNOW_VALUE_CORRUPT" });
          }
          const element = { kind: "int32", bits: 32 } as const,
            type = { kind: "array", element } as const,
            original = encodeArrayValue(element, {
              dimensions: [1],
              lowerBounds: [1],
              elements: [salt],
            });
          for (const [offset, value] of [
            [0, 2],
            [1, 255],
            [2, 7],
            [3, 128],
            [4, 0],
          ] as const) {
            const bytes = original.slice();
            bytes[offset] = value;
            const result = await validate(type, bytes, width);
            expect(result.error).toMatchObject({ code: "MINNOW_VALUE_CORRUPT" });
          }
          const truncated = await validate(type, original.subarray(0, original.length - 1), width);
          expect(truncated.error).toMatchObject({ code: "MINNOW_VALUE_CORRUPT" });
          reach.reach("internal corruption");
          const refused = await validate(type, original, width, 1);
          expect((refused.error as Error).message).toBe("test funding refused");
          expect(refused.peak).toBe(0);
          reach.reach("funding before producer");
          const cancelled = account(),
            reason = new Error("validation cancelled");
          let calls = 0,
            closed = false;
          cancelled.value.pace = async () => {
            if (++calls === 2 + (salt % 5)) throw reason;
          };
          const cancellingParts = async function* () {
            try {
              yield original;
            } finally {
              closed = true;
            }
          };
          await expect(
            validateStructuredParts(
              type,
              cancellingParts,
              original.length,
              cancelled.value,
              () => undefined,
            ),
          ).rejects.toBe(reason);
          expect(closed).toBe(true);
          expect(cancelled.used).toBe(0);
          reach.reach("cancellation releases producer");
          reach.reach("borrowed parts copied before advance");
          reach.reach("charges released");
        },
      ),
      { seed, numRuns: 8 },
    );
    // A large bitmap is streamed and counted, never retained or reread. No element graph.
    const bytes = new Uint8Array(12 + 125000),
      view = new DataView(bytes.buffer);
    bytes.set([1, 3, 1, 1]);
    view.setInt32(4, 1000000, true);
    view.setInt32(8, 1, true);
    bytes.fill(255, 12);
    const result = await validate(
      { kind: "array", element: { kind: "int32", bits: 32 } },
      bytes,
      8192,
    );
    expect(result.error).toBeUndefined();
    expect(result.output).toEqual(bytes);
    expect(result.peak).toBe(131072);
    reach.reach("large bounded bitmap");
    reach.assertReached();
  }, 120_000);
