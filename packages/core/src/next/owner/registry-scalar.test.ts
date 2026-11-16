/** Native image scalar fidelity: borrowed vector lanes, not JS/SQL parameter coercion. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, REGISTRY_SCALAR_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector, Vector, VectorData } from "../vector/vector.js";
import { sortRowView } from "../exec/sort-row.js";
import { RegistryFragments, REGISTRY_FRAGMENT_BYTES } from "./registry-fragments.js";
import { storeRegistryScalar, registryScalarBytes, loadRegistryScalar } from "./registry-scalar.js";

it.each(seedsFor("next-registry-scalars", [0xb5de]))(
  "round-trips native scalar lanes seed %i",
  async (seed) => {
    const reach = createReach("registry scalar fidelity", REGISTRY_SCALAR_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer(),
        fc.integer({ min: 0, max: 230000 }),
        fc.constantFrom("flat", "constant", "dictionary"),
        fc.boolean(),
        async (n, length, form, nulls) => {
          reach.beginHistory();
          let held = 0,
            peak = 0;
          const charge = {
            allocate: (bytes: number) => {
              held += bytes;
              peak = Math.max(peak, held);
              return true;
            },
            free: (bytes: number) => {
              held -= bytes;
              expect(held).toBeGreaterThanOrEqual(0);
            },
          };
          const spill = new MemorySpillService(),
            fragments = new RegistryFragments(
              charge,
              spill,
              1,
              new AbortController().signal,
              () => undefined,
            );
          const flat = (type: PhysicalType, data: VectorData): FlatVector => ({
            form: "flat",
            type,
            length: 2,
            validity: null,
            data,
          });
          const bytes = Uint8Array.from({ length: length + 7 }, (_, k) => (k + n) & 255);
          const numeric = { kind: "decimal", precision: null, scale: null } as const;
          const big =
            (BigInt(n) << (length > 131072 ? 600000n : 208n)) + 0x123456789abcdef0123456789n;
          const variants: FlatVector[] = [
            flat({ kind: "bool" }, { layout: "bool", values: Uint8Array.of(0, 1) }),
            flat({ kind: "int32", bits: 32 }, { layout: "i32", values: Int32Array.of(n, ~n) }),
            flat(
              { kind: "date" },
              { layout: "i32", values: Int32Array.of(-2147483648, 2147483647) },
            ),
            flat({ kind: "float32" }, { layout: "f32", values: Float32Array.of(n / 3, -0) }),
            flat({ kind: "float64" }, { layout: "f64", values: Float64Array.of(NaN, Infinity) }),
            flat(
              { kind: "int64" },
              { layout: "int64", values: { repr: "safe", values: Float64Array.of(n, -n) } },
            ),
            flat(
              { kind: "timestamp", zoned: true },
              {
                layout: "int64",
                values: { repr: "int64", values: BigInt64Array.of(-(1n << 63n), (1n << 63n) - 1n) },
              },
            ),
            flat(
              { kind: "uuid" },
              {
                layout: "fixed16",
                bytes: Uint8Array.from({ length: 32 }, (_, k) => (k + n) & 255),
              },
            ),
            flat(
              { kind: "interval" },
              {
                layout: "interval",
                months: Int32Array.of(n, ~n),
                days: Int32Array.of(~n, n),
                micros: { repr: "int64", values: BigInt64Array.of(123n, -456n) },
              },
            ),
            flat(
              { kind: "text" },
              { layout: "varlen", bytes, offsets: Int32Array.of(3, 7, length + 7) },
            ),
            ...(["safe", "int64", "int128", "wide"] as const).map((repr): FlatVector =>
              flat(numeric, {
                layout: "decimal",
                scale: null,
                scales: Uint16Array.of(16383, 4),
                specials: Uint8Array.of(1, 3),
                coefficients:
                  repr === "wide"
                    ? { repr, values: [big, -big] }
                    : repr === "int128"
                      ? { repr, limbs: Uint32Array.of(1, 2, 3, 0xffffffff, 5, 6, 7, 0x80000000) }
                      : repr === "int64"
                        ? {
                            repr,
                            values: BigInt64Array.of(0x123456789abcdefn, -0x123456789abcdefn),
                          }
                        : { repr, values: Float64Array.of(n, -n) },
              }),
            ),
          ];
          try {
            for (const source of variants)
              for (const row of [0, 1]) {
                const slot = form === "constant" ? row : row;
                const one = sortRowView(source, slot);
                if (nulls) one.validity = new Uint32Array(1);
                const vector: Vector =
                  form === "flat"
                    ? one
                    : form === "constant"
                      ? { form, type: source.type, length: 2, value: one }
                      : {
                          form: "dictionary",
                          type: source.type,
                          length: 2,
                          codes: Int32Array.of(0, 0),
                          dictionary: { ...one, validity: null },
                          dictionaryId: 1,
                          validity: nulls ? new Uint32Array(1) : null,
                        };
                const record = new Uint8Array(128);
                record.fill(99, 0, 16);
                await storeRegistryScalar(record, vector, 0, fragments, charge);
                expect(record.subarray(0, 16)).toEqual(new Uint8Array(16).fill(99));
                const size = registryScalarBytes(record, vector.type);
                charge.allocate(size);
                try {
                  const decoded = await loadRegistryScalar(
                    record,
                    vector.type,
                    fragments,
                    charge,
                    () => undefined,
                  );
                  record.fill(0); // Delivery must not alias reusable metadata.
                  const expected = sortRowView(vector, 0);
                  // Normalize varlen start offsets only; every other lane/tier/scale is exact.
                  if (expected.data.layout === "varlen") {
                    const from = expected.data.offsets[0] ?? 0,
                      to = expected.data.offsets[1] ?? from;
                    expect(decoded.data).toEqual({
                      layout: "varlen",
                      bytes: expected.data.bytes.slice(from, to),
                      offsets: Int32Array.of(0, to - from),
                    });
                  } else expect(decoded.data).toEqual(expected.data);
                  expect(decoded.validity).toEqual(expected.validity);
                  expect(decoded.type).toEqual(expected.type);
                  expect(decoded.length).toBe(1);
                } finally {
                  charge.free(size);
                }
                expect(held).toBe(REGISTRY_FRAGMENT_BYTES);
                reach.reach(source.data.layout);
                if (source.data.layout === "decimal") {
                  reach.reach(`decimal ${source.data.coefficients.repr}`);
                  if (source.data.coefficients.repr === "wide" && length > 131072 && !nulls)
                    reach.reach("wide coefficient above fragment");
                }
              }
            reach.reach(form);
            reach.reach(nulls ? "NULL" : "non-NULL");
            if (length > 131072 && !nulls) reach.reach("payload above scratch");
            expect(peak).toBeGreaterThanOrEqual(REGISTRY_FRAGMENT_BYTES);
          } finally {
            await fragments.close();
          }
          expect(held).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [-1, 230000, "flat", false],
          [2, 0, "constant", true],
          [3, 65505, "dictionary", false],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
