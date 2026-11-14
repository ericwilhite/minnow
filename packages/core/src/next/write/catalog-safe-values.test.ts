/** Authority: vector/vector.ts exact safe-integer and invalid-slot contracts. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_SAFE_VALUES_REACH } from "../../testing/reach.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector, VectorData } from "../vector/vector.js";
import { int64At } from "../vector/int64.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { Catalog } from "./catalog.js";
import { decodeCatalogStream, encodeCatalogStream } from "./catalog-stream.js";
import { column, tableInput } from "./test-support.js";
import { need } from "./bytes.js";

const types = [
  { kind: "int64" },
  { kind: "timestamp", zoned: false },
  { kind: "timestamp", zoned: true },
  { kind: "time" },
  { kind: "interval" },
  { kind: "decimal", precision: null, scale: null },
] as const satisfies readonly PhysicalType[];
function vector(type: PhysicalType, value: number, active: boolean): FlatVector {
  const values = { repr: "safe", values: Float64Array.of(value) } as const;
  const data: VectorData =
    type.kind === "decimal"
      ? { layout: "decimal", coefficients: values, scale: 0, scales: null, specials: null }
      : type.kind === "interval"
        ? { layout: "interval", months: Int32Array.of(0), days: Int32Array.of(0), micros: values }
        : { layout: "int64", values };
  return { form: "flat", type, length: 1, validity: active ? null : Uint32Array.of(0), data };
}
const invalid = [NaN, Infinity, -Infinity, 1.5, -1.5, 2 ** 53, -(2 ** 53), -0];
for (const seed of seedsFor("next-catalog-safe-values", [0xb776, 0xb777]))
  it(`admits exact live safe values and ignores NULL payloads seed ${String(seed)}`, () => {
    const reach = createReach("catalog safe values", CATALOG_SAFE_VALUES_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: types.length - 1 }),
        fc.oneof(
          fc.constantFrom(...invalid),
          fc.double({ noNaN: true, noDefaultInfinity: true }),
          fc.integer({ min: -Number.MAX_SAFE_INTEGER, max: Number.MAX_SAFE_INTEGER }),
        ),
        fc.boolean(),
        (which, value, active) => {
          reach.beginHistory();
          const type = need(types[which]);
          if (type.kind === "time" && Number.isSafeInteger(value) && !Object.is(value, -0))
            value = Math.abs(value) % 86_400_000_001;
          const source = Catalog.empty().change(
            {
              kind: "createTable",
              table: tableInput("t", [column("x", type)], { primaryKey: null }),
            },
            1,
          ).catalog;
          const stored = decodeCatalogStream(source.encode());
          const table = need(stored.tables[0]);
          const missing = vector(type, value, active);
          const bytes = encodeCatalogStream({
            ...stored,
            tables: [{ ...table, columns: [{ ...need(table.columns[0]), missing }] }],
          });
          const exact = Number.isSafeInteger(value) && !Object.is(value, -0);
          reach.reach(
            type.kind === "timestamp" ? (type.zoned ? "timestamptz" : "timestamp") : type.kind,
          );
          if (exact || !active) {
            const decoded = Catalog.decode(bytes).tables[0]?.columns[0]?.missing;
            expect(decoded).toEqual(missing);
            reach.reach(active ? "exact live" : "NULL ignored");
            if (active) {
              const data = need(decoded).data;
              const values =
                data.layout === "int64"
                  ? data.values
                  : data.layout === "interval"
                    ? data.micros
                    : data.layout === "decimal"
                      ? data.coefficients
                      : undefined;
              if (values?.repr !== "safe") throw new Error("Missing exact representation");
              expect(int64At(values, 0)).toBe(BigInt(value));
            }
          } else {
            expect(() => Catalog.decode(bytes)).toThrow(StoreCorruptionError);
            reach.reach("invalid live refused");
          }
          if (!exact)
            reach.reach(
              Number.isNaN(value)
                ? "NaN"
                : !Number.isFinite(value)
                  ? "infinity"
                  : Object.is(value, -0)
                    ? "negative zero"
                    : !Number.isInteger(value)
                      ? "fraction"
                      : "unsafe integer",
            );
        },
      ),
      {
        seed,
        numRuns: 256,
        examples: types.flatMap((_, which) => [
          ...invalid.flatMap(
            (value) =>
              [
                [which, value, true],
                [which, value, false],
              ] as Array<[number, number, boolean]>,
          ),
          ...[0, 1, -1, Number.MAX_SAFE_INTEGER, -Number.MAX_SAFE_INTEGER].map(
            (value) => [which, value, true] as [number, number, boolean],
          ),
        ]),
      },
    );
    reach.assertReached();
  });
