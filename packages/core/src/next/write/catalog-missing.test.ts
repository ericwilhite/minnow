import { need } from "./bytes.js";
/** Valid literal representations and malformed per-kind payloads at the catalog boundary. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_MISSING_REACH } from "../../testing/reach.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector, VectorData } from "../vector/vector.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { Catalog } from "./catalog.js";
import { decodeCatalogStream, encodeCatalogStream } from "./catalog-stream.js";
import { column, tableInput } from "./test-support.js";
function fixture(index: number, value: number): FlatVector {
  const decimal: PhysicalType = { kind: "decimal", precision: null, scale: null };
  let type: PhysicalType, data: VectorData;
  switch (index) {
    case 0:
      type = { kind: "bool" };
      data = { layout: "bool", values: Uint8Array.of(value % 2) };
      break;
    case 1:
      type = { kind: "int32", bits: 16 };
      data = { layout: "i32", values: Int32Array.of(value) };
      break;
    case 2:
      type = { kind: "float32" };
      data = { layout: "f32", values: Float32Array.of(value / 10) };
      break;
    case 3:
      type = { kind: "float64" };
      data = { layout: "f64", values: Float64Array.of(value / 10) };
      break;
    case 4:
      type = { kind: "int64" };
      data = { layout: "int64", values: { repr: "safe", values: Float64Array.of(value) } };
      break;
    case 5:
      type = { kind: "timestamp", zoned: false };
      data = {
        layout: "int64",
        values: { repr: "int64", values: BigInt64Array.of(BigInt(value)) },
      };
      break;
    case 6:
      type = { kind: "uuid" };
      data = { layout: "fixed16", bytes: new Uint8Array(16).fill(value % 256) };
      break;
    case 7:
      type = { kind: "interval" };
      data = {
        layout: "interval",
        months: Int32Array.of(value),
        days: Int32Array.of(-value),
        micros: { repr: "int64", values: BigInt64Array.of(BigInt(value)) },
      };
      break;
    case 8:
      type = decimal;
      data = {
        layout: "decimal",
        coefficients: { repr: "safe", values: Float64Array.of(value) },
        scale: 2,
        scales: null,
        specials: null,
      };
      break;
    case 9:
      type = decimal;
      data = {
        layout: "decimal",
        coefficients: { repr: "int128", limbs: Uint32Array.of(value, 0, 0, 0) },
        scale: 2,
        scales: null,
        specials: null,
      };
      break;
    case 10:
      type = decimal;
      data = {
        layout: "decimal",
        coefficients: { repr: "wide", values: [10n ** 60n + BigInt(value)] },
        scale: null,
        scales: Uint16Array.of(4),
        specials: Uint8Array.of(0),
      };
      break;
    case 11:
      type = { ...decimal, scale: 2 };
      data = {
        layout: "decimal",
        coefficients: { repr: "safe", values: Float64Array.of(value) },
        scale: 2,
        scales: null,
        specials: null,
      };
      break;
    default: {
      const bytes = new TextEncoder().encode("汉".repeat(value % 8));
      type = { kind: "text" };
      data = { layout: "varlen", offsets: Int32Array.of(0, bytes.length), bytes };
    }
  }
  return { form: "flat", type, length: 1, validity: null, data };
}
for (const seed of seedsFor("next-catalog-missing-shapes", [0xb772, 0xb773]))
  it(`keeps valid missing values and refuses conditional shape corruption seed ${String(seed)}`, () => {
    const reach = createReach("catalog missing shape", CATALOG_MISSING_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 12 }),
        fc.integer({ min: 0, max: 3 }),
        fc.integer({ min: 0, max: 32000 }),
        (which, fault, value) => {
          reach.beginHistory();
          const vector = fixture(which, value);
          const catalog = Catalog.empty().change(
            {
              kind: "createTable",
              table: tableInput("t", [column("x", vector.type)], { primaryKey: null }),
            },
            1,
          ).catalog;
          const data = decodeCatalogStream(catalog.encode());
          const table = need(data.tables[0]);
          const stored = {
            ...data,
            tables: [{ ...table, columns: [{ ...need(table.columns[0]), missing: vector }] }],
          };
          const restored = Catalog.decode(encodeCatalogStream(stored));
          expect(restored.tables[0]?.columns[0]?.missing).toEqual(vector);
          reach.reach(vector.data.layout);
          if (
            vector.type.kind === "decimal" &&
            vector.type.precision === null &&
            vector.type.scale !== null
          )
            reach.reach("scale-only declaration");
          if (vector.data.layout === "decimal") {
            reach.reach(vector.data.coefficients.repr);
            if (vector.data.scale === null) reach.reach("per-value scale");
          }
          const bad = structuredClone(stored);
          const missing = need(need(bad.tables[0]).columns[0]).missing as unknown as Record<
            string,
            unknown
          >;
          if (fault === 0) {
            missing.length = value + 2;
            reach.reach("length");
          }
          if (fault === 1) {
            missing.validity = new Uint32Array();
            reach.reach("validity");
          }
          if (fault === 2) {
            missing.type =
              vector.type.kind === "bool" ? { kind: "int32", bits: 32 } : { kind: "bool" };
            reach.reach("type");
          }
          if (fault === 3) {
            missing.data = {
              ...vector.data,
              ...(vector.data.layout === "decimal" ? { offsets: Int32Array.of(0) } : { scale: 2 }),
            };
            reach.reach("conditional payload");
          }
          expect(() => Catalog.decode(encodeCatalogStream(bad))).toThrow(StoreCorruptionError);
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          ...Array.from({ length: 13 }, (_, i) => [i, 0, 1] as [number, number, number]),
          [0, 1, 2],
          [0, 2, 3],
          [0, 3, 4],
        ],
      },
    );
    reach.assertReached();
  });
