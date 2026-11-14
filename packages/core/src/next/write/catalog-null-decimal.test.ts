/** The vector contract makes per-slot decimal payloads under NULL uninterpreted. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_NULL_DECIMAL_REACH } from "../../testing/reach.js";
import { Catalog } from "./catalog.js";
import { encodeCatalogStream, decodeCatalogStream } from "./catalog-stream.js";
import { column, tableInput } from "./test-support.js";
import { need } from "./bytes.js";
import { StoreCorruptionError } from "../storage/contract.js";
import type { FlatVector } from "../vector/vector.js";
const type = { kind: "decimal", precision: null, scale: null } as const;
for (const seed of seedsFor("next-catalog-null-decimal", [0xb774, 0xb775]))
  it(`interprets decimal payloads only for valid missing slots seed ${String(seed)}`, () => {
    const reach = createReach("catalog NULL decimal", CATALOG_NULL_DECIMAL_REACH);
    fc.assert(
      fc.property(
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 65535 }),
        fc.integer({ min: 0, max: 255 }),
        (active, perValue, scale, special) => {
          reach.beginHistory();
          const catalog = Catalog.empty().change(
            {
              kind: "createTable",
              table: tableInput("t", [column("x", type)], { primaryKey: null }),
            },
            1,
          ).catalog;
          const data = decodeCatalogStream(catalog.encode()),
            table = need(data.tables[0]);
          const vector: FlatVector = {
            form: "flat",
            type,
            length: 1,
            validity: active ? null : Uint32Array.of(0),
            data: {
              layout: "decimal",
              coefficients: { repr: "safe", values: Float64Array.of(1) },
              scale: perValue ? null : 2,
              scales: perValue ? Uint16Array.of(scale) : null,
              specials: Uint8Array.of(special),
            },
          };
          const bytes = encodeCatalogStream({
            ...data,
            tables: [{ ...table, columns: [{ ...need(table.columns[0]), missing: vector }] }],
          });
          if (active && (special > 3 || (perValue && scale > 16383))) {
            expect(() => Catalog.decode(bytes)).toThrow(StoreCorruptionError);
            if (special > 3) reach.reach("live special refused");
            if (perValue && scale > 16383) reach.reach("live scale refused");
          } else {
            expect(Catalog.decode(bytes).tables[0]?.columns[0]?.missing).toEqual(vector);
            reach.reach(active ? "live value" : "NULL payload ignored");
            if (active && perValue && scale === 0) reach.reach("scale zero");
            if (active && perValue && scale === 16383) reach.reach("scale boundary");
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [false, false, 65535, 255],
          [false, true, 65535, 255],
          [true, true, 16383, 0],
          [true, true, 16384, 0],
          [true, false, 0, 4],
          [true, false, 0, 3],
          [true, true, 0, 0],
        ],
      },
    );
    reach.assertReached();
  });
