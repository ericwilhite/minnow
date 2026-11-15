/** API negative-zero provenance is metadata, not a noncanonical SQL integer payload. */
import fc from "fast-check";
import { it, expect } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_BACKFILL_SIGN_REACH } from "../../testing/reach.js";
import { Catalog } from "./catalog.js";
import { INT, BIG, column, ints, tableInput } from "./test-support.js";
import { decodeCatalogStream, encodeCatalogStream } from "./catalog-stream.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import { encodeCatalogRecord, decodeCatalogRecord } from "./catalog-records.js";
for (const seed of seedsFor("next-catalog-backfill-sign", [0xb7f4, 0xb7f5]))
  it(`requires coherent integer backfill provenance seed ${String(seed)}`, () => {
    const reach = createReach("integer backfill sign", CATALOG_BACKFILL_SIGN_REACH);
    fc.assert(
      fc.property(fc.boolean(), fc.integer({ min: 0, max: 4 }), (wide, mode) => {
        reach.beginHistory();
        const type = wide ? BIG : INT;
        const source = Catalog.empty().change(
          {
            kind: "createTable",
            table: tableInput(
              "t",
              [
                {
                  ...column("v", type),
                  missing: ints([0], type),
                  backfillNegativeZero: true,
                },
              ],
              { primaryKey: null },
            ),
          },
          1,
        ).catalog;
        const data = decodeCatalogStream(source.encode()),
          table = data.tables[0],
          stored = table?.columns[0];
        if (!table || !stored) throw new Error("missing signed-zero fixture");
        expect(Catalog.decode(source.encode()).tables[0]?.columns[0]?.backfillNegativeZero).toBe(
          true,
        );
        const bad = { ...stored } as unknown as Record<string, unknown>;
        const labels = [
          "false sign",
          "missing absent",
          "missing NULL",
          "nonzero value",
          "noninteger type",
        ] as const;
        if (mode === 0) bad.backfillNegativeZero = false;
        if (mode === 1) bad.missing = null;
        if (mode === 2) bad.missing = ints([null], type);
        if (mode === 3) bad.missing = ints([1], type);
        if (mode === 4) {
          bad.type = { kind: "float64" };
          bad.declared = { type: "number", declaredTypeName: "DOUBLE PRECISION" };
          bad.missing = {
            form: "flat",
            type: bad.type,
            length: 1,
            validity: null,
            data: { layout: "f64", values: Float64Array.of(-0) },
          };
        }
        expect(() =>
          Catalog.decode(
            encodeCatalogStream({
              ...data,
              tables: [{ ...table, columns: [bad] }],
            } as unknown as typeof data),
          ),
        ).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
        const label = labels[mode];
        if (!label) throw new Error("missing sign reach");
        reach.reach(label);
        reach.reach(wide ? "int64" : "int32");
        const encoded = encodeCatalogRecord("column", stored);
        try {
          const input = new ByteReader(encoded.bytes),
            family = input.varuint(),
            version = input.varuint(),
            fields = new ByteReader(input.blob()),
            body = new ByteWriter();
          let found = false;
          while (!fields.done) {
            const id = fields.varuint(),
              flags = fields.u8(),
              bytes = fields.blob();
            body.varuint(id);
            body.u8(id === 17 ? 0 : flags);
            body.blob(bytes);
            if (id === 17) {
              expect(flags).toBe(1);
              found = true;
            }
          }
          expect(found).toBe(true);
          const out = new ByteWriter();
          out.varuint(family);
          out.varuint(version);
          out.blob(body.finish());
          expect(() => decodeCatalogRecord("column", out.finish())).toThrow(
            expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
          );
          reach.reach("required flag");
          reach.reach("downgrade refused");
        } finally {
          encoded.close();
        }
      }),
      {
        seed,
        numRuns: 20,
        examples: Array.from({ length: 5 }, (_, mode) => [false, mode] as [boolean, number]).concat(
          [[true, 0]],
        ),
      },
    );
    reach.assertReached();
  });
