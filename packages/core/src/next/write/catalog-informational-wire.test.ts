/** Semantic fallback labels are required-understanding fields, not ignorable display text. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_INFORMATIONAL_WIRE_REACH, createReach } from "../../testing/reach.js";
import { Catalog } from "./catalog.js";
import { column, INT, tableInput } from "./test-support.js";
import { decodeCatalogStream, encodeCatalogStream } from "./catalog-stream.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import { encodeCatalogRecord, decodeCatalogRecord } from "./catalog-records.js";

for (const seed of seedsFor("next-catalog-informational-wire", [0xb7e8, 0xb7e9]))
  it(`refuses incomplete semantic fallback targets seed ${String(seed)}`, () => {
    const reach = createReach("informational target framing", CATALOG_INFORMATIONAL_WIRE_REACH);
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 3 }), fc.integer({ min: 0, max: 4 }), (count, mode) => {
        reach.beginHistory();
        let catalog = Catalog.empty();
        catalog = catalog.change(
          {
            kind: "createTable",
            table: tableInput(
              "Case.汉",
              Array.from({ length: count }, (_, i) => column(`k${String(i)}`, INT)),
              { primaryKey: Array.from({ length: count }, (_, i) => i) },
            ),
          },
          1,
        ).catalog;
        const parent = catalog.tables[0];
        if (!parent?.primaryKey) throw new Error("missing parent fixture");
        catalog = catalog.change(
          {
            kind: "createTable",
            table: tableInput(
              "c",
              Array.from({ length: count }, (_, i) => column(`k${String(i)}`, INT)),
              {
                foreignKeys: [
                  {
                    name: "fk",
                    columns: Array.from({ length: count }, (_, i) => i),
                    parentTableId: parent.id,
                    parentIndexId: parent.primaryKey,
                    onDelete: "restrict",
                    enforced: false,
                  },
                ],
              },
            ),
          },
          2,
        ).catalog;
        catalog = catalog.change({ kind: "dropTable", tableId: parent.id }, 3).catalog;
        const data = decodeCatalogStream(catalog.encode()),
          table = data.tables[0],
          fk = table?.foreignKeys[0];
        if (!table || !fk) throw new Error("missing informational fixture");
        expect(fk.parentName).toBe("Case.汉");
        expect(fk.parentColumnNames).toHaveLength(count);
        const bad: Record<string, unknown> = { ...fk };
        const label = (
          [
            "missing both",
            "missing table",
            "missing columns",
            "wrong arity",
            "empty label",
          ] as const
        )[mode];
        if (!label) throw new Error("missing fault label");
        if (mode <= 1) delete bad.parentName;
        if (mode === 0 || mode === 2) delete bad.parentColumnNames;
        if (mode === 3) bad.parentColumnNames = [];
        if (mode === 4) bad.parentColumnNames = Array.from({ length: count }, () => "");
        const invalid = { ...data, tables: [{ ...table, foreignKeys: [bad] }] };
        expect(() =>
          Catalog.decode(encodeCatalogStream(invalid as unknown as typeof data)),
        ).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
        reach.reach(label);
        const encoded = encodeCatalogRecord("foreignKey", fk);
        try {
          const input = new ByteReader(encoded.bytes),
            family = input.varuint(),
            version = input.varuint(),
            body = new ByteReader(input.blob()),
            fields: Array<{ id: number; flags: number; bytes: Uint8Array }> = [];
          while (!body.done)
            fields.push({ id: body.varuint(), flags: body.u8(), bytes: body.blob() });
          for (const id of [10, 11]) {
            expect(fields.find((field) => field.id === id)?.flags).toBe(1);
            const altered = new ByteWriter();
            for (const field of fields) {
              altered.varuint(field.id);
              altered.u8(field.id === id ? 0 : field.flags);
              altered.blob(field.bytes);
            }
            const output = new ByteWriter();
            output.varuint(family);
            output.varuint(version);
            output.blob(altered.finish());
            expect(() => decodeCatalogRecord("foreignKey", output.finish())).toThrow(
              expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
            );
          }
          reach.reach("required flags10 and11");
        } finally {
          encoded.close();
        }
        reach.reach(count === 1 ? "scalar" : "composite");
      }),
      {
        seed,
        numRuns: 20,
        examples: Array.from(
          { length: 5 },
          (_, mode) => [(mode % 3) + 1, mode] as [number, number],
        ),
      },
    );
    reach.assertReached();
  });
