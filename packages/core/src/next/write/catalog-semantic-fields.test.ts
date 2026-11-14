/** Independently pinned field IDs/flags for semantic extensions (graph checks are separate). */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_SEMANTIC_FIELDS_REACH, createReach } from "../../testing/reach.js";
import { Catalog } from "./catalog.js";
import { column, tableInput, INT } from "./test-support.js";
import { ByteReader } from "./bytes.js";
import {
  encodeCatalogRecord,
  decodeCatalogRecord,
  type CatalogRecordKind,
} from "./catalog-records.js";
for (const seed of seedsFor("next-catalog-semantic-fields", [0xb790, 0xb791]))
  it(`requires understood semantic field flags seed ${String(seed)}`, () => {
    const reach = createReach("semantic catalog fields", CATALOG_SEMANTIC_FIELDS_REACH);
    const table = Catalog.empty().change(
      { kind: "createTable", table: tableInput("t", [column("a", INT, false)]) },
      1,
    ).catalog.tables[0];
    if (!table?.columns[0] || !table.indexes[0]) throw new Error("Missing record fixtures");
    const deps = { dialectVersion: 1, columnIds: [], sequenceIds: [] };
    const cases: ReadonlyArray<{
      kind: CatalogRecordKind;
      id: number;
      field: string;
      record: unknown;
    }> = [
      { kind: "table", id: 9, field: "managed", record: { ...table, managed: false } },
      { kind: "column", id: 10, field: "typeId", record: { ...table.columns[0], typeId: 3 } },
      {
        kind: "column",
        id: 11,
        field: "allowedValues",
        record: { ...table.columns[0], allowedValues: ["a"] },
      },
      {
        kind: "column",
        id: 12,
        field: "defaultDependencies",
        record: { ...table.columns[0], defaultDependencies: deps },
      },
      {
        kind: "column",
        id: 13,
        field: "generatedDependencies",
        record: { ...table.columns[0], generatedDependencies: deps },
      },
      {
        kind: "column",
        id: 14,
        field: "autoIncrement",
        record: { ...table.columns[0], autoIncrement: true },
      },
      {
        kind: "index",
        id: 10,
        field: "directions",
        record: { ...table.indexes[0], directions: ["desc"] },
      },
      {
        kind: "check",
        id: 5,
        field: "dependencies",
        record: { id: 1, name: "c", conditionSql: "TRUE", columnIds: [], dependencies: deps },
      },
      {
        kind: "view",
        id: 4,
        field: "managed",
        record: { id: 1, name: "v", bodySql: "SELECT 1", managed: true },
      },
      {
        kind: "view",
        id: 5,
        field: "outputs",
        record: { id: 1, name: "v", bodySql: "SELECT 1", outputs: [] },
      },
      {
        kind: "view",
        id: 6,
        field: "viewDependencies",
        record: { id: 1, name: "v", bodySql: "SELECT 1", dependencies: [] },
      },
      {
        kind: "view",
        id: 7,
        field: "viewSourceSchemas",
        record: { id: 1, name: "v", bodySql: "SELECT 1", sourceSchemas: [] },
      },
      {
        kind: "expressionDependencies",
        id: 4,
        field: "expressionTypes",
        record: { ...deps, typeIds: [1] },
      },
    ];
    fc.assert(
      fc.property(fc.integer({ min: 0, max: cases.length - 1 }), (which) => {
        reach.beginHistory();
        const sample = cases[which];
        if (!sample) throw new Error("Missing semantic field");
        const encoded = encodeCatalogRecord(sample.kind, sample.record);
        try {
          expect(decodeCatalogRecord(sample.kind, encoded.bytes)).toEqual(sample.record);
          const outer = new ByteReader(encoded.bytes);
          outer.varuint();
          outer.varuint();
          const body = new ByteReader(outer.blob());
          outer.end();
          let found = false;
          while (!body.done) {
            const id = body.varuint(),
              at = body.offset,
              flag = body.u8();
            body.blob();
            if (id !== sample.id) continue;
            found = true;
            expect(flag).toBe(1);
            body.bytes[at] = 0;
            expect(() => decodeCatalogRecord(sample.kind, encoded.bytes)).toThrow(
              expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
            );
            body.bytes[at] = flag;
          }
          expect(found).toBe(true);
          reach.reach(sample.field as (typeof CATALOG_SEMANTIC_FIELDS_REACH)[number]);
          reach.reach("downgrade refused");
        } finally {
          encoded.close();
        }
      }),
      { seed, numRuns: 24, examples: cases.map((_, i) => [i] as [number]) },
    );
    reach.assertReached();
  });
