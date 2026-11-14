/** Stable identities and semantic fields survive MCAT; malformed graphs fail before exposure. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_RELATIONSHIPS_REACH, createReach } from "../../testing/reach.js";
import { Catalog } from "./catalog.js";
import { column, INT, tableInput, TEXT } from "./test-support.js";
import { decodeCatalogStream, encodeCatalogStream } from "./catalog-stream.js";

for (const seed of seedsFor("next-catalog-relationships", [0xb782, 0xb783]))
  it(`validates extended catalog relationships seed ${String(seed)}`, () => {
    const reach = createReach("catalog relational metadata", CATALOG_RELATIONSHIPS_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 10 }),
        fc.integer({ min: 1, max: 9 }),
        (mode, padding) => {
          reach.beginHistory();
          let catalog = Catalog.empty();
          for (let i = 0; i < padding; i++)
            catalog = catalog.change(
              {
                kind: "createTable",
                table: tableInput(`p${String(i)}`, [column("id", INT, false)]),
              },
              catalog.version + 1,
            ).catalog;
          catalog = catalog.change(
            {
              kind: "createTable",
              table: {
                ...tableInput("t", [
                  { ...column("id", INT, false), identity: { start: 1n, increment: 1n } },
                  {
                    ...column("a.b", TEXT),
                    allowedValues: ["a", "b"],
                    defaultSql: "'a'",
                    defaultDependencies: { dialectVersion: 1, columns: [], sequenceIds: [] },
                  },
                  {
                    ...column("calc", INT),
                    generatedSql: '"id" + 1',
                    generatedDependencies: { dialectVersion: 1, columns: [0], sequenceIds: [] },
                  },
                ]),
                managed: true,
              },
            },
            catalog.version + 1,
          ).catalog;
          const table = catalog.tableByName("t");
          if (!table) throw new Error("Missing generated table");
          const id = table.columns[0]?.id;
          if (!id) throw new Error("Missing identity");
          expect(table.columns[2]?.generatedDependencies?.columnIds).toEqual([id]);
          catalog = catalog.change(
            {
              kind: "createView",
              view: {
                name: "v",
                bodySql: "SELECT id FROM t",
                managed: true,
                outputs: [
                  { name: "id", type: INT, declared: column("id", INT).declared, nullable: false },
                ],
                dependencies: [{ kind: "table", objectId: table.id, columnIds: [id] }],
              },
            },
            catalog.version + 1,
          ).catalog;
          expect(Catalog.decode(catalog.encode()).encode()).toEqual(catalog.encode());
          expect(() =>
            catalog.change({ kind: "dropTable", tableId: table.id }, catalog.version + 1),
          ).toThrow(/used by view/);
          const data = decodeCatalogStream(catalog.encode());
          const target = data.tables.at(-1);
          if (!target) throw new Error("Missing target");
          const columns = [...target.columns];
          const first = columns[0],
            second = columns[1],
            third = columns[2];
          if (!first || !second || !third) throw new Error("Missing columns");
          const views = [...data.views];
          const view = views[0];
          if (!view) throw new Error("Missing view");
          if (mode === 0)
            columns[2] = {
              ...third,
              generatedDependencies: {
                dialectVersion: 1,
                columnIds: [data.counters.nextColumnId],
                sequenceIds: [],
              },
            };
          if (mode === 1)
            columns[2] = {
              ...third,
              generatedDependencies: { dialectVersion: 1, columnIds: [third.id], sequenceIds: [] },
            };
          if (mode === 2)
            columns[1] = {
              ...second,
              defaultDependencies: { dialectVersion: 1, columnIds: [id], sequenceIds: [] },
            };
          if (mode === 3)
            columns[0] = {
              ...first,
              defaultDependencies: { dialectVersion: 1, columnIds: [], sequenceIds: [] },
            };
          if (mode === 4) columns[1] = { ...second, allowedValues: ["a", "a"] };
          if (mode === 5) columns[0] = { ...first, typeId: data.counters.nextTypeId };
          if (mode === 6)
            views[0] = {
              ...view,
              dependencies: [
                { kind: "table", objectId: table.id, columnIds: [data.counters.nextColumnId] },
              ],
            };
          if (mode === 7)
            views[0] = {
              ...view,
              dependencies: [{ kind: "view", objectId: view.id, columnIds: [] }],
            };
          if (mode === 8)
            views[0] = { ...view, outputs: [...(view.outputs ?? []), ...(view.outputs ?? [])] };
          if (mode === 9)
            columns[2] = {
              ...third,
              generatedDependencies: { dialectVersion: 1, columnIds: [id, id], sequenceIds: [] },
            };
          const bad = {
            ...data,
            ...(mode === 10
              ? { sequences: data.sequences.map((sequence) => ({ ...sequence, ownedBy: null })) }
              : {}),
            tables: [...data.tables.slice(0, -1), { ...target, columns }],
            views,
          };
          expect(() => Catalog.decode(encodeCatalogStream(bad))).toThrow(
            expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
          );
          const state = CATALOG_RELATIONSHIPS_REACH[mode];
          if (!state) throw new Error("Missing reach");
          reach.reach(state);
        },
      ),
      {
        seed,
        numRuns: 30,
        examples: Array.from({ length: 11 }, (_, i) => [i, 3] as [number, number]),
      },
    );
    reach.assertReached();
  });
