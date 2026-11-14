import { ftsDef } from "../access/fts-def.js";
/** MCAT must not expose a malformed identity/dependency graph as a CatalogView. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_GRAPH_REACH, CATALOG_LOG_SHAPE_REACH } from "../../testing/reach.js";
import { Catalog, decodeLogged, type LoggedChange, type CatalogOperation } from "./catalog.js";
import { tableInput, column, INT } from "./test-support.js";
import {
  encodeCatalogStream,
  encodeCatalogChange,
  decodeCatalogStream,
  type CatalogStreamData,
} from "./catalog-stream.js";
for (const seed of seedsFor("next-catalog-graph", [0xb75e, 0xb75f]))
  it(`refuses corrupt catalog identities and references seed ${String(seed)}`, () => {
    const reach = createReach("stored catalog graph", CATALOG_GRAPH_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 7 }),
        fc.integer({ min: 1, max: 6 }),
        fc.string({ maxLength: 50 }),
        (mode, count, suffix) => {
          reach.beginHistory();
          let catalog = Catalog.empty();
          for (let i = 0; i < count; i++)
            catalog = catalog.change(
              {
                kind: "createTable",
                table: tableInput(
                  `t${String(i)}.${suffix}`,
                  [column("a", INT, false), column("b", INT)],
                  {
                    foreignKeys: [
                      {
                        name: "self",
                        columns: [0],
                        parentTableId: "self",
                        parentIndexId: "primary",
                        onDelete: "restrict",
                        enforced: true,
                      },
                    ],
                  },
                ),
              },
              catalog.version + 1,
            ).catalog;
          const data = decodeCatalogStream(catalog.encode());
          expect(Catalog.decode(encodeCatalogStream(data)).encode()).toEqual(catalog.encode());
          const table = data.tables[0];
          if (!table) throw new Error("Missing generated table");
          let bad: CatalogStreamData;
          if (mode === 0) bad = { ...data, counters: { ...data.counters, nextTableId: table.id } };
          else if (mode === 1)
            bad = {
              ...data,
              tables: [
                {
                  ...table,
                  columns: table.columns.map((c) => ({ ...c, id: table.columns[0]?.id ?? 0 })),
                },
                ...data.tables.slice(1),
              ],
            };
          else if (mode === 2)
            bad = {
              ...data,
              tables: [
                {
                  ...table,
                  indexes: table.indexes.map((i) => ({
                    ...i,
                    columnIds: [data.counters.nextColumnId],
                  })),
                },
                ...data.tables.slice(1),
              ],
            };
          else if (mode === 3)
            bad = {
              ...data,
              tables: [
                {
                  ...table,
                  foreignKeys: table.foreignKeys.map((f) => ({
                    ...f,
                    parentTableId: data.counters.nextTableId,
                  })),
                },
                ...data.tables.slice(1),
              ],
            };
          else if (mode === 4)
            bad = {
              ...data,
              tables: [
                {
                  ...table,
                  columns: table.columns.map((c) => ({
                    ...c,
                    identitySequence: data.counters.nextSequenceId,
                  })),
                },
                ...data.tables.slice(1),
              ],
            };
          else if (mode === 5)
            bad = {
              ...data,
              counters: { ...data.counters, nextViewId: 2 },
              views: [{ id: 1, name: table.name, bodySql: "SELECT 1" }],
            };
          else if (mode === 6)
            bad = {
              ...data,
              tables: [
                { ...table, primaryKey: data.counters.nextIndexId },
                ...data.tables.slice(1),
              ],
            };
          else
            bad = {
              ...data,
              counters: { ...data.counters, nextTypeId: 2 },
              types: [{ id: 1, name: "dup", labels: [suffix, suffix] }],
            };
          expect(() => Catalog.decode(encodeCatalogStream(bad))).toThrow(
            expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
          );
          const state = CATALOG_GRAPH_REACH[mode];
          if (!state) throw new Error("Missing graph reach state");
          reach.reach(state);
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: Array.from(
          { length: 8 },
          (_, i) => [i, 2, "Case.汉"] as [number, number, string],
        ),
      },
    );
    reach.assertReached();
  });

// Conditional WAL fields are semantic: dropping an absent id must never become a no-op.
it.each(seedsFor("next-catalog-log-shape", [0xb760, 0xb761]))(
  "refuses incomplete and contradictory logged operations seed %i",
  (seed) => {
    const source = Catalog.empty().change(
      { kind: "createTable", table: tableInput("wal", [column("a", INT, false)]) },
      1,
    );
    const table = source.catalog.tables[0],
      index = table?.indexes[0];
    if (!table || !index) throw new Error("Missing WAL fixture");
    const sequence = {
      id: 1,
      name: "s",
      increment: 1n,
      min: 1n,
      max: 999n,
      start: 1n,
      cycle: false,
      ownedBy: null,
    };
    const inputs: CatalogOperation[] = [
      { op: "createTable", table, sequences: [] },
      { op: "dropTable", tableId: 1 },
      { op: "createIndex", index },
      { op: "invalidateIndex", tableId: 1, indexId: 1 },
      { op: "dropIndex", tableId: 1, indexId: 1 },
      { op: "putSequence", sequence },
      { op: "dropSequence", sequenceId: 1 },
      { op: "createView", view: { id: 1, name: "v", bodySql: "SELECT 1" } },
      { op: "dropView", viewId: 1 },
      { op: "createType", type: { id: 1, name: "typ", labels: ["a"] } },
      { op: "dropType", typeId: 1 },
      { op: "createFts", fts: ftsDef(1, 1, 1) },
      { op: "upsertFts", fts: ftsDef(1, 1, 1) },
      { op: "dropFts", ftsId: 1 },
    ];
    const operations: LoggedChange[] = inputs.map((operation) => ({
      ...operation,
      expectedVersion: 0,
      version: 1,
    }));
    const reach = createReach("catalog log shape", CATALOG_LOG_SHAPE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: operations.length - 1 }),
        fc.nat(),
        (which, position) => {
          reach.beginHistory();
          const change = operations[which];
          if (!change) throw new Error("Missing WAL operation");
          expect(decodeLogged(encodeCatalogChange(change))).toEqual(change);
          const fields = Object.keys(change).filter((k) => k !== "op");
          const removed = fields[position % fields.length];
          if (!removed) throw new Error("Missing WAL field");
          const bad = Object.fromEntries(Object.entries(change).filter(([key]) => key !== removed));
          expect(() => decodeLogged(encodeCatalogChange(bad as unknown as LoggedChange))).toThrow(
            expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
          );
          const extra = Object.hasOwn(change, "viewId") ? "indexId" : "viewId";
          expect(() => decodeLogged(encodeCatalogChange({ ...change, [extra]: 1 }))).toThrow(
            expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
          );
          reach.reach("missing required field");
          reach.reach("contradictory field");
          reach.reach(change.op);
        },
      ),
      { seed, numRuns: 24, examples: operations.map((_, i) => [i, 0] as [number, number]) },
    );
    reach.assertReached();
  },
);
