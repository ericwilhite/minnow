import { need } from "./bytes.js";
/** Storage admission follows the catalog contract, including dangling informational FKs. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  CATALOG_REVIEW_REACH,
  CATALOG_INFORMATIONAL_REACH,
  CATALOG_WAL_VERSION_REACH,
} from "../../testing/reach.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { Catalog, decodeLogged, encodeLogged } from "./catalog.js";
import { Memtable } from "./memtable.js";
import { LogCorruptionError } from "../format/log-frame.js";
import * as streamCodec from "./catalog-stream.js";
import { decodeCatalogStream, encodeCatalogStream } from "./catalog-stream.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  TEXT,
  rules,
  batch,
  ints,
} from "./test-support.js";

// eslint-disable-next-line @typescript-eslint/unbound-method -- Captured once; every invocation below supplies the original Catalog receiver with .call().
const originalChange = Catalog.prototype.change;
afterEach(() => vi.restoreAllMocks());

const modes = [
  "enum missing labels",
  "enum empty labels",
  "missing vector huge length",
  "missing vector short data",
  "declared mismatch",
  "physical wrong conditional field",
  "physical missing width",
  "decimal missing precision",
  "vector wrong layout",
  "support wrong columns",
  "missing support",
  "parent key type",
  "short validity",
  "array missing element",
] as const;
for (const seed of seedsFor("next-catalog-review-shapes", [0xb76c, 0xb76d]))
  it(`refuses semantic corruptions before exposing a catalog (seed ${String(seed)})`, () => {
    const reach = createReach("catalog review shapes", CATALOG_REVIEW_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: modes.length - 1 }),
        fc.integer({ min: 1, max: 100 }),
        (mode, value) => {
          reach.beginHistory();
          let catalog = Catalog.empty();
          catalog = catalog.change(
            { kind: "createTable", table: tableInput("p", [column("id", INT, false)]) },
            1,
          ).catalog;
          catalog = catalog.change(
            {
              kind: "createTable",
              table: tableInput(
                "c",
                [column("id", INT, false), column("p", INT), column("other", INT)],
                {
                  foreignKeys: [
                    {
                      name: "fk",
                      columns: [1],
                      parentTableId: 1,
                      parentIndexId: 1,
                      enforced: true,
                      onDelete: "restrict",
                    },
                  ],
                },
              ),
            },
            2,
          ).catalog;
          // Deliberately untyped corruption mutations; encode a structurally valid but semantically invalid record.
          const data = decodeCatalogStream(catalog.encode());
          const mutable = structuredClone(data) as unknown as {
            tables: Array<{
              columns: Array<Record<string, unknown>>;
              indexes: Array<{ id: number; columnIds: number[] }>;
              foreignKeys: Array<{ supportIndexId: number | null }>;
            }>;
          };
          const child = need(mutable.tables[1]);
          const target = need(child.columns[mode < 2 ? 2 : 1]);
          if (mode === 0) {
            target.type = { kind: "enum", name: "unresolved" };
            target.declared = { type: "string" };
          }
          if (mode === 1) {
            target.type = { kind: "enum", name: "unresolved", labels: [] };
            target.declared = {
              type: "string",
              sqlDomain: { kind: "enum", name: "unresolved", values: [] },
            };
          }
          if (mode === 2)
            target.missing = {
              form: "flat",
              type: INT,
              length: Number.MAX_SAFE_INTEGER,
              validity: null,
              data: { layout: "i32", values: Int32Array.of(value) },
            };
          if (mode === 3)
            target.missing = {
              form: "flat",
              type: INT,
              length: 1,
              validity: null,
              data: { layout: "i32", values: new Int32Array() },
            };
          if (mode === 4) target.declared = { type: "string" };
          if (mode === 5) target.type = { ...INT, zoned: true };
          if (mode === 6) target.type = { kind: "int32" };
          if (mode === 7) target.type = { kind: "decimal", scale: 2 };
          if (mode === 8)
            target.missing = {
              form: "flat",
              type: INT,
              length: 1,
              validity: null,
              data: { layout: "varlen", offsets: Int32Array.of(0, 0), bytes: new Uint8Array() },
            };
          if (mode === 9)
            need(
              child.indexes.find((i) => i.id === need(child.foreignKeys[0]).supportIndexId),
            ).columnIds = [need(child.columns[2]).id as number];
          if (mode === 10) need(child.foreignKeys[0]).supportIndexId = null;
          if (mode === 11) {
            need(need(mutable.tables[0]).columns[0]).type = TEXT;
            need(need(mutable.tables[0]).columns[0]).declared = { type: "string" };
          }
          if (mode === 12)
            target.missing = {
              form: "flat",
              type: INT,
              length: 1,
              validity: new Uint32Array(),
              data: { layout: "i32", values: Int32Array.of(value) },
            };
          if (mode === 13) target.type = { kind: "array" };
          expect(
            () => Catalog.decode(encodeCatalogStream(mutable as unknown as typeof data)),
            modes[mode],
          ).toThrow(StoreCorruptionError);
          reach.reach(need(modes[mode]));
        },
      ),
      { seed, numRuns: 28, examples: modes.map((_, i) => [i, 1] as [number, number]) },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-catalog-informational-drop", [0xb76e, 0xb76f]))
  it(`reopens an informational reference after its parent drops (seed ${String(seed)})`, async () => {
    const reach = createReach("informational FK lifecycle", CATALOG_INFORMATIONAL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.constantFrom("memory" as const, "sim" as const),
        async (checkpoint, store) => {
          reach.beginHistory();
          const h = new Harness("strict", store, 1024, undefined, { merges: { mode: "off" } });
          await h.open();
          try {
            await autocommit(h.path, (s) =>
              s.changeCatalog({
                kind: "createTable",
                table: tableInput("p", [column("id", INT, false)]),
              }),
            );
            await autocommit(h.path, (s) =>
              s.changeCatalog({
                kind: "createTable",
                table: tableInput("c", [column("id", INT, false)], {
                  foreignKeys: [
                    {
                      name: "fk",
                      columns: [0],
                      parentTableId: 1,
                      parentIndexId: 1,
                      enforced: false,
                      onDelete: "restrict",
                    },
                  ],
                }),
              }),
            );
            await autocommit(h.path, (s) => s.changeCatalog({ kind: "dropTable", tableId: 1 }));
            if (checkpoint) await h.path.recycleLog();
            await h.reopen();
            reach.reach(store);
            reach.reach(checkpoint ? "checkpoint" : "WAL only");
            expect(h.path.catalog.tableByName("p")).toBeUndefined();
            expect(h.path.catalog.tableByName("c")?.foreignKeys[0]).toMatchObject({
              enforced: false,
              parentTableId: 1,
            });
          } finally {
            await h.close();
            expect(h.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [false, "memory"],
          [true, "memory"],
          [false, "sim"],
          [true, "sim"],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

it("carries and checks the WAL catalog predecessor instead of inventing a replay version", () => {
  const before = Catalog.empty();
  const result = before.change(
    { kind: "createTable", table: tableInput("t", [column("id", INT)]) },
    17,
  );
  const logged = need(result.logged[0]);
  const decoded = decodeLogged(encodeLogged(logged));
  expect(decoded.expectedVersion).toBe(0);
  expect(decoded.version).toBe(17);
  expect(before.apply(decoded, 17).version).toBe(17);
  expect(() => before.withVersion(3).apply(decoded, 17)).toThrow();
});

for (const seed of seedsFor("next-catalog-WAL-precondition", [0xb770, 0xb771]))
  for (const streamed of [false, true])
    it(`checks native WAL versions before row effects ${streamed ? "streamed" : "ordinary"} seed ${String(seed)}`, async () => {
      const reach = createReach("native catalog WAL preconditions", CATALOG_WAL_VERSION_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 4 }),
          fc.constantFrom("sim" as const, "memory" as const),
          async (burn, store) => {
            reach.beginHistory();
            const h = new Harness("strict", store, 1024, undefined, { merges: { mode: "off" } });
            await h.open();
            try {
              await autocommit(h.path, (s) =>
                s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("p", [column("id", INT, false)]),
                }),
              );
              for (let i = 0; i < burn; i++) {
                const tx = await h.path.begin(),
                  statement = tx.statement({ rules });
                await statement.changeCatalog({
                  kind: "createTable",
                  table: tableInput("rolled_back", [column("id", INT)]),
                });
                await statement.end();
                tx.rollback();
              }
              await autocommit(h.path, (s) =>
                s.changeCatalog({
                  kind: "createTable",
                  table: tableInput("kept", [column("id", INT)]),
                }),
              );
              const version = h.path.catalog.version;
              expect(version).toBeGreaterThan(2);
              await h.reopen();
              expect(h.path.catalog.version).toBe(version);
              reach.reach("live-assigned version gap preserved");
              await h.path.recycleLog();
              vi.spyOn(Catalog.prototype, "change").mockImplementation(function (
                this: Catalog,
                change,
                nextVersion,
              ) {
                const result = originalChange.call(this, change, nextVersion);
                return {
                  ...result,
                  logged: result.logged.map((entry) => ({
                    ...entry,
                    expectedVersion: entry.expectedVersion - 1,
                  })),
                };
              });
              const streaming = vi.spyOn(streamCodec, "writeCatalogChange");
              const tx = await h.path.begin();
              const rows = tx.statement({ rules });
              await rows.table(1).insert(batch(ints([1])), null);
              await rows.end();
              const ddl = tx.statement({ rules });
              const text = column("note", TEXT);
              await ddl.changeCatalog({
                kind: "createTable",
                table: tableInput("bad", [
                  column("id", INT),
                  { ...text, defaultSql: streamed ? "'" + "x".repeat(600000) + "'" : "'x'" },
                ]),
              });
              await ddl.end();
              const receipt = await tx.commit(null);
              await receipt.durable;
              expect(streaming.mock.calls.length > 0).toBe(streamed);
              vi.restoreAllMocks();
              const append = vi.spyOn(Memtable.prototype, "append");
              let failure: unknown;
              try {
                await h.reopen();
              } catch (error) {
                failure = error;
              }
              expect(
                append,
                "catalog preflight must run before every row effect",
              ).not.toHaveBeenCalled();
              expect(failure).toBeInstanceOf(LogCorruptionError);
              reach.reach("bad predecessor refused");
              reach.reach("no earlier row effect applied");
              reach.reach(store);
            } finally {
              vi.restoreAllMocks();
              await h.close();
              expect(h.governor.usedBytes).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 2,
          examples: [
            [1, "sim"],
            [4, "memory"],
          ],
        },
      );
      reach.assertReached();
    }, 120000);
