/** Shipped schema planning is the authority for DSL ownership/counters; PG controls SQL reads. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { column, schema, table, view } from "../../engine/schema.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_MIGRATION_REACH, createReach } from "../../testing/reach.js";
import { migrateCatalog } from "../owner/catalog-migration.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
for (const seed of seedsFor("next-catalog-migration", [0xb788, 0xb789]))
  it(`migrates managed definitions atomically seed ${String(seed)}`, async () => {
    const reach = createReach("catalog migration", CATALOG_MIGRATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 40 }),
        fc.boolean(),
        fc.boolean(),
        async (maximum, published, numberType) => {
          reach.beginHistory();
          const core = await new StorageCore(4).open(),
            old = new MinnowDatabase(new MemoryBlockStore());
          const migrate = (
            definition: Parameters<typeof migrateCatalog>[3],
            options?: Parameters<typeof migrateCatalog>[4],
          ) =>
            migrateCatalog(
              core.harness.path,
              core.lowerer,
              core.harness.governor,
              definition,
              options,
            );
          try {
            const initial = table("managed_t", {
              id: (numberType ? column.number() : column.integer()).unique(),
              amount: column.integer().nullable(),
              label: column.enum(["z", "a"]).default("z"),
            });
            const managedView = view("managed_v", {
              sql: "SELECT id FROM managed_t",
              columns: { id: numberType ? column.number() : column.integer() },
            });
            await old.migrate(schema([initial], { views: [managedView] }));
            await migrate(schema([initial], { views: [managedView] }));
            reach.reach("view declaration");
            await old.execute("CREATE TABLE outside_t(id INTEGER PRIMARY KEY)");
            await core.execute("CREATE TABLE outside_t(id INTEGER PRIMARY KEY)");
            await old.execute(`INSERT INTO managed_t VALUES (${String(maximum)},NULL,'z')`);
            await core.writeBatch("insertBatch", "managed_t", [
              { id: maximum, amount: null, label: "z" },
            ]);
            if (published) {
              await core.harness.path.quiesce();
              reach.reach("published integer maximum");
            } else reach.reach("resident integer maximum");
            const next = table("managed_t", {
              id: (numberType ? column.number() : column.integer()).unique().autoIncrement(),
              amount2: column.integer().nullable().renamedFrom("amount"),
              label: column.enum(["z", "a", "汉"]).default("z"),
              frozen: column.string().backfill("汉"),
            });
            await old.migrate(schema([next], { views: [managedView] }));
            await migrate(schema([next], { views: [managedView] }));
            const catalog = core.harness.path.catalog,
              def = catalog.tableByName("managed_t"),
              id = def?.columns[0]?.identitySequence;
            if (!def || !id) throw new Error("Missing adopted sequence");
            expect(
              inspectCatalog(catalog).tables.find((table) => table.name === "managed_t")?.managed,
            ).toBe(true);
            reach.reach("managed");
            reach.reach("rename");
            reach.reach("widen set");
            reach.reach("backfill");
            reach.reach("adopt counter");
            reach.reach(numberType ? "DSL number counter" : "DSL integer counter");
            const output = { repr: "int64", values: new BigInt64Array(1) } as const;
            await core.harness.path.sequences.next(id, 1, output, 0);
            expect(output.values[0]).toBe(BigInt(maximum + 1));
            await old.execute("INSERT INTO managed_t(label,frozen) VALUES ('a','汉')");
            await core.writeBatch("insertBatch", "managed_t", [
              { id: maximum + 1, amount2: null, label: "a", frozen: "汉" },
            ]);
            const query = "SELECT * FROM managed_t ORDER BY id",
              actual = await core.execute(query);
            if (!("batches" in actual)) throw new Error("Missing migration result");
            expect(oracleRows(actual)).toEqual((await old.query(query)).rows);
            expect((await migrate(schema([next], { views: [managedView] }))).steps).toEqual([]);
            reach.reach("idempotent");
            await core.writeBatch("insertBatch", "managed_t", [
              { id: maximum + 10, amount2: null, label: "汉", frozen: "汉" },
            ]);
            await core.harness.path.sequences.next(id, 1, output, 0);
            expect(output.values[0]).toBe(BigInt(maximum + 11));
            reach.reach("explicit advances");
            const before = core.harness.path.catalog.encode();
            await expect(
              migrate(schema([]), { expectedVersion: catalog.version - 1 }),
            ).rejects.toMatchObject({ sqlState: "40001" });
            expect(core.harness.path.catalog.encode()).toEqual(before);
            reach.reach("stale CAS");
            const badView = view("bad_view", {
              sql: "SELECT id FROM managed_t",
              columns: { id: column.string() },
            });
            await expect(
              migrate(schema([next], { views: [managedView, badView] })),
            ).rejects.toMatchObject({ sqlState: "42804" });
            expect(core.harness.path.catalog.encode()).toEqual(before);
            reach.reach("invalid view rollback");
            const invalid = table("managed_t", {
              id: (numberType ? column.number() : column.integer()).unique().autoIncrement(),
              amount2: column.integer(),
              label: column.enum(["z", "a", "汉"]).default("z"),
              frozen: column.string().backfill("汉"),
            });
            await expect(
              migrate(schema([invalid], { views: [managedView] })),
            ).rejects.toMatchObject({ sqlState: "23502" });
            expect(core.harness.path.catalog.encode()).toEqual(before);
            reach.reach("NOT NULL rollback");
            await core.reopen();
            expect(inspectCatalog(core.harness.path.catalog)).toEqual(inspectCatalog(catalog));
            reach.reach("reopen");
            await expect(migrate(schema([]), { schemaOwnsDatabase: true })).rejects.toMatchObject({
              sqlState: "0A000",
            });
            await migrate(schema([]), { schemaOwnsDatabase: true, allowDestructive: true });
            expect(core.harness.path.catalog.tableByName("managed_t")).toBeUndefined();
            expect(core.harness.path.catalog.tableByName("outside_t")).toBeDefined();
            reach.reach("unmanaged survives");
          } finally {
            await core.close();
            await old.close();
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [3, false, false],
          [4, true, true],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
