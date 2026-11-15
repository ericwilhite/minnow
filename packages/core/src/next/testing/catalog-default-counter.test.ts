/** Adopting/removing an owned counter clears ordinary-default source metadata. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { column, schema, table } from "../../engine/schema.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_DEFAULT_COUNTER_REACH, createReach } from "../../testing/reach.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { migrateCatalog } from "../owner/catalog-migration.js";
import { StorageCore } from "./storage-core.js";
for (const seed of seedsFor("next-catalog-default-counter", [0xb7f0, 0xb7f1]))
  it(`clears source identity through counter transitions seed ${String(seed)}`, async () => {
    const reach = createReach("default counter source", CATALOG_DEFAULT_COUNTER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 1, max: 50 }),
        async (floating, expression, n) => {
          reach.beginHistory();
          const core = await new StorageCore().open(),
            old = new MinnowDatabase(new MemoryBlockStore());
          const base = () => (floating ? column.number().unique() : column.integer().unique());
          const initial = schema([
            table("counter", { id: expression ? base().defaultSql(String(n)) : base().default(n) }),
          ]);
          const automatic = schema([table("counter", { id: base().autoIncrement() })]);
          const plain = schema([table("counter", { id: base() })]);
          const migrate = (def: Parameters<typeof migrateCatalog>[3]) =>
            migrateCatalog(core.harness.path, core.lowerer, core.harness.governor, def);
          try {
            for (const def of [initial, automatic, plain, initial]) {
              await old.migrate(def);
              await migrate(def);
              expect(
                inspectCatalog(core.harness.path.catalog).tables[0]?.columns[0]?.defaultValue,
              ).toEqual((await old.introspect()).tables[0]?.columns[0]?.defaultValue);
              if (def === automatic) {
                expect(
                  core.harness.path.catalog.tables[0]?.columns[0]?.defaultSource,
                ).toBeUndefined();
                reach.reach("adopt counter");
              }
              if (def === plain) {
                expect(
                  core.harness.path.catalog.tables[0]?.columns[0]?.defaultSource,
                ).toBeUndefined();
                reach.reach("remove counter");
              }
              await core.reopen();
              expect((await migrate(def)).steps).toEqual([]);
            }
            reach.reach(floating ? "float counter" : "integer counter");
            reach.reach(expression ? "expression" : "literal");
            reach.reach("reopen idempotent");
          } finally {
            await core.close();
            await old.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          reach.reach("zero resources");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, false, 1],
          [false, true, 2],
          [true, false, 3],
          [true, true, 4],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
