/** Initial schema creation retains/fixes backfill values as shipped, including generators. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { column, table, schema } from "../../engine/schema.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_MIGRATE_BACKFILL_REACH, createReach } from "../../testing/reach.js";
import { migrateCatalog } from "../owner/catalog-migration.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { StorageCore } from "./storage-core.js";

const kinds = ["boolean", "integer", "float", "text", "enum", "timestamp", "timestamptz"] as const;
function definition(
  kind: (typeof kinds)[number],
  n: number,
  generated: boolean,
  called: () => void,
) {
  const freeze = <T>(value: T): T | (() => T) =>
    generated
      ? () => {
          called();
          return value;
        }
      : value;
  const text = `汉'${String(n)}`,
    date = new Date(Date.UTC(2000, 1, 29, 12, 34, 56, 123) + n);
  const value =
    kind === "boolean"
      ? column.boolean().backfill(freeze(n % 2 === 0))
      : kind === "integer"
        ? column.integer().backfill(freeze(n))
        : kind === "float"
          ? column.number().backfill(freeze(n + 0.25))
          : kind === "text"
            ? column.string().backfill(freeze(text))
            : kind === "enum"
              ? column.enum([text, "suffix"]).backfill(freeze(text))
              : kind === "timestamp"
                ? column.timestamp().backfill(freeze(date))
                : column.datetime().backfill(freeze(date));
  return schema([table("initial", { id: column.integer().unique(), value })]);
}
for (const seed of seedsFor("next-catalog-migrate-backfill", [0xb7ea, 0xb7eb]))
  it(`freezes initial literal/generator backfills once seed ${String(seed)}`, async () => {
    const reach = createReach("initial migrate backfill", CATALOG_MIGRATE_BACKFILL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...kinds),
        fc.integer({ min: -1000, max: 1000 }),
        fc.boolean(),
        fc.boolean(),
        async (kind, n, generated, checkpoint) => {
          reach.beginHistory();
          const core = await new StorageCore().open(),
            old = new MinnowDatabase(new MemoryBlockStore());
          let nativeCalls = 0,
            shippedCalls = 0;
          const native = definition(kind, n, generated, () => nativeCalls++),
            shipped = definition(kind, n, generated, () => shippedCalls++);
          const migrate = () =>
            migrateCatalog(core.harness.path, core.lowerer, core.harness.governor, native);
          try {
            await old.migrate(shipped);
            await migrate();
            const expected = (await old.introspect()).tables[0]?.columns[1]?.backfill;
            expect(expected).toBeDefined();
            const check = () =>
              expect(
                inspectCatalog(core.harness.path.catalog).tables[0]?.columns[1]?.backfill,
              ).toEqual(expected);
            check();
            expect(nativeCalls).toBe(generated ? 1 : 0);
            expect(shippedCalls).toBe(generated ? 1 : 0);
            expect((await old.migrate(shipped)).steps).toEqual([]);
            expect((await migrate()).steps).toEqual([]);
            if (checkpoint) await core.harness.path.recycleLog();
            await core.reopen();
            check();
            expect((await migrate()).steps).toEqual([]);
            expect(nativeCalls).toBe(generated ? 1 : 0);
            expect(shippedCalls).toBe(generated ? 1 : 0);
            expect(core.harness.path.catalog.tableByName("initial")?.managed).toBe(true);
            reach.reach(kind);
            reach.reach(generated ? "generator exactly once" : "literal");
            reach.reach(checkpoint ? "checkpoint" : "WAL");
            reach.reach("idempotent after reopen");
            // Removing a frozen declaration must keep the shared planner's refusal after reopen.
            const before = core.harness.path.catalog.encode();
            const valueColumn = native.tables[0]?.columns.value;
            if (!valueColumn) throw new Error("missing declaration");
            const { backfillValue, ...without } = valueColumn;
            void backfillValue;
            await expect(
              migrateCatalog(
                core.harness.path,
                core.lowerer,
                core.harness.governor,
                schema([table("initial", { id: column.integer().unique(), value: without })]),
              ),
            ).rejects.toThrow("Backfills cannot be removed");
            expect(core.harness.path.catalog.encode()).toEqual(before);
            reach.reach("failure atomic");
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
        numRuns: 32,
        examples: kinds.flatMap((kind) => [
          [kind, -7, false, false] as [(typeof kinds)[number], number, boolean, boolean],
          [kind, 8, true, true] as [(typeof kinds)[number], number, boolean, boolean],
        ]),
      },
    );
    reach.assertReached();
  }, 120_000);
