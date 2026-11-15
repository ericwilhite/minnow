/** CreateTableInput.backfill is durable metadata, distinct from the insert-time default.
 * Authority: storage/types.ts TableColumnRecord.backfill and the shipped public introspector.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase, type CreateTableInput } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_CREATE_BACKFILL_REACH, createReach } from "../../testing/reach.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const kinds = ["boolean", "integer", "float", "text", "enum", "timestamp", "timestamptz"] as const;
function definition(kind: (typeof kinds)[number], n: number): CreateTableInput {
  const base = { name: "value", nullable: false };
  const column =
    kind === "boolean"
      ? { ...base, type: "boolean" as const, backfill: n % 2 === 0 }
      : kind === "integer"
        ? { ...base, type: "number" as const, integer: true as const, backfill: n }
        : kind === "float"
          ? { ...base, type: "number" as const, backfill: n + 0.25 }
          : kind === "text" || kind === "enum"
            ? {
                ...base,
                type: "string" as const,
                backfill: `汉'${String(n)}`,
                ...(kind === "enum" ? { enumValues: [`汉'${String(n)}`, "suffix"] } : {}),
              }
            : {
                ...base,
                type: "datetime" as const,
                backfill: new Date(Date.UTC(2000, 1, 29, 12, 34, 56, 123) + n),
                ...(kind === "timestamp" ? { withoutTimeZone: true as const } : {}),
              };
  return {
    name: "frozen",
    managed: n % 2 === 0,
    columns: [
      { name: "id", type: "number", integer: true },
      { ...column, defaultValue: { kind: "literal", value: column.backfill } },
    ],
    uniqueKey: "id",
  };
}

for (const seed of seedsFor("next-catalog-create-backfill", [0xb7e2, 0xb7e3]))
  it(`retains creation-time backfills through ordinary reopen seed ${String(seed)}`, async () => {
    const reach = createReach("CREATE backfill", CATALOG_CREATE_BACKFILL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...kinds),
        fc.integer({ min: -1000, max: 1000 }),
        fc.boolean(),
        async (kind, n, checkpoint) => {
          reach.beginHistory();
          const input = definition(kind, n),
            core = await new StorageCore(4).open(),
            old = new MinnowDatabase(new MemoryBlockStore());
          try {
            await old.createTable(input);
            await core.createTable(input);
            const expected = (await old.introspect()).tables[0]?.columns[1];
            const catalogBytes = core.harness.path.catalog.encode();
            const check = () => {
              const actual = inspectCatalog(core.harness.path.catalog).tables[0]?.columns[1];
              expect(actual?.backfill).toEqual(expected?.backfill);
              // Native defaults are normalized SQL; the old API literal/expression tag is
              // not its storage format. Assert preservation independently of that tag.
              expect(actual?.defaultValue).toBeDefined();
              expect(core.harness.path.catalog.encode()).toEqual(catalogBytes);
              expect(core.harness.path.catalog.tables[0]?.columns[1]?.missing).not.toBeNull();
              expect(core.harness.path.catalog.tables[0]?.columns[0]?.missing).toBeNull();
            };
            check();
            const invalid = {
              ...input,
              name: "invalid_generated_backfill",
              columns: input.columns.map((column, i) => {
                if (i !== 1) return column;
                const { defaultValue, ...rest } = column;
                void defaultValue;
                return { ...rest, generatedValue: { kind: "stored" as const, sql: "id" } };
              }),
            };
            await expect(old.createTable(invalid)).rejects.toThrow("backfill");
            await expect(core.createTable(invalid)).rejects.toMatchObject({ sqlState: "42601" });
            expect(core.harness.path.catalog.encode()).toEqual(catalogBytes);
            reach.reach("generated backfill refused before mutation");
            reach.reach(kind);
            const value = input.columns[1]?.backfill;
            if (value === undefined) throw new Error("missing generated input");
            await old.insertBatch("frozen", [{ id: 1, value }]);
            await core.writeBatch("insertBatch", "frozen", [{ id: 1, value }]);
            if (checkpoint) {
              await core.harness.path.quiesce();
              await core.harness.path.recycleLog();
            }
            await core.reopen();
            check();
            reach.reach(checkpoint ? "checkpoint reopen" : "WAL reopen");
            const sql = "SELECT * FROM frozen ORDER BY id",
              result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("missing reopened rows");
            expect(oracleRows(result)).toEqual((await old.query(sql)).rows);
            await core.writeBatch("insertBatch", "frozen", [{ id: 2, value }]);
            await core.reopen();
            check();
            reach.reach("continue writing");
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
        numRuns: 24,
        examples: kinds.flatMap((kind) => [
          [kind, -7, false] as [(typeof kinds)[number], number, boolean],
          [kind, 8, true] as [(typeof kinds)[number], number, boolean],
        ]),
      },
    );
    reach.assertReached();
  }, 120_000);
