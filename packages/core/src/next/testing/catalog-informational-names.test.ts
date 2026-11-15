/** Informational FKs retain their declared target after a deliberate parent drop.
 * Authority: CatalogForeignKey's named target and the native informational-drop contract.
 * Current shipped SQL refuses the parent DROP; its pre-drop descriptor is the oracle for
 * labels, not a claim that its current DROP admission matches the native catalog API.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import type { Catalog } from "../../engine/catalog.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_INFORMATIONAL_NAMES_REACH, createReach } from "../../testing/reach.js";
import { inspectCatalog } from "../lower/catalog-schema.js";
import { StorageCore } from "./storage-core.js";

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
function target(catalog: Catalog) {
  return catalog.tables
    .find((t) => t.name === "child")
    ?.foreignKeys.map((key) => ({
      name: key.name,
      columns: key.columns,
      parentTable: key.parentTable,
      parentColumns: key.parentColumns,
      onDelete: key.onDelete,
      enforced: key.enforced,
    }));
}
for (const seed of seedsFor("next-catalog-informational-names", [0xb7e4, 0xb7e5]))
  it(`preserves informational target names after drop and reopen seed ${String(seed)}`, async () => {
    const reach = createReach("informational target names", CATALOG_INFORMATIONAL_NAMES_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("plain", "Case.汉", 'a"b'),
        fc.integer({ min: 1, max: 3 }),
        fc.boolean(),
        async (name, count, checkpoint) => {
          reach.beginHistory();
          const core = await new StorageCore(4).open(),
            old = new MinnowDatabase(new MemoryBlockStore()),
            names = Array.from({ length: count }, (_, i) => `${name}${String(i)}`),
            parent = `parent${name}`,
            parentInput = {
              name: parent,
              columns: names.map((name) => ({
                name,
                type: "number" as const,
                integer: true as const,
              })),
              ...(count === 1 ? { uniqueKey: names[0] ?? "" } : { compositePrimaryKey: names }),
            };
          try {
            await old.createTable(parentInput);
            await core.createTable(parentInput);
            const input = {
              name: "child",
              columns: ["id", ...names].map((name) => ({
                name,
                type: "number" as const,
                integer: true as const,
              })),
              uniqueKey: "id",
              foreignKeys: [
                {
                  name: `fk${name}`,
                  column: names[0] ?? "",
                  columns: names,
                  parentTable: parent,
                  parentColumns: names,
                  onDelete: "restrict" as const,
                  enforced: false,
                },
              ],
            };
            await old.createTable(input);
            await core.createTable(input);
            expect(target(inspectCatalog(core.harness.path.catalog))).toEqual(
              target(await old.introspect()),
            );
            if (checkpoint) {
              await core.harness.path.recycleLog();
              await core.reopen();
            }
            await expect(old.execute(`DROP TABLE ${quote(parent)}`)).rejects.toThrow(
              "references it",
            );
            const expected = target(await old.introspect());
            await core.execute(`DROP TABLE ${quote(parent)}`);
            expect(expected?.[0]?.parentTable).toBe(parent);
            expect(target(inspectCatalog(core.harness.path.catalog))).toEqual(expected);
            reach.reach("dropped parent keeps names");
            if (checkpoint) await core.harness.path.recycleLog();
            await core.reopen();
            expect(target(inspectCatalog(core.harness.path.catalog))).toEqual(expected);
            reach.reach(checkpoint ? "native checkpoint" : "WAL replay");
            reach.reach(count === 1 ? "scalar" : "composite");
            reach.reach(
              name === "plain" ? "plain" : name === "Case.汉" ? "case dot UTF8" : "quote",
            );
            const row = Object.fromEntries(["id", ...names].map((name) => [name, 17]));
            await old.insertBatch("child", [row]);
            await core.writeBatch("insertBatch", "child", [row]);
            reach.reach("remains informational");
            for (const sql of [
              "ALTER TABLE child ADD COLUMN extra INTEGER",
              "ALTER TABLE child RENAME COLUMN extra TO suffix",
            ]) {
              await old.execute(sql);
              await core.execute(sql);
            }
            await core.reopen();
            expect(target(inspectCatalog(core.harness.path.catalog))).toEqual(
              target(await old.introspect()),
            );
            reach.reach("later child ALTER");
            // A newly created table with the same name is not the retired target identity.
            await core.createTable(parentInput);
            await core.reopen();
            expect(target(inspectCatalog(core.harness.path.catalog))).toEqual(expected);
            reach.reach("replacement parent does not retarget");
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
        numRuns: 12,
        examples: [
          ["plain", 1, false],
          ["Case.汉", 2, true],
          ['a"b', 3, false],
          ["plain", 3, true],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
