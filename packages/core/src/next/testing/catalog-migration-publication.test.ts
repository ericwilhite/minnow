/** A schema check/adoption must keep scanning the suffix when its memory source publishes. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_MIGRATION_PUBLICATION_REACH, createReach } from "../../testing/reach.js";
import { identityMaximum } from "../write/identity-maximum.js";
import { validateAlterNullability } from "../write/alter-validation.js";
import { StorageCore } from "./storage-core.js";
for (const seed of seedsFor("next-catalog-migration-publication", [0xb78c, 0xb78d]))
  it(`checks live suffix after publication seed ${String(seed)}`, async () => {
    const reach = createReach("migration publication", CATALOG_MIGRATION_PUBLICATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2049, max: 4100 }),
        fc.boolean(),
        async (count, nullSuffix) => {
          reach.beginHistory();
          const core = await new StorageCore(count + 1).open();
          try {
            await core.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER)");
            await core.writeBatch(
              "insertBatch",
              "t",
              Array.from({ length: count }, (_, i) => ({
                id: i + 1,
                n: nullSuffix && i === count - 1 ? null : i + 1,
              })),
            );
            const snapshot = core.harness.path.pin(),
              grant = await core.harness.governor.admit({
                label: "test/migration",
                minimumBytes: 8 * 1024 * 1024,
                spillable: false,
              });
            try {
              const def = snapshot.catalog.tableByName("t");
              if (!def) throw new Error("Missing migration table");
              const table = snapshot.table(def.id, { budget: grant });
              const memory = table.segments[0];
              if (memory?.kind !== "memory" || !memory.portableChunk)
                throw new Error("Missing live prefix");
              const read = memory.portableChunk.bind(memory);
              let published = false;
              memory.portableChunk = async (id, chunk) => {
                const result = await read(id, chunk);
                if (!published) {
                  published = true;
                  // A quiet partial live memtable does not publish at quiesce. Force
                  // its freeze with later rows, which this already-pinned view excludes.
                  await core.writeBatch("insertBatch", "t", [
                    { id: count + 1, n: count + 100 },
                    { id: count + 2, n: count + 101 },
                  ]);
                  await core.harness.path.quiesce();
                  expect((await read(id, chunk)).kind).toBe("stored");
                  reach.reach("publication after read");
                }
                return result;
              };
              const check = () => {
                if (snapshot.released) throw new Error("Released snapshot");
              };
              const column = def.columns[1];
              if (!column) throw new Error("Missing integer column");
              if (nullSuffix) {
                await expect(
                  validateAlterNullability(
                    table,
                    {
                      ...def,
                      columns: def.columns.map((column) => ({ ...column, nullable: false })),
                    },
                    check,
                  ),
                ).rejects.toMatchObject({ sqlState: "23502" });
                reach.reach("NULL only in suffix");
              } else {
                expect(await identityMaximum(snapshot, table, column, grant, check)).toBe(
                  BigInt(count),
                );
                reach.reach("maximum only in suffix");
              }
              reach.reach(count > 4096 ? "three chunks" : "two chunks");
            } finally {
              snapshot.release();
              grant.close();
            }
          } finally {
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [2051, false],
          [4099, true],
          [4099, false],
          [2051, true],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
