/** FTS catalog bindings are stable column IDs: ALTER preserves them or refuses the drop. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_FTS_ALTER_REACH } from "../../testing/reach.js";
import { ftsDef } from "../access/fts-def.js";
import { StorageCore } from "./storage-core.js";
import { autocommit } from "../write/test-support.js";
import { oracleRows } from "./differential-adapter.js";
for (const seed of seedsFor("next-catalog-fts-alter", [0xb79e, 0xb79f]))
  it(`keeps FTS bindings across relational changes seed ${String(seed)}`, async () => {
    const reach = createReach("FTS relational ALTER", CATALOG_FTS_ALTER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 2 }),
        fc.boolean(),
        fc.boolean(),
        async (position, checkpoint, rename) => {
          reach.beginHistory();
          const core = await new StorageCore().open();
          try {
            await core.execute("CREATE TYPE e AS ENUM('x')");
            await core.execute("CREATE TABLE docs(kind e,c0 TEXT,c1 TEXT,c2 TEXT)");
            const table = core.harness.path.catalog.tableByName("docs"),
              col = table?.columns[position + 1];
            if (!table || !col) throw new Error("missing columns");
            const { id: _id, ...input } = ftsDef(1, table.id, col.id);
            void _id;
            await autocommit(core.harness.path, (s) =>
              s.changeCatalog({ kind: "createFts", fts: input }),
            );
            const before = core.harness.path.catalog,
              def = before.fts[0];
            // The SQL entry and direct catalog API both refuse before constructing corrupt state.
            await expect(
              core.execute(`ALTER TABLE docs DROP COLUMN ${col.name}`),
            ).rejects.toMatchObject({ sqlState: "2BP01" });
            expect(() =>
              before.change(
                {
                  kind: "alterTable",
                  tableId: table.id,
                  table: {
                    name: table.name,
                    columns: table.columns
                      .filter((c) => c.id !== col.id)
                      .map(({ defaultDependencies, generatedDependencies, ...c }) => {
                        expect(defaultDependencies).toBeUndefined();
                        expect(generatedDependencies).toBeUndefined();
                        return c;
                      }),
                    primaryKey: null,
                    unique: [],
                    checks: [],
                    foreignKeys: [],
                  },
                },
                before.version + 1,
              ),
            ).toThrow(expect.objectContaining({ code: "MINNOW_CATALOG_CHANGE" }));
            expect(core.harness.path.catalog).toBe(before);
            reach.reach("SQL dependency refusal");
            reach.reach("direct dependency refusal");
            await core.writeBatch("insertBatch", "docs", [
              { kind: "x", c0: "alpha", c1: "alpha", c2: "alpha" },
            ]);
            await core.execute("ALTER TABLE docs ADD COLUMN extra INTEGER DEFAULT 7");
            const name = rename ? "renamed" : col.name;
            if (rename) {
              await core.execute(`ALTER TABLE docs RENAME COLUMN ${col.name} TO renamed`);
              reach.reach("rename");
            } else reach.reach("original name");
            expect(core.harness.path.catalog.fts).toEqual([def]);
            if (checkpoint) {
              await core.harness.path.quiesce();
              await core.harness.path.recycleLog();
              reach.reach("checkpoint");
            } else reach.reach("WAL");
            await core.reopen();
            expect(core.harness.path.catalog.fts).toEqual([def]);
            const result = await core.execute(
              `SELECT MATCH(${name}) AGAINST 'alpha' AS matched,extra FROM docs`,
            );
            if (!("batches" in result)) throw new Error("missing result");
            expect(oracleRows(result)).toEqual([{ matched: true, extra: 7 }]);
            reach.reach("scan after ALTER");
            reach.reach(
              position === 0 ? "first column" : position === 1 ? "middle column" : "last column",
            );
          } finally {
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [0, false, false],
          [1, true, true],
          [2, false, true],
          [0, true, false],
        ],
      },
    );
    reach.assertReached();
  }, 120_000);
