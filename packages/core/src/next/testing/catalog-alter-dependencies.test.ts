/** ALTER rebases positions once from stable identities; removed unrelated slots never retarget SQL. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ALTER_DEPENDENCIES_REACH, ADDED_ENUM_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
const state = async (work: () => unknown) => {
  try {
    await work();
    return null;
  } catch (e) {
    if (e && typeof e === "object") {
      if ("sqlState" in e) return e.sqlState;
      if ("code" in e) return e.code;
    }
    throw e;
  }
};
for (const seed of seedsFor("next-catalog-alter-dependencies", [0xb7a0, 0xb7a1]))
  it(`preserves rule identities through DROP sequences seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("ALTER stable dependencies", ALTER_DEPENDENCIES_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 4 }),
          fc.integer({ min: 0, max: 3 }),
          fc.constantFrom("a", "Case", "a.b"),
          fc.boolean(),
          fc.boolean(),
          async (prefix, suffix, name, reverse, checkpoint) => {
            reach.beginHistory();
            const core = await new StorageCore().open();
            try {
              const dead = Array.from({ length: prefix }, (_, i) => `u${String(i)}`),
                a = quote(name);
              const sql = `CREATE TABLE t(id INT PRIMARY KEY,${dead.map((n) => n + " INT").join(",")},${a} INT,c INT,${Array.from({ length: suffix }, (_, i) => `d${String(i)} INT,`).join("")}b INT GENERATED ALWAYS AS (${a}+1) STORED,CHECK(c>=0))`;
              for (const cmd of [
                sql,
                `CREATE VIEW v AS SELECT ${a} FROM t`,
                `CREATE INDEX ix ON t(c DESC)`,
              ]) {
                await pg.exec(cmd);
                await core.execute(cmd);
              }
              const table = core.harness.path.catalog.tableByName("t"),
                aid = table?.columns.find((c) => c.name === name)?.id,
                cid = table?.columns.find((c) => c.name === "c")?.id;
              if (aid === undefined || cid === undefined) throw new Error("missing dependency IDs");
              for (const n of reverse ? [...dead].reverse() : dead) {
                const drop = `ALTER TABLE t DROP COLUMN ${n}`;
                await pg.exec(drop);
                await core.execute(drop);
                const after = core.harness.path.catalog.tableByName("t");
                expect(
                  after?.columns.find((c) => c.name === "b")?.generatedDependencies?.columnIds,
                ).toEqual([aid]);
                expect(after?.checks[0]?.dependencies?.columnIds).toEqual([cid]);
                expect(after?.indexes.find((i) => i.name === "ix")?.columnIds).toEqual([cid]);
                expect(
                  core.harness.path.catalog.views[0]?.dependencies?.find((d) => d.kind === "table")
                    ?.columnIds,
                ).toEqual([aid]);
              }
              reach.reach("generated");
              reach.reach("check");
              reach.reach("view");
              reach.reach("index");
              reach.reach(reverse ? "reverse drops" : "forward drops");
              if (prefix > 1) reach.reach("multiple drops");
              if (suffix === 0) reach.reach("short row");
              else reach.reach("long row");
              if (name !== "a") reach.reach("quoted name");
              if (checkpoint) {
                await core.harness.path.recycleLog();
                reach.reach("checkpoint");
              } else reach.reach("WAL");
              await core.reopen();
              const drop = `ALTER TABLE t DROP COLUMN ${a}`;
              const authority = await state(() => pg.exec(drop));
              expect(authority).toBe("2BP01");
              expect(await state(() => core.execute(drop))).toBe(authority);
              reach.reach("referenced drop refused");
            } finally {
              await core.close();
              await pg.exec("DROP VIEW IF EXISTS v;DROP TABLE IF EXISTS t");
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [1, 0, "a", false, false],
            [4, 3, "a.b", true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
for (const seed of seedsFor("next-catalog-added-enum", [0xb7a2, 0xb7a3]))
  it(`retains added enum identity seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("added enum identity", ADDED_ENUM_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("m", "Case", "a.b"),
          fc.boolean(),
          async (name, checkpoint) => {
            reach.beginHistory();
            const core = await new StorageCore().open();
            try {
              for (const sql of [
                "CREATE TYPE mood AS ENUM('z','a')",
                "CREATE TABLE t(id INT PRIMARY KEY)",
                `ALTER TABLE t ADD COLUMN ${quote(name)} mood`,
              ]) {
                await pg.exec(sql);
                await core.execute(sql);
              }
              const catalog = core.harness.path.catalog;
              expect(catalog.tableByName("t")?.columns[1]?.typeId).toBe(
                catalog.typeByName("mood")?.id,
              );
              reach.reach(name === "m" ? "plain" : "quoted");
              if (checkpoint) {
                await core.harness.path.recycleLog();
                reach.reach("checkpoint");
              } else reach.reach("WAL");
              await core.reopen();
              expect(core.harness.path.catalog.tableByName("t")?.columns[1]?.typeId).toBe(
                catalog.typeByName("mood")?.id,
              );
              expect(await state(() => core.execute("DROP TYPE mood"))).toBe(
                await state(() => pg.exec("DROP TYPE mood")),
              );
              reach.reach("type drop refused");
            } finally {
              await core.close();
              await pg.exec("DROP TABLE IF EXISTS t;DROP TYPE IF EXISTS mood");
            }
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            ["m", false],
            ["a.b", true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
