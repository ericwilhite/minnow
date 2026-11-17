/** PG authority for concrete native RI, through real row writers and the B6 queue. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_RI_MUTATION_REACH,
  NATIVE_RI_GENERATED_KEY_REACH,
  NATIVE_RI_FOLLOW_REACH,
  NATIVE_RI_INCREMENTAL_UNIQUE_REACH,
} from "../../testing/reach.js";
import { referentialFixture } from "./referential.test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { batch, ints } from "../write/test-support.js";

async function code(run: () => Promise<unknown>): Promise<string | null> {
  try {
    await run();
    return null;
  } catch (error) {
    if (typeof error !== "object" || error === null) throw error;
    if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
    if ("code" in error && typeof error.code === "string") return error.code;
    throw error;
  }
}
for (const seed of seedsFor("next-native-ri-mutations", [0xb639, 0xb63a]))
  it(`matches PG concrete native RI actions seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native mutating RI", NATIVE_RI_MUTATION_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION"),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 0, max: 9 }),
          fc.constantFrom("valid", "same", "missing"),
          fc.constantFrom("plain", "notNull", "check", "generated"),
          async (mode, update, published, count, defaultKind, rowRule) => {
            reach.beginHistory();
            const target = defaultKind === "same" ? 1 : defaultKind === "missing" ? 999 : 99,
              setup = [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT ${rowRule === "notNull" ? "NOT NULL" : ""} DEFAULT ${String(target)} REFERENCES p(id) ON DELETE ${mode} ON UPDATE ${mode}${rowRule === "check" ? " CHECK(pid<5)" : ""},v INT CHECK(v>0)${rowRule === "generated" ? ",g INT GENERATED ALWAYS AS(COALESCE(pid,0)*2) STORED CHECK(g>0)" : ""})`,
                "INSERT INTO p VALUES(1,1),(2,2),(99,99)",
                ...(count
                  ? [
                      `INSERT INTO c(id,pid,v) VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${rowRule !== "notNull" && rowRule !== "generated" && i % 4 === 0 ? "NULL" : String((i % 2) + 1)},1)`).join(",")}`,
                    ]
                  : []),
              ],
              f = await referentialFixture(setup, published);
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              const expected = await code(() =>
                pg.exec(update ? "UPDATE p SET id=id+10 WHERE id<3" : "DELETE FROM p WHERE id<3"),
              );
              const observed = await code(async () => {
                const input = await f.capture("p", [0, 1]);
                for (const [i, row] of input.refs.entries()) {
                  if (update)
                    await f.update(
                      input.tableId,
                      row.id,
                      batch(ints([i + 11]), ints([i + 1])),
                      row.image,
                    );
                  else
                    await f.rows.write(
                      f.controller.root,
                      input.tableId,
                      "delete",
                      row.image,
                      row.id,
                      f.controller,
                      f.stop.signal,
                    );
                }
                await f.end();
              });
              expect(observed, `${mode} ${update ? "UPDATE" : "DELETE"} ${defaultKind}`).toBe(
                expected,
              );
              if (observed !== null) await f.rollback();
              {
                const sql = `SELECT id,pid,v${rowRule === "generated" ? ",g" : ""} FROM c ORDER BY id`,
                  actual = await f.core.execute(sql),
                  desired = (await pg.query<Record<string, unknown>>(sql)).rows;
                expect(actual.kind).toBe("rows");
                if (!("batches" in actual)) throw new Error("missing child query");
                expect(
                  actual.batches.flatMap((batch) =>
                    Array.from({ length: batch.length }, (_, i) =>
                      batch.columns.map((vector) => valueAt(vector, i)),
                    ),
                  ),
                ).toEqual(desired.map((row) => Object.values(row)));
              }
              reach.reach(mode);
              reach.reach(rowRule);
              reach.reach(update ? "rekey" : "delete");
              reach.reach(published ? "published" : "resident");
              reach.reach(count ? "children" : "empty");
              reach.reach(defaultKind);
              reach.reach(observed === null ? "commit" : "rollback");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ["CASCADE", false, false, 9, "valid", "plain"],
            ["CASCADE", true, true, 8, "valid", "generated"],
            ["SET NULL", true, false, 7, "valid", "notNull"],
            ["SET DEFAULT", false, true, 8, "valid", "check"],
            ["SET DEFAULT", true, false, 5, "same", "plain"],
            ["SET DEFAULT", false, false, 4, "missing", "plain"],
            ["RESTRICT", false, false, 3, "valid", "plain"],
            ["NO ACTION", true, true, 3, "valid", "plain"],
            ["CASCADE", false, false, 0, "valid", "plain"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  });

for (const seed of seedsFor("next-native-ri-follow", [0xb63b, 0xb63c]))
  it(`preserves original child identities through native follow swaps ${String(seed)}`, async () => {
    const { currentEngine } = await import("../../testing/differential-current.js"),
      shipped = await currentEngine("memory"),
      reach = createReach("native original follow", NATIVE_RI_FOLLOW_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 2, max: 12 }),
          async (published, informational, count) => {
            reach.beginHistory();
            const setup = [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id)${informational ? " NOT ENFORCED" : ""},v INT GENERATED ALWAYS AS(pid+10) STORED CHECK(v>0))`,
                "INSERT INTO p VALUES(1,1),(2,2)",
                `INSERT INTO c(id,pid) VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String((i % 2) + 1)})`).join(",")}`,
              ],
              f = await referentialFixture(setup, published);
            try {
              await shipped.run({ kind: "sql", sql: "DROP TABLE IF EXISTS c" });
              await shipped.run({ kind: "sql", sql: "DROP TABLE IF EXISTS p" });
              for (const sql of setup) await shipped.run({ kind: "sql", sql });
              await shipped.run({ kind: "sql", sql: "UPDATE p SET id=3-id" });
              const input = await f.capture("p", [0, 1]);
              for (const [i, row] of input.refs.entries())
                await f.update(
                  input.tableId,
                  row.id,
                  batch(ints([2 - i]), ints([i + 1])),
                  row.image,
                );
              await f.end();
              const result = await f.core.execute("SELECT id,pid,v FROM c ORDER BY id");
              if (!("batches" in result)) throw new Error("missing native children");
              const native = result.batches.flatMap((batch) =>
                Array.from({ length: batch.length }, (_, i) =>
                  batch.columns.map((vector) => valueAt(vector, i)),
                ),
              );
              expect(native).toEqual(
                Array.from({ length: count }, (_, i) => [i + 1, 2 - (i % 2), 12 - (i % 2)]),
              );
              const old = await shipped.run({
                kind: "sql",
                sql: "SELECT id,pid,v FROM c ORDER BY id",
              });
              expect(native).toEqual(old.rows?.map((row) => [row.id, row.pid, row.v]));
              reach.reach(informational ? "informational scan" : "indexed follow");
              reach.reach(published ? "published" : "resident");
              reach.reach("swapped identities");
              reach.reach("generated child");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            [false, false, 4],
            [true, true, 6],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await shipped.close();
    }
  });

for (const seed of seedsFor("next-native-ri-incremental-unique", [0xb63d, 0xb63e]))
  it(`checks real newly written child uniqueness before RI resolves ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native RI incremental unique", NATIVE_RI_INCREMENTAL_UNIQUE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          async (actions, published, sameOldKey) => {
            reach.beginHistory();
            const setup = [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT DEFAULT ${sameOldKey ? "1" : "99"} REFERENCES p(id) ON UPDATE SET DEFAULT,v INT,UNIQUE(pid,v))`,
                "INSERT INTO p VALUES(1,1),(2,2),(3,3),(99,99)",
                `INSERT INTO c VALUES(1,1,1),(2,2,2),(3,3,3),(4,99,${String(actions)})`,
              ],
              f = await referentialFixture(setup, published, actions);
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(
                `CREATE OR REPLACE FUNCTION fail_after() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.id=${String(actions)} THEN PERFORM 1/0; END IF; RETURN NEW; END $$; CREATE TRIGGER "Z_after" AFTER UPDATE ON p FOR EACH ROW EXECUTE FUNCTION fail_after()`,
              );
              const expected = await code(() =>
                pg.exec(`UPDATE p SET id=id+10 WHERE id<=${String(actions)}`),
              );
              const observed = await code(async () => {
                const input = await f.capture(
                  "p",
                  Array.from({ length: actions }, (_, i) => i),
                );
                for (const [i, row] of input.refs.entries())
                  await f.update(
                    input.tableId,
                    row.id,
                    batch(ints([i + 11]), ints([i + 1])),
                    row.image,
                  );
                await f.execution.verifyCommandUnique(f.controller.root, f.stop.signal);
                await f.end();
                throw Object.assign(new Error("later user AFTER division"), { sqlState: "22012" });
              });
              expect(expected).toBe(sameOldKey ? "23503" : "23505");
              expect(observed).toBe(expected);
              reach.reach(published ? "published" : "resident");
              reach.reach(`actions ${String(actions)}` as "actions 1" | "actions 2" | "actions 3");
              reach.reach(
                sameOldKey ? "old reference before later error" : "child unique before later error",
              );
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [1, false],
            [2, true],
            [3, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  });

for (const seed of seedsFor("next-native-ri-generated-key-guard", [0xb64b, 0xb64c]))
  it(`refuses generated FK assignments instead of silently recomputing them ${String(seed)}`, async () => {
    const reach = createReach("native generated FK guard", NATIVE_RI_GENERATED_KEY_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.boolean(), async (published, remove) => {
        reach.beginHistory();
        const f = await referentialFixture(
          [
            "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
            "CREATE TABLE c(id INT PRIMARY KEY,v INT,g INT GENERATED ALWAYS AS(v) STORED REFERENCES p(id) ON DELETE CASCADE)",
            "INSERT INTO p VALUES(1,1),(2,2)",
            "INSERT INTO c(id,v) VALUES(1,1)",
          ],
          published,
        );
        try {
          const input = await f.capture("p", remove ? [0] : [0, 1]);
          for (const [i, row] of input.refs.entries()) {
            if (remove)
              await f.rows.write(
                f.controller.root,
                input.tableId,
                "delete",
                row.image,
                row.id,
                f.controller,
                f.stop.signal,
              );
            else
              await f.update(input.tableId, row.id, batch(ints([2 - i]), ints([i + 1])), row.image);
          }
          expect(await code(() => f.end())).toBe(remove ? null : "0A000");
          if (!remove) await f.rollback();
          await f.retire();
          const result = await f.core.execute("SELECT id,g FROM c");
          if (!("batches" in result)) throw new Error("Missing generated child");
          expect(
            result.batches.flatMap((batch) =>
              Array.from({ length: batch.length }, (_, i) =>
                batch.columns.map((v) => valueAt(v, i)),
              ),
            ),
          ).toEqual(remove ? [] : [[1, 1]]);
          reach.reach(published ? "published" : "resident");
          reach.reach(remove ? "delete cascade allowed" : "assignment refused");
        } finally {
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [false, false],
          [true, true],
        ],
      },
    );
    reach.assertReached();
  });
