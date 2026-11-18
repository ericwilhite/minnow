/** Lead 1.0 decision: row rules precede statement-end UNIQUE/PK verification.
 * PG chooses the earlier duplicate; shipped 0.x does so for the primary row key. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import { createReach, MODIFY_UNIQUE_TIMING_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

async function state(run: () => Promise<unknown>): Promise<string | null> {
  try {
    await run();
    return null;
  } catch (error) {
    if (!(error instanceof Error)) throw error;
    if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
    if ("code" in error && typeof error.code === "string") return error.code;
    // Shipped JS errors predate SQLSTATE: normalize their independent error identity only.
    if (error.name === "UniqueConstraintError") return "23505";
    if (error.message.includes("cannot be null")) return "23502";
    if (/CHECK .*failed/.test(error.message)) return "23514";
    throw error;
  }
}

for (const seed of seedsFor("next-modify-unique-timing", [0xb629, 0xb62a]))
  it(`keeps row-rule errors ahead of statement-end uniqueness ${String(seed)}`, async () => {
    const reach = createReach("statement-end uniqueness", MODIFY_UNIQUE_TIMING_REACH),
      core = await new StorageCore(256).open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("PK", "UNIQUE"),
          fc.constantFrom("NOT NULL", "CHECK"),
          fc.constantFrom("VALUES", "SELECT"),
          fc.constantFrom(0, 1, 255, 256, 257),
          fc.integer({ min: 1, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          async (key, rule, form, prefix, gap, published, returning) => {
            reach.beginHistory();
            const all = async (sql: string) => {
              await core.execute(sql);
              await pg.exec(sql);
              await shipped.run({ kind: "sql", sql });
            };
            await all("DROP TABLE IF EXISTS unique_timing");
            await all(
              `CREATE TABLE unique_timing(id INT PRIMARY KEY,u INT UNIQUE,v INT NOT NULL${rule === "CHECK" ? ",CONSTRAINT positive CHECK(v>0)" : ""})`,
            );
            await all("INSERT INTO unique_timing VALUES(1,1,1)");
            if (published) {
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
            }
            const late = prefix + gap;
            const sqlFor = (mode: "both" | "unique-only" | "row-only" | "valid") => {
              const rows = Array.from({ length: late + 1 }, (_, i) => {
                const duplicate = i === prefix && (mode === "both" || mode === "unique-only"),
                  invalid = i === late && (mode === "both" || mode === "row-only");
                const id = duplicate && key === "PK" ? 1 : 100 + i,
                  u = duplicate && key === "UNIQUE" ? 1 : 1000 + i,
                  v = invalid ? (rule === "NOT NULL" ? "NULL" : "0") : "1";
                return form === "VALUES"
                  ? `(${String(id)},${String(u)},${v})`
                  : `(${String(i)},${String(id)},${String(u)},${v})`;
              }).join(",");
              return (
                "INSERT INTO unique_timing " +
                (form === "VALUES"
                  ? `VALUES ${rows}`
                  : `SELECT id,u,v FROM (VALUES ${rows})q(ord,id,u,v) ORDER BY ord`) +
                (returning ? " RETURNING id,u,v" : "")
              );
            };
            const rowState = rule === "NOT NULL" ? "23502" : "23514";
            for (const mode of ["both", "unique-only", "row-only"] as const) {
              const sql = sqlFor(mode),
                nativeState = mode === "unique-only" ? "23505" : rowState,
                referenceState = mode === "row-only" ? rowState : "23505",
                // Shipped's #assertInsertKeysAbsent contract checks the primary identity
                // early; outside a write scope secondary terms reach the atomic commit
                // check only after row validation. Keep both outcomes independent of PG.
                shippedState = mode === "both" && key === "UNIQUE" ? rowState : referenceState;
              expect(
                await state(() => core.execute(sql)),
                `native ${mode}: row rules precede statement-end uniqueness`,
              ).toBe(nativeState);
              expect(
                await state(() => pg.exec(sql)),
                `PG ${mode}: immediate duplicate priority`,
              ).toBe(referenceState);
              expect(
                await state(() => shipped.run({ kind: "sql", sql })),
                `shipped ${mode}: primary versus secondary timing`,
              ).toBe(shippedState);
              const select = "SELECT * FROM unique_timing ORDER BY id",
                nativeRows = await core.execute(select);
              if (!("batches" in nativeRows)) throw new Error("Missing native rows");
              expect(oracleRows(nativeRows), "the whole failed statement rolls back").toEqual([
                { id: 1, u: 1, v: 1 },
              ]);
              expect((await pg.query(select)).rows).toEqual([{ id: 1, u: 1, v: 1 }]);
              expect((await shipped.run({ kind: "sql", sql: select })).rows).toEqual([
                { id: 1, u: 1, v: 1 },
              ]);
              expect(core.spill.bytesInUse).toBe(0);
            }
            await all(sqlFor("valid"));
            const select = "SELECT * FROM unique_timing ORDER BY id",
              nativeRows = await core.execute(select);
            if (!("batches" in nativeRows)) throw new Error("Missing native success rows");
            expect(oracleRows(nativeRows)).toEqual((await pg.query(select)).rows);
            expect(oracleRows(nativeRows)).toEqual(
              (await shipped.run({ kind: "sql", sql: select })).rows,
            );
            reach.reach(key);
            reach.reach(rule);
            reach.reach(form);
            if (rule === "CHECK" || returning) {
              reach.reach("spooled input");
              reach.reach(
                Math.floor(prefix / 256) === Math.floor(late / 256) ? "same page" : "across pages",
              );
            } else reach.reach("direct input");
            reach.reach(published ? "published" : "resident");
            reach.reach(returning ? "RETURNING" : "no RETURNING");
            reach.reach(
              key === "PK" ? "shipped primary unique-first" : "shipped secondary row-first",
            );
            for (const reached of [
              "native row-level first",
              "PG unique first",
              "unique-only rejected",
              "row-only rejected",
              "atomic rollback",
              "success",
            ] as const)
              reach.reach(reached);
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ["PK", "NOT NULL", "VALUES", 0, 1, false, false],
            ["UNIQUE", "CHECK", "SELECT", 255, 1, true, true],
            ["PK", "CHECK", "SELECT", 257, 2, false, true],
            ["UNIQUE", "NOT NULL", "VALUES", 256, 2, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await shipped.close();
      await pg.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  }, 120000);
