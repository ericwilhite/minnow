/** Production owner dispatch without a test enablement override. PostgreSQL is authority
 * for row order/statement atomicity; the shipped surface remains FOR EACH ROW only. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_SQL_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
import { orderingScript, orderingTrace } from "./trigger-ordering.test-support.js";

const queries = [
  "SELECT * FROM p ORDER BY id",
  "SELECT * FROM c ORDER BY id",
  "SELECT label,n FROM log ORDER BY label,n",
  "SELECT label,n FROM audit ORDER BY label,n",
  "SELECT * FROM trace",
];
const capBytes = (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20;
async function sqlState(work: () => Promise<unknown>): Promise<string | null> {
  try {
    await work();
    return null;
  } catch (error) {
    if (typeof error === "object" && error !== null) {
      if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
      if ("code" in error && typeof error.code === "string") return error.code;
    }
    throw error;
  }
}

it("enables ordinary row-trigger SQL and refuses statement-level/transaction-control bodies", async () => {
  const owner = await publicOwner({ capBytes });
  const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  try {
    await db.execute("CREATE TABLE t(id INT PRIMARY KEY)");
    await db.execute("CREATE TABLE audit(n INT)");
    await expect(
      db.execute(
        "CREATE TRIGGER s AFTER INSERT ON t FOR EACH STATEMENT BEGIN INSERT INTO audit VALUES(NEW.id); END",
      ),
    ).rejects.toMatchObject({ message: "Expected FOR EACH ROW", sqlState: "42601" });
    for (const control of ["SAVEPOINT s", "ROLLBACK", "ROLLBACK TO s"])
      await expect(
        db.execute(`CREATE TRIGGER bad AFTER INSERT ON t BEGIN ${control}; END`),
      ).rejects.toMatchObject({
        sqlState: "42601",
        message: "Trigger bodies support INSERT, UPDATE, and DELETE statements",
      });
    await db.execute(
      "CREATE TRIGGER r AFTER INSERT ON t FOR EACH ROW BEGIN INSERT INTO audit VALUES(NEW.id); END",
    );
    await db.execute("INSERT INTO t VALUES(1)");
    expect((await db.query("SELECT * FROM t")).rows()).toEqual([{ id: 1 }]);
    expect((await db.query("SELECT * FROM audit")).rows()).toEqual([{ n: 1 }]);
  } finally {
    await db.close();
    await owner.close();
  }
  expect(owner.core.harness.governor.usedBytes).toBe(0);
});

for (const seed of seedsFor("next-native-trigger-sql", [46976, 46977])) {
  const reach = createReach("guarded native SQL", NATIVE_TRIGGER_SQL_REACH);
  const ledgers = [
    new Map<number, { signature: string; completed: number }>(),
    new Map<number, { signature: string; completed: number }>(),
  ];
  const attempts = [
    [0, 0],
    [0, 0],
  ];
  // Replay the same draws/examples in both child groups; the audit proves each
  // original history executes once. Bound coverage work without losing cases.
  for (const failure of [false, true])
    for (const childGroup of [false, true])
      it(`qualifies production row-trigger SQL and statement undo ${String(seed)}, failure ${String(failure)}, child ${String(childGroup)}`, async () => {
        const pg = new PGlite(),
          group = failure ? 1 : 0,
          ledger = ledgers[group];
        if (!ledger) throw new Error("Missing SQL history ledger");
        let ordinal = 0;
        try {
          await fc.assert(
            fc.asyncProperty(
              fc.integer({ min: 2, max: 4 }),
              fc.boolean(),
              fc.boolean(),
              async (rows, published, child) => {
                const at = ordinal++,
                  signature = JSON.stringify([rows, published, child]);
                let record = ledger.get(at);
                if (record)
                  expect(record.signature, "same SQL history in both groups").toBe(signature);
                else {
                  record = { signature, completed: 0 };
                  ledger.set(at, record);
                }
                if (child !== childGroup) return;
                reach.beginHistory();
                const owner = await publicOwner({ capBytes }),
                  db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() }),
                  script = orderingScript(
                    {
                      rows,
                      event: child ? "delete" : "update",
                      beforeNames: ["Z_b", "a_b"],
                      afterNames: ["A_a", "Z_a", "a_a"],
                      twoBodies: true,
                      reverseCreation: published,
                      published,
                    },
                    "accumulator",
                  );
                const event = child ? "DELETE" : "UPDATE",
                  alias = child ? "OLD" : "NEW";
                const target = child ? "c" : "p",
                  column = child ? "pid" : "id";
                // Row 2 fails only after earlier root rows and their queued RI/body effects.
                const denominator = child ? `${alias}.${column}-2` : `${alias}.${column}-102`;
                const body = `UPDATE trace SET v=v || CAST(1/(${denominator}) AS TEXT) WHERE id=1`;
                try {
                  await pg.exec("DROP TABLE IF EXISTS c,p,log,audit,trace CASCADE");
                  for (const text of [...script.postgresSetup, ...script.seed]) await pg.exec(text);
                  for (const text of [...script.nativeSetup, ...script.seed])
                    await db.execute(text);
                  if (failure) {
                    await db.execute(
                      `CREATE TRIGGER zz_fail AFTER ${event} ON ${target} BEGIN ${body}; END`,
                    );
                    await pg.exec(
                      `CREATE OR REPLACE FUNCTION sql_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN ${alias}; END $$; CREATE TRIGGER zz_fail AFTER ${event} ON ${target} FOR EACH ROW EXECUTE FUNCTION sql_failure()`,
                    );
                  }
                  if (published) {
                    await owner.core.harness.path.recycleLog();
                    await owner.core.harness.path.quiesce();
                  }
                  await db.execute("BEGIN");
                  await pg.exec("BEGIN");
                  await db.execute("SAVEPOINT s");
                  await pg.exec("SAVEPOINT s");
                  const before = new Map<string, unknown>();
                  for (const query of queries) before.set(query, (await pg.query(query)).rows);
                  const expected = await sqlState(() => pg.exec(script.mutation));
                  expect(await sqlState(() => db.execute(script.mutation))).toBe(expected);
                  if (failure) {
                    expect(expected).toBe("22012");
                    expect(await sqlState(() => db.query("SELECT * FROM p"))).toBe("25P02");
                    expect(await sqlState(() => pg.query("SELECT * FROM p"))).toBe("25P02");
                    reach.reach("failed SQL transaction");
                    reach.reach(child ? "late child failure" : "late parent failure");
                  } else {
                    expect(expected).toBeNull();
                    for (const query of queries)
                      expect((await db.query(query)).rows()).toEqual((await pg.query(query)).rows);
                    expect(
                      orderingTrace(
                        script,
                        (await db.query(script.traceSql)).rows() as Array<{ s: string }>,
                      ),
                    ).toEqual(
                      orderingTrace(script, (await pg.query<{ s: string }>(script.traceSql)).rows),
                    );
                    reach.reach("BEFORE and AFTER name order");
                    reach.reach(child ? "delete RI" : "update RI");
                    reach.reach("successful body undo");
                  }
                  await db.execute("ROLLBACK TO s");
                  await pg.exec("ROLLBACK TO s");
                  for (const query of queries) {
                    expect((await db.query(query)).rows()).toEqual(before.get(query));
                    expect((await pg.query(query)).rows).toEqual(before.get(query));
                  }
                  reach.reach("exact savepoint recovery");
                  if (failure) {
                    await db.execute("DROP TRIGGER zz_fail");
                    await pg.exec("DROP TRIGGER zz_fail ON " + target);
                  }
                  await db.execute(script.mutation);
                  await pg.exec(script.mutation);
                  await db.execute("RELEASE SAVEPOINT s");
                  await pg.exec("RELEASE SAVEPOINT s");
                  await db.execute("COMMIT");
                  await pg.exec("COMMIT");
                  reach.reach("transaction continues");
                  for (const query of queries)
                    expect((await db.query(query)).rows()).toEqual((await pg.query(query)).rows);
                  reach.reach("exact committed rows");
                  reach.reach(published ? "published source" : "resident source");
                } finally {
                  await pg.exec("ROLLBACK");
                  await db.close();
                  await owner.close();
                  expect(owner.core.harness.governor.usedBytes).toBe(0);
                  expect(owner.core.spill.bytesInUse).toBe(0);
                }
                reach.reach("zero ownership");
                record.completed++;
              },
            ),
            {
              seed,
              numRuns: 6,
              examples: [
                [2, false, false],
                [3, true, true],
              ],
            },
          );
          const counts = attempts[group];
          if (!counts) throw new Error("Missing SQL history count");
          counts[childGroup ? 1 : 0] = ordinal;
        } finally {
          await pg.close();
        }
      }, 120000);
  it(`meets guarded SQL reach ${String(seed)}`, () => {
    for (let group = 0; group < 2; group++) {
      const ledger = ledgers[group],
        counts = attempts[group];
      if (!ledger || !counts) throw new Error("Missing SQL history audit");
      expect(counts[0]).toBeGreaterThan(0);
      expect(counts[1]).toBe(counts[0]);
      expect(ledger.size).toBe(counts[0]);
      for (const record of ledger.values())
        expect(record.completed, "each SQL history exactly once").toBe(1);
    }
    reach.assertReached();
  });
}
