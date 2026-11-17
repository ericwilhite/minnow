/** Native statement rollback and real enclosing savepoints, including RI/nested effects. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_SAVEPOINT_REACH } from "../../testing/reach.js";
import { ints } from "../write/test-support.js";
import { nativeTriggerSession } from "./native-trigger-session.test-support.js";
import { orderingScript } from "./trigger-ordering.test-support.js";

const queries = [
  "SELECT * FROM p ORDER BY id",
  "SELECT * FROM c ORDER BY id",
  "SELECT label,n FROM log ORDER BY label,n",
  "SELECT label,n FROM audit ORDER BY label,n",
  "SELECT * FROM trace",
];
async function state(work: () => Promise<unknown>): Promise<string | null> {
  try {
    await work();
    return null;
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "sqlState" in error &&
      typeof error.sqlState === "string"
    )
      return error.sqlState;
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      typeof error.code === "string"
    )
      return error.code;
    throw error;
  }
}
for (const seed of seedsFor("next-native-trigger-savepoints", [0xb7e4, 0xb7e5])) {
  const reach = createReach("native trigger savepoints", NATIVE_TRIGGER_SAVEPOINT_REACH);
  const ledgers = [
    new Map<number, { signature: string; completed: number }>(),
    new Map<number, { signature: string; completed: number }>(),
  ];
  const attempts = [
    [0, 0, 0, 0],
    [0, 0, 0, 0],
  ];
  // Same eight histories and two examples per outcome, replayed in bounded groups.
  // Failure location and ordinal parity assign each history exactly once. The ledger
  // audits all inputs, including every explicit example; no case or count is dropped.
  for (const succeeds of [false, true])
    for (const childGroup of [false, true])
      for (const parity of [0, 1])
        it(`restores native triggering statements/savepoints seed ${String(seed)}, success ${String(succeeds)}, child ${String(childGroup)}, parity ${String(parity)}`, async () => {
          const pg = new PGlite();
          let ordinal = 0;
          const group = succeeds ? 1 : 0,
            ledger = ledgers[group];
          if (!ledger) throw new Error("Missing savepoint history ledger");
          try {
            await fc.assert(
              fc.asyncProperty(
                fc.integer({ min: 2, max: 4 }),
                fc.boolean(),
                fc.boolean(),
                fc.boolean(),
                async (rows, published, child, reverseCreation) => {
                  const at = ordinal++,
                    signature = JSON.stringify([rows, published, child, reverseCreation]);
                  let record = ledger.get(at);
                  if (record)
                    expect(record.signature, "same savepoint history in each group").toBe(
                      signature,
                    );
                  else {
                    record = { signature, completed: 0 };
                    ledger.set(at, record);
                  }
                  if (child !== childGroup || at % 2 !== parity) return;
                  reach.beginHistory();
                  const script = orderingScript(
                    {
                      rows,
                      event: "update",
                      beforeNames: ["Z_b", "a_b"],
                      afterNames: ["A_a", "Z_a", "a_a"],
                      twoBodies: true,
                      reverseCreation,
                      published,
                    },
                    "accumulator",
                  );
                  const owner = child ? "c" : "p",
                    column = child ? "pid" : "id";
                  const body = `UPDATE trace SET v=v || CAST(1/(NEW.${column}-12) AS TEXT) WHERE id=1`;
                  const setup = [
                    ...script.nativeSetup,
                    `CREATE TRIGGER zz_fail AFTER UPDATE ON ${owner} BEGIN ${body}; END`,
                    ...script.seed,
                  ];
                  const sql = "UPDATE p SET id=id+$1";
                  const native = await nativeTriggerSession(setup, sql, published);
                  try {
                    await pg.exec("DROP TABLE IF EXISTS c,p,log,audit,trace CASCADE");
                    for (const text of script.postgresSetup) await pg.exec(text);
                    await pg.exec(
                      `CREATE OR REPLACE FUNCTION savepoint_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN NEW; END $$; CREATE TRIGGER zz_fail AFTER UPDATE ON ${owner} FOR EACH ROW EXECUTE FUNCTION savepoint_failure()`,
                    );
                    for (const text of script.seed) await pg.exec(text);
                    await pg.exec("BEGIN");
                    expect(await native.execute({ slots: [ints([20])] })).toBe(rows);
                    await pg.query(sql, [20]);
                    const before = new Map<string, ReadonlyArray<Record<string, unknown>>>();
                    for (const query of queries) {
                      const expected = (await pg.query<Record<string, unknown>>(query)).rows;
                      expect(await native.query(query), `pre-savepoint ${query}`).toEqual(expected);
                      before.set(query, expected);
                    }
                    reach.reach("pre-savepoint trigger and RI effects");
                    const save = native.savepoint();
                    await pg.exec("SAVEPOINT s");
                    const delta = succeeds ? 10 : -10;
                    const expected = await state(() => pg.query(sql, [delta]));
                    const actual = await state(() => native.execute({ slots: [ints([delta])] }));
                    expect(actual).toBe(expected);
                    if (succeeds) {
                      expect(actual).toBeNull();
                      for (const query of queries)
                        expect(await native.query(query), `successful body ${query}`).toEqual(
                          (await pg.query(query)).rows,
                        );
                      reach.reach("successful body effects rolled back");
                    } else {
                      expect(actual).toBe("22012");
                      // The native access layer permits observation after statement.rollback(). This
                      // checks undo BEFORE rollbackTo, without claiming SQL sessions can query while failed.
                      for (const query of queries)
                        await expect(
                          native.query(query),
                          `statement rollback ${query}`,
                        ).resolves.toEqual(before.get(query));
                      reach.reach("whole statement rollback includes RI and nested bodies");
                      reach.reach(child ? "late child AFTER failure" : "late parent AFTER failure");
                    }
                    native.rollbackTo(save);
                    await pg.exec("ROLLBACK TO s");
                    for (const query of queries) {
                      expect(await native.query(query), `rollback-to exact ${query}`).toEqual(
                        before.get(query),
                      );
                      expect((await pg.query(query)).rows).toEqual(before.get(query));
                    }
                    reach.reach("exact pre-savepoint restoration");
                    expect(await native.execute({ slots: [ints([100])] })).toBe(rows);
                    await pg.query(sql, [100]);
                    for (const query of queries)
                      expect(await native.query(query), `continued transaction ${query}`).toEqual(
                        (await pg.query(query)).rows,
                      );
                    reach.reach("continued triggering transaction");
                    native.release(save);
                    await pg.exec("RELEASE SAVEPOINT s");
                    await native.commit();
                    await pg.exec("COMMIT");
                    for (const query of queries)
                      expect(
                        await native.committedQuery(query),
                        `committed state ${query}`,
                      ).toEqual((await pg.query(query)).rows);
                    reach.reach(published ? "published source" : "resident source");
                    reach.reach(rows === 2 ? "two parent rows" : "more parent rows");
                    reach.reach("exact committed rows");
                  } finally {
                    await pg.exec("ROLLBACK");
                    await native.close();
                  }
                  reach.reach("zero native ownership");
                  record.completed++;
                },
              ),
              {
                seed,
                numRuns: 8,
                examples: [
                  [2, false, false, false],
                  [4, true, true, true],
                ],
              },
            );
            const counts = attempts[group];
            if (!counts) throw new Error("Missing savepoint attempt counts");
            counts[(childGroup ? 2 : 0) + parity] = ordinal;
          } finally {
            await pg.close();
          }
        }, 120000);
  it(`executes every savepoint history once and meets Reach seed ${String(seed)}`, () => {
    for (let group = 0; group < 2; group++) {
      const counts = attempts[group],
        ledger = ledgers[group];
      if (!counts || !ledger) throw new Error("Missing savepoint history audit");
      expect(counts[0]).toBeGreaterThan(0);
      for (const count of counts) expect(count).toBe(counts[0]);
      expect(ledger.size).toBe(counts[0]);
      for (const record of ledger.values())
        expect(record.completed, "every savepoint history exactly once").toBe(1);
    }
    reach.assertReached();
  });
}
