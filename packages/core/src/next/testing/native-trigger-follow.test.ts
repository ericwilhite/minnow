/** Original-target follow is a Minnow extension. PG reference schemas/queries below
 * model that documented identity mapping, not PostgreSQL's omitted ON UPDATE default. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_FOLLOW_REACH } from "../../testing/reach.js";
import { nativeTriggerSession } from "./native-trigger-session.test-support.js";
import {
  followScript,
  followQueries as queries,
  type Mode,
} from "./native-trigger-follow.test-support.js";
for (const seed of seedsFor("next-native-trigger-follow", [0xb7ea, 0xb7eb])) {
  const reach = createReach("native trigger follow", NATIVE_TRIGGER_FOLLOW_REACH),
    ledger = new Map<number, { signature: string; completed: number }>(),
    attempts = [0, 0];
  // Replay the same inputs in each group, executing each history exactly once. This
  // bounds coverage cost without dropping any seed, run, example or generated case.
  for (const selfGroup of [false, true])
    it(`matches captured-identity PG references for native follow seed ${String(seed)}, self ${String(selfGroup)}`, async () => {
      const pg = new PGlite();
      let ordinal = 0;
      try {
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 2, max: 5 }),
            fc.constantFrom<Mode>("shift", "swap", "rotate"),
            fc.boolean(),
            fc.boolean(),
            fc.constantFrom<string | null>(null, "", "é漢"),
            fc.boolean(),
            fc.boolean(),
            fc.boolean(),
            async (size, mode, self, info, payload, nested, nullRef, published) => {
              const at = ordinal++,
                signature = JSON.stringify([
                  size,
                  mode,
                  self,
                  info,
                  payload,
                  nested,
                  nullRef,
                  published,
                ]);
              let record = ledger.get(at);
              if (record)
                expect(record.signature, "same follow history in each group").toBe(signature);
              else {
                record = { signature, completed: 0 };
                ledger.set(at, record);
              }
              if (self !== selfGroup) return;
              reach.beginHistory();
              const h = followScript(size, mode, self, info, payload, nested, nullRef);
              const native = await nativeTriggerSession(
                [
                  ...h.nativeSchema,
                  ...h.nativeSeed,
                  ...h.specs.map(
                    (s) =>
                      `CREATE TRIGGER "${s.name}" ${s.timing} ${"event" in s ? s.event : "UPDATE"} ON ${s.table} BEGIN ${s.commands.join(";")}; END`,
                  ),
                ],
                h.sql,
                published,
              );
              try {
                await pg.exec("ROLLBACK;DROP TABLE IF EXISTS c,p,p_log,c_log,audit,tally CASCADE");
                for (const sql of [...h.pgSchema, ...h.pgSeed]) await pg.exec(sql);
                for (const [i, s] of h.specs.entries())
                  await pg.exec(
                    `CREATE OR REPLACE FUNCTION follow_f${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${s.commands.join(";")}; RETURN NEW; END $$; CREATE TRIGGER "${s.name}" ${s.timing} ${"event" in s ? s.event : "UPDATE"} ON ${s.table} FOR EACH ROW EXECUTE FUNCTION follow_f${String(i)}()`,
                  );
                await pg.exec("BEGIN");
                await pg.exec(h.sql);
                if (h.reference) await pg.exec(h.reference);
                await pg.exec("SET CONSTRAINTS ALL IMMEDIATE;COMMIT");
                await expect(native.execute({ slots: [] })).resolves.toBe(size);
                await native.commit();
                for (const sql of [
                  ...queries,
                  ...(!self ? ["SELECT id,pid,payload FROM c ORDER BY id"] : []),
                ])
                  expect(
                    await native.committedQuery(sql),
                    `follow PG data ${mode}/${self ? "self" : "child"}: ${sql}`,
                  ).toEqual((await pg.query(sql)).rows);
                reach.reach(mode);
                reach.reach(self ? "self reference" : "external child");
                reach.reach(info ? "informational orphan unchanged" : "enforced reference");
                reach.reach(published ? "published" : "resident");
                reach.reach(
                  h.direct ? "raw PG cascade counterpart" : "PG captured-identity reference",
                );
                reach.reach(nested ? "nested tally body" : "no further body");
                reach.reach(
                  payload === null
                    ? "NULL payload"
                    : payload === ""
                      ? "empty payload"
                      : "UTF8 payload",
                );
                if (nullRef) reach.reach("NULL reference unchanged");
                if (mode === "swap" && size > 2) reach.reach("unchanged roots still fire");
                reach.reach("exact original-target rows");
              } finally {
                await native.close();
              }
              reach.reach("zero native ownership");
              record.completed++;
            },
          ),
          {
            seed,
            numRuns: 12,
            examples: [
              [2, "swap", false, false, "é漢", true, false, false],
              [4, "rotate", true, false, null, true, true, true],
              [3, "shift", false, false, "", false, true, false],
              [4, "swap", true, true, "é漢", true, false, true],
              [3, "rotate", false, true, null, false, true, false],
              [3, "shift", true, false, "", true, false, true],
            ],
          },
        );
        attempts[selfGroup ? 1 : 0] = ordinal;
      } finally {
        await pg.close();
      }
    }, 120000);

  it(`executes every follow history once and meets Reach seed ${String(seed)}`, () => {
    expect(attempts[0]).toBeGreaterThan(0);
    expect(attempts[1]).toBe(attempts[0]);
    expect(ledger.size).toBe(attempts[0]);
    for (const record of ledger.values())
      expect(record.completed, "every follow history exactly once").toBe(1);
    reach.assertReached();
  });
}
