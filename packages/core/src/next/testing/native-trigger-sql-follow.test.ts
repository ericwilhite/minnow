/** Same documented follow identities, now through the funded ordinary SQL owner. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_SQL_FOLLOW_REACH } from "../../testing/reach.js";
import * as gate from "../owner/native-trigger-gate.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
import { followScript, followQueries, type Mode } from "./native-trigger-follow.test-support.js";
afterEach(() => vi.restoreAllMocks());
const capBytes = (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20;
for (const seed of seedsFor("next-native-trigger-sql-follow", [46978, 46979]))
  it(`qualifies funded SQL follow and nested bodies (${String(seed)})`, async () => {
    vi.spyOn(gate, "nativeTriggerFiringEnabled").mockReturnValue(true);
    const pg = new PGlite(),
      reach = createReach("funded SQL follow", NATIVE_TRIGGER_SQL_FOLLOW_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 4 }),
          fc.constantFrom<Mode>("shift", "swap", "rotate"),
          fc.boolean(),
          fc.boolean(),
          fc.constantFrom(null, "", "é漢"),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (size, mode, self, info, payload, nested, nullRef, published) => {
            reach.beginHistory();
            const h = followScript(size, mode, self, info, payload, nested, nullRef);
            const owner = await publicOwner({ capBytes }),
              db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
            try {
              for (const sql of [
                ...h.nativeSchema,
                ...h.nativeSeed,
                ...h.specs.map(
                  (s) =>
                    `CREATE TRIGGER "${s.name}" ${s.timing} ${"event" in s ? s.event : "UPDATE"} ON ${s.table} BEGIN ${s.commands.join(";")}; END`,
                ),
              ])
                await db.execute(sql);
              if (published) {
                await owner.core.harness.path.recycleLog();
                await owner.core.harness.path.quiesce();
              }
              await pg.exec("ROLLBACK;DROP TABLE IF EXISTS c,p,p_log,c_log,audit,tally CASCADE");
              for (const sql of [...h.pgSchema, ...h.pgSeed]) await pg.exec(sql);
              for (const [i, s] of h.specs.entries())
                await pg.exec(
                  `CREATE OR REPLACE FUNCTION sql_follow_f${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${s.commands.join(";")}; RETURN NEW; END $$; CREATE TRIGGER "${s.name}" ${s.timing} ${"event" in s ? s.event : "UPDATE"} ON ${s.table} FOR EACH ROW EXECUTE FUNCTION sql_follow_f${String(i)}()`,
                );
              await pg.exec("BEGIN");
              await pg.exec(h.sql);
              if (h.reference) await pg.exec(h.reference);
              await pg.exec("SET CONSTRAINTS ALL IMMEDIATE;COMMIT");
              expect((await db.execute(h.sql)).affectedRows).toBe(size);
              reach.reach("root counts");
              for (const sql of [
                ...followQueries,
                ...(!self ? ["SELECT id,pid,payload FROM c ORDER BY id"] : []),
              ])
                expect((await db.query(sql)).rows(), sql).toEqual((await pg.query(sql)).rows);
              reach.reach(mode);
              reach.reach(self ? "self" : "child");
              reach.reach(info ? "informational" : "enforced");
              reach.reach(published ? "published" : "resident");
              reach.reach(h.direct ? "raw PG cascade" : "captured-identity PG");
              if (nested) reach.reach("nested bodies");
              reach.reach("exact rows");
            } finally {
              await pg.exec("ROLLBACK");
              await db.close();
              await owner.close();
              expect(owner.core.harness.governor.usedBytes).toBe(0);
              expect(owner.core.spill.bytesInUse).toBe(0);
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [2, "swap", false, false, "é漢", true, false, false],
            [4, "rotate", true, false, null, true, true, true],
            [3, "shift", false, true, "", false, true, false],
            [3, "shift", false, false, "", false, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
