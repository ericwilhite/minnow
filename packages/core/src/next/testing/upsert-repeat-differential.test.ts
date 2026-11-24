/**
 * A key proposed twice in one INSERT … ON CONFLICT statement, against PostgreSQL.
 *
 * Law (PostgreSQL, INSERT "ON CONFLICT Clause"): "INSERT with an ON CONFLICT DO UPDATE clause is a
 * deterministic statement. This means that the command will not be allowed to affect any single
 * existing row more than once; a cardinality violation error will be raised when this situation
 * arises" (SQLSTATE 21000, "ON CONFLICT DO UPDATE command cannot affect row a second time"). That
 * covers a row the statement inserted from an earlier proposal and a row it updated. With DO
 * NOTHING, the later proposal for a key an earlier one inserted is skipped: the first is kept.
 *
 * Generated: rows already in the table (none in half the cases: an empty table's snapshot has no
 * segment, so a row the statement inserts has no position in it, which is where the untyped
 * "Row … is not in this view" came from; else 1 to 20, beside 16-row memtables), 2 to 24 proposals
 * with one key proposed twice (an existing key or a new one, anywhere in the list), proposals from
 * a VALUES list or from INSERT … SELECT … ORDER BY, and DO UPDATE, DO UPDATE with a false WHERE, or
 * DO NOTHING. Each statement must end as PostgreSQL's does: the same SQLSTATE, or the same rows.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, UPSERT_REPEAT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MEMTABLE_ROWS = 16;
type Source = "values" | "select";
type Action = "update" | "update-where" | "nothing";

const ACTIONS: Record<Action, string> = {
  update: "DO UPDATE SET v = EXCLUDED.v",
  "update-where": "DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > 100000",
  nothing: "DO NOTHING",
};

async function outcome(
  run: () => Promise<unknown>,
  code: (error: unknown) => unknown,
): Promise<{ error: unknown } | null> {
  try {
    await run();
    return null;
  } catch (error) {
    return { error: code(error) };
  }
}

it.each(seedsFor("next-upsert-repeat", [0x21000, 0x21001]))(
  "affects a row at most once per ON CONFLICT statement, as PostgreSQL does (seed %i)",
  async (seed) => {
    const reach = createReach("upsert repeat", UPSERT_REPEAT_REACH);
    const pg = new PGlite();
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.oneof(fc.constant(0), fc.integer({ min: 1, max: 20 })),
          fc.integer({ min: 2, max: 24 }),
          fc.nat(),
          fc.nat(),
          fc.boolean(),
          fc.constantFrom<Source>("values", "select"),
          fc.constantFrom<Action>("update", "update-where", "nothing"),
          async (prefill, count, first, gap, existing, source, action) => {
            reach.beginHistory();
            const core = await new StorageCore(MEMTABLE_ROWS).open();
            try {
              const both = async (sql: string) => {
                await core.execute(sql);
                await pg.exec(sql);
              };
              await pg.exec("DROP TABLE IF EXISTS t; DROP TABLE IF EXISTS src");
              await both("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
              await both("CREATE TABLE src (ord INTEGER PRIMARY KEY, id INTEGER, v INTEGER)");
              if (prefill > 0)
                await both(
                  `INSERT INTO t VALUES ${Array.from({ length: prefill }, (_, k) => `(${String(k + 1)}, ${String(k + 1)})`).join(", ")}`,
                );
              reach.reach(prefill === 0 ? "empty table" : "prefilled table");
              // Distinct new keys, then one key proposed twice at positions i < j.
              const i = first % (count - 1),
                j = i + 1 + (gap % (count - 1 - i));
              const key = existing && prefill > 0 ? 1 + (first % prefill) : 1000;
              reach.reach(existing && prefill > 0 ? "repeated existing key" : "repeated new key");
              const ids = Array.from({ length: count }, (_, k) =>
                k === i || k === j ? key : 2000 + k,
              );
              const rows = ids.map((id, k) => `(${String(id)}, ${String(k + 1)})`);
              let sql: string;
              if (source === "values") {
                sql = `INSERT INTO t VALUES ${rows.join(", ")} ON CONFLICT (id) ${ACTIONS[action]}`;
                reach.reach("repeat in VALUES");
              } else {
                await both(
                  `INSERT INTO src VALUES ${ids.map((id, k) => `(${String(k)}, ${String(id)}, ${String(k + 1)})`).join(", ")}`,
                );
                sql = `INSERT INTO t SELECT id, v FROM src ORDER BY ord ON CONFLICT (id) ${ACTIONS[action]}`;
                reach.reach("repeat in INSERT SELECT");
              }
              reach.reach(
                action === "update"
                  ? "DO UPDATE"
                  : action === "update-where"
                    ? "DO UPDATE WHERE false"
                    : "DO NOTHING",
              );
              // The SQLSTATE and PostgreSQL's message text, both.
              const actual = await outcome(
                () => core.execute(sql),
                (error) => {
                  const { sqlState, message } = error as { sqlState?: string; message?: string };
                  return sqlState === undefined ? String(error) : `${sqlState} ${String(message)}`;
                },
              );
              const expected = await outcome(
                () => pg.query(sql),
                (error) => {
                  const { code, message } = error as { code?: string; message?: string };
                  return `${String(code)} ${String(message)}`;
                },
              );
              expect(actual, sql).toEqual(expected);
              reach.reach(expected === null ? "accepted" : "21000");
              const select = "SELECT id, v FROM t ORDER BY id";
              const table = await core.execute(select);
              if (!("batches" in table)) throw new Error("rows");
              expect(oracleRows(table), sql).toEqual((await pg.query(select)).rows);
            } finally {
              await core.close();
            }
            expect(core.harness.governor.usedBytes).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 48,
          examples: [
            // The reported defect: an empty table, a new key twice in VALUES.
            [0, 2, 0, 0, false, "values", "update"],
            // A full 16-row memtable before the statement.
            [16, 3, 0, 0, false, "values", "update"],
            [15, 6, 3, 1, false, "select", "update-where"],
            [4, 2, 0, 0, true, "values", "update"],
            [0, 2, 0, 0, false, "select", "nothing"],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  },
  120_000,
);
