/** A small query sharing the native writer's governor must choose a fitting source
 * recipe before admission, rather than queue behind that persistent native floor. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_QUERY_SOURCE_PRESSURE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StatementImpl } from "../write/transaction.js";
import { rules } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-native-query-source-pressure", [0xb737, 0xb738]))
  it(`chooses a fitting query source beside the native writer seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native shared query source", NATIVE_QUERY_SOURCE_PRESSURE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 6 }),
          fc.integer({ min: 0, max: 2048 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (count, width, published, indexed, tail) => {
            reach.beginHistory();
            const core = new StorageCore(64),
              governor = new Governor(32 << 20);
            Object.defineProperty(core.harness, "governor", { value: governor });
            await core.open();
            let tx: Awaited<ReturnType<typeof core.harness.path.begin>> | null = null;
            try {
              const ddl = `CREATE TABLE t(id INTEGER${indexed ? " PRIMARY KEY" : ""},payload TEXT)`;
              await pg.exec("DROP TABLE IF EXISTS t;" + ddl);
              await core.execute(ddl);
              for (let row = 0; row < count; row++) {
                const params = [row, "雪".repeat(width) + ":" + String(row)];
                await pg.query("INSERT INTO t VALUES($1,$2)", params);
                await core.execute("INSERT INTO t VALUES($1,$2)", params);
              }
              if (published) {
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
              }
              if (tail) {
                const params = [count, "new tail"];
                await core.execute("INSERT INTO t VALUES($1,$2)", params);
                await pg.query("INSERT INTO t VALUES($1,$2)", params);
                if (published) reach.reach("mixed source");
              }
              tx = await core.harness.path.begin();
              const statement = tx.statement({ rules });
              if (!(statement instanceof StatementImpl))
                throw new Error("missing native statement");
              const boundary = await statement.prepareNativeStep(0, new AbortController().signal, {
                  includesProgress: true,
                }),
                floor = boundary.protectedWriteBytes;
              boundary.release();
              const admit = governor.admitGroup.bind(governor);
              const immediate = governor.tryAdmit.bind(governor);
              let complete = 0;
              const check = (label: string, minimum: number) => {
                if (!label.endsWith("/fixed-buffers")) return;
                complete++;
                expect(
                  minimum + floor,
                  "query minimum must fit beside protected native storage",
                ).toBeLessThanOrEqual(governor.capBytes);
              };
              const fast = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
                check(options.label, options.minimumBytes);
                return immediate(options);
              });
              const spy = vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
                // Reject before waiting: a bad recipe is a bounded counterexample,
                // not a test timeout behind the deliberately persistent writer.
                check(
                  options.label,
                  options.grants.reduce((n, g) => n + g.minimumBytes, 0),
                );
                return admit(options);
              });
              try {
                for (const sql of [
                  "SELECT payload AS s FROM t",
                  "SELECT id,payload FROM t ORDER BY id",
                ]) {
                  const actual = await core.execute(sql);
                  if (!("batches" in actual)) throw new Error("missing query rows");
                  expect(oracleRows(actual)).toEqual((await pg.query(sql)).rows);
                }
                expect(complete).toBeGreaterThan(0);
                reach.reach(published ? "published" : "resident");
                reach.reach(count === 1 ? "one row" : "several rows");
                reach.reach(indexed ? "indexed order" : "plain order");
                if (indexed && !published) reach.reach("resident indexed order");
                reach.reach("compound minimum before admission");
                reach.reach("exact PostgreSQL rows");
              } finally {
                spy.mockRestore();
                fast.mockRestore();
                statement.rollback();
              }
            } finally {
              tx?.rollback();
              await core.close();
            }
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            [1, 0, false, false, false],
            [1, 0, false, true, false],
            [6, 2048, true, true, false],
            [1, 0, true, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
