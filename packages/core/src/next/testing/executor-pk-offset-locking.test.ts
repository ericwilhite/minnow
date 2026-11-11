/** Locking intent excludes the cursor shortcut; this does not assert implemented row locks. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi, type MockInstance } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { ViewIndex } from "../write/view.js";
import { Governor } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

it("keeps the review FOR UPDATE probe on the generic route", async () => {
  const core = await new StorageCore().open();
  const spy = vi.spyOn(ViewIndex.prototype, "planRange");
  try {
    await core.execute("CREATE TABLE t(id INTEGER PRIMARY KEY,p INTEGER)");
    await core.writeBatch("insertBatch", "t", [
      { id: 1, p: 1 },
      { id: 2, p: 2 },
    ]);
    spy.mockClear();
    const result = await core.execute("SELECT p FROM t ORDER BY id LIMIT 1 OFFSET 1 FOR UPDATE");
    if (!("batches" in result)) throw new Error("Expected rows");
    expect(oracleRows(result)).toEqual([{ p: 2 }]);
    expect(spy).not.toHaveBeenCalled();
  } finally {
    spy.mockRestore();
    await core.close();
  }
}, 60000);

const locks = ["UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"] as const;
it.each(seedsFor("next-executor-pk-offset-locking", [0xe143, 0xe144]))(
  "preserves locking fallback beside an eligible unlocked control (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    const governor = new Governor(128 * 1024 * 1024);
    const reach = createReach("PK OFFSET locking fallback", [
      ...locks,
      "unlocked cursor",
      "locking fallback",
      "constant",
      "parameter",
      "memory",
      "stored",
      "ASC",
      "DESC",
      "limit zero",
      "past end",
      "positive offset",
      "PG exact",
      "released",
    ]);
    const original = core.executor.prepare.bind(core.executor);
    const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
      const prepared = original(plan);
      return { ...prepared, execute: (request) => prepared.execute({ ...request, governor }) };
    });
    let spy: MockInstance<ViewIndex["planRange"]> | undefined;
    try {
      await pg.waitReady;
      for (const name of ["s", "m"]) {
        const ddl = `CREATE TABLE ${name}(id INTEGER PRIMARY KEY,p INTEGER)`;
        await core.execute(ddl);
        await pg.exec(ddl);
        await pg.exec(`INSERT INTO ${name} VALUES(1,1),(2,2)`);
      }
      await core.writeBatch("insertBatch", "s", [
        { id: 1, p: 1 },
        { id: 2, p: 2 },
      ]);
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      await core.writeBatch("insertBatch", "m", [
        { id: 1, p: 1 },
        { id: 2, p: 2 },
      ]);
      spy = vi.spyOn(ViewIndex.prototype, "planRange");
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...locks),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.constantFrom(0, 1, 99),
          fc.constantFrom(0, 1, null),
          async (lock, descending, bound, stored, offset, limit) => {
            reach.beginHistory();
            const base =
              `SELECT p FROM ${stored ? "s" : "m"} ORDER BY id ${descending ? "DESC" : "ASC"} ` +
              (bound
                ? "LIMIT $1 OFFSET $2"
                : `${limit === null ? "" : `LIMIT ${String(limit)} `}OFFSET ${String(offset)}`);
            const params = bound ? [limit, offset] : [];
            for (const locking of [false, true]) {
              spy?.mockClear();
              const sql = base + (locking ? " FOR " + lock : "");
              const expected = (await pg.query(sql, params)).rows;
              const result = await core.execute(sql, params);
              if (!("batches" in result)) throw new Error("Expected rows");
              expect(oracleRows(result), sql).toEqual(expected);
              expect(spy?.mock.calls.length, sql).toBe(locking || limit === 0 ? 0 : 1);
              if (locking) {
                reach.reach("locking fallback");
                reach.reach(lock);
              } else if (limit !== 0) reach.reach("unlocked cursor");
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
            }
            reach.reach(descending ? "DESC" : "ASC");
            reach.reach(bound ? "parameter" : "constant");
            reach.reach(stored ? "stored" : "memory");
            if (limit === 0) reach.reach("limit zero");
            if (offset > 2) reach.reach("past end");
            if (offset > 0) reach.reach("positive offset");
            reach.reach("PG exact");
            reach.reach("released");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            ["UPDATE", false, false, false, 1, 1],
            ["NO KEY UPDATE", true, true, true, 1, 1],
            ["SHARE", false, true, false, 99, null],
            ["KEY SHARE", true, false, true, 0, null],
            ["UPDATE", false, true, true, 1, 0],
          ],
        },
      );
      reach.assertReached();
    } finally {
      spy?.mockRestore();
      prepare.mockRestore();
      await core.close();
      await pg.close();
    }
  },
  120000,
);
