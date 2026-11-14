/** Actual SQL scan fallback under all-credit competition, not a synthetic scorer invocation. */
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, FTS_SQL_PRESSURE_REACH } from "../../testing/reach.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { Governor, type Grant } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

for (const seed of seedsFor("next-fts-sql-pressure", [0xb7fb, 0xb7fc]))
  it(`executes and spills with no ordinary credit left (${String(seed)})`, async () => {
    const core = await new StorageCore(32).open(),
      db = new MinnowDatabase(new MemoryBlockStore(), { ftsAutoIndexRows: 1_000_000 });
    const governor = new Governor(128 << 20),
      native = governor.admitGroup.bind(governor),
      random = mulberry32(seed);
    const reach = createReach(`FTS SQL pressure ${String(seed)}`, FTS_SQL_PRESSURE_REACH);
    reach.beginHistory();
    let blocker: Grant | undefined,
      occupied = 0;
    const controlled = new Proxy(governor, {
      get(target, key) {
        if (key === "tryAdmit")
          return (options: Parameters<Governor["tryAdmit"]>[0]) =>
            options.label.endsWith("/fixed-buffers") ? null : target.tryAdmit(options);
        if (key === "admitGroup")
          return async (options: Parameters<Governor["admitGroup"]>[0]) => {
            if (options.label.endsWith("/fixed-buffers")) {
              blocker?.close();
              blocker = undefined;
            }
            const grants = await native(options);
            if (options.label.endsWith("/fixed-buffers")) {
              const bytes = governor.capBytes - governor.usedBytes;
              [blocker] = await native({
                label: "competitor",
                includesProgress: true,
                grants: [{ label: "competitor", minimumBytes: bytes, spillable: true }],
              });
              if (!blocker?.allocate(bytes)) throw Error("competitor");
              occupied++;
            }
            return grants;
          };
        const value: unknown = Reflect.get(target, key, target);
        return typeof value === "function"
          ? (value as (...args: unknown[]) => unknown).bind(target)
          : value;
      },
    });
    try {
      const ddl = "CREATE TABLE docs(id INTEGER PRIMARY KEY,a TEXT,b TEXT)";
      await core.execute(ddl);
      await db.execute(ddl);
      const rows = Array.from({ length: 2400 }, (_, id) => ({
        id,
        a: id % 17 === 0 ? null : (random() < 0.5 ? "alpha " : "alpine ") + "x".repeat(4096),
        b: id % 9 === 0 ? "alpha beta" : null,
      }));
      await core.writeBatch("insertBatch", "docs", rows);
      await db.insertBatch("docs", rows);
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      const prepare = core.executor.prepare.bind(core.executor);
      core.executor.prepare = (plan) => {
        const prepared = prepare(plan);
        return {
          ...prepared,
          execute: (request) => prepared.execute({ ...request, governor: controlled }),
        };
      };
      const sql =
        "SELECT id,BM25(a,a,b) AGAINST 'al* alpha' s FROM docs WHERE id%4=0 AND MATCH(a,b) AGAINST 'al*' ORDER BY s DESC NULLS LAST,id LIMIT 25";
      const expected = await db.query(sql, { memoize: false }),
        actual = await core.execute(sql);
      if (!("batches" in actual)) throw Error("query");
      expect(oracleRows(actual)).toEqual(expected.rows);
      reach.reach("shipped exact");
      expect(occupied).toBeGreaterThan(0);
      reach.reach("all credit occupied");
      expect(
        Math.max(
          ...(core.lastProfile?.operators
            .filter((operator) => operator.op === "fts")
            .map((operator) => operator.spilledBytes) ?? [0]),
        ),
      ).toBeGreaterThan(0);
      reach.reach("native SQL spill");
    } finally {
      blocker?.close();
      await core.close();
      await db.close();
    }
    expect(governor.usedBytes).toBe(0);
    expect(core.spill.bytesInUse).toBe(0);
    reach.reach("drained");
    reach.assertReached();
  }, 60000);
