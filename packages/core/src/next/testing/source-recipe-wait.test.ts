/**
 * A query's retained source recipe (exec/run.ts retryResidentSources) must not be held while the
 * query waits for another admission (memory governor: no hold-and-wait under FIFO admission).
 */
import { expect, it, vi } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

it("never waits for memory while holding a retained source recipe (FIFO repro)", async () => {
  const core = await new StorageCore(8192).open();
  try {
    await core.execute("CREATE TABLE m (id INTEGER PRIMARY KEY, v INTEGER)");
    // A flushed group plus rows still in the memtable: retaining it takes an admitted recipe.
    await core.writeBatch(
      "insertBatch",
      "m",
      Array.from({ length: 3000 }, (_, i) => ({ id: i + 1, v: i })),
    );
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
    await core.writeBatch(
      "insertBatch",
      "m",
      Array.from({ length: 100 }, (_, i) => ({ id: 3001 + i, v: i })),
    );
    const governor = new Governor(64 << 20);
    const nativePrepare = core.executor.prepare.bind(core.executor);
    const prepare = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
      const prepared = nativePrepare(plan);
      return { ...prepared, execute: (request) => prepared.execute({ ...request, governor }) };
    });
    const queued: { competitor: Promise<Grant> | null; recipe: number } = {
      competitor: null,
      recipe: 0,
    };
    const tryAdmit = governor.tryAdmit.bind(governor);
    const spy = vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
      if (options.label.endsWith("/source-recipe")) queued.recipe = options.minimumBytes;
      // The first wait-capable admission after the recipe: queue an admission that only the
      // recipe's bytes block (all the room the query holds), then let the query ask.
      if (
        queued.competitor === null &&
        queued.recipe > 0 &&
        !options.label.endsWith("/source-recipe")
      )
        queued.competitor = governor.admit({
          label: "recipe-blocked competitor",
          minimumBytes:
            governor.capBytes - governor.reserveBytes - (governor.usedBytes - queued.recipe),
          spillable: false,
        });
      return tryAdmit(options);
    });
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const sql = "SELECT id, v FROM m ORDER BY id LIMIT 5 OFFSET 2500";
      const query = core.execute(sql, [], controller.signal);
      const competed = (async () => {
        while (queued.competitor === null) await new Promise((resolve) => setTimeout(resolve, 1));
        (await queued.competitor).close();
      })();
      const outcome = await Promise.race([
        query.then(() => "done" as const),
        new Promise<"deadlock">((resolve) => {
          timer = setTimeout(() => resolve("deadlock"), 10_000);
        }),
      ]);
      if (outcome === "deadlock") controller.abort(new Error("deadlocked"));
      expect(queued.recipe, "the query retained its resident source").toBeGreaterThan(0);
      expect(outcome, "no admission waits while the recipe is held").toBe("done");
      await competed;
      const result = await query;
      if (result.kind !== "rows" || !("batches" in result)) throw new Error("rows");
      expect(oracleRows(result).map((r) => Number(r.id))).toEqual([2501, 2502, 2503, 2504, 2505]);
      expect(governor.usedBytes).toBe(0);
    } finally {
      clearTimeout(timer);
      spy.mockRestore();
      prepare.mockRestore();
    }
  } finally {
    await core.close();
  }
}, 30_000);
