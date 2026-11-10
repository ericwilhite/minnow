/** Both producers use the worker's shared task clock, including resumed recursive levels. */
import { expect, it, vi } from "vitest";
import { taskClock } from "../task.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
it.each([
  "SELECT count(*) AS n FROM generate_series(1,20) AS g(x)",
  "WITH RECURSIVE r(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<20) SELECT count(*) AS n FROM r",
])("yields real tasks under the shared clock: %s", async (sql) => {
  const core = await new StorageCore().open();
  const epoch = taskClock.epoch;
  let ticks = 0;
  const due = vi.spyOn(taskClock, "due").mockImplementation(() => ++ticks % 4 < 2);
  try {
    let observed = false;
    const observer = new Promise<void>((resolve) => {
      setImmediate(() => {
        observed = true;
        resolve();
      });
    });
    const result = await core.execute(sql);
    if (!("batches" in result)) throw new Error("Expected count");
    expect(oracleRows(result)).toEqual([{ n: 20 }]);
    expect(observed).toBe(true);
    await observer;
    expect(taskClock.epoch - epoch).toBeGreaterThan(0);
  } finally {
    due.mockRestore();
    await core.close();
  }
});
