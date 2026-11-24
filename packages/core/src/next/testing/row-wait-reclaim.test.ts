/**
 * A write step waiting for rows keeps the rows it already holds: its own admission never takes
 * them back.
 *
 * `#waitForRows` (write/engine.ts) admitted the step's deficit from the governor. When that
 * admission needed memory, the governor asked the write path to reclaim, and `reclaimRows` gave
 * back the idle row credit the step was about to use (it kept only the row minimum and a prepared
 * step's floor). The credit then arrived, the step was short by what reclaim returned, and it
 * asked again: at a 40 MiB cap, after `UPDATE t SET v = $1` with 2.5 MB rows,
 * `UPDATE t SET v = $1 WHERE id = 1` with a 4 MiB value alternated the row grant between 14.71
 * and 10.80 MB forever, in microtasks: the statement never finished and no timer ran again.
 * The waiting step now floors the row account (`WriteCredits.holdRowsWhileWaiting`), as a
 * prepared step does, so reclaim, idle funding and merge exchange leave those rows.
 *
 * Law (GOVERNOR-WAIT-POLICY, next/memory/README.md): every request is served, or refused with
 * 53200 before its effect; none waits forever. PostgreSQL statement atomicity for a refusal,
 * read back through SQL (since #52, a read by key is sized to the row it fetches) and through
 * the write path's own view.
 *
 * The spin keeps timers from running, so a timeout cannot see it: the test counts the step's
 * admissions instead (a wait that makes progress needs a few, never hundreds).
 */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { Governor } from "../memory/governor.js";
import type { NativeGrantGroupOptions } from "../memory/governor.js";
import { createReach, ROW_WAIT_RECLAIM_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { rowsOf } from "../write/test-support.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
/** Far more admissions than any wait that makes progress needs. */
const SPIN = 200;
const TYPED = { name: "MemoryAdmissionError", code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" };

afterEach(() => {
  Governor.stallAfterMs = null;
  vi.restoreAllMocks();
});

async function history(
  capMiB: number,
  first: number,
  second: number,
  reach?: ReturnType<typeof createReach>,
): Promise<void> {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  const governor = new Governor(capMiB * MiB);
  Object.defineProperty(core.harness, "governor", { value: governor });
  await core.open();
  let admissions = 0;
  const admitGroup = governor.admitGroup.bind(governor);
  vi.spyOn(governor, "admitGroup").mockImplementation((options: NativeGrantGroupOptions) => {
    if (options.label === "write path memtable" && ++admissions > SPIN)
      return Promise.reject(new Error(`spin: ${String(admissions)} memtable admissions`));
    return admitGroup(options);
  });
  const context = `${String(capMiB)} MiB cap, ${String(first)}-byte rows, then ${String(second)}`;
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')");
    let v1 = "x";
    const outcome = async (sql: string, value: string) => {
      const waits = core.harness.path.maintenance.rowWaits;
      const result: unknown = await core.execute(sql, [value]).then(
        () => "written",
        (error: unknown) => error,
      );
      if (core.harness.path.maintenance.rowWaits > waits) reach?.reach("waited for rows");
      if (result === "written") {
        reach?.reach("written");
        return true;
      }
      expect(result, `${context}: ${sql}: written or refused typed`).toMatchObject(TYPED);
      reach?.reach("refused typed");
      return false;
    };
    if (await outcome("UPDATE t SET v = $1", "a".repeat(first))) v1 = "a".repeat(first);
    if (await outcome("UPDATE t SET v = $1 WHERE id = 1", "b".repeat(second)))
      v1 = "b".repeat(second);
    expect(admissions, `${context}: the waits make progress`).toBeLessThanOrEqual(SPIN);
    // Statement atomicity: what was written is there, and nothing of a refused statement.
    // Through SQL (#52 sizes a read by key to the row it fetches, so it is served at these
    // caps), and through the write path's own view, which needs no admission at all.
    const read = await core.execute("SELECT v FROM t WHERE id = 1").then(
      (result) => result,
      (error: unknown) => error,
    );
    if (!(typeof read === "object" && read !== null && "batches" in read))
      throw new Error(`${context}: the read by key is served: ${String(read)}`);
    const served = oracleRows(read as Parameters<typeof oracleRows>[0])[0]?.v;
    expect(served === v1, `${context}: SELECT shows the last written value`).toBe(true);
    reach?.reach("read back through SQL");
    const path = core.harness.path;
    const table = path.catalog.tableByName("t")?.id;
    if (table === undefined) throw new Error("table t");
    const view = path.pin();
    try {
      const v = (await rowsOf(view, table)).find((row) => Number(row[0]) === 1)?.[1];
      expect(v === v1, `${context}: row 1 holds the last written value`).toBe(true);
    } finally {
      view.release();
    }
    expect(core.harness.path.failure).toBeNull();
  } finally {
    await core.close();
  }
}

it("writes or refuses a 4 MiB update at 40 MiB after 2.5 MB rows, never spinning", async () => {
  await history(40, 2_500_000, 4 * MiB);
}, 60_000);

for (const seed of seedsFor("next-row-wait-reclaim", [0x6201, 0x6202]))
  it(`a waiting write step keeps its rows from reclaim (seed ${String(seed)})`, async () => {
    const reach = createReach("row wait reclaim", ROW_WAIT_RECLAIM_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 36, 40, 44, 48, 56),
        fc.constantFrom(1_500_000, 2_000_000, 2_500_000),
        fc.constantFrom(3 * MiB, 4 * MiB, 5 * MiB),
        async (cap, first, second) => {
          reach.beginHistory();
          await history(cap, first, second, reach);
        },
      ),
      { seed, numRuns: 10, examples: [[40, 2_500_000, 4 * MiB]] },
    );
    reach.assertReached();
  }, 600_000);
