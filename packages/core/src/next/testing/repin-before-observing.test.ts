/**
 * A query refused only because its own snapshot keeps memory moves to a fresh snapshot, when the
 * owner offers one, and is served.
 *
 * With #56, `SELECT v FROM t WHERE id = 1` after C1's history (two 2.5 MB rows, a refused 4 MiB
 * update) at 36.5 MiB is refused with 53200: while it waits for its fixed buffers, the write
 * path flushes, and the flushed rows stay for its own snapshot alone (`Snapshot.exclusiveBytes`),
 * so it could never be served beside them. It has delivered no row and written nothing, so
 * nothing it could show depends on that snapshot: the execution asks the owner for a fresh one
 * (`ExecuteRequest.repin`) and prepares again on it (exec/run.ts `#repinnable`, `#repin`).
 *
 * Law: PostgreSQL docs, Transaction Isolation (13.2): a statement "sees a snapshot of the
 * database as of the instant the query begins to run"; a query that has returned no row, taken
 * no lock and called no sequence function has not begun to run in any visible sense, so taking
 * its snapshot when it is admitted is indistinguishable from that (design note, lead decisions
 * 2026-11-23). GOVERNOR-WAIT-POLICY point 5 (next/memory/README.md) for the refusal it replaces.
 *
 * The window is narrow and measured: on the fresh snapshot the rows are in flushed groups, and
 * the read needs 34.97 MB instead of 27.46 MB, so from 31 to 36.25 MiB it re-pins and is still
 * refused (typed), and from 37 MiB its snapshot no longer decides. At 30 and 48 MiB its snapshot
 * keeps a few KB alone without deciding the refusal: the control for that check.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import type { Snapshot } from "../access/snapshot.js";
import type { Execution, PreparedPlan } from "../exec/executor.js";
import { Governor } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { rowsOf } from "../write/test-support.js";
import { createReach, REPIN_BEFORE_OBSERVING_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { oracleRows, type NextRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const MiB = 1 << 20;
const TYPED = { name: "MemoryAdmissionError", code: "MINNOW_MEMORY_ADMISSION", sqlState: "53200" };

afterEach(() => {
  Governor.stallAfterMs = null;
});

type Offer = "none" | "fresh" | "declined";

/** C1's history at `capMiB`, then the point read, with the owner's offer as given. */
async function read(capMiB: number, offer: Offer) {
  Governor.stallAfterMs = 5_000;
  const core = new StorageCore();
  const governor = new Governor(Math.round(capMiB * MiB));
  Object.defineProperty(core.harness, "governor", { value: governor });
  await core.open();
  const path = core.harness.path;
  try {
    await core.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
    await core.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')");
    await core.execute("UPDATE t SET v = $1", ["a".repeat(2_500_000)]);
    // Refused or written: either way the memtable holds the wide rows.
    await core
      .execute("UPDATE t SET v = $1 WHERE id = 1", ["b".repeat(4 * MiB)])
      .catch((error: unknown) => {
        expect(error).toMatchObject(TYPED);
      });
    const lowered = core.lowerer.lower("SELECT v FROM t WHERE id = 1", core.context());
    if (lowered.kind !== "query") throw new Error("query plan required");
    const prepared = core.executor.prepare(lowered.plan);
    let pin: Snapshot = path.pin();
    const first = pin;
    let offers = 0;
    const repin = (catalogVersion: number): Snapshot | null => {
      offers++;
      if (offer === "declined") return null;
      const fresh = path.pin();
      if (fresh.catalog.version !== catalogVersion) {
        fresh.release();
        return null;
      }
      pin.release();
      pin = fresh;
      return fresh;
    };
    const execution = prepared.execute({
      snapshot: pin,
      statement: null,
      parameters: [],
      statementTime: 0n,
      governor,
      spill: core.spill,
      queryId: 1_000,
      reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
      sequences: null,
      sequenceSession: null,
      returning: "atExecuted",
      ...(offer === "none" ? {} : { repin }),
    });
    // The adapter's own collection (private to it), so the served value can be compared.
    const collect = (
      core as unknown as {
        collect(execution: Execution, prepared: PreparedPlan): Promise<NextRows["batches"]>;
      }
    ).collect.bind(core);
    let served: unknown = undefined;
    const outcome: unknown = await collect(execution, prepared).then(
      async (batches) => {
        await execution.delivered;
        served = oracleRows({ columns: prepared.columns, batches })[0]?.v;
        return "served";
      },
      (error: unknown) => error,
    );
    // What row 1 holds, read through the write path's own view.
    const table = path.catalog.tableByName("t")?.id;
    if (table === undefined) throw new Error("table t");
    const view = path.pin();
    const committed = (await rowsOf(view, table)).find((row) => Number(row[0]) === 1)?.[1];
    view.release();
    const result = { outcome, offers, moved: pin !== first, served, committed };
    pin.release();
    expect(path.failure).toBeNull();
    return result;
  } finally {
    await core.close();
  }
}

for (const cap of [36.5, 36.75])
  it(`moves to a fresh snapshot and serves the read at ${String(cap)} MiB`, async () => {
    const without = await read(cap, "none");
    expect(without.outcome, "without an offer: refused typed (#56)").toMatchObject(TYPED);
    const moved = await read(cap, "fresh");
    expect(moved.outcome, "with an offer: served").toBe("served");
    expect(moved.offers, "one re-pin").toBe(1);
    expect(moved.moved, "the execution read the fresh snapshot").toBe(true);
    expect(moved.served === moved.committed, "it shows what is committed").toBe(true);
  }, 120_000);

it("keeps the refusal when the owner declines (a later statement, a moved catalog)", async () => {
  const declined = await read(36.5, "declined");
  expect(declined.outcome).toMatchObject(TYPED);
  expect(declined.offers, "it asked once").toBe(1);
  expect(declined.moved).toBe(false);
}, 60_000);

for (const cap of [30, 48])
  it(`does not move at ${String(cap)} MiB, where its snapshot did not decide the refusal`, async () => {
    // Its snapshot keeps a few KB alone, but the read needs 35 MB beside 28.4 MB available (57 MB
    // beside 47.3 MB at 48 MiB): no snapshot frees that.
    const result = await read(cap, "fresh");
    expect(result.outcome).toMatchObject(TYPED);
    expect(result.offers, "no re-pin for a refusal its snapshot did not decide").toBe(0);
  }, 60_000);

for (const seed of seedsFor("next-repin-before-observing", [0x6401, 0x6402]))
  it(`an offer never makes a read worse, and a moved read shows what is committed (seed ${String(seed)})`, async () => {
    const reach = createReach("re-pin before observing", REPIN_BEFORE_OBSERVING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 26 * 4, max: 64 * 4 }).map((quarters) => quarters / 4),
        async (cap) => {
          reach.beginHistory();
          const without = await read(cap, "none");
          const moved = await read(cap, "fresh");
          const context = `${String(cap)} MiB`;
          if (without.outcome === "served")
            expect(moved.outcome, `${context}: served without an offer, so with one`).toBe(
              "served",
            );
          else expect(without.outcome, context).toMatchObject(TYPED);
          if (moved.outcome === "served")
            expect(moved.served === moved.committed, `${context}: shows what is committed`).toBe(
              true,
            );
          else expect(moved.outcome, context).toMatchObject(TYPED);
          // Each move follows a publication; this history publishes once while the read waits.
          expect(moved.offers, `${context}: at most one move`).toBeLessThanOrEqual(1);
          if (moved.offers === 1)
            reach.reach(moved.outcome === "served" ? "moved and served" : "moved and refused");
          else reach.reach(moved.outcome === "served" ? "served in place" : "refused in place");
        },
      ),
      { seed, numRuns: 16, examples: [[36.5], [33], [30], [48]] },
    );
    reach.assertReached();
  }, 600_000);
