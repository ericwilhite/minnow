/**
 * Review B3-p2b-3 C1: a commit spanning memtables can only flush whole. When frozen memtables pass
 * their cap and their commit continues into the live memtable, begin() freezes that committed
 * tail so the commit flushes, instead of waiting on a flush that can never run. The wait yields a
 * task each round and stops if a round changes nothing.
 */
import { describe, expect, it } from "vitest";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  rowsOf,
  TEXT,
  tableInput,
  texts,
} from "./test-support.js";

/** begin() with a guard: a spin would read `failure` without end; 1,000 reads stop it. */
async function guardedBegin(path: WritePathImpl): Promise<unknown> {
  let checks = 0;
  const stop = new Error("guard: 1,000 failure checks without the slot");
  Object.defineProperty(path, "failure", {
    configurable: true,
    get: () => (++checks >= 1000 ? stop : null),
  });
  try {
    const tx = await path.begin();
    tx.rollback();
    return null;
  } catch (error) {
    return error;
  } finally {
    Reflect.deleteProperty(path, "failure");
  }
}

async function table(path: WritePathImpl): Promise<number> {
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
        })
      ).id ?? 0;
  });
  return t;
}

describe("frozen memtables whose commit continues into the live one", () => {
  for (const logRecycleBytes of [8 * 1024 * 1024, 64 * 1024 * 1024])
    it(`one 1,500-row commit over 1-row memtables (log bound ${String(logRecycleBytes >> 20)} MiB; the reviewer's probe)`, async () => {
      const harness = new Harness("strict", "memory", 1, undefined, { logRecycleBytes });
      const path = await harness.open();
      const t = await table(path);
      const ids = Array.from({ length: 1500 }, (_, k) => k);
      await autocommit(path, (s) =>
        s.table(t).insert(batch(ints(ids), texts(ids.map(() => "x"))), null),
      );
      await path.quiesce();
      expect(path.stats().frozenMemtables).toBe(1499);
      expect(path.stats().backpressure).toBe(true);
      // The writer gets the slot: the tail froze and the whole commit flushed.
      expect(await guardedBegin(path)).toBeNull();
      expect(path.stats().frozenMemtables).toBe(0);
      expect(path.stats().backpressure).toBe(false);
      const view = path.pin();
      expect((await rowsOf(view, t)).length).toBe(1500);
      view.release();
      await harness.space.audit();
      await harness.close();
    }, 300_000);

  // The reviewer's probe used 1 MiB values; a value that large needs the page track's
  // out-of-line layout, which this write path does not use yet, so the flush itself refuses it.
  // 1,000,000-byte values keep the shape (a commit over many memtables past the frozen cap).
  it("21 values of 1,000,000 bytes over 4-row memtables in one commit (the reviewer's second probe)", async () => {
    const harness = new Harness("strict", "memory", 4, undefined, {
      logRecycleBytes: 256 * 1024 * 1024,
    });
    const path = await harness.open();
    const t = await table(path);
    const ids = Array.from({ length: 21 }, (_, k) => k);
    const big = "y".repeat(1_000_000);
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(ids), texts(ids.map(() => big))), null),
    );
    await path.quiesce();
    expect(path.stats().backpressure).toBe(true);
    expect(await guardedBegin(path)).toBeNull();
    expect(path.failure).toBeNull();
    expect(path.stats().frozenMemtables).toBe(0);
    const reopened = await harness.reopen();
    const view = reopened.pin();
    expect((await rowsOf(view, t)).length).toBe(21);
    view.release();
    await harness.close();
  }, 300_000);
});
