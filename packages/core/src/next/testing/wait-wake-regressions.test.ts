/** Review 8 of #11 (GOVERNOR-WAIT-POLICY): the writer's lifetime holdings are registered, and a
 * statement ending wakes queued admissions that the now-idle writer's forecasts can serve. */
import { expect, it } from "vitest";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  tableInput,
} from "../write/test-support.js";
import { Governor } from "../memory/governor.js";

const settleWithin = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<"pending">((resolve) => {
      setTimeout(() => {
        resolve("pending");
      }, ms);
    }),
  ]);

it("registers an idle writer's lifetime holdings: the whole usable cap is served, one byte more is 53200", async () => {
  const h = new Harness();
  Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
  await h.open();
  const governor = h.governor;
  try {
    expect(governor.permanentBytes).toBeGreaterThanOrEqual(2 << 20);
    const usable = governor.usableCapacity();
    // One byte above the usable cap: never servable while the writer is open.
    const above = await settleWithin(
      governor.admit({ label: "above", minimumBytes: usable + 1, spillable: false }).then(
        () => "served",
        (error: unknown) => error,
      ),
      2_000,
    );
    expect(above).toMatchObject({ sqlState: "53200" });
    // The whole usable cap: the idle writer's revocable forecasts are reclaimed for it.
    const whole = await settleWithin(
      governor.admit({ label: "whole usable", minimumBytes: usable, spillable: false }),
      2_000,
    );
    expect(whole, "the usable cap waited behind unregistered lifetime holdings").not.toBe(
      "pending",
    );
    if (whole !== "pending") whole.close();
  } finally {
    await h.close();
  }
  expect(governor.usedBytes).toBe(0);
});

// After writes the commit writers, key offsets and metadata have grown and stay grown, and the
// memtable holds unflushed rows. A request sized now must settle (the idle writer spills its
// memtable, so it never waits on rows only a later write would flush); once it has, the
// registration is exact: the whole usable cap is served and one byte more is 53200.
it.each([1, 2_000, 60_000])(
  "settles, then registers the lifetime holdings exactly, after a quiesced write of %i rows",
  async (rows) => {
    const h = new Harness();
    Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
    const path = await h.open();
    const governor = h.governor;
    const usable = () => governor.usableCapacity();
    try {
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("t", [column("id", INT, false)], { primaryKey: null }),
            })
          ).id ?? 0;
      });
      await autocommit(path, (s) =>
        s.table(t).insert(batch(ints(Array.from({ length: rows }, (_, k) => k))), null),
      );
      await path.quiesce();
      const first = await settleWithin(
        governor
          .admit({ label: "sized before spill", minimumBytes: usable(), spillable: false })
          .then(
            (grant) => {
              grant.close();
              return "served";
            },
            (error: unknown) => (error as { sqlState?: string }).sqlState ?? error,
          ),
        2_000,
      );
      expect(first, "waited on memtable rows no flush was coming for").not.toBe("pending");
      await path.quiesce();
      const above = await settleWithin(
        governor.admit({ label: "above", minimumBytes: usable() + 1, spillable: false }).then(
          () => "served",
          (error: unknown) => error,
        ),
        2_000,
      );
      expect(above).toMatchObject({ sqlState: "53200" });
      const whole = await settleWithin(
        governor.admit({ label: "whole usable", minimumBytes: usable(), spillable: false }),
        2_000,
      );
      expect(whole, "the usable cap waited behind unregistered lifetime holdings").not.toBe(
        "pending",
      );
      if (whole !== "pending") whole.close();
    } finally {
      await h.close();
    }
    expect(governor.usedBytes).toBe(0);
  },
);

it("wakes a queued admission when the writable statement holding the writer's forecasts ends", async () => {
  const h = new Harness();
  Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
  const path = await h.open();
  const governor = h.governor;
  const tx = await path.begin();
  const statement = tx.statement({
    rules: () => ({ check: () => null, generate: () => undefined }),
  });
  const stop = new AbortController();
  let state = "pending";
  const waiting = governor
    .admit({
      label: "statement-blocked reader",
      minimumBytes: governor.capBytes - governor.reserveBytes - (4 << 20),
      spillable: false,
      signal: stop.signal,
    })
    .then(
      (grant) => {
        state = "served";
        grant.close();
      },
      (error: unknown) => {
        state = String((error as { sqlState?: string }).sqlState ?? error);
      },
    );
  try {
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(state, "served while the writable statement still protected its forecasts").toBe(
      "pending",
    );
    await statement.end();
    expect(
      await settleWithin(
        waiting.then(() => state),
        2_000,
      ),
    ).toBe("served");
  } finally {
    stop.abort();
    await waiting;
    tx.rollback();
    await h.close();
  }
  expect(governor.usedBytes).toBe(0);
});

// Review 9: run readers kept only by an older snapshot are finite holders, not lifetime ones. A
// request they block waits for the snapshot (no 53200), and releasing it changes no lifetime sum.
it("waits for a snapshot's retired run readers instead of refusing", async () => {
  const h = new Harness("strict", "sim", 16);
  Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
  const path = await h.open();
  const governor = h.governor;
  try {
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(Array.from({ length: 16 }, (_, i) => i))), null),
    );
    await autocommit(path, (s) => s.table(t).insert(batch(ints([16])), null));
    await path.quiesce();
    const view = path.pin();
    await view
      .index(t, path.catalog.table(t)?.primaryKey ?? 0)
      .collect({ lower: null, upper: null });
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
    await path.quiesce();
    const before = governor.permanentBytes;
    const bytes = governor.usableCapacity();
    let state = "pending";
    const waiting = governor
      .admit({ label: "behind a snapshot", minimumBytes: bytes, spillable: false })
      .then(
        (grant) => {
          state = "served";
          grant.close();
        },
        (error: unknown) => {
          state = String((error as { sqlState?: string }).sqlState ?? error);
        },
      );
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(state, "refused or served while the snapshot held the dropped table").toBe("pending");
    view.release();
    expect(governor.permanentBytes, "the snapshot's readers were counted as lifetime").toBe(before);
    expect(
      await settleWithin(
        waiting.then(() => state),
        4_000,
      ),
    ).toBe("served");
  } finally {
    await h.close();
  }
  expect(governor.usedBytes).toBe(0);
});

// Review 9: a committed DELETE of published, indexed rows leaves only tombstones (index deltas)
// in the live memtable; the idle spill flushes them too, and the registration is then exact.
it("spills a deletion-only memtable, then serves the whole usable cap", async () => {
  const h = new Harness();
  Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
  const path = await h.open();
  const governor = h.governor;
  const outcome = (label: string, bytes: number) =>
    governor.admit({ label, minimumBytes: bytes, spillable: false }).then(
      (grant) => {
        grant.close();
        return "served";
      },
      (error: unknown) => (error as { sqlState?: string }).sqlState ?? error,
    );
  try {
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("d", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2, 3])), null));
    await path.quiesce();
    expect(
      await settleWithin(outcome("publish inserts", governor.usableCapacity()), 4_000),
    ).not.toBe("pending");
    await path.quiesce();
    const view = path.pin();
    const cursor = view
      .index(t, path.catalog.table(t)?.primaryKey ?? 0)
      .range({ lower: null, upper: null }, "asc");
    const out = new Float64Array(3);
    const n = await cursor.next(out);
    cursor.close();
    view.release();
    expect(n).toBe(3);
    await autocommit(path, (s) =>
      s.table(t).delete(batch(locators(Array.from(out.subarray(0, n)), t)), null),
    );
    await path.quiesce();
    const first = await settleWithin(
      outcome("deletion-only reclaim", governor.usableCapacity()),
      4_000,
    );
    expect(first, "waited on a deletion-only memtable").not.toBe("pending");
    await path.quiesce();
    const usable = governor.usableCapacity();
    expect(await settleWithin(outcome("above", usable + 1), 500)).toBe("53200");
    expect(await settleWithin(outcome("whole usable", usable), 4_000)).toBe("served");
  } finally {
    await h.close();
  }
  expect(governor.usedBytes).toBe(0);
});

// Review 10 (full 64 MiB run, write-query-reclaim seed 47226): close must wait for an idle spill
// already running. Its no-change publication reads the FTS mutation entries; closing under it
// failed with "FTS mutation owner closed before its work drained".
it.each([0, 1, 2, 4, 8])("closes after an idle spill already running (%i turns)", async (turns) => {
  const h = new Harness();
  Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
  const path = await h.open();
  const governor = h.governor;
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("c", [column("id", INT, false)], { primaryKey: null }),
        })
      ).id ?? 0;
  });
  await autocommit(path, (s) => s.table(t).insert(batch(ints([0, 1, 2])), null));
  await path.quiesce();
  const spilled = governor
    .admit({ label: "publish", minimumBytes: governor.usableCapacity(), spillable: false })
    .then(
      (grant) => {
        grant.close();
      },
      () => undefined,
    );
  await settleWithin(spilled, 4_000);
  await path.quiesce();
  // Deleting published rows of an unindexed table leaves only FTS mutation entries: the next idle
  // reclaim runs a no-change publication over them.
  await autocommit(path, (s) => s.table(t).delete(batch(locators([0, 1], t)), null));
  await path.quiesce();
  const stop = new AbortController();
  const waiting = governor
    .admit({
      label: "starts the spill",
      minimumBytes: governor.usableCapacity(),
      spillable: false,
      signal: stop.signal,
    })
    .then(
      (grant) => {
        grant.close();
      },
      () => undefined,
    );
  for (let turn = 0; turn < turns; turn++) await Promise.resolve();
  stop.abort();
  await waiting;
  await h.close();
  expect(governor.usedBytes).toBe(0);
});

// Review 10: the idle spill republishes the state after freezing; a begin() racing it must see a
// funded state (the previous one until the new pin's charge succeeds), never a ParseBudgetError.
it.each([0, 1, 2, 4])(
  "begins a transaction while an idle spill publishes (%i turns)",
  async (turns) => {
    const h = new Harness();
    Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
    const path = await h.open();
    const governor = h.governor;
    const outcome = (label: string, bytes: number, signal?: AbortSignal) =>
      governor
        .admit({ label, minimumBytes: bytes, spillable: false, ...(signal ? { signal } : {}) })
        .then(
          (grant) => {
            grant.close();
            return "served";
          },
          (error: unknown) =>
            (error as { sqlState?: string; name?: string }).sqlState ?? (error as Error).name,
        );
    try {
      let t = 0;
      await autocommit(path, async (s) => {
        t =
          (
            await s.changeCatalog({
              kind: "createTable",
              table: tableInput("d", [column("id", INT, false)], { primaryKey: null }),
            })
          ).id ?? 0;
      });
      await autocommit(path, (s) => s.table(t).insert(batch(ints([0, 1, 2])), null));
      await path.quiesce();
      await settleWithin(outcome("publish inserts", governor.usableCapacity()), 4_000);
      await path.quiesce();
      await autocommit(path, (s) => s.table(t).delete(batch(locators([0, 1], t)), null));
      await path.quiesce();
      const stop = new AbortController();
      const racing = outcome("racing request", governor.usableCapacity(), stop.signal);
      for (let turn = 0; turn < turns; turn++) await Promise.resolve();
      let failure: unknown = null;
      try {
        const tx = await path.begin();
        tx.rollback();
      } catch (error) {
        failure = error;
      }
      expect(failure, "begin() pinned an unfunded state").toBeNull();
      const settled = await settleWithin(racing, 4_000);
      stop.abort();
      await racing;
      expect(["served", "53200"]).toContain(settled);
    } finally {
      await h.close();
    }
    expect(governor.usedBytes).toBe(0);
  },
);
