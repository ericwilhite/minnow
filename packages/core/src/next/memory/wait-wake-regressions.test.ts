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
  tableInput,
} from "../write/test-support.js";
import { Governor } from "./governor.js";

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
