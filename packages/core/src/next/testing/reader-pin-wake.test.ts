/**
 * What readers keep from release changes the queue's answer at once, whatever the writer is doing.
 *
 * Found by #71's review (P2, P3): the wake a reader's close or a new hold sent the governor was
 * gated on the writer's idleness, so while a flush was busy a released pin left a servable request
 * pending, and a new hold left an impossible request pending instead of refusing it, until some
 * unrelated release came. And `blockedBytes` on a pin of the current state built identity sets over
 * the whole directory before finding that the current state keeps nothing a reader returns.
 *
 * Law: GOVERNOR-WAIT-POLICY point 5 (next/memory/README.md): a request is served once it fits and
 * refused (53200) once it could never be served beside what its statement keeps; it is never left
 * waiting for an unrelated event. A pin of the current state keeps nothing a reader returns.
 */
import { expect, it } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  tableInput,
} from "../write/test-support.js";

const MiB = 1 << 20;

const outcome = (pending: Promise<readonly Grant[]>) =>
  pending.then(
    (grants) => {
      for (const grant of grants) grant.close();
      return "served";
    },
    (error: unknown) => (error as { sqlState?: string }).sqlState ?? (error as Error).message,
  );
function resolvable(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}
const turn = () =>
  new Promise<"pending">((resolve) =>
    setImmediate(() => {
      resolve("pending");
    }),
  );
/** Its outcome within `ms` (the wakes are microtasks and one asynchronous reclaim), else "pending". */
const within = (pending: Promise<string>, ms = 2_000) =>
  Promise.race([
    pending,
    new Promise<string>((resolve) =>
      setTimeout(() => {
        resolve("pending");
      }, ms),
    ),
  ]);

it("serves on a reader's close and refuses on a new hold while a flush is busy", async () => {
  const governor = new Governor(32 * MiB);
  const paused = resolvable();
  const resume = resolvable();
  let holdFlush = false;
  const harness = new Harness("strict", "memory", 2, undefined, {
    beforeFlushPublish: () => {
      if (!holdFlush) return Promise.resolve();
      paused.resolve();
      return resume.promise;
    },
  });
  Object.defineProperty(harness, "governor", { value: governor });
  const path = await harness.open();
  let blocker: Grant | null = null;
  const stop = new AbortController();
  try {
    let tableId = 0;
    await autocommit(path, async (statement) => {
      tableId =
        (
          await statement.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    holdFlush = true;
    for (let id = 0; id < 3; id++)
      await autocommit(path, (statement) =>
        statement.table(tableId).insert(batch(ints([id])), null),
      );
    // A flush is busy (held before its publish) for the rest of this test.
    await paused.promise;
    const pin = path.pin();
    const free = governor.capBytes - governor.reserveBytes - governor.usedBytes;
    blocker = governor.tryAdmit({
      label: "retained allocation",
      minimumBytes: free - 1024,
      spillable: true,
    });
    if (blocker === null || blocker.held < MiB) throw new Error("blocker too small");
    const retained = blocker;
    expect(retained.allocate(retained.held)).toBe(true);
    let reclaimable = false;
    retained.onReclaimRequest(async () => {
      if (reclaimable) retained.release(MiB);
    });
    // 1 MiB of it returns once every reader older than now has closed (the pin's).
    path.shared.readers.holdUntilOlderClose(
      () => {
        retained.free(MiB);
        reclaimable = true;
      },
      () => MiB,
    );
    await turn();
    const available = governor.capBytes - governor.reserveBytes - governor.usedBytes;
    const waiting = outcome(
      governor.admitGroup({
        label: "queued before an unpin",
        grants: [{ label: "query", minimumBytes: available + MiB / 2, spillable: false }],
        signal: stop.signal,
      }),
    );
    expect(await within(waiting, 50)).toBe("pending");
    pin.release();
    expect(await within(waiting), "served once the pin is gone, the flush still busy").toBe(
      "served",
    );

    const nextPin = path.pin();
    reclaimable = false;
    try {
      const bounded = outcome(
        governor.admitGroup({
          label: "queued before a new hold",
          grants: [
            {
              label: "bounded query",
              minimumBytes: governor.usableCapacity(true) - MiB / 2,
              spillable: false,
            },
          ],
          signal: stop.signal,
          includesProgress: true,
          holdings: () => ({ grants: [], pinned: () => nextPin.blockedBytes() }),
        }),
      );
      expect(await within(bounded, 50)).toBe("pending");
      // The pin now keeps 1 MiB more from release: the request can never be served beside it.
      path.shared.readers.holdUntilOlderClose(
        () => {
          retained.free(MiB);
        },
        () => MiB,
      );
      expect(await within(bounded), "refused at once, the flush still busy").toBe("53200");
    } finally {
      nextPin.release();
    }
  } finally {
    stop.abort(new Error("done"));
    blocker?.close();
    resume.resolve();
    await harness.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it("reads no directory for a pin of the current state", async () => {
  const harness = new Harness("strict", "memory");
  const path = await harness.open();
  try {
    for (let i = 0; i < 8; i++)
      await autocommit(path, (statement) =>
        statement.changeCatalog({
          kind: "createTable",
          table: tableInput(`table_${String(i)}`, [column("id", INT, false)]),
        }),
      );
    await path.quiesce();
    const pin = path.pin();
    const tables = path.state().tables;
    let visits = 0;
    const values = tables.values.bind(tables);
    Object.defineProperty(tables, "values", {
      configurable: true,
      value: function* () {
        for (const table of values()) {
          visits++;
          yield table;
        }
      },
    });
    try {
      for (let k = 0; k < 10; k++) expect(pin.blockedBytes()).toBe(0);
    } finally {
      Reflect.deleteProperty(tables, "values");
      pin.release();
    }
    expect(visits).toBe(0);
  } finally {
    await harness.close();
  }
  expect(harness.governor.usedBytes).toBe(0);
});
