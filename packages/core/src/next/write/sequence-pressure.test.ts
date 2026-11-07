/** R4 and governor rules 1/2/4: standalone sequence frames share bounded commit backpressure. */
import { expect, it } from "vitest";
import { yieldTask } from "../exec/pipeline.js";
import { COMMIT_BUFFER_OVERHEAD, LOG_QUEUE_BYTES } from "./commit-buffers.js";
import { Governor } from "../memory/governor.js";
import { Harness, autocommit } from "./test-support.js";

it("keeps fitting calls synchronous and waits at the cap until the log drains", async () => {
  let blocked = false;
  let queuedCost = 0;
  const frames = new Set<Uint8Array>();
  const h = new Harness("strict", "memory", undefined, undefined, {
    onCommitBuffer: (buffer, owned) => {
      if (
        owned &&
        blocked &&
        buffer.length >= 32 &&
        buffer.length <= 64 &&
        buffer.length % 8 === 0
      ) {
        frames.add(buffer);
        queuedCost += buffer.length + COMMIT_BUFFER_OVERHEAD;
        if (queuedCost > LOG_QUEUE_BYTES)
          throw new Error("Sequence queued frame cost exceeded byte bound");
      } else if (!owned && frames.delete(buffer))
        queuedCost -= buffer.length + COMMIT_BUFFER_OVERHEAD;
    },
  });
  const governor = new Governor(32 * 1024 * 1024);
  Object.assign(h, { governor });
  let release: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const open = h.backend.open.bind(h.backend);
  h.backend.open = async (lease) => {
    const session = await open(lease);
    const append = session.appendLog.bind(session);
    session.appendLog = async (bytes) => {
      if (blocked) await gate;
      return append(bytes);
    };
    return session;
  };
  const path = await h.open();
  let id = 0;
  await autocommit(path, async (st) => {
    id =
      (
        await st.changeCatalog({
          kind: "createSequence",
          sequence: {
            name: "s",
            start: 1n,
            increment: 1n,
            min: 1n,
            max: 1000000n,
            cycle: false,
            ownedBy: null,
          },
        })
      ).id ?? 0;
  });
  const out = { repr: "safe" as const, values: new Float64Array(2) };
  const fitting = path.sequences.next(id, 1, out, 0);
  expect(typeof fitting).toBe("number");
  await path.flush();
  const ordinary = governor.capBytes - governor.reserveBytes;
  const pressure = await governor.admit({
    label: "sequence-pressure witness",
    minimumBytes: ordinary - governor.usedBytes,
    spillable: false,
  });
  expect(governor.usedBytes).toBe(ordinary);
  blocked = true;
  let waiting: Promise<number> | null = null;
  let last = 0;
  try {
    for (let k = 1; k <= 40000; k++) {
      const result = path.sequences.set(id, BigInt(k), true);
      last = k;
      if (result instanceof Promise) {
        waiting = result;
        break;
      }
    }
    expect(waiting, "sequence queue must wait at its byte bound").not.toBeNull();
    let settled = false;
    const tracked = waiting?.then((lsn) => {
      settled = true;
      return lsn;
    });
    const next = path.sequences.next(id, 2, out, 0);
    expect(next).toBeInstanceOf(Promise);
    await yieldTask();
    expect(settled).toBe(false);
    release();
    const lsn = await tracked;
    expect(lsn).toBeGreaterThan(0);
    await next;
    expect(Array.from(out.values)).toEqual([last + 1, last + 2]);
    await path.flush();
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
  } finally {
    release();
    pressure.close();
    await h.close();
  }
  expect(governor.usedBytes).toBe(0);
  expect(frames.size).toBe(0);
});
