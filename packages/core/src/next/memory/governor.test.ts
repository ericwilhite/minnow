import { describe, expect, it } from "vitest";
import { VarlenBuilder } from "../vector/varlen.js";
import {
  Governor,
  GrantClosedError,
  ValueTooLargeError,
  batchTargetFor,
  bigintBytes,
  type EvictableCache,
} from "./governor.js";

const MiB = 1 << 20;
const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
};

/** A signal that counts its live abort listeners. */
function countingSignal() {
  const controller = new AbortController();
  let live = 0;
  const signal = controller.signal;
  const add = signal.addEventListener.bind(signal);
  const remove = signal.removeEventListener.bind(signal);
  signal.addEventListener = ((...args: Parameters<typeof add>) => {
    live += 1;
    add(...args);
  }) as typeof signal.addEventListener;
  signal.removeEventListener = ((...args: Parameters<typeof remove>) => {
    live -= 1;
    remove(...args);
  }) as typeof signal.removeEventListener;
  return { controller, signal, live: () => live };
}

describe("memory governor: admission and progress", () => {
  it("admits within the cap and makes the next pipeline wait, never refuse (rule 2)", async () => {
    const governor = new Governor(16 * MiB);
    const usable = 16 * MiB - governor.reserveBytes;
    const first = await governor.admit({
      label: "a",
      minimumBytes: usable - MiB,
      spillable: false,
    });
    let admitted = false;
    const second = governor
      .admit({ label: "b", minimumBytes: 2 * MiB, spillable: false })
      .then((grant) => {
        admitted = true;
        return grant;
      });
    await settle();
    expect(admitted).toBe(false);
    first.close();
    expect((await second).held).toBe(2 * MiB);
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
  });

  it("a queued admission never blocks an admitted pipeline's progress (review probe)", async () => {
    const governor = new Governor(16 * MiB);
    const a = await governor.admit({ label: "A", minimumBytes: 2 * MiB, spillable: true });
    expect(a.tryGrow(12 * MiB)).toBe(true);
    let admitted = false;
    const b = governor.admit({ label: "B", minimumBytes: MiB, spillable: false }).then((grant) => {
      admitted = true;
      return grant;
    });
    const loan = await a.borrow(MiB);
    expect(admitted).toBe(false);
    loan.repay();
    a.close();
    expect((await b).held).toBe(MiB);
  });

  it("two queries that each need progress both finish (dependency cycle)", async () => {
    const governor = new Governor(16 * MiB);
    const usable = 16 * MiB - governor.reserveBytes;
    const q1 = await governor.admit({ label: "q1", minimumBytes: usable / 2, spillable: false });
    const q2 = await governor.admit({ label: "q2", minimumBytes: usable / 2, spillable: false });
    const q3 = governor.admit({ label: "q3", minimumBytes: MiB, spillable: false });
    const big = (governor.reserveBytes * 3) / 4;
    const finished: string[] = [];
    const run = async (grant: typeof q1, name: string) => {
      const loan = await grant.borrow(big);
      await Promise.resolve();
      loan.repay();
      grant.close();
      finished.push(name);
    };
    await Promise.all([run(q1, "q1"), run(q2, "q2")]);
    expect(finished.sort()).toEqual(["q1", "q2"]);
    expect((await q3).held).toBe(MiB);
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
  });

  it("keeps minimums bounded by the plan and refuses only a value above the reserve", async () => {
    expect(batchTargetFor(64 * MiB, 4)).toBe(MiB);
    expect(batchTargetFor(16 * MiB, 40)).toBe(Math.floor((16 * MiB) / 320));
    const governor = new Governor(16 * MiB);
    await expect(
      governor.admit({ label: "huge", minimumBytes: 15 * MiB, spillable: false }),
    ).rejects.toThrow(RangeError);
    const grant = await governor.admit({ label: "g", minimumBytes: MiB, spillable: false });
    await expect(grant.borrow(governor.reserveBytes + 1)).rejects.toThrow(ValueTooLargeError);
    const loan = await grant.borrow(MiB);
    await expect(grant.borrow(MiB)).rejects.toThrow(RangeError);
    loan.repay();
    expect(grant.held).toBe(MiB);
  });
});

describe("memory governor: cache, reclaim and lifecycle", () => {
  it("evicts the cache before refusing growth, and growth never takes the reserve", async () => {
    const governor = new Governor(16 * MiB);
    let cached = 8 * MiB;
    const handle = governor.registerCache({
      get evictableBytes() {
        return cached;
      },
      evict(bytes) {
        const freed = Math.min(bytes, cached);
        cached -= freed;
        handle.release(freed);
        return freed;
      },
    });
    expect(handle.charge(8 * MiB)).toBe(true);
    const grant = await governor.admit({ label: "agg", minimumBytes: MiB, spillable: true });
    expect(grant.tryGrow(6 * MiB)).toBe(true);
    expect(cached).toBe(7 * MiB);
    expect(grant.tryGrow(16 * MiB)).toBe(false);
    expect(governor.usedBytes).toBeLessThanOrEqual(16 * MiB - governor.reserveBytes);
  });

  it("charges each request once when the cache releases synchronously (review probe)", async () => {
    const governor = new Governor(16 * MiB);
    let cached = 14 * MiB;
    const cache: EvictableCache = {
      get evictableBytes() {
        return cached;
      },
      evict() {
        const freed = cached;
        cached = 0;
        handle.release(freed);
        return freed;
      },
    };
    const handle = governor.registerCache(cache);
    handle.charge(cached);
    const grant = await governor.admit({ label: "one", minimumBytes: MiB, spillable: false });
    expect(governor.usedBytes).toBe(grant.held);
    grant.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("survives reclaim callbacks that release synchronously inside the drain (rule 7)", async () => {
    const governor = new Governor(16 * MiB);
    const usable = 16 * MiB - governor.reserveBytes;
    const agg = await governor.admit({ label: "agg", minimumBytes: MiB, spillable: true });
    expect(agg.tryGrow(usable - MiB)).toBe(true);
    let calls = 0;
    agg.onReclaimRequest(() => {
      calls += 1;
      agg.release(Math.min(4 * MiB, agg.held - agg.allocated));
    });
    const next = await governor.admit({ label: "join", minimumBytes: 2 * MiB, spillable: false });
    expect(calls).toBeGreaterThan(0);
    expect(governor.usedBytes).toBe(agg.held + next.held);
    agg.close();
    next.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("makes a closed grant inert and rejects its queued loan (review probe)", async () => {
    const governor = new Governor(16 * MiB);
    const closed = await governor.admit({ label: "closed", minimumBytes: MiB, spillable: true });
    closed.close();
    await expect(closed.borrow(MiB)).rejects.toThrow(GrantClosedError);
    expect(closed.tryGrow(1)).toBe(false);
    expect(closed.allocate(1)).toBe(false);
    closed.close();
    expect(governor.usedBytes).toBe(0);

    const usable = 16 * MiB - governor.reserveBytes;
    const full = await governor.admit({ label: "full", minimumBytes: usable, spillable: false });
    const filler = await full.borrow(governor.reserveBytes);
    const waiter = await governor.admit({ label: "w", minimumBytes: 0, spillable: false });
    const { signal, live } = countingSignal();
    const queued = waiter.borrow(MiB, signal);
    await settle();
    expect(live()).toBe(1);
    waiter.close();
    await expect(queued).rejects.toThrow(GrantClosedError);
    expect(live()).toBe(0);
    filler.repay();
    full.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("detaches abort listeners when requests settle or are cancelled", async () => {
    const governor = new Governor(16 * MiB);
    const { signal, live, controller } = countingSignal();
    const grant = await governor.admit({ label: "a", minimumBytes: MiB, spillable: false, signal });
    expect(live()).toBe(0);
    const usable = 16 * MiB - governor.reserveBytes;
    const pending = governor.admit({ label: "b", minimumBytes: usable, spillable: false, signal });
    await settle();
    expect(live()).toBe(1);
    controller.abort(new Error("cancelled"));
    await expect(pending).rejects.toThrow("cancelled");
    expect(live()).toBe(0);
    grant.close();
    expect(governor.usedBytes).toBe(0);
  });
});

describe("suballocation inside admitted minimums (rule 4)", () => {
  it("a non-spillable grant builds varlen buffers from its minimum, then a loan (review probe)", async () => {
    const governor = new Governor(16 * MiB);
    const scan = await governor.admit({ label: "scan", minimumBytes: 64 * 1024, spillable: false });
    const builder = new VarlenBuilder(64, scan, 32 * 1024, 1024);
    expect(scan.allocated).toBe(builder.retainedBytes);
    expect(governor.usedBytes).toBe(64 * 1024);
    // An oversized value cannot fit the minimum, and the grant cannot grow: denied.
    expect(builder.appendBytes(new Uint8Array(200 * 1024))).toBe("denied");
    // Progress: borrow a loan; the retry allocates from it (no second request to the governor).
    const loan = await scan.borrow(256 * 1024);
    expect(builder.appendBytes(new Uint8Array(200 * 1024))).toBe("ok");
    expect(scan.allocated).toBe(builder.retainedBytes);
    builder.reset();
    loan.repay();
    builder.close();
    expect(scan.allocated).toBe(0);
    scan.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("a spillable grant charges growth overlap once, not twice (review probe)", async () => {
    const governor = new Governor(16 * MiB);
    const grant = await governor.admit({ label: "concat", minimumBytes: 8192, spillable: true });
    const builder = new VarlenBuilder(64, grant, 1 << 16, 1024);
    for (let index = 0; index < 20; index++) builder.appendBytes(new Uint8Array(900));
    expect(grant.allocated).toBe(builder.retainedBytes);
    expect(governor.usedBytes).toBe(grant.held);
    expect(grant.held - grant.allocated).toBeLessThan(builder.retainedBytes);
    builder.close();
    grant.close();
    expect(governor.usedBytes).toBe(0);
    expect(bigintBytes(0n)).toBe(24);
    expect(bigintBytes(2n ** 200n)).toBe(16 + 8 * 4);
  });
});
