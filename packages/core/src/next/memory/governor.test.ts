import { describe, expect, it } from "vitest";
import { VarlenBuilder } from "../vector/varlen.js";
import {
  Governor,
  GrantClosedError,
  ValueTooLargeError,
  batchTargetFor,
  bigintBytes,
  type EvictableCache,
  type Loan,
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

describe("loan races (review A1-3 finding 3)", () => {
  const ticks = async (count: number) => {
    for (let i = 0; i < count; i++) await Promise.resolve();
  };
  /** After everything is closed: nothing charged, nothing loaned, and the reserve is intact. */
  async function reserveIntact(governor: Governor) {
    expect(governor.usedBytes).toBe(0);
    expect(governor.loanedBytes).toBe(0);
    const probe = await governor.admit({
      label: "probe",
      minimumBytes: governor.capBytes - governor.reserveBytes,
      spillable: true,
    });
    expect(probe.tryGrow(1)).toBe(false);
    probe.close();
  }

  it.each([0, 1, 2, 3, 5, 8])(
    "close %i microtasks after a served borrow returns the loan (review probe)",
    async (delay) => {
      const governor = new Governor(16 * MiB);
      const a = await governor.admit({ label: "A", minimumBytes: 14 * MiB, spillable: true });
      const pending = a.borrow(2 * MiB);
      await ticks(delay);
      a.close();
      const settled: Loan | GrantClosedError = await pending.then(
        (loan) => loan,
        (error: unknown) => {
          expect(error).toBeInstanceOf(GrantClosedError);
          return error as GrantClosedError;
        },
      );
      if (!(settled instanceof GrantClosedError)) settled.repay();
      await reserveIntact(governor);
    },
  );

  it.each([0, 1, 3])("close or abort %i microtasks into a queued borrow", async (delay) => {
    for (const how of ["close", "abort"] as const) {
      const governor = new Governor(16 * MiB);
      const full = await governor.admit({
        label: "full",
        minimumBytes: 14 * MiB,
        spillable: false,
      });
      const holder = await full.borrow(2 * MiB);
      const waiter = await governor.admit({ label: "w", minimumBytes: 0, spillable: false });
      const controller = new AbortController();
      const pending = waiter.borrow(MiB, controller.signal);
      await ticks(delay);
      if (how === "close") waiter.close();
      else controller.abort(new Error("cancelled"));
      await expect(pending).rejects.toThrow(how === "close" ? GrantClosedError : Error);
      holder.repay();
      full.close();
      waiter.close();
      await reserveIntact(governor);
    }
  });

  it("repay validates before it mutates, so a refused repay can be retried (review probe)", async () => {
    const governor = new Governor(16 * MiB);
    const c = await governor.admit({ label: "C", minimumBytes: 14 * MiB, spillable: true });
    const loan = await c.borrow(2 * MiB);
    expect(c.allocate(16 * MiB)).toBe(true);
    expect(() => {
      loan.repay();
    }).toThrow(RangeError);
    expect(c.held).toBe(16 * MiB);
    expect(governor.loanedBytes).toBe(2 * MiB);
    c.free(2 * MiB);
    loan.repay();
    expect(c.held).toBe(14 * MiB);
    expect(governor.loanedBytes).toBe(0);
    c.close();
    await reserveIntact(governor);
  });

  it("keeps its accounting exact under random interleavings of every operation", async () => {
    for (let seed = 1; seed <= 40; seed++) {
      let state = seed;
      const random = () => {
        state = (Math.imul(state ^ (state >>> 15), 0x2c1b3c6d) + 0x9e3779b9) >>> 0;
        return state / 2 ** 32;
      };
      const governor = new Governor(16 * MiB);
      interface Live {
        grant: Awaited<ReturnType<Governor["admit"]>>;
        loan: { repay(): void } | null;
      }
      const grants: Live[] = [];
      const pending: Array<Promise<unknown>> = [];
      // Bounds hold at every step; exact equality with the grants' holdings is checked once
      // everything has settled (an admission is charged a few microtasks before the test sees it).
      const check = () => {
        const held = grants.reduce((sum, live) => sum + live.grant.held, 0);
        expect(governor.usedBytes).toBeGreaterThanOrEqual(held);
        expect(governor.usedBytes).toBeLessThanOrEqual(governor.capBytes);
        expect(governor.usedBytes - governor.loanedBytes).toBeLessThanOrEqual(
          governor.capBytes - governor.reserveBytes,
        );
      };
      for (let step = 0; step < 60; step++) {
        const roll = random();
        const live = grants[Math.floor(random() * grants.length)];
        if (roll < 0.25 || live === undefined) {
          pending.push(
            governor
              .admit({
                label: `g${String(step)}`,
                minimumBytes: Math.floor(random() * 4) * MiB,
                spillable: random() < 0.7,
              })
              .then((grant) => {
                grants.push({ grant, loan: null });
              }),
          );
        } else if (roll < 0.45) live.grant.tryGrow(Math.floor(random() * 4) * MiB);
        else if (roll < 0.65 && live.loan === null)
          pending.push(
            live.grant.borrow(Math.floor(random() * 2 + 1) * (MiB / 2)).then(
              (loan) => {
                live.loan = loan;
              },
              () => undefined,
            ),
          );
        else if (roll < 0.8 && live.loan !== null) {
          live.loan.repay();
          live.loan = null;
        } else if (roll < 0.9) {
          live.grant.close();
          grants.splice(grants.indexOf(live), 1);
        }
        await ticks(Math.floor(random() * 4));
        check();
      }
      for (const live of [...grants]) live.grant.close();
      grants.length = 0;
      // Anything admitted late is closed as it arrives.
      for (let round = 0; round < 10; round++) {
        await ticks(5);
        for (const live of grants.splice(0)) live.grant.close();
      }
      await Promise.all(pending);
      for (const live of grants.splice(0)) live.grant.close();
      check();
      await reserveIntact(governor);
    }
  });
});
