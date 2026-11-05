import { describe, expect, it } from "vitest";
import { VarlenBuilder } from "../vector/varlen.js";
import {
  Governor,
  ValueTooLargeError,
  batchTargetFor,
  bigintBytes,
  reserveFor,
  type EvictableCache,
} from "./governor.js";

const MiB = 1 << 20;

describe("memory governor", () => {
  it("admits within the cap and makes the next pipeline wait, never refuse (rule 2)", async () => {
    const governor = new Governor(16 * MiB);
    const usable = 16 * MiB - reserveFor(16 * MiB);
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
    await Promise.resolve();
    expect(admitted).toBe(false);
    first.close();
    const grant = await second;
    expect(admitted).toBe(true);
    expect(grant.bytes).toBe(2 * MiB);
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
  });

  it("keeps minimums bounded by the plan, not the data (rule 3)", async () => {
    expect(batchTargetFor(64 * MiB, 4)).toBe(MiB);
    expect(batchTargetFor(16 * MiB, 40)).toBe(Math.floor((16 * MiB) / 320));
    const governor = new Governor(16 * MiB);
    await expect(
      governor.admit({ label: "huge", minimumBytes: 15 * MiB, spillable: false }),
    ).rejects.toThrow(RangeError);
  });

  it("evicts the cache before refusing growth, and refuses growth into the reserve (rules 4, 6)", async () => {
    const governor = new Governor(16 * MiB);
    let cached = 8 * MiB;
    const cache: EvictableCache = {
      get evictableBytes() {
        return cached;
      },
      evict(bytes) {
        const freed = Math.min(bytes, cached);
        cached -= freed;
        handle.release(freed);
        return freed;
      },
    };
    const handle = governor.registerCache(cache);
    expect(handle.charge(8 * MiB)).toBe(true);
    const grant = await governor.admit({ label: "agg", minimumBytes: MiB, spillable: true });
    // 8 cached + 1 + 6 > 14 usable: the cache gives up a MiB rather than the grant being refused.
    expect(grant.tryGrow(6 * MiB)).toBe(true);
    expect(cached).toBe(7 * MiB);
    expect(grant.tryGrow(16 * MiB)).toBe(false);
    expect(governor.usedBytes).toBeLessThanOrEqual(16 * MiB - governor.reserveBytes);
    const plain = await governor.admit({ label: "scan", minimumBytes: MiB, spillable: false });
    expect(plain.tryGrow(1)).toBe(false);
  });

  it("asks spillable grants to give memory back while an admission waits (rule 7)", async () => {
    const governor = new Governor(16 * MiB);
    const usable = 16 * MiB - governor.reserveBytes;
    const agg = await governor.admit({ label: "agg", minimumBytes: MiB, spillable: true });
    expect(agg.tryGrow(usable - MiB)).toBe(true);
    const requests: number[] = [];
    agg.onReclaimRequest((bytes) => {
      requests.push(bytes);
      agg.release(4 * MiB);
    });
    const next = await governor.admit({ label: "join", minimumBytes: 2 * MiB, spillable: false });
    expect(requests.length).toBeGreaterThan(0);
    expect(next.bytes).toBe(2 * MiB);
  });

  it("serves waits from the reserve in order, and refuses only a value larger than it (rule 5)", async () => {
    const governor = new Governor(16 * MiB);
    const usable = 16 * MiB - governor.reserveBytes;
    const grant = await governor.admit({
      label: "project",
      minimumBytes: usable,
      spillable: false,
    });
    await grant.waitFor(governor.reserveBytes);
    expect(governor.usedBytes).toBe(16 * MiB);
    const blocked = grant.waitFor(MiB);
    let done = false;
    void blocked.then(() => (done = true));
    await Promise.resolve();
    expect(done).toBe(false);
    grant.release(governor.reserveBytes);
    await blocked;
    await expect(grant.waitFor(governor.reserveBytes + 1)).rejects.toThrow(ValueTooLargeError);
    const controller = new AbortController();
    const cancelled = governor.admit({
      label: "late",
      minimumBytes: usable,
      spillable: false,
      signal: controller.signal,
    });
    controller.abort(new Error("cancelled"));
    await expect(cancelled).rejects.toThrow("cancelled");
    expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
  });

  it("accounts growth overlap and variable-size values (VarlenBuilder under a grant)", async () => {
    const governor = new Governor(16 * MiB);
    const grant = await governor.admit({
      label: "concat",
      minimumBytes: 64 * 1024,
      spillable: true,
    });
    const builder = new VarlenBuilder(64, grant, 4096, 1024);
    let peakDuringGrowth = 0;
    for (let index = 0; index < 3; index++) {
      builder.appendBytes(new Uint8Array(900));
      peakDuringGrowth = Math.max(peakDuringGrowth, governor.peakBytes);
    }
    expect(governor.usedBytes).toBe(grant.bytes);
    expect(peakDuringGrowth).toBeGreaterThan(
      grant.bytes - 64 * 1024 + builder.retainedBytes - 4096,
    );
    builder.close();
    expect(bigintBytes(0n)).toBe(24);
    expect(bigintBytes(2n ** 200n)).toBe(16 + 8 * 4);
    grant.close();
    expect(governor.usedBytes).toBe(0);
  });
});
