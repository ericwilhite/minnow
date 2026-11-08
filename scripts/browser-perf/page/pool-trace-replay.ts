/** Isolated pool policy/allocator component over captured real export calls. No IO or decode.
 * Charges precede allocation. Real transfers reconstruct issue/return chronology. Outstanding
 * output buffers are matched by exact size; their values are synthetic. This is not executor
 * throughput, and it does not reproduce MessagePort wall latency.
 */
import { ReturnPool } from "../../../packages/core/src/next/exec/return-pool.js";
import { Governor } from "../../../packages/core/src/next/memory/governor.js";
import { performance } from "node:perf_hooks";
import { strict as assert } from "node:assert";
import { outputSize } from "./output-size.js";
export interface PoolRequestTrace {
  readonly version: 1;
  readonly capBytes: number;
  readonly events: readonly number[];
}
export async function replayPoolTrace(
  trace: PoolRequestTrace,
  verifyBaseline = false,
  capBytes = trace.capBytes,
  bucketed = false,
) {
  if (trace.version !== 1 || trace.events.length % 4 !== 0)
    throw new Error("Invalid request trace");
  const governor = new Governor(256 * 1048576),
    pool = new ReturnPool(governor, capBytes);
  const grant = await governor.admit({
    label: "pool-trace-output",
    minimumBytes: 16 * 1048576,
    spillable: true,
  });
  const outputs = new Map<number, ArrayBuffer[]>(),
    issued = new Map<number, { cap: number; logical: number; buffer: ArrayBuffer }>();
  let requests = 0,
    allocations = 0,
    allocatedBytes = 0,
    checksum = 0,
    slackBytes = 0;
  const baselineRss = process.memoryUsage.rss();
  let peakRss = baselineRss;
  const allocate = (size: number) => {
    if (!grant.allocate(size)) throw new Error("Output charge refused");
    allocations++;
    allocatedBytes += size;
    return new ArrayBuffer(size);
  };
  const before = process.cpuUsage(),
    threadBefore = process.threadCpuUsage(),
    start = performance.now();
  try {
    for (let at = 0; at < trace.events.length;) {
      if (at % 1024 === 0) peakRss = Math.max(peakRss, process.memoryUsage.rss());
      const op = trace.events[at],
        a = trace.events[at + 1] ?? 0,
        b = trace.events[at + 2] ?? 0,
        c = trace.events[at + 3] ?? 0;
      if (op === 0) {
        requests++;
        const capacity = bucketed ? outputSize(a) : a;
        slackBytes += capacity - a;
        if (!grant.allocate(capacity)) throw new Error("Output charge refused");
        const reused = pool.take(capacity);
        if (verifyBaseline && Number(reused !== null) !== b)
          throw new Error(`Trace hit mismatch at ${String(at / 4)}`);
        let buffer: ArrayBuffer;
        if (reused) buffer = reused;
        else {
          allocations++;
          allocatedBytes += capacity;
          buffer = new ArrayBuffer(capacity);
        }
        const view = new Uint8Array(buffer);
        view.fill(requests & 255, 0, a);
        checksum += view[0] ?? 0;
        const list = outputs.get(a) ?? [];
        list.push(buffer);
        outputs.set(a, list);
        at += 4;
      } else if (op === 1) {
        const slots: Array<{ id: number; logical: number; buffer: ArrayBuffer }> = [];
        while (at < trace.events.length && trace.events[at] === 1 && trace.events[at + 3] === c) {
          const id = trace.events[at + 1] ?? -1,
            size = trace.events[at + 2] ?? 0,
            list = outputs.get(size);
          const buffer = list?.shift() ?? allocate(size);
          if (list?.length === 0) outputs.delete(size);
          slots.push({ id, logical: size, buffer });
          at += 4;
        }
        const buffers = slots.map((s) => s.buffer),
          caps = pool.issue(c, buffers),
          client = structuredClone(buffers, { transfer: buffers });
        if (buffers.some((buffer) => buffer.byteLength !== 0))
          throw new Error("Sender did not detach");
        slots.forEach((slot, k) => {
          const buffer = client[k];
          if (!buffer) throw new Error("Missing client slot");
          issued.set(slot.id, { cap: caps[k] ?? -1, logical: slot.logical, buffer });
        });
      } else if (op === 2) {
        const slots: Array<{ id: number; buffer: ArrayBuffer }> = [];
        while (at < trace.events.length && trace.events[at] === 2 && trace.events[at + 3] === c) {
          const id = trace.events[at + 1] ?? -1,
            size = trace.events[at + 2] ?? 0,
            sent = issued.get(id);
          if (!sent || sent.logical !== size) throw new Error("Trace return lacks matching issue");
          issued.delete(id);
          slots.push({ id: sent.cap, buffer: sent.buffer });
          at += 4;
        }
        const transfer = slots.map((s) => s.buffer),
          back = structuredClone(slots, { transfer });
        if (transfer.some((buffer) => buffer.byteLength !== 0))
          throw new Error("Client did not detach");
        for (const slot of back) grant.free(slot.buffer.byteLength);
        pool.release(c, back);
      } else if (op === 3) {
        pool.evict(a);
        at += 4;
      } else throw new Error(`Unknown trace operation ${String(op)}`);
    }
    const wallMs = performance.now() - start,
      cpu = process.cpuUsage(before),
      thread = process.threadCpuUsage(threadBefore);
    return {
      capBytes,
      bucketed,
      slackBytes,
      measuredRssPeakMiB: Math.max(peakRss, process.memoryUsage.rss()) / 1048576,
      measuredRssGrowthMiB: (Math.max(peakRss, process.memoryUsage.rss()) - baselineRss) / 1048576,
      requests,
      hits: pool.hits,
      allocations,
      allocatedBytes,
      accepted: pool.accepted,
      rejected: pool.rejected,
      checksum,
      poolBytes: pool.evictableBytes,
      governorPeak: governor.peakBytes,
      cpuMs: (cpu.user + cpu.system) / 1000,
      threadCpuMs: (thread.user + thread.system) / 1000,
      wallMs,
    };
  } finally {
    for (const list of outputs.values()) for (const b of list) grant.free(b.byteLength);
    for (const slot of issued.values()) grant.free(slot.buffer.byteLength);
    pool.close();
    grant.close();
    assert.equal(governor.usedBytes, 0, "Trace replay charge leak");
  }
}
