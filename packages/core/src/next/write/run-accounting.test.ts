/* eslint-disable no-restricted-imports -- compare complete native stored bytes without per-byte assertion objects. */
/** Governor rule 1 and ExtentBuilder.append: prepay capacities/overlap, retain until consumption. */
import { Buffer } from "node:buffer";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RUN_ACCOUNTING_STATES } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MetadataBudget } from "./budget.js";
import { KeyDelta, type DeltaVisitor } from "./key-hash.js";
import { MemtableMemoryError, type Charge } from "./bytes.js";
import { RunEncoder, RunBuilder, type EncodedRun } from "./runs.js";

const RUN_BUFFER_OVERHEAD = 192; // three governor object units per owned native buffer
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const reach = createReach("encoder native ownership", WRITE_RUN_ACCOUNTING_STATES);
// The optional extra argument is intentionally accepted by the old constructor too: RED runs
// against the parent without requiring a changed implementation or a compilation failure.
const Builder = RunBuilder as new (drop: boolean, bulk: boolean, budget: Charge) => RunBuilder;
function key(i: number, width: number): Uint8Array {
  const out = new Uint8Array(width).fill(0x81);
  new DataView(out.buffer).setUint32(0, i);
  return out;
}
function owned(run: EncodedRun): number {
  return [...run.parts, run.minKey, run.maxKey].reduce(
    (sum, part) => sum + part.buffer.byteLength + RUN_BUFFER_OVERHEAD,
    0,
  );
}

it.each([false, true])("refuses before the first native allocation (bulk=%s)", (bulk) => {
  let calls = 0;
  const budget = {
    allocate: () => {
      calls++;
      return false;
    },
    free: () => undefined,
  };
  expect(() => new Builder(false, bulk, budget)).toThrow(MemtableMemoryError);
  expect(calls).toBe(1);
});

for (const seed of seedsFor("next-write-run-accounting", [0xb5a1]))
  it(`keeps output charged through delivery, denial and retry (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          bulk: fc.boolean(),
          count: fc.constantFrom(256, 2048, 8192),
          width: fc.constantFrom(5, 129, 2048),
          failure: fc.constantFrom("none", "budget", "native", "visitor", "drain"),
          failAt: fc.integer({ min: 1, max: 20 }),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.bulk ? "bulk" : "ordinary");
          let inputPaid = 0;
          const delta = new KeyDelta({
            allocate(n) {
              inputPaid += n;
              return true;
            },
            free(n) {
              inputPaid -= n;
            },
          });
          const count = s.width === 2048 ? 256 : s.count;
          for (let i = 0; i < count; i++) {
            const bytes = key(i, s.width);
            delta.add(bytes, 0, bytes.length, i + 1, 1, false);
          }
          const reference = new RunBuilder(false, s.bulk);
          const feed = async (
            builder: RunBuilder,
            visit?: () => void,
            flush?: () => Promise<void>,
          ) => {
            const emit: DeltaVisitor = (a, from, to, row, tombstone, hash, bloomHash) => {
              builder.addRange(a, from, to, row, tombstone, hash, bloomHash);
              visit?.();
            };
            if (s.bulk)
              emit.singles = (data, from, to, bound, keep) => {
                const end = builder.addSingleBatch(data, from, to, bound, keep);
                visit?.();
                return end;
              };
            if (flush !== undefined) emit.flush = flush;
            await delta.forEachSorted(1, emit);
          };
          await feed(reference);
          const expected = reference.finish();
          expect(expected).not.toBeNull();
          let paid = 0,
            allocations = 0,
            token = 0,
            native = 0;
          let armed = s.failure !== "none";
          const fault = { stopped: false };
          let peak = 0;
          const injected = new Error("encoder stopped");
          const budget: Charge = {
            allocate(bytes) {
              allocations++;
              if (armed && s.failure === "budget" && allocations === s.failAt) {
                fault.stopped = true;
                reach.reach("budget denial");
                return false;
              }
              paid += bytes;
              token = bytes;
              peak = Math.max(peak, paid);
              return true;
            },
            free(bytes) {
              paid -= bytes;
              expect(paid, "no duplicate refund").toBeGreaterThanOrEqual(0);
            },
          };
          const Native = Uint8Array;
          // Constructor observation is independent of the encoder's buffer registry. A missing
          // precharge fails here, before a native allocation (including new resize storage).
          vi.stubGlobal(
            "Uint8Array",
            new Proxy(Native, {
              construct(target, args) {
                const capacity = typeof args[0] === "number" ? args[0] : 0;
                if (capacity > 0 && new Error().stack?.includes("/write/")) {
                  expect(token, "native capacity prepaid before construction").toBe(
                    capacity + RUN_BUFFER_OVERHEAD,
                  );
                  token = 0;
                  native++;
                  if (armed && s.failure === "native" && native === s.failAt) {
                    fault.stopped = true;
                    reach.reach("native failure");
                    throw injected;
                  }
                }
                return Reflect.construct(target, args, target) as object;
              },
            }),
          );
          const attempt = async () => {
            const builder = new Builder(false, s.bulk, budget);
            try {
              await feed(
                builder,
                () => {
                  if (armed && s.failure === "visitor") {
                    fault.stopped = true;
                    reach.reach("abort");
                    throw injected;
                  }
                },
                () => {
                  if (armed && s.failure === "drain") {
                    fault.stopped = true;
                    reach.reach("drain rejection");
                    return Promise.reject(injected);
                  }
                  return Promise.resolve();
                },
              );
              return builder.finish();
            } finally {
              builder.close();
            }
          };
          try {
            let actual: EncodedRun | null = null;
            try {
              actual = await attempt();
            } catch (error) {
              if (!fault.stopped) throw error;
              expect(error instanceof MemtableMemoryError || error === injected).toBe(true);
              expect(paid, "abort refunds scratch and already emitted output").toBe(0);
              armed = false;
              reach.reach("retry");
              if (native < (s.bulk ? 6 : 8)) reach.reach("constructor refund");
              actual = await attempt();
            }
            expect(actual).not.toBeNull();
            if (actual === null || expected === null) throw new Error("missing run");
            expect(actual.entryCount).toBe(expected.entryCount);
            expect(actual.parts.length).toBe(expected.parts.length);
            for (let i = 0; i < actual.parts.length; i++)
              expect(
                Buffer.from(actual.parts[i] ?? []).equals(Buffer.from(expected.parts[i] ?? [])),
              ).toBe(true);
            expect(native).toBeGreaterThan(0);
            expect(paid, "finish transfers charged ownership; it does not free output").toBe(
              owned(actual),
            );
            expect(peak).toBeGreaterThan(paid);
            reach.reach("retained output");
            if (s.count === 8192 && s.width === 129) reach.reach("bloom partition");
            actual.release();
            actual.release();
            expect(paid, "consumer settlement refunds all output").toBe(0);
            reach.reach("consumed output");
            expected.release();
          } finally {
            vi.unstubAllGlobals();
            delta.releaseCharge();
            expect(inputPaid, "rejected drains close retained input traversals").toBe(0);
          }
        },
      ),
      { seed, numRuns: 160 },
    );
  }, 120_000);

it.each([false, true])(
  "pays hash resize overlap with the old array still live (bulk=%s)",
  async (bulk) => {
    let paid = 0,
      base = 0,
      grew = false;
    const parts: Uint8Array[] = [];
    const budget = {
      allocate(n: number) {
        if (n === 16384 + RUN_BUFFER_OVERHEAD) {
          // At the first hash resize, the writers have not grown (five-byte keys). Bulk may
          // additionally own the block it is currently filling, before emit observes it.
          const output = parts.reduce(
            (sum, p) => sum + p.buffer.byteLength + RUN_BUFFER_OVERHEAD,
            0,
          );
          expect(
            paid,
            "old hashes remain charged until the copy installs their replacement",
          ).toBeGreaterThanOrEqual(base + output + 5 + RUN_BUFFER_OVERHEAD);
          grew = true;
        }
        paid += n;
        return true;
      },
      free(n: number) {
        paid -= n;
      },
    };
    const encoder = new RunEncoder((p) => parts.push(p), false, bulk, budget);
    base = paid;
    const delta = new KeyDelta();
    try {
      for (let i = 0; i < 2048; i++) {
        const bytes = key(i, 5);
        delta.add(bytes, 0, bytes.length, i, 1, false);
      }
      const visit: DeltaVisitor = (a, f, t, row, tombstone, h, b) =>
        encoder.add(a, f, t, row, tombstone, h, b);
      if (bulk)
        visit.singles = (data, f, t, bound, keep) =>
          encoder.addSingleBatch(data, f, t, bound, keep);
      await delta.forEachSorted(1, visit);
      const run = encoder.finish();
      expect(grew).toBe(true);
      encoder.close();
      expect(paid).toBeGreaterThan(0);
      run?.release();
      expect(paid).toBe(0);
    } finally {
      encoder.close();
      delta.releaseCharge();
    }
  },
);

for (const seed of seedsFor("next-write-run-stream-budget", [0xb5a2]))
  it(`streams output larger than its constrained budget (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 4096, max: 8192 }), async (bulk, count) => {
        reach.beginHistory();
        let paid = 0,
          peak = 0,
          delivered = 0,
          waits = 0;
        const cap = 256 * 1024;
        const budget = {
          allocate(n: number) {
            if (paid + n > cap) return false;
            paid += n;
            peak = Math.max(peak, paid);
            return true;
          },
          free(n: number) {
            paid -= n;
          },
        };
        const delta = new KeyDelta();
        for (let i = 0; i < count; i++) {
          const bytes = key(i, 129);
          delta.add(bytes, 0, bytes.length, i, 1, false);
        }
        const builder = new RunBuilder(false, bulk, budget);
        const drain = async () => {
          const parts = builder.takeParts();
          const held = paid;
          // A semantic delivery barrier, not a timer. No buffer is released before the
          // simulated append acknowledges consumption, even across another microtask.
          await Promise.resolve();
          expect(paid).toBe(held);
          for (const part of parts) {
            delivered += part.length;
            builder.releasePart(part);
          }
          waits++;
        };
        let run: EncodedRun | null = null;
        try {
          const visit: DeltaVisitor = (a, f, t, row, tombstone, h, b) =>
            builder.addRange(a, f, t, row, tombstone, h, b);
          if (bulk)
            visit.singles = (data, f, t, bound, keep) =>
              builder.addSingleBatch(data, f, t, bound, keep);
          visit.flush = drain;
          await delta.forEachSorted(1, visit);
          run = builder.finish();
          await drain();
          expect(delivered).toBeGreaterThan(cap);
          expect(peak).toBeLessThanOrEqual(cap);
          expect(waits).toBeGreaterThan(1);
          expect(paid).toBe(2 * (129 + RUN_BUFFER_OVERHEAD));
          reach.reach("bounded stream");
          reach.reach("acknowledged consumption");
        } finally {
          run?.release();
          builder.close();
          delta.releaseCharge();
        }
        expect(paid).toBe(0);
      }),
      { seed, numRuns: 60 },
    );
  }, 120_000);

for (const seed of seedsFor("next-write-run-pressure", [0xb5a3]))
  it(`waits cancellably after unwinding below-cap encoding (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.boolean(), async (cancel, bulk) => {
        reach.beginHistory();
        const governor = new Governor(8 * 1024 * 1024);
        const grant = await governor.admit({
          label: "encoder",
          minimumBytes: 32768,
          spillable: true,
        });
        const holder = await governor.admit({
          label: "reader",
          minimumBytes: governor.capBytes - governor.reserveBytes - grant.held,
          spillable: false,
        });
        const stop = new AbortController(),
          stopped = new Error("encoding cancelled");
        const waiting = barrier();
        let attempts = 0;
        const budget = new MetadataBudget(
          grant,
          () => 0,
          governor.capBytes,
          async (wanted) => {
            waiting.resolve();
            const credit = await governor.admit({
              label: "encoder growth",
              minimumBytes: Math.max(wanted - grant.held, 65536),
              spillable: false,
              signal: stop.signal,
            });
            try {
              expect(governor.transferCredit(credit, grant, credit.held)).toBe(true);
            } finally {
              credit.close();
            }
          },
        );
        const delta = new KeyDelta();
        for (let i = 0; i < 512; i++) {
          const bytes = key(i, 129);
          delta.add(bytes, 0, bytes.length, i, 1, false);
        }
        const work = async () => {
          attempts++;
          const builder = new RunBuilder(false, bulk, budget);
          try {
            const visit: DeltaVisitor = (a, f, t, row, tombstone, h, b) =>
              builder.addRange(a, f, t, row, tombstone, h, b);
            if (bulk)
              visit.singles = (data, f, t, bound, keep) =>
                builder.addSingleBatch(data, f, t, bound, keep);
            await delta.forEachSorted(1, visit);
            return builder.finish();
          } finally {
            builder.close();
          }
        };
        const pending = Promise.resolve(budget.retry(work, () => stop.signal.throwIfAborted()));
        try {
          expect(
            await Promise.race([waiting.promise.then(() => true), pending.then(() => false)]),
          ).toBe(true);
          expect(grant.allocated, "no provisional buffers held while waiting").toBe(0);
          reach.reach("pressure wait");
          if (cancel) {
            stop.abort(stopped);
            await expect(pending).rejects.toBe(stopped);
            reach.reach("pressure cancellation");
          } else {
            holder.close();
            const run = await pending;
            expect(run?.entryCount).toBe(512);
            expect(attempts).toBeGreaterThan(1);
            expect(grant.allocated).toBeGreaterThan(0);
            run?.release();
            reach.reach("pressure resume");
          }
          expect(grant.allocated).toBe(0);
        } finally {
          holder.close();
          grant.close();
          delta.releaseCharge();
        }
        expect(governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 20 },
    );
  });

it("reaches the declared encoder ownership states", () => reach.assertReached());
