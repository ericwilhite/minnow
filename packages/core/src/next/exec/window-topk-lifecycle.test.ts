/** Native bytes stay owned until spill and downstream borrowers settle, even after abort. */
import { afterAll, expect, it } from "vitest";
import { createReach, WINDOW_TOPK_LIFECYCLE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, ValueTooLargeError } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { INT, DBL, TEXT, BIG, CollectSink, batchesOf, type Row } from "./join-fixtures.js";
import type { Operator } from "./pipeline.js";
import { WindowOperator, windowMinimumBytes } from "./window.js";
const reach = createReach("bounded ranking borrowers", WINDOW_TOPK_LIFECYCLE_REACH);
afterAll(() => reach.assertReached());
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
for (const seed of seedsFor("next-window-topk-borrow", [0xb730, 0xb731]))
  for (const mode of ["append", "read", "consumer"] as const)
    it(`ranking drains ${mode} on cancellation (${String(seed)})`, async () => {
      reach.beginHistory();
      const governor = new Governor(64 << 20),
        spill = new MemorySpillService(),
        types = [INT, DBL, TEXT];
      const busy = await governor.admit({ label: "competitor", minimumBytes: 0, spillable: true });
      const grant = await governor.admit({
        label: "bounded window",
        minimumBytes: windowMinimumBytes(types),
        spillable: true,
      });
      const entered = deferred(),
        release = deferred();
      let armed = mode === "append",
        gated = false;
      const create = spill.create.bind(spill);
      spill.create = async (...args) => {
        const file = await create(...args),
          append = file.append.bind(file),
          read = file.read.bind(file);
        file.append = async (page) => {
          if (armed && !gated && mode === "append") {
            gated = true;
            entered.resolve();
            await release.promise;
          }
          return append(page);
        };
        file.read = async (ref) => {
          if (armed && !gated && mode === "read") {
            gated = true;
            entered.resolve();
            await release.promise;
          }
          return read(ref);
        };
        return file;
      };
      const collected = new CollectSink();
      let finishes = 0;
      const sink: Operator =
        mode === "consumer"
          ? {
              push(batch) {
                collected.push(batch);
                if (!gated) {
                  gated = true;
                  entered.resolve();
                  return "blocked";
                }
                return "ready";
              },
              ready: () => release.promise,
              finish: () => {
                finishes++;
              },
              abort: () => undefined,
            }
          : collected;
      const cancel = new AbortController();
      const actor = new WindowOperator(
        {
          topK: 2,
          types,
          partitionBy: [0],
          orderBy: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
          functions: [{ fn: "rank", arg: null, frame: null, type: BIG }],
        },
        { grant, spill, queryId: seed, signal: cancel.signal },
        sink,
      );
      if (mode !== "consumer")
        expect(busy.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes)).toBe(
          true,
        );
      const rows: Row[] = Array.from({ length: mode === "consumer" ? 37 : 10000 }, (_, i) => [
        i % 3,
        seed % 2 === 0 ? 0 : null,
        "x".repeat(512),
      ]);
      const feed = async () => {
        for (const batch of batchesOf(types, rows, { batchRows: 128 }))
          if (actor.push(batch) === "blocked") await actor.ready();
      };
      const work = (async () => {
        await feed();
        if (mode === "read") {
          expect(actor.profile.sort.runs).toBeGreaterThan(0);
          armed = true;
        }
        await actor.finish();
      })();
      void work.catch(() => undefined);
      try {
        await Promise.race([
          entered.promise,
          work.then(() => {
            throw new Error("expected an actual borrower");
          }),
        ]);
        const borrowedBytes = grant.allocated;
        const reason = new Error("cancel while borrowed");
        cancel.abort(reason);
        actor.abort(reason);
        let closed = false;
        void actor.closed.then(
          () => {
            closed = true;
          },
          () => {
            closed = true;
          },
        );
        await new Promise<void>((done) => setImmediate(done));
        expect(grant.held, "grant still owns borrowed native bytes").toBeGreaterThan(0);
        expect(grant.allocated, "borrowed buffers stay charged").toBeGreaterThan(0);
        expect(grant.allocated, "a pending borrower prevents freeing its native buffers").toBe(
          borrowedBytes,
        );
        expect(closed, "closed must drain its borrower").toBe(false);
        reach.reach("charged until settlement");
        reach.reach("closed waits");
        reach.reach(mode);
        if (mode === "read") reach.reach("all peers spill");
        release.resolve();
        await expect(work).rejects.toBe(reason);
        await actor.closed;
        expect(finishes).toBe(0);
      } finally {
        release.resolve();
        await work.catch(() => undefined);
        actor.abort(new Error("cleanup"));
        await actor.closed;
        busy.close();
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
        reach.reach("zero resources");
      }
    }, 30000);

// Main reserves the complete merge/held-row step before borrowing. Preserve the original
// five 600k TEXT values: 64 MiB must refuse its oversized legacy step; 128 MiB can queue it.
for (const seed of seedsFor("next-window-topk-loan-cancel", [0xb732, 0xb733]))
  for (const capMiB of [64, 128])
    it(`wide-step refusal or queued abort without caller signal (${String(seed)}, ${String(capMiB)} MiB)`, async () => {
      reach.beginHistory();
      const governor = new Governor(capMiB << 20),
        spill = new MemorySpillService(),
        types = [INT, DBL, TEXT, TEXT, TEXT, TEXT, TEXT];
      const busy = await governor.admit({
          label: "ordinary competitor",
          minimumBytes: 0,
          spillable: true,
        }),
        holder = await governor.admit({
          label: "outstanding foreign step",
          minimumBytes: 0,
          spillable: true,
        }),
        native = await governor.admit({
          label: "window",
          minimumBytes: windowMinimumBytes(types),
          spillable: true,
        });
      const entered = deferred();
      let requested = 0;
      const grant = new Proxy(native, {
        get(target, key) {
          if (key === "borrow")
            return (bytes: number, signal?: AbortSignal) => {
              requested = bytes;
              entered.resolve();
              return target.borrow(bytes, signal);
            };
          const value: unknown = Reflect.get(target, key, target);
          return typeof value === "function" ? (value.bind(target) as unknown) : value;
        },
      });
      const actor = new WindowOperator(
        {
          topK: 2,
          types,
          partitionBy: [0],
          orderBy: [{ column: 1, direction: "asc", nulls: "last", collation: null }],
          functions: [{ fn: "rank", arg: null, frame: null, type: BIG }],
        },
        { grant, spill, queryId: seed },
        new CollectSink(),
      );
      expect(busy.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes)).toBe(
        true,
      );
      const loan = await holder.borrow(governor.reserveBytes);
      expect(holder.allocate(loan.bytes)).toBe(true);
      const work = (async () => {
        for (const batch of batchesOf(
          types,
          [
            [1, 0, ...Array.from({ length: 5 }, () => "x".repeat(600000))],
            [1, 0, ...Array.from({ length: 5 }, () => "y".repeat(600000))],
          ],
          { batchRows: 1 },
        ))
          if (actor.push(batch) === "blocked") await actor.ready();
        await actor.finish();
      })();
      let settled = false;
      void work.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );
      try {
        await Promise.race([
          entered.promise,
          work.then(() => {
            throw new Error("expected queued legacy progress");
          }),
        ]);
        expect(requested).toBeGreaterThan(0);
        if (capMiB === 64) {
          expect(requested).toBeGreaterThan(governor.reserveBytes);
          await expect(work).rejects.toBeInstanceOf(ValueTooLargeError);
          await expect(work).rejects.toMatchObject({ code: "MINNOW_VALUE_TOO_LARGE" });
          reach.reach("complete legacy step refused");
          return;
        }
        expect(requested).toBeLessThanOrEqual(governor.reserveBytes);
        const reason = new Error("cancel queued native loan");
        actor.abort(reason);
        await new Promise<void>((done) => setImmediate(done));
        expect(settled, "native abort must cancel its queued loan").toBe(true);
        await expect(work).rejects.toBe(reason);
        await actor.closed;
        reach.reach("queued loan cancellation");
      } finally {
        holder.free(loan.bytes);
        loan.repay();
        holder.close();
        await work.catch(() => undefined);
        actor.abort(new Error("cleanup"));
        await actor.closed;
        busy.close();
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
        reach.reach("zero resources");
      }
    }, 30000);
