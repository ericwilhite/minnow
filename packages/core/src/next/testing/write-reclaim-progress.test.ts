/** Governor rules4/7: zero-progress reclaim cannot recursively reschedule the same drain. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RECLAIM_PROGRESS_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { WriteCredits } from "../write/credits.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
  watchGrants,
} from "../write/test-support.js";

const MiB = 1 << 20;
for (const seed of seedsFor("next-write-reclaim-progress", [47235, 47236]))
  it(`waits or cancels without zero-progress reclaim recursion (seed ${String(seed)})`, async () => {
    const reach = createReach("native reclaim progress", WRITE_RECLAIM_PROGRESS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(32, 48, 64),
        fc.integer({ min: 1, max: 8 }),
        fc.boolean(),
        async (cap, units, cancel) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", 128, undefined, { merges: { mode: "off" } });
          Object.defineProperty(h, "governor", { value: new Governor(cap * MiB) });
          const stop = new AbortController(),
            reason = new Error("cancel blocked native reader");
          let credits: WriteCredits | undefined,
            holder: Grant | undefined,
            reader: Grant | undefined;
          const buffers: { head?: Uint8Array; tail?: Uint8Array; output?: Uint8Array } = {};
          let armed = false,
            callbacks = 0,
            closed = false,
            closing: Promise<void> | undefined,
            pending: Promise<void> | undefined;
          const state: { settled: boolean; error: unknown } = { settled: false, error: null };
          const open = WriteCredits.open.bind(WriteCredits);
          const capture = vi
            .spyOn(WriteCredits, "open")
            .mockImplementation(async (...args) => (credits = await open(...args)));
          watchGrants(h.governor, (grant) => {
            if (grant.label !== "write path metadata") return;
            const register = grant.onReclaimRequest.bind(grant);
            grant.onReclaimRequest = (callback) =>
              register((bytes) => {
                if (armed && ++callbacks > 16)
                  throw new Error("Zero-progress metadata reclaim rescheduled its own drain");
                return callback(bytes);
              });
          });
          const releaseHolder = () => {
            if (buffers.head) expect(buffers.head.at(-1)).toBe(41);
            if (buffers.tail) expect(buffers.tail.at(-1)).toBe(43);
            delete buffers.head;
            delete buffers.tail;
            holder?.free(holder.allocated);
            holder?.close();
            holder = undefined;
          };
          const close = () =>
            (closing ??= (async () => {
              armed = false;
              stop.abort(reason);
              releaseHolder();
              if (pending) await pending;
              delete buffers.output;
              reader?.free(reader.allocated);
              reader?.close();
              reader = undefined;
              capture.mockRestore();
              if (!closed) {
                closed = true;
                await h.close();
              }
            })());
          onTestFinished(close);
          try {
            const path = await h.open();
            await autocommit(path, (s) =>
              s.changeCatalog({
                kind: "createTable",
                table: tableInput("native_wait", [column("id", INT)], { primaryKey: null }),
              }),
            );
            await autocommit(path, (s) => s.table(1).insert(batch(ints([0, 1])), null));
            await path.quiesce();
            if (!credits) throw new Error("Missing native credits");
            credits.reclaimIdleForecasts();
            const free = h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes,
              delta = units * 65536;
            expect(free).toBeGreaterThan(delta);
            holder = await h.governor.admit({
              label: "active competing read",
              minimumBytes: free,
              spillable: false,
            });
            expect(holder.allocate(free)).toBe(true);
            buffers.head = new Uint8Array(delta).fill(41);
            buffers.tail = new Uint8Array(free - delta).fill(43);
            reach.reach("competitor owns real buffers");
            armed = true;
            pending = h.governor
              .admitGroup({
                label: "fitting reader waits for active holder",
                grants: [
                  {
                    label: "native reader",
                    minimumBytes: h.governor.reserveBytes + delta,
                    spillable: false,
                  },
                ],
                includesProgress: true,
                signal: stop.signal,
              })
              .then(
                (grants) => {
                  reader = grants[0];
                  state.settled = true;
                },
                (error: unknown) => {
                  state.error = error;
                  state.settled = true;
                },
              );
            for (let turn = 0; turn < 8; turn++) await Promise.resolve();
            expect(
              state.error,
              "idle reclaim must not recursively reschedule a drain without returning credit",
            ).toBeNull();
            expect(state.settled).toBe(false);
            expect(callbacks).toBeGreaterThan(0);
            expect(callbacks).toBeLessThanOrEqual(16);
            reach.reach("zero-progress callback returns");
            if (cancel) {
              stop.abort(reason);
              await pending;
              expect(state.error).toBe(reason);
              reach.reach("waiting admission cancelled");
            } else {
              expect(buffers.head.at(-1)).toBe(41);
              delete buffers.head;
              holder.free(delta);
              holder.release(delta);
              await pending;
              expect(state.error).toBeNull();
              expect(reader?.allocate(4096)).toBe(true);
              buffers.output = new Uint8Array(4096).fill(47);
              expect(buffers.output.at(-1)).toBe(47);
              reach.reach("waiting admission completed");
            }
            armed = false;
            releaseHolder();
            const pin = path.pin();
            try {
              expect(await rowsOf(pin, 1)).toEqual([[0], [1]]);
            } finally {
              pin.release();
            }
            reach.reach("exact native rows");
            reach.reach(cap === 32 ? "32 MiB" : cap === 48 ? "48 MiB" : "64 MiB");
          } finally {
            await close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 14,
        examples: [
          [32, 1, false],
          [32, 8, true],
          [48, 4, false],
          [64, 8, true],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
