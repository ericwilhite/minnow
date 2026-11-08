/** Shared cold fills are reclaimable cache work, never a private per-cursor directory. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RANGE_NAVIGATION_STATES } from "../../testing/reach.js";
import { taskClock, beginTask } from "../exec/pipeline.js";
import { ByteReader, need } from "./bytes.js";
import { Harness } from "./test-support.js";
import { RunBuilder, runRef } from "./runs.js";
import { RunRangeInput, rangeRunBytes } from "./range-input.js";
import { DirectoryNavigation, withDirectory } from "./range-navigation.js";

const MiB = 1024 * 1024;
const reach = createReach("shared range metadata lifecycle", WRITE_RANGE_NAVIGATION_STATES);
async function fixture(rows: number) {
  const h = new Harness("strict", "memory");
  await h.open();
  const writer = h.space.beginWrite("shared directory lifecycle"),
    builder = new RunBuilder(),
    key = new Uint8Array(64),
    view = new DataView(key.buffer);
  for (let k = 0; k < rows; k++) {
    view.setUint32(0, k, false);
    builder.add(key, k + 1, false);
  }
  const encoded = need(builder.finish()),
    written = await writer.write(encoded.parts),
    ref = runRef(encoded, written.blobs),
    lastBlock = need(written.blobs.at(-5)).offset;
  return {
    h,
    writer,
    ref,
    lastBlock,
    key: (k: number) => {
      const b = new Uint8Array(64);
      new DataView(b.buffer).setUint32(0, k, false);
      return b;
    },
  };
}
for (const seed of seedsFor("next-write-range-navigation", [0xb3d0]))
  it(`shares a cold fill and isolates subscriber cancellation (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          rows: fc.constantFrom(16384, 32768),
          cancel: fc.constantFrom("owner", "follower", "neither"),
          phase: fc.constantFrom("before parse", "during parse"),
        }),
        async (s) => {
          reach.beginHistory();
          const { h, writer, ref, lastBlock } = await fixture(s.rows),
            a = new AbortController(),
            b = new AbortController();
          const first = await h.governor.admit({
              label: "first navigation",
              minimumBytes: 2 * MiB,
              spillable: false,
            }),
            second = await h.governor.admit({
              label: "second navigation",
              minimumBytes: 2 * MiB,
              spillable: false,
            });
          let arrived!: () => void, release!: () => void;
          const arrival = new Promise<void>((r) => {
              arrived = r;
            }),
            gate = new Promise<void>((r) => {
              release = r;
            });
          const read = h.space.read.bind(h.space);
          let reads = 0;
          const spy = vi.spyOn(h.space, "read").mockImplementation(async (refs, options) => {
            reads++;
            const lease = await read(refs, options);
            arrived();
            await gate;
            return lease;
          });
          const oldClock = taskClock.now;
          let time = 0,
            finished = false,
            partial = false;
          // eslint-disable-next-line require-yield -- a ready cache borrow is intentionally synchronous.
          const use = function* (bytes: Uint8Array, positions: Uint32Array | null, count: number) {
            if (positions === null) throw new Error("fixture must admit its shared index");
            const r = new ByteReader(bytes, positions[count - 1]);
            return r.varuint();
          };
          try {
            taskClock.now = () => (time += 2);
            beginTask();
            const p = Promise.resolve(
              withDirectory(h.space, ref.entries, "entries", first, a.signal, use),
            );
            const caught = p.catch((error: unknown) => error);
            await arrival;
            const q = Promise.resolve(
              withDirectory(h.space, ref.entries, "entries", second, b.signal, use),
            );
            const caughtQ = q.catch((error: unknown) => error);
            reach.reach("concurrent followers");
            const reason = new Error("subscriber cancelled");
            let cancelled = false;
            const cancel = () => {
              if (cancelled || s.cancel === "neither") return;
              cancelled = true;
              if (s.cancel === "owner") a.abort(reason);
              else b.abort(reason);
            };
            if (s.phase === "before parse") {
              cancel();
              if (s.cancel === "follower") {
                expect(await caughtQ).toBe(reason);
                expect(first.allocated).toBeGreaterThan(0);
              }
            }
            const tick = () => {
              const state = h.space.parsed?.(ref.entries);
              if (
                state instanceof DirectoryNavigation &&
                state.next > 0 &&
                state.next < state.count
              ) {
                partial = true;
                if (s.phase === "during parse") cancel();
              }
              if (!finished) setImmediate(tick);
            };
            setImmediate(tick);
            release();
            const x = await caught,
              y = await caughtQ;
            finished = true;
            if (s.cancel === "owner") {
              expect(x).toBe(reason);
              expect(y).toBe(lastBlock);
              if (s.phase === "during parse") reach.reach("resumed partial fill");
              expect(reads).toBe(2);
              reach.reach("owner cancellation retry");
            } else {
              expect(x).toBe(lastBlock);
              expect(reads).toBe(1);
              if (s.cancel !== "follower") expect(y).toBe(x);
              else {
                expect(y).toBe(reason);
                reach.reach("follower cancellation");
              }
            }
            expect(partial).toBe(true);
            reach.reach("timer during cold fill");
            expect(first.allocated).toBe(0);
            expect(second.allocated).toBe(0);
            reach.reach("released I/O holds");
            const navigation = h.space.parsed?.(ref.entries);
            expect(navigation).toBeInstanceOf(DirectoryNavigation);
            expect(navigation instanceof DirectoryNavigation && navigation.ready).toBe(true);
            reach.reach("shared ready descriptor");
          } finally {
            finished = true;
            release();
            taskClock.now = oldClock;
            beginTask();
            spy.mockRestore();
            first.close();
            second.close();
            await writer.abort();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 24 },
    );
  }, 120000);

for (const seed of seedsFor("next-write-range-navigation-pressure", [0xb3d1]))
  it(`uses scalar navigation when optional cache admission is refused (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({ rows: fc.constantFrom(1024, 16384), descending: fc.boolean() }),
        async (s) => {
          reach.beginHistory();
          const { h, writer, ref, key } = await fixture(s.rows);
          const grant = await h.governor.admit({
            label: "bounded navigation at cap",
            minimumBytes: 2 * MiB + 128 * 1024,
            spillable: false,
          });
          const direction = s.descending ? -1 : 1;
          expect(grant.allocate(rangeRunBytes(ref, direction))).toBe(true);
          const held = need(
            h.governor.tryAdmit({
              label: "pressure holder",
              minimumBytes: h.governor.capBytes - h.governor.reserveBytes - h.governor.usedBytes,
              spillable: false,
            }),
          );
          const stop = new AbortController(),
            waiting = h.governor
              .admit({
                label: "queued admission prevents cache growth",
                minimumBytes: MiB,
                spillable: false,
                signal: stop.signal,
              })
              .catch(() => null);
          const input = new RunRangeInput(
            h.space,
            grant,
            ref,
            {
              lower: { kind: "atLeast", key: key(s.rows / 2) },
              upper: { kind: "through", key: key(s.rows / 2 + 31) },
            },
            direction,
            Infinity,
            new AbortController().signal,
          );
          try {
            await input.open();
            expect(await input.advance()).toBe(true);
            expect(input.rowId).toBe(s.rows / 2 + (s.descending ? 32 : 1));
            expect(h.space.parsed?.(ref.entries) ?? null).toBeNull();
            expect(h.space.parsed?.(ref.fences) ?? null).toBeNull();
            reach.reach("optional cache refusal");
            reach.reach(s.descending ? "pressure descending" : "pressure ascending");
            expect(h.governor.peakBytes).toBeLessThanOrEqual(h.governor.capBytes);
          } finally {
            input.close();
            stop.abort();
            (await waiting)?.close();
            held.close();
            grant.close();
            await writer.abort();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 8 },
    );
  }, 120000);
it("reaches shared navigation lifecycle states", () => reach.assertReached());
