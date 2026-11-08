/** One owner can repartition held credit without a gap in its complete reservation. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_CREDIT_STATES,
  NATIVE_CREDIT_CACHE_STATES,
} from "../../testing/reach.js";
import { Governor, type Grant, type Loan, type NativeGrantGroupOptions } from "./governor.js";

const MiB = 1048576;
const reach = createReach("native credit ownership", NATIVE_CREDIT_STATES);
const retained = createReach("native credit after retained takeover", NATIVE_CREDIT_CACHE_STATES);
let nativeCacheSeen = false;
const turns = async () => {
  for (let n = 0; n < 8; n++) await Promise.resolve();
};
/** Parent RED runs its existing sequential admissions, never a missing-method failure. */
function groupFor(governor: Governor, options: NativeGrantGroupOptions): Promise<readonly Grant[]> {
  if (typeof governor.admitGroup === "function") return governor.admitGroup(options);
  const partial: Grant[] = [];
  return Promise.all(
    options.grants.map(async (part) => {
      const grant = await governor.admit({
        ...part,
        ...(options.signal === undefined ? {} : { signal: options.signal }),
      });
      partial.push(grant);
      if (options.signal?.aborted) grant.close();
      return grant;
    }),
  ).catch((error: unknown) => {
    for (const grant of partial) grant.close();
    throw error;
  });
}
interface Live {
  grant: Grant;
  held: number;
  loan: Loan | null;
  closed: boolean;
  buffers: Set<Uint8Array>;
}

for (const seed of seedsFor("next-native-credit", [0xc2ed]))
  it(`keeps independent buffer and credit ledgers exact (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          minima: fc.array(fc.integer({ min: 64, max: 8192 }), { minLength: 2, maxLength: 5 }),
          actions: fc.array(
            fc.record({
              op: fc.constantFrom(
                "allocate",
                "free",
                "transfer",
                "release",
                "take",
                "cache",
                "close",
              ),
              from: fc.nat(),
              to: fc.nat(),
              bytes: fc.integer({ min: 0, max: 8192 }),
            }),
            { minLength: 40, maxLength: 80 },
          ),
          reverseClose: fc.boolean(),
        }),
        async (s) => {
          reach.beginHistory();
          retained.beginHistory();
          const governor = new Governor(4 * MiB),
            foreign = new Governor(4 * MiB),
            other = await foreign.admit({
              label: "another owner",
              minimumBytes: 64,
              spillable: false,
            }),
            grants = await governor.admitGroup({
              label: "complete native group",
              grants: s.minima.map((minimumBytes, n) => ({
                label: `part ${String(n)}`,
                minimumBytes,
                spillable: false,
              })),
            }),
            live: Live[] = grants.map((grant, n) => ({
              grant,
              held: s.minima[n] ?? 0,
              loan: null,
              closed: false,
              buffers: new Set(),
            })),
            cache = governor.registerCache({ evictableBytes: 0, evict: () => 0 }) as Omit<
              ReturnType<Governor["registerCache"]>,
              "takeFrom"
            > & {
              takeFrom?: (source: unknown, bytes: number) => boolean;
            },
            cached = new Set<Uint8Array>();
          const allocated = (entry: Live) =>
            [...entry.buffers].reduce((n, b) => n + b.byteLength, 0);
          const cachedBytes = () => [...cached].reduce((n, b) => n + b.byteLength, 0);
          const check = () => {
            for (const entry of live) {
              expect(entry.grant.held).toBe(entry.held);
              expect(entry.grant.allocated).toBe(allocated(entry));
              expect(entry.grant.loaned).toBe(entry.loan?.bytes ?? 0);
            }
            expect(governor.usedBytes).toBe(live.reduce((n, e) => n + e.held, 0) + cachedBytes());
            expect(governor.usedBytes).toBeLessThanOrEqual(governor.capBytes);
            expect(foreign.usedBytes).toBe(64);
          };
          const allocate = (entry: Live, bytes: number) => {
            const fits = !entry.closed && bytes <= entry.held - allocated(entry);
            expect(entry.grant.allocate(bytes)).toBe(fits);
            if (fits) entry.buffers.add(new Uint8Array(bytes).fill(0x5a));
          };
          const free = (entry: Live) => {
            const buffer = entry.buffers.values().next().value;
            if (buffer === undefined) return;
            expect(buffer[0] ?? 0x5a).toBe(0x5a);
            entry.grant.free(buffer.byteLength);
            entry.buffers.delete(buffer);
          };
          const move = (from: Live, to: Live, bytes: number) => {
            const before = governor.usedBytes,
              peak = governor.peakBytes,
              sum = live.reduce((n, e) => n + e.held, 0),
              fits =
                !from.closed &&
                !to.closed &&
                (bytes === 0 || bytes <= from.held - allocated(from) - (from.loan?.bytes ?? 0));
            expect(governor.transferCredit(from.grant, to.grant, bytes)).toBe(fits);
            if (fits && from !== to) {
              from.held -= bytes;
              to.held += bytes;
              if (bytes > 0) reach.reach("same owner transfer");
            }
            if (!fits && !from.closed && !to.closed) reach.reach("live allocation protected");
            expect(live.reduce((n, e) => n + e.held, 0)).toBe(sum);
            expect(governor.usedBytes).toBe(before);
            expect(governor.peakBytes).toBe(peak);
            check();
          };
          const take = (entry: Live) => {
            const buffer = entry.buffers.values().next().value;
            if (buffer === undefined || entry.closed) return;
            if (cache.takeFrom === undefined) {
              expect(process.env.MINNOW_NATIVE_CREDIT_CACHE).not.toBe("1");
              check();
              return;
            }
            nativeCacheSeen = true;
            if (cache.takeFrom(entry.grant, buffer.byteLength)) {
              entry.held -= buffer.byteLength;
              entry.buffers.delete(buffer);
              cached.add(buffer);
              retained.reach("takeover owns real buffer");
              check();
            }
          };
          const close = (entry: Live) => {
            const held = entry.held,
              before = governor.usedBytes;
            entry.buffers.clear();
            entry.loan = null;
            entry.grant.close();
            entry.closed = true;
            entry.held = 0;
            expect(governor.usedBytes).toBe(before - held);
            entry.grant.close();
            expect(governor.usedBytes).toBe(before - held);
            reach.reach("close returns exact holdings");
            if (cached.size > 0) retained.reach("close leaves retained owner paid");
            check();
          };
          try {
            check();
            expect(governor.peakBytes).toBe(s.minima.reduce((sum, n) => sum + n, 0));
            reach.reach("named native split");
            const first = live[0],
              second = live[1];
            if (first === undefined || second === undefined) throw new Error("group fixture");
            allocate(first, 16);
            move(first, second, first.held - allocated(first) + 1);
            move(first, second, 1);
            move(second, first, 1);
            take(first);
            move(first, second, 1);
            if (nativeCacheSeen && cached.size > 0) retained.reach("transfer after takeover");
            second.loan = await second.grant.borrow(32);
            second.held += 32;
            allocate(second, second.held);
            move(second, first, 0);
            move(second, first, 1);
            reach.reach("loan credit protected");
            free(second);
            move(second, first, second.held - 32 + 1);
            second.loan.repay();
            second.held -= 32;
            second.loan = null;
            expect(governor.transferCredit(second.grant, other, 1)).toBe(false);
            expect(governor.transferCredit(other, second.grant, 1)).toBe(false);
            reach.reach("foreign owner refused");
            close(first);
            move(first, second, 0);
            reach.reach("closed side refused");
            for (const action of s.actions) {
              const from = live[action.from % live.length],
                to = live[action.to % live.length];
              if (from === undefined || to === undefined) throw new Error("action fixture");
              if (action.op === "transfer") move(from, to, action.bytes);
              else if (action.op === "allocate") allocate(from, action.bytes);
              else if (action.op === "free") free(from);
              else if (action.op === "take") take(from);
              else if (action.op === "close") close(from);
              else if (action.op === "release" && !from.closed) {
                const bytes = Math.min(action.bytes, from.held - allocated(from));
                from.grant.release(bytes);
                from.held -= bytes;
              } else if (action.op === "cache") {
                const buffer = cached.values().next().value;
                if (buffer !== undefined) {
                  expect(buffer[0]).toBe(0x5a);
                  cached.delete(buffer);
                  cache.release(buffer.byteLength);
                }
              }
              check();
            }
            for (const entry of s.reverseClose ? [...live].reverse() : live) close(entry);
          } finally {
            for (const entry of live) entry.grant.close();
            if (cachedBytes() > 0) cache.release(cachedBytes());
            cached.clear();
            other.close();
            expect(governor.usedBytes).toBe(0);
            expect(foreign.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 40 },
    );
  });

for (const seed of seedsFor("next-native-credit-admission", [0xc2ee]))
  it(`waits atomically and cancels without partial reservations (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          minima: fc.array(fc.integer({ min: 64, max: 8192 }), { minLength: 2, maxLength: 5 }),
          cancel: fc.boolean(),
        }),
        async (s) => {
          reach.beginHistory();
          const governor = new Governor(4 * MiB),
            blockerBytes = governor.capBytes - governor.reserveBytes - (s.minima[0] ?? 0),
            blocker = await governor.admit({
              label: "running minimum",
              minimumBytes: blockerBytes,
              spillable: false,
            }),
            parts = s.minima.map((minimumBytes, n) => ({
              label: `part ${String(n)}`,
              minimumBytes,
              spillable: false,
            })),
            stop = new AbortController(),
            reason = new Error("group cancelled");
          const observed: Grant[] = [],
            admit = governor.admit.bind(governor);
          governor.admit = async (options) => {
            const grant = await admit(options);
            observed.push(grant);
            return grant;
          };
          expect(blocker.allocate(blockerBytes)).toBe(true);
          const buffer = new Uint8Array(blockerBytes).fill(7);
          let settled = false;
          const pending = groupFor(governor, {
              label: "next complete minimum",
              grants: parts,
              signal: stop.signal,
            }),
            checked = pending.then(
              (grants) => {
                settled = true;
                return grants;
              },
              (error: unknown) => {
                settled = true;
                return error;
              },
            );
          try {
            const oldNames = parts.map((p) => p.label);
            parts[0] = { label: "changed after queuing", minimumBytes: 0, spillable: true };
            await turns();
            expect(settled).toBe(false);
            expect(governor.usedBytes).toBe(blockerBytes);
            reach.reach("queued group holds nothing");
            const empty = await governor.admitGroup({
              label: "empty budgets",
              grants: [{ label: "private target", minimumBytes: 0, spillable: false }],
            });
            expect(empty[0]?.allocate(1)).toBe(false);
            for (const grant of empty) grant.close();
            expect(governor.usedBytes).toBe(blockerBytes);
            reach.reach("zero group takes no credit");
            if (s.cancel) stop.abort(reason);
            else {
              expect(buffer[0]).toBe(7);
              blocker.free(blockerBytes);
              blocker.close();
            }
            const result = await checked;
            if (s.cancel) {
              expect(result).toBe(reason);
              expect(governor.usedBytes).toBe(blockerBytes);
              reach.reach("queued cancellation");
            } else {
              if (!Array.isArray(result)) throw new Error("group did not admit");
              const grants = result as Grant[];
              expect(grants.map((g) => g.held)).toEqual(s.minima);
              expect(grants.map((g) => g.label)).toEqual(oldNames);
              expect(grants.every((g) => !g.spillable)).toBe(true);
              expect(governor.usedBytes).toBe(s.minima.reduce((sum, n) => sum + n, 0));
              reach.reach("queued request snapshot");
              for (const grant of grants) grant.close();
            }
            blocker.close();
            expect(governor.usedBytes).toBe(0);
          } finally {
            stop.abort(reason);
            blocker.close();
            const result = await checked;
            if (Array.isArray(result)) for (const grant of result as Grant[]) grant.close();
            for (const grant of observed) grant.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 20 },
    );
  });

for (const seed of seedsFor("next-native-credit-validation", [0xc2ef]))
  it(`rejects invalid moves and unwinds cancellation after admission (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          invalid: fc.constantFrom(-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY),
          bytes: fc.integer({ min: 1, max: 8192 }),
        }),
        async ({ invalid, bytes }) => {
          reach.beginHistory();
          const governor = new Governor(4 * MiB),
            grants = await governor.admitGroup({
              label: "validation",
              grants: [
                { label: "a", minimumBytes: bytes, spillable: false },
                { label: "b", minimumBytes: bytes, spillable: false },
              ],
            });
          const a = grants[0],
            b = grants[1];
          if (!a || !b) throw new Error("validation fixture");
          try {
            expect(() => governor.transferCredit(a, b, invalid)).toThrow(RangeError);
            expect(governor.usedBytes).toBe(2 * bytes);
            reach.reach("invalid credit rejected");
            await expect(
              governor.admitGroup({
                label: "invalid minimum",
                grants: [{ label: "invalid", minimumBytes: invalid, spillable: false }],
              }),
            ).rejects.toThrow(RangeError);
            await expect(
              governor.admitGroup({
                label: "duplicate names",
                grants: [
                  { label: "same", minimumBytes: bytes, spillable: false },
                  { label: "same", minimumBytes: bytes, spillable: false },
                ],
              }),
            ).rejects.toThrow(RangeError);
            expect(governor.usedBytes).toBe(2 * bytes);
            reach.reach("invalid group rejected");
            const stop = new AbortController(),
              reason = new Error("cancel served group"),
              admit = governor.admit.bind(governor);
            governor.admit = async (options) => {
              const grant = await admit(options);
              if (options.label === "cancel after serving") stop.abort(reason);
              return grant;
            };
            await expect(
              governor.admitGroup({
                label: "cancel after serving",
                grants: [{ label: "cancelled native part", minimumBytes: bytes, spillable: false }],
                signal: stop.signal,
              }),
            ).rejects.toBe(reason);
            expect(governor.usedBytes).toBe(2 * bytes);
            reach.reach("served cancellation");
          } finally {
            for (const grant of grants) grant.close();
          }
          expect(governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 20 },
    );
  });

for (const seed of seedsFor("next-native-credit-private-progress", [0xc2f0]))
  it(`moves private credit while ordinary admission waits (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 1024 }), async (extra) => {
        reach.beginHistory();
        const governor = new Governor(4 * MiB),
          total = governor.capBytes - governor.reserveBytes + extra,
          grants = await governor.admitGroup({
            label: "complete private progress",
            includesProgress: true,
            grants: [
              { label: "source private budget", minimumBytes: total - 32, spillable: false },
              { label: "destination private budget", minimumBytes: 32, spillable: false },
            ],
          }),
          stop = new AbortController(),
          reason = new Error("queued peer cleanup");
        let served = false;
        const peer = governor.admit({
            label: "ordinary peer",
            minimumBytes: 1,
            spillable: false,
            signal: stop.signal,
          }),
          checked = peer.then(
            (g) => {
              served = true;
              return g;
            },
            (error: unknown) => error,
          );
        const source = grants[0],
          target = grants[1];
        if (!source || !target) throw new Error("private fixture");
        try {
          expect(source.allocate(16)).toBe(true);
          const buffer = new Uint8Array(16).fill(9);
          await turns();
          expect(served).toBe(false);
          expect(governor.usedBytes).toBe(total);
          reach.reach("private progress group");
          expect(governor.transferCredit(source, target, extra + 1)).toBe(true);
          await turns();
          expect(served).toBe(false);
          expect(governor.usedBytes).toBe(total);
          expect(source.held + target.held).toBe(total);
          expect(source.allocated).toBe(16);
          expect(buffer[0]).toBe(9);
          reach.reach("transfer keeps queued admission waiting");
          source.free(16);
        } finally {
          stop.abort(reason);
          const peerResult = await checked;
          if (
            peerResult !== reason &&
            peerResult !== null &&
            typeof peerResult === "object" &&
            "close" in peerResult
          )
            (peerResult as Grant).close();
          for (const grant of grants) grant.close();
        }
        expect(governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 20 },
    );
  });

it("reaches native credit ownership states", () => {
  reach.assertReached();
  if (nativeCacheSeen || process.env.MINNOW_NATIVE_CREDIT_CACHE === "1") retained.assertReached();
});
