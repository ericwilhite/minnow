/** Governor rules 1/4: an asynchronous spill read owns charged bytes until settlement. */
import { afterAll, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { HashJoin, hashJoinMinimumBytes } from "./join.js";
import { INT, TEXT, batchesOf } from "./join-fixtures.js";
import * as spillPages from "./spill-pages.js";
function latch() {
  let open!: () => void;
  const wait = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { wait, open };
}
const readState = { 1: "read 1", 2: "read 2", 5: "read 5", 20: "read 20" } as const;
const reach = createReach("next-join-read-fence", [
  "load",
  "replay",
  "read 1",
  "read 2",
  "read 5",
  "read 20",
  "direct abort",
  "reentrant abort",
  "read resolves",
  "read rejects",
  "close rejects",
  "close rejects undefined",
  "read charged",
  "closed waits",
  "no decode after cancel",
  "no output after cancel",
  "close charged",
  "released",
]);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-join-read-fence", [0xead1, 0xead2]))
  for (const ordinal of [1, 2, 5, 20] as const)
    for (const reentrant of [false, true])
      for (const failure of ["none", "read", "close", "close undefined"] as const)
        it(`read ${String(ordinal)}, ${reentrant ? "reentrant" : "direct"} abort, ${failure}, seed ${String(seed)}`, async () => {
          reach.beginHistory();
          const random = mulberry32(seed),
            shift = seed === 0xead1 ? 0 : 1 + Math.floor(random() * 2);
          const width = 600000;
          const base = new MemorySpillService(),
            entered = latch(),
            gate = latch(),
            closeEntered = latch(),
            closeGate = latch();
          const reason = new Error("abort with a read buffer"),
            readError = new Error("read rejected"),
            closeError = new Error("read close rejected");
          const rejectUndefined = vi.fn<() => Promise<void>>().mockRejectedValue(undefined);
          let reads = 0,
            held: Uint8Array | undefined,
            borrowing = false,
            cancelled = false,
            closedBorrowed = false;
          let readReleased = false;
          let phase = "build",
            gatePhase = "",
            before = 0,
            outputsAfterCancel = 0,
            closes = 0,
            created = 0;
          let targetCloseCalls = 0;
          const spill: SpillService = {
            get bytesInUse() {
              return base.bytesInUse;
            },
            sweep: () => base.sweep(),
            async create(queryId) {
              const file = await base.create(queryId);
              created++;
              let target = false;
              return {
                get queryId() {
                  return file.queryId;
                },
                get length() {
                  return file.length;
                },
                append: (page) => file.append(page),
                readInto: (ref, bytes) => file.readInto(ref, bytes),
                async read(ref) {
                  const bytes = await file.read(ref);
                  if (++reads !== ordinal) return bytes;
                  target = true;
                  held = bytes;
                  borrowing = true;
                  before = activeGovernor.usedBytes;
                  gatePhase = phase;
                  if (reentrant) {
                    cancelled = true;
                    join.abort(reason);
                  }
                  entered.open();
                  await gate.wait;
                  try {
                    if (failure === "read") throw readError;
                    return bytes;
                  } finally {
                    borrowing = false;
                  }
                },
                async close() {
                  closes++;
                  if (target) {
                    targetCloseCalls++;
                    closedBorrowed ||= borrowing;
                  }
                  if (cancelled && readReleased) {
                    closeEntered.open();
                    await closeGate.wait;
                  }
                  await file.close();
                  if (target && failure === "close") throw closeError;
                  if (target && failure === "close undefined") await rejectUndefined();
                },
              };
            },
          };
          const spec = {
            kind: "full",
            probeTypes: [INT, TEXT],
            buildTypes: [INT, TEXT],
            probeKeys: [0],
            buildKeys: [0],
            nullSafe: [false],
            residual: null,
            publishes: [],
          } as const;
          const minimum = hashJoinMinimumBytes(spec);
          // The admitted minimum, including its one-MiB progress reserve.
          const tight = new Governor(
            Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7) + 1),
          );
          const grant = await tight.admit({
            label: "read fence",
            minimumBytes: minimum,
            spillable: true,
          });
          // Keep the diagnostic ledger on the same governor as the grant.
          const activeGovernor = tight;
          const join = new HashJoin(spec, { grant, spill, queryId: seed });
          const probe = join.probe({
            push() {
              if (cancelled) outputsAfterCancel++;
              return "ready";
            },
            ready: () => Promise.resolve(),
            finish: () => undefined,
            abort: () => undefined,
          });
          const work = (async () => {
            for (let i = 0; i < 64; i++)
              for (const batch of batchesOf(spec.buildTypes, [
                [(i + shift) % 3, "b".repeat(width)],
              ]))
                if (join.build.push(batch) === "blocked") await join.build.ready();
            phase = "finishBuild";
            await join.build.finish();
            phase = "probe";
            for (const batch of batchesOf(spec.probeTypes, [
              [0, "p".repeat(width)],
              [1, "p".repeat(width)],
            ]))
              if (probe.push(batch) === "blocked") await probe.ready();
            phase = "finishProbe";
            await probe.finish();
          })().then(
            () => ({ ok: true as const }),
            (error: unknown) => ({ ok: false as const, error }),
          );
          let restore = () => undefined;
          try {
            await Promise.race([
              entered.wait,
              work.then(() => {
                throw new Error("read gate not reached");
              }),
            ]);
            // No decode may be entered for this cancelled result, even after the backend returns.
            const decodedAfterCancel = latch();
            const originalDecode = spillPages.decodePage;
            const decode = vi.spyOn(spillPages, "decodePage").mockImplementation((...args) => {
              if (cancelled) decodedAfterCancel.open();
              return originalDecode(...args);
            });
            restore = () => {
              decode.mockRestore();
              return undefined;
            };
            cancelled = true;
            join.abort(reason);
            let settled = false;
            const closed = join.closed,
              observed = closed.then(
                () => {
                  settled = true;
                },
                () => {
                  settled = true;
                },
              );
            await new Promise<void>((resolve) => setTimeout(resolve, 0));
            expect(borrowing).toBe(true);
            expect(held?.byteLength).toBeGreaterThan(width);
            expect(activeGovernor.usedBytes).toBeGreaterThanOrEqual(held?.byteLength ?? Infinity);
            expect(grant.allocated).toBeGreaterThan(0);
            expect(settled).toBe(false);
            expect(targetCloseCalls).toBe(0);
            reach.reach("read charged");
            reach.reach("closed waits");
            readReleased = true;
            gate.open();
            const result = await Promise.race([
              work,
              decodedAfterCancel.wait.then(() => {
                throw new Error("Decoded a cancelled spill-read result");
              }),
            ]);
            expect(result.ok).toBe(false);
            if (!result.ok) expect(result.error).toBe(reason);
            expect(decode).not.toHaveBeenCalled();
            expect(outputsAfterCancel).toBe(0);
            reach.reach("no decode after cancel");
            reach.reach("no output after cancel");
            await closeEntered.wait;
            expect(activeGovernor.usedBytes).toBeGreaterThan(0);
            expect(settled).toBe(false);
            reach.reach("close charged");
            closeGate.open();
            if (failure === "close" || failure === "close undefined")
              await expect(closed).rejects.toBe(failure === "close" ? closeError : undefined);
            else await closed;
            await observed;
            expect(closedBorrowed).toBe(false);
            expect(targetCloseCalls).toBe(1);
            expect(closes).toBe(created);
            expect(activeGovernor.usedBytes).toBe(0);
            expect(base.bytesInUse).toBe(0);
            expect(before).toBeGreaterThan(width);
            expect(["finishBuild", "finishProbe"]).toContain(gatePhase);
            reach.reach(gatePhase === "finishBuild" ? "load" : "replay");
            reach.reach(readState[ordinal]);
            reach.reach(reentrant ? "reentrant abort" : "direct abort");
            reach.reach(
              failure === "close"
                ? "close rejects"
                : failure === "close undefined"
                  ? "close rejects undefined"
                  : failure === "read"
                    ? "read rejects"
                    : "read resolves",
            );
            reach.reach("released");
          } finally {
            gate.open();
            closeGate.open();
            join.abort(reason);
            await work;
            await join.closed.catch(() => undefined);
            restore();
            grant.close();
            await base.sweep();
          }
        }, 30000);
