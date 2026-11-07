/** SpillFile.append borrows its input until settlement; governor rules 1/4 retain that charge. */
import { afterAll, expect, it, vi } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { HashJoin, hashJoinMinimumBytes } from "./join.js";
import { INT, TEXT, batchesOf } from "./join-fixtures.js";
function latch() {
  let open!: () => void;
  const wait = new Promise<void>((r) => (open = r));
  return { wait, open };
}
const reach = createReach("next-join-append-fence", [
  "direct abort",
  "reentrant abort",
  "append resolves",
  "append rejects",
  "close rejects",
  "close rejects undefined",
  "borrow charged",
  "closed waits",
  "close buffer charged",
  "released",
]);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-join-append-fence", [0xa901, 0xa902]))
  for (const reentrant of [false, true])
    for (const failure of ["none", "append", "close", "close undefined"] as const)
      it(`append borrow survives ${reentrant ? "reentrant" : "direct"} abort, ${failure}, seed ${String(seed)}`, async () => {
        reach.beginHistory();
        const random = mulberry32(seed);
        const width = seed === 0xa901 ? 600000 : 400000 + Math.floor(random() * 200000);
        const base = new MemorySpillService(),
          entered = latch(),
          gate = latch(),
          closeEntered = latch(),
          closeGate = latch();
        const rejectUndefined = vi.fn<() => Promise<void>>().mockRejectedValue(undefined);
        const appendError = new Error("gated append rejected"),
          closeError = new Error("gated close rejected"),
          reason = new Error("abort with a borrowed page");
        let borrowed = false,
          page: Uint8Array | undefined,
          before = 0,
          closeCalls = 0,
          closedBorrowed = false;
        const governor = new Governor(8 << 20);
        const spill: SpillService = {
          get bytesInUse() {
            return base.bytesInUse;
          },
          sweep: () => base.sweep(),
          async create(queryId) {
            const file = await base.create(queryId);
            return {
              get queryId() {
                return file.queryId;
              },
              get length() {
                return file.length;
              },
              read: (ref) => file.read(ref),
              readInto: (ref, target) => file.readInto(ref, target),
              async append(bytes) {
                page = bytes;
                borrowed = true;
                before = governor.usedBytes;
                if (reentrant) join.abort(reason);
                entered.open();
                await gate.wait;
                try {
                  if (failure === "append") throw appendError;
                  return await file.append(bytes);
                } finally {
                  borrowed = false;
                }
              },
              async close() {
                closeCalls++;
                closedBorrowed ||= borrowed;
                closeEntered.open();
                await closeGate.wait;
                await file.close();
                if (failure === "close") throw closeError;
                if (failure === "close undefined") await rejectUndefined();
              },
            };
          },
        };
        const spec = {
          kind: "inner",
          probeTypes: [INT],
          buildTypes: [INT, TEXT],
          probeKeys: [0],
          buildKeys: [0],
          nullSafe: [false],
          residual: null,
          publishes: [],
        } as const;
        const grant = await governor.admit({
          label: "gated append",
          minimumBytes: hashJoinMinimumBytes(spec),
          spillable: true,
        });
        const join = new HashJoin(spec, { grant, spill, queryId: seed });
        join.probe({
          push() {
            throw new Error("output during build");
          },
          ready: () => Promise.resolve(),
          finish: () => undefined,
          abort: () => undefined,
        });
        const work = (async () => {
          for (let i = 0; i < 64; i++)
            for (const batch of batchesOf(spec.buildTypes, [[i, "x".repeat(width)]]))
              if (join.build.push(batch) === "blocked") await join.build.ready();
          await join.build.finish();
        })().catch((error: unknown) => error);
        try {
          await entered.wait;
          expect(before).toBeGreaterThan(0);
          join.abort(reason);
          let settled = false;
          const closed = join.closed;
          const observed = closed.then(
            () => {
              settled = true;
            },
            () => {
              settled = true;
            },
          );
          await new Promise<void>((r) => setTimeout(r, 0));
          expect(borrowed).toBe(true);
          expect(page?.byteLength).toBeGreaterThan(width);
          expect(governor.usedBytes).toBeGreaterThan(0);
          expect(grant.allocated).toBeGreaterThan(0);
          expect(settled).toBe(false);
          expect(closeCalls).toBe(0);
          reach.reach("borrow charged");
          reach.reach("closed waits");
          gate.open();
          await work;
          await closeEntered.wait;
          expect(governor.usedBytes).toBeGreaterThan(0);
          expect(settled).toBe(false);
          reach.reach("close buffer charged");
          closeGate.open();
          if (failure === "close" || failure === "close undefined")
            await expect(closed).rejects.toBe(failure === "close" ? closeError : undefined);
          else await closed;
          await observed;
          expect(closedBorrowed).toBe(false);
          expect(closeCalls).toBe(1);
          expect(borrowed).toBe(false);
          expect(governor.usedBytes).toBe(0);
          expect(base.bytesInUse).toBe(0);
          reach.reach(reentrant ? "reentrant abort" : "direct abort");
          reach.reach(
            failure === "close"
              ? "close rejects"
              : failure === "close undefined"
                ? "close rejects undefined"
                : failure === "append"
                  ? "append rejects"
                  : "append resolves",
          );
          reach.reach("released");
        } finally {
          gate.open();
          closeGate.open();
          join.abort(reason);
          await work;
          await join.closed.catch(() => undefined);
          grant.close();
          await base.sweep();
        }
      }, 30000);
