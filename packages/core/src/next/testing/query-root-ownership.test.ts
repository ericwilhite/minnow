import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, QUERY_ROOT_OWNERSHIP_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { encodeGroupRoot, parseCharge } from "../format/manifest-codec.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { GroupRead } from "../page/group-read.js";
import { QueryRuntime } from "../exec/run.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { I32 } from "../exec/kernels/test-support.js";
import type { Snapshot } from "../access/snapshot.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { ExecuteRequest } from "../exec/executor.js";
import type { ChunkReads } from "../exec/scan.js";

function input(options: { signal?: AbortSignal; windowBytes?: number } = {}) {
  const catalog = { version: 0 };
  const plan: PhysicalPlan = {
    root: { id: 1, op: "values", rows: [], output: [{ name: "v", type: I32, nullable: false }] },
    columns: [{ name: "v", type: I32, nullable: false }],
    parameters: [],
    placeholderCount: 0,
    clockSlot: null,
    initPlans: [],
    catalogVersion: catalog.version,
    volatile: false,
  };
  const governor = new Governor(8 * 1024 * 1024);
  // Values-only lifecycle tests do not read tables. The end-to-end adapter uses real snapshots.
  const snapshot = { catalog, observedLsn: 17 } as unknown as Snapshot;
  const request: ExecuteRequest = {
    snapshot,
    statement: null,
    parameters: [],
    statementTime: 0n,
    reads: {} as ChunkReads,
    governor,
    spill: new MemorySpillService(),
    queryId: 1,
    sequences: null,
    sequenceSession: null,
    returning: "atExecuted",
    resultWindowBytes: options.windowBytes ?? 64,
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  };
  return { plan, request, governor };
}

it.each(seedsFor("next-query-root-ownership", [0xb2e3, 0xb2e4]))(
  "charges, isolates and evicts query roots without changing admission (seed %i)",
  async (seed) => {
    const reach = createReach("query-root ownership", QUERY_ROOT_OWNERSHIP_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 65536 }), async (rowCount) => {
        reach.beginHistory();
        const h = input();
        const controller = new AbortController();
        const runtime = new QueryRuntime(h.plan, h.request, h.request.snapshot, controller.signal);
        const space = new MemoryExtentSpace();
        const expected = {
          rowCount,
          rowIds: { kind: "dense" as const },
          columns: Array.from({ length: 3 }, (_, i) => ({
            columnId: i + 1,
            type: I32,
            data: { extent: { offset: 4096 * (i + 1), length: 1024 }, offset: 0, length: 128 },
            directory: {
              extent: { offset: 4096 * (i + 1), length: 1024 },
              offset: 128,
              length: 0,
              checksum: 0,
            },
            statistics: {
              nullCount: Math.floor(rowCount / 3),
              min: null,
              max: null,
              dictionarySize: null,
            },
            distinctSketch: null,
          })),
        };
        const bytes = encodeGroupRoot(expected);
        const written = await space.beginWrite("query root").write([bytes]);
        const ref = written.blobs[0];
        if (!ref) throw new Error("missing root");
        const grant = await h.governor.admit({
          label: "root caller",
          minimumBytes: 65536,
          spillable: false,
        });
        const caches = runtime.metadataCaches(true, [
          { tableId: 0, columns: [1] },
          { tableId: 1, columns: [2] },
        ]);
        const roots = caches?.roots;
        if (!roots) throw new Error("missing query cache");
        const pins = vi.spyOn(roots, "pin");
        try {
          const first = await caches.read(0, ref, space, grant, [1]);
          expect(first.value).toEqual({ rowCount, columns: [expected.columns[0]] });
          expect(grant.allocated).toBe(parseCharge(bytes.length));
          reach.reach("exact borrower charge");
          first.release();
          expect(roots.residentBytes).toBe(parseCharge(bytes.length));
          expect(grant.allocated).toBe(0);
          const key = pins.mock.calls[0]?.[0];
          if (!key) throw new Error("missing cache key");
          const alias = { ...key, extent: { ...ref.extent, length: ref.extent.length + 8 } };
          expect(roots.pin(alias)).toBeNull();
          reach.reach("full reference isolation");
          const reader = new GroupRead(ref, space, grant, caches);
          await reader.prepare([1]);
          const second = await caches.read(0, ref, space, grant, [1]);
          second.release();
          expect(second.value).toBe(first.value);
          expect(grant.allocated).toBe(parseCharge(bytes.length));
          expect(roots.evict(Infinity)).toBe(0);
          reach.reach("active pin survives eviction");
          await reader.prepare([1]);
          reader.release();
          // Execution's complete admission can reclaim every idle root; no sizing lease remains.
          expect(roots.evict(Infinity)).toBe(parseCharge(bytes.length));
          expect(roots.residentBytes).toBe(0);
          const third = await caches.read(0, ref, space, grant, [1]);
          expect(third.value).toEqual({ rowCount, columns: [expected.columns[0]] });
          expect(third.value).not.toBe(second.value);
          reach.reach("idle eviction reparses safely");
          third.release();
          const otherTable = await caches.read(1, ref, space, grant, [2]);
          expect(otherTable.value.columns).toEqual([expected.columns[1]]);
          otherTable.release();
          const unplanned = await caches.read(0, ref, space, grant, [3]);
          expect(unplanned.value).toEqual(expected);
          unplanned.release();
          roots.clear();
          const refused = await caches.read(
            0,
            ref,
            space,
            {
              allocate: (bytes) =>
                grant.allocated + bytes <= parseCharge(ref.length) && grant.allocate(bytes),
              free: (bytes) => grant.free(bytes),
            },
            [1],
          );
          expect(refused.value).toEqual(expected);
          expect(roots.entryCount).toBe(0);
          refused.release();
          reach.reach("optional memo refusal keeps complete root");
          // Preparing a scan never changes the ordinary complete-root contract.
          const complete = new GroupRead(ref, space, grant, caches);
          await complete.prepare([1]);
          expect(await complete.root()).toEqual(expected);
          complete.release();
          roots.clear();
          let unblock: () => void = () => undefined;
          const gate = new Promise<void>((resolve) => {
            unblock = resolve;
          });
          const delayed = new GroupRead(
            ref,
            {
              peek: () => null,
              read: async (refs, options) => {
                await gate;
                return space.read(refs, options);
              },
            },
            grant,
            caches,
          );
          const pending = Promise.resolve(delayed.prepare([1]));
          runtime.onAbort(() => delayed.release());
          runtime.dispose(() =>
            pending.then(
              () => undefined,
              () => undefined,
            ),
          );
          controller.abort();
          runtime.abort(new Error("cancel a pending root"));
          expect(runtime.metadataCaches()).toBeUndefined();
          const closing = runtime.close();
          unblock();
          await closing;
          await expect(pending).rejects.toThrow("Group metadata read ended");
          reach.reach("pending cancellation drains");
        } finally {
          grant.close();
          await runtime.close();
          expect(roots.residentBytes).toBe(0);
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("zero owner charges");
        }
      }),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  },
);
