/** R8: whole-generation replacement consumes the old root's immutable disk inventory,
 * not a caller array proportional to the number of old allocations. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_RETIREMENT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import {
  SnapshotKind,
  SnapshotStreamWriter,
  snapshotNumbers,
  snapshotCrc32,
} from "../format/snapshot.js";
import { Catalog } from "./catalog.js";
import { encodeSnapshotCatalogHeader } from "./snapshot-catalog.js";
import { loadSnapshot, type SnapshotLoadTarget } from "./snapshot-load.js";
import { snapshotBlob } from "./snapshot-source.js";
import { column, INT, tableInput } from "./test-support.js";
async function archive(rows: number): Promise<Uint8Array[]> {
  const catalog =
    rows === 0
      ? Catalog.empty()
      : Catalog.empty().change(
          {
            kind: "createTable",
            table: tableInput("old", [column("n", INT)], { primaryKey: null }),
          },
          1,
        ).catalog;
  const native = catalog.encode(),
    bytes = new Uint8Array(24 + native.length);
  encodeSnapshotCatalogHeader(native.length, 0, bytes.subarray(0, 24));
  bytes.set(native, 24);
  const parts: Uint8Array[] = [],
    budget = { allocate: () => true, free: () => undefined };
  const writer = await SnapshotStreamWriter.open(
    { id: new Uint8Array(16), sourceLsn: 0 },
    {
      write(bytes) {
        parts.push(bytes.slice());
      },
    },
    budget,
  );
  const data = new Uint8Array(64);
  await writer.frame(SnapshotKind.catalogBegin, 1, 0, data.subarray(0, 0));
  await writer.frame(SnapshotKind.catalogPart, 1, 1, bytes);
  snapshotNumbers([bytes.length, 0], data.subarray(0, 16));
  new DataView(data.buffer).setUint32(8, snapshotCrc32(bytes), true);
  await writer.frame(SnapshotKind.catalogEnd, 1, 2, data.subarray(0, 16));
  if (rows > 0) {
    snapshotNumbers([1, 1, 1], data.subarray(0, 24));
    await writer.frame(SnapshotKind.tableBegin, 2, 0, data.subarray(0, 24));
    const { encodeSnapshotColumnHeader } = await import("./snapshot-column.js");
    for (let row = 0; row < rows; row++) {
      snapshotNumbers([1, 1, 1, row, 1, 21], data.subarray(0, 48));
      await writer.frame(SnapshotKind.columnBegin, row + 3, 0, data.subarray(0, 48));
      encodeSnapshotColumnHeader(INT, 1, data.subarray(0, 16));
      data[16] = 1;
      new DataView(data.buffer).setInt32(17, row, true);
      await writer.frame(SnapshotKind.columnPart, row + 3, 1, data.subarray(0, 21));
      snapshotNumbers([21, 1], data.subarray(0, 16));
      await writer.frame(SnapshotKind.columnEnd, row + 3, 2, data.subarray(0, 16));
    }
    snapshotNumbers([rows], data.subarray(0, 8));
    await writer.frame(SnapshotKind.tableEnd, 2, 1, data.subarray(0, 8));
  }
  await writer.finish();
  writer.close();
  return parts;
}
function input(chunks: readonly Uint8Array[]) {
  return snapshotBlob(new Blob(chunks.map((chunk) => chunk.slice().buffer)));
}
async function openTarget(
  backend: MemoryBackend,
  governor: Governor,
  lsn: number,
): Promise<SnapshotLoadTarget> {
  const lease = await backend.acquire(),
    session = await backend.open(lease),
    startup = await governor.admit({ label: "target", minimumBytes: 8 << 20, spillable: false });
  try {
    const space = await openExtentSpace(session, {
      reachable: listedRootExtents(),
      walk: reachableExtents(),
      verifyAtOpen: true,
      budget: startup,
      registerCache: (cache) => governor.registerCache(cache),
    });
    return {
      space,
      epoch: session.epoch,
      currentLsn: lsn,
      empty: lsn === 0,
      async close() {
        try {
          await space.close();
        } finally {
          startup.close();
          try {
            await session.close();
          } finally {
            await lease.release();
          }
        }
      },
    };
  } catch (error) {
    startup.close();
    await session.close();
    await lease.release();
    throw error;
  }
}
for (const seed of seedsFor("next-snapshot-retirement", [0xb26b, 0xb26c]))
  it(`replaces growing old inventories under a fixed real governor cap seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot retirement", SNAPSHOT_RETIREMENT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2500, max: 2700 }), async (large) => {
        reach.beginHistory();
        const peaks: number[] = [];
        for (const rows of [2, large]) {
          const backend = new MemoryBackend(),
            governor = new Governor(32 << 20),
            chunks = await archive(rows);
          const prepare = new Governor(16 << 20);
          await loadSnapshot(input(chunks), () => openTarget(backend, governor, 0), prepare);
          expect(prepare.usedBytes).toBe(0);
          expect(governor.usedBytes).toBe(0);
          const loadGovernor = new Governor(6 << 20);
          let peak = 0;
          const admit = loadGovernor.admitGroup.bind(loadGovernor);
          loadGovernor.admitGroup = async (options) => {
            const grants = await admit(options);
            for (const grant of grants) {
              const allocate = grant.allocate.bind(grant);
              grant.allocate = (bytes) => {
                const ok = allocate(bytes);
                peak = Math.max(peak, grant.allocated);
                return ok;
              };
            }
            return grants;
          };
          await loadSnapshot(
            input(await archive(0)),
            () => openTarget(backend, governor, 1),
            loadGovernor,
            { replace: true },
          );
          expect(loadGovernor.usedBytes).toBe(0);
          expect(governor.usedBytes).toBe(0);
          peaks.push(peak);
          const opened = await openTarget(backend, governor, 2),
            check = await governor.admit({
              label: "check",
              minimumBytes: 1 << 20,
              spillable: false,
            });
          try {
            const bytes = opened.space.root;
            if (bytes === null) throw new Error("missing imported root");
            const parsed = decodeManifestRoot(bytes, check);
            expect(parsed.value.tables).toHaveLength(0);
            check.free(parsed.charge);
            await opened.space.audit();
          } finally {
            check.close();
            await opened.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach(rows === 2 ? "small old inventory" : "large old inventory");
        }
        expect(Math.abs((peaks[1] ?? 0) - (peaks[0] ?? 0))).toBeLessThan(65536);
        reach.reach("constant replacement workspace");
      }),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  }, 120000);
