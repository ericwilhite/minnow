/** Test bridge, frozen together with the entire baseline module graph. Only physical files and
 * locks cross the build boundary; each build opens its own backend/session/space/engine/codecs. */
import { Governor } from "../memory/governor.js";
import { SimBackend, type SimDisk, type SimLocks } from "../storage/sim-disk.js";
import type { ExtentSpace } from "../storage/extents.js";
import { openExtentSpace } from "../storage/space.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import { decodeGroupIndex } from "../format/manifest-codec.js";
import { packLocator } from "../vector/locator.js";
import { ChunkReader } from "../page/reader.js";
import { VarlenBuilder } from "../vector/varlen.js";
import type { FlatVector } from "../vector/vector.js";
import { WritePathImpl } from "./engine.js";
import { autocommit, batch, column, INT, ints, locators, tableInput } from "./test-support.js";
const FREE = { allocate: () => true, free: () => undefined };
export async function openDirectory(disk: unknown, locks: unknown, verifyAtOpen = false) {
  const backend = new SimBackend(disk as SimDisk, "handle", { locks: locks as SimLocks });
  const lease = await backend.acquire();
  const session = await backend.open(lease);
  const governor = new Governor(256 << 20);
  const startup = await governor.admit({
    label: "directory fixture startup",
    minimumBytes: 8 << 20,
    spillable: true,
  });
  let space: ExtentSpace | undefined;
  try {
    space = await openExtentSpace(session, {
      reachable: listedRootExtents(),
      walk: reachableExtents(),
      // Match ordinary startup. Transition cases separately audit current/previous ownership.
      verifyAtOpen,
      budget: startup,
      registerCache: (c) => governor.registerCache(c),
    });
    const opening = WritePathImpl.open({
      session,
      space,
      governor,
      options: { durability: "strict" },
      tuning: { memtableRows: 1, merges: { mode: "off" } },
    });
    startup.release(startup.held - startup.allocated - startup.loaned);
    const path = await opening;
    const ownedSpace = space;
    return {
      path,
      space: ownedSpace,
      session,
      governor,
      async create() {
        await autocommit(path, async (s) => {
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)], { primaryKey: null }),
          });
          await s.changeCatalog({
            kind: "createSequence",
            sequence: {
              name: "stamp",
              start: 1n,
              increment: 1n,
              min: 0n,
              max: 1000000n,
              cycle: false,
              ownedBy: null,
            },
          });
        });
      },
      async rows() {
        const view = path.pin();
        try {
          // One-row INT fixtures: keep the ordinary segment/chunk reader and all payload reads,
          // but avoid the general harness's two dynamic imports and fresh buffers per segment.
          const table = view.table(1, { budget: FREE });
          const def = table.columns[0];
          if (table.columns.length !== 1 || def?.type.kind !== "int32")
            throw new Error("Unexpected fixture schema");
          const flat: FlatVector = {
            form: "flat",
            type: INT,
            length: 1,
            validity: new Uint32Array(1),
            data: { layout: "i32", values: new Int32Array(1) },
          };
          const output = { flat, codes: new Int32Array(1), varlen: new VarlenBuilder(1, FREE, 64) };
          const values: number[] = [];
          for (const segment of table.segments) {
            if (segment.rowCount === 0) continue;
            if (segment.rowCount !== 1) throw new Error("Unexpected fixture row count");
            const deleted = await segment.deletions();
            let vector: FlatVector;
            if (segment.kind === "memory") {
              const result = await segment.chunk(def.id, 0, output);
              if (!result.complete || result.vector.form !== "flat")
                throw new Error("Incomplete fixture chunk");
              vector = result.vector;
            } else {
              await segment.root();
              const chunk = segment.chunk(def.id);
              if (chunk === null) throw new Error("Missing fixture column");
              const reader = await ChunkReader.open(view.pages, chunk, { budget: FREE });
              try {
                await reader.load(
                  Array.from({ length: reader.directory.pages.length }, (_, n) => n),
                );
                await reader.loadValues(0, 1);
                reader.decodeRange(0, 1, flat, 0);
                vector = flat;
              } finally {
                reader.close();
              }
            }
            if (
              vector.data.layout !== "i32" ||
              (vector.validity !== null && ((vector.validity[0] ?? 0) & 1) === 0)
            )
              throw new Error("Invalid fixture value");
            const value = vector.data.values[0];
            if (value === undefined) throw new Error("Missing fixture value");
            if (!deleted?.isDeleted(0)) values.push(value);
          }
          return values.sort((a, b) => a - b);
        } finally {
          view.release();
        }
      },
      async pages() {
        const root = path.state().root.tables.find((t) => t.tableId === 1);
        if (root === undefined || root.groupCount === 0) return [];
        const lease = await ownedSpace.read([root.groups], { budget: FREE });
        try {
          return decodeGroupIndex(lease.bytes[0] ?? new Uint8Array(), FREE).value;
        } finally {
          lease.release();
        }
      },
      insert(values: number[]) {
        return autocommit(path, (s) => s.table(1).insert(batch(ints(values)), null));
      },
      delete(segment: number) {
        return autocommit(path, (s) =>
          s.table(1).delete(batch(locators([packLocator(segment, 0)], 1)), null),
        );
      },
      async stamp(n: number) {
        const lsn = await path.sequences.set(1, BigInt(n), true);
        await path.durable(lsn);
      },
      async takeStamp() {
        const out = { repr: "safe" as const, values: new Float64Array(1) };
        const lsn = await path.sequences.next(1, 1, out, 0);
        await path.durable(lsn);
        return out.values[0];
      },
      async settle() {
        await path.recycleLog();
        await path.quiesce();
      },
      async close() {
        try {
          await path.close();
        } finally {
          try {
            await ownedSpace.close();
          } finally {
            startup.close();
            await session.close();
            await lease.release();
          }
        }
      },
    };
  } catch (error) {
    try {
      await space?.close();
    } finally {
      startup.close();
      await session.close();
      await lease.release();
    }
    throw error;
  }
}
export type DirectoryDb = Awaited<ReturnType<typeof openDirectory>>;
