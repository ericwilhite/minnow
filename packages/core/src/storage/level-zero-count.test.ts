/**
 * The level-zero ceiling counts a table's visible committed level-zero segments on every commit
 * that adds one. The memory and OPFS stores keep that count from one commit to the next instead
 * of walking every segment and block again; these tests read the count the store enforces — a
 * refused commit reports it — after each kind of change that can move it, and hold it to the
 * visible segments the engine itself reads.
 */
import { expect, it } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import {
  CompactionBacklogError,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
  type SegmentRecord,
} from "./index.js";

/**
 * The visible level-zero count the store enforces, read from a single-shot write it refuses: two
 * level-zero segments against a ceiling of one fail whatever the table holds. A refused write
 * changes nothing, so reading the count does not disturb what is being measured.
 */
async function enforcedLevelZeroCount(
  store: BlockStore,
  tableId: string,
  keyColumnId: string,
): Promise<number> {
  const write = store.writeTransaction?.bind(store);
  if (write === undefined) throw new Error("The store has no single-shot write");
  const id = `probe-${crypto.randomUUID()}`;
  const now = new Date().toISOString();
  const probe = await store.getCatalogProbe();
  const segments: SegmentRecord[] = [0, 1].map((ordinal) => ({
    id: `${id}/segment-${String(ordinal)}`,
    tableId,
    transactionId: id,
    kind: "update",
    keyColumnId,
    level: 0,
    logicalOrder: 0,
    commitOrdinal: ordinal,
    rowCount: 1,
    rowIdStart: 0n,
    rowIdEndExclusive: 0n,
    rowIdSpans: [],
    columnBlockIds: { [keyColumnId]: [`${id}/block-${String(ordinal)}`] },
    createdAt: now,
  }));
  try {
    await write({
      transaction: {
        record: {
          id,
          ownerId: "probe",
          expiresAt: new Date(Date.now() + 600_000).toISOString(),
          pendingBlockIds: [],
          pendingSegmentIds: [],
          status: "active",
          revision: 0,
          startedAt: now,
          updatedAt: now,
          committedVersion: null,
          schemaEpochGuard: probe.schemaEpoch,
        },
      },
      blocks: [0, 1].map((ordinal) => ({
        id: `${id}/block-${String(ordinal)}`,
        bytes: Uint8Array.of(ordinal + 1),
      })),
      segments,
      expectedManifestVersion: probe.manifestVersion,
      committedAt: now,
      levelZeroSegmentLimits: [{ tableId, limit: 1 }],
    });
  } catch (error) {
    if (!(error instanceof CompactionBacklogError)) throw error;
    return error.levelZeroSegments - segments.length;
  }
  throw new Error("The probe write was not refused");
}

async function visibleLevelZero(db: MinnowDatabase, store: BlockStore): Promise<number> {
  const page = await db.listVisibleSegmentPage("t", { limit: 64 });
  expect(page.nextCursor).toBeNull();
  let count = 0;
  for (const visible of page.records) {
    if ((await store.getSegment(visible.id))?.level === 0) count += 1;
  }
  return count;
}

const adapters: Array<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: async () => new MemoryBlockStore() },
  {
    name: "opfs",
    open: () => OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

for (const adapter of adapters) {
  it(`${adapter.name}: the enforced level-zero count follows commits, folds, and collection`, async () => {
    const store = await adapter.open();
    const db = new MinnowDatabase(store, { autoCompact: false });
    try {
      await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
      const table = await store.getTableByName("t");
      const keyColumnId = table?.uniqueKeyColumnId;
      if (table === undefined || keyColumnId === undefined) throw new Error("table is missing");
      const check = async (): Promise<number> => {
        const enforced = await enforcedLevelZeroCount(store, table.id, keyColumnId);
        expect(enforced).toBe(await visibleLevelZero(db, store));
        return enforced;
      };
      for (let batch = 0; batch < 3; batch += 1) {
        await db.insertBatch(
          "t",
          Array.from({ length: 10 }, (_, index) => ({ id: batch * 10 + index, amount: 0 })),
        );
      }
      expect(await check()).toBe(3);
      // Ordinary commits carry the count forward, one segment each.
      for (let index = 0; index < 5; index += 1) {
        await db.execute("UPDATE t SET amount = amount + 1 WHERE id = ?", [index]);
      }
      expect(await check()).toBe(8);
      // A fold retires the level-zero segments' blocks: the count must not carry through it.
      await db.compactTable("t");
      const folded = await check();
      expect(folded).toBeLessThan(8);
      await db.execute("UPDATE t SET amount = amount + 1 WHERE id = 1");
      await db.execute("DELETE FROM t WHERE id = 2");
      expect(await check()).toBe(folded + 2);
      // Collection reclaims records without publishing a version.
      await db.collectGarbage();
      expect(await check()).toBe(folded + 2);
      await db.execute("UPDATE t SET amount = amount + 1 WHERE id = 3");
      expect(await check()).toBe(folded + 3);
    } finally {
      await db.close();
    }
  });
}
