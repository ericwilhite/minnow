import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
  type SegmentRecord,
} from "./index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";

beforeEach(() => vi.stubGlobal("IDBKeyRange", IDBKeyRange));
afterEach(() => vi.unstubAllGlobals());

const now = "2026-01-01T00:00:00.000Z";
const adapters: Array<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: async () => new MemoryBlockStore() },
  {
    name: "indexeddb",
    open: () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    open: () => OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

for (const adapter of adapters) {
  it(`${adapter.name}: multi-table commit refusals preserve the journal and permit corrected retry`, async () => {
    const store = await adapter.open();
    try {
      for (const id of ["a", "b"]) {
        await store.addTable({
          id,
          name: id,
          columns: [{ id: "value", name: "value", type: "number", nullable: false }],
          managed: false,
          revision: 0,
          createdAt: now,
        });
      }
      const begun = await store.beginTransaction({
        record: {
          id: "writer",
          ownerId: "owner",
          expiresAt: "2026-01-01T00:30:00.000Z",
          pendingBlockIds: [],
          pendingSegmentIds: [],
          status: "active",
          revision: 0,
          startedAt: now,
          updatedAt: now,
          committedVersion: null,
        },
      });
      const segments: SegmentRecord[] = ["a", "b", "a"].map((tableId, ordinal) => ({
        id: `segment-${String(ordinal)}`,
        tableId,
        transactionId: "writer",
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: ordinal,
        rowCount: 1,
        rowIdStart: BigInt(ordinal + 1),
        rowIdEndExclusive: BigInt(ordinal + 2),
        rowIdSpans: [],
        columnBlockIds: { value: [`block-${String(ordinal)}`] },
        createdAt: now,
      }));
      const blocks = segments.map((_, ordinal) => ({
        id: `block-${String(ordinal)}`,
        bytes: Uint8Array.of(ordinal + 1),
      }));
      const staged = await store.stageTransactionArtifacts({
        transactionId: "writer",
        expectedRevision: begun.record.revision,
        blocks,
        segments,
        updatedAt: now,
      });
      const commit = {
        transactionId: "writer",
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        committedAt: now,
      } as const;
      const before = await store.getTransaction("writer");
      const probe = await store.getCatalogProbe();
      const invalidLimits = [
        [],
        [{ tableId: "a", limit: 2 }],
        [
          { tableId: "a", limit: 2 },
          { tableId: "a", limit: 2 },
        ],
        [
          { tableId: "a", limit: 2 },
          { tableId: "missing", limit: 1 },
        ],
        ...[0, -1, 0.5, NaN, Infinity, 4097].map((limit) => [
          { tableId: "a", limit },
          { tableId: "b", limit: 1 },
        ]),
        [
          { tableId: "b", limit: 1 },
          { tableId: "a", limit: 1 },
        ],
      ];
      for (const [index, limits] of invalidLimits.entries()) {
        await expect(
          store.commitTransaction({ ...commit, levelZeroSegmentLimits: limits }),
        ).rejects.toMatchObject({
          name: index < 4 ? "TypeError" : index < 10 ? "RangeError" : "CompactionBacklogError",
        });
        expect(await store.getTransaction("writer")).toEqual(before);
        expect(await store.getCatalogProbe()).toEqual(probe);
        for (const block of blocks) expect(await store.getBlock(block.id)).toEqual(block.bytes);
      }
      const published = await store.commitTransaction({
        ...commit,
        levelZeroSegmentLimits: [
          { tableId: "b", limit: 1 },
          { tableId: "a", limit: 2 },
        ],
      });
      expect(published.version).toBe(0);
      expect(published.changedTableIds).toEqual(["a", "b"]);
      expect(await store.getTransaction("writer")).toMatchObject({
        status: "committed",
        committedVersion: 0,
      });
    } finally {
      store.close();
    }
  });
}
