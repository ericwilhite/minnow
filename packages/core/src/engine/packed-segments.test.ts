/**
 * Packed segments: a small commit to a wide table stores its columns as one packed block, and
 * every read path cuts the members back out. These tests write packed segments on every store
 * and check that each read path, maintenance step, and snapshot sees exactly the rows an
 * unpacked database holds.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout12";
import { MemoryBlockStore as ReleasedMemoryStore } from "@minnowdb/core-layout12/storage/memory";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  PACKED_SEGMENTS,
  acceptsPackedSegments,
  type BlockStore,
  type SegmentRecord,
  type SnapshotFrame,
} from "../storage/index.js";
import { decodeSnapshotMetadataItems } from "../storage/snapshot-stream.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { internalsOf } from "./internals.js";
import { packedSegmentTestHooks } from "./packed-members.js";
import { scopeWriteSetTestHooks } from "./scope-write-set.js";
import { allSegmentRecords, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const DEFAULT_INDEX_BOUND = packedSegmentTestHooks.maxIndexedMembers;

afterEach(() => {
  packedSegmentTestHooks.forcePacking = false;
  packedSegmentTestHooks.missIsFatal = false;
  packedSegmentTestHooks.maxIndexedMembers = DEFAULT_INDEX_BOUND;
});

const WIDTH = 24;
const OPTIONS = { autoCompact: false, autoCollect: false } as const;

function columns(): Array<{
  name: string;
  type: "number" | "string" | "boolean";
  nullable?: boolean;
}> {
  const result: Array<{ name: string; type: "number" | "string" | "boolean"; nullable?: boolean }> =
    [
      { name: "id", type: "number" },
      { name: "flag", type: "boolean", nullable: true },
    ];
  for (let index = 0; index < WIDTH; index += 1) {
    result.push({
      name: `c${String(index)}`,
      type: index % 2 === 0 ? "number" : "string",
      nullable: true,
    });
  }
  return result;
}

function row(id: number, version = 0): Record<string, unknown> {
  const value: Record<string, unknown> = { id, flag: id % 3 === 0 ? null : id % 2 === 0 };
  for (let index = 0; index < WIDTH; index += 1) {
    value[`c${String(index)}`] =
      (id + index) % 7 === 0
        ? null
        : index % 2 === 0
          ? id * 10 + index + version
          : `v${String(id + index + version)}`;
  }
  return value;
}

type StoreKind = "memory" | "indexeddb" | "opfs";

/** How each opened store was opened, so a test can open it again. */
const opfsRoots = new WeakMap<BlockStore, FileSystemDirectoryHandle>();
const idbOptions = new WeakMap<BlockStore, { name: string; indexedDB: IDBFactory }>();

async function openStore(kind: StoreKind): Promise<BlockStore> {
  if (kind === "memory") return new MemoryBlockStore();
  if (kind === "indexeddb") {
    const options = { name: crypto.randomUUID(), indexedDB: new IDBFactory() };
    const store = await IndexedDbBlockStore.open(options);
    idbOptions.set(store, options);
    return store;
  }
  const root = new MemoryOpfs().root;
  const store = await OpfsBlockStore.open({ name: "packed", root });
  opfsRoots.set(store, root);
  return store;
}

async function packedSegments(store: BlockStore): Promise<SegmentRecord[]> {
  return (await allSegmentRecords(store)).filter((segment) => segment.packedBlockId !== undefined);
}

/** The packed segments a read of the table sees now. */
async function visiblePacked(db: MinnowDatabase, table: string): Promise<string[]> {
  const ids: string[] = [];
  let cursor: Parameters<MinnowDatabase["listVisibleSegmentPage"]>[1] = {};
  for (;;) {
    const page = await db.listVisibleSegmentPage(table, cursor);
    for (const record of page.records) if (record.packedBlockId !== undefined) ids.push(record.id);
    if (page.nextCursor === null) return ids;
    cursor = { cursor: page.nextCursor };
  }
}

describe.each(["memory", "indexeddb", "opfs"] as const)("packed segments on %s", (kind) => {
  it("packs single-row writes, reads them back on every path, and folds them", async () => {
    packedSegmentTestHooks.missIsFatal = true;
    const store = await openStore(kind);
    expect(acceptsPackedSegments(store)).toBe(true);
    const db = new MinnowDatabase(store, OPTIONS);
    try {
      await db.createTable({ name: "wide", uniqueKey: "id", columns: columns() });
      // Past the packed-block ceiling: ordinary blocks.
      await db.insertBatch(
        "wide",
        Array.from({ length: 2_000 }, (_, index) => row(index)) as never,
      );
      for (let id = 2_000; id < 2_010; id += 1) await db.insertBatch("wide", [row(id)] as never);
      await db.upsertBatch("wide", [row(3, 1), row(2_070, 1)] as never);
      await db.execute("UPDATE wide SET c1 = 'changed', c2 = 42 WHERE id = 2005");
      await db.deleteBatch("wide", { keys: [4, 2_006] });
      const packed = await packedSegments(store);
      // Ten single-row inserts, one two-row upsert, one update; the delete names one column.
      expect(packed.length).toBe(12);
      // A full integrity check reads every packed block and finds each segment's members in it.
      const integrity = await store.checkIntegrity?.({ mode: "full" });
      if (kind !== "memory") expect(integrity?.ok).toBe(true);
      for (const segment of packed) expect(segment.packedBlockId).toBe(segment.id);

      const expected = new Map<number, Record<string, unknown>>();
      for (let id = 0; id < 2_010; id += 1) expected.set(id, row(id));
      expected.set(3, row(3, 1));
      expected.set(2_070, row(2_070, 1));
      expected.set(2_005, { ...row(2_005), c1: "changed", c2: 42 });
      expected.delete(4);
      expected.delete(2_006);
      const all = [...expected.entries()]
        .sort((left, right) => left[0] - right[0])
        .map(([, value]) => value);

      const read = async (): Promise<void> => {
        const scanned = await db.query("SELECT * FROM wide ORDER BY id", { memoize: false });
        expect(scanned.rows).toEqual(all);
        const point = await db.query("SELECT * FROM wide WHERE id = 2007", { memoize: false });
        expect(point.rows).toEqual([expected.get(2_007)]);
        const updated = await db.query("SELECT c1, c2 FROM wide WHERE id = 2005", {
          memoize: false,
        });
        expect(updated.rows).toEqual([{ c1: "changed", c2: 42 }]);
        const aggregate = await db.query("SELECT count(*) AS n, sum(c0) AS s FROM wide", {
          memoize: false,
        });
        expect(aggregate.rows).toEqual([
          {
            n: all.length,
            s: all.reduce((total, value) => total + ((value.c0 as number | null) ?? 0), 0),
          },
        ]);
      };
      await read();
      expect((await visiblePacked(db, "wide")).length).toBe(12);
      await db.compactTable("wide");
      expect(await visiblePacked(db, "wide")).toEqual([]);
      await read();
    } finally {
      await db.close();
    }
  });
});

describe.each(["memory", "indexeddb", "opfs"] as const)(
  "packed segment lifecycle on %s",
  (kind) => {
    it("keeps packed segments right through write scopes, ALTER, rekeys, delta merges, and collection", async () => {
      packedSegmentTestHooks.missIsFatal = true;
      const budget = scopeWriteSetTestHooks.budgetBytes;
      const store = await openStore(kind);
      const db = new MinnowDatabase(store, OPTIONS);
      const all = "SELECT * FROM t ORDER BY id";
      const read = async () => (await db.query(all, { memoize: false })).rows;
      try {
        await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT)");
        await db.insertBatch(
          "t",
          Array.from({ length: 600 }, (_, id) => ({
            id,
            a: `a${String(id)}`,
            b: id,
            c: "c",
          })),
        );
        await db.compactTable("t", { partitionRows: 200 });
        const expected = new Map<number, Record<string, unknown>>(
          Array.from({ length: 600 }, (_, id) => [id, { id, a: `a${String(id)}`, b: id, c: "c" }]),
        );
        const check = async () => {
          expect(await read()).toEqual(
            [...expected.values()].sort((left, right) => Number(left.id) - Number(right.id)),
          );
        };

        // A write scope that flushes durably at every statement, then commits.
        scopeWriteSetTestHooks.budgetBytes = 1;
        await db.write(async (scope) => {
          await scope.execute("INSERT INTO t VALUES (1000, 'scoped', 1, 'x')");
          await scope.execute("UPDATE t SET a = 'in-scope' WHERE id = 5");
          expect((await scope.query("SELECT a FROM t WHERE id = 1000")).rows).toEqual([
            { a: "scoped" },
          ]);
          await scope.execute("INSERT INTO t VALUES (1001, 'scoped', 2, 'y')");
        });
        scopeWriteSetTestHooks.budgetBytes = budget;
        expected.set(1000, { id: 1000, a: "scoped", b: 1, c: "x" });
        expected.set(1001, { id: 1001, a: "scoped", b: 2, c: "y" });
        expected.set(5, { ...expected.get(5), a: "in-scope" });
        expect((await packedSegments(store)).length).toBeGreaterThan(0);
        await check();

        // Single-row updates over folded partitions: delta merges take packed updates.
        for (let id = 0; id < 12; id += 1) {
          await db.execute("UPDATE t SET b = b + 1000 WHERE id = ?", [id * 41]);
          const row = expected.get(id * 41);
          expected.set(id * 41, { ...row, b: Number(row?.b) + 1000 });
        }
        await check();
        const merged = await internalsOf(db).automaticCompactionStep("t");
        expect(merged.result?.compacted).toBe(true);
        await check();

        // ALTER over packed segments: an added column backfills, a dropped one goes.
        await db.execute("INSERT INTO t VALUES (1002, 'before-alter', 3, 'z')");
        expected.set(1002, { id: 1002, a: "before-alter", b: 3, c: "z" });
        await db.execute("ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'none'");
        await db.execute("ALTER TABLE t DROP COLUMN c");
        for (const [id, row] of expected) {
          const { c, ...rest } = row;
          void c;
          expected.set(id, { ...rest, d: "none" });
        }
        await db.execute("INSERT INTO t VALUES (1003, 'after-alter', 4, 'new')");
        expected.set(1003, { id: 1003, a: "after-alter", b: 4, d: "new" });
        await check();

        // A rekey of a row a packed segment wrote.
        await db.execute("UPDATE t SET id = 2000 WHERE id = 1003");
        expected.delete(1003);
        expected.set(2000, { id: 2000, a: "after-alter", b: 4, d: "new" });
        await check();
        expect(await db.resolveKey("t", 1003)).toBe(2000);

        // Fold everything, collect what the folds retired, and read again.
        await db.compactTable("t");
        expect(await visiblePacked(db, "t")).toEqual([]);
        await db.collectGarbage();
        await check();
        if (kind !== "memory") {
          expect((await store.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
        }
      } finally {
        scopeWriteSetTestHooks.budgetBytes = budget;
        await db.close();
      }
    });
  },
);

describe("members the index does not hold", () => {
  it("reads a table with more packed members than the index holds, from the member ids", async () => {
    const store = await openStore("memory");
    const writer = new MinnowDatabase(store, OPTIONS);
    // A connection that reads what another wrote, with nothing cached.
    const reader = new MinnowDatabase(store, OPTIONS);
    try {
      await writer.createTable({ name: "wide", uniqueKey: "id", columns: columns() });
      for (let id = 0; id < 12; id += 1) await writer.insertBatch("wide", [row(id)] as never);
      expect((await packedSegments(store)).length).toBe(12);
      // Room for about one segment's 26 members: listing the table drops each segment's members
      // as later ones are indexed, so the read finds most members from their ids.
      packedSegmentTestHooks.maxIndexedMembers = 30;
      const before = packedSegmentTestHooks.unindexedReads;
      const expected = Array.from({ length: 12 }, (_, id) => row(id));
      expect(
        (await reader.query("SELECT * FROM wide ORDER BY id", { memoize: false })).rows,
      ).toEqual(expected);
      expect(packedSegmentTestHooks.unindexedReads).toBeGreaterThan(before);
    } finally {
      await writer.close();
      await reader.close();
    }
  });

  it("makes a member found from its id fatal under the test hook", async () => {
    const store = await openStore("memory");
    const writer = new MinnowDatabase(store, OPTIONS);
    const covered = new MinnowDatabase(store, OPTIONS);
    const starved = new MinnowDatabase(store, OPTIONS);
    try {
      await writer.createTable({ name: "wide", uniqueKey: "id", columns: columns() });
      for (let id = 0; id < 4; id += 1) await writer.insertBatch("wide", [row(id)] as never);
      packedSegmentTestHooks.missIsFatal = true;
      // A reader lists the table before it reads, so its own index covers every member.
      expect(
        (await covered.query("SELECT * FROM wide ORDER BY id", { memoize: false })).rows,
      ).toEqual(Array.from({ length: 4 }, (_, id) => row(id)));
      // With the index too small to hold them, a reader must find members from their ids,
      // which the hook turns into a failure naming the member.
      packedSegmentTestHooks.maxIndexedMembers = 30;
      await expect(
        starved.query("SELECT * FROM wide ORDER BY id", { memoize: false }),
      ).rejects.toThrow(/read before its segment was indexed/);
    } finally {
      packedSegmentTestHooks.missIsFatal = false;
      await writer.close();
      await covered.close();
      await starved.close();
    }
  });
});

describe("what packs", () => {
  it("packs a single-row write to a 101-column table, and not a write past 64 KiB", async () => {
    const store = await openStore("memory");
    const db = new MinnowDatabase(store, OPTIONS);
    try {
      const wide = Array.from({ length: 100 }, (_, index) => ({
        name: `c${String(index)}`,
        type: index % 2 === 0 ? ("number" as const) : ("string" as const),
        nullable: true,
      }));
      await db.createTable({
        name: "wide",
        uniqueKey: "id",
        columns: [{ name: "id", type: "number" }, ...wide],
      } as never);
      const row = (id: number, text: string): Record<string, unknown> =>
        Object.fromEntries<unknown>([
          ["id", id],
          ...wide.map(({ name, type }) => [name, type === "number" ? id : text] as const),
        ]);
      // One block for all 101 columns.
      const small = await db.insertBatch("wide", [row(1, "short")] as never);
      expect(small.blockCount).toBe(1);
      // Fifty 2 KiB strings: past the packed-block ceiling, so one block per column.
      const large = await db.insertBatch("wide", [row(2, "x".repeat(2_048))] as never);
      expect(large.blockCount).toBe(101);
      expect((await packedSegments(store)).length).toBe(1);
      expect(
        (await db.query("SELECT c1, c99 FROM wide ORDER BY id", { memoize: false })).rows,
      ).toEqual([
        { c1: "short", c99: "short" },
        { c1: "x".repeat(2_048), c99: "x".repeat(2_048) },
      ]);
    } finally {
      await db.close();
    }
  });
});

describe("store opt-in", () => {
  it("never writes a packed segment to a store that does not declare it", async () => {
    packedSegmentTestHooks.forcePacking = true;
    const inner = new MemoryBlockStore();
    // A wrapper that forwards every method but not the capability: an adapter written before
    // packed segments existed.
    const undeclared = new Proxy(inner, {
      get(target, property, receiver) {
        if (property === PACKED_SEGMENTS) return undefined;
        const value: unknown = Reflect.get(target, property, receiver);
        return typeof value === "function" ? (value.bind(target) as unknown) : value;
      },
    });
    expect(acceptsPackedSegments(undeclared)).toBe(false);
    const db = new MinnowDatabase(undeclared, OPTIONS);
    try {
      await db.createTable({ name: "wide", uniqueKey: "id", columns: columns() });
      for (let id = 0; id < 5; id += 1) await db.insertBatch("wide", [row(id)] as never);
      await db.execute("UPDATE wide SET c1 = 'x' WHERE id = 2");
      expect(await packedSegments(inner)).toEqual([]);
      expect((await allSegmentRecords(inner)).length).toBeGreaterThan(0);
    } finally {
      await db.close();
    }
  });
});

/** A database holding packed segments beside ordinary ones, and the rows it reads. */
async function packedDatabase(db: MinnowDatabase): Promise<unknown[]> {
  await db.createTable({ name: "wide", uniqueKey: "id", columns: columns() });
  await db.insertBatch("wide", Array.from({ length: 300 }, (_, index) => row(index)) as never);
  for (let id = 300; id < 306; id += 1) await db.insertBatch("wide", [row(id)] as never);
  await db.execute("UPDATE wide SET c1 = 'changed' WHERE id = 302");
  await db.deleteBatch("wide", { keys: [7] });
  return (await db.query("SELECT * FROM wide ORDER BY id", { memoize: false })).rows;
}

function lifetime(): { createdAt: string; expiresAt: string } {
  const created = Date.now();
  return {
    createdAt: new Date(created).toISOString(),
    expiresAt: new Date(created + 10 * 60_000).toISOString(),
  };
}

/**
 * Every frame of one export session, read in order. With `pause`, the store is reopened before
 * frame `at`, and the frame before it is read again first: a replay returns the same bytes.
 */
async function exportFrames(
  initial: BlockStore,
  pause?: { at: number; reopen: (store: BlockStore) => Promise<BlockStore> },
): Promise<SnapshotFrame[]> {
  let store = initial;
  const ownerId = "packed-export";
  if (store.beginSnapshotFrameExport === undefined) {
    throw new Error("The store has no frame export");
  }
  const session = await store.beginSnapshotFrameExport({ ownerId, ...lifetime() });
  const frameCount = Object.values(session.header.kinds).reduce(
    (total, summary) => total + summary.frameCount,
    0,
  );
  const read = async (sequence: number): Promise<SnapshotFrame> => {
    const clock = lifetime();
    const frame = await store.readSnapshotExportFrame?.({
      sessionId: session.sessionId,
      ownerId,
      sequence,
      expiresAtCutoff: clock.createdAt,
      expiresAt: clock.expiresAt,
    });
    if (frame === undefined) throw new Error(`Missing snapshot frame ${String(sequence)}`);
    return frame;
  };
  const frames: SnapshotFrame[] = [];
  for (let sequence = 0; sequence < frameCount; sequence += 1) {
    if (pause?.at === sequence) {
      store = await pause.reopen(store);
      const replayed = await read(sequence - 1);
      expect(replayed.key).toBe(frames[sequence - 1]?.key);
      expect(replayed.payload).toEqual(frames[sequence - 1]?.payload);
    }
    frames.push(await read(sequence));
  }
  await store.closeSnapshotFrameExport?.({ sessionId: session.sessionId, ownerId });
  return frames;
}

function byKey(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

describe.each(["memory", "indexeddb", "opfs"] as const)(
  "packed segment snapshots on %s",
  (kind) => {
    it("exports packed segments unpacked, and this build and 0.16.0 import the same rows", async () => {
      const store = await openStore(kind);
      const db = new MinnowDatabase(store, OPTIONS);
      try {
        const expected = await packedDatabase(db);
        // Six single-row inserts and one update pack; the delete names one column.
        expect((await packedSegments(store)).length).toBe(7);
        const bytes = await db.exportSnapshot();

        const current = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
        await current.importSnapshot(bytes);
        expect(
          (await current.query("SELECT * FROM wide ORDER BY id", { memoize: false })).rows,
        ).toEqual(expected);
        // Imported segments stand on their own: one block per column, as an older build wrote.
        expect(await visiblePacked(current, "wide")).toEqual([]);
        await current.close();

        const released = new ReleasedDatabase(new ReleasedMemoryStore(), OPTIONS);
        await released.importSnapshot(bytes);
        expect((await released.query("SELECT * FROM wide ORDER BY id")).rows).toEqual(expected);
        await released.close();
      } finally {
        await db.close();
      }
    });

    it("writes each member as a block frame in key order, and replays and resumes across them", async () => {
      const store = await openStore(kind);
      const db = new MinnowDatabase(store, OPTIONS);
      await packedDatabase(db);
      await db.close();
      const packed = await packedSegments(store);
      const frames = await exportFrames(store);
      const blockKeys = frames
        .filter((frame) => frame.kind === "block")
        .map(({ key }) => key ?? "");
      expect(blockKeys).toEqual([...blockKeys].sort(byKey));
      for (const segment of packed) {
        // No packed block leaves the store; each member does, under its own id.
        expect(blockKeys).not.toContain(segment.packedBlockId);
        for (const [member] of Object.values(segment.columnBlockIds)) {
          expect(blockKeys).toContain(member);
        }
      }
      for (const frame of frames.filter(({ kind: frameKind }) => frameKind === "segment-page")) {
        for (const item of decodeSnapshotMetadataItems("segment-page", frame.payload)) {
          expect((item as { record: SegmentRecord }).record.packedBlockId).toBeUndefined();
        }
      }
      if (kind === "memory") return;
      // Reopen in the middle of one packed block's members: the durable session resumes there.
      // An insert's: it names every column, so the pause lands inside its members.
      const first = packed.find(
        ({ columnBlockIds }) => Object.keys(columnBlockIds).length === columns().length,
      );
      const members = Object.values(first?.columnBlockIds ?? {})
        .map(([member]) => member ?? "")
        .sort(byKey);
      const at = frames.findIndex(({ key }) => key === members[0]) + 2;
      expect(frames[at]?.key).toBe(members[2]);
      let current = store;
      const resumed = await exportFrames(current, {
        at,
        reopen: async (open) => {
          if (kind === "opfs") {
            (open as OpfsBlockStore)._crashForTests();
            current = await OpfsBlockStore.open({
              name: "packed",
              root: opfsRoots.get(open) as never,
            });
            opfsRoots.set(current, opfsRoots.get(open) as never);
          } else {
            open.close();
            current = await IndexedDbBlockStore.open(idbOptions.get(open) as never);
          }
          return current;
        },
      });
      expect(resumed.map(({ kind: frameKind, key }) => [frameKind, key])).toEqual(
        frames.map(({ kind: frameKind, key }) => [frameKind, key]),
      );
      expect(resumed.map(({ payload }) => payload)).toEqual(frames.map(({ payload }) => payload));
      current.close();
    });
  },
);
