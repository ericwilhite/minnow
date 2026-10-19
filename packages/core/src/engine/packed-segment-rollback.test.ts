/**
 * Packed blocks staged ahead of publication. Ordinary scopes and session transactions buffer
 * their artifacts in memory and commit in one storage call, so a packed block is journaled
 * before its commit only when a transaction outgrows one batch (more than 64 deferred blocks,
 * 1,024 per call, or 64 MiB). These force that path with one row per block and check that a
 * savepoint rollback, an aborted scope, and a crash between the stage and the commit leave no
 * packed block behind that collection cannot reclaim. The store-level contract follows: the
 * journal partitions packed blocks by id, an expired owner's stage is reclaimed, and a packed
 * segment staged beside a block that lacks a member it names is refused.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { encodeBlock, encodePackedBlock } from "../block-format/index.js";
import {
  IndexedDbBlockStore,
  MAX_LEVEL_ZERO_SEGMENTS,
  MemoryBlockStore,
  OpfsBlockStore,
  packedSegmentMemberId,
  type BlockStore,
  type SegmentRecord,
  type TableRecord,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { packedSegmentTestHooks } from "./packed-members.js";
import {
  allSegmentRecords,
  allTransactionRecords,
  heavyTestTimeout,
  instrumentStoreCalls,
} from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(180_000) });

afterEach(() => {
  packedSegmentTestHooks.forcePacking = false;
  packedSegmentTestHooks.missIsFatal = false;
});

const KINDS = ["memory", "indexeddb", "opfs"] as const;
type StoreKind = (typeof KINDS)[number];
const OPTIONS = { autoCompact: false, autoCollect: false } as const;
const WIDTH = 24;

interface OpenedStore {
  store: BlockStore;
  reopen(): Promise<BlockStore>;
  crash(): void;
}

async function openStore(kind: StoreKind): Promise<OpenedStore> {
  if (kind === "memory") {
    const store = new MemoryBlockStore();
    return { store, reopen: async () => store, crash: () => undefined };
  }
  if (kind === "indexeddb") {
    const options = { name: crypto.randomUUID(), indexedDB: new IDBFactory() };
    const store = await IndexedDbBlockStore.open(options);
    return {
      store,
      reopen: () => IndexedDbBlockStore.open(options),
      crash: () => store.close(),
    };
  }
  const shim = new MemoryOpfs();
  const name = `packed-rollback-${crypto.randomUUID()}`;
  const store = await OpfsBlockStore.open({ name, root: shim.root });
  return {
    store,
    reopen: () => OpfsBlockStore.open({ name, root: shim.root }),
    crash: () => store._crashForTests(),
  };
}

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

function row(id: number): Record<string, unknown> {
  const value: Record<string, unknown> = { id, flag: id % 3 === 0 ? null : id % 2 === 0 };
  for (let index = 0; index < WIDTH; index += 1) {
    value[`c${String(index)}`] =
      (id + index) % 7 === 0 ? null : index % 2 === 0 ? id * 10 + index : `v${String(id + index)}`;
  }
  return value;
}

async function readAll(db: MinnowDatabase, table = "wide"): Promise<unknown[]> {
  return (await db.query(`SELECT * FROM ${table} ORDER BY id`, { memoize: false })).rows;
}

async function stored(store: BlockStore, ids: readonly string[]): Promise<string[]> {
  const found: string[] = [];
  for (const id of ids) if ((await store.getBlock(id)) !== undefined) found.push(id);
  return found;
}

async function activeTransactions(store: BlockStore): Promise<string[]> {
  return (await allTransactionRecords(store))
    .filter((record) => record.status === "active")
    .map((record) => record.id);
}

/** Seeds the wide table with ordinary blocks and a small table, with default block sizing. */
async function seed(store: BlockStore): Promise<unknown[]> {
  const seeder = new MinnowDatabase(store, OPTIONS);
  try {
    await seeder.createTable({ name: "wide", uniqueKey: "id", columns: columns() });
    await seeder.insertBatch(
      "wide",
      Array.from({ length: 300 }, (_, index) => row(index)) as never,
    );
    await seeder.createTable({
      name: "small",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "a", type: "string", nullable: true },
      ],
    });
    return await readAll(seeder);
  } finally {
    await seeder.close();
  }
}

/** Records the block ids of every stage call, in order, and the calls made. */
function traceStaging(store: BlockStore): { staged: string[][]; calls: string[] } {
  const trace = { staged: [] as string[][], calls: [] as string[] };
  for (const name of [
    "stageTransactionArtifacts",
    "writeTransaction",
    "commitTransaction",
    "rollbackTransactionArtifacts",
  ] as const) {
    const original = (store as unknown as Record<string, unknown>)[name];
    if (typeof original !== "function") continue;
    Object.defineProperty(store, name, {
      configurable: true,
      writable: true,
      value: function (
        this: unknown,
        input: { blocks?: ReadonlyArray<{ id: string }>; removeBlockIds?: readonly string[] },
      ) {
        trace.calls.push(name);
        if (name === "stageTransactionArtifacts" || name === "writeTransaction") {
          trace.staged.push((input.blocks ?? []).map((block) => block.id));
        }
        return (original as (...args: unknown[]) => unknown).apply(this, [input]);
      },
    });
  }
  return trace;
}

/** 48 rows of 26 columns at one row per block: 1,248 blocks, past the 64 deferred and one batch. */
const BIG = Array.from({ length: 48 }, (_, index) => row(5_000 + index));
const bigInsert = `INSERT INTO wide (id, c0, c1) VALUES ${BIG.map(
  (value) => `(${String(value.id)}, ${String(value.c0)}, '${String(value.c1)}')`,
).join(", ")}`;

/** Member ids are ordinary ids plus a NUL; the packed block's id is its segment's. */
const isPacked = (id: string): boolean => !id.includes("/part/");

describe.each(KINDS)("packed blocks staged ahead of a multi-batch commit on %s", (kind) => {
  it("a savepoint rollback in a persisted session transaction removes the packed blocks staged after it", async () => {
    packedSegmentTestHooks.missIsFatal = true;
    const { store } = await openStore(kind);
    const rows = await seed(store);
    const db = new MinnowDatabase(store, { ...OPTIONS, rowsPerBlock: 1 });
    const trace = traceStaging(store);
    try {
      await db.execute("BEGIN");
      await db.execute(bigInsert);
      await db.execute("INSERT INTO small VALUES (1, 'kept')");
      await db.execute("SAVEPOINT s");
      await db.execute("INSERT INTO small VALUES (2, 'dropped')");
      await db.execute("UPDATE wide SET c1 = 'dropped' WHERE id = 201");
      await db.execute("ROLLBACK TO s");
      await db.execute("INSERT INTO small VALUES (3, 'kept-too')");
      await db.execute("COMMIT");
      expect(trace.calls).toContain("stageTransactionArtifacts");
      expect(trace.calls).toContain("rollbackTransactionArtifacts");
      const stagedIds = trace.staged.flat();
      expect(stagedIds.filter(isPacked).length).toBeGreaterThanOrEqual(2);
      expect(await readAll(db, "small")).toEqual([
        { id: 1, a: "kept" },
        { id: 3, a: "kept-too" },
      ]);
      expect((await readAll(db)).length).toBe(rows.length + BIG.length);
      expect(
        (await db.query("SELECT c1 FROM wide WHERE id = 201", { memoize: false })).rows,
      ).toEqual([{ c1: row(201).c1 }]);
      // Every staged block is named by a segment or gone; none dangles.
      const live = new Set(
        (await allSegmentRecords(store)).flatMap((segment) =>
          segment.packedBlockId === undefined
            ? Object.values(segment.columnBlockIds).flat()
            : [segment.packedBlockId],
        ),
      );
      expect((await stored(store, stagedIds)).filter((id) => !live.has(id))).toEqual([]);
      expect(await activeTransactions(store)).toEqual([]);
      if (kind !== "memory") {
        expect((await store.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
      }
    } finally {
      await db.close();
    }
  });

  it("an aborted multi-batch scope leaves none of its staged packed blocks once collected", async () => {
    packedSegmentTestHooks.missIsFatal = true;
    const { store } = await openStore(kind);
    const rows = await seed(store);
    const db = new MinnowDatabase(store, { ...OPTIONS, rowsPerBlock: 1 });
    const trace = traceStaging(store);
    try {
      await expect(
        db.write(async (scope) => {
          await scope.insertBatch("wide", BIG as never);
          await scope.execute("INSERT INTO small VALUES (1, 'x')");
          await scope.execute("UPDATE wide SET c1 = 'scoped' WHERE id = 201");
          await scope.query("SELECT count(*) AS n FROM wide");
          throw new Error("abort the scope");
        }),
      ).rejects.toThrow(/abort the scope/);
      const stagedIds = trace.staged.flat();
      expect(stagedIds.filter(isPacked).length).toBeGreaterThanOrEqual(2);
      expect(await readAll(db)).toEqual(rows);
      expect(await readAll(db, "small")).toEqual([]);
      // An abort retires the stage; collection reclaims it, a bounded page at a time.
      for (let pass = 0; pass < 4 && (await stored(store, stagedIds)).length > 0; pass += 1) {
        await db.collectGarbage();
      }
      expect(await stored(store, stagedIds)).toEqual([]);
      expect(await activeTransactions(store)).toEqual([]);
      if (kind !== "memory") {
        expect((await store.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
      }
    } finally {
      await db.close();
    }
  });

  it("a crash between the stage and the commit publishes nothing; recovery and collection reclaim the stage", async () => {
    if (kind === "memory") return;
    let clock = Date.parse("2026-10-19T12:00:00.000Z");
    const now = () => new Date(clock);
    const opened = await openStore(kind);
    const { store } = opened;
    const rows = await seed(store);
    const db = new MinnowDatabase(store, { ...OPTIONS, now, rowsPerBlock: 1 });
    const probe = instrumentStoreCalls(store);
    const trace = traceStaging(store);
    let dead = false;
    probe.onCall = (call) => {
      const name = probe.calls[call - 1];
      if (dead) throw new Error("store is dead");
      if (name === "commitTransaction" || name === "writeTransaction") {
        dead = true;
        throw new Error("crash before publish");
      }
    };
    probe.armed = true;
    const outcome = await db
      .write(async (scope) => {
        await scope.insertBatch("wide", BIG as never);
        await scope.execute("INSERT INTO small VALUES (1, 'x')");
        await scope.execute("UPDATE wide SET c1 = 'scoped' WHERE id = 201");
      })
      .then(
        () => "committed",
        (error: unknown) => (error instanceof Error ? error.message : String(error)),
      );
    probe.armed = false;
    expect(outcome).not.toBe("committed");
    const stagedIds = trace.staged.flat();
    expect(stagedIds.length).toBeGreaterThan(64);
    expect(stagedIds.filter(isPacked).length).toBeGreaterThanOrEqual(2);
    try {
      await db.close();
    } catch {
      // The dead store may refuse the close.
    }
    opened.crash();

    clock += 3 * 60 * 60_000;
    const reopened = await opened.reopen();
    const survivor = new MinnowDatabase(reopened, { ...OPTIONS, now });
    try {
      expect(await readAll(survivor)).toEqual(rows);
      expect(await readAll(survivor, "small")).toEqual([]);
      expect((await reopened.checkIntegrity?.())?.issues).toEqual([]);
      for (let pass = 0; pass < 5 && (await stored(reopened, stagedIds)).length > 0; pass += 1) {
        await survivor.collectGarbage();
      }
      expect(await activeTransactions(reopened)).toEqual([]);
      expect(await stored(reopened, stagedIds)).toEqual([]);
      expect((await reopened.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
      await survivor.insertBatch("small", [{ id: 9, a: "after" }] as never);
      expect(await readAll(survivor, "small")).toEqual([{ id: 9, a: "after" }]);
    } finally {
      await survivor.close();
    }
  });
});

const PAIR: TableRecord = {
  managed: false,
  id: "pair",
  name: "pair",
  columns: [
    { id: "value", name: "value", type: "number", nullable: false },
    { id: "other", name: "other", type: "number", nullable: false },
  ],
  revision: 0,
  createdAt: "2026-10-19T12:00:00.000Z",
};

function packedSegment(
  id: string,
  transactionId: string,
  ordinal: number,
  rowIdStart: bigint,
  members: ReadonlyArray<"value" | "other"> = ["value", "other"],
): { segment: SegmentRecord; bytes: Uint8Array } {
  const packed = encodePackedBlock(
    members.map((columnId) => ({
      memberId: packedSegmentMemberId(id, columnId),
      columnId,
      column: { type: "number", values: [columnId === "value" ? 1 : 2] },
    })),
  );
  if (packed === undefined) throw new Error("The fixture's packed block did not encode");
  return {
    bytes: packed.bytes,
    segment: {
      id,
      tableId: PAIR.id,
      transactionId,
      rowCount: 1,
      rowIdStart,
      rowIdEndExclusive: rowIdStart + 1n,
      columnBlockIds: {
        value: [packedSegmentMemberId(id, "value")],
        other: [packedSegmentMemberId(id, "other")],
      },
      packedBlockId: id,
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: ordinal,
      rowIdSpans: [],
      createdAt: new Date().toISOString(),
    },
  };
}

async function openTransaction(
  store: BlockStore,
  id: string,
  ttlMs = 30 * 60_000,
): Promise<number | null> {
  const version = await store.getCurrentManifestVersion();
  const started = new Date().toISOString();
  await store.createTransaction({
    id,
    ownerId: `owner-${id}`,
    expiresAt: new Date(Date.now() + ttlMs).toISOString(),
    snapshotVersion: version,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: started,
    updatedAt: started,
    committedVersion: null,
  });
  return version;
}

describe.each(KINDS)("the store-level packed staging contract on %s", (kind) => {
  it("rollbackTransactionArtifacts partitions packed blocks by id; the retained one commits and reads", async () => {
    const { store } = await openStore(kind);
    await store.addTable(PAIR);
    const version = await openTransaction(store, "t1");
    const keep = packedSegment("keep", "t1", 0, 1n);
    const drop = packedSegment("drop", "t1", 1, 2n);
    let record = await store.stageTransactionArtifacts({
      transactionId: "t1",
      expectedRevision: 0,
      blocks: [
        { id: "keep", bytes: keep.bytes },
        { id: "drop", bytes: drop.bytes },
      ],
      segments: [keep.segment, drop.segment],
      updatedAt: new Date().toISOString(),
    });
    expect(record.pendingBlockIds).toEqual(["keep", "drop"]);
    record = await store.rollbackTransactionArtifacts({
      transactionId: "t1",
      expectedRevision: record.revision,
      pendingBlockIds: ["keep"],
      pendingSegmentIds: ["keep"],
      removeBlockIds: ["drop"],
      removeSegmentIds: ["drop"],
      updatedAt: new Date().toISOString(),
    });
    expect(await store.getBlock("drop")).toBeUndefined();
    expect(await store.getSegment("drop")).toBeUndefined();
    expect(await store.getBlock("keep")).toBeDefined();
    await store.commitTransaction({
      transactionId: "t1",
      expectedTransactionRevision: record.revision,
      expectedManifestVersion: version,
      levelZeroSegmentLimits: [{ tableId: PAIR.id, limit: MAX_LEVEL_ZERO_SEGMENTS }],
      committedAt: new Date().toISOString(),
    });
    const db = new MinnowDatabase(store, OPTIONS);
    try {
      expect((await db.query("SELECT value, other FROM pair", { memoize: false })).rows).toEqual([
        { value: 1, other: 2 },
      ]);
      if (kind !== "memory") {
        expect((await store.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
      }
    } finally {
      await db.close();
    }
  });

  it("a packed block staged by a transaction that never commits is reclaimed once its owner expires", async () => {
    if (kind === "memory") return;
    const opened = await openStore(kind);
    const { store } = opened;
    await store.addTable(PAIR);
    await openTransaction(store, "t2", 60_000);
    const lost = packedSegment("lost", "t2", 0, 1n);
    await store.stageTransactionArtifacts({
      transactionId: "t2",
      expectedRevision: 0,
      blocks: [{ id: "lost", bytes: lost.bytes }],
      segments: [lost.segment],
      updatedAt: new Date().toISOString(),
    });
    opened.crash();
    const clock = Date.now() + 3 * 60 * 60_000;
    const reopened = await opened.reopen();
    const db = new MinnowDatabase(reopened, { ...OPTIONS, now: () => new Date(clock) });
    try {
      expect((await db.query("SELECT count(*) AS n FROM pair", { memoize: false })).rows).toEqual([
        { n: 0 },
      ]);
      expect((await reopened.checkIntegrity?.())?.issues).toEqual([]);
      await db.collectGarbage();
      expect(await reopened.getBlock("lost")).toBeUndefined();
      expect(await activeTransactions(reopened)).toEqual([]);
      expect((await reopened.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
    } finally {
      await db.close();
    }
  });

  it.each(["stage", "write"] as const)(
    "%s refuses a packed segment whose block lacks a named member, or is not a packed block",
    async (path) => {
      const { store } = await openStore(kind);
      await store.addTable(PAIR);
      const version = await openTransaction(store, "t3");
      const whole = packedSegment("forged", "t3", 0, 1n);
      const lacking = packedSegment("forged", "t3", 0, 1n, ["value"]);
      const unpacked = await encodeBlock({ type: "number", values: [1] });
      const attempt = async (bytes: Uint8Array): Promise<void> => {
        if (path === "stage") {
          await store.stageTransactionArtifacts({
            transactionId: "t3",
            expectedRevision: 0,
            blocks: [{ id: "forged", bytes }],
            segments: [whole.segment],
            updatedAt: new Date().toISOString(),
          });
          return;
        }
        if (store.writeTransaction === undefined)
          throw new Error("The store has no single-shot write");
        await store.writeTransaction({
          transaction: { id: "t3", expectedRevision: 0 },
          blocks: [{ id: "forged", bytes }],
          segments: [whole.segment],
          expectedManifestVersion: version,
          levelZeroSegmentLimits: [{ tableId: PAIR.id, limit: MAX_LEVEL_ZERO_SEGMENTS }],
          committedAt: new Date().toISOString(),
        });
      };
      // The record names `other`; the block holds only `value`.
      await expect(attempt(lacking.bytes)).rejects.toThrow(
        "Segment forged: packed block lacks member forged\u0000other",
      );
      await expect(attempt(unpacked)).rejects.toThrow(/packed block is not a packed block/);
      // Nothing of either refusal stays: the same id then stages and commits with the whole block.
      expect(await store.getBlock("forged")).toBeUndefined();
      expect(await store.getSegment("forged")).toBeUndefined();
      expect((await store.getTransaction("t3"))?.revision).toBe(0);
      await attempt(whole.bytes);
      if (path === "stage") {
        const record = await store.getTransaction("t3");
        await store.commitTransaction({
          transactionId: "t3",
          expectedTransactionRevision: record?.revision ?? 0,
          expectedManifestVersion: version,
          levelZeroSegmentLimits: [{ tableId: PAIR.id, limit: MAX_LEVEL_ZERO_SEGMENTS }],
          committedAt: new Date().toISOString(),
        });
      }
      const db = new MinnowDatabase(store, OPTIONS);
      try {
        expect((await db.query("SELECT value, other FROM pair", { memoize: false })).rows).toEqual([
          { value: 1, other: 2 },
        ]);
      } finally {
        await db.close();
      }
    },
  );
});
