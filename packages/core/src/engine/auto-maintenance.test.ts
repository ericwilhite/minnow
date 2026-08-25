/**
 * Background maintenance as a property of a database left to itself: a burst of writes lands,
 * nothing else is asked of it, and afterwards the table is folded, the garbage is collected,
 * the store is no bigger than it needs to be, and every row is still right.
 *
 * These are the guarantees that `autoCompact` and `autoCollect` exist to provide, pinned as
 * bounds rather than as timings: how many segments a scan reads, how many blocks and bytes the
 * store holds, how many manifest versions stay unpruned. A change that stops a fold, stops a
 * collection pass, or lets either one grow without limit fails here; a change that merely makes
 * them slower does not, which is what the benchmark gate is for.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryBlockStore, type Manifest, type TransactionRecord } from "../storage/index.js";
import { SnapshotManifestMissingError } from "../storage/types.js";
import { MaintenanceBacklogError, MinnowDatabase } from "./database.js";
import { allVisibleSegments } from "./storage-test-helpers.js";

async function manifestRecords(store: MemoryBlockStore) {
  const records: Manifest[] = [];
  let cursor: number | null = null;
  for (;;) {
    const page = await store.listManifestPage(cursor, 256);
    records.push(...page.records);
    if (page.nextCursor === null) return records;
    cursor = page.nextCursor;
  }
}

async function transactionRecords(store: MemoryBlockStore) {
  const records: TransactionRecord[] = [];
  let cursor: string | null = null;
  for (;;) {
    const page = await store.listTransactionPage(cursor, 256);
    records.push(...page.records);
    if (page.nextCursor === null) return records;
    cursor = page.nextCursor;
  }
}

class CollectionFaultStore extends MemoryBlockStore {
  failuresRemaining = 0;
  attempts = 0;

  override async listGarbageCollectionJobPage(
    afterId: Parameters<MemoryBlockStore["listGarbageCollectionJobPage"]>[0],
    limit: Parameters<MemoryBlockStore["listGarbageCollectionJobPage"]>[1],
  ) {
    this.attempts += 1;
    if (this.failuresRemaining > 0) {
      this.failuresRemaining -= 1;
      throw new Error("injected collection failure");
    }
    return super.listGarbageCollectionJobPage(afterId, limit);
  }
}

afterEach(() => vi.useRealTimers());

const REGIONS = ["west", "east", "north", "south"] as const;
const ROWS = 4_000;

interface Row {
  id: number;
  region: string;
  amount: number;
  [column: string]: string | number;
}

function seedRows(): Row[] {
  return Array.from({ length: ROWS }, (_, index) => ({
    id: index + 1,
    region: REGIONS[index % REGIONS.length] ?? "west",
    amount: index % 1000,
  }));
}

/**
 * A clock the test advances: background collection keeps a version for at most a minute, and
 * a test cannot wait one. Starts well past epoch zero so "a minute ago" is a real instant.
 */
function testClock(): { now: () => Date; advance: (ms: number) => void } {
  let current = Date.parse("2026-01-01T00:00:00Z");
  return {
    now: () => new Date(current),
    advance: (ms) => {
      current += ms;
    },
  };
}

async function seeded(options: ConstructorParameters<typeof MinnowDatabase>[1] = {}) {
  const store = new MemoryBlockStore();
  const clock = testClock();
  const database = new MinnowDatabase(store, { rowsPerBlock: 256, now: clock.now, ...options });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "region", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  const reference = new Map<number, Row>();
  const rows = seedRows();
  for (let start = 0; start < rows.length; start += 1_000) {
    await database.insertBatch("items", rows.slice(start, start + 1_000));
  }
  for (const row of rows) reference.set(row.id, row);
  return { store, database, reference, clock };
}

/**
 * A quiet minute, as the database's clock sees it, then one commit: background collection
 * keeps the last versions readable for a minute, and runs a pass on the first commit after a
 * quiet period, so this is what lets it reclaim what the burst before it superseded.
 */
async function quietMinute(
  database: MinnowDatabase,
  reference: Map<number, Row>,
  clock: ReturnType<typeof testClock>,
): Promise<void> {
  clock.advance(61_000);
  const row = reference.get(1);
  if (row !== undefined) {
    await database.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [1]);
    reference.set(1, { ...row, amount: row.amount + 1 });
  }
}

/** Point updates and deletes, as an application would issue them: one statement at a time. */
async function burst(
  database: MinnowDatabase,
  reference: Map<number, Row>,
  updates: number,
  deletes: number,
): Promise<void> {
  for (let index = 0; index < updates; index += 1) {
    const id = ((index * 37) % ROWS) + 1;
    const row = reference.get(id);
    if (row === undefined) continue;
    await database.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [id]);
    reference.set(id, { ...row, amount: row.amount + 1 });
  }
  for (let index = 0; index < deletes; index += 1) {
    const id = ((index * 53) % ROWS) + 7;
    if (!reference.has(id)) continue;
    await database.execute("DELETE FROM items WHERE id = ?", [id]);
    reference.delete(id);
  }
}

/** The store's footprint as a reader and as a disk see it. */
async function footprint(store: MemoryBlockStore, database: MinnowDatabase) {
  const stats = await store.getStorageStats();
  const manifests = await manifestRecords(store);
  return {
    visibleSegments: (await allVisibleSegments(database, "items")).length,
    liveBlocks: stats.liveBlockCount,
    storedBlocks: stats.liveBlockCount + stats.obsoleteBlockCount,
    storedBytes: stats.liveBlockBytes + stats.obsoleteBlockBytes,
    manifests: manifests.length,
    unprunedManifests: manifests.filter((manifest) => manifest.prunedAt === undefined).length,
    transactions: (await transactionRecords(store)).length,
  };
}

/**
 * Waits for the background loops to go quiet: no active job, the footprint not moving for half
 * a second, and — because a fold is only a job record once its planning is done, and planning
 * takes longer on a loaded machine — the table no longer due for one. Gives up after a minute,
 * leaving the assertions to say what did not happen.
 */
async function settle(store: MemoryBlockStore, database: MinnowDatabase): Promise<void> {
  let previous = JSON.stringify(await footprint(store, database));
  let quiet = 0;
  for (let attempt = 0; attempt < 1_200 && quiet < 20; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    const activeCompaction = (await database.listCompactionJobs()).some(
      (job) => job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
    );
    const activeCollection = (await database.listGarbageCollectionJobs()).some(
      (job) => job.state === "planned" || job.state === "running",
    );
    const current = await footprint(store, database);
    const due = current.visibleSegments >= 32;
    const serialized = JSON.stringify(current);
    quiet =
      !activeCompaction && !activeCollection && !due && serialized === previous ? quiet + 1 : 0;
    previous = serialized;
  }
}

async function expectContents(database: MinnowDatabase, reference: Map<number, Row>) {
  const rows = (
    await database.query("SELECT id, region, amount FROM items ORDER BY id", { memoize: false })
  ).rows as unknown as Row[];
  const expected = [...reference.values()].sort((left, right) => left.id - right.id);
  expect(rows.length).toBe(expected.length);
  for (let index = 0; index < expected.length; index += 1) {
    const want = expected[index];
    const got = rows[index];
    if (want === undefined || got === undefined) continue;
    if (got.id !== want.id || got.region !== want.region || got.amount !== want.amount) {
      throw new Error(
        `row ${String(index)} diverged: wanted ${JSON.stringify(want)}, got ${JSON.stringify(got)}`,
      );
    }
  }
  const total = expected.reduce((sum, row) => sum + row.amount, 0);
  expect(
    (
      await database.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM items", {
        memoize: false,
      })
    ).rows,
  ).toEqual([{ n: expected.length, total }]);
}

describe("background maintenance", () => {
  it("retries a failed collection on a timer without another committed write", async () => {
    const store = new CollectionFaultStore();
    const database = new MinnowDatabase(store, {
      autoCompact: false,
      autoCollectDebtLimitCommits: 1,
    });
    await database.createTable({
      name: "retry_items",
      uniqueKey: "id",
      columns: [{ name: "id", type: "number" }],
    });
    await database.insert("retry_items", { id: 1 });
    // Seed on real timers so the test is not responsible for the unrelated cooperative yields
    // of setup maintenance; fake time controls the retry interval under test from here on.
    vi.useFakeTimers();
    store.failuresRemaining = 1;
    await expect(database.insert("retry_items", { id: 2 })).rejects.toBeInstanceOf(
      MaintenanceBacklogError,
    );
    const failed = database.maintenanceStatus();
    expect(failed).toMatchObject({
      pendingCommitDebt: 1,
      consecutiveFailures: 1,
      collectionRunning: false,
    });
    expect(failed.lastError?.message).toBe("injected collection failure");
    expect(failed.nextRetryAt).not.toBeNull();
    const attemptsAfterFailure = store.attempts;

    // Drive the retry timer and the collector's zero-delay cooperative yields separately. The
    // synchronous timer advance avoids Vitest waiting on the async collector that those yields
    // themselves unblock.
    vi.advanceTimersByTime(1_000);
    for (let attempt = 0; attempt < 1_000; attempt += 1) {
      await Promise.resolve();
      vi.runOnlyPendingTimers();
      await Promise.resolve();
      const status = database.maintenanceStatus();
      if (!status.collectionRunning && status.nextRetryAt === null) break;
    }
    expect(store.attempts).toBeGreaterThan(attemptsAfterFailure);
    expect(database.maintenanceStatus()).toMatchObject({
      pendingCommitDebt: 0,
      consecutiveFailures: 0,
      collectionRunning: false,
      lastError: null,
      nextRetryAt: null,
    });
    expect((await database.query("SELECT id FROM retry_items ORDER BY id")).rows).toEqual([
      { id: 1 },
    ]);
    await database.close();
  });

  it("requeues beyond one 32-pass run until a large history backlog is empty", async () => {
    const store = new MemoryBlockStore();
    let version: number | null = null;
    // Automatic planning is capped at the same 64-record work page as reclamation. This is
    // deliberately more than the 32-pass per-run ceiling, without constructing a quadratic
    // 32k-manifest fixture merely to prove scheduler requeueing.
    for (let index = 0; index < 32 * 64 + 257; index += 1) {
      const transactionId = `history-${String(index)}`;
      await store.createTransaction({
        id: transactionId,
        ownerId: `owner-${String(index)}`,
        expiresAt: "2025-01-01T00:30:00.000Z",
        snapshotVersion: version,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: "2025-01-01T00:00:00.000Z",
        updatedAt: "2025-01-01T00:00:00.000Z",
        committedVersion: null,
      });
      const manifest = await store.commitTransaction({
        transactionId,
        expectedTransactionRevision: 0,
        expectedManifestVersion: version,
        changedTableIds: [],
        committedAt: "2025-01-01T00:00:00.000Z",
      });
      version = manifest.version;
    }
    const database = new MinnowDatabase(store, {
      autoCompact: false,
      autoCollectDebtLimitCommits: 1,
      now: () => new Date("2026-01-01T00:00:00.000Z"),
    });
    await database.createTable({
      name: "backlog_items",
      uniqueKey: "id",
      columns: [{ name: "id", type: "number" }],
    });
    await database.insert("backlog_items", { id: 1 });
    // Foreground assistance is one bounded run: this old history needs a follow-up, so the
    // attempted write is refused before staging instead of blocking in proportion to the whole
    // backlog. The collector requeues itself and settles the remainder without another write.
    await expect(database.insert("backlog_items", { id: 2 })).rejects.toBeInstanceOf(
      MaintenanceBacklogError,
    );
    expect((await database.query("SELECT COUNT(*) AS n FROM backlog_items")).rows).toEqual([
      { n: 1 },
    ]);
    for (let attempt = 0; attempt < 2_000; attempt += 1) {
      const status = database.maintenanceStatus();
      if (
        status.pendingCommitDebt === 0 &&
        !status.collectionRunning &&
        !status.collectionRequested
      ) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    // Finished marker cleanup may remove the pruned record entirely; either way the oldest
    // version is no longer readable and the >32-pass backlog has settled.
    expect(await store.getManifest(0)).toBeUndefined();
    expect(database.maintenanceStatus()).toMatchObject({
      pendingCommitDebt: 0,
      consecutiveFailures: 0,
      lastError: null,
    });
    await database.insert("backlog_items", { id: 2 });
    expect((await store.listGarbageCollectionJobs()).length).toBeLessThanOrEqual(8);
    expect((await database.query("SELECT COUNT(*) AS n FROM backlog_items")).rows).toEqual([
      { n: 2 },
    ]);
    await database.close();
  }, 120_000);

  it("keeps tombstone provenance across bounded jobs and reopen", async () => {
    class CandidateReadCountingStore extends MemoryBlockStore {
      candidateReadCalls = 0;

      override getBlocks(ids: readonly string[]) {
        this.candidateReadCalls += 1;
        return super.getBlocks(ids);
      }
    }
    const store = new CandidateReadCountingStore();
    const options = {
      autoCollect: false,
      autoCompact: false,
      rowsPerBlock: 4,
      now: () => new Date("2026-01-01T00:00:00.000Z"),
    } as const;
    let database = new MinnowDatabase(store, options);
    await database.createTable({
      name: "wide_history",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "payload", type: "string" },
      ],
    });
    await database.insertBatch(
      "wide_history",
      Array.from({ length: 200 }, (_, id) => ({ id, payload: `row-${String(id)}` })),
    );
    const initialStats = await store.getStorageStats();
    const obsoleteBlockCount = initialStats.liveBlockCount + initialStats.obsoleteBlockCount;
    expect(obsoleteBlockCount).toBeGreaterThan(64);
    await database.dropTable("wide_history");
    expect((await store.getCurrentManifest())?.liveBlockCount).toBe(0);
    await database.close();

    let previous = obsoleteBlockCount;
    for (let pass = 0; pass < 16 && previous > 0; pass += 1) {
      database = new MinnowDatabase(store, options);
      await database.collectGarbage({ maxItemsPerStep: 64, maxPlanningItems: 64 });
      await database.close();
      const stats = await store.getStorageStats();
      const remaining = stats.liveBlockCount + stats.obsoleteBlockCount;
      expect(remaining).toBeLessThan(previous);
      previous = remaining;
    }
    expect(previous).toBe(0);
    expect(store.candidateReadCalls).toBeLessThanOrEqual(2);
    expect(await store.getManifest(0)).toBeUndefined();
  });

  it("folds a burst of writes back down and collects what the folds superseded", async () => {
    const { store, database, reference, clock } = await seeded();
    const fresh = await footprint(store, database);
    await burst(database, reference, 400, 40);
    await settle(store, database);
    const folded = await footprint(store, database);
    // Folded: the scan reads the anchor plus at most the deltas under the fold threshold (32),
    // not one segment per write.
    expect(folded.visibleSegments).toBeLessThanOrEqual(33);
    expect(folded.liveBlocks).toBeLessThanOrEqual(fresh.liveBlocks);
    // Right after the burst the last sixty-four versions are still readable, and they are
    // pre-fold versions that root the burst's blocks: bounded, but not yet small.
    expect(folded.unprunedManifests).toBeLessThanOrEqual(64 + 2);

    await quietMinute(database, reference, clock);
    await settle(store, database);
    const settled = await footprint(store, database);
    // Collected: a minute of quiet later, the store holds the data and little else — not the
    // hundreds of delta blocks and manifests the burst wrote, which is what an uncollected
    // store holds forever. The bound leaves room for the last fold's superseded anchor.
    expect(settled.storedBytes).toBeLessThanOrEqual(fresh.storedBytes * 2);
    expect(settled.storedBlocks).toBeLessThanOrEqual(fresh.storedBlocks + 2 * 32 + 16);
    expect(settled.unprunedManifests).toBeLessThanOrEqual(4);
    // Every surviving committed transaction either owns visible segments or sits in the small
    // retained maintenance/history tail; old per-commit records do not accumulate forever.
    expect(settled.transactions).toBeLessThanOrEqual(settled.visibleSegments + 8);
    // And a fold or a collection pass changed no answer.
    await expectContents(database, reference);
    // The maintenance that ran is visible to the caller as terminal jobs.
    expect((await database.listCompactionJobs()).some((job) => job.state === "published")).toBe(
      true,
    );
    expect(
      (await database.listGarbageCollectionJobs()).some((job) => job.state === "completed"),
    ).toBe(true);
  }, 120_000);

  it("keeps the footprint bounded across repeated bursts", async () => {
    // The property that matters for a tab open all day: a second and third burst settle back
    // to the same place the first did, rather than each leaving a residue the next builds on.
    const { store, database, reference, clock } = await seeded();
    const after: Array<Awaited<ReturnType<typeof footprint>>> = [];
    for (let round = 0; round < 3; round += 1) {
      await burst(database, reference, 200, 20);
      await settle(store, database);
      await quietMinute(database, reference, clock);
      await settle(store, database);
      after.push(await footprint(store, database));
    }
    const [first, , third] = after;
    if (first === undefined || third === undefined) throw new Error("Expected three rounds");
    // Each round settles to the same place, within the last fold's leftovers; without
    // collection each round would add its whole burst to the previous one.
    expect(third.storedBytes).toBeLessThanOrEqual(first.storedBytes * 1.5);
    expect(third.storedBlocks).toBeLessThanOrEqual(first.storedBlocks * 1.5);
    expect(third.unprunedManifests).toBeLessThanOrEqual(64 + 2);
    expect(third.visibleSegments).toBeLessThanOrEqual(33);
    expect(third.transactions).toBeLessThanOrEqual(third.visibleSegments + 8);
    await expectContents(database, reference);
  }, 180_000);

  it("keeps recent versions readable and prunes old ones", async () => {
    const { store, database, reference, clock } = await seeded();
    const early = await store.getCurrentManifestVersion();
    await burst(database, reference, 300, 0);
    await settle(store, database);
    const current = await store.getCurrentManifestVersion();
    if (early === null || current === null) throw new Error("Expected manifest versions");
    // Ten commits back is inside the retained window: a caller naming it still reads it.
    expect((await database.readTable("items", current - 10)).length).toBe(reference.size);
    // The version before the burst is far outside it and has been pruned.
    await expect(database.readTable("items", early)).rejects.toBeInstanceOf(
      SnapshotManifestMissingError,
    );
    // A minute later even the recent one is gone: the window is bounded in age as well.
    await quietMinute(database, reference, clock);
    await settle(store, database);
    await expect(database.readTable("items", current - 10)).rejects.toBeInstanceOf(
      SnapshotManifestMissingError,
    );
  }, 120_000);

  it("still collects when only compaction is disabled", async () => {
    const { store, database, reference, clock } = await seeded({ autoCompact: false });
    await burst(database, reference, 120, 0);
    await quietMinute(database, reference, clock);
    for (let attempt = 0; attempt < 1_000; attempt += 1) {
      const active = (await database.listGarbageCollectionJobs()).some(
        (job) => job.state === "planned" || job.state === "running",
      );
      const status = database.maintenanceStatus();
      if (
        !active &&
        status.pendingCommitDebt === 0 &&
        !status.collectionRunning &&
        !status.collectionRequested
      ) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const idle = await footprint(store, database);
    expect(database.maintenanceStatus()).toMatchObject({
      pendingCommitDebt: 0,
      collectionRunning: false,
      collectionRequested: false,
      consecutiveFailures: 0,
      lastError: null,
    });
    expect(idle.visibleSegments).toBe(4 + 120 + 1);
    expect(await database.listCompactionJobs()).toEqual([]);
    expect((await database.listGarbageCollectionJobs()).length).toBeGreaterThan(0);
    expect(idle.unprunedManifests).toBeLessThanOrEqual(64 + 2);
    await expectContents(database, reference);
  }, 60_000);

  it("does nothing when compaction and collection are both disabled", async () => {
    const { store, database, reference } = await seeded({
      autoCompact: false,
      autoCollect: false,
    });
    await burst(database, reference, 120, 0);
    await new Promise((resolve) => setTimeout(resolve, 200));
    const idle = await footprint(store, database);
    expect(idle.visibleSegments).toBe(4 + 120);
    expect(await database.listCompactionJobs()).toEqual([]);
    expect(await database.listGarbageCollectionJobs()).toEqual([]);
    expect(idle.unprunedManifests).toBe(idle.manifests);
    await expectContents(database, reference);
  }, 60_000);

  it("reclaims a reopened backlog of crashed active writers without another write", async () => {
    vi.useFakeTimers();
    const reopenedAt = new Date("2026-01-01T01:00:00.000Z");
    vi.setSystemTime(reopenedAt);
    const store = new MemoryBlockStore();
    await store.addTable({
      id: "abandoned-table",
      name: "abandoned_rows",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    for (let index = 0; index < 100; index += 1) {
      const id = `abandoned-${String(index).padStart(3, "0")}`;
      const blockId = `${id}/block`;
      const segmentId = `${id}/segment`;
      await store.createTransaction({
        id,
        ownerId: `${id}/owner`,
        expiresAt: "2026-01-01T00:59:59.999Z",
        snapshotVersion: null,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
        committedVersion: null,
      });
      await store.stageTransactionArtifacts({
        transactionId: id,
        expectedRevision: 0,
        blocks: [{ id: blockId, bytes: Uint8Array.of(index) }],
        segments: [
          {
            id: segmentId,
            tableId: "abandoned-table",
            transactionId: id,
            rowCount: 1,
            rowIdStart: BigInt(index + 1),
            rowIdEndExclusive: BigInt(index + 2),
            columnBlockIds: { value: [blockId] },
            kind: "insert",
            level: 0,
            logicalOrder: index,
            commitOrdinal: 0,
            rowIdSpans: [],
            createdAt: "2026-01-01T00:00:00.001Z",
          },
        ],
        updatedAt: "2026-01-01T00:00:00.001Z",
      });
    }

    const reopened = new MinnowDatabase(store, {
      now: () => new Date(Date.now()),
      autoCompact: false,
    });
    const stagedStats = await store.getStorageStats();
    expect(stagedStats.liveBlockCount + stagedStats.obsoleteBlockCount).toBe(100);
    await vi.advanceTimersByTimeAsync(60_000);
    for (let attempt = 0; attempt < 1_000; attempt += 1) {
      const status = reopened.maintenanceStatus();
      if (!status.collectionRunning && !status.collectionRequested) break;
      await vi.advanceTimersByTimeAsync(1);
    }
    const collectedStats = await store.getStorageStats();
    expect(collectedStats.liveBlockCount + collectedStats.obsoleteBlockCount).toBe(0);
    expect(await transactionRecords(store)).toEqual([]);
    await reopened.close();
  });

  it("folds but keeps every version when only collection is off", async () => {
    const { store, database, reference } = await seeded({ autoCollect: false });
    await burst(database, reference, 120, 0);
    await settle(store, database);
    const idle = await footprint(store, database);
    expect(idle.visibleSegments).toBeLessThanOrEqual(33);
    expect(await database.listGarbageCollectionJobs()).toEqual([]);
    expect(idle.unprunedManifests).toBe(idle.manifests);
    await expectContents(database, reference);
  }, 60_000);

  it("keeps the buffer pool inside its budget under many distinct queries", async () => {
    const { database, reference } = await seeded({ bufferPoolBytes: 256 * 1024 });
    await burst(database, reference, 40, 0);
    for (let index = 0; index < 300; index += 1) {
      const low = (index * 13) % ROWS;
      await database.query(
        `SELECT id, region, amount FROM items WHERE id BETWEEN ${String(low)} AND ${String(low + 50)} ORDER BY id`,
        { memoize: index % 2 === 0 },
      );
    }
    const stats = database.bufferPoolStats();
    expect(stats.usedBytes).toBeLessThanOrEqual(stats.limitBytes);
    expect(stats.evictions).toBeGreaterThan(0);
    await expectContents(database, reference);
  }, 60_000);
});
