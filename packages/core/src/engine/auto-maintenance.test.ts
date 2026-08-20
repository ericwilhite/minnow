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
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { SnapshotManifestMissingError } from "../storage/types.js";
import { MinnowDatabase } from "./database.js";

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
  const blockIds = await store.listBlockIds();
  let storedBytes = 0;
  for (const id of blockIds) storedBytes += (await store.getBlock(id))?.byteLength ?? 0;
  const manifests = await store.listManifests();
  return {
    visibleSegments: (await database.listVisibleSegments("items")).length,
    liveBlocks: (await store.getCurrentManifest())?.blockIds.length ?? 0,
    storedBlocks: blockIds.length,
    storedBytes,
    manifests: manifests.length,
    unprunedManifests: manifests.filter((manifest) => manifest.prunedAt === undefined).length,
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
  for (let attempt = 0; attempt < 1_200 && quiet < 10; attempt += 1) {
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
  it("folds a burst of writes back down and collects what the folds superseded", async () => {
    const { store, database, reference, clock } = await seeded();
    const fresh = await footprint(store, database);
    await burst(database, reference, 400, 40);
    await settle(store, database);
    const folded = await footprint(store, database);
    // Folded: the scan reads a handful of segments, not one per write.
    expect(folded.visibleSegments).toBeLessThanOrEqual(8);
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
    expect(settled.storedBlocks).toBeLessThanOrEqual(fresh.storedBlocks + 16);
    expect(settled.unprunedManifests).toBeLessThanOrEqual(4);
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
    expect(third.visibleSegments).toBeLessThanOrEqual(8);
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

  it("does nothing when told not to", async () => {
    const { store, database, reference } = await seeded({ autoCompact: false });
    await burst(database, reference, 120, 0);
    await new Promise((resolve) => setTimeout(resolve, 200));
    const idle = await footprint(store, database);
    expect(idle.visibleSegments).toBe(4 + 120);
    expect(await database.listCompactionJobs()).toEqual([]);
    expect(await database.listGarbageCollectionJobs()).toEqual([]);
    expect(idle.unprunedManifests).toBe(idle.manifests);
    await expectContents(database, reference);
  }, 60_000);

  it("folds but keeps every version when only collection is off", async () => {
    const { store, database, reference } = await seeded({ autoCollect: false });
    await burst(database, reference, 120, 0);
    await settle(store, database);
    const idle = await footprint(store, database);
    expect(idle.visibleSegments).toBeLessThanOrEqual(8);
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
