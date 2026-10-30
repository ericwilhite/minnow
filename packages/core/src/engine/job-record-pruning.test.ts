import { expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabase } from "./database.js";

/**
 * Runs a hook at the start of every full listing of compaction or collection job records, so a
 * test can finish a job on another connection while garbage collection reads them.
 */
class ListingHookStore extends MemoryBlockStore {
  onCompactionListing: (() => Promise<void>) | undefined;
  onCollectionListing: (() => Promise<void>) | undefined;

  override async listCompactionJobPage(
    afterId: Parameters<MemoryBlockStore["listCompactionJobPage"]>[0],
    limit: Parameters<MemoryBlockStore["listCompactionJobPage"]>[1],
  ) {
    if (afterId === null && limit > 1) await this.onCompactionListing?.();
    return super.listCompactionJobPage(afterId, limit);
  }

  override async listGarbageCollectionJobPage(
    afterId: Parameters<MemoryBlockStore["listGarbageCollectionJobPage"]>[0],
    limit: Parameters<MemoryBlockStore["listGarbageCollectionJobPage"]>[1],
  ) {
    if (afterId === null && limit > 1) await this.onCollectionListing?.();
    return super.listGarbageCollectionJobPage(afterId, limit);
  }
}

const published = async (store: MemoryBlockStore): Promise<string[]> =>
  (await store.listCompactionJobs())
    .filter((job) => job.state === "published")
    .map((job) => job.id);

const completed = async (store: MemoryBlockStore): Promise<string[]> =>
  (await store.listGarbageCollectionJobs())
    .filter((job) => job.state === "completed")
    .map((job) => job.id);

// With no finished records before collection starts, or more than it keeps.
it.each([0, 12])(
  "keeps a compaction published while collection prunes finished job records (%i before)",
  async (rounds) => {
    const store = new ListingHookStore();
    const options = { autoCompact: false, autoCollect: false } as const;
    const database = new MinnowDatabase(store, options);
    const other = new MinnowDatabase(store, options);
    await database.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER)");
    let nextId = 0;
    const compactOnOther = async (): Promise<void> => {
      for (let row = 0; row < 3; row += 1) {
        await other.execute("INSERT INTO items VALUES (?, 0)", [nextId]);
        nextId += 1;
      }
      await other.compactTable("items");
    };
    for (let round = 0; round < rounds; round += 1) await compactOnOther();
    expect((await published(store)).length).toBe(rounds);

    // Another connection publishes a compaction each time collection starts reading job records:
    // the last one lands after collection chose which records to keep, and is the newest of all.
    const during: string[] = [];
    const listing = async (): Promise<void> => {
      store.onCompactionListing = undefined;
      const known = new Set(await published(store));
      await compactOnOther();
      during.push(...(await published(store)).filter((id) => !known.has(id)));
      store.onCompactionListing = listing;
    };
    store.onCompactionListing = listing;
    await database.collectGarbage();
    store.onCompactionListing = undefined;

    const after = new Set(await published(store));
    expect(during.length).toBeGreaterThanOrEqual(2);
    for (const id of during) expect(after.has(id)).toBe(true);
    // The old records still go: collection keeps a bounded tail.
    expect(after.size).toBeLessThanOrEqual(8 + during.length);
    await other.close();
    await database.close();
  },
);

it("keeps a collection completed while collection prunes finished job records", async () => {
  const store = new ListingHookStore();
  const options = { autoCompact: false, autoCollect: false } as const;
  const database = new MinnowDatabase(store, options);
  const other = new MinnowDatabase(store, options);
  await database.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER)");
  let nextId = 0;
  const collectOnOther = async (): Promise<void> => {
    await other.execute("INSERT INTO items VALUES (?, 0)", [nextId]);
    nextId += 1;
    await other.collectGarbage();
  };
  for (let round = 0; round < 12; round += 1) await collectOnOther();
  expect((await completed(store)).length).toBeGreaterThanOrEqual(8);

  const during: string[] = [];
  const listing = async (): Promise<void> => {
    store.onCollectionListing = undefined;
    const known = new Set(await completed(store));
    await collectOnOther();
    during.push(...(await completed(store)).filter((id) => !known.has(id)));
    store.onCollectionListing = listing;
  };
  store.onCollectionListing = listing;
  await database.collectGarbage();
  store.onCollectionListing = undefined;

  const after = new Set(await completed(store));
  expect(during.length).toBeGreaterThanOrEqual(2);
  for (const id of during) expect(after.has(id)).toBe(true);
  expect(after.size).toBeLessThanOrEqual(8 + during.length + 1);
  await other.close();
  await database.close();
});
