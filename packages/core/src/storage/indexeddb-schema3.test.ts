/**
 * IndexedDB schema 2 to 3: no stored record changes; the version bars a schema-2 reader from
 * compaction jobs with replayed merge plans, which it cannot parse. The released 0.12.1 package —
 * the last schema-2 writer — leaves a merge-v1 fold in flight; the current build upgrades through
 * the ordinary open API (through schema 3 to the current schema), finishes that fold, keeps
 * writing, and reopens, and the released reader is refused afterwards. The upgrade runs inside
 * IndexedDB's version-change transaction, so an interrupted one leaves schema 2 exactly as it was.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout7";
import { IndexedDbBlockStore as ReleasedIndexedDbStore } from "@minnowdb/core-layout7/storage/indexeddb";
import { MinnowDatabase } from "../engine/database.js";
import { IndexedDbBlockStore } from "./indexeddb.js";

/** The schema this build writes; schema 4 (multi-part postings deltas) followed schema 3. */
const CURRENT_SCHEMA = 4;

function rows(version: number, count = 600) {
  return Array.from({ length: count }, (_, id) => ({
    id,
    label: `label-${String((id + version) % 13)}`,
    amount: id * 10 + version,
  }));
}

function shuffled<T>(values: readonly T[]): T[] {
  const result = [...values];
  let state = 29;
  for (let index = result.length - 1; index > 0; index -= 1) {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31;
    const other = state % (index + 1);
    const value = result[index];
    const swap = result[other];
    if (value === undefined || swap === undefined) continue;
    result[index] = swap;
    result[other] = value;
  }
  return result;
}

function nativeVersion(indexedDB: IDBFactory, name: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => {
      const version = request.result.version;
      request.result.close();
      resolve(version);
    };
    request.onerror = () => reject(request.error ?? new Error("native open failed"));
  });
}

async function releasedFoldInFlight(indexedDB: IDBFactory, name: string) {
  const store = await ReleasedIndexedDbStore.open({ name, indexedDB });
  const database = new ReleasedDatabase(store, { autoCompact: false, autoCollect: false });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("items", rows(0));
  await database.upsertBatch("items", shuffled(rows(1)));
  await database.execute("UPDATE items SET label = 'patched' WHERE id % 5 = 0");
  const progress = await database.compactTableStep("items", { maxBlocks: 1 });
  if (progress.jobId === null || progress.result !== null) {
    throw new Error("Expected the released writer to leave a fold in flight");
  }
  const job = (await database.listCompactionJobs("items")).find(
    (candidate) => candidate.id === progress.jobId,
  );
  expect(job?.rewritePlan.kind).toBe("merge-v1");
  expect(job?.outputBlockIds.length).toBeGreaterThan(0);
  const expected = await database.readTable("items");
  await database.close();
  store.close();
  return { jobId: progress.jobId, expected };
}

describe("IndexedDB schema 2 to 3", () => {
  it("finishes the released 0.12.1 writer's fold in flight, then writes and reopens", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const { jobId, expected } = await releasedFoldInFlight(indexedDB, name);
    expect(await nativeVersion(indexedDB, name)).toBe(2);

    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await database.readTable("items")).toEqual(expected);
    let progress = await database.resumeCompactionJob(jobId, { maxBlocks: 2 });
    while (progress.result === null) {
      progress = await database.resumeCompactionJob(jobId, { maxBlocks: 2 });
    }
    expect(progress.result.compacted).toBe(true);
    expect(await database.readTable("items")).toEqual(expected);

    await database.upsertBatch("items", shuffled(rows(2)));
    const second = await database.compactTable("items", { minimumLevel0Segments: 1 });
    expect(second.compacted).toBe(true);
    const job = second.jobId === undefined ? undefined : await store.getCompactionJob(second.jobId);
    expect(job?.rewritePlan.kind).toBe("merge-v2");
    const latest = await database.readTable("items");
    expect(latest).toHaveLength(600);
    await database.close();
    store.close();

    const reopened = await IndexedDbBlockStore.open({ name, indexedDB });
    const reader = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect(await reader.readTable("items")).toEqual(latest);
    expect((await reopened.checkIntegrity()).ok).toBe(true);
    await reader.close();
    reopened.close();
  });

  it("refuses the released 0.12.1 reader after the upgrade and leaves the current schema in place", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    await releasedFoldInFlight(indexedDB, name);
    (await IndexedDbBlockStore.open({ name, indexedDB })).close();
    await expect(ReleasedIndexedDbStore.open({ name, indexedDB })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_SCHEMA,
      supportedVersion: 2,
      relation: "newer",
    });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect((await store.checkIntegrity()).ok).toBe(true);
    store.close();
  });

  it("closes a released connection that is open when the upgrade arrives", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    await releasedFoldInFlight(indexedDB, name);
    const released = await ReleasedIndexedDbStore.open({ name, indexedDB });
    const current = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    await expect(released.getCurrentManifestVersion()).rejects.toThrow(/connection is closed/);
    expect((await current.listTables()).map(({ name: table }) => table)).toEqual(["items"]);
    current.close();
  });
});
