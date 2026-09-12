/**
 * A QuotaExceededError raised by IndexedDB on a queued request aborts the whole transaction, and
 * every request awaited after it fails with AbortError. The adapter must report the
 * transaction's own error — the quota refusal — not the AbortError it happened to see first,
 * and must leave the stored state exactly as it was before the refused call.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import {
  EVENTS_TABLE,
  NOW,
  activeTransaction,
  instrumentFactory,
  openStore,
  readRawKeys,
  readRawValue,
  segment,
  stageBlocks,
  type InstrumentedFactory,
} from "./indexeddb-audit-helpers.js";

/** Makes the next request matching `storeName`/`method` fail with a QuotaExceededError. */
function armQuota(
  instrumented: InstrumentedFactory,
  storeName: string,
  method: "put" | "add",
): void {
  let armed = true;
  instrumented.setHook((info) => {
    if (armed && info.storeName === storeName && info.method === method) {
      armed = false;
      return "throw-quota";
    }
    return undefined;
  });
}

async function outcome(run: Promise<unknown>): Promise<unknown> {
  return run.then(
    () => undefined,
    (error: unknown) => error,
  );
}

function expectQuotaError(failure: unknown): void {
  expect(failure).toBeInstanceOf(Error);
  expect((failure as Error).name).toBe("QuotaExceededError");
}

describe("IndexedDB quota refusals", () => {
  it("surfaces a quota error on a journal chunk write and leaves the stage unapplied", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB, name);
    await store.addTable(EVENTS_TABLE);
    let record = activeTransaction("quota", null);
    await store.createTransaction(record);
    record = await stageBlocks(
      store,
      record,
      Array.from({ length: 1_000 }, (_, index) => `q${String(index)}`),
    );
    const headerBefore = await readRawValue(indexedDB, name, "transactions", "quota");
    const ledgerBefore = await readRawValue(indexedDB, name, "statistics", "resource/global");
    const chunk0Before = await readRawValue(indexedDB, name, "transactionJournal", ["quota", 0]);
    const blockKeysBefore = await readRawKeys(indexedDB, name, "blocks");

    // The 64-block stage tops chunk 0 up and opens chunk 1; the second chunk write is refused.
    let puts = 0;
    instrumented.setHook((info) => {
      if (info.storeName === "transactionJournal" && info.method === "put") {
        puts += 1;
        if (puts === 2) return "throw-quota";
      }
      return undefined;
    });
    const stage = () =>
      store.stageTransactionArtifacts({
        transactionId: "quota",
        expectedRevision: record.revision,
        blocks: Array.from({ length: 64 }, (_, index) => ({
          id: `qq${String(index)}`,
          bytes: Uint8Array.of(1),
        })),
        segments: [],
        updatedAt: NOW,
      });
    expectQuotaError(await outcome(stage()));
    instrumented.setHook(undefined);

    expect(await readRawValue(indexedDB, name, "transactions", "quota")).toEqual(headerBefore);
    expect(await readRawValue(indexedDB, name, "statistics", "resource/global")).toEqual(
      ledgerBefore,
    );
    expect(await readRawValue(indexedDB, name, "transactionJournal", ["quota", 0])).toEqual(
      chunk0Before,
    );
    expect(await readRawValue(indexedDB, name, "transactionJournal", ["quota", 1])).toBeUndefined();
    expect(await readRawKeys(indexedDB, name, "blocks")).toEqual(blockKeysBefore);

    // The same stage succeeds afterwards without reopening the store.
    const after = await stage();
    expect(after.pendingBlockIds).toHaveLength(1_064);
    expect(await store.getTransaction("quota")).toEqual(after);
    expect((await store.checkIntegrity()).issues).toEqual([]);
    store.close();
  });

  it("surfaces a quota error on writeTransaction and publishes nothing", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB, name);
    await store.addTable(EVENTS_TABLE);
    await store.createTransaction(activeTransaction("single", null));
    const headerBefore = await readRawValue(indexedDB, name, "transactions", "single");
    const blockKeysBefore = await readRawKeys(indexedDB, name, "blocks");
    const write = () =>
      store.writeTransaction({
        transaction: { id: "single", expectedRevision: 0 },
        expectedManifestVersion: null,
        blocks: [{ id: "single-block", bytes: Uint8Array.of(1) }],
        segments: [segment("single-seg", "single", "single-block")],
        levelZeroSegmentLimits: [{ tableId: "events", limit: 4_096 }],
        committedAt: NOW,
      });

    armQuota(instrumented, "blocks", "add");
    expectQuotaError(await outcome(write()));
    instrumented.setHook(undefined);

    expect(await store.getCurrentManifest()).toBeUndefined();
    expect(await readRawValue(indexedDB, name, "transactions", "single")).toEqual(headerBefore);
    expect(await store.getTransaction("single")).toMatchObject({
      status: "active",
      revision: 0,
      pendingBlockIds: [],
      pendingSegmentIds: [],
    });
    expect(await readRawKeys(indexedDB, name, "blocks")).toEqual(blockKeysBefore);

    const manifest = await write();
    expect(manifest.version).toBe(0);
    expect(await store.getTransaction("single")).toMatchObject({
      status: "committed",
      pendingBlockIds: ["single-block"],
      pendingSegmentIds: ["single-seg"],
    });
    expect(await store.readManifestBlock(0, "single-block")).toEqual(Uint8Array.of(1));
    expect((await store.checkIntegrity()).issues).toEqual([]);
    store.close();
  });

  it("surfaces a quota error on commitTransaction and keeps the record active", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB, name);
    await store.addTable(EVENTS_TABLE);
    await store.createTransaction(activeTransaction("staged", null));
    const staged = await store.stageTransactionArtifacts({
      transactionId: "staged",
      expectedRevision: 0,
      blocks: [{ id: "staged-block", bytes: Uint8Array.of(1) }],
      segments: [segment("staged-seg", "staged", "staged-block")],
      updatedAt: NOW,
    });
    const headerBefore = await readRawValue(indexedDB, name, "transactions", "staged");
    const commit = () =>
      store.commitTransaction({
        transactionId: "staged",
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "events", limit: 4_096 }],
        committedAt: NOW,
      });

    armQuota(instrumented, "manifests", "add");
    expectQuotaError(await outcome(commit()));
    instrumented.setHook(undefined);

    expect(await store.getCurrentManifest()).toBeUndefined();
    expect(await readRawValue(indexedDB, name, "transactions", "staged")).toEqual(headerBefore);
    expect(await store.getTransaction("staged")).toEqual(staged);

    const manifest = await commit();
    expect(manifest.version).toBe(0);
    expect(await store.getTransaction("staged")).toMatchObject({ status: "committed" });
    expect((await store.checkIntegrity()).issues).toEqual([]);
    store.close();
  });

  it("surfaces through the engine as QuotaExceededError with nothing half-applied", async () => {
    const indexedDB = new IDBFactory();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB);
    const database = new MinnowDatabase(store, { autoCompact: false, rowsPerBlock: 16 });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [{ name: "id", type: "number" }],
    });
    await database.insertBatch(
      "items",
      Array.from({ length: 100 }, (_, index) => ({ id: index })),
    );
    const count = async (): Promise<number> => {
      const rows = (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false }))
        .rows as Array<{ n: number }>;
      return rows[0]?.n ?? -1;
    };
    const insert = () =>
      database.insertBatch(
        "items",
        Array.from({ length: 50 }, (_, index) => ({ id: 1_000 + index })),
      );

    armQuota(instrumented, "blocks", "add");
    expectQuotaError(await outcome(insert()));
    instrumented.setHook(undefined);

    expect(await count()).toBe(100);
    await insert();
    expect(await count()).toBe(150);
    expect((await store.checkIntegrity()).issues).toEqual([]);
    await database.close();
  });
});
