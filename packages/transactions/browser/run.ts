import { IndexedDbBlockStore, WriteConflictError } from "@browserdatabase/storage-idb";
import { FaultInjectingBlockStore } from "@browserdatabase/testing";
import { BrowserDatabase } from "@browserdatabase/engine";
import { TransactionManager } from "../src/index.js";

interface BrowserTransactionResult {
  competingCommits: {
    fulfilled: number;
    conflicts: number;
    finalVersion: number | undefined;
    finalBlockIds: string[];
    stableBlockIds: string[];
  };
  lostResponseRecovered: boolean;
  leases: { renewed: boolean; released: boolean };
  rowIdsDisjoint: boolean;
  batchWrite: {
    tables: string[];
    rowCount: number;
    blockCount: number;
    storedBytesPositive: boolean;
    visibleSegments: number;
    upsertInserted: number;
    upsertUpdated: number;
    finalRows: number;
    updatedValue: number | null;
    partialUpdatedRows: number;
    projectedColumns: string[];
    deletedRows: number;
    writeMetricsValid: boolean;
    bufferedRows: number;
    compaction: {
      compacted: boolean;
      sourceSegments: number;
      visibleSegments: number;
      currentRows: number;
      oldSnapshotRows: number;
      physicallyReclaimedBytes: number;
    };
    mutationCompaction: {
      compacted: boolean;
      sourceSegments: number;
      visibleSegments: number;
      currentNames: string[];
      historicalRows: number;
    };
  };
}

declare global {
  interface Window {
    runTransactionBrowserTest(): Promise<BrowserTransactionResult>;
  }
}

window.runTransactionBrowserTest = async () => {
  const databaseName = `browserdatabase-transactions-${crypto.randomUUID()}`;
  const firstStore = await IndexedDbBlockStore.open({ name: databaseName });
  const secondStore = await IndexedDbBlockStore.open({ name: databaseName });
  const first = await new TransactionManager(firstStore).begin();
  const second = await new TransactionManager(secondStore).begin();
  await first.stageBlock("first", Uint8Array.of(1));
  await second.stageBlock("second", Uint8Array.of(2));
  const results = await Promise.allSettled([first.commit(), second.commit()]);
  const fulfilled = results.filter((result) => result.status === "fulfilled").length;
  const conflicts = results.filter(
    (result) => result.status === "rejected" && result.reason instanceof WriteConflictError,
  ).length;
  const stable = await new TransactionManager(firstStore).openSnapshot();
  const loser = first.status === "active" ? first : second;
  await loser.rebase();
  await loser.commit();
  const finalManifest = await firstStore.getCurrentManifest();
  const leaseManager = new TransactionManager(firstStore);
  const leased = await leaseManager.openLeasedSnapshot({ ownerId: "browser-test", ttlMs: 1_000 });
  const originalLease = await firstStore.getLease(leased.leaseId);
  await leased.renew(2_000);
  const renewedLease = await firstStore.getLease(leased.leaseId);
  const leaseRenewed = renewedLease?.revision === (originalLease?.revision ?? -1) + 1;
  await leased.release();
  const leaseReleased = (await firstStore.getLease(leased.leaseId)) === undefined;
  const rowRanges = await Promise.all([
    firstStore.reserveRowIds("browser-table", 4),
    secondStore.reserveRowIds("browser-table", 4),
  ]);
  const rowIdsDisjoint = rowRanges[0].start !== rowRanges[1].start;
  firstStore.close();
  secondStore.close();
  await deleteDatabase(databaseName);

  const recoveryName = `browserdatabase-response-${crypto.randomUUID()}`;
  const recoveryInner = await IndexedDbBlockStore.open({ name: recoveryName });
  const recoveryStore = new FaultInjectingBlockStore(recoveryInner, (point) => {
    if (point === "afterTransactionCommit") throw new Error("response lost");
  });
  const recoveryTransaction = await new TransactionManager(recoveryStore).begin();
  await recoveryTransaction.stageBlock("saved", Uint8Array.of(3));
  const recoveredManifest = await recoveryTransaction.commit();
  const lostResponseRecovered =
    recoveredManifest.blockIds.includes("saved") && recoveryTransaction.status === "committed";
  recoveryStore.close();
  await deleteDatabase(recoveryName);

  const libraryName = `browserdatabase-library-${crypto.randomUUID()}`;
  const libraryStore = await IndexedDbBlockStore.open({ name: libraryName });
  const database = new BrowserDatabase(libraryStore, { rowsPerBlock: 2 });
  await database.createTable({
    name: "people",
    uniqueKey: "name",
    columns: [
      { name: "name", type: "string" },
      { name: "score", type: "number" },
    ],
  });
  await database.createTable({
    name: "events",
    columns: [{ name: "happened", type: "datetime" }],
  });
  const batch = await database.insertBatch("people", {
    columns: { name: ["Ada", "Grace", "Linus"], score: [10, 20, 30] },
  });
  const upsert = await database.upsertBatch("people", {
    columns: { name: ["Grace", "Katherine"], score: [25, 40] },
  });
  const partialUpdate = await database.update("people", "Grace", { score: 26 });
  const projected = await database.readTable("people", { columns: ["name"] });
  const deleted = await database.deleteBatch("people", { keys: ["Ada", "Missing"] });
  const rows = await database.readTable("people");
  const updatedScore = rows.find((row) => row.name === "Grace")?.score;
  const writer = database.bufferedWriter("events", { maxRows: 2, maxAgeMs: 60_000 });
  await writer.add({ happened: new Date("2026-01-01T00:00:00.000Z") });
  const thresholdFlush = await writer.add({ happened: new Date("2026-01-02T00:00:00.000Z") });
  const closeFlush = await writer.close();
  const buffered = thresholdFlush ?? closeFlush;
  if (buffered === undefined) throw new Error("Buffered event batch did not flush");
  await database.insert("events", { happened: new Date("2026-01-03T00:00:00.000Z") });
  const compaction = await database.compactTable("events");
  const bufferedRows = (await database.readTable("events")).length;
  const oldSnapshotRows = (await database.readTable("events", buffered.version)).length;
  const compactedVisibleSegments = (await database.listVisibleSegments("events")).length;
  const tableNames = (await database.listTables()).map((table) => table.name);
  const visibleSegments = (await database.listVisibleSegments("people")).length;
  const mutationCompaction = await database.compactTable("people", { maxBlocksPerStep: 1 });
  const mutationRows = await database.readTable("people");
  const mutationHistoricalRows = await database.readTable("people", batch.version);
  const mutationVisibleSegments = (await database.listVisibleSegments("people")).length;
  libraryStore.close();
  await deleteDatabase(libraryName);

  return {
    competingCommits: {
      fulfilled,
      conflicts,
      finalVersion: finalManifest?.version,
      finalBlockIds: finalManifest?.blockIds ?? [],
      stableBlockIds: stable.listBlockIds(),
    },
    lostResponseRecovered,
    leases: { renewed: leaseRenewed, released: leaseReleased },
    rowIdsDisjoint,
    batchWrite: {
      tables: tableNames,
      rowCount: batch.rowCount,
      blockCount: batch.blockCount,
      storedBytesPositive: batch.storedBytes > 0,
      visibleSegments,
      upsertInserted: upsert.insertedRowCount,
      upsertUpdated: upsert.updatedRowCount,
      finalRows: rows.length,
      updatedValue: typeof updatedScore === "number" ? updatedScore : null,
      partialUpdatedRows: partialUpdate.updatedRowCount,
      projectedColumns: Object.keys(projected[0] ?? {}),
      deletedRows: deleted.deletedRowCount,
      writeMetricsValid: [batch, upsert, partialUpdate, deleted].every(
        (result) => result.metrics.rowsPerSecond > 0 && result.metrics.storedBytes > 0,
      ),
      bufferedRows,
      compaction: {
        compacted: compaction.compacted,
        sourceSegments: compaction.sourceSegmentCount,
        visibleSegments: compactedVisibleSegments,
        currentRows: bufferedRows,
        oldSnapshotRows,
        physicallyReclaimedBytes: compaction.physicallyReclaimedBytes,
      },
      mutationCompaction: {
        compacted: mutationCompaction.compacted,
        sourceSegments: mutationCompaction.sourceSegmentCount,
        visibleSegments: mutationVisibleSegments,
        currentNames: mutationRows.map((row) => String(row.name)),
        historicalRows: mutationHistoricalRows.length,
      },
    },
  };
};

const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "Transaction tests ready";

async function deleteDatabase(name: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(name);
    request.addEventListener("success", () => resolve(), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("Database deletion failed")),
      {
        once: true,
      },
    );
    request.addEventListener("blocked", () => reject(new Error("Database deletion was blocked")), {
      once: true,
    });
  });
}
