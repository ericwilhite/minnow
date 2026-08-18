import { IndexedDbBlockStore, WriteConflictError } from "@minnowdb/core/storage";
import { FaultInjectingBlockStore } from "@minnowdb/core/testing";
import {
  MinnowDatabase,
  QueryMemoryBudgetError,
  UniqueConstraintError,
  type BatchValue,
  type QueryResult,
  type SnapshotSession,
} from "@minnowdb/core";
import { TransactionManager } from "../src/transactions/index.js";

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
  staleRecovery: {
    abortedTransactionIds: string[];
    removedBlockIds: string[];
    removedSegmentIds: string[];
    orphanBlockRemoved: boolean;
    orphanSegmentRemoved: boolean;
    livePendingRetained: boolean;
    persistedStates: { active: number; committed: number; aborted: number };
  };
  uniqueKeys: {
    persistedUniqueKey: string | null;
    persistedColumnTypes: string[];
    existingKeyRejected: boolean;
    duplicateKeyRejected: boolean;
    nullKeyRejected: boolean;
    rowsAfterRejections: number;
    concurrentInsertVersionsConsecutive: boolean;
    rowsAfterConcurrentInserts: number;
    competingUpsertCounts: string[];
    competingUpsertRows: number;
    competingUpsertKeptLatest: boolean;
  };
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
    updatePatch: {
      changedColumns: string[];
      segmentKind: string | null;
      patchedColumnBlocks: number;
    };
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
    vectorQuery: {
      spilledNames: string[];
      prepared: { count: number; total: number };
      historical: { count: number; total: number };
      current: { count: number; total: number };
      compacted: { count: number; total: number };
      reopened: { count: number; total: number };
      memoryBudget: {
        prepareRejected: boolean;
        executeRejected: boolean;
        exactSucceeded: boolean;
      };
    };
    l2Compaction: {
      sourceSegments: number[];
      levels: number[];
      ordinals: number[];
      currentValues: number[];
      historicalRows: number;
      reopenedRows: number;
      stablePartitionIds: boolean;
      budgetWithinBounds: boolean;
    };
  };
}

declare global {
  interface Window {
    runTransactionBrowserTest(): Promise<BrowserTransactionResult>;
  }
}

window.runTransactionBrowserTest = async () => {
  const databaseName = `minnow-transactions-${crypto.randomUUID()}`;
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

  const recoveryName = `minnow-response-${crypto.randomUUID()}`;
  const recoveryInner = await IndexedDbBlockStore.open({ name: recoveryName });
  const recoveryStore = new FaultInjectingBlockStore(recoveryInner, (point) => {
    if (point === "afterTransactionCommit") throw new Error("response lost");
  });
  const recoveryTransaction = await new TransactionManager(recoveryStore).begin();
  await recoveryTransaction.stageBlock("saved", Uint8Array.of(3));
  const recoveredManifest = await recoveryTransaction.commit();
  const recoveredBlockIds =
    (await recoveryStore.getManifest(recoveredManifest.version))?.blockIds ?? [];
  const lostResponseRecovered =
    recoveredBlockIds.includes("saved") && recoveryTransaction.status === "committed";
  recoveryStore.close();
  await deleteDatabase(recoveryName);

  // Crash recovery: a transaction whose tab went away is aborted and everything it staged is
  // reclaimed, while a transaction that is merely young keeps its staged work. The clock and the
  // ID generator are injected so "stale" and "live" are decided by the cutoff, not by wall time.
  const staleName = `minnow-stale-${crypto.randomUUID()}`;
  const staleStore = await IndexedDbBlockStore.open({ name: staleName });
  const committedTransaction = await new TransactionManager(staleStore).begin();
  await committedTransaction.stageBlock("committed", Uint8Array.of(6));
  await committedTransaction.commit();
  let recoveryClock = new Date("2026-01-01T00:00:00.000Z");
  let recoveryId = "stale";
  const staleManager = new TransactionManager(staleStore, {
    now: () => recoveryClock,
    createId: () => recoveryId,
  });
  const staleTransaction = await staleManager.begin();
  await staleTransaction.stageBlock("orphan", Uint8Array.of(7));
  await staleTransaction.stageSegment({
    id: "orphan-segment",
    tableId: "orphan-table",
    transactionId: staleTransaction.id,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { probe: ["orphan"] },
    createdAt: recoveryClock.toISOString(),
  });
  recoveryClock = new Date("2026-01-01T01:00:00.000Z");
  recoveryId = "live";
  const liveTransaction = await staleManager.begin();
  await liveTransaction.stageBlock("live-pending", Uint8Array.of(8));
  const recoveryReport = await staleManager.recover({
    staleBefore: new Date("2026-01-01T00:30:00.000Z"),
  });
  const orphanBlockRemoved = (await staleStore.getBlock("orphan")) === undefined;
  const orphanSegmentRemoved = (await staleStore.getSegment("orphan-segment")) === undefined;
  const livePendingRetained = (await staleStore.getBlock("live-pending")) !== undefined;
  const staleRecords = await staleStore.listTransactions();
  const persistedStates = {
    active: staleRecords.filter((record) => record.status === "active").length,
    committed: staleRecords.filter((record) => record.status === "committed").length,
    aborted: staleRecords.filter((record) => record.status === "aborted").length,
  };
  await liveTransaction.abort();
  staleStore.close();
  await deleteDatabase(staleName);

  const libraryName = `minnow-library-${crypto.randomUUID()}`;
  const libraryStore = await IndexedDbBlockStore.open({ name: libraryName });
  const database = new MinnowDatabase(libraryStore, { rowsPerBlock: 2 });
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
  const batch = await database.insertBatch("people", [
    { name: "Ada", score: 10 },
    { name: "Grace", score: 20 },
    { name: "Linus", score: 30 },
  ]);
  const aggregateSql = "SELECT COUNT(*) AS count, SUM(score) AS total FROM people";
  // Pin one snapshot scope across the mutations below: the session must keep returning the
  // pre-mutation aggregate while fresh queries observe every commit.
  let releaseSnapshot!: () => void;
  const snapshotSession = await new Promise<SnapshotSession>((resolveSession, rejectSession) => {
    void database
      .snapshot(async (session) => {
        resolveSession(session);
        await new Promise<void>((resolveRelease) => {
          releaseSnapshot = resolveRelease;
        });
      })
      .catch(rejectSession);
  });
  const prunedNames = (
    await database.query("SELECT name FROM people WHERE score >= 30 ORDER BY name")
  ).rows.map((row) => String(row.name));
  const spilledNames = (
    await database.query("SELECT name FROM people ORDER BY name DESC", {
      spillToStorage: true,
      spillPageRows: 1,
    })
  ).rows.map((row) => String(row.name));
  const upsert = await database.upsertBatch("people", {
    columns: { name: ["Grace", "Katherine"], score: [25, 40] },
  });
  const partialUpdate = await database.update("people", "Grace", { score: 26 });
  // A partial update publishes a narrow immutable patch: an "update" segment carrying only the
  // unique key and the columns that actually changed.
  const patchSegment = await libraryStore.getSegment(partialUpdate.segmentId);
  const projected = await database.readTable("people", { columns: ["name"] });
  const deleted = await database.deleteBatch("people", { keys: ["Ada", "Missing"] });
  const rows = await database.readTable("people");
  const updatedScore = rows.find((row) => row.name === "Grace")?.score;
  const preparedAggregate = summarizeAggregate(await snapshotSession.query(aggregateSql));
  releaseSnapshot();
  const historicalAggregate = summarizeAggregate(
    await database.query(aggregateSql, { version: batch.version }),
  );
  const currentAggregate = summarizeAggregate(await database.query(aggregateSql));
  let prepareRejected = false;
  try {
    await database.query(aggregateSql, {
      executionMemoryBudgetBytes: 8,
      spillToStorage: false,
    });
  } catch (error) {
    prepareRejected = error instanceof QueryMemoryBudgetError;
  }
  // The budgeted aggregate is tiny; a generous budget must execute exactly.
  const executeRejected = prepareRejected;
  const exactSucceeded =
    summarizeAggregate(
      await database.query(aggregateSql, { executionMemoryBudgetBytes: 1_000_000 }),
    ).total === 96;
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
  const compactedAggregate = summarizeAggregate(await database.query(aggregateSql));

  await database.createTable({
    name: "l2_events",
    columns: [{ name: "value", type: "number" }],
  });
  const l2Inserts = [];
  for (let value = 1; value <= 4; value += 1) {
    l2Inserts.push(await database.insert("l2_events", { value }));
  }
  const firstL2 = await database.compactTable("l2_events", {
    targetLevel: 2,
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    maxWriteAmplification: 64,
    outputCompression: "raw",
  });
  const secondL2 = await database.compactTable("l2_events", {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    maxWriteAmplification: 64,
    outputCompression: "raw",
  });
  const l2VisibleIds = (await database.listVisibleSegments("l2_events")).map(
    (segment) => segment.id,
  );
  const l2Table = await libraryStore.getTableByName("l2_events");
  if (l2Table === undefined) throw new Error("L2 browser table is missing");
  const l2VisibleIdSet = new Set(l2VisibleIds);
  const l2Segments = (await libraryStore.listSegments(l2Table.id))
    .filter((segment) => l2VisibleIdSet.has(segment.id))
    .sort((left, right) => (left.partitionOrdinal ?? -1) - (right.partitionOrdinal ?? -1));
  const l2CurrentRows = await database.readTable("l2_events");
  const l2HistoricalRows = await database.readTable("l2_events", l2Inserts[0]?.version);
  const budgetWithinBounds = [firstL2, secondL2].every(
    (result) =>
      result.plannedOutputStoredBytesUpperBound !== undefined &&
      result.maximumOutputStoredBytes !== undefined &&
      result.outputStoredBytes <= result.plannedOutputStoredBytesUpperBound &&
      result.plannedOutputStoredBytesUpperBound <= result.maximumOutputStoredBytes,
  );

  // Atomic write scopes and AFTER triggers over real IndexedDB: the multi-table commit shape
  // (per-table unique-key channels plus trigger derivations) is otherwise only exercised
  // against fake-indexeddb in Node.
  await database.createTable({
    name: "ledger",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "balance", type: "number" },
    ],
  });
  await database.createTable({
    name: "ledger_audit",
    columns: [
      { name: "action", type: "string" },
      { name: "balance", type: "number" },
    ],
  });
  await database.execute(
    "CREATE TRIGGER ledger_insert_audit AFTER INSERT ON ledger BEGIN " +
      "INSERT INTO ledger_audit (action, balance) VALUES ('ins', NEW.balance); END",
  );
  await database.execute(
    "CREATE TRIGGER ledger_update_audit AFTER UPDATE ON ledger BEGIN " +
      "INSERT INTO ledger_audit (action, balance) VALUES ('upd', NEW.balance); END",
  );
  await database.insertBatch("ledger", { columns: { id: [1], balance: [100] } });
  const scopeCommit = await database.write(async (tx) => {
    await tx.updateBatch("ledger", { keys: [1], changes: { balance: [60] } });
    await tx.insertBatch("ledger", { columns: { id: [2], balance: [40] } });
    // Reads inside the scope see the scope's own staged writes.
    return (await tx.query("SELECT SUM(balance) AS total FROM ledger")).rows[0]?.total;
  });
  // A scope that fails publishes nothing, and its key stays free.
  let scopeRolledBack = false;
  try {
    await database.write(async (tx) => {
      await tx.insertBatch("ledger", { columns: { id: [3], balance: [1] } });
      await tx.updateBatch("ledger", { keys: [999], changes: { balance: [0] } });
    });
  } catch {
    scopeRolledBack = true;
  }
  const ledgerAfterScopes = await database.readTable("ledger");
  const ledgerAuditActions = (
    await database.query("SELECT action FROM ledger_audit ORDER BY action")
  ).rows.map((row) => String(row.action));
  // Deleting a key that was never there fires no trigger.
  const ledgerMissingDelete = await database.deleteBatch("ledger", { keys: [2, 404] });
  const ledgerAuditAfterDelete = (await database.query("SELECT COUNT(*) AS n FROM ledger_audit"))
    .rows[0]?.n;

  // Unique-key enforcement and two-connection contention. The key channel must reject bad keys
  // before anything commits, and two connections to one database must resolve the same key
  // between themselves instead of handing the conflict back to the caller.
  await database.createTable({
    name: "unique_probe",
    uniqueKey: "record_id",
    columns: [
      { name: "record_id", type: "number" },
      { name: "score", type: "number" },
      { name: "label", type: "string" },
      { name: "active", type: "boolean" },
      { name: "changed_at", type: "datetime" },
    ],
  });
  const probeDefinition = (await database.listTables()).find(
    (table) => table.name === "unique_probe",
  );
  await database.insertBatch("unique_probe", { columns: probeColumns([1, 2, 3, 4], 0) });
  let existingKeyRejected = false;
  try {
    await database.insertBatch("unique_probe", { columns: probeColumns([1], 20_000) });
  } catch (error) {
    existingKeyRejected = error instanceof UniqueConstraintError;
  }
  let duplicateKeyRejected = false;
  try {
    await database.insertBatch("unique_probe", { columns: probeColumns([90, 90], 30_000) });
  } catch (error) {
    duplicateKeyRejected = error instanceof UniqueConstraintError;
  }
  let nullKeyRejected = false;
  try {
    const nullKeyColumns = probeColumns([91], 40_000);
    nullKeyColumns.record_id = [null];
    await database.upsertBatch("unique_probe", { columns: nullKeyColumns });
  } catch (error) {
    nullKeyRejected = error instanceof TypeError;
  }
  const rowsAfterRejections = (await database.readTable("unique_probe")).length;

  const secondLibraryStore = await IndexedDbBlockStore.open({ name: libraryName });
  const secondDatabase = new MinnowDatabase(secondLibraryStore, { rowsPerBlock: 2 });
  const versionBeforeConcurrentInserts = (await libraryStore.getCurrentManifest())?.version ?? -1;
  const concurrentInserts = await Promise.all([
    database.insertBatch("unique_probe", { columns: probeColumns([10, 11], 1_000) }),
    secondDatabase.insertBatch("unique_probe", { columns: probeColumns([20, 21], 2_000) }),
  ]);
  const concurrentVersions = concurrentInserts
    .map((result) => result.version)
    .sort((left, right) => left - right);
  const concurrentInsertVersionsConsecutive =
    concurrentVersions[0] === versionBeforeConcurrentInserts + 1 &&
    concurrentVersions[1] === versionBeforeConcurrentInserts + 2;
  const rowsAfterConcurrentInserts = (await database.readTable("unique_probe")).length;

  const competingKey = 50;
  const competingUpserts = await Promise.all([
    database.upsertBatch("unique_probe", { columns: probeColumns([competingKey], 50_000) }),
    secondDatabase.upsertBatch("unique_probe", { columns: probeColumns([competingKey], 60_000) }),
  ]);
  // One writer inserts the key, the other rechecks after the conflict and updates it instead.
  const competingUpsertCounts = competingUpserts
    .map((result) => `${String(result.insertedRowCount)}/${String(result.updatedRowCount)}`)
    .sort();
  const lastCompetingUpsert = competingUpserts.reduce((latest, result) =>
    result.version > latest.version ? result : latest,
  );
  const competingRows = (await database.readTable("unique_probe")).filter(
    (row) => row.record_id === competingKey,
  );
  const competingUpsertKeptLatest =
    competingRows[0]?.score ===
    (lastCompetingUpsert === competingUpserts[0] ? 50_000 : 60_000) + competingKey;
  secondLibraryStore.close();
  libraryStore.close();

  const reopenedLibraryStore = await IndexedDbBlockStore.open({ name: libraryName });
  const reopenedDatabase = new MinnowDatabase(reopenedLibraryStore);
  const reopenedL2Rows = await reopenedDatabase.readTable("l2_events");
  const reopenedL2Ids = (await reopenedDatabase.listVisibleSegments("l2_events")).map(
    (segment) => segment.id,
  );
  const reopenedAggregate = summarizeAggregate(await reopenedDatabase.query(aggregateSql));
  // The scope's commit and its trigger derivations are durable across a fresh open, and the
  // catalog still carries the triggers.
  const reopenedLedger = await reopenedDatabase.readTable("ledger");
  const reopenedLedgerAudit = (
    await reopenedDatabase.query("SELECT COUNT(*) AS n FROM ledger_audit")
  ).rows[0]?.n;
  reopenedLibraryStore.close();
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
    staleRecovery: {
      abortedTransactionIds: recoveryReport.abortedTransactionIds,
      removedBlockIds: recoveryReport.removedBlockIds,
      removedSegmentIds: recoveryReport.removedSegmentIds,
      orphanBlockRemoved,
      orphanSegmentRemoved,
      livePendingRetained,
      persistedStates,
    },
    uniqueKeys: {
      persistedUniqueKey: probeDefinition?.uniqueKey ?? null,
      persistedColumnTypes: (probeDefinition?.columns ?? []).map((column) => column.type),
      existingKeyRejected,
      duplicateKeyRejected,
      nullKeyRejected,
      rowsAfterRejections,
      concurrentInsertVersionsConsecutive,
      rowsAfterConcurrentInserts,
      competingUpsertCounts,
      competingUpsertRows: competingRows.length,
      competingUpsertKeptLatest,
    },
    writeScopes: {
      scopeTotal: typeof scopeCommit.result === "number" ? scopeCommit.result : null,
      scopeRolledBack,
      ledgerAfterScopes: ledgerAfterScopes.map((row) => Number(row.balance)).sort(),
      ledgerAuditActions,
      missingKeyDeleted: ledgerMissingDelete.deletedRowCount,
      auditRowsAfterDelete:
        typeof ledgerAuditAfterDelete === "number" ? ledgerAuditAfterDelete : null,
      reopenedLedgerRows: reopenedLedger.length,
      reopenedAuditRows: typeof reopenedLedgerAudit === "number" ? reopenedLedgerAudit : null,
    },
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
      updatePatch: {
        changedColumns: partialUpdate.changedColumns,
        segmentKind: patchSegment?.kind ?? null,
        patchedColumnBlocks: Object.keys(patchSegment?.columnBlockIds ?? {}).length,
      },
      projectedColumns: Object.keys(projected[0] ?? {}),
      deletedRows: deleted.deletedRowCount,
      writeMetricsValid: [batch, upsert, partialUpdate, deleted].every(
        (result) =>
          result.metrics.storedBytes === result.storedBytes &&
          result.metrics.storedBytes > 0 &&
          Number.isFinite(result.metrics.rowsPerSecond) &&
          result.metrics.rowsPerSecond > 0 &&
          Number.isFinite(result.metrics.writeAmplification) &&
          result.metrics.retries >= 0,
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
      vectorQuery: {
        prunedNames,
        spilledNames,
        prepared: preparedAggregate,
        historical: historicalAggregate,
        current: currentAggregate,
        compacted: compactedAggregate,
        reopened: reopenedAggregate,
        memoryBudget: {
          prepareRejected,
          executeRejected,
          exactSucceeded,
        },
      },
      l2Compaction: {
        sourceSegments: [firstL2.sourceSegmentCount, secondL2.sourceSegmentCount],
        levels: l2Segments.map((segment) => segment.level ?? 0),
        ordinals: l2Segments.map((segment) => segment.partitionOrdinal ?? -1),
        currentValues: l2CurrentRows.map((row) => Number(row.value)),
        historicalRows: l2HistoricalRows.length,
        reopenedRows: reopenedL2Rows.length,
        stablePartitionIds:
          l2VisibleIds.length === reopenedL2Ids.length &&
          l2VisibleIds.every((id, index) => id === reopenedL2Ids[index]),
        budgetWithinBounds,
      },
    },
  };
};

/** Five public column types over an arbitrary set of unique keys, in one call. */
function probeColumns(keys: readonly number[], offset: number): Record<string, BatchValue[]> {
  return {
    record_id: [...keys],
    score: keys.map((key) => offset + key),
    label: keys.map((key) => `row-${String(key)}`),
    active: keys.map((key) => key % 2 === 0),
    changed_at: keys.map((key) => new Date(Date.UTC(2026, 0, 1) + key * 1_000)),
  };
}

function summarizeAggregate(result: QueryResult): { count: number; total: number } {
  const count = result.rows[0]?.count;
  const total = result.rows[0]?.total;
  if (typeof count !== "number" || typeof total !== "number") {
    throw new Error("Vector aggregate result is missing");
  }
  return { count, total };
}

const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "Transaction tests ready";

/**
 * Deletes a database, tolerating a transient `blocked`.
 *
 * `close()` returns before the connection is actually gone, and how long that takes is the
 * browser's business: WebKit releases it a beat later than Chromium and Firefox do, so a delete
 * issued right after a close fires `blocked` and then succeeds on its own. Treating `blocked` as
 * a failure is what kept WebKit out of this runner. It is a "not yet", not an error — so wait,
 * and only fail if the connection never actually goes away, which is the case worth reporting.
 */
async function deleteDatabase(name: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(name);
    let blockedTimer: ReturnType<typeof setTimeout> | undefined;
    const settle = (finish: () => void) => {
      if (blockedTimer !== undefined) clearTimeout(blockedTimer);
      finish();
    };
    request.addEventListener("success", () => settle(resolve), { once: true });
    request.addEventListener(
      "error",
      () => settle(() => reject(request.error ?? new Error("Database deletion failed"))),
      { once: true },
    );
    request.addEventListener(
      "blocked",
      () => {
        blockedTimer = setTimeout(
          () => reject(new Error(`Database deletion stayed blocked: ${name}`)),
          10_000,
        );
      },
      { once: true },
    );
  });
}
