/**
 * The MinnowDatabase storage-configuration benchmark, moved nearly verbatim from the
 * old dashboard worker: block write/readback with full value verification, transaction
 * conflict and recovery probes, write-API measurements, and the reference queries with
 * JavaScript baselines. Every database it opens is throwaway — dataset persistence is
 * the datasets page's job now, not a benchmark side effect.
 */
import {
  decodeBlock,
  encodeBlock,
  inspectBlock,
  type ColumnInput,
  type DecodedColumn,
} from "@minnowdb/core/block-format";
import {
  MinnowDatabase,
  UniqueConstraintError,
  type BatchValue,
  type DatabaseRow,
} from "@minnowdb/core";
import { IndexedDbBlockStore, WriteConflictError } from "@minnowdb/core/storage";
import { FaultInjectingBlockStore } from "@minnowdb/core/testing";
import { TransactionManager } from "@minnowdb/core/transactions";
import {
  benchmarkFocuses,
  durabilityModes,
  getScenario,
  scenarioIds,
  type BenchmarkConfig,
  type BenchmarkResult,
  type BlockMeasurement,
  type ColumnDefinition,
  type EntityDefinition,
  type EntityMeasurement,
  type LibraryCheckMeasurement,
  type LibraryTableMeasurement,
  type TransactionCheckMeasurement,
} from "../benchmark.js";
import { canonicalTuples, referenceChecksum, tuplesMatch } from "./canonical.js";
import {
  buildReferenceQueryContext,
  dateField,
  numberField,
  referenceQueryDefinitions,
  stringField,
  type ReferenceQueryDefinition,
} from "./reference-suite.js";
import {
  assertNotCancelled,
  cancelledRuns,
  deleteDatabase,
  describePlatform,
  difference,
  formatRate,
  progress,
  requestMemorySample,
  storageEstimate,
  sum,
} from "./support.js";

interface PendingBlock {
  measurement: BlockMeasurement;
  definition: ColumnDefinition;
  startRow: number;
  entityRows: number;
  scale: number;
}
export async function runStorageBenchmark(
  requestId: string,
  config: BenchmarkConfig,
): Promise<BenchmarkResult> {
  const scenario = getScenario(config.scenario);
  const runId = crypto.randomUUID();
  const databaseName = `minnow-bench-${runId}`;
  const estimateBefore = await storageEstimate();
  // Sampled before any timer starts; the measurement waits for the next garbage collection.
  const memoryBefore = await requestMemorySample(requestId, "Measuring memory before the run");
  const store = await IndexedDbBlockStore.open({
    name: databaseName,
    durability: config.durability,
  });
  const transactionManager = new TransactionManager(store);
  const timings = {
    transactionBegin: 0,
    generate: 0,
    encode: 0,
    write: 0,
    manifestCommit: 0,
    read: 0,
    decode: 0,
    verify: 0,
    aggregate: 0,
    total: 0,
  };
  const started = performance.now();
  const transactionBeginStarted = performance.now();
  const dataTransaction = await transactionManager.begin();
  timings.transactionBegin = performance.now() - transactionBeginStarted;
  const blocks: PendingBlock[] = [];
  const blockIds: string[] = [];
  const entityMeasurements: EntityMeasurement[] = [];
  const totalBlocks = estimateBlockCount(config);
  const totalSteps = totalBlocks * 2 + 4;
  let completed = 0;
  let numericValues = 0;
  let numericSum = 0;

  progress(requestId, {
    phase: "preparing",
    completed,
    total: totalSteps,
    message: `Creating ${scenario.name.toLowerCase()} data`,
  });

  try {
    for (const entity of scenario.entities) {
      const entityRows = entity.rows(config.scale);
      const entityMeasurement: EntityMeasurement = {
        name: entity.name,
        rows: entityRows,
        columns: entity.columns.length,
        blocks: 0,
        encodedBytes: 0,
        storedBytes: 0,
      };
      for (const definition of entity.columns) {
        const rowsPerBlock = blockRows(definition, config.targetBlockBytes);
        for (
          let startRow = 0, part = 0;
          startRow < entityRows;
          startRow += rowsPerBlock, part += 1
        ) {
          assertNotCancelled(requestId);
          const rowCount = Math.min(rowsPerBlock, entityRows - startRow);
          const generateStarted = performance.now();
          const input = generateInput(definition, startRow, rowCount, entityRows, config.scale);
          timings.generate += performance.now() - generateStarted;

          const encodeStarted = performance.now();
          const bytes = await encodeBlock(input, config.compression);
          timings.encode += performance.now() - encodeStarted;
          const description = inspectBlock(bytes);
          const id = `${entity.name}/${definition.name}/${String(part).padStart(5, "0")}`;

          const writeStarted = performance.now();
          await dataTransaction.stageBlock(id, bytes);
          timings.write += performance.now() - writeStarted;

          const measurement: BlockMeasurement = {
            id,
            entity: entity.name,
            column: definition.name,
            type: definition.type,
            rows: rowCount,
            encodedBytes: description.encodedLength,
            storedBytes: bytes.byteLength,
            compressionRatio: description.encodedLength / bytes.byteLength,
            checksum: description.checksum,
            verified: false,
          };
          blocks.push({ measurement, definition, startRow, entityRows, scale: config.scale });
          blockIds.push(id);
          entityMeasurement.blocks += 1;
          entityMeasurement.encodedBytes += description.encodedLength;
          entityMeasurement.storedBytes += bytes.byteLength;
          completed += 1;
          progress(requestId, {
            phase: "writing",
            completed,
            total: totalSteps,
            message: `Saving ${entity.name}.${definition.name}`,
          });
        }
      }
      entityMeasurements.push(entityMeasurement);
    }

    assertNotCancelled(requestId);
    progress(requestId, {
      phase: "committing",
      completed,
      total: totalSteps,
      message: "Finishing the save",
    });
    const commitStarted = performance.now();
    const committedManifest = await dataTransaction.commit();
    timings.manifestCommit += performance.now() - commitStarted;
    completed += 1;
    const committedSnapshot = await transactionManager.openSnapshot(committedManifest.version);

    for (const block of blocks) {
      assertNotCancelled(requestId);
      const readStarted = performance.now();
      const stored = await committedSnapshot.getBlock(block.measurement.id);
      timings.read += performance.now() - readStarted;
      if (stored === undefined)
        throw new Error(`Stored block disappeared: ${block.measurement.id}`);

      const decodeStarted = performance.now();
      const decoded = await decodeBlock(stored);
      timings.decode += performance.now() - decodeStarted;

      const verifyStarted = performance.now();
      verifyBlock(decoded.column, block);
      timings.verify += performance.now() - verifyStarted;

      if (decoded.column.type === "number") {
        const aggregateStarted = performance.now();
        for (const value of decoded.column.values) {
          if (value !== null) {
            numericValues += 1;
            numericSum += value;
          }
        }
        timings.aggregate += performance.now() - aggregateStarted;
      }
      block.measurement.verified = true;
      completed += 1;
      progress(requestId, {
        phase: "reading",
        completed,
        total: totalSteps,
        message: `Loading and checking ${block.measurement.entity}.${block.measurement.column}`,
      });
    }

    const estimateAfter = await storageEstimate();
    const memoryAfter = await requestMemorySample(requestId, "Measuring memory after the run");
    timings.total = performance.now() - started;
    assertNotCancelled(requestId);
    progress(requestId, {
      phase: "transactions",
      completed,
      total: totalSteps,
      message: "Checking transaction conflicts and recovery",
    });
    const persistedDataTransaction = await store.getTransaction(dataTransaction.id);
    const currentManifest = await store.getCurrentManifest();
    const persistedStatus =
      persistedDataTransaction === undefined
        ? "missing"
        : persistedDataTransaction.status === "committed"
          ? "committed"
          : "unexpected";
    const dataCommitPassed =
      persistedStatus === "committed" &&
      persistedDataTransaction?.committedVersion === committedManifest.version &&
      currentManifest?.version === committedManifest.version &&
      currentManifest.blockIds.length === blockIds.length;
    const dataCommitCheck: TransactionCheckMeasurement = {
      id: "data-commit",
      label: "Journaled atomic commit",
      durationMs: timings.manifestCommit,
      passed: dataCommitPassed,
      detail: `${String(blockIds.length)} pending block IDs moved to committed version ${String(committedManifest.version)} with the transaction record`,
    };
    const transactionProbe = await benchmarkTransactions(config.durability, runId);
    const transactionChecks = [dataCommitCheck, ...transactionProbe.checks];
    completed += 1;
    assertNotCancelled(requestId);
    progress(requestId, {
      phase: "library",
      completed,
      total: totalSteps,
      message: "Benchmarking createTable and insertBatch",
    });
    const libraryBenchmark = await benchmarkLibraryWrites(config, runId);
    completed += 1;
    assertNotCancelled(requestId);
    progress(requestId, {
      phase: "queries",
      completed,
      total: totalSteps,
      message: "Running the relational reference query suite",
    });
    const referenceQueries = await benchmarkReferenceQueries(config, runId);
    completed += 1;
    const measuredBlocks = blocks.map(({ measurement }) => measurement);
    const encodedBytes = sum(measuredBlocks.map((block) => block.encodedBytes));
    const storedBytes = sum(measuredBlocks.map((block) => block.storedBytes));
    const result: BenchmarkResult = {
      runId,
      recordedAt: new Date().toISOString(),
      config,
      environment: {
        userAgent: navigator.userAgent,
        platform: describePlatform(navigator.userAgent),
        hardwareConcurrency: navigator.hardwareConcurrency,
      },
      storage: {
        ...(estimateBefore.usage === undefined ? {} : { usageBefore: estimateBefore.usage }),
        ...(estimateAfter.usage === undefined ? {} : { usageAfter: estimateAfter.usage }),
        ...(estimateAfter.quota === undefined ? {} : { quota: estimateAfter.quota }),
        ...(estimateBefore.usage === undefined || estimateAfter.usage === undefined
          ? {}
          : { measuredDelta: estimateAfter.usage - estimateBefore.usage }),
        note: "The browser estimates total site storage. The size of each saved block is exact.",
      },
      memory: {
        agentStatus: memoryAfter.agentStatus,
        agentDetail: memoryAfter.agentDetail,
        agentBytesBefore: memoryBefore.agentBytes,
        agentBytesAfter: memoryAfter.agentBytes,
        agentDeltaBytes: difference(memoryAfter.agentBytes, memoryBefore.agentBytes),
        agentByType: memoryAfter.agentByType,
        heapBytesBefore: memoryBefore.heapBytes,
        heapBytesAfter: memoryAfter.heapBytes,
        heapDeltaBytes: difference(memoryAfter.heapBytes, memoryBefore.heapBytes),
      },
      totals: {
        rows: sum(entityMeasurements.map((entity) => entity.rows)),
        entities: entityMeasurements.length,
        columns: sum(entityMeasurements.map((entity) => entity.columns)),
        blocks: measuredBlocks.length,
        encodedBytes,
        storedBytes,
        compressionRatio: encodedBytes / storedBytes,
      },
      timingsMs: timings,
      kernel: {
        name: "numeric sum verification",
        numericValues,
        sum: numericSum,
      },
      transactions: {
        committedVersion: committedManifest.version,
        journaledBlocks: blockIds.length,
        persistedStatus,
        probeDurationMs: transactionProbe.durationMs,
        checks: transactionChecks,
        passed: transactionChecks.every((check) => check.passed),
      },
      library: libraryBenchmark,
      referenceQueries,
      entities: entityMeasurements,
      blocks: measuredBlocks,
      verified:
        measuredBlocks.every((block) => block.verified) &&
        transactionChecks.every((check) => check.passed) &&
        libraryBenchmark.passed &&
        referenceQueries.passed,
    };
    progress(requestId, {
      phase: "complete",
      completed: totalSteps,
      total: totalSteps,
      message: "Storage, writes, and reference queries pass",
    });
    return result;
  } finally {
    cancelledRuns.delete(requestId);
    store.close();
    await deleteDatabase(databaseName);
  }
}
async function benchmarkTransactions(
  durability: IDBTransactionDurability,
  runId: string,
): Promise<{ durationMs: number; checks: TransactionCheckMeasurement[] }> {
  const databaseName = `minnow-transaction-bench-${runId}`;
  const firstStore = await IndexedDbBlockStore.open({ name: databaseName, durability });
  const secondStore = await IndexedDbBlockStore.open({ name: databaseName, durability });
  const checks: TransactionCheckMeasurement[] = [];
  const started = performance.now();

  try {
    const firstManager = new TransactionManager(firstStore);
    const secondManager = new TransactionManager(secondStore);
    const base = await firstManager.begin();
    await base.stageBlock("base", Uint8Array.of(0));
    await base.commit();
    const stableSnapshot = await firstManager.openSnapshot();

    const left = await firstManager.begin();
    const right = await secondManager.begin();
    await left.stageBlock("left", Uint8Array.of(1));
    await right.stageBlock("right", Uint8Array.of(2));
    const competingStarted = performance.now();
    const competingResults = await Promise.allSettled([left.commit(), right.commit()]);
    const competingDuration = performance.now() - competingStarted;
    const fulfilled = competingResults.filter((result) => result.status === "fulfilled").length;
    const conflicts = competingResults.filter(
      (result) => result.status === "rejected" && result.reason instanceof WriteConflictError,
    ).length;
    const winner = left.status === "committed" ? left : right;
    const winnerRecord = await firstStore.getTransaction(winner.id);
    const winnerManifest = await firstStore.getCurrentManifest();
    const competingPassed =
      fulfilled === 1 &&
      conflicts === 1 &&
      winnerRecord?.status === "committed" &&
      winnerRecord.committedVersion === winnerManifest?.version;
    checks.push({
      id: "competing-writers",
      label: "Two writers, one atomic winner",
      durationMs: competingDuration,
      passed: competingPassed,
      detail: `${String(fulfilled)} commit · ${String(conflicts)} write conflict · visible version ${String(winnerManifest?.version)}`,
    });

    const loser = left.status === "active" ? left : right;
    const rebaseStarted = performance.now();
    await loser.rebase();
    const rebasedManifest = await loser.commit();
    const rebaseDuration = performance.now() - rebaseStarted;
    const rebasedBlocks = (await firstStore.getManifest(rebasedManifest.version))?.blockIds ?? [];
    const rebasePassed =
      rebasedManifest.version === 2 &&
      ["base", "left", "right"].every((id) => rebasedBlocks.includes(id));
    checks.push({
      id: "rebase",
      label: "Losing writer rebases and retries",
      durationMs: rebaseDuration,
      passed: rebasePassed,
      detail: `retry committed version ${String(rebasedManifest.version)} with both writers’ blocks`,
    });

    const snapshotStarted = performance.now();
    const stableBlockIds = stableSnapshot.listBlockIds();
    const stablePassed =
      stableSnapshot.version === 0 &&
      stableBlockIds.length === 1 &&
      stableBlockIds[0] === "base" &&
      (await stableSnapshot.getBlock("left")) === undefined &&
      (await stableSnapshot.getBlock("right")) === undefined;
    checks.push({
      id: "stable-snapshot",
      label: "Older snapshot stays stable",
      durationMs: performance.now() - snapshotStarted,
      passed: stablePassed,
      detail: `snapshot version ${String(stableSnapshot.version)} still exposes only its original block after version ${String(rebasedManifest.version)} commits`,
    });

    let recoveryNow = new Date("2026-01-01T00:00:00Z");
    let recoveryId = "stale";
    const recoveryManager = new TransactionManager(firstStore, {
      now: () => recoveryNow,
      createId: () => recoveryId,
    });
    const stale = await recoveryManager.begin();
    await stale.stageBlock("orphan", Uint8Array.of(3));
    await stale.stageSegment({
      id: "orphan-segment",
      tableId: "probe-table",
      transactionId: stale.id,
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: { probe: ["orphan"] },
      createdAt: recoveryNow.toISOString(),
    });
    recoveryNow = new Date("2026-01-01T01:00:00Z");
    recoveryId = "live";
    const live = await recoveryManager.begin();
    await live.stageBlock("live-pending", Uint8Array.of(4));
    const recoveryStarted = performance.now();
    const recovery = await recoveryManager.recover({
      staleBefore: new Date("2026-01-01T00:30:00Z"),
    });
    const recoveryDuration = performance.now() - recoveryStarted;
    const recoveryPassed =
      recovery.abortedTransactionIds.length === 1 &&
      recovery.abortedTransactionIds[0] === "stale" &&
      recovery.removedBlockIds.length === 1 &&
      recovery.removedBlockIds[0] === "orphan" &&
      recovery.removedSegmentIds.length === 1 &&
      recovery.removedSegmentIds[0] === "orphan-segment" &&
      (await firstStore.getBlock("orphan")) === undefined &&
      (await firstStore.getSegment("orphan-segment")) === undefined &&
      (await firstStore.getBlock("live-pending")) !== undefined;
    checks.push({
      id: "stale-recovery",
      label: "Stale transaction recovery",
      durationMs: recoveryDuration,
      passed: recoveryPassed,
      detail: `${String(recovery.abortedTransactionIds.length)} stale transaction aborted · ${String(recovery.removedSegmentIds.length)} segment and ${String(recovery.removedBlockIds.length)} block removed · live work retained`,
    });

    const statesStarted = performance.now();
    const records = await firstStore.listTransactions();
    const statusCounts = {
      active: records.filter((record) => record.status === "active").length,
      committed: records.filter((record) => record.status === "committed").length,
      aborted: records.filter((record) => record.status === "aborted").length,
    };
    const statesPassed =
      statusCounts.active >= 1 && statusCounts.committed >= 3 && statusCounts.aborted >= 1;
    checks.push({
      id: "persistent-states",
      label: "Transaction states persist",
      durationMs: performance.now() - statesStarted,
      passed: statesPassed,
      detail: `${String(statusCounts.committed)} committed · ${String(statusCounts.active)} active · ${String(statusCounts.aborted)} aborted records`,
    });
    await live.abort();

    const leaseStarted = performance.now();
    const leaseManager = new TransactionManager(firstStore);
    const readerLease = await leaseManager.openLeasedSnapshot({
      ownerId: "benchmark-reader",
      kind: "reader",
      ttlMs: 1_000,
    });
    const backupLease = await leaseManager.openLeasedSnapshot({
      ownerId: "benchmark-backup",
      kind: "backup",
      ttlMs: 1_000,
    });
    await readerLease.renew(2_000);
    await backupLease.renew(2_000);
    const persistedLeases = await firstStore.listLeases();
    const leasesPassed =
      persistedLeases.length === 2 &&
      persistedLeases.some((lease) => lease.kind === "reader") &&
      persistedLeases.some((lease) => lease.kind === "backup") &&
      persistedLeases.every((lease) => lease.revision === 1);
    await readerLease.release();
    await backupLease.release();
    checks.push({
      id: "leases",
      label: "Reader and backup leases renew",
      durationMs: performance.now() - leaseStarted,
      passed: leasesPassed && (await firstStore.listLeases()).length === 0,
      detail: "reader and backup snapshots renewed once, persisted, then released",
    });

    const faultStore = new FaultInjectingBlockStore(firstStore, (point) => {
      if (point === "afterTransactionCommit") throw new Error("simulated lost response");
    });
    const responseTransaction = await new TransactionManager(faultStore).begin();
    await responseTransaction.stageBlock("response-lost", Uint8Array.of(5));
    const responseStarted = performance.now();
    const recoveredManifest = await responseTransaction.commit();
    const responseDuration = performance.now() - responseStarted;
    const recoveredRecord = await firstStore.getTransaction(responseTransaction.id);
    const responsePassed =
      responseTransaction.status === "committed" &&
      recoveredRecord?.status === "committed" &&
      recoveredRecord.committedVersion === recoveredManifest.version &&
      ((await firstStore.getManifest(recoveredManifest.version))?.blockIds.includes(
        "response-lost",
      ) ??
        false);
    checks.push({
      id: "lost-response",
      label: "Lost commit response is reconciled",
      durationMs: responseDuration,
      passed: responsePassed,
      detail: `commit response was dropped; persisted transaction recovered version ${String(recoveredManifest.version)}`,
    });
  } finally {
    firstStore.close();
    secondStore.close();
    await deleteDatabase(databaseName);
  }

  return { durationMs: performance.now() - started, checks };
}
async function benchmarkLibraryWrites(
  config: BenchmarkConfig,
  runId: string,
): Promise<BenchmarkResult["library"]> {
  const scenario = getScenario(config.scenario);
  const databaseName = `minnow-library-bench-${runId}`;
  const store = await IndexedDbBlockStore.open({
    name: databaseName,
    durability: config.durability,
  });
  const rowsPerBlock = Math.max(1, Math.min(100_000, Math.floor(config.targetBlockBytes / 8.125)));
  const maxBatchRows = 100_000;
  const database = new MinnowDatabase(store, {
    compression: config.compression,
    rowsPerBlock,
    maxCommitRetries: 8,
  });
  const timings = {
    createTables: 0,
    generateBatches: 0,
    insertBatches: 0,
    upsertBatch: 0,
    updateBatch: 0,
    deleteBatch: 0,
    projectedRead: 0,
    verifyInventory: 0,
    total: 0,
  };
  const checks: LibraryCheckMeasurement[] = [];
  const tables: LibraryTableMeasurement[] = [];
  const started = performance.now();
  let finalVersion = -1;
  let probeDurationMs: number;
  let upsertMeasurement: {
    sampleRows: number;
    insertedRows: number;
    updatedRows: number;
    finalRows: number;
    baseVersion: number;
    version: number;
  };
  let mutationMeasurement: {
    updatedRows: number;
    deletedRows: number;
    projectedRows: number;
    updateVersion: number;
    deleteVersion: number | null;
    updateStoredBytes: number;
    deleteStoredBytes: number;
  };

  try {
    const createStarted = performance.now();
    for (const entity of scenario.entities) {
      await database.createTable({
        name: entity.name,
        columns: entity.columns.map((column) => ({
          name: column.name,
          type: column.type,
          nullable: true,
        })),
      });
    }
    timings.createTables = performance.now() - createStarted;

    for (const entity of scenario.entities) {
      const entityRows = entity.rows(config.scale);
      const table: LibraryTableMeasurement = {
        name: entity.name,
        rows: 0,
        segments: 0,
        blocks: 0,
        storedBytes: 0,
      };
      for (let startRow = 0; startRow < entityRows; startRow += maxBatchRows) {
        const rowCount = Math.min(maxBatchRows, entityRows - startRow);
        const generateStarted = performance.now();
        const columns = generateBatchColumns(entity, startRow, rowCount, entityRows, config.scale);
        timings.generateBatches += performance.now() - generateStarted;
        const insertStarted = performance.now();
        const result = await database.insertBatch(entity.name, { columns });
        timings.insertBatches += performance.now() - insertStarted;
        table.rows += result.rowCount;
        table.segments += 1;
        table.blocks += result.blockCount;
        table.storedBytes += result.storedBytes;
        finalVersion = result.version;
      }
      tables.push(table);
    }

    const inventoryStarted = performance.now();
    const tableDefinitions = await database.listTables();
    const tableInventoryPassed =
      tableDefinitions.length === scenario.entities.length &&
      tableDefinitions.every(
        (table) =>
          scenario.entities.some((entity) => entity.name === table.name) &&
          table.columns.every((column) =>
            ["boolean", "number", "string", "datetime"].includes(column.type),
          ),
      );
    checks.push({
      id: "table-inventory",
      label: "Persistent table definitions",
      durationMs: performance.now() - inventoryStarted,
      passed: tableInventoryPassed,
      detail: `${String(tableDefinitions.length)} tables · public types limited to boolean, number, string, and datetime`,
    });

    const segmentStarted = performance.now();
    let visibleSegments = 0;
    let visibleRows = 0;
    for (const table of tables) {
      const segments = await database.listVisibleSegments(table.name);
      visibleSegments += segments.length;
      visibleRows += sum(segments.map((segment) => segment.rowCount));
    }
    timings.verifyInventory = performance.now() - inventoryStarted;
    const expectedSegments = sum(tables.map((table) => table.segments));
    const expectedRows = sum(tables.map((table) => table.rows));
    checks.push({
      id: "segment-inventory",
      label: "Immutable segments are visible",
      durationMs: performance.now() - segmentStarted,
      passed: visibleSegments === expectedSegments && visibleRows === expectedRows,
      detail: `${String(visibleSegments)} segments expose ${String(visibleRows)} inserted rows at manifest version ${String(finalVersion)}`,
    });

    const probeStarted = performance.now();
    const upsertRows = Math.min(1_000, Math.max(10, config.rows));
    const upsertTableName = "unique_key_probe";
    await database.createTable({
      name: upsertTableName,
      uniqueKey: "record_id",
      columns: [
        { name: "record_id", type: "number" },
        { name: "score", type: "number" },
        { name: "label", type: "string" },
        { name: "active", type: "boolean" },
        { name: "changed_at", type: "datetime" },
      ],
    });
    const uniqueSchemaStarted = performance.now();
    const uniqueDefinition = (await database.listTables()).find(
      (table) => table.name === upsertTableName,
    );
    checks.push({
      id: "unique-schema",
      label: "Simple unique-key metadata persists",
      durationMs: performance.now() - uniqueSchemaStarted,
      passed:
        uniqueDefinition?.uniqueKey === "record_id" &&
        uniqueDefinition.columns.every((column) =>
          ["boolean", "number", "string", "datetime"].includes(column.type),
        ),
      detail: "record_id is the unique key; public schema types remain unchanged",
    });

    const seedKeys = Array.from({ length: upsertRows }, (_, index) => index + 1);
    const seed = await database.insertBatch(upsertTableName, {
      columns: generateUpsertColumns(seedKeys, 0),
    });
    const updatedRows = Math.floor(upsertRows / 2);
    const insertedRows = upsertRows - updatedRows;
    const upsertKeys = [
      ...seedKeys.slice(0, updatedRows),
      ...Array.from({ length: insertedRows }, (_, index) => upsertRows + index + 1),
    ];
    const upsertStarted = performance.now();
    const upsert = await database.upsertBatch(upsertTableName, {
      columns: generateUpsertColumns(upsertKeys, 10_000),
    });
    timings.upsertBatch = performance.now() - upsertStarted;
    const rowsAfterUpsert = await database.readTable(upsertTableName);
    upsertMeasurement = {
      sampleRows: upsertRows,
      insertedRows: upsert.insertedRowCount,
      updatedRows: upsert.updatedRowCount,
      finalRows: rowsAfterUpsert.length,
      baseVersion: seed.version,
      version: upsert.version,
    };
    checks.push({
      id: "upsert-counts",
      label: "upsertBatch reports inserted and updated rows",
      durationMs: timings.upsertBatch,
      passed:
        upsert.insertedRowCount === insertedRows &&
        upsert.updatedRowCount === updatedRows &&
        rowsAfterUpsert.length === upsertRows + insertedRows,
      detail: `${String(upsert.insertedRowCount)} inserted · ${String(upsert.updatedRowCount)} updated · ${String(rowsAfterUpsert.length)} rows visible`,
    });

    const patchKeys = seedKeys.slice(0, Math.min(100, seedKeys.length));
    const updateStarted = performance.now();
    const partialUpdate = await database.updateBatch(upsertTableName, {
      keys: patchKeys,
      changes: { score: patchKeys.map((key) => 90_000 + key) },
    });
    timings.updateBatch = performance.now() - updateStarted;
    const probeTable = (await store.listTables()).find((table) => table.name === upsertTableName);
    const updateSegment = (await store.listSegments(probeTable?.id)).find(
      (segment) => segment.id === partialUpdate.segmentId,
    );
    checks.push({
      id: "update-patches",
      label: "Partial updates write narrow immutable patches",
      durationMs: timings.updateBatch,
      passed:
        partialUpdate.updatedRowCount === patchKeys.length &&
        partialUpdate.changedColumns.join(",") === "score" &&
        updateSegment?.kind === "update" &&
        Object.keys(updateSegment.columnBlockIds).length === 2,
      detail: `${String(partialUpdate.updatedRowCount)} rows updated with key + score blocks only at version ${String(partialUpdate.version)}`,
    });

    const projectedReadStarted = performance.now();
    const projectedRows = await database.readTable(upsertTableName, {
      columns: ["record_id", "score"],
    });
    timings.projectedRead = performance.now() - projectedReadStarted;
    const projectedFirst = projectedRows.find((row) => row.record_id === patchKeys[0]);
    checks.push({
      id: "projected-read",
      label: "Projected reads skip unrelated columns",
      durationMs: timings.projectedRead,
      passed:
        projectedRows.length === rowsAfterUpsert.length &&
        projectedRows.every((row) => Object.keys(row).join(",") === "record_id,score") &&
        projectedFirst?.score === 90_000 + (patchKeys[0] ?? 0),
      detail: `${String(projectedRows.length)} rows loaded with 2 of 5 public columns`,
    });

    const deleteKeys = upsertKeys.slice(
      updatedRows,
      updatedRows + Math.min(50, Math.max(1, insertedRows)),
    );
    const deleteStarted = performance.now();
    const deletion = await database.deleteBatch(upsertTableName, { keys: deleteKeys });
    timings.deleteBatch = performance.now() - deleteStarted;
    const rowsAfterDelete = await database.readTable(upsertTableName, {
      columns: ["record_id"],
    });
    const remainingKeys = new Set(rowsAfterDelete.map((row) => row.record_id));
    checks.push({
      id: "delete-keys",
      label: "Key deletes publish immutable markers",
      durationMs: timings.deleteBatch,
      passed:
        deletion.deletedRowCount === deleteKeys.length &&
        deleteKeys.every((key) => !remainingKeys.has(key)),
      detail: `${String(deletion.deletedRowCount)} keys removed at version ${String(deletion.version)}`,
    });

    const measuredWrites = [seed, upsert, partialUpdate, deletion];
    const writeMetricsPassed = measuredWrites.every(
      (result) =>
        result.metrics.storedBytes === result.storedBytes &&
        Number.isFinite(result.metrics.rowsPerSecond) &&
        result.metrics.rowsPerSecond > 0 &&
        Number.isFinite(result.metrics.writeAmplification) &&
        result.metrics.retries >= 0,
    );
    checks.push({
      id: "write-metrics",
      label: "Write metrics expose cost and contention",
      durationMs: 0,
      passed: writeMetricsPassed,
      detail: `insert ${formatRate(seed.metrics.rowsPerSecond)} · upsert ${formatRate(upsert.metrics.rowsPerSecond)} · update ${formatRate(partialUpdate.metrics.rowsPerSecond)} · delete ${formatRate(deletion.metrics.rowsPerSecond)}`,
    });
    mutationMeasurement = {
      updatedRows: partialUpdate.updatedRowCount,
      deletedRows: deletion.deletedRowCount,
      projectedRows: projectedRows.length,
      updateVersion: partialUpdate.version,
      deleteVersion: deletion.version,
      updateStoredBytes: partialUpdate.storedBytes,
      deleteStoredBytes: deletion.storedBytes,
    };

    const constraintStarted = performance.now();
    let existingRejected = false;
    let duplicateRejected = false;
    let nullRejected = false;
    try {
      await database.insertBatch(upsertTableName, {
        columns: generateUpsertColumns([seedKeys[0] ?? 1], 20_000),
      });
    } catch (error) {
      existingRejected = error instanceof UniqueConstraintError;
    }
    try {
      await database.insertBatch(upsertTableName, {
        columns: generateUpsertColumns([upsertRows * 4, upsertRows * 4], 30_000),
      });
    } catch (error) {
      duplicateRejected = error instanceof UniqueConstraintError;
    }
    try {
      const columns = generateUpsertColumns([upsertRows * 5], 40_000);
      columns.record_id = [null];
      await database.upsertBatch(upsertTableName, { columns });
    } catch (error) {
      nullRejected = error instanceof TypeError;
    }
    checks.push({
      id: "unique-errors",
      label: "Invalid unique keys are rejected",
      durationMs: performance.now() - constraintStarted,
      passed: existingRejected && duplicateRejected && nullRejected,
      detail: "saved duplicates, within-batch duplicates, and null keys all failed before commit",
    });

    const snapshotReadStarted = performance.now();
    const oldRows = await database.readTable(upsertTableName, seed.version);
    const oldFirst = oldRows.find((row) => row.record_id === 1);
    const currentFirst = rowsAfterUpsert.find((row) => row.record_id === 1);
    const upsertSegments = await database.listVisibleSegments(upsertTableName, upsert.version);
    checks.push({
      id: "snapshot-read",
      label: "readTable preserves older values",
      durationMs: performance.now() - snapshotReadStarted,
      passed:
        oldRows.length === upsertRows &&
        oldFirst?.score === 1 &&
        currentFirst?.score === 10_001 &&
        upsertSegments.length === 2,
      detail: `version ${String(seed.version)} retains the seed value while version ${String(upsert.version)} reads the immutable upsert segment`,
    });

    const compactionStarted = performance.now();
    const compactionTableName = "append_compaction_probe";
    await database.createTable({
      name: compactionTableName,
      columns: [
        { name: "record_id", type: "number" },
        { name: "label", type: "string" },
      ],
    });
    await database.insertBatch(compactionTableName, {
      columns: { record_id: [1, 2], label: ["one", "two"] },
    });
    const compactionSource = await database.insertBatch(compactionTableName, {
      columns: { record_id: [3, 4], label: ["three", "four"] },
    });
    const sourceBlockIds = new Set(await store.listBlockIds());
    const compaction = await database.compactTable(compactionTableName);
    const compactedRows = await database.readTable(compactionTableName);
    const historicalRows = await database.readTable(compactionTableName, compactionSource.version);
    const compactedSegments = await database.listVisibleSegments(compactionTableName);
    const retainedSourceBlocks = (await store.listBlockIds()).filter((id) =>
      sourceBlockIds.has(id),
    );
    checks.push({
      id: "append-compaction",
      label: "Append compaction preserves snapshots",
      durationMs: performance.now() - compactionStarted,
      passed:
        compaction.compacted &&
        compaction.sourceSegmentCount === 2 &&
        compactedSegments.length === 1 &&
        compactedRows.length === 4 &&
        historicalRows.length === 4 &&
        retainedSourceBlocks.length === sourceBlockIds.size,
      detail: `${String(compaction.sourceSegmentCount)} append segments → ${String(compactedSegments.length)} current segment; historical blocks retained for old snapshots`,
    });

    const secondStore = await IndexedDbBlockStore.open({
      name: databaseName,
      durability: config.durability,
    });
    try {
      const secondDatabase = new MinnowDatabase(secondStore, {
        compression: config.compression,
        rowsPerBlock,
        maxCommitRetries: 8,
      });
      const target = scenario.entities[0];
      if (target === undefined) throw new Error("Benchmark scenario has no tables");
      const targetRows = target.rows(config.scale);
      const leftColumns = generateBatchColumns(target, targetRows, 1, targetRows + 2, config.scale);
      const rightColumns = generateBatchColumns(
        target,
        targetRows + 1,
        1,
        targetRows + 2,
        config.scale,
      );
      const versionBeforeConcurrentInserts = (await store.getCurrentManifest())?.version ?? -1;
      const concurrentStarted = performance.now();
      const concurrentResults = await Promise.all([
        database.insertBatch(target.name, { columns: leftColumns }),
        secondDatabase.insertBatch(target.name, { columns: rightColumns }),
      ]);
      const concurrentDuration = performance.now() - concurrentStarted;
      const versions = concurrentResults.map((result) => result.version).sort((a, b) => a - b);
      const concurrentPassed =
        versions.length === 2 &&
        versions[0] === versionBeforeConcurrentInserts + 1 &&
        versions[1] === versionBeforeConcurrentInserts + 2;
      checks.push({
        id: "concurrent-inserts",
        label: "Competing insertBatch calls retry safely",
        durationMs: concurrentDuration,
        passed: concurrentPassed,
        detail: `two connections committed versions ${versions.join(" and ")} without caller retries`,
      });

      const rowIdStarted = performance.now();
      const tableRecord = (await store.listTables()).find((table) => table.name === target.name);
      const segments = await store.listSegments(tableRecord?.id);
      const ranges = segments
        .map((segment) => ({ start: segment.rowIdStart, end: segment.rowIdEndExclusive }))
        .sort((left, right) => (left.start < right.start ? -1 : 1));
      const rangesPassed = ranges.every(
        (range, index) => index === 0 || (ranges[index - 1]?.end ?? 0n) <= range.start,
      );
      checks.push({
        id: "row-id-ranges",
        label: "Internal row-ID ranges do not overlap",
        durationMs: performance.now() - rowIdStarted,
        passed: rangesPassed,
        detail: `${String(ranges.length)} segment ranges allocated atomically; integer sizing remains internal`,
      });

      const competingUpsertKey = upsertRows * 10;
      const competingUpsertStarted = performance.now();
      const competingUpserts = await Promise.all([
        database.upsertBatch(upsertTableName, {
          columns: generateUpsertColumns([competingUpsertKey], 50_000),
        }),
        secondDatabase.upsertBatch(upsertTableName, {
          columns: generateUpsertColumns([competingUpsertKey], 60_000),
        }),
      ]);
      const competingCounts = competingUpserts
        .map((result) => `${String(result.insertedRowCount)}/${String(result.updatedRowCount)}`)
        .sort();
      const latestResult = competingUpserts.reduce((latest, result) =>
        result.version > latest.version ? result : latest,
      );
      const expectedScore =
        latestResult === competingUpserts[0]
          ? 50_000 + competingUpsertKey
          : 60_000 + competingUpsertKey;
      const finalUpsertRows = await database.readTable(upsertTableName);
      const matchingRows = finalUpsertRows.filter((row) => row.record_id === competingUpsertKey);
      checks.push({
        id: "concurrent-upserts",
        label: "Competing upserts recheck keys after conflicts",
        durationMs: performance.now() - competingUpsertStarted,
        passed:
          competingCounts.join(",") === "0/1,1/0" &&
          matchingRows.length === 1 &&
          matchingRows[0]?.score === expectedScore,
        detail: `one insert and one update committed as versions ${competingUpserts
          .map((result) => result.version)
          .sort((a, b) => a - b)
          .join(" and ")}`,
      });
    } finally {
      secondStore.close();
    }
    probeDurationMs = performance.now() - probeStarted;
    timings.total = performance.now() - started;
  } finally {
    store.close();
    await deleteDatabase(databaseName);
  }

  return {
    rowsPerBlock,
    maxBatchRows,
    finalVersion,
    probeDurationMs,
    timingsMs: timings,
    upsert: upsertMeasurement,
    mutations: mutationMeasurement,
    totals: {
      tables: tables.length,
      rows: sum(tables.map((table) => table.rows)),
      segments: sum(tables.map((table) => table.segments)),
      blocks: sum(tables.map((table) => table.blocks)),
      storedBytes: sum(tables.map((table) => table.storedBytes)),
    },
    tables,
    checks,
    passed: checks.every((check) => check.passed),
  };
}
async function benchmarkReferenceQueries(
  config: BenchmarkConfig,
  runId: string,
): Promise<BenchmarkResult["referenceQueries"]> {
  const databaseName = `mdb-storage-suite-reference-${runId}`;
  const store = await IndexedDbBlockStore.open({
    name: databaseName,
    durability: config.durability,
  });
  const database = new MinnowDatabase(store, {
    compression: config.compression,
    rowsPerBlock: Math.max(1, Math.min(50_000, Math.floor(config.targetBlockBytes / 16))),
  });
  const relationalScenario = getScenario("commerce");
  const orderRows =
    relationalScenario.entities.find((entity) => entity.name === "orders")?.rows(config.scale) ?? 0;
  const tableMetadata: Array<{
    name: string;
    rows: number;
    loadMs: number;
    relationship: string;
  }> = relationalScenario.entities.map((entity) => ({
    name: entity.name,
    rows: entity.rows(config.scale),
    loadMs: 0,
    relationship: entity.relationship ?? "supporting relation",
  }));
  const setupStarted = performance.now();

  try {
    await createReferenceTables(database, relationalScenario.entities);
    await insertReferenceDataset(database, relationalScenario.entities, config.scale);
    const setupMs = performance.now() - setupStarted;
    const integrityStarted = performance.now();
    const integrityChecks = await validatePersistedReferenceDataset(database, tableMetadata);
    let loadMs = performance.now() - integrityStarted;
    let indexMs = 0;
    const queries = referenceQueryDefinitions(orderRows);
    const measurements: BenchmarkResult["referenceQueries"]["queries"] = [];
    const sampleCount = 7;

    for (const query of queries) {
      const measured = await measureReferenceQueryFromStorage(database, query, sampleCount);
      loadMs += measured.loadMs;
      indexMs += measured.indexMs;
      measurements.push(measured.measurement);
    }

    const engineSupportedQueries = measurements.filter((query) => query.engine.supported).length;
    return {
      orderRows,
      totalRows: sum(tableMetadata.map((table) => table.rows)),
      setupMs,
      loadMs,
      indexMs,
      totalMs: loadMs + indexMs + sum(measurements.map((query) => query.javascript.measurementMs)),
      engineTotalMs: sum(measurements.map((query) => query.engine.medianMs)),
      engineSupportedQueries,
      sampleCount,
      tables: tableMetadata,
      integrityChecks,
      queries: measurements,
      passed:
        integrityChecks.every((check) => check.passed) &&
        measurements.every((query) => query.verified),
      note: `Each query is submitted to MinnowDatabase as SQL and executed through the public prepareQuery()/execute() API; ${String(engineSupportedQueries)} of ${String(measurements.length)} compile against the current SQL surface, and the rest record the engine's own error. Engine results are verified tuple for tuple against an independent JavaScript oracle, with numbers compared inside a relative tolerance because aggregates accumulate in a different row order. The hand-written JavaScript column beside each engine timing is a baseline over rows already materialized in memory, not a second engine: it excludes the storage read the engine timing also excludes, but it does no planning and keeps whole tables resident. Validation and each query load bounded table subsets and release them before the next query, so the complete 50-table dataset is never one JavaScript object graph.`,
    };
  } finally {
    store.close();
    await deleteDatabase(databaseName);
  }
}
async function measureReferenceQueryFromStorage(
  database: MinnowDatabase,
  query: ReferenceQueryDefinition,
  sampleCount: number,
): Promise<{
  loadMs: number;
  indexMs: number;
  measurement: BenchmarkResult["referenceQueries"]["queries"][number];
}> {
  const loadStarted = performance.now();
  const tableEntries: Array<[string, DatabaseRow[]]> = [];
  for (const table of query.tables) tableEntries.push([table, await database.readTable(table)]);
  const materialized = new Map(tableEntries);
  const loadMs = performance.now() - loadStarted;
  const indexStarted = performance.now();
  const context = buildReferenceQueryContext(materialized);
  const indexMs = performance.now() - indexStarted;
  const warmupStarted = performance.now();
  let result = query.baseline(context);
  const warmupMs = performance.now() - warmupStarted;
  const executionsPerSample = Math.min(
    5_000,
    Math.max(1, Math.ceil(10 / Math.max(warmupMs, 0.002))),
  );
  const samples: number[] = [];
  let measurementMs = 0;
  for (let sample = 0; sample < sampleCount; sample += 1) {
    const computeStarted = performance.now();
    for (let execution = 0; execution < executionsPerSample; execution += 1) {
      result = query.baseline(context);
    }
    const elapsed = performance.now() - computeStarted;
    measurementMs += elapsed;
    samples.push(elapsed / executionsPerSample);
  }
  samples.sort((left, right) => left - right);
  const medianMs = samples[Math.floor(samples.length / 2)] ?? 0;
  const p95Ms = samples[Math.ceil(samples.length * 0.95) - 1] ?? medianMs;
  const oracleStarted = performance.now();
  const oracleResult = query.oracle(materialized);
  const oracleMs = performance.now() - oracleStarted;
  const baselineTuples = canonicalTuples(result, query.project);
  const oracleTuples = canonicalTuples(oracleResult, query.project);
  const engine = await measureReferenceQueryOnEngine(database, query, oracleTuples, sampleCount);
  const javascript = {
    sampleCount,
    iterations: sampleCount * executionsPerSample,
    executionsPerSample,
    medianMs,
    p95Ms,
    oracleMs,
    totalMs: sum(samples),
    measurementMs,
    resultRows: result.length,
    checksum: referenceChecksum(baselineTuples),
    oracleChecksum: referenceChecksum(oracleTuples),
    verified:
      result.length === query.expectedRows &&
      oracleResult.length === query.expectedRows &&
      tuplesMatch(baselineTuples, oracleTuples),
  };
  return {
    loadMs,
    indexMs,
    measurement: {
      id: query.id,
      name: query.name,
      complexity: query.complexity,
      sql: query.sql,
      tables: query.tables,
      expectedRows: query.expectedRows,
      ...(query.surfaceGap === undefined ? {} : { surfaceGap: query.surfaceGap }),
      engine,
      javascript,
      // A query the engine cannot compile is a recorded gap, not a failure: the JavaScript
      // reference still has to agree with its oracle for the run to pass.
      verified: javascript.verified && (!engine.supported || engine.verified),
    },
  };
}

/**
 * Runs the query's own SQL through the public prepare/execute API. Support is decided here,
 * by compiling against the shipped engine, so a widened SQL surface shows up on the next run
 * without anyone editing the catalog.
 */
async function measureReferenceQueryOnEngine(
  database: MinnowDatabase,
  query: ReferenceQueryDefinition,
  oracleTuples: readonly unknown[][],
  sampleCount: number,
): Promise<BenchmarkResult["referenceQueries"]["queries"][number]["engine"]> {
  let prepared: Awaited<ReturnType<MinnowDatabase["prepareQuery"]>> | undefined;
  try {
    const prepareStarted = performance.now();
    prepared = await database.prepareQuery(query.sql);
    const prepareMs = performance.now() - prepareStarted;
    prepared.execute();
    const samples: number[] = [];
    let result = prepared.execute();
    for (let sample = 0; sample < sampleCount; sample += 1) {
      const started = performance.now();
      result = prepared.execute();
      samples.push(performance.now() - started);
    }
    samples.sort((left, right) => left - right);
    const tuples = canonicalTuples(result.rows, (row) =>
      query.columns.map((column) => row[column]),
    );
    return {
      supported: true,
      plan: await database.explain(query.sql),
      prepareMs,
      medianMs: samples[Math.floor(samples.length / 2)] ?? 0,
      p95Ms: samples[Math.ceil(samples.length * 0.95) - 1] ?? 0,
      iterations: samples.length,
      resultRows: result.rows.length,
      checksum: referenceChecksum(tuples),
      verified: tuplesMatch(tuples, oracleTuples),
    };
  } catch (error) {
    return {
      supported: false,
      error: error instanceof Error ? error.message : String(error),
      prepareMs: 0,
      medianMs: 0,
      p95Ms: 0,
      iterations: 0,
      resultRows: 0,
      checksum: 0,
      verified: false,
    };
  } finally {
    prepared?.close();
  }
}
async function createReferenceTables(
  database: MinnowDatabase,
  entities: readonly EntityDefinition[],
): Promise<void> {
  for (const entity of entities) {
    await database.createTable({
      name: entity.name,
      ...(entity.primaryKey === undefined ? {} : { uniqueKey: entity.primaryKey }),
      columns: entity.columns.map((column) => ({ name: column.name, type: column.type })),
    });
  }
}

async function insertReferenceDataset(
  database: MinnowDatabase,
  entities: readonly EntityDefinition[],
  scale: number,
): Promise<void> {
  const maxBatchRows = 100_000;
  for (const entity of entities) {
    const entityRows = entity.rows(scale);
    for (let start = 0; start < entityRows; start += maxBatchRows) {
      const rowCount = Math.min(maxBatchRows, entityRows - start);
      await database.insertBatch(entity.name, {
        columns: generateBatchColumns(entity, start, rowCount, entityRows, scale),
      });
    }
  }
}

const referenceRelationships = [
  ["regions", "country_id", "countries", "country_id"],
  ["tax_jurisdictions", "region_id", "regions", "region_id"],
  ["tax_rates", "jurisdiction_id", "tax_jurisdictions", "jurisdiction_id"],
  ["warehouses", "region_id", "regions", "region_id"],
  ["suppliers", "region_id", "regions", "region_id"],
  ["products", "brand_id", "brands", "brand_id"],
  ["products", "category_id", "categories", "category_id"],
  ["product_suppliers", "product_id", "products", "product_id"],
  ["product_suppliers", "supplier_id", "suppliers", "supplier_id"],
  ["customers", "region_id", "regions", "region_id"],
  ["customer_addresses", "customer_id", "customers", "customer_id"],
  ["customer_addresses", "region_id", "regions", "region_id"],
  ["customer_addresses", "jurisdiction_id", "tax_jurisdictions", "jurisdiction_id"],
  ["customer_payment_methods", "customer_id", "customers", "customer_id"],
  ["orders", "customer_id", "customers", "customer_id"],
  ["orders", "shipping_address_id", "customer_addresses", "address_id"],
  ["orders", "currency_id", "currencies", "currency_id"],
  ["order_items", "order_id", "orders", "order_id"],
  ["order_items", "product_id", "products", "product_id"],
  ["order_discounts", "order_id", "orders", "order_id"],
  ["order_discounts", "promotion_id", "promotions", "promotion_id"],
  ["order_taxes", "order_id", "orders", "order_id"],
  ["order_taxes", "item_id", "order_items", "item_id"],
  ["order_taxes", "tax_rate_id", "tax_rates", "tax_rate_id"],
  ["payments", "order_id", "orders", "order_id"],
  ["payments", "payment_method_id", "customer_payment_methods", "payment_method_id"],
  ["payments", "currency_id", "currencies", "currency_id"],
  ["payment_transactions", "payment_id", "payments", "payment_id"],
  ["shipments", "order_id", "orders", "order_id"],
  ["shipments", "warehouse_id", "warehouses", "warehouse_id"],
  ["shipment_items", "shipment_id", "shipments", "shipment_id"],
  ["shipment_items", "item_id", "order_items", "item_id"],
  ["returns", "order_id", "orders", "order_id"],
  ["returns", "item_id", "order_items", "item_id"],
  ["return_items", "return_id", "returns", "return_id"],
  ["return_items", "item_id", "order_items", "item_id"],
  ["refunds", "return_id", "returns", "return_id"],
  ["refunds", "payment_id", "payments", "payment_id"],
  ["inventory_movements", "product_id", "products", "product_id"],
  ["inventory_movements", "warehouse_id", "warehouses", "warehouse_id"],
  ["inventory_movements", "supplier_id", "suppliers", "supplier_id"],
  ["stores", "region_id", "regions", "region_id"],
  ["stores", "warehouse_id", "warehouses", "warehouse_id"],
  ["employees", "store_id", "stores", "store_id"],
  ["loyalty_accounts", "customer_id", "customers", "customer_id"],
  ["loyalty_accounts", "segment_id", "customer_segments", "segment_id"],
  ["loyalty_transactions", "loyalty_account_id", "loyalty_accounts", "loyalty_account_id"],
  ["loyalty_transactions", "order_id", "orders", "order_id"],
  ["price_lists", "currency_id", "currencies", "currency_id"],
  ["price_lists", "channel_id", "sales_channels", "channel_id"],
  ["product_prices", "product_id", "products", "product_id"],
  ["product_prices", "price_list_id", "price_lists", "price_list_id"],
  ["purchase_orders", "supplier_id", "suppliers", "supplier_id"],
  ["purchase_orders", "warehouse_id", "warehouses", "warehouse_id"],
  ["purchase_orders", "currency_id", "currencies", "currency_id"],
  ["purchase_order_items", "purchase_order_id", "purchase_orders", "purchase_order_id"],
  ["purchase_order_items", "product_id", "products", "product_id"],
  ["receipts", "purchase_order_id", "purchase_orders", "purchase_order_id"],
  ["receipts", "warehouse_id", "warehouses", "warehouse_id"],
  ["receipt_items", "receipt_id", "receipts", "receipt_id"],
  ["receipt_items", "purchase_order_item_id", "purchase_order_items", "purchase_order_item_id"],
  ["carts", "customer_id", "customers", "customer_id"],
  ["carts", "channel_id", "sales_channels", "channel_id"],
  ["carts", "currency_id", "currencies", "currency_id"],
  ["cart_items", "cart_id", "carts", "cart_id"],
  ["cart_items", "product_id", "products", "product_id"],
  ["order_events", "order_id", "orders", "order_id"],
  ["order_events", "employee_id", "employees", "employee_id"],
  ["payment_attempts", "payment_id", "payments", "payment_id"],
  ["fraud_reviews", "payment_id", "payments", "payment_id"],
  ["fraud_reviews", "employee_id", "employees", "employee_id"],
  ["shipment_events", "shipment_id", "shipments", "shipment_id"],
  ["delivery_attempts", "shipment_id", "shipments", "shipment_id"],
  ["support_tickets", "customer_id", "customers", "customer_id"],
  ["support_tickets", "order_id", "orders", "order_id"],
  ["support_messages", "ticket_id", "support_tickets", "ticket_id"],
  ["support_messages", "employee_id", "employees", "employee_id"],
  ["inventory_snapshots", "product_id", "products", "product_id"],
  ["inventory_snapshots", "warehouse_id", "warehouses", "warehouse_id"],
  ["audit_events", "employee_id", "employees", "employee_id"],
  ["audit_events", "order_id", "orders", "order_id"],
] as const;

async function validatePersistedReferenceDataset(
  database: MinnowDatabase,
  metadata: Array<{ name: string; rows: number; loadMs: number }>,
): Promise<BenchmarkResult["referenceQueries"]["integrityChecks"]> {
  const checks: BenchmarkResult["referenceQueries"]["integrityChecks"] = [];
  const primaryKeys = new Map(
    getScenario("commerce").entities.map((entity) => [entity.name, entity.primaryKey ?? ""]),
  );
  let started = performance.now();
  const invalidTables: string[] = [];
  for (const table of metadata) {
    const key = primaryKeys.get(table.name) ?? "";
    const tableStarted = performance.now();
    const tableRows = await database.readTable(table.name, { columns: [key] });
    table.loadMs = performance.now() - tableStarted;
    const keys = new Set(tableRows.map((row) => numberField(row, key)));
    if (tableRows.length !== table.rows || keys.size !== table.rows) invalidTables.push(table.name);
  }
  checks.push({
    id: "row-counts-and-keys",
    label: "Row counts and primary keys",
    detail:
      invalidTables.length === 0
        ? `All ${String(metadata.length)} table counts match and every primary key is unique`
        : `Mismatch in: ${invalidTables.join(", ")}`,
    durationMs: performance.now() - started,
    passed: invalidTables.length === 0,
  });

  started = performance.now();
  let missingForeignKeys = 0;
  for (const [childTable, childColumn, parentTable, parentColumn] of referenceRelationships) {
    const parentRows = await database.readTable(parentTable, { columns: [parentColumn] });
    const parentKeys = new Set(parentRows.map((row) => numberField(row, parentColumn)));
    const childRows = await database.readTable(childTable, { columns: [childColumn] });
    missingForeignKeys += childRows.filter(
      (row) => !parentKeys.has(numberField(row, childColumn)),
    ).length;
  }
  checks.push({
    id: "foreign-keys",
    label: "Foreign-key graph",
    detail:
      missingForeignKeys === 0
        ? `All ${String(referenceRelationships.length)} foreign-key paths resolve without orphan rows`
        : `${String(missingForeignKeys)} orphan references found`,
    durationMs: performance.now() - started,
    passed: missingForeignKeys === 0,
  });

  started = performance.now();
  const orders = await database.readTable("orders", {
    columns: ["status", "total", "placed_at"],
  });
  const items = await database.readTable("order_items", {
    columns: ["quantity", "unit_price", "discount_pct"],
  });
  const payments = await database.readTable("payments", {
    columns: ["status", "captured_amount"],
  });
  const validStatuses = new Set(["created", "paid", "shipped", "returned"]);
  const validPaymentStatuses = new Set(["failed", "captured"]);
  const invalidValues =
    orders.filter(
      (row) =>
        !validStatuses.has(stringField(row, "status")) ||
        numberField(row, "total") < 0 ||
        !Number.isFinite(dateField(row, "placed_at").getTime()),
    ).length +
    items.filter(
      (row) =>
        numberField(row, "quantity") <= 0 ||
        numberField(row, "unit_price") < 0 ||
        numberField(row, "discount_pct") < 0 ||
        numberField(row, "discount_pct") >= 1,
    ).length +
    payments.filter(
      (row) =>
        !validPaymentStatuses.has(stringField(row, "status")) ||
        numberField(row, "captured_amount") < 0,
    ).length;
  checks.push({
    id: "value-domains",
    label: "Types and value domains",
    detail:
      invalidValues === 0
        ? "Dates, statuses, quantities, discounts, and monetary values are valid"
        : `${String(invalidValues)} invalid values found`,
    durationMs: performance.now() - started,
    passed: invalidValues === 0,
  });

  started = performance.now();
  const orderCount = metadata.find((table) => table.name === "orders")?.rows ?? 0;
  const coverage = new Uint8Array(orderCount + 1);
  for (const [tableName, bit] of [
    ["order_items", 1],
    ["order_taxes", 2],
    ["payments", 4],
  ] as const) {
    const coverageRows = await database.readTable(tableName, { columns: ["order_id"] });
    for (const row of coverageRows) {
      const orderId = numberField(row, "order_id");
      coverage[orderId] = (coverage[orderId] ?? 0) | bit;
    }
  }
  const orderIds = await database.readTable("orders", { columns: ["order_id"] });
  const uncoveredOrders = orderIds.filter(
    (row) => coverage[numberField(row, "order_id")] !== 7,
  ).length;
  const paymentValues = await database.readTable("payments", {
    columns: ["status", "captured_amount"],
  });
  const invalidCapturedAmounts = paymentValues.filter((row) => {
    const captured = numberField(row, "captured_amount");
    return stringField(row, "status") === "failed" ? captured !== 0 : captured <= 0;
  }).length;
  let invalidLedgerRows = 0;
  for (const [tableName, columnName] of [
    ["payment_transactions", "amount"],
    ["order_taxes", "tax_amount"],
    ["inventory_movements", "quantity_delta"],
  ] as const) {
    const ledgerRows = await database.readTable(tableName, { columns: [columnName] });
    invalidLedgerRows += ledgerRows.filter((row) => {
      const value = numberField(row, columnName);
      return columnName === "quantity_delta" ? value === 0 : value <= 0;
    }).length;
  }
  const transactionErrors = uncoveredOrders + invalidCapturedAmounts + invalidLedgerRows;
  checks.push({
    id: "transaction-balances",
    label: "Transaction and ledger coverage",
    detail:
      transactionErrors === 0
        ? "Every order has items, tax, and payment coverage; money and inventory ledgers balance their status rules"
        : `${String(transactionErrors)} transaction coverage or ledger errors found`,
    durationMs: performance.now() - started,
    passed: transactionErrors === 0,
  });
  return checks;
}
function generateBatchColumns(
  entity: EntityDefinition,
  startRow: number,
  rowCount: number,
  entityRows: number,
  scale: number,
): Record<string, BatchValue[]> {
  return Object.fromEntries(
    entity.columns.map((column) => [
      column.name,
      Array.from({ length: rowCount }, (_, index) =>
        column.valueAt(startRow + index, entityRows, scale),
      ),
    ]),
  );
}

function generateUpsertColumns(
  keys: readonly number[],
  scoreOffset: number,
): Record<string, BatchValue[]> {
  return {
    record_id: [...keys],
    score: keys.map((key) => scoreOffset + key),
    label: keys.map((key) => `record-${String(key)}-${String(scoreOffset)}`),
    active: keys.map((key) => key % 2 === 0),
    changed_at: keys.map((key) => new Date(Date.UTC(2026, 0, 1) + (scoreOffset + key) * 1_000)),
  };
}

export function validateConfig(value: unknown): BenchmarkConfig {
  if (typeof value !== "object" || value === null) throw new Error("Invalid benchmark config");
  const config = value as Partial<BenchmarkConfig>;
  if (!scenarioIds.includes(config.scenario as never)) throw new Error("Invalid scenario");
  if (!benchmarkFocuses.includes(config.focus as never)) throw new Error("Invalid benchmark focus");
  if (
    typeof config.scale !== "number" ||
    !Number.isFinite(config.scale) ||
    config.scale < 0.1 ||
    config.scale > 100
  ) {
    throw new Error("Scale must be between 0.1 and 100");
  }
  if (
    !Number.isSafeInteger(config.rows) ||
    (config.rows ?? 0) < 1 ||
    (config.rows ?? 0) > 5_000_000
  ) {
    throw new Error("Rows must be between 1 and 5,000,000");
  }
  if (!durabilityModes.includes(config.durability as never)) throw new Error("Invalid durability");
  if (!["raw", "rle", "gzip"].includes(String(config.compression))) {
    throw new Error("Invalid compression");
  }
  if (
    !Number.isSafeInteger(config.targetBlockBytes) ||
    (config.targetBlockBytes ?? 0) < 64 * 1024 ||
    (config.targetBlockBytes ?? 0) > 8 * 1024 * 1024
  ) {
    throw new Error("Invalid target block size");
  }
  return config as BenchmarkConfig;
}
function generateInput(
  definition: ColumnDefinition,
  startRow: number,
  rowCount: number,
  entityRows: number,
  scale: number,
): ColumnInput {
  switch (definition.type) {
    case "boolean":
      return {
        type: "boolean",
        values: Array.from({ length: rowCount }, (_, index) =>
          definition.valueAt(startRow + index, entityRows, scale),
        ),
      };
    case "number":
      return {
        type: "number",
        values: Array.from({ length: rowCount }, (_, index) =>
          definition.valueAt(startRow + index, entityRows, scale),
        ),
      };
    case "string":
      return {
        type: "string",
        values: Array.from({ length: rowCount }, (_, index) =>
          definition.valueAt(startRow + index, entityRows, scale),
        ),
      };
    case "datetime":
      return {
        type: "datetime",
        values: Array.from({ length: rowCount }, (_, index) =>
          definition.valueAt(startRow + index, entityRows, scale),
        ),
      };
  }
}

function verifyBlock(column: DecodedColumn, block: PendingBlock): void {
  if (column.type !== block.definition.type || column.values.length !== block.measurement.rows) {
    throw new Error(`Decoded shape mismatch: ${block.measurement.id}`);
  }
  for (let index = 0; index < column.values.length; index += 1) {
    const actual = column.values[index];
    const expected = block.definition.valueAt(
      block.startRow + index,
      block.entityRows,
      block.scale,
    );
    const matches =
      actual instanceof Date && expected instanceof Date
        ? actual.getTime() === expected.getTime()
        : actual === expected;
    if (!matches)
      throw new Error(`Verification failed: ${block.measurement.id} row ${String(index)}`);
  }
}

function estimateBlockCount(config: BenchmarkConfig): number {
  const scenario = getScenario(config.scenario);
  let count = 0;
  for (const entity of scenario.entities) {
    const rows = entity.rows(config.scale);
    for (const column of entity.columns) {
      count += Math.ceil(rows / blockRows(column, config.targetBlockBytes));
    }
  }
  return count;
}

function blockRows(definition: ColumnDefinition, targetBytes: number): number {
  return Math.max(1, Math.floor(targetBytes / definition.estimatedBytesPerRow));
}
