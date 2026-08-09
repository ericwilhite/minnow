/// <reference lib="webworker" />
import {
  decodeBlock,
  encodeBlock,
  inspectBlock,
  type ColumnInput,
  type DecodedColumn,
} from "@browserdatabase/block-format";
import {
  BrowserDatabase,
  UniqueConstraintError,
  type BatchValue,
  type DatabaseRow,
} from "@browserdatabase/engine";
import { IndexedDbBlockStore, WriteConflictError } from "@browserdatabase/storage-idb";
import { FaultInjectingBlockStore } from "@browserdatabase/testing";
import { TransactionManager } from "@browserdatabase/transactions";
import {
  failure,
  parseRequest,
  protocolVersion,
  success,
  type ProgressResponse,
} from "@browserdatabase/worker-protocol";
import {
  benchmarkFocuses,
  durabilityModes,
  getScenario,
  scenarioIds,
  type AdHocQueryResult,
  type BenchmarkConfig,
  type BenchmarkProgress,
  type BenchmarkResult,
  type BlockMeasurement,
  type ColumnDefinition,
  type EntityMeasurement,
  type EntityDefinition,
  type LibraryCheckMeasurement,
  type LibraryTableMeasurement,
  type PersistedDatasetStatus,
  type TransactionCheckMeasurement,
} from "./benchmark.js";

interface PendingBlock {
  measurement: BlockMeasurement;
  definition: ColumnDefinition;
  startRow: number;
  entityRows: number;
  scale: number;
}

const cancelledRuns = new Set<string>();
const DATASET_REGISTRY_NAME = "browserdatabase-dashboard-datasets-v1";
const DATASET_REGISTRY_STORE = "datasets";
const DATASET_DATABASE_PREFIX = "browserdatabase-dashboard-dataset-";

interface PersistedDatasetRecord {
  runId: string;
  databaseName: string;
  createdAt: string;
  scale: number;
  compression: BenchmarkConfig["compression"];
  targetBlockBytes: number;
  durability: BenchmarkConfig["durability"];
  totalRows: number;
  storedBytes: number;
  tableRows: Record<string, number>;
}

self.addEventListener("message", (event: MessageEvent<unknown>) => {
  void runRequest(event.data);
});

async function runRequest(raw: unknown): Promise<void> {
  let request: ReturnType<typeof parseRequest>;
  try {
    request = parseRequest(raw);
    if (request.operation === "cancelBenchmark") {
      const payload = request.payload as { requestId?: unknown };
      if (typeof payload.requestId === "string") cancelledRuns.add(payload.requestId);
      self.postMessage(success(request.requestId, { cancelled: payload.requestId }));
      return;
    }
    if (request.operation === "datasetStatus") {
      self.postMessage(success(request.requestId, await persistedDatasetStatus()));
      return;
    }
    if (request.operation === "wipeDatasets") {
      self.postMessage(success(request.requestId, await wipePersistedDatasets()));
      return;
    }
    if (request.operation === "adHocQuery") {
      const payload = request.payload as { sql?: unknown };
      if (typeof payload.sql !== "string") throw new TypeError("Query SQL must be a string");
      self.postMessage(success(request.requestId, await executePersistedAdHocQuery(payload.sql)));
      return;
    }
    if (request.operation === "compareEngines") {
      const { runEngineComparison } = await import("./engine-comparison.js");
      const payload = request.payload as {
        scale?: unknown;
        compression?: unknown;
        targetBlockBytes?: unknown;
        durability?: unknown;
        batchRows?: unknown;
      };
      if (typeof payload.scale !== "number" || payload.scale <= 0) {
        throw new TypeError("Comparison scale must be a positive number");
      }
      if (!["raw", "rle", "gzip"].includes(String(payload.compression))) {
        throw new TypeError("Invalid comparison compression");
      }
      if (typeof payload.targetBlockBytes !== "number" || payload.targetBlockBytes <= 0) {
        throw new TypeError("Invalid comparison target block size");
      }
      if (!durabilityModes.includes(payload.durability as never)) {
        throw new TypeError("Invalid comparison durability");
      }
      const result = await runEngineComparison(
        {
          scale: payload.scale,
          compression: payload.compression as "raw" | "rle" | "gzip",
          targetBlockBytes: payload.targetBlockBytes,
          durability: payload.durability as "relaxed" | "strict",
          ...(typeof payload.batchRows === "number" ? { batchRows: payload.batchRows } : {}),
        },
        (message, progressCompleted, progressTotal) => {
          progress(request.requestId, {
            phase: "library",
            completed: progressCompleted,
            total: progressTotal,
            message,
          });
        },
      );
      self.postMessage(success(request.requestId, result));
      return;
    }
    if (request.operation !== "benchmark") throw new Error("Unsupported benchmark operation");
    const config = validateConfig(request.payload);
    const result = await benchmark(request.requestId, config);
    self.postMessage(success(request.requestId, result));
  } catch (error) {
    self.postMessage(failure(getRequestId(raw), error));
  }
}

async function benchmark(requestId: string, config: BenchmarkConfig): Promise<BenchmarkResult> {
  const scenario = getScenario(config.scenario);
  const runId = crypto.randomUUID();
  const databaseName = `browserdatabase-bench-${runId}`;
  const estimateBefore = await storageEstimate();
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
      committedManifest.blockIds.length === blockIds.length;
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
  const databaseName = `browserdatabase-transaction-bench-${runId}`;
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
    const rebasedBlocks = rebasedManifest.blockIds;
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
      recoveredManifest.blockIds.includes("response-lost");
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
  const databaseName = `browserdatabase-library-bench-${runId}`;
  const store = await IndexedDbBlockStore.open({
    name: databaseName,
    durability: config.durability,
  });
  const rowsPerBlock = Math.max(1, Math.min(100_000, Math.floor(config.targetBlockBytes / 8.125)));
  const maxBatchRows = 100_000;
  const database = new BrowserDatabase(store, {
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
  let probeDurationMs = 0;
  let upsertMeasurement = {
    sampleRows: 0,
    insertedRows: 0,
    updatedRows: 0,
    finalRows: 0,
    baseVersion: -1,
    version: -1,
  };
  let mutationMeasurement = {
    updatedRows: 0,
    deletedRows: 0,
    projectedRows: 0,
    updateVersion: -1,
    deleteVersion: null as number | null,
    updateStoredBytes: 0,
    deleteStoredBytes: 0,
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
      const secondDatabase = new BrowserDatabase(secondStore, {
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

interface ReferenceQueryDefinition {
  id: string;
  name: string;
  complexity: "simple" | "moderate" | "complex";
  sql: string;
  tables: string[];
  expectedRows: number;
  execute: (context: ReferenceQueryContext) => unknown[];
  oracle: (tables: ReadonlyMap<string, DatabaseRow[]>) => unknown[];
}

interface ReferenceQueryContext {
  tables: ReadonlyMap<string, DatabaseRow[]>;
  regionsById: ReadonlyMap<number, DatabaseRow>;
  customersById: ReadonlyMap<number, DatabaseRow>;
  productsById: ReadonlyMap<number, DatabaseRow>;
  ordersById: ReadonlyMap<number, DatabaseRow>;
  returnedItemIds: ReadonlySet<number>;
  returnedOrderIds: ReadonlySet<number>;
}

async function benchmarkReferenceQueries(
  config: BenchmarkConfig,
  runId: string,
): Promise<BenchmarkResult["referenceQueries"]> {
  const databaseName = `${DATASET_DATABASE_PREFIX}${Date.now().toString(36)}-${runId}`;
  const store = await IndexedDbBlockStore.open({
    name: databaseName,
    durability: config.durability,
  });
  const database = new BrowserDatabase(store, {
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
  let retained = false;

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

    await registerPersistedDataset({
      runId,
      databaseName,
      createdAt: new Date().toISOString(),
      scale: config.scale,
      compression: config.compression,
      targetBlockBytes: config.targetBlockBytes,
      durability: config.durability,
      totalRows: sum(tableMetadata.map((table) => table.rows)),
      storedBytes: await store.getLogicalStorageBytes(),
      tableRows: Object.fromEntries(tableMetadata.map((table) => [table.name, table.rows])),
    });
    retained = true;
    return {
      orderRows,
      totalRows: sum(tableMetadata.map((table) => table.rows)),
      setupMs,
      loadMs,
      indexMs,
      totalMs: loadMs + indexMs + sum(measurements.map((query) => query.measurementMs)),
      sampleCount,
      tables: tableMetadata,
      integrityChecks,
      queries: measurements,
      passed:
        integrityChecks.every((check) => check.passed) &&
        measurements.every((query) => query.verified),
      note: "Reference workload only: validation and each query load bounded table subsets from the persisted BrowserDatabase dataset, then release them before the next query. The complete 50-table dataset is never retained as one JavaScript object graph. Each query reports seven normalized timing samples; fast queries repeat inside a sample to exceed timer resolution. These are not native query-engine operator timings.",
    };
  } finally {
    store.close();
    if (!retained) await deleteDatabase(databaseName);
  }
}

async function measureReferenceQueryFromStorage(
  database: BrowserDatabase,
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
  let result = query.execute(context);
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
      result = query.execute(context);
    }
    const elapsed = performance.now() - computeStarted;
    measurementMs += elapsed;
    samples.push(elapsed / executionsPerSample);
  }
  samples.sort((left, right) => left - right);
  const medianMs = samples[Math.floor(samples.length / 2)] ?? 0;
  const p95Ms = samples[Math.ceil(samples.length * 0.95) - 1] ?? medianMs;
  const checksum = referenceChecksum(result);
  const oracleStarted = performance.now();
  const oracleResult = query.oracle(materialized);
  const oracleMs = performance.now() - oracleStarted;
  const oracleChecksum = referenceChecksum(oracleResult);
  return {
    loadMs,
    indexMs,
    measurement: {
      id: query.id,
      name: query.name,
      complexity: query.complexity,
      sql: query.sql,
      tables: query.tables,
      sampleCount,
      iterations: sampleCount * executionsPerSample,
      executionsPerSample,
      medianMs,
      p95Ms,
      oracleMs,
      totalMs: sum(samples),
      measurementMs,
      resultRows: result.length,
      expectedRows: query.expectedRows,
      checksum,
      oracleChecksum,
      verified:
        result.length === query.expectedRows &&
        oracleResult.length === query.expectedRows &&
        checksum === oracleChecksum,
    },
  };
}

async function createReferenceTables(
  database: BrowserDatabase,
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
  database: BrowserDatabase,
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
  database: BrowserDatabase,
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

function numericKeySet(
  tables: ReadonlyMap<string, DatabaseRow[]>,
  table: string,
  column: string,
): Set<number> {
  return new Set(rows(tables, table).map((row) => numberField(row, column)));
}

function buildReferenceQueryContext(
  tables: ReadonlyMap<string, DatabaseRow[]>,
): ReferenceQueryContext {
  const regionsById = indexRows(tables, "regions", "region_id");
  const customersById = indexRows(tables, "customers", "customer_id");
  const productsById = indexRows(tables, "products", "product_id");
  const ordersById = indexRows(tables, "orders", "order_id");
  const returnedItemIds = numericKeySet(tables, "returns", "item_id");
  const returnedOrderIds = new Set<number>();
  for (const item of rows(tables, "order_items")) {
    if (returnedItemIds.has(numberField(item, "item_id"))) {
      returnedOrderIds.add(numberField(item, "order_id"));
    }
  }
  return {
    tables,
    regionsById,
    customersById,
    productsById,
    ordersById,
    returnedItemIds,
    returnedOrderIds,
  };
}

function indexRows(
  tables: ReadonlyMap<string, DatabaseRow[]>,
  table: string,
  key: string,
): Map<number, DatabaseRow> {
  return new Map(rows(tables, table).map((row) => [numberField(row, key), row]));
}

function referenceQueryDefinitions(orderRows: number): ReferenceQueryDefinition[] {
  const pointId = Math.max(1, Math.floor(orderRows / 2));
  const dateRangeRows = Math.min(
    25,
    Array.from({ length: orderRows }, (_, index) => index).filter(
      (index) => index % 4 === 1 && index % 365 >= 90 && index % 365 < 181,
    ).length,
  );
  return [
    {
      id: "q1",
      name: "Order point lookup",
      complexity: "simple",
      tables: ["orders"],
      expectedRows: 1,
      sql: `SELECT * FROM orders WHERE order_id = ${String(pointId)};`,
      execute: (context) => {
        const row = context.ordersById.get(pointId);
        return row === undefined ? [] : [row];
      },
      oracle: (tables) =>
        rows(tables, "orders").filter((row) => numberField(row, "order_id") === pointId),
    },
    {
      id: "q2",
      name: "Paid orders in a date range",
      complexity: "simple",
      tables: ["orders"],
      expectedRows: dateRangeRows,
      sql: "SELECT order_id, customer_id, total FROM orders WHERE status = 'paid' AND placed_at >= DATE '2025-04-01' AND placed_at < DATE '2025-07-01' ORDER BY total DESC LIMIT 25;",
      execute: (context) =>
        rows(context.tables, "orders")
          .filter((row) => {
            const placed = dateField(row, "placed_at").getTime();
            return (
              stringField(row, "status") === "paid" &&
              placed >= Date.UTC(2025, 3, 1) &&
              placed < Date.UTC(2025, 6, 1)
            );
          })
          .sort((left, right) => numberField(right, "total") - numberField(left, "total"))
          .slice(0, 25),
      oracle: (tables) =>
        rows(tables, "orders")
          .filter((row) => {
            const placed = dateField(row, "placed_at").getTime();
            return (
              stringField(row, "status") === "paid" &&
              placed >= Date.UTC(2025, 3, 1) &&
              placed < Date.UTC(2025, 6, 1)
            );
          })
          .sort((left, right) => numberField(right, "total") - numberField(left, "total"))
          .slice(0, 25),
    },
    {
      id: "q3",
      name: "Revenue by order status",
      complexity: "moderate",
      tables: ["orders"],
      expectedRows: 4,
      sql: "SELECT status, COUNT(*) AS orders, SUM(total) AS revenue, AVG(total) AS avg_order FROM orders GROUP BY status ORDER BY revenue DESC;",
      execute: (context) => groupRevenue(rows(context.tables, "orders"), "status", "total"),
      oracle: groupRevenueOracle,
    },
    {
      id: "q4",
      name: "Top customers by captured revenue",
      complexity: "moderate",
      tables: ["customers", "orders", "payments"],
      expectedRows: 20,
      sql: "SELECT c.customer_id, c.segment, SUM(p.captured_amount) AS revenue FROM customers c JOIN orders o ON o.customer_id = c.customer_id JOIN payments p ON p.order_id = o.order_id WHERE p.status = 'captured' GROUP BY c.customer_id, c.segment ORDER BY revenue DESC LIMIT 20;",
      execute: topCustomersIndexed,
      oracle: topCustomers,
    },
    {
      id: "q5",
      name: "Category revenue after discounts",
      complexity: "moderate",
      tables: ["order_items", "products"],
      expectedRows: 5,
      sql: "SELECT p.category, SUM(i.quantity * i.unit_price * (1 - i.discount_pct)) AS net_revenue FROM order_items i JOIN products p ON p.product_id = i.product_id GROUP BY p.category ORDER BY net_revenue DESC;",
      execute: categoryRevenueIndexed,
      oracle: categoryRevenue,
    },
    {
      id: "q6",
      name: "Repeat customers without returns",
      complexity: "complex",
      tables: ["customers", "orders", "order_items", "returns"],
      expectedRows: expectedRepeatCustomerRows(orderRows),
      sql: "SELECT c.customer_id, COUNT(DISTINCT o.order_id) AS orders FROM customers c JOIN orders o ON o.customer_id = c.customer_id WHERE NOT EXISTS (SELECT 1 FROM order_items i JOIN returns r ON r.item_id = i.item_id WHERE i.order_id = o.order_id) GROUP BY c.customer_id HAVING COUNT(DISTINCT o.order_id) >= 2;",
      execute: repeatCustomersWithoutReturnsIndexed,
      oracle: repeatCustomersWithoutReturns,
    },
    {
      id: "q7",
      name: "Top products within each category",
      complexity: "complex",
      tables: ["order_items", "products"],
      expectedRows: 15,
      sql: "WITH product_revenue AS (...) SELECT category, product_id, revenue, DENSE_RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank FROM product_revenue QUALIFY rank <= 3;",
      execute: rankedProductsIndexed,
      oracle: rankedProducts,
    },
    {
      id: "q8",
      name: "Monthly cohort revenue and return rate",
      complexity: "complex",
      tables: ["customers", "orders", "order_items", "returns", "payments"],
      expectedRows: 12,
      sql: "WITH cohorts AS (...), customer_revenue AS (...), returned_orders AS (...) SELECT cohort_month, COUNT(DISTINCT customer_id), SUM(revenue), AVG(has_return) FROM ... GROUP BY cohort_month ORDER BY cohort_month;",
      execute: cohortAnalysisIndexed,
      oracle: cohortAnalysis,
    },
    {
      id: "q9",
      name: "Region and segment revenue matrix",
      complexity: "complex",
      tables: ["regions", "customers", "orders", "payments"],
      expectedRows: expectedRegionSegmentRows(orderRows),
      sql: "SELECT r.name AS region, c.segment, COUNT(DISTINCT c.customer_id) AS customers, SUM(p.captured_amount) AS revenue FROM regions r JOIN customers c ON c.region_id = r.region_id JOIN orders o ON o.customer_id = c.customer_id JOIN payments p ON p.order_id = o.order_id WHERE p.status = 'captured' GROUP BY r.name, c.segment ORDER BY r.name, c.segment;",
      execute: regionSegmentRevenueIndexed,
      oracle: regionSegmentRevenue,
    },
    {
      id: "q10",
      name: "Return rate by product category",
      complexity: "complex",
      tables: ["products", "order_items", "returns"],
      expectedRows: 5,
      sql: "SELECT p.category, COUNT(*) AS items, SUM(CASE WHEN r.return_id IS NOT NULL THEN 1 ELSE 0 END) AS returns, AVG(CASE WHEN r.return_id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS return_rate FROM products p JOIN order_items i ON i.product_id = p.product_id LEFT JOIN returns r ON r.item_id = i.item_id GROUP BY p.category ORDER BY p.category;",
      execute: categoryReturnRateIndexed,
      oracle: categoryReturnRate,
    },
    {
      id: "q11",
      name: "Tax collected by jurisdiction",
      complexity: "complex",
      tables: ["order_taxes", "tax_rates", "tax_jurisdictions"],
      expectedRows: Math.max(1, Math.ceil((orderRows * 24) / 1_000)),
      sql: "SELECT j.name, SUM(t.tax_amount) AS tax_collected FROM order_taxes t JOIN tax_rates r ON r.tax_rate_id = t.tax_rate_id JOIN tax_jurisdictions j ON j.jurisdiction_id = r.jurisdiction_id GROUP BY j.name ORDER BY tax_collected DESC;",
      execute: taxByJurisdictionIndexed,
      oracle: taxByJurisdiction,
    },
    {
      id: "q12",
      name: "Fulfillment volume by warehouse",
      complexity: "moderate",
      tables: ["shipments", "shipment_items", "warehouses"],
      expectedRows: Math.max(1, Math.ceil((orderRows * 8) / 1_000)),
      sql: "SELECT w.name, COUNT(DISTINCT s.shipment_id) AS shipments, SUM(si.quantity) AS units FROM warehouses w JOIN shipments s ON s.warehouse_id = w.warehouse_id JOIN shipment_items si ON si.shipment_id = s.shipment_id GROUP BY w.name ORDER BY units DESC;",
      execute: fulfillmentByWarehouseIndexed,
      oracle: fulfillmentByWarehouse,
    },
    {
      id: "q13",
      name: "Supplier inventory ledger",
      complexity: "moderate",
      tables: ["inventory_movements", "suppliers"],
      expectedRows: Math.max(1, Math.ceil((orderRows * 40) / 1_000)),
      sql: "SELECT s.name, COUNT(*) AS movements, SUM(m.quantity_delta) AS net_units FROM suppliers s JOIN inventory_movements m ON m.supplier_id = s.supplier_id GROUP BY s.name ORDER BY movements DESC;",
      execute: supplierInventoryIndexed,
      oracle: supplierInventory,
    },
    {
      id: "q14",
      name: "Payment transaction funnel",
      complexity: "simple",
      tables: ["payment_transactions"],
      expectedRows: 4,
      sql: "SELECT kind, status, COUNT(*) AS transactions, SUM(amount) AS amount FROM payment_transactions GROUP BY kind, status ORDER BY kind, status;",
      execute: paymentFunnelIndexed,
      oracle: paymentFunnel,
    },
    {
      id: "q15",
      name: "Discount and tax burden by order status",
      complexity: "complex",
      tables: ["orders", "order_discounts", "order_taxes"],
      expectedRows: 4,
      sql: "WITH discounts AS (SELECT order_id, SUM(amount) AS amount FROM order_discounts GROUP BY order_id), taxes AS (SELECT order_id, SUM(tax_amount) AS amount FROM order_taxes GROUP BY order_id) SELECT o.status, SUM(COALESCE(d.amount, 0)) AS discounts, SUM(COALESCE(t.amount, 0)) AS taxes FROM orders o LEFT JOIN discounts d ON d.order_id = o.order_id LEFT JOIN taxes t ON t.order_id = o.order_id GROUP BY o.status ORDER BY o.status;",
      execute: orderAdjustmentsIndexed,
      oracle: orderAdjustments,
    },
  ];
}

function expectedRegionSegmentRows(orderRows: number): number {
  const customerRows = Math.max(1, Math.ceil(orderRows / 5));
  const regionRows = Math.max(1, Math.ceil((orderRows * 16) / 1_000));
  const groups = new Set<string>();
  for (let index = 0; index < orderRows; index += 1) {
    if (index % 7 === 0) continue;
    const customerIndex = index % customerRows;
    groups.add(`${String(customerIndex % regionRows)}:${String(customerIndex % 3)}`);
  }
  return groups.size;
}

function expectedRepeatCustomerRows(orderRows: number): number {
  const customerRows = Math.max(20, Math.ceil(orderRows / 5));
  const returnRows = Math.max(1, Math.floor(orderRows / 10));
  const returnedOrders = new Set(Array.from({ length: returnRows }, (_, index) => index * 10 + 1));
  const counts = new Map<number, number>();
  for (let orderId = 1; orderId <= orderRows; orderId += 1) {
    if (returnedOrders.has(orderId)) continue;
    const customerId = ((orderId - 1) % customerRows) + 1;
    counts.set(customerId, (counts.get(customerId) ?? 0) + 1);
  }
  return [...counts.values()].filter((count) => count >= 2).length;
}

function rows(tables: ReadonlyMap<string, DatabaseRow[]>, name: string): DatabaseRow[] {
  return tables.get(name) ?? [];
}

function numberField(row: DatabaseRow, name: string): number {
  const value = row[name];
  if (typeof value !== "number") throw new Error(`Expected numeric field: ${name}`);
  return value;
}

function stringField(row: DatabaseRow, name: string): string {
  const value = row[name];
  if (typeof value !== "string") throw new Error(`Expected string field: ${name}`);
  return value;
}

function dateField(row: DatabaseRow, name: string): Date {
  const value = row[name];
  if (!(value instanceof Date)) throw new Error(`Expected datetime field: ${name}`);
  return value;
}

function groupRevenue(input: DatabaseRow[], group: string, amount: string): unknown[] {
  const groups = new Map<string, { count: number; total: number }>();
  for (const row of input) {
    const key = stringField(row, group);
    const current = groups.get(key) ?? { count: 0, total: 0 };
    current.count += 1;
    current.total += numberField(row, amount);
    groups.set(key, current);
  }
  return [...groups]
    .map(([key, value]) => ({
      key,
      count: value.count,
      total: value.total,
      average: value.total / value.count,
    }))
    .sort((left, right) => right.total - left.total);
}

function groupRevenueOracle(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const statuses: Record<string, { count: number; total: number }> = {};
  for (const order of rows(tables, "orders")) {
    const status = stringField(order, "status");
    const current = statuses[status] ?? { count: 0, total: 0 };
    statuses[status] = {
      count: current.count + 1,
      total: current.total + numberField(order, "total"),
    };
  }
  return Object.entries(statuses)
    .map(([key, value]) => ({
      key,
      count: value.count,
      total: value.total,
      average: value.total / value.count,
    }))
    .sort((left, right) => right.total - left.total);
}

function topCustomers(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const customers = new Map(
    rows(tables, "customers").map((row) => [numberField(row, "customer_id"), row]),
  );
  const ordersById = new Map(
    rows(tables, "orders").map((row) => [numberField(row, "order_id"), row]),
  );
  const revenue = new Map<number, number>();
  for (const payment of rows(tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const order = ordersById.get(numberField(payment, "order_id"));
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    revenue.set(
      customerId,
      (revenue.get(customerId) ?? 0) + numberField(payment, "captured_amount"),
    );
  }
  return [...revenue]
    .map(([customerId, total]) => ({
      customerId,
      segment: stringField(customers.get(customerId) ?? {}, "segment"),
      total,
    }))
    .sort((left, right) => right.total - left.total)
    .slice(0, 20);
}

function categoryRevenue(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const products = new Map(
    rows(tables, "products").map((row) => [numberField(row, "product_id"), row]),
  );
  const totals = new Map<string, number>();
  for (const item of rows(tables, "order_items")) {
    const product = products.get(numberField(item, "product_id"));
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const net =
      numberField(item, "quantity") *
      numberField(item, "unit_price") *
      (1 - numberField(item, "discount_pct"));
    totals.set(category, (totals.get(category) ?? 0) + net);
  }
  return [...totals]
    .map(([category, revenue]) => ({ category, revenue }))
    .sort((left, right) => right.revenue - left.revenue);
}

function repeatCustomersWithoutReturns(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const returnedItems = new Set(rows(tables, "returns").map((row) => numberField(row, "item_id")));
  const returnedOrders = new Set<number>();
  for (const item of rows(tables, "order_items")) {
    if (returnedItems.has(numberField(item, "item_id")))
      returnedOrders.add(numberField(item, "order_id"));
  }
  const counts = new Map<number, number>();
  for (const order of rows(tables, "orders")) {
    if (returnedOrders.has(numberField(order, "order_id"))) continue;
    const customerId = numberField(order, "customer_id");
    counts.set(customerId, (counts.get(customerId) ?? 0) + 1);
  }
  return [...counts]
    .filter(([, count]) => count >= 2)
    .map(([customerId, count]) => ({ customerId, count }));
}

function rankedProducts(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const products = new Map(
    rows(tables, "products").map((row) => [numberField(row, "product_id"), row]),
  );
  const totals = new Map<number, number>();
  for (const item of rows(tables, "order_items")) {
    const productId = numberField(item, "product_id");
    totals.set(
      productId,
      (totals.get(productId) ?? 0) +
        numberField(item, "quantity") * numberField(item, "unit_price"),
    );
  }
  const byCategory = new Map<string, Array<{ productId: number; revenue: number }>>();
  for (const [productId, revenue] of totals) {
    const product = products.get(productId);
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const list = byCategory.get(category) ?? [];
    list.push({ productId, revenue });
    byCategory.set(category, list);
  }
  return [...byCategory].flatMap(([category, values]) =>
    values
      .sort((left, right) => right.revenue - left.revenue)
      .slice(0, 3)
      .map((value, index) => ({ category, ...value, rank: index + 1 })),
  );
}

function cohortAnalysis(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const customers = new Map(
    rows(tables, "customers").map((row) => [numberField(row, "customer_id"), row]),
  );
  const orders = new Map(rows(tables, "orders").map((row) => [numberField(row, "order_id"), row]));
  const returnedItems = new Set(rows(tables, "returns").map((row) => numberField(row, "item_id")));
  const returnedOrders = new Set<number>();
  for (const item of rows(tables, "order_items"))
    if (returnedItems.has(numberField(item, "item_id")))
      returnedOrders.add(numberField(item, "order_id"));
  const cohorts = new Map<
    string,
    { customers: Set<number>; revenue: number; orders: number; returned: number }
  >();
  for (const payment of rows(tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const orderId = numberField(payment, "order_id");
    const order = orders.get(orderId);
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    const customer = customers.get(customerId);
    if (customer === undefined) continue;
    const joined = dateField(customer, "joined_at");
    const cohort = `${String(joined.getUTCFullYear())}-${String(joined.getUTCMonth() + 1).padStart(2, "0")}`;
    const value = cohorts.get(cohort) ?? {
      customers: new Set<number>(),
      revenue: 0,
      orders: 0,
      returned: 0,
    };
    value.customers.add(customerId);
    value.revenue += numberField(payment, "captured_amount");
    value.orders += 1;
    if (returnedOrders.has(orderId)) value.returned += 1;
    cohorts.set(cohort, value);
  }
  return [...cohorts]
    .map(([cohort, value]) => ({
      cohort,
      customers: value.customers.size,
      revenue: value.revenue,
      returnRate: value.returned / value.orders,
    }))
    .sort((left, right) => left.cohort.localeCompare(right.cohort));
}

function topCustomersIndexed(context: ReferenceQueryContext): unknown[] {
  const revenue = new Map<number, number>();
  for (const payment of rows(context.tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const order = context.ordersById.get(numberField(payment, "order_id"));
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    revenue.set(
      customerId,
      (revenue.get(customerId) ?? 0) + numberField(payment, "captured_amount"),
    );
  }
  return [...revenue]
    .map(([customerId, total]) => ({
      customerId,
      segment: stringField(context.customersById.get(customerId) ?? {}, "segment"),
      total,
    }))
    .sort((left, right) => right.total - left.total)
    .slice(0, 20);
}

function categoryRevenueIndexed(context: ReferenceQueryContext): unknown[] {
  const totals = new Map<string, number>();
  for (const item of rows(context.tables, "order_items")) {
    const product = context.productsById.get(numberField(item, "product_id"));
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const net =
      numberField(item, "quantity") *
      numberField(item, "unit_price") *
      (1 - numberField(item, "discount_pct"));
    totals.set(category, (totals.get(category) ?? 0) + net);
  }
  return [...totals]
    .map(([category, revenue]) => ({ category, revenue }))
    .sort((left, right) => right.revenue - left.revenue);
}

function repeatCustomersWithoutReturnsIndexed(context: ReferenceQueryContext): unknown[] {
  const counts = new Map<number, number>();
  for (const order of rows(context.tables, "orders")) {
    if (context.returnedOrderIds.has(numberField(order, "order_id"))) continue;
    const customerId = numberField(order, "customer_id");
    counts.set(customerId, (counts.get(customerId) ?? 0) + 1);
  }
  return [...counts]
    .filter(([, count]) => count >= 2)
    .map(([customerId, count]) => ({ customerId, count }));
}

function rankedProductsIndexed(context: ReferenceQueryContext): unknown[] {
  const totals = new Map<number, number>();
  for (const item of rows(context.tables, "order_items")) {
    const productId = numberField(item, "product_id");
    totals.set(
      productId,
      (totals.get(productId) ?? 0) +
        numberField(item, "quantity") * numberField(item, "unit_price"),
    );
  }
  const byCategory = new Map<string, Array<{ productId: number; revenue: number }>>();
  for (const [productId, revenue] of totals) {
    const product = context.productsById.get(productId);
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const list = byCategory.get(category) ?? [];
    list.push({ productId, revenue });
    byCategory.set(category, list);
  }
  return [...byCategory].flatMap(([category, values]) =>
    values
      .sort((left, right) => right.revenue - left.revenue)
      .slice(0, 3)
      .map((value, index) => ({ category, ...value, rank: index + 1 })),
  );
}

function cohortAnalysisIndexed(context: ReferenceQueryContext): unknown[] {
  const cohorts = new Map<
    string,
    { customers: Set<number>; revenue: number; orders: number; returned: number }
  >();
  for (const payment of rows(context.tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const orderId = numberField(payment, "order_id");
    const order = context.ordersById.get(orderId);
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    const customer = context.customersById.get(customerId);
    if (customer === undefined) continue;
    const joined = dateField(customer, "joined_at");
    const cohort = `${String(joined.getUTCFullYear())}-${String(joined.getUTCMonth() + 1).padStart(2, "0")}`;
    const value = cohorts.get(cohort) ?? {
      customers: new Set<number>(),
      revenue: 0,
      orders: 0,
      returned: 0,
    };
    value.customers.add(customerId);
    value.revenue += numberField(payment, "captured_amount");
    value.orders += 1;
    if (context.returnedOrderIds.has(orderId)) value.returned += 1;
    cohorts.set(cohort, value);
  }
  return [...cohorts]
    .map(([cohort, value]) => ({
      cohort,
      customers: value.customers.size,
      revenue: value.revenue,
      returnRate: value.returned / value.orders,
    }))
    .sort((left, right) => left.cohort.localeCompare(right.cohort));
}

function regionSegmentRevenueIndexed(context: ReferenceQueryContext): unknown[] {
  return collectRegionSegmentRevenue(
    rows(context.tables, "payments"),
    context.ordersById,
    context.customersById,
    context.regionsById,
  );
}

function regionSegmentRevenue(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const ordersById = indexRows(tables, "orders", "order_id");
  const customersById = indexRows(tables, "customers", "customer_id");
  const regionsById = indexRows(tables, "regions", "region_id");
  const groups: Record<
    string,
    { region: string; segment: string; customers: Set<number>; revenue: number }
  > = {};
  for (const payment of rows(tables, "payments")) {
    if (stringField(payment, "status") !== "captured") continue;
    const order = ordersById.get(numberField(payment, "order_id"));
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    const customer = customersById.get(customerId);
    if (customer === undefined) continue;
    const region = regionsById.get(numberField(customer, "region_id"));
    if (region === undefined) continue;
    const regionName = stringField(region, "name");
    const segment = stringField(customer, "segment");
    const key = `${regionName}\u0000${segment}`;
    const current = groups[key] ?? {
      region: regionName,
      segment,
      customers: new Set<number>(),
      revenue: 0,
    };
    current.customers.add(customerId);
    current.revenue += numberField(payment, "captured_amount");
    groups[key] = current;
  }
  return Object.values(groups)
    .map((value) => ({
      region: value.region,
      segment: value.segment,
      customers: value.customers.size,
      revenue: value.revenue,
    }))
    .sort(
      (left, right) =>
        left.region.localeCompare(right.region) || left.segment.localeCompare(right.segment),
    );
}

function collectRegionSegmentRevenue(
  payments: DatabaseRow[],
  ordersById: ReadonlyMap<number, DatabaseRow>,
  customersById: ReadonlyMap<number, DatabaseRow>,
  regionsById: ReadonlyMap<number, DatabaseRow>,
): unknown[] {
  const groups = new Map<
    string,
    { region: string; segment: string; customers: Set<number>; revenue: number }
  >();
  for (const payment of payments) {
    if (stringField(payment, "status") !== "captured") continue;
    const order = ordersById.get(numberField(payment, "order_id"));
    if (order === undefined) continue;
    const customerId = numberField(order, "customer_id");
    const customer = customersById.get(customerId);
    if (customer === undefined) continue;
    const region = regionsById.get(numberField(customer, "region_id"));
    if (region === undefined) continue;
    const regionName = stringField(region, "name");
    const segment = stringField(customer, "segment");
    const key = `${regionName}\u0000${segment}`;
    const value = groups.get(key) ?? {
      region: regionName,
      segment,
      customers: new Set<number>(),
      revenue: 0,
    };
    value.customers.add(customerId);
    value.revenue += numberField(payment, "captured_amount");
    groups.set(key, value);
  }
  return [...groups.values()]
    .map((value) => ({
      region: value.region,
      segment: value.segment,
      customers: value.customers.size,
      revenue: value.revenue,
    }))
    .sort(
      (left, right) =>
        left.region.localeCompare(right.region) || left.segment.localeCompare(right.segment),
    );
}

function categoryReturnRateIndexed(context: ReferenceQueryContext): unknown[] {
  return collectCategoryReturnRate(
    rows(context.tables, "order_items"),
    context.productsById,
    context.returnedItemIds,
  );
}

function categoryReturnRate(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const productsById = indexRows(tables, "products", "product_id");
  const returnedItemIds = numericKeySet(tables, "returns", "item_id");
  const groups: Record<string, { items: number; returns: number }> = {};
  for (const item of rows(tables, "order_items")) {
    const product = productsById.get(numberField(item, "product_id"));
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const current = groups[category] ?? { items: 0, returns: 0 };
    current.items += 1;
    if (returnedItemIds.has(numberField(item, "item_id"))) current.returns += 1;
    groups[category] = current;
  }
  return Object.entries(groups)
    .map(([category, value]) => ({
      category,
      items: value.items,
      returns: value.returns,
      returnRate: value.returns / value.items,
    }))
    .sort((left, right) => left.category.localeCompare(right.category));
}

function collectCategoryReturnRate(
  items: DatabaseRow[],
  productsById: ReadonlyMap<number, DatabaseRow>,
  returnedItemIds: ReadonlySet<number>,
): unknown[] {
  const groups = new Map<string, { items: number; returns: number }>();
  for (const item of items) {
    const product = productsById.get(numberField(item, "product_id"));
    if (product === undefined) continue;
    const category = stringField(product, "category");
    const value = groups.get(category) ?? { items: 0, returns: 0 };
    value.items += 1;
    if (returnedItemIds.has(numberField(item, "item_id"))) value.returns += 1;
    groups.set(category, value);
  }
  return [...groups]
    .map(([category, value]) => ({
      category,
      items: value.items,
      returns: value.returns,
      returnRate: value.returns / value.items,
    }))
    .sort((left, right) => left.category.localeCompare(right.category));
}

function taxByJurisdictionIndexed(context: ReferenceQueryContext): unknown[] {
  const rates = indexRows(context.tables, "tax_rates", "tax_rate_id");
  const jurisdictions = indexRows(context.tables, "tax_jurisdictions", "jurisdiction_id");
  const totals = new Map<string, number>();
  for (const tax of rows(context.tables, "order_taxes")) {
    const rate = rates.get(numberField(tax, "tax_rate_id"));
    const jurisdiction =
      rate === undefined ? undefined : jurisdictions.get(numberField(rate, "jurisdiction_id"));
    if (jurisdiction === undefined) continue;
    const name = stringField(jurisdiction, "name");
    totals.set(name, (totals.get(name) ?? 0) + numberField(tax, "tax_amount"));
  }
  return [...totals]
    .map(([name, tax]) => ({ name, tax }))
    .sort((left, right) => right.tax - left.tax || left.name.localeCompare(right.name));
}

function taxByJurisdiction(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const rateToJurisdiction: Record<string, number> = {};
  for (const rate of rows(tables, "tax_rates")) {
    rateToJurisdiction[String(numberField(rate, "tax_rate_id"))] = numberField(
      rate,
      "jurisdiction_id",
    );
  }
  const names = Object.fromEntries(
    rows(tables, "tax_jurisdictions").map((row) => [
      String(numberField(row, "jurisdiction_id")),
      stringField(row, "name"),
    ]),
  );
  const totals: Record<string, number> = {};
  for (const tax of rows(tables, "order_taxes")) {
    const jurisdictionId = rateToJurisdiction[String(numberField(tax, "tax_rate_id"))];
    const name = jurisdictionId === undefined ? undefined : names[String(jurisdictionId)];
    if (name === undefined) continue;
    totals[name] = (totals[name] ?? 0) + numberField(tax, "tax_amount");
  }
  return Object.entries(totals)
    .map(([name, tax]) => ({ name, tax }))
    .sort((left, right) => right.tax - left.tax || left.name.localeCompare(right.name));
}

function fulfillmentByWarehouseIndexed(context: ReferenceQueryContext): unknown[] {
  const shipments = indexRows(context.tables, "shipments", "shipment_id");
  const warehouses = indexRows(context.tables, "warehouses", "warehouse_id");
  const groups = new Map<string, { shipments: Set<number>; units: number }>();
  for (const item of rows(context.tables, "shipment_items")) {
    const shipment = shipments.get(numberField(item, "shipment_id"));
    const warehouse =
      shipment === undefined ? undefined : warehouses.get(numberField(shipment, "warehouse_id"));
    if (warehouse === undefined) continue;
    const name = stringField(warehouse, "name");
    const value = groups.get(name) ?? { shipments: new Set<number>(), units: 0 };
    value.shipments.add(numberField(item, "shipment_id"));
    value.units += numberField(item, "quantity");
    groups.set(name, value);
  }
  return [...groups]
    .map(([name, value]) => ({ name, shipments: value.shipments.size, units: value.units }))
    .sort((left, right) => right.units - left.units || left.name.localeCompare(right.name));
}

function fulfillmentByWarehouse(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const warehouseNames = Object.fromEntries(
    rows(tables, "warehouses").map((row) => [
      String(numberField(row, "warehouse_id")),
      stringField(row, "name"),
    ]),
  );
  const shipmentWarehouses = Object.fromEntries(
    rows(tables, "shipments").map((row) => [
      String(numberField(row, "shipment_id")),
      numberField(row, "warehouse_id"),
    ]),
  );
  const groups: Record<string, { shipments: Set<number>; units: number }> = {};
  for (const item of rows(tables, "shipment_items")) {
    const shipmentId = numberField(item, "shipment_id");
    const warehouseId = shipmentWarehouses[String(shipmentId)];
    const name = warehouseId === undefined ? undefined : warehouseNames[String(warehouseId)];
    if (name === undefined) continue;
    const value = groups[name] ?? { shipments: new Set<number>(), units: 0 };
    value.shipments.add(shipmentId);
    value.units += numberField(item, "quantity");
    groups[name] = value;
  }
  return Object.entries(groups)
    .map(([name, value]) => ({ name, shipments: value.shipments.size, units: value.units }))
    .sort((left, right) => right.units - left.units || left.name.localeCompare(right.name));
}

function supplierInventoryIndexed(context: ReferenceQueryContext): unknown[] {
  const suppliers = indexRows(context.tables, "suppliers", "supplier_id");
  const groups = new Map<string, { movements: number; netUnits: number }>();
  for (const movement of rows(context.tables, "inventory_movements")) {
    const supplier = suppliers.get(numberField(movement, "supplier_id"));
    if (supplier === undefined) continue;
    const name = stringField(supplier, "name");
    const value = groups.get(name) ?? { movements: 0, netUnits: 0 };
    value.movements += 1;
    value.netUnits += numberField(movement, "quantity_delta");
    groups.set(name, value);
  }
  return [...groups]
    .map(([name, value]) => ({ name, ...value }))
    .sort((left, right) => right.movements - left.movements || left.name.localeCompare(right.name));
}

function supplierInventory(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const names = Object.fromEntries(
    rows(tables, "suppliers").map((row) => [
      String(numberField(row, "supplier_id")),
      stringField(row, "name"),
    ]),
  );
  const groups: Record<string, { movements: number; netUnits: number }> = {};
  for (const movement of rows(tables, "inventory_movements")) {
    const name = names[String(numberField(movement, "supplier_id"))];
    if (name === undefined) continue;
    const value = groups[name] ?? { movements: 0, netUnits: 0 };
    value.movements += 1;
    value.netUnits += numberField(movement, "quantity_delta");
    groups[name] = value;
  }
  return Object.entries(groups)
    .map(([name, value]) => ({ name, ...value }))
    .sort((left, right) => right.movements - left.movements || left.name.localeCompare(right.name));
}

function paymentFunnelIndexed(context: ReferenceQueryContext): unknown[] {
  const groups = new Map<string, { kind: string; status: string; count: number; amount: number }>();
  for (const transaction of rows(context.tables, "payment_transactions")) {
    const kind = stringField(transaction, "kind");
    const status = stringField(transaction, "status");
    const key = `${kind}\u0000${status}`;
    const value = groups.get(key) ?? { kind, status, count: 0, amount: 0 };
    value.count += 1;
    value.amount += numberField(transaction, "amount");
    groups.set(key, value);
  }
  return [...groups.values()].sort(
    (left, right) => left.kind.localeCompare(right.kind) || left.status.localeCompare(right.status),
  );
}

function paymentFunnel(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  const groups: Record<string, { kind: string; status: string; count: number; amount: number }> =
    {};
  for (const transaction of rows(tables, "payment_transactions")) {
    const kind = stringField(transaction, "kind");
    const status = stringField(transaction, "status");
    const key = `${kind}\u0000${status}`;
    const value = groups[key] ?? { kind, status, count: 0, amount: 0 };
    value.count += 1;
    value.amount += numberField(transaction, "amount");
    groups[key] = value;
  }
  return Object.values(groups).sort(
    (left, right) => left.kind.localeCompare(right.kind) || left.status.localeCompare(right.status),
  );
}

function orderAdjustmentsIndexed(context: ReferenceQueryContext): unknown[] {
  return collectOrderAdjustments(
    rows(context.tables, "orders"),
    rows(context.tables, "order_discounts"),
    rows(context.tables, "order_taxes"),
    true,
  );
}

function orderAdjustments(tables: ReadonlyMap<string, DatabaseRow[]>): unknown[] {
  return collectOrderAdjustments(
    rows(tables, "orders"),
    rows(tables, "order_discounts"),
    rows(tables, "order_taxes"),
    false,
  );
}

function collectOrderAdjustments(
  orderRows: DatabaseRow[],
  discounts: DatabaseRow[],
  taxes: DatabaseRow[],
  useMaps: boolean,
): unknown[] {
  const discountByOrder = new Map<number, number>();
  const taxByOrder = new Map<number, number>();
  for (const discount of discounts) {
    const orderId = numberField(discount, "order_id");
    discountByOrder.set(
      orderId,
      (discountByOrder.get(orderId) ?? 0) + numberField(discount, "amount"),
    );
  }
  for (const tax of taxes) {
    const orderId = numberField(tax, "order_id");
    taxByOrder.set(orderId, (taxByOrder.get(orderId) ?? 0) + numberField(tax, "tax_amount"));
  }
  const groups = new Map<string, { discounts: number; taxes: number }>();
  for (const order of useMaps ? orderRows : [...orderRows].reverse()) {
    const status = stringField(order, "status");
    const orderId = numberField(order, "order_id");
    const value = groups.get(status) ?? { discounts: 0, taxes: 0 };
    value.discounts += discountByOrder.get(orderId) ?? 0;
    value.taxes += taxByOrder.get(orderId) ?? 0;
    groups.set(status, value);
  }
  return [...groups]
    .map(([status, value]) => ({ status, ...value }))
    .sort((left, right) => left.status.localeCompare(right.status));
}

function referenceChecksum(value: unknown): number {
  const text = JSON.stringify(value, (_key, item: unknown) => {
    if (item instanceof Date) return item.toISOString();
    // Equivalent aggregates can differ by sub-cent floating-point noise when
    // their independent oracles visit rows in a different order.
    if (typeof item === "number" && Number.isFinite(item)) {
      return Math.round(item * 1_000_000_000) / 1_000_000_000;
    }
    return item;
  });
  let hash = 2_166_136_261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return hash >>> 0;
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

function validateConfig(value: unknown): BenchmarkConfig {
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

function progress(requestId: string, value: BenchmarkProgress): void {
  const response: ProgressResponse<BenchmarkProgress> = {
    version: protocolVersion,
    requestId,
    kind: "progress",
    progress: value,
  };
  self.postMessage(response);
}

function assertNotCancelled(requestId: string): void {
  if (cancelledRuns.has(requestId)) throw new DOMException("Benchmark cancelled", "AbortError");
}

async function storageEstimate(): Promise<StorageEstimate> {
  try {
    return await navigator.storage.estimate();
  } catch {
    return {};
  }
}

async function executePersistedAdHocQuery(sql: string): Promise<AdHocQueryResult> {
  const dataset = (await listPersistedDatasets())[0];
  if (dataset === undefined) {
    throw new Error("Run the relational benchmark before executing an ad-hoc query");
  }
  const totalStarted = performance.now();
  const store = await IndexedDbBlockStore.open({
    name: dataset.databaseName,
    durability: dataset.durability,
  });
  const database = new BrowserDatabase(store, {
    compression: dataset.compression,
    rowsPerBlock: Math.max(1, Math.min(50_000, Math.floor(dataset.targetBlockBytes / 16))),
  });
  let prepared: Awaited<ReturnType<BrowserDatabase["prepareQuery"]>> | undefined;
  try {
    const prepareStarted = performance.now();
    prepared = await database.prepareQuery(sql);
    const prepareMs = performance.now() - prepareStarted;
    prepared.execute();
    const samples: number[] = [];
    let result = prepared.execute();
    for (let index = 0; index < 7; index += 1) {
      const started = performance.now();
      result = prepared.execute();
      samples.push(performance.now() - started);
    }
    samples.sort((left, right) => left - right);
    const median = samples[Math.floor(samples.length / 2)] ?? 0;
    const p95 = samples[Math.ceil(samples.length * 0.95) - 1] ?? median;
    const previewRows = result.rows.slice(0, 100);
    const sourceRows = [...new Set(prepared.tables)].reduce(
      (total, table) => total + (dataset.tableRows[table] ?? 0),
      0,
    );
    return {
      runId: dataset.runId,
      sql: sql.trim(),
      tables: prepared.tables,
      columns: result.columns,
      rowCount: result.rows.length,
      previewRows,
      truncated: result.rows.length > previewRows.length,
      metrics: {
        prepareMs,
        executeMedianMs: median,
        executeP95Ms: p95,
        totalMs: performance.now() - totalStarted,
        iterations: samples.length,
        sourceRows,
        datasetRows: dataset.totalRows,
        storedBytes: dataset.storedBytes,
      },
    };
  } finally {
    prepared?.close();
    store.close();
  }
}

async function persistedDatasetStatus(): Promise<PersistedDatasetStatus> {
  const datasets = await listPersistedDatasets();
  const active = datasets[0];
  if (active === undefined) {
    return { kind: "persisted-dataset-status", available: false, datasetCount: 0 };
  }
  return {
    kind: "persisted-dataset-status",
    available: true,
    datasetCount: datasets.length,
    runId: active.runId,
    databaseName: active.databaseName,
    createdAt: active.createdAt,
    scale: active.scale,
    totalRows: active.totalRows,
    storedBytes: active.storedBytes,
  };
}

async function wipePersistedDatasets(): Promise<PersistedDatasetStatus> {
  const datasets = await listPersistedDatasets();
  for (const dataset of datasets) await deleteDatabase(dataset.databaseName);
  const registry = await openDatasetRegistry();
  try {
    const transaction = registry.transaction(DATASET_REGISTRY_STORE, "readwrite");
    transaction.objectStore(DATASET_REGISTRY_STORE).clear();
    await indexedDbTransactionDone(transaction);
  } finally {
    registry.close();
  }
  return { kind: "persisted-dataset-status", available: false, datasetCount: 0 };
}

async function registerPersistedDataset(record: PersistedDatasetRecord): Promise<void> {
  const registry = await openDatasetRegistry();
  try {
    const transaction = registry.transaction(DATASET_REGISTRY_STORE, "readwrite");
    transaction.objectStore(DATASET_REGISTRY_STORE).put(record);
    await indexedDbTransactionDone(transaction);
  } finally {
    registry.close();
  }
}

async function listPersistedDatasets(): Promise<PersistedDatasetRecord[]> {
  const registry = await openDatasetRegistry();
  try {
    const transaction = registry.transaction(DATASET_REGISTRY_STORE, "readonly");
    const request = transaction.objectStore(DATASET_REGISTRY_STORE).getAll();
    const records = (await indexedDbRequest<unknown[]>(request as IDBRequest<unknown[]>)).filter(
      isPersistedDatasetRecord,
    );
    await indexedDbTransactionDone(transaction);
    return records.sort((left, right) => right.createdAt.localeCompare(left.createdAt));
  } finally {
    registry.close();
  }
}

function isPersistedDatasetRecord(value: unknown): value is PersistedDatasetRecord {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  return (
    typeof record.runId === "string" &&
    typeof record.databaseName === "string" &&
    record.databaseName.startsWith(DATASET_DATABASE_PREFIX) &&
    typeof record.createdAt === "string" &&
    typeof record.scale === "number" &&
    typeof record.totalRows === "number" &&
    typeof record.storedBytes === "number" &&
    typeof record.tableRows === "object" &&
    record.tableRows !== null &&
    ["raw", "rle", "gzip"].includes(String(record.compression)) &&
    typeof record.targetBlockBytes === "number" &&
    ["relaxed", "strict"].includes(String(record.durability))
  );
}

function openDatasetRegistry(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DATASET_REGISTRY_NAME, 1);
    request.addEventListener("upgradeneeded", () => {
      if (!request.result.objectStoreNames.contains(DATASET_REGISTRY_STORE)) {
        request.result.createObjectStore(DATASET_REGISTRY_STORE, { keyPath: "runId" });
      }
    });
    request.addEventListener("success", () => resolve(request.result), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("Dataset registry could not be opened")),
      { once: true },
    );
  });
}

function indexedDbRequest<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.addEventListener("success", () => resolve(request.result), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("IndexedDB request failed")),
      { once: true },
    );
  });
}

function indexedDbTransactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.addEventListener("complete", () => resolve(), { once: true });
    transaction.addEventListener(
      "error",
      () => reject(transaction.error ?? new Error("IndexedDB transaction failed")),
      { once: true },
    );
    transaction.addEventListener(
      "abort",
      () => reject(transaction.error ?? new Error("IndexedDB transaction aborted")),
      { once: true },
    );
  });
}

async function deleteDatabase(name: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(name);
    request.addEventListener("success", () => resolve(), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("Database deletion failed")),
      { once: true },
    );
    request.addEventListener("blocked", () => reject(new Error("Database deletion was blocked")), {
      once: true,
    });
  });
}

function sum(values: number[]): number {
  return values.reduce((total, value) => total + value, 0);
}

function formatRate(rowsPerSecond: number): string {
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(rowsPerSecond)} rows/s`;
}

function getRequestId(value: unknown): string {
  if (typeof value === "object" && value !== null && "requestId" in value) {
    const requestId = value.requestId;
    if (typeof requestId === "string") return requestId;
  }
  return "unknown";
}

function describePlatform(userAgent: string): string {
  if (userAgent.includes("Mac OS X") || userAgent.includes("Macintosh")) return "macOS";
  if (userAgent.includes("Windows")) return "Windows";
  if (userAgent.includes("Android")) return "Android";
  if (userAgent.includes("Linux")) return "Linux";
  return "Unknown";
}
