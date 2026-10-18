import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { encodeBlock, inspectBlock } from "../packages/core/src/block-format/index.ts";
import { MemoryOpfs } from "../packages/core/src/testing/opfs-shim.ts";
import { keyForwardsTableRecord } from "../packages/core/src/engine/key-forwards.ts";
import { OpfsBlockStore } from "../packages/core/src/storage/opfs/index.ts";
import { walStagingTestHooks } from "../packages/core/src/storage/opfs/leader.ts";
import { OPFS_LAYOUT_VERSION } from "../packages/core/src/storage/opfs/upgrades.ts";
import type { TableRecord } from "../packages/core/src/storage/types.ts";

const databaseName = "native-fixture";
const prefix = `minnowdb/${databaseName}`;
const shim = new MemoryOpfs();

function table(id: string, name: string): TableRecord {
  return {
    id,
    name,
    managed: false,
    columns: [{ id: "value", name: "value", type: "string", nullable: true }],
    revision: 0,
    createdAt: "2026-08-24T12:00:00.000Z",
  };
}

/**
 * Layout 13's own shape: a table with a column declared TIMESTAMP without time zone, beside an
 * ordinary datetime column, which an older build would render as `timestamp with time zone`.
 */
function stampedTable(id: string, name: string): TableRecord {
  const record = table(id, name);
  return {
    ...record,
    columns: [
      ...record.columns,
      { id: "local_at", name: "local_at", type: "datetime", nullable: true, withoutTimeZone: true },
      { id: "zoned_at", name: "zoned_at", type: "datetime", nullable: true },
    ],
  };
}

const store = await OpfsBlockStore.open({
  name: databaseName,
  root: shim.root,
  // Eight entries land the first follower request in the checkpoint; the follower's staging
  // call, its withheld answer, and the last table stay in the write-ahead log's tail.
  // Nine with layout 11's job first: the same eight later entries as before join it there.
  // Ten with layout 12's job after it, for the same reason.
  checkpointEntries: 10,
  // A row-id reservation's answer is kept; a staging call's answer (a transaction record) is
  // too large for this ceiling and is withheld, so the fixture holds both ledger shapes.
  servedLedgerResultBytes: 160,
});
// Layout 11's own shape, in the checkpoint: a fold of the keyed table (written below) planned
// after a column was added, so its source has no blocks for that column, beside an update left
// holding only its key by DROP COLUMN. An older build refuses this plan. The attempt is recorded
// as aborted — a diagnostic that roots nothing — so every table and block below stays as it was.
const keyedSourceBlock = await encodeBlock({ type: "number", values: [1] }, "raw");
const keyedSourceDescription = inspectBlock(keyedSourceBlock);
const keyedSource = {
  blockId: "fixture-keyed-block",
  rowStart: 0,
  rowCount: 1,
  storedBytes: keyedSourceBlock.byteLength,
  encodedBytes: keyedSourceDescription.encodedLength,
  checksum: keyedSourceDescription.checksum,
};
const inertSource = { ...keyedSource, blockId: "fixture-inert-update-block" };
await store.createCompactionJob({
  id: "fixture-layout11-fold",
  tableId: "fixture-keyed",
  sourceManifestVersion: 2,
  sourceSegmentIds: ["fixture-keyed-segment", "fixture-inert-update"],
  sourceBlockIds: [inertSource.blockId, keyedSource.blockId].sort(),
  outputBlockIds: [],
  cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
  processedRows: 0,
  sourceStoredBytes: keyedSource.storedBytes * 2,
  outputStoredBytes: 0,
  logicalBytes: keyedSource.encodedBytes * 2,
  rewritePlan: {
    kind: "merge-v2",
    targetBlockBytes: 1024 * 1024,
    outputCompression: "raw",
    keyColumnId: "id",
    totalRows: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    rowIdSpans: [{ rowStart: 0, rowCount: 1, rowIdStart: 1n }],
    logicalOrder: 0,
    sourceSegments: [
      {
        segmentId: "fixture-keyed-segment",
        transactionId: "fixture-keyed-transaction",
        committedVersion: 2,
        kind: "insert",
        keyColumnId: "id",
        level: 0,
        logicalOrder: 0,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        rowIdSpans: [{ rowStart: 0, rowCount: 1, rowIdStart: 1n }],
        columns: [{ columnId: "id", type: "number", sourceBlocks: [keyedSource] }],
      },
      {
        segmentId: "fixture-inert-update",
        transactionId: "fixture-inert-update-transaction",
        committedVersion: 3,
        kind: "update",
        keyColumnId: "id",
        level: 0,
        logicalOrder: 1,
        rowCount: 1,
        rowIdStart: 0n,
        rowIdEndExclusive: 0n,
        rowIdSpans: [],
        columns: [{ columnId: "id", type: "number", sourceBlocks: [inertSource] }],
      },
    ],
    columns: [
      { columnId: "id", type: "number" },
      { columnId: "note", type: "string" },
    ],
    outputs: [{ rowStart: 0, rowCount: 1 }],
    resolution: { runs: 1, patches: 0, checksum: 0 },
  },
  outputCursor: { outputIndex: 0, columnIndex: 0, rowStart: 0 },
  memoryBudgetBytes: 1024 * 1024,
  minimumMemoryBytes: 4096,
  level0SourceStoredBytes: keyedSource.storedBytes * 2,
  anchorSourceStoredBytes: 0,
  peakWorkingBytes: 0,
  outputLogicalBytes: 0,
  targetLevel: 1,
  state: "aborted",
  transactionId: null,
  outputSegmentId: "fixture-layout11-fold/output",
  publishedVersion: null,
  revision: 0,
  createdAt: "2026-08-24T11:59:59.000Z",
  updatedAt: "2026-08-24T11:59:59.000Z",
  error: "fixture: an aborted fold attempt",
});
// Layout 12's own shape, in the checkpoint too: a delta merge of two single-row updates of the
// keyed table into one, under the order of the later. An older build refuses the plan kind; the
// attempt is recorded as aborted, so it roots nothing either.
const deltaKeyBlock = await encodeBlock({ type: "number", values: [1] }, "raw");
const deltaNoteBlock = await encodeBlock({ type: "string", values: ["merged"] }, "raw");
const deltaSourceBlock = (blockId: string, bytes: Uint8Array) => {
  const description = inspectBlock(bytes);
  return {
    blockId,
    rowStart: 0,
    rowCount: 1,
    storedBytes: bytes.byteLength,
    encodedBytes: description.encodedLength,
    checksum: description.checksum,
  };
};
const deltaSources = ["a", "b"].map((name, index) => ({
  segmentId: `fixture-delta-update-${name}`,
  transactionId: `fixture-delta-transaction-${name}`,
  committedVersion: 3 + index,
  kind: "update" as const,
  keyColumnId: "id",
  level: 0,
  logicalOrder: 1 + index,
  rowCount: 1,
  rowIdStart: 0n,
  rowIdEndExclusive: 0n,
  rowIdSpans: [],
  columns: [
    {
      columnId: "id",
      type: "number" as const,
      sourceBlocks: [deltaSourceBlock(`fixture-delta-key-${name}`, deltaKeyBlock)],
    },
    {
      columnId: "note",
      type: "string" as const,
      sourceBlocks: [deltaSourceBlock(`fixture-delta-note-${name}`, deltaNoteBlock)],
    },
  ],
}));
const deltaBlocks = deltaSources.flatMap(({ columns }) =>
  columns.flatMap(({ sourceBlocks }) => sourceBlocks),
);
const deltaStoredBytes = deltaBlocks.reduce((total, block) => total + block.storedBytes, 0);
await store.createCompactionJob({
  id: "fixture-layout12-delta",
  tableId: "fixture-keyed",
  sourceManifestVersion: 4,
  sourceSegmentIds: deltaSources.map(({ segmentId }) => segmentId),
  sourceBlockIds: deltaBlocks.map(({ blockId }) => blockId).sort(),
  outputBlockIds: [],
  cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
  processedRows: 0,
  sourceStoredBytes: deltaStoredBytes,
  outputStoredBytes: 0,
  logicalBytes: deltaBlocks.reduce((total, block) => total + block.encodedBytes, 0),
  rewritePlan: {
    kind: "delta-v1",
    keyColumnId: "id",
    columns: [
      { columnId: "id", type: "number" },
      { columnId: "note", type: "string" },
    ],
    logicalOrder: 2,
    sourceSegments: deltaSources,
    outputs: [
      {
        kind: "update",
        columns: [
          { columnId: "id", type: "number" },
          { columnId: "note", type: "string" },
        ],
        rowCount: 1,
        blockRows: [1],
      },
    ],
    resolution: { rows: 1, checksum: 0 },
  },
  outputCursor: null,
  memoryBudgetBytes: 0,
  minimumMemoryBytes: 0,
  level0SourceStoredBytes: deltaStoredBytes,
  anchorSourceStoredBytes: 0,
  peakWorkingBytes: 0,
  outputLogicalBytes: 0,
  targetLevel: 0,
  state: "aborted",
  transactionId: null,
  outputSegmentId: "fixture-layout12-delta/output-segment",
  publishedVersion: null,
  revision: 0,
  createdAt: "2026-08-24T11:59:59.500Z",
  updatedAt: "2026-08-24T11:59:59.500Z",
  error: "fixture: an aborted delta merge attempt",
});
await store.addTable(table("fixture-data", "data"));
// Layout 13's shape in the checkpoint, and again in the log's tail below.
await store.addTable(stampedTable("fixture-checkpoint-b", "checkpoint-b"));
// A follower on the same tree: its mutations reach the log through the leader with their
// request identity, and the served-request ledger records what they answered.
const follower = await OpfsBlockStore.open({ name: databaseName, root: shim.root });
await follower.reserveRowIds("fixture-data", 2);
const transaction = await store.beginTransaction({
  record: {
    id: "fixture-transaction",
    ownerId: "fixture-owner",
    expiresAt: "2026-08-24T13:00:00.000Z",
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: "2026-08-24T12:00:00.000Z",
    updatedAt: "2026-08-24T12:00:00.000Z",
    committedVersion: null,
  },
});
const block = await encodeBlock({ type: "string", values: ["native", null, "fixture"] }, "raw");
const staged = await store.stageTransactionArtifacts({
  transactionId: transaction.record.id,
  expectedRevision: transaction.record.revision,
  blocks: [{ id: "fixture-block", bytes: block }],
  segments: [
    {
      id: "fixture-segment",
      tableId: "fixture-data",
      transactionId: transaction.record.id,
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowCount: 3,
      rowIdStart: 1n,
      rowIdEndExclusive: 4n,
      rowIdSpans: [],
      columnBlockIds: { value: ["fixture-block"] },
      createdAt: "2026-08-24T12:00:01.000Z",
    },
  ],
  updatedAt: "2026-08-24T12:00:01.000Z",
});
await store.commitTransaction({
  transactionId: transaction.record.id,
  expectedTransactionRevision: staged.revision,
  expectedManifestVersion: null,
  levelZeroSegmentLimits: [{ tableId: "fixture-data", limit: 4096 }],
  committedAt: "2026-08-24T12:00:02.000Z",
});
const followerTransaction = await follower.beginTransaction({
  record: {
    id: "fixture-follower-transaction",
    ownerId: "fixture-follower",
    expiresAt: "2026-08-24T13:00:00.000Z",
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: "2026-08-24T12:00:03.000Z",
    updatedAt: "2026-08-24T12:00:03.000Z",
    committedVersion: null,
  },
});
await follower.stageTransactionArtifacts({
  transactionId: followerTransaction.record.id,
  expectedRevision: followerTransaction.record.revision,
  blocks: [{ id: "fixture-follower-block", bytes: block }],
  segments: [],
  updatedAt: "2026-08-24T12:00:04.000Z",
});
// Layout 9's own shape in the log's tail: one commit whose frame spans continuation frames.
// A real one is megabytes; smaller pieces freeze the same framing in a few hundred kilobytes.
walStagingTestHooks.pieceBytes = 16 * 1024;
await store.addTable({
  ...table("fixture-keyed", "keyed"),
  columns: [{ id: "id", name: "id", type: "number", nullable: false }],
  uniqueKeyColumnId: "id",
});
const keyedTransaction = await store.beginTransaction({
  record: {
    id: "fixture-keyed-transaction",
    ownerId: "fixture-owner",
    expiresAt: "2026-08-24T13:00:00.000Z",
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: "2026-08-24T12:00:05.000Z",
    updatedAt: "2026-08-24T12:00:05.000Z",
    committedVersion: null,
  },
});
// Layout 10's own shape, also in the log's tail: the internal key-forwards table, added while the
// keyed transaction is open. It leaves the schema epoch alone, so that transaction's commit,
// logged after it, keeps its guard. The commit stages and publishes in one entry, which keeps
// the tail short enough that no second checkpoint absorbs it.
await store.addTable(keyForwardsTableRecord("2026-08-24T12:00:05.500Z"));
const keyedBlock = await encodeBlock({ type: "number", values: [1] }, "raw");
if (store.writeTransaction === undefined)
  throw new Error("The OPFS store has no single-shot write");
await store.writeTransaction({
  transaction: {
    id: keyedTransaction.record.id,
    expectedRevision: keyedTransaction.record.revision,
  },
  blocks: [{ id: "fixture-keyed-block", bytes: keyedBlock }],
  segments: [
    {
      id: "fixture-keyed-segment",
      tableId: "fixture-keyed",
      transactionId: keyedTransaction.record.id,
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      rowIdSpans: [],
      columnBlockIds: { id: ["fixture-keyed-block"] },
      createdAt: "2026-08-24T12:00:06.000Z",
    },
  ],
  expectedManifestVersion: (await store.getCurrentManifestVersion()) ?? null,
  levelZeroSegmentLimits: [{ tableId: "fixture-keyed", limit: 4096 }],
  uniqueKeyChanges: [
    {
      tableId: "fixture-keyed",
      keyTokens: Array.from({ length: 16_384 }, (_, index) => `number:${String(index)}`),
      requireAbsent: true,
    },
  ],
  committedAt: "2026-08-24T12:00:07.000Z",
});
await store.addTable(stampedTable("fixture-wal-tail", "wal-tail"));
follower.close();
store._crashForTests();

const paths = [
  `${prefix}/format.json`,
  `${prefix}/wal`,
  `${prefix}/wal-acknowledgements`,
  `${prefix}/checkpoint-a`,
  `${prefix}/checkpoint-b`,
  `${prefix}/extents/000000`,
];
const files = Object.fromEntries(
  paths.map((path) => {
    const bytes = shim.readFileBytes(path) ?? new Uint8Array(0);
    return [path, Buffer.from(bytes).toString("base64")];
  }),
);
const { version: writerPackageVersion } = JSON.parse(
  await readFile(new URL("../packages/core/package.json", import.meta.url), "utf8"),
) as { version: string };
const fixture = {
  layoutFormatVersion: OPFS_LAYOUT_VERSION,
  writerPackageVersion,
  files,
  expectations: {
    // In \`listTables\` order, which compares names with \`localeCompare\`.
    tables: ["checkpoint-b", "data", "keyed", "\u0000minnow_key_forwards", "wal-tail"],
    blockId: "fixture-block",
    blockValues: ["native", null, "fixture"],
  },
};
const target = fileURLToPath(
  new URL(
    `../packages/core/format-fixtures/opfs-layout${String(OPFS_LAYOUT_VERSION)}.json`,
    import.meta.url,
  ),
);
await writeFile(target, `${JSON.stringify(fixture, null, 2)}\n`);
console.log(`Wrote ${target}`);
