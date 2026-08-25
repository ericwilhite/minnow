import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { encodeBlock } from "../packages/core/src/block-format/index.ts";
import { MemoryOpfs } from "../packages/core/src/testing/opfs-shim.ts";
import { OpfsBlockStore } from "../packages/core/src/storage/opfs/index.ts";
import { LOG_FORMAT_VERSION } from "../packages/core/src/storage/toolkit/wire.ts";
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

const store = await OpfsBlockStore.open({
  name: databaseName,
  root: shim.root,
  checkpointEntries: 5,
});
await store.addTable(table("fixture-data", "data"));
await store.addTable(table("fixture-checkpoint-b", "checkpoint-b"));
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
await store.addTable(table("fixture-wal-tail", "wal-tail"));
store._crashForTests();

const paths = [
  `${prefix}/format.json`,
  `${prefix}/wal`,
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
const fixture = {
  layoutFormatVersion: LOG_FORMAT_VERSION,
  files,
  expectations: {
    tables: ["checkpoint-b", "data", "wal-tail"],
    blockId: "fixture-block",
    blockValues: ["native", null, "fixture"],
  },
};
const target = fileURLToPath(
  new URL(
    `../packages/core/format-fixtures/opfs-layout${String(LOG_FORMAT_VERSION)}.json`,
    import.meta.url,
  ),
);
await writeFile(target, `${JSON.stringify(fixture, null, 2)}\n`);
console.log(`Wrote ${target}`);
