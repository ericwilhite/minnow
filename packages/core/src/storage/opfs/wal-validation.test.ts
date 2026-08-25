import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import {
  MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL,
  MAX_FTS_BASE_CHUNKS,
  MAX_STORAGE_ID_CHARACTERS,
  MAX_TEMP_RUN_BATCH_BYTES,
  MAX_TEMP_RUN_PAGES_PER_BATCH,
  MAX_TRANSACTION_STAGE_BLOCKS,
  MAX_TRANSACTION_STAGE_SEGMENTS,
  SNAPSHOT_FRAME_KINDS,
  type SnapshotFrameStreamHeader,
} from "../types.js";
import { OpfsTree } from "./files.js";
import { OpfsBlockStore } from "./index.js";
import { WalWriter } from "../toolkit/wal.js";

const CREATED_AT = "2026-08-24T00:00:00.000Z";
const EXPIRES_AT = "2026-08-24T00:30:00.000Z";
const PLACEMENT = { extent: 0, offset: 0, length: 1, checksum: 0 };

function emptyHeader(databaseVersion = 0): SnapshotFrameStreamHeader {
  return {
    formatVersion: 1,
    databaseVersion,
    createdAt: CREATED_AT,
    kinds: Object.fromEntries(
      SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
    ) as SnapshotFrameStreamHeader["kinds"],
  };
}

function emptyObserved(): Record<
  string,
  { frameCount: number; itemCount: number; storedBytes: number }
> {
  return Object.fromEntries(
    SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
  );
}

function exportState(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    sessionId: "export-session",
    ownerId: "export-owner",
    createdAt: CREATED_AT,
    expiresAt: EXPIRES_AT,
    leaseRevision: 0,
    manifestVersion: 0,
    header: emptyHeader(),
    ledgerId: "export-ledger",
    ledgerLength: 0,
    metadataFrameCount: 0,
    nextSequence: 0,
    metadataOffset: 0,
    blockCursor: null,
    lastSequence: null,
    lastMetadataOffset: null,
    lastBlockId: null,
    ...overrides,
  };
}

function importState(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    identity: "snapshot-identity",
    ownerId: "import-owner",
    version: 0,
    createdAt: CREATED_AT,
    expiresAt: EXPIRES_AT,
    header: emptyHeader(),
    ledgerId: "import-ledger",
    ledgerLength: 0,
    nextSequence: 0,
    stagedBytes: 0,
    blockCount: 0,
    blockBytes: 0,
    checksum: 0,
    currentKindIndex: 0,
    observed: emptyObserved(),
    lastBatchStartSequence: null,
    lastBatchOffset: null,
    lastBatchFrameCount: 0,
    completedReplay: false,
    ...overrides,
  };
}

const BUILD_IDENTITY = {
  tableId: "table",
  columnId: "column",
  buildId: "build",
  ownerId: "builder",
};
const BUILD_RENEWAL = {
  ...BUILD_IDENTITY,
  expiresAtCutoff: CREATED_AT,
  expiresAt: EXPIRES_AT,
  updatedAt: CREATED_AT,
};
const EXPORT_RENEWAL = {
  sessionId: "export-session",
  ownerId: "export-owner",
  sequence: 0,
  expiresAtCutoff: CREATED_AT,
  expiresAt: EXPIRES_AT,
};
const IMPORT_RENEWAL = {
  identity: "snapshot-identity",
  ownerId: "import-owner",
  expiresAtCutoff: CREATED_AT,
  expiresAt: EXPIRES_AT,
};

interface InvalidWalCase {
  readonly label: string;
  readonly entry: unknown;
  readonly error: RegExp;
}

const invalidWalCases: readonly InvalidWalCase[] = [
  { label: "scalar-body", entry: 1, error: /WAL entry is not an object/ },
  {
    label: "zero-sequence",
    entry: { seq: 0, op: "removePrunedManifestRecords", maxItems: 1 },
    error: /invalid sequence: 0/,
  },
  {
    label: "non-string-operation",
    entry: { seq: 1, op: 7 },
    error: /Unsupported OPFS WAL operation: 7/,
  },
  {
    label: "remove-table-catalog-epoch",
    entry: {
      seq: 1,
      op: "removeTable",
      id: "table",
      expectedRevision: 0,
      expectedCatalogEpoch: -1,
    },
    error: /Invalid removeTable expectedCatalogEpoch/,
  },
  {
    label: "stage-block-array",
    entry: {
      seq: 1,
      op: "stageTransactionArtifacts",
      transactionId: "transaction",
      expectedRevision: 0,
      blocks: null,
      segments: [],
      updatedAt: CREATED_AT,
    },
    error: /Invalid stageTransactionArtifacts blocks/,
  },
  {
    label: "stage-duplicate-block",
    entry: {
      seq: 1,
      op: "stageTransactionArtifacts",
      transactionId: "transaction",
      expectedRevision: 0,
      blocks: [
        { id: "block", placement: PLACEMENT },
        { id: "block", placement: PLACEMENT },
      ],
      segments: [],
      updatedAt: CREATED_AT,
    },
    error: /Invalid or duplicate stageTransactionArtifacts blocks/,
  },
  {
    label: "stage-block-limit",
    entry: {
      seq: 1,
      op: "stageTransactionArtifacts",
      transactionId: "transaction",
      expectedRevision: 0,
      blocks: Array.from({ length: MAX_TRANSACTION_STAGE_BLOCKS + 1 }, (_, index) => ({
        id: `block-${String(index)}`,
        placement: PLACEMENT,
      })),
      segments: [],
      updatedAt: CREATED_AT,
    },
    error: /Invalid stageTransactionArtifacts block count/,
  },
  {
    label: "stage-segment-array",
    entry: {
      seq: 1,
      op: "stageTransactionArtifacts",
      transactionId: "transaction",
      expectedRevision: 0,
      blocks: [],
      segments: null,
      updatedAt: CREATED_AT,
    },
    error: /Invalid stageTransactionArtifacts segments/,
  },
  {
    label: "stage-segment-limit",
    entry: {
      seq: 1,
      op: "stageTransactionArtifacts",
      transactionId: "transaction",
      expectedRevision: 0,
      blocks: [],
      segments: Array.from({ length: MAX_TRANSACTION_STAGE_SEGMENTS + 1 }, () => null),
      updatedAt: CREATED_AT,
    },
    error: /Invalid stageTransactionArtifacts segment count/,
  },
  {
    label: "write-block-limit",
    entry: {
      seq: 1,
      op: "writeTransaction",
      input: { segments: [] },
      blocks: Array.from({ length: MAX_TRANSACTION_STAGE_BLOCKS + 1 }, (_, index) => ({
        id: `block-${String(index)}`,
        placement: PLACEMENT,
      })),
    },
    error: /Invalid writeTransaction block count/,
  },
  {
    label: "write-segment-array",
    entry: { seq: 1, op: "writeTransaction", input: { segments: null }, blocks: [] },
    error: /Invalid writeTransaction segment count/,
  },
  {
    label: "restore-page-array",
    entry: { seq: 1, op: "restoreTempRunPages", pages: null },
    error: /Invalid restoreTempRunPages pages/,
  },
  {
    label: "restore-page-limit",
    entry: {
      seq: 1,
      op: "restoreTempRunPages",
      pages: Array.from({ length: MAX_TEMP_RUN_PAGES_PER_BATCH + 1 }, () => null),
    },
    error: /Invalid restoreTempRunPages page count/,
  },
  {
    label: "reserve-missing-length",
    entry: {
      seq: 1,
      op: "reserveTempRunPages",
      pages: [{ ownerId: "owner", runId: "run", pageIndex: 0, length: null }],
    },
    error: /Temp page reservation length is missing/,
  },
  {
    label: "reserve-byte-limit",
    entry: {
      seq: 1,
      op: "reserveTempRunPages",
      pages: [
        { ownerId: "owner", runId: "run", pageIndex: 0, length: MAX_TEMP_RUN_BATCH_BYTES },
        { ownerId: "owner", runId: "run", pageIndex: 1, length: 1 },
      ],
    },
    error: /Temp page reservation byte count is invalid/,
  },
  {
    label: "remove-temp-owner",
    entry: { seq: 1, op: "removeTempRun", ownerId: "", runId: "run" },
    error: /Invalid removeTempRun ownerId/,
  },
  {
    label: "remove-temp-run-limit",
    entry: {
      seq: 1,
      op: "removeTempRun",
      ownerId: "owner",
      runId: "x".repeat(MAX_STORAGE_ID_CHARACTERS + 1),
    },
    error: /Invalid removeTempRun runId/,
  },
  {
    label: "relocation-entry",
    entry: {
      seq: 1,
      op: "relocatePayloads",
      blocks: [],
      ftsChunks: [null],
      ftsBuildChunks: [],
    },
    error: /Invalid full-text relocations entry/,
  },
  {
    label: "relocation-key",
    entry: {
      seq: 1,
      op: "relocatePayloads",
      blocks: [],
      ftsChunks: [{ key: "", ordinal: 0, from: PLACEMENT, placement: PLACEMENT }],
      ftsBuildChunks: [],
    },
    error: /Invalid full-text relocations identity/,
  },
  {
    label: "relocation-ordinal",
    entry: {
      seq: 1,
      op: "relocatePayloads",
      blocks: [],
      ftsChunks: [{ key: "key", ordinal: -1, from: PLACEMENT, placement: PLACEMENT }],
      ftsBuildChunks: [],
    },
    error: /Invalid full-text relocations ordinal/,
  },
  {
    label: "relocation-source-placement",
    entry: {
      seq: 1,
      op: "relocatePayloads",
      blocks: [],
      ftsChunks: [{ key: "key", ordinal: 0, from: null, placement: PLACEMENT }],
      ftsBuildChunks: [],
    },
    error: /storage corruption at recovery/,
  },
  {
    label: "relocation-target-placement",
    entry: {
      seq: 1,
      op: "relocatePayloads",
      blocks: [],
      ftsChunks: [{ key: "key", ordinal: 0, from: PLACEMENT, placement: null }],
      ftsBuildChunks: [],
    },
    error: /storage corruption at recovery/,
  },
  {
    label: "pointer-bounds-count",
    entry: {
      seq: 1,
      op: "writeFtsBase",
      tableId: "table",
      columnId: "column",
      pointer: { chunks: [PLACEMENT], chunkBounds: [], coversVersion: 0, totalTokens: 0 },
    },
    error: /Invalid writeFtsBase pointer bounds/,
  },
  {
    label: "pointer-chunk-limit",
    entry: {
      seq: 1,
      op: "writeFtsBase",
      tableId: "table",
      columnId: "column",
      pointer: {
        chunks: Array.from({ length: MAX_FTS_BASE_CHUNKS + 1 }, () => PLACEMENT),
        chunkBounds: Array.from({ length: MAX_FTS_BASE_CHUNKS + 1 }, () => ({
          first: "a",
          last: "a",
        })),
        coversVersion: 0,
        totalTokens: 0,
      },
    },
    error: /Invalid writeFtsBase pointer chunk count/,
  },
  {
    label: "pointer-placement",
    entry: {
      seq: 1,
      op: "writeFtsBase",
      tableId: "table",
      columnId: "column",
      pointer: {
        chunks: [null],
        chunkBounds: [{ first: "a", last: "a" }],
        coversVersion: 0,
        totalTokens: 0,
      },
    },
    error: /storage corruption at recovery/,
  },
  {
    label: "pointer-coverage-version",
    entry: {
      seq: 1,
      op: "writeFtsBase",
      tableId: "table",
      columnId: "column",
      pointer: { chunks: [], chunkBounds: [], coversVersion: -2, totalTokens: 0 },
    },
    error: /Invalid writeFtsBase pointer coversVersion/,
  },
  {
    label: "pointer-total-tokens",
    entry: {
      seq: 1,
      op: "writeFtsBase",
      tableId: "table",
      columnId: "column",
      pointer: { chunks: [], chunkBounds: [], coversVersion: 0, totalTokens: -1 },
    },
    error: /Invalid writeFtsBase pointer totalTokens/,
  },
  {
    label: "pointer-chunk-bounds",
    entry: {
      seq: 1,
      op: "writeFtsBase",
      tableId: "table",
      columnId: "column",
      pointer: {
        chunks: [PLACEMENT],
        chunkBounds: [{ first: "z", last: "a" }],
        coversVersion: 0,
        totalTokens: 0,
      },
    },
    error: /Invalid writeFtsBase pointer chunk bounds/,
  },
  {
    label: "build-begin-lifetime",
    entry: {
      seq: 1,
      op: "beginFtsBaseBuild",
      input: { ...BUILD_IDENTITY, createdAt: CREATED_AT, expiresAt: CREATED_AT },
    },
    error: /Posting build expiration interval is invalid/,
  },
  {
    label: "build-renew-lifetime",
    entry: {
      seq: 1,
      op: "renewFtsBaseBuild",
      input: { ...BUILD_RENEWAL, expiresAt: CREATED_AT },
    },
    error: /Posting build renewal interval is invalid/,
  },
  {
    label: "build-chunk-ordinal-limit",
    entry: {
      seq: 1,
      op: "writeFtsBaseBuildChunk",
      input: { ...BUILD_RENEWAL, ordinal: MAX_FTS_BASE_CHUNKS },
      placement: PLACEMENT,
      bounds: { first: "a", last: "a" },
      totalTokens: 0,
      retainedEntries: 0,
    },
    error: /ordinal exceeds the chunk-count limit/,
  },
  {
    label: "build-chunk-entry-limit",
    entry: {
      seq: 1,
      op: "writeFtsBaseBuildChunk",
      input: { ...BUILD_RENEWAL, ordinal: 0 },
      placement: PLACEMENT,
      bounds: { first: "a", last: "a" },
      totalTokens: 0,
      retainedEntries: MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL + 1,
    },
    error: /entry count exceeds the staged limit/,
  },
  {
    label: "build-chunk-bounds",
    entry: {
      seq: 1,
      op: "writeFtsBaseBuildChunk",
      input: { ...BUILD_RENEWAL, ordinal: 0 },
      placement: PLACEMENT,
      bounds: null,
      totalTokens: 0,
      retainedEntries: 0,
    },
    error: /Invalid full-text build chunk bounds/,
  },
  {
    label: "build-finish-chunk-limit",
    entry: {
      seq: 1,
      op: "finishFtsBaseBuild",
      input: {
        ...BUILD_IDENTITY,
        expiresAtCutoff: CREATED_AT,
        coversVersion: 0,
        chunkCount: MAX_FTS_BASE_CHUNKS + 1,
        totalTokens: 0,
        completedAt: CREATED_AT,
      },
    },
    error: /Posting build exceeds the chunk-count limit/,
  },
  {
    label: "build-abort-cutoff",
    entry: {
      seq: 1,
      op: "abortFtsBaseBuild",
      input: { ...BUILD_IDENTITY, expiresAtCutoff: "not-a-timestamp" },
    },
    error: /Invalid posting build abort cutoff/,
  },
  {
    label: "export-cursor",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameExport",
      state: exportState({ metadataFrameCount: 1 }),
    },
    error: /Snapshot frame export cursor is inconsistent/,
  },
  {
    label: "export-header-version",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameExport",
      state: exportState({ manifestVersion: 1 }),
    },
    error: /Snapshot frame export header version changed/,
  },
  {
    label: "export-last-sequence",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameExport",
      state: exportState({ lastSequence: -1 }),
    },
    error: /Invalid snapshot frame export lastSequence/,
  },
  {
    label: "export-lifetime",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameExport",
      state: exportState({ expiresAt: CREATED_AT }),
    },
    error: /Snapshot frame export expiration interval is invalid/,
  },
  {
    label: "import-checksum",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameImport",
      state: importState({ checksum: 0x1_0000_0000 }),
    },
    error: /Snapshot frame import checksum or kind cursor is invalid/,
  },
  {
    label: "import-replay-mode",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameImport",
      state: importState({ completedReplay: "no" }),
    },
    error: /Snapshot frame import replay mode is invalid/,
  },
  {
    label: "import-header-version",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameImport",
      state: importState({ version: 1 }),
    },
    error: /Snapshot frame import header version changed/,
  },
  {
    label: "import-observed-summary",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameImport",
      state: importState({
        observed: {
          ...emptyObserved(),
          "catalog-page": { frameCount: 1, itemCount: 0, storedBytes: 0 },
        },
      }),
    },
    error: /Snapshot catalog-page observed summary exceeds its header/,
  },
  {
    label: "import-sequence",
    entry: {
      seq: 1,
      op: "beginSnapshotFrameImport",
      state: importState({ nextSequence: 1 }),
    },
    error: /Snapshot frame import sequence exceeds its header/,
  },
  {
    label: "advance-export-sequence",
    entry: {
      seq: 1,
      op: "advanceSnapshotFrameExport",
      input: { ...EXPORT_RENEWAL, sequence: -1 },
      expectedLeaseRevision: 0,
      next: {},
    },
    error: /Invalid snapshot frame export sequence/,
  },
  {
    label: "advance-export-revision",
    entry: {
      seq: 1,
      op: "advanceSnapshotFrameExport",
      input: EXPORT_RENEWAL,
      expectedLeaseRevision: -1,
      next: {},
    },
    error: /Invalid snapshot frame export lease revision/,
  },
  {
    label: "advance-export-cursor",
    entry: {
      seq: 1,
      op: "advanceSnapshotFrameExport",
      input: EXPORT_RENEWAL,
      expectedLeaseRevision: 0,
      next: null,
    },
    error: /Invalid snapshot frame export cursor/,
  },
  {
    label: "close-export-input",
    entry: { seq: 1, op: "closeSnapshotFrameExport", input: null },
    error: /Invalid closeSnapshotFrameExport input/,
  },
  {
    label: "close-export-session",
    entry: {
      seq: 1,
      op: "closeSnapshotFrameExport",
      input: { sessionId: "", ownerId: "owner" },
    },
    error: /Invalid closeSnapshotFrameExport session id/,
  },
  {
    label: "close-export-owner",
    entry: {
      seq: 1,
      op: "closeSnapshotFrameExport",
      input: { sessionId: "session", ownerId: "" },
    },
    error: /Invalid closeSnapshotFrameExport owner id/,
  },
  {
    label: "renew-import-lifetime",
    entry: {
      seq: 1,
      op: "renewSnapshotFrameImport",
      input: { ...IMPORT_RENEWAL, expiresAt: CREATED_AT },
    },
    error: /Snapshot frame import expiration interval is invalid/,
  },
  {
    label: "append-import-replay",
    entry: {
      seq: 1,
      op: "appendSnapshotImportFrames",
      input: IMPORT_RENEWAL,
      state: {},
      blockPlacements: [],
      replay: "no",
    },
    error: /Invalid snapshot frame replay mode/,
  },
  {
    label: "cancel-import-input",
    entry: { seq: 1, op: "cancelSnapshotFrameImport", input: null },
    error: /Invalid cancelSnapshotFrameImport input/,
  },
  {
    label: "cancel-import-fields",
    entry: {
      seq: 1,
      op: "cancelSnapshotFrameImport",
      input: { identity: "identity", ownerId: "owner", ignored: true },
    },
    error: /Invalid cancelSnapshotFrameImport input fields/,
  },
  {
    label: "cancel-import-identity",
    entry: {
      seq: 1,
      op: "cancelSnapshotFrameImport",
      input: { identity: "", ownerId: "owner" },
    },
    error: /Invalid cancelSnapshotFrameImport identity/,
  },
  {
    label: "cancel-import-owner",
    entry: {
      seq: 1,
      op: "cancelSnapshotFrameImport",
      input: { identity: "identity", ownerId: "" },
    },
    error: /Invalid cancelSnapshotFrameImport owner id/,
  },
];

describe("OPFS checksum-valid WAL validation", () => {
  it.each(invalidWalCases)("fails closed on $label", async ({ label, entry, error }) => {
    const shim = new MemoryOpfs();
    const name = `wal-validation-${label}`;
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    store._crashForTests();
    const handle = await new OpfsTree(shim.root).openHandle(["minnowdb", name, "wal"], {
      create: false,
    });
    new WalWriter(handle, 0).append(entry, false);
    handle.close();

    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toThrow(error);
  });
});
