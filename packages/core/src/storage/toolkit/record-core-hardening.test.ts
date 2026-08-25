import { describe, expect, it } from "vitest";
import {
  MAX_MANIFEST_BLOCK_PRESENCE_IDS,
  MAX_PINNED_MANIFEST_VERSION_LAG,
  MAX_PINNED_RETIRED_BLOCKS,
  MAX_PINNED_RETIRED_BYTES,
  MAX_CATALOG_RETAINED_BYTES,
  MAX_MANIFEST_RECORDS,
  MAX_SEGMENT_RECORDS,
  MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_BYTES,
  MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_ENTRIES,
  SNAPSHOT_FRAME_KINDS,
  MAX_STORAGE_ID_CHARACTERS,
  MAX_TRANSACTION_PENDING_BLOCKS,
  CompactionJobConflictError,
  GarbageCollectionJobConflictError,
  StorageResourceLimitError,
  UniqueKeyConflictError,
  secondaryUniqueKeyNamespace,
  catalogRecordRetainedBytes,
  manifestRecordRetainedBytes,
  manifestRecordRetainedReservationBytes,
  segmentRecordRetainedBytes,
  assertSnapshotImportAcceleratorUsage,
  type CompactionJobRecord,
  type GarbageCollectionDiscovery,
  type Manifest,
  type SegmentRecord,
  type SnapshotMetadataItem,
  type TransactionRecord,
  type TableRecord,
} from "../types.js";
import {
  OrderedKeyIndex,
  OrderedStringSet,
  RecordCore,
  SegmentRecordMap,
  validateAutoIncrementReservation,
  validateBeginTransactionInput,
  validateBlockWriteBytes,
  validateFtsBaseInput,
  validateFtsPostingChunks,
  validateId,
  validateSegmentRuntimeRecord,
  validateTempRunPage,
  validateTempRunPageIdentity,
} from "./record-core.js";
import { encodeRecordJson } from "./wire.js";

function transaction(id: string, snapshotVersion: number | null): TransactionRecord {
  return {
    id,
    ownerId: `owner-${id}`,
    expiresAt: "2026-08-24T01:00:00.000Z",
    snapshotVersion,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: "2026-08-24T00:00:00.000Z",
    updatedAt: "2026-08-24T00:00:00.000Z",
    committedVersion: null,
  };
}

function discovery(
  overrides: Partial<GarbageCollectionDiscovery> = {},
): GarbageCollectionDiscovery {
  return {
    phase: "manifests",
    currentManifestVersion: 3,
    retainAboveVersion: 3,
    retainAfter: Date.parse("2026-08-24T00:00:00.000Z"),
    maxPlanningItems: 64,
    manifestCursor: null,
    segmentCursor: null,
    transactionCursor: null,
    compactionCursor: null,
    visitedRecords: 0,
    ...overrides,
  };
}

function snapshotHeader(version = 0) {
  return {
    formatVersion: 1,
    databaseVersion: version,
    createdAt: "2026-08-24T00:00:00.000Z",
    kinds: Object.fromEntries(
      SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
    ) as Record<
      (typeof SNAPSHOT_FRAME_KINDS)[number],
      { frameCount: number; itemCount: number; storedBytes: number }
    >,
  } as const;
}

function* uniqueSnapshotItems(
  tokenCount: number,
  token: (index: number) => string,
  chunkSize: number,
): IterableIterator<SnapshotMetadataItem> {
  const table: TableRecord = {
    id: "snapshot-unique-table",
    name: "snapshot_unique_table",
    columns: [{ id: "key", name: "key", type: "string", nullable: false }],
    uniqueKeyColumnId: "key",
    managed: false,
    revision: 0,
    createdAt: "2026-08-24T00:00:00.000Z",
  };
  yield { kind: "table", record: table, nextRowId: 1n, autoIncrement: [] };
  const chunkCount = Math.ceil(tokenCount / chunkSize);
  yield {
    kind: "unique-generation",
    tableId: table.id,
    indexId: null,
    namespaceId: table.id,
    generationId: "snapshot-unique-generation",
    chunkCount,
    tokenCount,
  };
  for (let ordinal = 0; ordinal < chunkCount; ordinal += 1) {
    const start = ordinal * chunkSize;
    const end = Math.min(tokenCount, start + chunkSize);
    yield {
      kind: "unique-chunk",
      namespaceId: table.id,
      generationId: "snapshot-unique-generation",
      ordinal,
      keyTokens: Array.from({ length: end - start }, (_, offset) => token(start + offset)),
    };
  }
}

describe("RecordCore hardening", () => {
  it("rejects reordered or noncanonical segment journals before replacing live checkpoint state", () => {
    const physical = new Set(["ordinal-block-0", "ordinal-block-1"]);
    const core = new RecordCore({
      hasBlock: (id) => physical.has(id),
      blockByteLength: (id) => (physical.has(id) ? 1 : undefined),
    });
    core.addTable({
      id: "ordinal-table",
      name: "ordinal_table",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    core.createTransaction(transaction("ordinal-owner", null));
    const segments: SegmentRecord[] = [0, 1].map((commitOrdinal) => ({
      id: `ordinal-segment-${String(commitOrdinal)}`,
      tableId: "ordinal-table",
      transactionId: "ordinal-owner",
      rowCount: 1,
      rowIdStart: BigInt(commitOrdinal + 1),
      rowIdEndExclusive: BigInt(commitOrdinal + 2),
      columnBlockIds: { value: [`ordinal-block-${String(commitOrdinal)}`] },
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal,
      rowIdSpans: [],
      createdAt: "2026-08-24T00:00:01.000Z",
    }));
    core.stageTransactionArtifacts(
      {
        transactionId: "ordinal-owner",
        expectedRevision: 0,
        blocks: segments.map((segment, index) => ({
          id: `ordinal-block-${String(index)}`,
          bytes: new Uint8Array(0),
        })),
        segments,
        updatedAt: "2026-08-24T00:00:01.000Z",
      },
      { blocksPrevalidated: true },
    );
    const checkpoint = core.dump();
    core.load(checkpoint);
    core.load(checkpoint);
    const before = core.dump();

    const reordered = structuredClone(checkpoint);
    const owner = reordered.transactions.find((record) => record.id === "ordinal-owner");
    if (owner === undefined) throw new Error("Missing ordinal owner fixture");
    owner.pendingSegmentIds.reverse();
    expect(() => core.load(reordered)).toThrow(/noncanonical commit ordinal/);
    expect(core.dump()).toEqual(before);

    const duplicateOrdinal = structuredClone(checkpoint);
    const second = duplicateOrdinal.segments.find((segment) => segment.id === "ordinal-segment-1");
    if (second === undefined) throw new Error("Missing ordinal segment fixture");
    second.commitOrdinal = 0;
    expect(() => core.load(duplicateOrdinal)).toThrow(/noncanonical commit ordinal/);
    expect(core.dump()).toEqual(before);
  });

  it("rebuilds pending catalog reservations exactly across repeated loads and releases them", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const pending: TableRecord = {
      id: "pending-table",
      name: "pending_table",
      columns: [{ id: "value", name: "value", type: "number", nullable: true }],
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    };
    const input = {
      record: {
        id: "pending-owner",
        ownerId: "owner-pending",
        expiresAt: "2026-08-24T01:00:00.000Z",
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active" as const,
        revision: 0,
        startedAt: "2026-08-24T00:00:00.000Z",
        updatedAt: "2026-08-24T00:00:00.000Z",
        committedVersion: null,
      },
      pendingTable: { record: pending, nextRowId: 1n, expectedCatalogEpoch: 0 },
    };
    core.beginTransaction(input);
    const checkpoint = core.dump();
    core.load(checkpoint);
    core.load(checkpoint);
    expect(core.getTransaction("pending-owner")?.pendingTable).toEqual(pending);

    core.updateTransaction("pending-owner", 0, {
      status: "aborted",
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    expect(() =>
      core.beginTransaction({
        ...input,
        record: { ...input.record, id: "replacement", ownerId: "owner-replacement" },
      }),
    ).not.toThrow();
  });

  it("reserves exact tombstone wire bytes so manifest pruning remains byte-neutral at cap", () => {
    const manifest: Manifest = {
      version: 0,
      previousVersion: null,
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: [],
      createdAt: "2026-08-24T00:00:00.000Z",
    };
    const pruned = { ...manifest, prunedAt: "2026-08-24T00:01:00.000Z" };
    expect(manifestRecordRetainedReservationBytes(manifest)).toBe(
      manifestRecordRetainedBytes(pruned),
    );
    expect(manifestRecordRetainedReservationBytes(pruned)).toBe(
      manifestRecordRetainedBytes(pruned),
    );

    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    core.load({
      ...core.dump(),
      currentVersion: 1,
      manifests: [
        manifest,
        {
          ...manifest,
          version: 1,
          previousVersion: 0,
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
    });
    const job = core.createGarbageCollectionJob({
      id: "exact-cap-prune",
      candidateManifestVersions: [0],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: "2026-08-24T00:01:00.000Z",
      createdAt: "2026-08-24T00:01:00.000Z",
    });
    const beforeNoncanonical = core.dump();
    expect(() =>
      core.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: 1,
        updatedAt: "2026-08-23T20:01:00.000-04:00",
      }),
    ).toThrow(/canonical UTC ISO-8601/);
    expect(core.dump()).toEqual(beforeNoncanonical);
    expect(() =>
      core.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: 1,
        updatedAt: "+010000-01-01T00:00:00.000Z",
      }),
    ).toThrow(/canonical UTC ISO-8601/);
    expect(core.dump()).toEqual(beforeNoncanonical);
    expect(
      core.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: 1,
        updatedAt: "2026-08-24T00:01:00.000Z",
      }).prunedManifestVersions,
    ).toEqual([0]);
    expect(core.getManifest(0)).toEqual(pruned);
  });

  it("restores large UNIQUE generations and fails closed at accelerator import ceilings", () => {
    const large = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    large.loadSnapshotFrameItems(
      snapshotHeader(),
      uniqueSnapshotItems(65_537, (index) => `token-${String(index).padStart(6, "0")}`, 4_096),
      [],
    );
    expect(large.getExistingUniqueKeys("snapshot-unique-table", ["token-065536"])).toEqual([
      "token-065536",
    ]);

    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const before = core.dump();
    expect(() =>
      core.loadSnapshotFrameItems(
        snapshotHeader(),
        uniqueSnapshotItems(
          MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_ENTRIES + 1,
          (index) => `token-${String(index).padStart(6, "0")}`,
          4_096,
        ),
        [],
      ),
    ).toThrow(
      expect.objectContaining<Partial<StorageResourceLimitError>>({
        resource: "snapshot accelerator entry",
      }),
    );
    expect(core.dump()).toEqual(before);
    expect(() =>
      assertSnapshotImportAcceleratorUsage(MAX_SNAPSHOT_IMPORT_ACCELERATOR_RETAINED_BYTES + 1, 0),
    ).toThrow(
      expect.objectContaining<Partial<StorageResourceLimitError>>({
        resource: "snapshot accelerator byte",
      }),
    );
  });

  it("accounts segment bigint and Unicode bytes exactly like the durable record wire", () => {
    const record: SegmentRecord = {
      id: "segment-€",
      tableId: "table-€",
      transactionId: "transaction-€",
      rowCount: 2,
      rowIdStart: 9_007_199_254_740_993n,
      rowIdEndExclusive: 9_007_199_254_740_995n,
      columnBlockIds: { "column-€": ["block-€"] },
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: "2026-08-24T00:00:00.000Z",
    };
    expect(segmentRecordRetainedBytes(record)).toBe(encodeRecordJson(record).byteLength);
  });

  it("refuses retained manifest and segment cardinality overflow before replacing live state", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const before = core.dump();
    const manifests = Array.from({ length: MAX_MANIFEST_RECORDS + 1 }, (_, version) => ({
      version,
      previousVersion: version === 0 ? null : version - 1,
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: [],
      createdAt: "2026-08-24T00:00:00.000Z",
    }));
    expect(() => core.load({ ...before, currentVersion: MAX_MANIFEST_RECORDS, manifests })).toThrow(
      StorageResourceLimitError,
    );
    expect(core.dump()).toEqual(before);

    const segment: SegmentRecord = {
      id: "over-count",
      tableId: "table",
      transactionId: "transaction",
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: { value: ["block"] },
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: "2026-08-24T00:00:00.000Z",
    };
    expect(() =>
      core.load({
        ...before,
        segments: Array<SegmentRecord>(MAX_SEGMENT_RECORDS + 1).fill(segment),
      }),
    ).toThrow(StorageResourceLimitError);
    expect(core.dump()).toEqual(before);
  });

  it("atomically rejects new zero-block segments and levels outside the v1 range", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    core.addTable({
      id: "segments-table",
      name: "segments_table",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    core.createTransaction(transaction("segments-transaction", null));
    const base: SegmentRecord = {
      id: "invalid-segment",
      tableId: "segments-table",
      transactionId: "segments-transaction",
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: {},
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: "2026-08-24T00:00:00.000Z",
    };
    const before = core.dump();
    expect(() =>
      core.stageTransactionArtifacts({
        transactionId: "segments-transaction",
        expectedRevision: 0,
        blocks: [],
        segments: [base],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    ).toThrow(/at least one .*block/);
    expect(core.dump()).toEqual(before);
    expect(() =>
      core.stageTransactionArtifacts({
        transactionId: "segments-transaction",
        expectedRevision: 0,
        blocks: [],
        segments: [{ ...base, level: 3 }],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    ).toThrow(/level is invalid/);
    expect(core.dump()).toEqual(before);
    expect(() =>
      core.stageTransactionArtifacts({
        transactionId: "segments-transaction",
        expectedRevision: 0,
        blocks: [],
        segments: [
          {
            ...base,
            level: 2,
            columnBlockIds: { value: ["missing-partition-block"] },
          },
        ],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    ).toThrow(/partition ordinal/);
    expect(core.dump()).toEqual(before);
  });

  it("enforces exact aggregate catalog UTF-8 bytes and validates a reloaded state", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    let retainedBytes = 0;
    let ordinal = 0;
    while (retainedBytes < MAX_CATALOG_RETAINED_BYTES) {
      const base: TableRecord = {
        id: `catalog-${String(ordinal)}`,
        name: `catalog_${String(ordinal)}`,
        managed: false,
        revision: 0,
        columns: [{ id: "value", name: "value", type: "string", nullable: true }],
        view: { sql: "", managed: false },
        createdAt: "2026-08-24T00:00:00.000Z",
      };
      const baseBytes = catalogRecordRetainedBytes(base);
      const remaining = MAX_CATALOG_RETAINED_BYTES - retainedBytes;
      const targetBytes = remaining > 3_000_000 ? 3_000_000 : remaining;
      const euroCharacters = Math.floor((targetBytes - baseBytes) / 3);
      const asciiCharacters = targetBytes - baseBytes - euroCharacters * 3;
      const record: TableRecord = {
        ...base,
        view: {
          sql: `${"€".repeat(euroCharacters)}${"x".repeat(asciiCharacters)}`,
          managed: false,
        },
      };
      const bytes = catalogRecordRetainedBytes(record);
      core.addTable(record);
      retainedBytes += bytes;
      ordinal += 1;
    }
    expect(retainedBytes).toBe(MAX_CATALOG_RETAINED_BYTES);
    expect(() =>
      core.addTable({
        id: "catalog-over",
        name: "catalog_over",
        managed: false,
        revision: 0,
        columns: [{ id: "value", name: "value", type: "string", nullable: true }],
        createdAt: "2026-08-24T00:00:00.000Z",
      }),
    ).toThrow(StorageResourceLimitError);
    expect(core.getTable("catalog-over")).toBeUndefined();

    const state = core.dump();
    const recovered = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    recovered.load(state);
    expect(recovered.listTables()).toHaveLength(ordinal);
    const last = state.tables.at(-1);
    if (last?.view === undefined) throw new Error("Expected catalog boundary view");
    last.view.sql += "x";
    expect(() => recovered.load(state)).toThrow(StorageResourceLimitError);
    expect(recovered.listTables()).toHaveLength(ordinal);
  });

  it("admits the exact pinned-retired boundary and atomically refuses its union plus one", () => {
    const blockCount = MAX_PINNED_RETIRED_BLOCKS + 1;
    const core = new RecordCore({
      hasBlock: () => true,
      blockByteLength: () => 1,
      blockChecksum: () => 0,
    });
    core.load({
      ...core.dump(),
      currentVersion: 2,
      manifests: [
        {
          version: 0,
          previousVersion: null,
          liveBlockCount: MAX_PINNED_RETIRED_BLOCKS,
          liveBlockBytes: MAX_PINNED_RETIRED_BLOCKS,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:00.000Z",
        },
        {
          version: 1,
          previousVersion: 0,
          liveBlockCount: 1,
          liveBlockBytes: 1,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:01.000Z",
        },
        {
          version: 2,
          previousVersion: 1,
          liveBlockCount: 0,
          liveBlockBytes: 0,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:02.000Z",
        },
      ],
      manifestBlocks: Array.from({ length: blockCount }, (_, index) => ({
        blockId: `pinned-${String(index).padStart(5, "0")}`,
        byteLength: 1,
        checksum: 0,
        addedVersion: index === MAX_PINNED_RETIRED_BLOCKS ? 1 : 0,
        removedVersion: index === MAX_PINNED_RETIRED_BLOCKS ? 2 : 1,
      })),
    });
    core.createLease({
      id: "boundary-pin",
      kind: "reader",
      manifestVersion: 0,
      ownerId: "boundary-owner",
      createdAt: "2026-08-24T00:00:03.000Z",
      expiresAt: "2026-08-24T01:00:03.000Z",
      revision: 0,
    });
    expect(core.getLease("boundary-pin")?.manifestVersion).toBe(0);
    expect(() =>
      core.createLease({
        id: "over-pin",
        kind: "reader",
        manifestVersion: 1,
        ownerId: "over-owner",
        createdAt: "2026-08-24T00:00:04.000Z",
        expiresAt: "2026-08-24T01:00:04.000Z",
        revision: 0,
      }),
    ).toThrow(StorageResourceLimitError);
    expect(core.getLease("over-pin")).toBeUndefined();
    core.validatePinnedRetiredLimits("2026-08-24T00:00:05.000Z");
  });

  it("enforces the exact pinned-retired byte union independently of block count", () => {
    const chunkBytes = MAX_PINNED_RETIRED_BYTES / 8;
    const core = new RecordCore({
      hasBlock: () => true,
      blockByteLength: (id) => (id === "pinned-byte-extra" ? 1 : chunkBytes),
      blockChecksum: () => 0,
    });
    core.load({
      ...core.dump(),
      currentVersion: 2,
      manifests: [
        {
          version: 0,
          previousVersion: null,
          liveBlockCount: 8,
          liveBlockBytes: MAX_PINNED_RETIRED_BYTES,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:00.000Z",
        },
        {
          version: 1,
          previousVersion: 0,
          liveBlockCount: 1,
          liveBlockBytes: 1,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:01.000Z",
        },
        {
          version: 2,
          previousVersion: 1,
          liveBlockCount: 0,
          liveBlockBytes: 0,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:02.000Z",
        },
      ],
      manifestBlocks: [
        ...Array.from({ length: 8 }, (_, index) => ({
          blockId: `pinned-byte-${String(index)}`,
          byteLength: chunkBytes,
          checksum: 0,
          addedVersion: 0,
          removedVersion: 1,
        })),
        {
          blockId: "pinned-byte-extra",
          byteLength: 1,
          checksum: 0,
          addedVersion: 1,
          removedVersion: 2,
        },
      ],
    });
    core.createLease({
      id: "byte-boundary-pin",
      kind: "reader",
      manifestVersion: 0,
      ownerId: "byte-boundary-owner",
      createdAt: "2026-08-24T00:00:03.000Z",
      expiresAt: "2026-08-24T01:00:03.000Z",
      revision: 0,
    });
    expect(() =>
      core.createLease({
        id: "byte-over-pin",
        kind: "reader",
        manifestVersion: 1,
        ownerId: "byte-over-owner",
        createdAt: "2026-08-24T00:00:04.000Z",
        expiresAt: "2026-08-24T01:00:04.000Z",
        revision: 0,
      }),
    ).toThrow(StorageResourceLimitError);
    expect(core.getLease("byte-over-pin")).toBeUndefined();
  });

  it("stages and promotes a bounded UNIQUE generation atomically", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const namespaceId = secondaryUniqueKeyNamespace("unique-table", "unique-index");
    core.addTable({
      id: "unique-table",
      name: "unique_table",
      managed: false,
      revision: 0,
      columns: [{ id: "value", name: "value", type: "string", nullable: false }],
      secondaryIndexes: {
        "unique-index": {
          name: "unique_index",
          columnId: "value",
          columnIds: ["value"],
          directions: ["asc"],
          unique: true,
          termEncoding: "tuple-v1",
          storage: "postings-v1",
          storageColumnId: "unique-storage",
          locator: "row-id",
          state: "building",
          buildId: "unique-build",
          buildFromVersion: -1,
        },
      },
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    const begun = core.beginUniqueKeyBuild({
      buildId: "unique-build",
      tableId: "unique-table",
      indexId: "unique-index",
      namespaceId,
      ownerId: "unique-owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
    });
    expect(begun).toMatchObject({ nextOrdinal: 0, tokenCount: 0, retainedBytes: 0 });
    const append = {
      buildId: "unique-build",
      ownerId: "unique-owner",
      expiresAtCutoff: "2026-08-24T00:01:00.000Z",
      ordinal: 0,
      keyTokens: ["alpha", "beta"],
      updatedAt: "2026-08-24T00:01:00.000Z",
    } as const;
    const appended = core.appendUniqueKeyBuildChunk(append);
    expect(appended).toMatchObject({ nextOrdinal: 1, tokenCount: 2 });
    expect(core.appendUniqueKeyBuildChunk(append)).toEqual(appended);
    const beforeChangedReplay = core.dump();
    expect(() =>
      core.appendUniqueKeyBuildChunk({ ...append, keyTokens: ["alpha", "changed"] }),
    ).toThrow(/chunk replay changed/);
    expect(core.dump()).toEqual(beforeChangedReplay);
    expect(() =>
      core.appendUniqueKeyBuildChunk({
        ...append,
        ordinal: 1,
        keyTokens: ["beta"],
        updatedAt: "2026-08-24T00:02:00.000Z",
      }),
    ).toThrow(UniqueKeyConflictError);
    expect(core.dump()).toEqual(beforeChangedReplay);

    const finish = {
      buildId: "unique-build",
      ownerId: "unique-owner",
      expiresAtCutoff: "2026-08-24T00:03:00.000Z",
      expectedTableRevision: 0,
      expectedManifestVersion: null,
      chunkCount: 1,
      coversVersion: -1,
      completedAt: "2026-08-24T00:03:00.000Z",
    } as const;
    const ready = core.finishUniqueKeyBuild(finish);
    expect(ready.secondaryIndexes?.["unique-index"]).toMatchObject({
      state: "ready",
      uniqueEnforced: true,
    });
    expect(core.getExistingUniqueKeys(namespaceId, ["missing", "beta", "alpha"])).toEqual([
      "alpha",
      "beta",
    ]);
    expect(core.finishUniqueKeyBuild(finish)).toEqual(ready);

    const reopened = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    reopened.load(core.dump());
    expect(reopened.getUniqueKeyBuild("unique-build")).toMatchObject({
      state: "completed",
      retainedBytes: 0,
    });
    expect(reopened.getExistingUniqueKeys(namespaceId, ["alpha"])).toEqual(["alpha"]);
  });

  it("bounds every opaque storage identity", () => {
    expect(() => validateId("x".repeat(MAX_STORAGE_ID_CHARACTERS))).not.toThrow();
    expect(() => validateId("x".repeat(MAX_STORAGE_ID_CHARACTERS + 1))).toThrow(
      /exceeds 1024 characters/,
    );
  });

  it("checks exact manifest membership without cloning the complete block set", () => {
    const physical = new Set(["manifest-a", "manifest-b", "manifest-c"]);
    const core = new RecordCore({
      hasBlock: (id) => physical.has(id),
      blockByteLength: (id) => (physical.has(id) ? 1 : undefined),
    });
    core.createTransaction(transaction("manifest-owner-0", null));
    core.stageTransactionArtifacts(
      {
        transactionId: "manifest-owner-0",
        expectedRevision: 0,
        blocks: ["manifest-b", "manifest-a"].map((id) => ({ id, bytes: new Uint8Array(0) })),
        segments: [],
        updatedAt: "2026-08-24T00:00:01.000Z",
      },
      { blocksPrevalidated: true },
    );
    core.commitTransaction({
      transactionId: "manifest-owner-0",
      expectedTransactionRevision: 1,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [],
      committedAt: "2026-08-24T00:00:02.000Z",
    });
    core.createTransaction(transaction("manifest-owner-1", 0));
    core.stageTransactionArtifacts(
      {
        transactionId: "manifest-owner-1",
        expectedRevision: 0,
        blocks: [{ id: "manifest-c", bytes: new Uint8Array(0) }],
        segments: [],
        updatedAt: "2026-08-24T00:00:03.000Z",
      },
      { blocksPrevalidated: true },
    );
    core.commitTransaction({
      transactionId: "manifest-owner-1",
      expectedTransactionRevision: 1,
      expectedManifestVersion: 0,
      levelZeroSegmentLimits: [],
      committedAt: "2026-08-24T00:00:04.000Z",
    });

    expect(core.hasManifestBlocks(1, ["manifest-a", "missing", "manifest-c"])).toEqual([
      true,
      false,
      true,
    ]);
    expect(core.hasManifestBlocks(null, ["manifest-a"])).toEqual([false]);
    expect(() =>
      core.hasManifestBlocks(
        1,
        Array.from({ length: MAX_MANIFEST_BLOCK_PRESENCE_IDS + 1 }, () => "manifest-a"),
      ),
    ).toThrow(/at most 1024 ids/);

    const state = core.dump();
    const oldest = state.manifests.find(({ version }) => version === 0);
    if (oldest === undefined) throw new Error("Expected oldest manifest");
    oldest.prunedAt = "2026-08-24T00:00:05.000Z";
    core.load(state);
    expect(core.hasManifestBlocks(0, ["manifest-a"])).toEqual([false]);

    const restored = new RecordCore({
      hasBlock: (id) => physical.has(id),
      blockByteLength: (id) => (physical.has(id) ? 1 : undefined),
      blockChecksum: (id) => (physical.has(id) ? 0 : undefined),
    });
    restored.loadSnapshotFrameItems(
      {
        formatVersion: 1,
        databaseVersion: 7,
        createdAt: "2026-08-24T00:00:06.000Z",
        kinds: Object.fromEntries(
          SNAPSHOT_FRAME_KINDS.map((kind) => [
            kind,
            {
              frameCount: kind === "block" ? 2 : 0,
              itemCount: kind === "block" ? 2 : 0,
              storedBytes: kind === "block" ? 2 : 0,
            },
          ]),
        ) as Record<
          (typeof SNAPSHOT_FRAME_KINDS)[number],
          { frameCount: number; itemCount: number; storedBytes: number }
        >,
      },
      [],
      [
        { blockId: "manifest-a", byteLength: 1, checksum: 0 },
        { blockId: "manifest-b", byteLength: 1, checksum: 0 },
      ],
    );
    expect(restored.listManifestBlockPage({ version: 7, afterBlockId: null, limit: 10 })).toEqual({
      records: [
        { blockId: "manifest-a", byteLength: 1, checksum: 0 },
        { blockId: "manifest-b", byteLength: 1, checksum: 0 },
      ],
      nextCursor: null,
    });
  });

  it("refuses a new manifest before an unrelated active snapshot exceeds the lag ceiling", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const currentVersion = MAX_PINNED_MANIFEST_VERSION_LAG;
    core.load({
      currentVersion,
      catalogEpoch: 0,
      schemaEpoch: 0,
      manifests: [
        {
          version: 0,
          previousVersion: null,
          liveBlockCount: 0,
          liveBlockBytes: 0,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:00.000Z",
        },
        {
          version: currentVersion,
          previousVersion: 0,
          liveBlockCount: 0,
          liveBlockBytes: 0,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      manifestBlocks: [],
      transactions: [
        { ...transaction("old-reader", 0), schemaEpochGuard: 0 },
        { ...transaction("writer", currentVersion), schemaEpochGuard: 0 },
      ],
      tables: [],
      segments: [],
      leases: [],
      compactionJobs: [],
      garbageCollectionJobs: [],
      nextRowIds: [],
      nextAutoIncrement: [],
      ftsBases: [],
      ftsDeltas: [],
      uniqueKeys: [],
      uniqueKeyBuilds: [],
      tempOwners: [],
    });
    const before = core.dump();
    expect(() =>
      core.commitTransaction({
        transactionId: "writer",
        expectedTransactionRevision: 0,
        expectedManifestVersion: currentVersion,
        levelZeroSegmentLimits: [],
        committedAt: "2026-08-24T00:00:02.000Z",
      }),
    ).toThrow(/pinned manifest version lag/);
    expect(core.dump()).toEqual(before);
  });

  it("admits only one nonterminal maintenance job per scope and restores fail closed", () => {
    const physical = new Set(["maintenance-source"]);
    const core = new RecordCore({
      hasBlock: (id) => physical.has(id),
      blockByteLength: (id) => (physical.has(id) ? 1 : undefined),
    });
    core.addTable({
      id: "maintenance-table",
      name: "maintenance_table",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    core.createTransaction(transaction("maintenance-owner", null));
    core.stageTransactionArtifacts(
      {
        transactionId: "maintenance-owner",
        expectedRevision: 0,
        blocks: [{ id: "maintenance-source", bytes: new Uint8Array(0) }],
        segments: [
          {
            id: "maintenance-segment",
            tableId: "maintenance-table",
            transactionId: "maintenance-owner",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: ["maintenance-source"] },
            kind: "insert",
            level: 0,
            logicalOrder: 0,
            commitOrdinal: 0,
            rowIdSpans: [],
            createdAt: "2026-08-24T00:00:01.000Z",
          },
        ],
        updatedAt: "2026-08-24T00:00:01.000Z",
      },
      { blocksPrevalidated: true },
    );
    core.commitTransaction({
      transactionId: "maintenance-owner",
      expectedTransactionRevision: 1,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "maintenance-table", limit: 10 }],
      committedAt: "2026-08-24T00:00:02.000Z",
    });
    const compaction = (id: string): CompactionJobRecord => ({
      id,
      tableId: "maintenance-table",
      sourceManifestVersion: 0,
      sourceSegmentIds: ["maintenance-segment"],
      sourceBlockIds: ["maintenance-source"],
      outputBlockIds: [],
      cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
      processedRows: 0,
      sourceStoredBytes: 0,
      outputStoredBytes: 0,
      logicalBytes: 0,
      rewritePlan: { kind: "copy-v1" },
      outputCursor: null,
      memoryBudgetBytes: 0,
      minimumMemoryBytes: 0,
      level0SourceStoredBytes: 0,
      anchorSourceStoredBytes: 0,
      peakWorkingBytes: 0,
      outputLogicalBytes: 0,
      targetLevel: 1,
      state: "planned",
      transactionId: null,
      outputSegmentId: null,
      publishedVersion: null,
      revision: 0,
      createdAt: "2026-08-24T00:00:03.000Z",
      updatedAt: "2026-08-24T00:00:03.000Z",
    });
    const firstCompaction = compaction("maintenance-compaction-1");
    const secondCompaction = compaction("maintenance-compaction-2");
    core.createCompactionJob(firstCompaction);
    const beforeCompactionConflict = core.dump();
    expect(() => core.createCompactionJob(secondCompaction)).toThrow(CompactionJobConflictError);
    expect(core.dump()).toEqual(beforeCompactionConflict);
    expect(() =>
      core.load({
        ...beforeCompactionConflict,
        compactionJobs: [firstCompaction, secondCompaction],
      }),
    ).toThrow(/Nonterminal compaction job already exists/);
    expect(core.dump()).toEqual(beforeCompactionConflict);

    const collectionInput = (id: string) => ({
      id,
      candidateManifestVersions: [0],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: "2026-08-24T00:10:00.000Z",
      createdAt: "2026-08-24T00:10:00.000Z",
    });
    const firstCollection = core.createGarbageCollectionJob(
      collectionInput("maintenance-collection-1"),
    );
    const beforeCollectionConflict = core.dump();
    expect(() =>
      core.createGarbageCollectionJob(collectionInput("maintenance-collection-2")),
    ).toThrow(GarbageCollectionJobConflictError);
    expect(core.dump()).toEqual(beforeCollectionConflict);
    const secondCollection = {
      ...firstCollection,
      id: "maintenance-collection-2",
    };
    expect(() =>
      core.load({
        ...beforeCollectionConflict,
        garbageCollectionJobs: [firstCollection, secondCollection],
      }),
    ).toThrow(/Garbage collection job already active/);
    expect(core.dump()).toEqual(beforeCollectionConflict);

    core.cancelCompactionJob(
      firstCompaction.id,
      firstCompaction.revision,
      "2026-08-24T00:11:00.000Z",
    );
    expect(() => core.createCompactionJob(secondCompaction)).not.toThrow();
    const completed = core.runGarbageCollectionStep({
      jobId: firstCollection.id,
      expectedRevision: firstCollection.revision,
      maxItems: 1,
      updatedAt: "2026-08-24T00:11:00.000Z",
    });
    expect(completed.job.state).toBe("completed");
    expect(() =>
      core.createGarbageCollectionJob(collectionInput("maintenance-collection-2")),
    ).not.toThrow();
  });

  it("seeks bounded pages and table segments without visiting unrelated records", () => {
    let comparisons = 0;
    const index = new OrderedKeyIndex<string>((left, right) => {
      comparisons += 1;
      return left.localeCompare(right);
    });
    for (let value = 9_999; value >= 0; value -= 1) {
      index.add(`key-${String(value).padStart(5, "0")}`);
    }
    comparisons = 0;
    const page: string[] = [];
    for (const key of index.after("key-04999")) {
      page.push(key);
      if (page.length === 64) break;
    }
    expect(page).toEqual(
      Array.from({ length: 64 }, (_, offset) => `key-${String(5_000 + offset).padStart(5, "0")}`),
    );
    expect(comparisons).toBeLessThan(32);

    const segments = new SegmentRecordMap();
    const makeSegment = (id: string, tableId: string): SegmentRecord => ({
      id,
      tableId,
      transactionId: "owner",
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: {},
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    for (let index = 0; index < 10_000; index += 1) {
      const id = `unrelated-${String(index).padStart(5, "0")}`;
      segments.set(id, makeSegment(id, `table-${String(index % 100)}`));
    }
    segments.set("wanted-b", makeSegment("wanted-b", "wanted"));
    segments.set("wanted-a", makeSegment("wanted-a", "wanted"));
    expect([...segments.tableValues("wanted")].map(({ id }) => id)).toEqual([
      "wanted-a",
      "wanted-b",
    ]);
    expect([...segments.segmentIdsForBlock("missing")]).toEqual([]);
    segments.set("wanted-block", {
      ...makeSegment("wanted-block", "wanted"),
      columnBlockIds: { value: ["needle"] },
    });
    expect([...segments.segmentIdsForBlock("needle")]).toEqual(["wanted-block"]);
  });

  it("checks a bounded collection step without rescanning unrelated readable blocks", () => {
    let existenceChecks = 0;
    const physical = new Set([
      "old-block",
      ...Array.from({ length: 10_000 }, (_, index) => `current-${String(index).padStart(5, "0")}`),
    ]);
    const core = new RecordCore({
      hasBlock: (id) => {
        existenceChecks += 1;
        return physical.has(id);
      },
      blockByteLength: (id) => (physical.has(id) ? 1 : undefined),
    });
    core.load({
      ...core.dump(),
      currentVersion: 1,
      manifests: [
        {
          version: 0,
          previousVersion: null,
          liveBlockCount: 1,
          liveBlockBytes: 1,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:00.000Z",
        },
        {
          version: 1,
          previousVersion: 0,
          liveBlockCount: 10_000,
          liveBlockBytes: 10_000,
          changedTableIds: [],
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      manifestBlocks: [
        {
          blockId: "old-block",
          byteLength: 1,
          checksum: 0,
          addedVersion: 0,
          removedVersion: 1,
        },
        ...Array.from({ length: 10_000 }, (_, index) => ({
          blockId: `current-${String(index).padStart(5, "0")}`,
          byteLength: 1,
          checksum: 0,
          addedVersion: 1,
          removedVersion: null,
        })),
      ],
    });
    existenceChecks = 0;
    const job = core.createGarbageCollectionJob({
      id: "bounded-root-probe",
      candidateManifestVersions: [0],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: "2026-08-24T00:10:00.000Z",
      createdAt: "2026-08-24T00:10:00.000Z",
    });
    const step = core.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: job.revision,
      maxItems: 1,
      updatedAt: "2026-08-24T00:10:01.000Z",
    });
    expect(step.prunedManifestVersions).toEqual([0]);
    expect(existenceChecks).toBeLessThan(10);
  });

  it("rejects committed transactions without a manifest version before any mutation", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const invalid = {
      ...transaction("invalid-commit", null),
      status: "committed" as const,
      committedVersion: null,
    };
    const before = structuredClone(core.dump());
    expect(() => core.createTransaction(invalid)).toThrow(/committed status and version disagree/);
    expect(core.dump()).toEqual(before);
    expect(() => core.load({ ...before, transactions: [invalid] })).toThrow(
      /committed status and version disagree/,
    );
    expect(core.dump()).toEqual(before);
  });

  it("refuses journal growth at the durable ceiling without mutating the transaction", () => {
    const blockIds = Array.from(
      { length: MAX_TRANSACTION_PENDING_BLOCKS },
      (_, index) => `pending-${String(index)}`,
    );
    const physical = new Set(blockIds);
    const core = new RecordCore({
      hasBlock: (id) => physical.has(id),
      blockByteLength: (id) => (physical.has(id) ? 1 : undefined),
    });
    core.createTransaction(transaction("bounded-journal", null));
    const current = core.updateTransaction("bounded-journal", 0, {
      pendingBlockIds: blockIds,
      updatedAt: "2026-08-24T00:00:00.000Z",
    });
    expect(() =>
      core.stageTransactionArtifacts({
        transactionId: current.id,
        expectedRevision: current.revision,
        blocks: [{ id: "one-too-many", bytes: Uint8Array.of(1) }],
        segments: [],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    ).toThrow(/journal exceeds 4096 pending blocks/);
    expect(core.getTransaction(current.id)).toEqual(current);
    expect(core.getSegment("one-too-many")).toBeUndefined();
  });

  it("removes pruned manifest descriptors in bounded convergent pages", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    for (let version = 0; version < 4; version += 1) {
      const id = `transaction-${String(version)}`;
      core.createTransaction(transaction(id, version === 0 ? null : version - 1));
      core.commitTransaction({
        transactionId: id,
        expectedTransactionRevision: 0,
        expectedManifestVersion: version === 0 ? null : version - 1,
        committedAt: `2026-08-24T00:00:0${String(version)}.000Z`,
      });
    }
    const job = core.createGarbageCollectionJob({
      id: "manifest-pruning",
      candidateManifestVersions: [0, 1, 2],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: "2026-08-24T00:10:00.000Z",
      createdAt: "2026-08-24T00:10:00.000Z",
    });
    const step = core.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: job.revision,
      maxItems: 10,
      updatedAt: "2026-08-24T00:10:01.000Z",
    });
    expect(step.prunedManifestVersions).toEqual([0, 1, 2]);
    expect(core.removePrunedManifestRecords(1)).toBe(1);
    expect(core.dump().manifests.map(({ version }) => version)).toEqual([1, 2, 3]);
    expect(core.removePrunedManifestRecords(1)).toBe(1);
    expect(core.removePrunedManifestRecords(1)).toBe(1);
    expect(core.removePrunedManifestRecords(1)).toBe(0);
    expect(core.dump().manifests.map(({ version }) => version)).toEqual([3]);
  });

  it("advances pruned-manifest cleanup without rescanning an ineligible prefix", () => {
    const retained = new Set(
      Array.from({ length: 2_048 }, (_, version) => version)
        .filter((version) => version % 2 === 0)
        .map((version) => `tomb-${String(version)}`),
    );
    let existenceChecks = 0;
    const core = new RecordCore({
      hasBlock: (id) => {
        existenceChecks += 1;
        return retained.has(id);
      },
      blockByteLength: (id) => (retained.has(id) ? 1 : undefined),
    });
    core.load({
      ...core.dump(),
      currentVersion: 2_048,
      manifests: Array.from({ length: 2_049 }, (_, version) => ({
        version,
        previousVersion: version === 0 ? null : version - 1,
        liveBlockCount: version === 2_048 ? 0 : 1,
        liveBlockBytes: version === 2_048 ? 0 : 1,
        changedTableIds: [],
        createdAt: "2026-08-24T00:00:00.000Z",
        ...(version === 2_048 ? {} : { prunedAt: "2026-08-24T00:01:00.000Z" }),
      })),
      manifestBlocks: Array.from({ length: 2_048 }, (_, version) => ({
        blockId: `tomb-${String(version)}`,
        byteLength: 1,
        checksum: 0,
        addedVersion: version,
        removedVersion: version + 1,
      })),
    });
    existenceChecks = 0;
    expect(core.removePrunedManifestRecords(64)).toBe(64);
    expect(existenceChecks).toBe(0);
    existenceChecks = 0;
    expect(core.removePrunedManifestRecords(64)).toBe(64);
    expect(existenceChecks).toBe(0);
  });

  it("keeps bounded garbage-collection discovery parameters fixed and updates atomically", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const initial = core.createGarbageCollectionJob({
      id: "bounded-discovery",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: "2026-08-24T00:10:00.000Z",
      createdAt: "2026-08-24T00:10:00.000Z",
      discovery: discovery(),
    });
    expect(() =>
      core.updateGarbageCollectionPlanning({
        jobId: initial.id,
        expectedRevision: initial.revision,
        discovery: discovery({ maxPlanningItems: 65 }),
        updatedAt: "2026-08-24T00:10:01.000Z",
      }),
    ).toThrow(/immutable/);
    expect(core.getGarbageCollectionJob(initial.id)).toEqual(initial);

    const completed = core.updateGarbageCollectionPlanning({
      jobId: initial.id,
      expectedRevision: initial.revision,
      discovery: discovery({ phase: "complete", visitedRecords: 4 }),
      updatedAt: "2026-08-24T00:10:01.000Z",
    });
    expect(completed).toMatchObject({ revision: 1, state: "completed" });
    expect(core.getGarbageCollectionJob(initial.id)).toEqual(completed);
  });

  it("retains a terminal compaction record until GC takes over its sole provenance", () => {
    const physical = new Map([["orphan-block", 4]]);
    const core = new RecordCore({
      hasBlock: (id) => physical.has(id),
      blockByteLength: (id) => physical.get(id),
    });
    core.addTable({
      id: "orphan-table",
      name: "orphan_table",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    const owner = {
      ...transaction("orphan-owner", null),
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "aborted" as const,
    };
    core.load({
      ...core.dump(),
      transactions: [owner],
      segments: [
        {
          id: "orphan-segment",
          tableId: "orphan-table",
          transactionId: owner.id,
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { value: ["orphan-block"] },
          kind: "base",
          level: 1,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt: "2026-08-24T00:00:00.000Z",
        },
      ],
      compactionJobs: [
        {
          id: "orphan-compaction",
          tableId: "orphan-table",
          sourceManifestVersion: 0,
          sourceSegmentIds: ["orphan-segment"],
          sourceBlockIds: ["orphan-block"],
          outputBlockIds: [],
          cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
          processedRows: 1,
          sourceStoredBytes: 4,
          outputStoredBytes: 0,
          logicalBytes: 4,
          rewritePlan: { kind: "copy-v1" },
          outputCursor: null,
          memoryBudgetBytes: 0,
          minimumMemoryBytes: 0,
          level0SourceStoredBytes: 4,
          anchorSourceStoredBytes: 0,
          peakWorkingBytes: 0,
          outputLogicalBytes: 4,
          targetLevel: 1,
          state: "cancelled",
          transactionId: null,
          outputSegmentId: null,
          publishedVersion: null,
          revision: 0,
          createdAt: "2026-08-24T00:00:00.000Z",
          updatedAt: "2026-08-24T00:00:01.000Z",
        },
      ],
    });

    const before = core.getCompactionJob("orphan-compaction");
    expect(core.removeCompactionJob("orphan-compaction")).toBe(false);
    expect(core.getCompactionJob("orphan-compaction")).toEqual(before);

    const job = core.createGarbageCollectionJob({
      id: "orphan-collection",
      candidateManifestVersions: [],
      candidateSegmentIds: ["orphan-segment"],
      candidateBlockIds: ["orphan-block"],
      candidateTransactionIds: [],
      leaseCutoff: "2026-08-24T00:10:00.000Z",
      createdAt: "2026-08-24T00:10:00.000Z",
    });
    const result = core.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: job.revision,
      maxItems: 2,
      updatedAt: "2026-08-24T00:10:01.000Z",
    });
    expect(result.reclaimedSegmentIds).toEqual(["orphan-segment"]);
    expect(result.reclaimedBlockIds).toEqual(["orphan-block"]);
    physical.delete("orphan-block");
    expect(core.removeCompactionJob("orphan-compaction")).toBe(true);
    expect(core.getCompactionJob("orphan-compaction")).toBeUndefined();
  });

  it("atomically adopts an unpublished aborted output into its active replacement", () => {
    const physical = new Set(["source-block", "replacement-block"]);
    const core = new RecordCore({
      hasBlock: (id) => physical.has(id),
      blockByteLength: (id) => (physical.has(id) ? 1 : undefined),
    });
    core.addTable({
      id: "replacement-table",
      name: "replacement_table",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    core.createTransaction(transaction("bootstrap", null));
    core.stageTransactionArtifacts(
      {
        transactionId: "bootstrap",
        expectedRevision: 0,
        blocks: [{ id: "source-block", bytes: new Uint8Array(0) }],
        segments: [
          {
            id: "source-segment",
            tableId: "replacement-table",
            transactionId: "bootstrap",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: ["source-block"] },
            kind: "insert",
            level: 0,
            logicalOrder: 0,
            commitOrdinal: 0,
            rowIdSpans: [],
            createdAt: "2026-08-24T00:00:01.000Z",
          },
        ],
        updatedAt: "2026-08-24T00:00:01.000Z",
      },
      { blocksPrevalidated: true },
    );
    core.commitTransaction({
      transactionId: "bootstrap",
      expectedTransactionRevision: 1,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "replacement-table", limit: 10 }],
      committedAt: "2026-08-24T00:00:02.000Z",
    });
    core.createTransaction(transaction("aborted-output", 0));
    const segment: SegmentRecord = {
      id: "replacement-segment",
      tableId: "replacement-table",
      transactionId: "aborted-output",
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: { value: ["replacement-block"] },
      kind: "base",
      level: 1,
      logicalOrder: 1,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: "2026-08-24T00:00:02.000Z",
    };
    core.stageTransactionArtifacts(
      {
        transactionId: "aborted-output",
        expectedRevision: 0,
        blocks: [{ id: "replacement-block", bytes: new Uint8Array(0) }],
        segments: [segment],
        updatedAt: "2026-08-24T00:00:02.000Z",
      },
      { blocksPrevalidated: true },
    );
    core.updateTransaction("aborted-output", 1, {
      status: "aborted",
      updatedAt: "2026-08-24T00:00:03.000Z",
    });
    core.createTransaction(transaction("replacement-owner", 0));
    core.stageTransactionArtifacts(
      {
        transactionId: "replacement-owner",
        expectedRevision: 0,
        blocks: [{ id: "replacement-block", bytes: new Uint8Array(0) }],
        segments: [],
        updatedAt: "2026-08-24T00:00:04.000Z",
      },
      { blocksPrevalidated: true },
    );
    core.createCompactionJob({
      id: "replacement-job",
      tableId: "replacement-table",
      sourceManifestVersion: 0,
      sourceSegmentIds: ["source-segment"],
      sourceBlockIds: ["source-block"],
      outputBlockIds: ["replacement-block"],
      cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
      processedRows: 1,
      sourceStoredBytes: 1,
      outputStoredBytes: 1,
      logicalBytes: 1,
      rewritePlan: { kind: "copy-v1" },
      outputCursor: null,
      memoryBudgetBytes: 0,
      minimumMemoryBytes: 0,
      level0SourceStoredBytes: 1,
      anchorSourceStoredBytes: 0,
      peakWorkingBytes: 0,
      outputLogicalBytes: 1,
      targetLevel: 1,
      state: "running",
      transactionId: "replacement-owner",
      outputSegmentId: "replacement-segment",
      publishedVersion: null,
      revision: 0,
      createdAt: "2026-08-24T00:00:04.000Z",
      updatedAt: "2026-08-24T00:00:04.000Z",
    });

    // This is the intended crash state: the replacement journals the output bytes while the
    // immutable segment still names its aborted producer. It must remain reopenable.
    const reopened = new RecordCore({
      hasBlock: (id) => physical.has(id),
      blockByteLength: (id) => (physical.has(id) ? 1 : undefined),
    });
    reopened.load(core.dump());
    const before = reopened.dump();
    const desired = { ...segment, transactionId: "replacement-owner" };
    const adoption = {
      segment: desired,
      expectedAbortedTransactionId: "aborted-output",
      expectedAbortedTransactionRevision: 2,
      replacementTransactionId: "replacement-owner",
      expectedReplacementTransactionRevision: 1,
      compactionJobId: "foreign-job",
      updatedAt: "2026-08-24T00:00:05.000Z",
    };
    expect(() => reopened.adoptAbortedSegment(adoption)).toThrow(/does not authorize/);
    expect(reopened.dump()).toEqual(before);

    const replacement = reopened.adoptAbortedSegment({
      ...adoption,
      compactionJobId: "replacement-job",
    });
    expect(replacement).toMatchObject({ revision: 2, pendingSegmentIds: [segment.id] });
    expect(reopened.getSegment(segment.id)?.transactionId).toBe("replacement-owner");
    expect(reopened.getTransaction("aborted-output")).toMatchObject({
      revision: 3,
      pendingSegmentIds: [],
    });
  });

  it("fails closed across the complete runtime segment envelope validation matrix", () => {
    const valid: SegmentRecord = {
      id: "validated-segment",
      tableId: "validated-table",
      transactionId: "validated-transaction",
      rowCount: 2,
      rowIdStart: 10n,
      rowIdEndExclusive: 22n,
      columnBlockIds: { value: ["block-a", "block-b"] },
      kind: "insert",
      level: 1,
      logicalOrder: 1,
      commitOrdinal: 0,
      rowIdSpans: [
        { rowStart: 0, rowCount: 1, rowIdStart: 10n },
        { rowStart: 1, rowCount: 1, rowIdStart: 21n },
      ],
      createdAt: "2026-08-25T00:00:00.000Z",
    };
    expect(validateSegmentRuntimeRecord(valid, "fixture")).toEqual(valid);

    const cases: Array<(record: Record<string, unknown>) => void> = [
      (record) => {
        record.extra = true;
      },
      (record) => {
        record.rowCount = 0;
      },
      (record) => {
        record.rowIdStart = -1n;
      },
      (record) => {
        record.rowIdEndExclusive = 9n;
      },
      (record) => {
        record.columnBlockIds = [];
      },
      (record) => {
        record.columnBlockIds = { value: [] };
      },
      (record) => {
        record.columnBlockIds = { value: ["block-a", "block-a"] };
      },
      (record) => {
        record.kind = "replace";
      },
      (record) => {
        record.level = 3;
      },
      (record) => {
        record.commitOrdinal = -1;
      },
      (record) => {
        record.partitionOrdinal = -1;
      },
      (record) => {
        record.level = 2;
      },
      (record) => {
        record.partitionOrdinal = 0;
      },
      (record) => {
        record.logicalOrder = Number.NaN;
      },
      (record) => {
        record.rowIdSpans = {};
      },
      (record) => {
        record.rowIdSpans = [null];
      },
      (record) => {
        record.rowIdSpans = [{ rowStart: 1, rowCount: 1, rowIdStart: 10n }];
      },
      (record) => {
        record.rowIdSpans = [{ rowStart: 0, rowCount: 0, rowIdStart: 10n }];
      },
      (record) => {
        record.rowIdSpans = [
          { rowStart: 0, rowCount: 1, rowIdStart: 10n },
          { rowStart: 1, rowCount: 1, rowIdStart: 10n },
        ];
      },
      (record) => {
        record.rowIdSpans = [{ rowStart: 0, rowCount: 2, rowIdStart: 10n }];
      },
      (record) => {
        record.createdAt = "not-canonical";
      },
    ];
    for (const mutate of cases) {
      const candidate = structuredClone(valid) as unknown as Record<string, unknown>;
      mutate(candidate);
      expect(() => validateSegmentRuntimeRecord(candidate, "fixture")).toThrow();
    }

    const keyed = {
      ...valid,
      kind: "delete" as const,
      keyColumnId: "value",
      rowIdStart: 0n,
      rowIdEndExclusive: 0n,
      rowIdSpans: [],
    };
    expect(validateSegmentRuntimeRecord(keyed, "keyed")).toEqual(keyed);
    expect(() =>
      validateSegmentRuntimeRecord(
        { ...keyed, rowIdSpans: [{ rowStart: 0, rowCount: 2, rowIdStart: 1n }] },
        "keyed",
      ),
    ).toThrow(/spans/);
  });

  it("bounds full-text postings and base metadata at every durable validation boundary", () => {
    const chunks = [
      [
        { term: "alpha", rowIds: [1n, 3n], tf: [1, 2] },
        { term: "beta", rowIds: [4n], tf: [1] },
      ],
    ];
    expect(validateFtsPostingChunks(chunks, "fixture")).toBe(4);
    expect(() => validateFtsPostingChunks({}, "fixture")).toThrow(/array/);
    for (const candidate of [
      [[]],
      [[null]],
      [[{ term: "", rowIds: [1n], tf: [1] }]],
      [[{ term: "alpha", rowIds: [], tf: [] }]],
      [[{ term: "alpha", rowIds: [1n], tf: [] }]],
      [[{ term: "alpha", rowIds: [0n], tf: [1] }]],
      [[{ term: "alpha", rowIds: [1n, 1n], tf: [1, 1] }]],
      [[{ term: "alpha", rowIds: [1n], tf: [0] }]],
      [[{ term: "alpha", rowIds: [1n], tf: [1], extra: true }]],
      [
        [
          { term: "beta", rowIds: [1n], tf: [1] },
          { term: "alpha", rowIds: [2n], tf: [1] },
        ],
      ],
    ]) {
      expect(() => validateFtsPostingChunks(candidate, "fixture")).toThrow();
    }
    expect(() =>
      validateFtsPostingChunks([[{ term: "x".repeat(65_537), rowIds: [1n], tf: [1] }]], "fixture"),
    ).toThrow(/character limit/);
    expect(() =>
      validateFtsBaseInput({ coversVersion: -2, chunks, totalTokens: 4 }, "base"),
    ).toThrow(/metadata/);
    expect(() =>
      validateFtsBaseInput({ coversVersion: 0, chunks, totalTokens: 3 }, "base"),
    ).toThrow(/token total/);
    expect(() =>
      validateFtsBaseInput(
        {
          coversVersion: 0,
          chunks: Array.from({ length: 4097 }, () => chunks[0] ?? []),
          totalTokens: 0,
        },
        "base",
      ),
    ).toThrow(/chunk-count/);
  });

  it("rejects non-owned buffers, invalid reservations, and storage-owned fresh-transaction state", () => {
    expect(() => validateBlockWriteBytes([])).toThrow(/Uint8Array/);
    validateBlockWriteBytes(new Uint8Array(1));
    if (typeof SharedArrayBuffer !== "undefined") {
      expect(() => validateBlockWriteBytes(new Uint8Array(new SharedArrayBuffer(1)))).toThrow(
        /SharedArrayBuffer/,
      );
    }
    for (const [count, atLeast] of [
      [-1, undefined],
      [0.5, undefined],
      [0, 0n],
      [0, (1n << 64n) + 1n],
    ] as const) {
      expect(() => validateAutoIncrementReservation(count, atLeast)).toThrow(RangeError);
    }
    validateAutoIncrementReservation(0, undefined);
    validateAutoIncrementReservation(1, 1n);
    expect(() => validateTempRunPageIdentity("owner", "run", -1)).toThrow(RangeError);
    validateTempRunPageIdentity("owner", "run", 0);
    expect(() =>
      validateTempRunPage({ ownerId: "owner", runId: "run", pageIndex: 0, bytes: [] as never }),
    ).toThrow(/Uint8Array/);
    validateTempRunPage({ ownerId: "owner", runId: "run", pageIndex: 0, bytes: new Uint8Array(1) });

    const fresh = transaction("fresh", null);
    validateBeginTransactionInput({ record: fresh });
    for (const record of [
      { ...fresh, pendingBlockIds: ["block"] },
      { ...fresh, pendingSegmentIds: ["segment"] },
      { ...fresh, schemaEpochGuard: 0 },
      { ...fresh, catalogEpochGuard: 0 },
    ]) {
      expect(() => validateBeginTransactionInput({ record })).toThrow();
    }
  });

  it("maintains ordered indexes and segment reverse indexes through delete merges and clear", () => {
    const index = new OrderedKeyIndex<number>((left, right) => left - right);
    for (let value = 0; value < 260; value += 1) index.add(value);
    index.add(129);
    expect([...index.after(127)]).toEqual(Array.from({ length: 132 }, (_, offset) => offset + 128));
    for (let value = 128; value < 260; value += 1) index.delete(value);
    index.delete(999);
    expect([...index.after(null)]).toEqual(Array.from({ length: 128 }, (_, value) => value));
    index.clear();
    expect(index.empty).toBe(true);
    expect([...index.after(null)]).toEqual([]);

    const segments = new SegmentRecordMap();
    const record: SegmentRecord = {
      id: "reverse-segment",
      tableId: "reverse-table",
      transactionId: "reverse-owner",
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: { value: ["reverse-block", "reverse-block"] },
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: "2026-08-25T00:00:00.000Z",
    };
    segments.set(record.id, record);
    expect(segments.blockReferenceCount("reverse-block")).toBe(1);
    expect(segments.tableBlockReferenceCount(record.tableId, "reverse-block")).toBe(1);
    expect(segments.ownerReferenceCount(record.transactionId)).toBe(1);
    expect([...segments.segmentIdsForBlock("reverse-block")]).toEqual([record.id]);
    segments.delete(record.id);
    segments.delete(record.id);
    expect(segments.blockReferenceCount("reverse-block")).toBe(0);
    segments.set(record.id, record);
    segments.clear();
    expect(segments.size).toBe(0);
  });

  it("rebuilds bulk unique-key order lazily and stays exact across later mutations", () => {
    const tokens = new OrderedStringSet();
    tokens.addMany(["z", "a", "m", "a"]);
    expect(tokens.size).toBe(3);
    expect(tokens.has("m")).toBe(true);
    expect([...tokens.orderedValues()]).toEqual(["a", "m", "z"]);
    // A repeated ordered read reuses the current index and remains deterministic.
    expect([...tokens.orderedValues()]).toEqual(["a", "m", "z"]);

    tokens.delete("m");
    tokens.add("b");
    expect([...tokens.orderedValues()]).toEqual(["a", "b", "z"]);
    tokens.addMany(["y", "c", "b"]);
    tokens.delete("a");
    expect(tokens.has("a")).toBe(false);
    expect([...tokens.orderedValues()]).toEqual(["b", "c", "y", "z"]);
    tokens.addMany(["dirty-after-rebuild"]);
    tokens.clear();
    expect(tokens.size).toBe(0);
    expect([...tokens.orderedValues()]).toEqual([]);
    tokens.add("after-clear");
    expect([...tokens.orderedValues()]).toEqual(["after-clear"]);
  });

  it("exports bulk unique-key commits in canonical order after an index rebuild", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const table: TableRecord = {
      id: "lazy-unique-table",
      name: "lazy_unique_table",
      columns: [{ id: "key", name: "key", type: "string", nullable: false }],
      uniqueKeyColumnId: "key",
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    };
    core.addTable(table);
    core.createTransaction(transaction("lazy-unique-owner-0", null));
    core.commitTransaction({
      transactionId: "lazy-unique-owner-0",
      expectedTransactionRevision: 0,
      expectedManifestVersion: null,
      changedTableIds: [table.id],
      uniqueKeyChanges: [
        {
          tableId: table.id,
          keyTokens: ["string:z", "string:a", "string:m"],
          requireAbsent: true,
        },
      ],
      levelZeroSegmentLimits: [],
      committedAt: "2026-08-24T00:00:01.000Z",
    });
    const exportedTokens = () =>
      [...core.snapshotFrameMetadataItems()].flatMap((item) =>
        item.kind === "unique-chunk" ? item.keyTokens : [],
      );
    expect(exportedTokens()).toEqual(["string:a", "string:m", "string:z"]);
    expect(exportedTokens()).toEqual(["string:a", "string:m", "string:z"]);

    // Membership is eager even while ordered iteration is dirty: a duplicate commit must fail
    // before the snapshot-only index is ever rebuilt.
    core.createTransaction(transaction("lazy-unique-conflict", 0));
    expect(() =>
      core.commitTransaction({
        transactionId: "lazy-unique-conflict",
        expectedTransactionRevision: 0,
        expectedManifestVersion: 0,
        changedTableIds: [table.id],
        uniqueKeyChanges: [{ tableId: table.id, keyTokens: ["string:m"], requireAbsent: true }],
        levelZeroSegmentLimits: [],
        committedAt: "2026-08-24T00:00:02.000Z",
      }),
    ).toThrow(UniqueKeyConflictError);

    core.createTransaction(transaction("lazy-unique-owner-1", 0));
    core.commitTransaction({
      transactionId: "lazy-unique-owner-1",
      expectedTransactionRevision: 0,
      expectedManifestVersion: 0,
      changedTableIds: [table.id],
      uniqueKeyChanges: [
        { tableId: table.id, keyTokens: ["string:m"], requireAbsent: false, remove: true },
        { tableId: table.id, keyTokens: ["string:b"], requireAbsent: true },
      ],
      levelZeroSegmentLimits: [],
      committedAt: "2026-08-24T00:00:03.000Z",
    });
    expect(exportedTokens()).toEqual(["string:a", "string:b", "string:z"]);

    const snapshotItems = [...core.snapshotFrameMetadataItems()];
    const restored = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    restored.loadSnapshotFrameItems(snapshotHeader(1), snapshotItems, []);
    expect(
      restored.getExistingUniqueKeys(table.id, ["string:z", "string:m", "string:b", "string:a"]),
    ).toEqual(["string:a", "string:b", "string:z"]);
    expect(
      [...restored.snapshotFrameMetadataItems()].flatMap((item) =>
        item.kind === "unique-chunk" ? item.keyTokens : [],
      ),
    ).toEqual(["string:a", "string:b", "string:z"]);
  });

  it("keeps temp-owner and lease lifecycle cleanup bounded and conflict-safe", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    const owner = {
      ownerId: "temp-owner",
      createdAt: "2026-08-25T00:00:00.000Z",
      expiresAt: "2026-08-25T00:30:00.000Z",
      revision: 0,
    };
    core.createTempOwner(owner);
    expect(core.getTempOwner(owner.ownerId)).toEqual(owner);
    expect(() => core.createTempOwner(owner)).toThrow(/already exists/);
    expect(() =>
      core.renewTempOwner({
        ownerId: owner.ownerId,
        expectedRevision: 1,
        expiresAtCutoff: "2026-08-25T00:10:00.000Z",
        expiresAt: "2026-08-25T00:40:00.000Z",
      }),
    ).toThrow();
    expect(
      core.renewTempOwner({
        ownerId: owner.ownerId,
        expectedRevision: 0,
        expiresAtCutoff: "2026-08-25T00:10:00.000Z",
        expiresAt: "2026-08-25T00:40:00.000Z",
      }),
    ).toMatchObject({ revision: 1 });
    expect(() => core.listExpiredTempOwnerPage("2026-08-25T00:45:00.000Z", "bad", 1)).toThrow(
      /cursor/,
    );
    expect(core.removeTempOwnerIfExpired(owner.ownerId, "2026-08-25T00:20:00.000Z")).toBe(false);
    expect(core.removeTempOwnerIfExpired(owner.ownerId, "2026-08-25T00:45:00.000Z")).toBe(true);
    expect(core.removeTempOwnerIfExpired("missing-owner", "2026-08-25T00:45:00.000Z")).toBe(true);
    core.removeTempOwner("missing-owner");
    core.createTempOwner({ ...owner, ownerId: "b-owner" });
    expect(core.listTempOwnerIdsPage(null, 2, ["a-orphan", "c-orphan", "a-orphan"])).toEqual({
      records: ["a-orphan", "b-owner"],
      nextCursor: "b-owner",
    });

    const lease = {
      id: "lease",
      ownerId: "lease-owner",
      kind: "reader" as const,
      manifestVersion: null,
      createdAt: "2026-08-25T00:00:00.000Z",
      expiresAt: "2026-08-25T00:30:00.000Z",
      revision: 0,
    };
    core.createLease(lease);
    expect(core.getLease(lease.id)).toEqual(lease);
    expect(core.listLeases()).toEqual([lease]);
    expect(() => core.createLease(lease)).toThrow(/already exists/);
    expect(() =>
      core.renewLease({
        id: lease.id,
        expectedRevision: 1,
        expiresAtCutoff: "2026-08-25T00:10:00.000Z",
        expiresAt: "2026-08-25T00:40:00.000Z",
      }),
    ).toThrow();
    const renewed = core.renewLease({
      id: lease.id,
      expectedRevision: 0,
      expiresAtCutoff: "2026-08-25T00:10:00.000Z",
      expiresAt: "2026-08-25T00:40:00.000Z",
    });
    expect(renewed.revision).toBe(1);
    expect(
      core.moveLease({
        id: lease.id,
        expectedRevision: 1,
        manifestVersion: null,
        expiresAtCutoff: "2026-08-25T00:20:00.000Z",
        expiresAt: "2026-08-25T00:50:00.000Z",
      }),
    ).toMatchObject({ revision: 2 });
    expect(core.removeLeaseIfExpired(lease.id, 2, "2026-08-25T00:30:00.000Z")).toBe(false);
    expect(() => core.removeLease({ id: lease.id, ownerId: "other" })).toThrow();
    expect(core.removeLease({ id: lease.id, ownerId: lease.ownerId })).toBe(true);
    expect(core.removeLease({ id: lease.id, ownerId: lease.ownerId })).toBe(false);
  });
});
