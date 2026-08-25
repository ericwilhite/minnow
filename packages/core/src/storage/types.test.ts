import { describe, expect, it } from "vitest";
import {
  advanceGarbageCollectionJobRecord,
  assertSnapshotImportAcceleratorUsage,
  assertTransactionArtifactBatchLimits,
  assertTransactionArtifactJournalLimits,
  catalogRecordRetainedBytes,
  createGarbageCollectionJobRecord,
  invalidateUncoveredFtsColumns,
  invalidateUncoveredSecondaryIndexes,
  assertTempRunPageBatchLimits,
  assertCompactionOutputProvenance,
  MAX_MAINTENANCE_BATCH_ITEMS,
  MAX_FTS_QUERY_TERMS,
  MAX_CATALOG_NAME_CHARACTERS,
  MAX_ENUM_VALUES,
  MAX_MANIFEST_CHANGED_TABLE_IDS,
  MAX_SECONDARY_INDEXES,
  MAX_TABLE_CONSTRAINTS,
  MAX_TABLE_COLUMNS,
  MAX_TABLE_TRIGGERS,
  MAX_TRIGGER_STATEMENTS,
  MAX_TABLE_RECORD_ENTRIES,
  MAX_TABLE_RECORD_CHARACTERS,
  MAX_TEMP_RUN_PAGE_BYTES,
  MAX_TEMP_RUN_PAGES_PER_BATCH,
  MAX_TRANSACTION_PENDING_SEGMENTS,
  normalizeGarbageCollectionJobRecord,
  StorageFormatVersionError,
  updateGarbageCollectionPlanningRecord,
  validateCanonicalManifestChangedTableIds,
  validateCatalogName,
  validateColumnDefault,
  validateEnumValues,
  validateFtsOrderedReadLimits,
  validateFtsPostingQueries,
  validateSecondaryIndexes,
  validateSqlDomain,
  validateTableColumns,
  validateTableRecordBounds,
  type TableColumnRecord,
  type TableRecord,
  type CompactionJobRecord,
  type SegmentRecord,
} from "./types.js";

describe("storage format version errors", () => {
  it("carries clone-safe compatibility and recovery policy fields", () => {
    const error = new StorageFormatVersionError("opfs", "format.json", 6, 5, "newer");
    const fields = {
      backend: "opfs",
      location: "format.json",
      actualVersion: 6,
      supportedVersion: 5,
      relation: "newer",
      name: "StorageFormatVersionError",
    };
    expect(
      structuredClone({
        backend: error.backend,
        location: error.location,
        actualVersion: error.actualVersion,
        supportedVersion: error.supportedVersion,
        relation: error.relation,
        name: error.name,
      }),
    ).toEqual(fields);
    expect(error.message).toContain("version 6, which is newer than the supported version 5");
  });

  it("describes an unavailable actual version as unknown", () => {
    const error = new StorageFormatVersionError("indexeddb", "database", null, 1, "newer");
    expect(error.message).toContain("an unknown version");
  });
});

function indexedTable(): TableRecord {
  return {
    id: "table-t",
    name: "t",
    columns: [{ id: "col-v", name: "v", type: "string", nullable: false }],
    ftsColumns: {
      "col-v": {
        storage: "fts-chunks-v1",
        tokenizerVersion: 1,
        state: "ready",
        buildFromVersion: 0,
      },
    },
    secondaryIndexes: {
      index: {
        name: "t_v_idx",
        columnId: "col-v",
        columnIds: ["col-v"],
        directions: ["asc"],
        termEncoding: "tuple-v1",
        storage: "postings-v1",
        storageColumnId: "secondary-index:index",
        locator: "row-id",
        state: "ready",
        buildFromVersion: 0,
      },
    },
    managed: false,
    revision: Number.MAX_SAFE_INTEGER,
    createdAt: "2026-01-01T00:00:00.000Z",
  };
}

describe("bounded catalog validation", () => {
  it("rejects cyclic and aliased structured records immediately", () => {
    const cyclic = indexedTable() as TableRecord & { cycle?: unknown };
    cyclic.cycle = cyclic;
    expect(() => validateTableRecordBounds(cyclic)).toThrow("cycles or aliases");

    const shared: unknown[] = [];
    const aliased = indexedTable() as TableRecord & { first?: unknown; second?: unknown };
    aliased.first = shared;
    aliased.second = shared;
    expect(() => validateTableRecordBounds(aliased)).toThrow("cycles or aliases");
  });

  it("rejects huge short-entry graphs independently of the text bound", () => {
    const record = indexedTable() as TableRecord & { padding?: unknown[] };
    record.padding = new Array(MAX_TABLE_RECORD_ENTRIES + 1).fill(null);
    expect(() => validateTableRecordBounds(record)).toThrow(
      `${String(MAX_TABLE_RECORD_ENTRIES)} aggregate entries`,
    );
  });

  it("rejects duplicate names across durable constraint kinds", () => {
    const record = indexedTable();
    const index = record.secondaryIndexes?.index;
    if (index === undefined) expect.unreachable("index fixture missing");
    record.secondaryIndexes = { index: { ...index, unique: true } };
    record.checks = [{ name: "t_v_idx", sql: "v <> ''" }];

    expect(() => validateTableRecordBounds(record)).toThrow("Constraint already exists: t_v_idx");
  });
});

describe("storage metadata admission validation", () => {
  const column = (): TableColumnRecord => ({
    id: "value",
    name: "value",
    type: "string",
    nullable: false,
  });

  it("rejects non-canonical manifest changes and malformed column domains before persistence", () => {
    expect(() =>
      validateCanonicalManifestChangedTableIds(
        Array.from(
          { length: MAX_MANIFEST_CHANGED_TABLE_IDS + 1 },
          (_, index) => `t-${String(index)}`,
        ),
      ),
    ).toThrow("cannot exceed");
    expect(() => validateCanonicalManifestChangedTableIds(["b", "a"])).toThrow(
      "unique and lexically sorted",
    );
    expect(() => validateCanonicalManifestChangedTableIds(["a", "a"])).toThrow(
      "unique and lexically sorted",
    );
    expect(() => validateTableColumns([])).toThrow("at least one column");
    expect(() =>
      validateTableColumns(
        Array.from({ length: MAX_TABLE_COLUMNS + 1 }, (_, index) => ({
          ...column(),
          id: `c-${String(index)}`,
          name: `c_${String(index)}`,
        })),
      ),
    ).toThrow("cannot exceed");
    expect(() =>
      validateTableColumns([{ ...column(), type: "number", sqlDomain: { kind: "uuid" } }]),
    ).toThrow("must use string storage");
    expect(() =>
      validateTableColumns([
        {
          ...column(),
          sqlDomain: { kind: "enum", name: "status", values: ["open"] },
          enumValues: ["open"],
        },
      ]),
    ).toThrow("both enum restrictions and a SQL domain");
    expect(() =>
      validateTableColumns([{ ...column(), type: "number", enumValues: ["one"] }]),
    ).toThrow("require a string column");
    expect(() => validateTableColumns([{ ...column(), backfill: 1 }])).toThrow(
      "Invalid backfill value",
    );
    expect(() => validateTableColumns([column(), column()])).toThrow("unique IDs and names");
  });

  it("rejects unsafe defaults and unbounded or structurally invalid postings queries", () => {
    expect(() =>
      validateColumnDefault(
        {
          name: "count",
          type: "number",
          integer: true,
          nullable: false,
          isUniqueKey: false,
        },
        { kind: "literal", value: 1.5 },
      ),
    ).toThrow("safe integer");
    expect(() =>
      validateColumnDefault(
        {
          name: "status",
          type: "string",
          nullable: false,
          isUniqueKey: false,
          enumValues: ["open"],
        },
        { kind: "literal", value: "closed" },
      ),
    ).toThrow("one of the enum values");
    expect(() => validateFtsPostingQueries(null)).toThrow("must be an array");
    expect(() => validateFtsPostingQueries(new Array(MAX_FTS_QUERY_TERMS + 1).fill({}))).toThrow(
      "cannot exceed",
    );
    expect(() => validateFtsPostingQueries(["term"])).toThrow("must be an object");
    expect(() => validateFtsPostingQueries([{ term: "", prefix: false }])).toThrow(
      "exact/prefix query is invalid",
    );
    expect(() => validateFtsPostingQueries([{ lower: 1 }])).toThrow("range query is invalid");
    expect(() => validateFtsOrderedReadLimits(0)).toThrow("row limit");
    expect(() => validateFtsOrderedReadLimits(1, 0)).toThrow("byte limit");
  });

  it("rejects malformed retained values and unsafe accounting before adapter mutation", () => {
    expect(() => validateSqlDomain({ kind: "enum", name: "", values: ["open"] }, "status")).toThrow(
      "trimmed non-empty name",
    );
    expect(() =>
      validateEnumValues(new Array<string>(MAX_ENUM_VALUES + 1).fill("value"), "status"),
    ).toThrow("cannot exceed");
    expect(() =>
      validateColumnDefault(
        {
          name: "status",
          type: "string",
          nullable: false,
          isUniqueKey: false,
          sqlDomain: { kind: "enum", name: "status", values: ["open"] },
        },
        { kind: "literal", value: "closed" },
      ),
    ).toThrow("one of the enum values");
    expect(() =>
      catalogRecordRetainedBytes(
        Object.assign(indexedTable(), { invalidBigint: -1n }) as TableRecord,
      ),
    ).toThrow("unsigned 64-bit");
    expect(() =>
      validateTableRecordBounds(
        Object.assign(indexedTable(), { invalidDate: new Date(Number.NaN) }) as TableRecord,
      ),
    ).toThrow("record date is invalid");
    expect(() =>
      validateTableRecordBounds(
        Object.assign(indexedTable(), {
          oversizedText: "x".repeat(MAX_TABLE_RECORD_CHARACTERS + 1),
        }) as TableRecord,
      ),
    ).toThrow("modeled characters");
    expect(() =>
      assertTransactionArtifactBatchLimits(
        [{ id: "block", bytes: [] as unknown as Uint8Array }],
        [],
      ),
    ).toThrow("must be a Uint8Array");
    expect(() =>
      assertTransactionArtifactJournalLimits(
        [],
        new Array<string>(MAX_TRANSACTION_PENDING_SEGMENTS + 1).fill("segment"),
      ),
    ).toThrow("pending segments");
    expect(() => assertSnapshotImportAcceleratorUsage(-1, 0)).toThrow("bytes are invalid");
    expect(() => assertSnapshotImportAcceleratorUsage(0, -1)).toThrow("entries are invalid");
    expect(() => validateCatalogName(null)).toThrow("cannot be empty");
    expect(() => validateCatalogName("x".repeat(MAX_CATALOG_NAME_CHARACTERS + 1))).toThrow(
      "exceeds",
    );
  });

  it("rejects malformed trigger ownership and catalog cardinality at the durable boundary", () => {
    const trigger = {
      id: "trigger-id",
      name: "trigger_name",
      timing: "before" as const,
      event: "insert" as const,
      statements: [{ sql: "SELECT 1", bindings: [] }],
      createdAt: "2026-08-24T12:00:00.000Z",
    };
    const malformed = (mutate: (record: TableRecord) => void): TableRecord => {
      const record = indexedTable();
      record.triggers = [structuredClone(trigger)];
      mutate(record);
      return record;
    };
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          Object.assign(record, { managed: undefined });
        }),
      ),
    ).toThrow("managed flag");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          record.revision = -1;
        }),
      ),
    ).toThrow("revision");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          record.primaryKeyColumnIds = Array.from({ length: MAX_TABLE_COLUMNS + 1 }, () => "col-v");
        }),
      ),
    ).toThrow("primary key names too many");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          record.secondaryIndexes = Object.fromEntries(
            Array.from({ length: MAX_SECONDARY_INDEXES + 1 }, (_, index) => [
              `i-${String(index)}`,
              record.secondaryIndexes?.index,
            ]),
          ) as NonNullable<TableRecord["secondaryIndexes"]>;
        }),
      ),
    ).toThrow("cannot exceed");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          record.triggers = Array.from({ length: MAX_TABLE_TRIGGERS + 1 }, () =>
            structuredClone(trigger),
          );
        }),
      ),
    ).toThrow("cannot exceed");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0] !== undefined) {
            Object.assign(record.triggers[0], { event: "truncate" });
          }
        }),
      ),
    ).toThrow("event is invalid");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0] !== undefined) {
            Object.assign(record.triggers[0], { timing: "during" });
          }
        }),
      ),
    ).toThrow("timing is invalid");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          record.triggers = [trigger, { ...trigger, name: "other_name" }];
        }),
      ),
    ).toThrow("Trigger ID already exists");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          record.triggers = [trigger, { ...trigger, id: "other-id" }];
        }),
      ),
    ).toThrow("Trigger already exists");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0] !== undefined) record.triggers[0].createdAt = "invalid";
        }),
      ),
    ).toThrow("timestamp is invalid");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0] !== undefined) record.triggers[0].statements = [];
        }),
      ),
    ).toThrow("needs at least one statement");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0] !== undefined) {
            record.triggers[0].statements = Array.from(
              { length: MAX_TRIGGER_STATEMENTS + 1 },
              () => ({ sql: "SELECT 1", bindings: [] }),
            );
          }
        }),
      ),
    ).toThrow("cannot exceed");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0]?.statements[0] !== undefined) {
            record.triggers[0].statements[0].sql = "";
          }
        }),
      ),
    ).toThrow("statement SQL is invalid");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0]?.statements[0] !== undefined) {
            Object.assign(record.triggers[0].statements[0], { bindings: null });
          }
        }),
      ),
    ).toThrow("bindings are invalid");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0]?.statements[0] !== undefined) {
            record.triggers[0].statements[0].bindings = [{ source: "new", column: "missing" }];
          }
        }),
      ),
    ).toThrow("unknown column");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          record.ftsColumns = Object.fromEntries(
            Array.from({ length: MAX_TABLE_COLUMNS + 1 }, (_, index) => [
              `c-${String(index)}`,
              {
                storage: "fts-chunks-v1",
                tokenizerVersion: 1,
                state: "ready",
                buildFromVersion: 0,
              },
            ]),
          );
        }),
      ),
    ).toThrow("too many full-text columns");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0] !== undefined) {
            Object.assign(record.triggers[0], { createdAt: 1 });
          }
        }),
      ),
    ).toThrow("creation timestamp is invalid");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0]?.statements[0] !== undefined) {
            Object.assign(record.triggers[0].statements[0], {
              bindings: [{ source: "future", column: "v" }],
            });
          }
        }),
      ),
    ).toThrow("binding source is invalid");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          if (record.triggers?.[0]?.statements[0] !== undefined) {
            record.triggers[0].statements[0].bindings = [{ source: "old", column: "v" }];
          }
        }),
      ),
    ).toThrow("source is unavailable");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          record.checks = Array.from({ length: MAX_TABLE_CONSTRAINTS + 1 }, (_, index) => ({
            name: `check_${String(index)}`,
            sql: "v IS NOT NULL",
          }));
        }),
      ),
    ).toThrow("named constraints");
    expect(() =>
      validateTableRecordBounds(
        malformed((record) => {
          record.enumType = {
            name: "status",
            values: Array.from({ length: MAX_ENUM_VALUES + 1 }, (_, index) => String(index)),
          };
        }),
      ),
    ).toThrow("too many values");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          const index = record.secondaryIndexes?.index;
          if (index === undefined) expect.unreachable("index fixture missing");
          record.secondaryIndexes = { "": index };
        }),
      ),
    ).toThrow("must be non-empty");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          const index = record.secondaryIndexes?.index;
          if (index === undefined) expect.unreachable("index fixture missing");
          record.secondaryIndexes = {
            index: { ...index, columnId: "missing", columnIds: ["missing"] },
          };
        }),
      ),
    ).toThrow("unknown column");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          const index = record.secondaryIndexes?.index;
          if (index === undefined) expect.unreachable("index fixture missing");
          record.secondaryIndexes = { index: { ...index, uniqueEnforced: true } };
        }),
      ),
    ).toThrow("without UNIQUE");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          const index = record.secondaryIndexes?.index;
          if (index === undefined) expect.unreachable("index fixture missing");
          record.secondaryIndexes = {
            first: structuredClone(index),
            second: { ...structuredClone(index), storageColumnId: "other-storage" },
          };
        }),
      ),
    ).toThrow("Index already exists");
    expect(() =>
      validateSecondaryIndexes(
        malformed((record) => {
          const index = record.secondaryIndexes?.index;
          if (index === undefined) expect.unreachable("index fixture missing");
          record.secondaryIndexes = {
            first: structuredClone(index),
            second: { ...structuredClone(index), name: "other_index" },
          };
        }),
      ),
    ).toThrow("storage ID is already used");
  });
});

describe("compaction output provenance", () => {
  it("proves every partition field and exact block/journal order from the immutable merge plan", () => {
    const table: TableRecord = {
      id: "table-merge",
      name: "merge_table",
      columns: [{ id: "key", name: "key", type: "number", nullable: false }],
      uniqueKeyColumnId: "key",
      managed: false,
      revision: 0,
      createdAt: "2026-01-01T00:00:00.000Z",
    };
    const source: SegmentRecord = {
      id: "merge-source",
      tableId: table.id,
      transactionId: "source-owner",
      rowCount: 4,
      rowIdStart: 1n,
      rowIdEndExclusive: 12n,
      columnBlockIds: { key: ["source-block"] },
      kind: "base",
      keyColumnId: "key",
      level: 1,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [
        { rowStart: 0, rowCount: 2, rowIdStart: 1n },
        { rowStart: 2, rowCount: 2, rowIdStart: 10n },
      ],
      createdAt: "2026-01-01T00:00:00.000Z",
    };
    const jobId = "merge-proof";
    const block0 = `${jobId}/rewrite/window/00000000/column/00000000`;
    const block1 = `${jobId}/rewrite/window/00000001/column/00000000`;
    const job: CompactionJobRecord = {
      id: jobId,
      tableId: table.id,
      sourceManifestVersion: 0,
      sourceSegmentIds: [source.id],
      sourceBlockIds: ["source-block"],
      outputBlockIds: [block0, block1],
      cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
      processedRows: 4,
      sourceStoredBytes: 1,
      outputStoredBytes: 2,
      logicalBytes: 4,
      rewritePlan: {
        kind: "merge-v1",
        targetBlockBytes: 1024,
        outputCompression: "raw",
        keyColumnId: "key",
        totalRows: 4,
        rowIdStart: 1n,
        rowIdEndExclusive: 12n,
        rowIdSpans: source.rowIdSpans,
        logicalOrder: 5,
        sourceSegments: [],
        columns: [{ columnId: "key", type: "number", sourceRanges: [] }],
        outputs: [
          { rowStart: 0, rowCount: 2 },
          { rowStart: 2, rowCount: 2 },
        ],
        partitions: [
          { rowStart: 0, rowCount: 2, logicalOrder: 5 },
          { rowStart: 2, rowCount: 2, logicalOrder: 6 },
        ],
      },
      outputCursor: { outputIndex: 2, columnIndex: 0, rowStart: 4 },
      memoryBudgetBytes: 1024,
      minimumMemoryBytes: 1,
      level0SourceStoredBytes: 1,
      anchorSourceStoredBytes: 0,
      outputPartitionOrdinal: 7,
      maxWriteAmplification: 4,
      maximumOutputStoredBytes: 4,
      plannedOutputStoredBytesUpperBound: 4,
      peakWorkingBytes: 1,
      outputLogicalBytes: 4,
      targetLevel: 2,
      state: "ready",
      transactionId: "merge-output-owner",
      outputSegmentId: "merge-output",
      publishedVersion: null,
      revision: 0,
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:01.000Z",
    };
    const outputs: SegmentRecord[] = [
      {
        id: "merge-output",
        tableId: table.id,
        transactionId: "merge-output-owner",
        rowCount: 2,
        rowIdStart: 1n,
        rowIdEndExclusive: 3n,
        columnBlockIds: { key: [block0] },
        kind: "base",
        keyColumnId: "key",
        level: 2,
        logicalOrder: 5,
        commitOrdinal: 0,
        rowIdSpans: [{ rowStart: 0, rowCount: 2, rowIdStart: 1n }],
        partitionOrdinal: 7,
        createdAt: "2026-01-01T00:00:01.000Z",
      },
      {
        id: "merge-output/1",
        tableId: table.id,
        transactionId: "merge-output-owner",
        rowCount: 2,
        rowIdStart: 10n,
        rowIdEndExclusive: 12n,
        columnBlockIds: { key: [block1] },
        kind: "base",
        keyColumnId: "key",
        level: 2,
        logicalOrder: 6,
        commitOrdinal: 1,
        rowIdSpans: [{ rowStart: 0, rowCount: 2, rowIdStart: 10n }],
        partitionOrdinal: 7,
        createdAt: "2026-01-01T00:00:01.000Z",
      },
    ];
    const transaction = {
      id: "merge-output-owner",
      pendingBlockIds: [block0, block1],
      pendingSegmentIds: outputs.map(({ id }) => id),
    };
    expect(() =>
      assertCompactionOutputProvenance(job, table, transaction, [source], outputs),
    ).not.toThrow();
    const [firstOutput, secondOutput] = outputs;
    if (firstOutput === undefined || secondOutput === undefined) {
      throw new Error("Expected two partition outputs");
    }

    const corruptions: Array<[string, typeof transaction, SegmentRecord[]]> = [
      ["block order", { ...transaction, pendingBlockIds: [block1, block0] }, outputs],
      [
        "segment order",
        { ...transaction, pendingSegmentIds: [...transaction.pendingSegmentIds].reverse() },
        [...outputs].reverse(),
      ],
      ["kind", transaction, [{ ...firstOutput, kind: "insert", rowIdSpans: [] }, secondOutput]],
      ["key", transaction, [{ ...firstOutput, keyColumnId: "other" }, secondOutput]],
      ["level", transaction, [{ ...firstOutput, level: 1 }, secondOutput]],
      ["logical order", transaction, [firstOutput, { ...secondOutput, logicalOrder: 7 }]],
      ["partition", transaction, [firstOutput, { ...secondOutput, partitionOrdinal: 8 }]],
      [
        "row spans",
        transaction,
        [
          firstOutput,
          { ...secondOutput, rowIdSpans: [{ rowStart: 0, rowCount: 2, rowIdStart: 9n }] },
        ],
      ],
      [
        "column blocks",
        transaction,
        [firstOutput, { ...secondOutput, columnBlockIds: { key: [block0] } }],
      ],
    ];
    for (const [label, candidateTransaction, candidateOutputs] of corruptions) {
      expect(
        () =>
          assertCompactionOutputProvenance(
            job,
            table,
            candidateTransaction,
            [source],
            candidateOutputs,
          ),
        label,
      ).toThrow();
    }
  });
});

describe("storage revision bounds", () => {
  it("refuses an FTS invalidation revision overflow without mutating its input", () => {
    const record = indexedTable();
    const before = structuredClone(record);
    expect(() => invalidateUncoveredFtsColumns(record, new Set())).toThrow(RangeError);
    expect(record).toEqual(before);
  });

  it("refuses a secondary-index invalidation revision overflow without mutating its input", () => {
    const record = indexedTable();
    const before = structuredClone(record);
    expect(() => invalidateUncoveredSecondaryIndexes(record, new Set())).toThrow(RangeError);
    expect(record).toEqual(before);
  });
});

describe("bounded temp spill writes", () => {
  it("refuses too many pages before adapter work", () => {
    expect(() =>
      assertTempRunPageBatchLimits(
        Array.from({ length: MAX_TEMP_RUN_PAGES_PER_BATCH + 1 }, (_, pageIndex) => ({
          ownerId: "owner",
          runId: "run",
          pageIndex,
          bytes: new Uint8Array(),
        })),
      ),
    ).toThrow(`exceeds ${String(MAX_TEMP_RUN_PAGES_PER_BATCH)} pages`);
  });

  it("refuses a single oversized page", () => {
    const bytes = new Uint8Array(MAX_TEMP_RUN_PAGE_BYTES + 1);
    expect(() =>
      assertTempRunPageBatchLimits([{ ownerId: "owner", runId: "run", pageIndex: 0, bytes }]),
    ).toThrow(`exceeds ${String(MAX_TEMP_RUN_PAGE_BYTES)} bytes`);
  });
});

describe("bounded garbage-collection discovery", () => {
  const discovery = {
    phase: "manifests" as const,
    currentManifestVersion: 10,
    retainAboveVersion: 8,
    retainAfter: 0,
    maxPlanningItems: 2,
    manifestCursor: null,
    segmentCursor: null,
    transactionCursor: null,
    compactionCursor: null,
    visitedRecords: 0,
  };

  it("persists an empty plan as discovering and refuses reclamation before completion", () => {
    const job = createGarbageCollectionJobRecord({
      id: "gc",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: "2026-01-01T00:00:00.000Z",
      createdAt: "2026-01-01T00:00:00.000Z",
      discovery,
    });
    expect(job).toMatchObject({ state: "planned", discovery });
    expect(() =>
      advanceGarbageCollectionJobRecord(job, {
        examinedManifestCount: 0,
        prunedManifestCount: 0,
        alreadyPrunedManifestCount: 0,
        retainedManifestCount: 0,
        missingManifestCount: 0,
        examinedSegmentCount: 0,
        reclaimedSegmentCount: 0,
        retainedSegmentCount: 0,
        missingSegmentCount: 0,
        examinedBlockCount: 0,
        reclaimedBlockCount: 0,
        retainedBlockCount: 0,
        missingBlockCount: 0,
        reclaimedBlockBytes: 0,
        examinedTransactionCount: 0,
        reclaimedTransactionCount: 0,
        retainedTransactionCount: 0,
        missingTransactionCount: 0,
        updatedAt: "2026-01-01T00:00:01.000Z",
      }),
    ).toThrow("discovery must complete");
  });

  it("CAS-appends bounded candidates and completes an empty discovery durably", () => {
    const initial = createGarbageCollectionJobRecord({
      id: "gc",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      leaseCutoff: "2026-01-01T00:00:00.000Z",
      createdAt: "2026-01-01T00:00:00.000Z",
      discovery,
    });
    const appended = updateGarbageCollectionPlanningRecord(initial, {
      jobId: "gc",
      expectedRevision: 0,
      candidateBlockIds: ["block-a"],
      discovery: { ...discovery, phase: "segments", manifestCursor: 4, visitedRecords: 64 },
      updatedAt: "2026-01-01T00:00:01.000Z",
    });
    expect(normalizeGarbageCollectionJobRecord(appended)).toMatchObject({
      revision: 1,
      candidateBlockIds: ["block-a"],
      discovery: { phase: "segments", manifestCursor: 4, visitedRecords: 64 },
    });
    expect(() =>
      updateGarbageCollectionPlanningRecord(appended, {
        jobId: "gc",
        expectedRevision: 1,
        candidateBlockIds: ["block-b", "block-c"],
        discovery: { ...discovery, phase: "complete", visitedRecords: 65 },
        updatedAt: "2026-01-01T00:00:02.000Z",
      }),
    ).toThrow("exceed");
    const completed = updateGarbageCollectionPlanningRecord(initial, {
      jobId: "gc",
      expectedRevision: 0,
      discovery: { ...discovery, phase: "complete", visitedRecords: 1 },
      updatedAt: "2026-01-01T00:00:01.000Z",
    });
    expect(completed.state).toBe("completed");
  });

  it("caps durable planning arrays at the absolute maintenance batch limit", () => {
    expect(() =>
      createGarbageCollectionJobRecord({
        id: "oversized-plan",
        candidateManifestVersions: [],
        candidateSegmentIds: [],
        candidateBlockIds: [],
        leaseCutoff: "2026-01-01T00:00:00.000Z",
        createdAt: "2026-01-01T00:00:00.000Z",
        discovery: {
          ...discovery,
          maxPlanningItems: MAX_MAINTENANCE_BATCH_ITEMS + 1,
        },
      }),
    ).toThrow(`cannot exceed ${String(MAX_MAINTENANCE_BATCH_ITEMS)}`);
  });
});
