import { describe, expect, it } from "vitest";
import type { Manifest, SegmentRecord, TableRecord, TransactionRecord } from "../types.js";
import { RecordCore, type RecordCoreState } from "./record-core.js";

const timestamp = "2026-08-25T00:00:00.000Z";
const later = "2026-08-25T00:30:00.000Z";

function table(overrides: Partial<TableRecord> = {}): TableRecord {
  return {
    id: "table",
    name: "items",
    columns: [{ id: "value", name: "value", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: timestamp,
    ...overrides,
  };
}

function transaction(overrides: Partial<TransactionRecord> = {}): TransactionRecord {
  return {
    id: "transaction",
    ownerId: "owner",
    expiresAt: later,
    snapshotVersion: null,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: timestamp,
    updatedAt: timestamp,
    committedVersion: null,
    schemaEpochGuard: 0,
    ...overrides,
  };
}

function manifest(overrides: Partial<Manifest> = {}): Manifest {
  return {
    version: 0,
    previousVersion: null,
    liveBlockCount: 0,
    liveBlockBytes: 0,
    changedTableIds: [],
    createdAt: timestamp,
    ...overrides,
  };
}

function emptyState(): RecordCoreState {
  return new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined }).dump();
}

function first<Value>(values: readonly Value[]): Value {
  const value = values[0];
  if (value === undefined) throw new Error("Missing recovery fixture record");
  return value;
}

function expectAtomicRefusal(
  candidates: readonly RecordCoreState[],
  blocks: ReadonlyMap<string, number> = new Map(),
): void {
  const live = new RecordCore({
    hasBlock: (id) => blocks.has(id),
    blockByteLength: (id) => blocks.get(id),
  });
  live.addTable(table({ id: "sentinel", name: "sentinel" }));
  const before = live.dump();
  for (const [index, candidate] of candidates.entries()) {
    expect(() => live.load(candidate), `candidate ${String(index)}`).toThrow();
    expect(live.dump()).toEqual(before);
  }
}

function mutated(
  state: RecordCoreState,
  change: (candidate: RecordCoreState & Record<string, unknown>) => void,
): RecordCoreState {
  const candidate = structuredClone(state) as RecordCoreState & Record<string, unknown>;
  change(candidate);
  return candidate;
}

describe("RecordCore checkpoint recovery refusal", () => {
  it("rejects missing record families, invalid epochs, and non-array roots atomically", () => {
    const base = emptyState();
    const candidates = [
      mutated(base, (state) => {
        state.manifests = null as never;
      }),
      mutated(base, (state) => {
        state.transactions = {} as never;
      }),
      mutated(base, (state) => {
        state.tempOwners = "owners" as never;
      }),
      mutated(base, (state) => {
        state.currentVersion = -1;
      }),
      mutated(base, (state) => {
        state.catalogEpoch = -1;
      }),
      mutated(base, (state) => {
        state.schemaEpoch = 1;
      }),
    ];
    expectAtomicRefusal(candidates);
    expect(() =>
      new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined }).load(
        null as never,
      ),
    ).toThrow(/must be an object/);
  });

  it("rejects malformed, duplicate, discontinuous, summarized, and pruned manifests atomically", () => {
    const base = { ...emptyState(), currentVersion: 0, manifests: [manifest()] };
    const candidates = [
      mutated(base, (state) => {
        first(state.manifests).version = -1;
      }),
      mutated(base, (state) => {
        state.manifests.push(structuredClone(first(state.manifests)));
      }),
      mutated(base, (state) => {
        first(state.manifests).createdAt = "not-canonical";
      }),
      mutated(base, (state) => {
        first(state.manifests).liveBlockCount = -1;
      }),
      mutated(base, (state) => {
        first(state.manifests).changedTableIds = ["table", "table"];
      }),
      mutated(base, (state) => {
        first(state.manifests).previousVersion = 0;
      }),
      mutated(base, (state) => {
        state.manifests.push(manifest({ version: 2, previousVersion: 1 }));
        state.currentVersion = 2;
      }),
      mutated(base, (state) => {
        state.currentVersion = 1;
      }),
      mutated(base, (state) => {
        first(state.manifests).prunedAt = later;
      }),
    ];
    expectAtomicRefusal(candidates);
  });

  it("rejects corrupt manifest-block provenance before exposing readable bytes", () => {
    const blocks = new Map([["block", 3]]);
    const base: RecordCoreState = {
      ...emptyState(),
      currentVersion: 0,
      manifests: [manifest({ liveBlockCount: 1, liveBlockBytes: 3 })],
      manifestBlocks: [
        {
          blockId: "block",
          byteLength: 3,
          checksum: 1,
          addedVersion: 0,
          removedVersion: null,
        },
      ],
    };
    const candidates = [
      mutated(base, (state) => {
        state.manifestBlocks.push(structuredClone(first(state.manifestBlocks)));
      }),
      mutated(base, (state) => {
        Object.assign(first(state.manifestBlocks), { blockId: "" });
      }),
      mutated(base, (state) => {
        Object.assign(first(state.manifestBlocks), { byteLength: 0 });
      }),
      mutated(base, (state) => {
        Object.assign(first(state.manifestBlocks), { checksum: 0x1_0000_0000 });
      }),
      mutated(base, (state) => {
        Object.assign(first(state.manifestBlocks), { removedVersion: 0 });
      }),
      mutated(base, (state) => {
        first(state.manifests).liveBlockBytes = 2;
      }),
    ];
    expectAtomicRefusal(candidates, blocks);
    expectAtomicRefusal([base]);
    expectAtomicRefusal(
      [{ ...emptyState(), manifestBlocks: structuredClone(base.manifestBlocks) }],
      blocks,
    );
  });

  it("rejects duplicate catalog identities and invalid table/view/foreign-key state", () => {
    const base = { ...emptyState(), catalogEpoch: 1, schemaEpoch: 1, tables: [table()] };
    const candidates = [
      mutated(base, (state) => {
        state.tables.push(table({ name: "other" }));
      }),
      mutated(base, (state) => {
        state.tables.push(table({ id: "other" }));
      }),
      mutated(base, (state) => {
        first(state.tables).id = "";
      }),
      mutated(base, (state) => {
        first(state.tables).name = "";
      }),
      mutated(base, (state) => {
        first(state.tables).revision = -1;
      }),
      mutated(base, (state) => {
        first(state.tables).createdAt = "yesterday";
      }),
      mutated(base, (state) => {
        first(state.tables).view = {} as never;
      }),
      mutated(base, (state) => {
        first(state.tables).view = { sql: "", managed: false };
      }),
      mutated(base, (state) => {
        first(state.tables).foreignKeys = [
          {
            name: "missing_parent",
            columns: ["value"],
            parentTable: "parents",
            parentColumns: ["id"],
            onDelete: "restrict",
          },
        ];
      }),
    ];
    expectAtomicRefusal(candidates);
  });

  it("rejects corrupt active and terminal transaction lifecycle records", () => {
    const base = { ...emptyState(), transactions: [transaction()] };
    const candidates = [
      mutated(base, (state) => {
        state.transactions.push(structuredClone(first(state.transactions)));
      }),
      mutated(base, (state) => {
        first(state.transactions).snapshotVersion = -1;
      }),
      mutated(base, (state) => {
        first(state.transactions).pendingBlockIds = ["a", "a"];
      }),
      mutated(base, (state) => {
        first(state.transactions).pendingSegmentIds = null as never;
      }),
      mutated(base, (state) => {
        first(state.transactions).status = "unknown" as never;
      }),
      mutated(base, (state) => {
        first(state.transactions).revision = -1;
      }),
      mutated(base, (state) => {
        first(state.transactions).startedAt = "not-canonical";
      }),
      mutated(base, (state) => {
        first(state.transactions).ownerId = "";
      }),
      mutated(base, (state) => {
        first(state.transactions).expiresAt = timestamp;
      }),
      mutated(base, (state) => {
        first(state.transactions).committedVersion = 0;
      }),
      mutated(base, (state) => {
        delete first(state.transactions).schemaEpochGuard;
      }),
      mutated(base, (state) => {
        state.transactions[0] = transaction({
          status: "aborted",
          schemaEpochGuard: 0,
        });
      }),
      mutated(base, (state) => {
        state.transactions[0] = transaction({
          status: "committed",
          committedVersion: null,
        });
        delete first(state.transactions).schemaEpochGuard;
      }),
    ];
    expectAtomicRefusal(candidates);
  });

  it("rejects corrupt pending-table ownership and catalog reservations", () => {
    const pending = transaction({
      pendingTable: table({ id: "pending", name: "pending" }),
      pendingTableNextRowId: 1n,
      catalogEpochGuard: 0,
    });
    const base = { ...emptyState(), transactions: [pending] };
    const candidates = [
      mutated(base, (state) => {
        first(state.transactions).pendingTableNextRowId = 0n;
      }),
      mutated(base, (state) => {
        first(state.transactions).catalogEpochGuard = -1;
      }),
      mutated(base, (state) => {
        delete first(state.transactions).pendingTable;
      }),
      mutated(base, (state) => {
        first(state.transactions).status = "aborted";
        delete first(state.transactions).schemaEpochGuard;
      }),
      mutated(base, (state) => {
        state.tables = [table({ id: "pending", name: "published" })];
        state.catalogEpoch = 1;
        state.schemaEpoch = 1;
      }),
    ];
    expectAtomicRefusal(candidates);
  });

  it("rejects segment ownership, journal, table, block, and ordinal corruption", () => {
    const blocks = new Map([["segment-block", 1]]);
    const physical = {
      hasBlock: (id: string) => blocks.has(id),
      blockByteLength: (id: string) => blocks.get(id),
    };
    const fixture = new RecordCore(physical);
    fixture.addTable(table());
    fixture.createTransaction(transaction({ schemaEpochGuard: 1 }));
    const segment: SegmentRecord = {
      id: "segment",
      tableId: "table",
      transactionId: "transaction",
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: { value: ["segment-block"] },
      kind: "insert",
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: timestamp,
    };
    fixture.stageTransactionArtifacts(
      {
        transactionId: "transaction",
        expectedRevision: 0,
        blocks: [{ id: "segment-block", bytes: new Uint8Array() }],
        segments: [segment],
        updatedAt: timestamp,
      },
      { blocksPrevalidated: true },
    );
    const base = fixture.dump();
    const candidates = [
      mutated(base, (state) => {
        state.segments.push(structuredClone(first(state.segments)));
      }),
      mutated(base, (state) => {
        first(state.segments).transactionId = "missing-owner";
      }),
      mutated(base, (state) => {
        first(state.segments).tableId = "missing-table";
      }),
      mutated(base, (state) => {
        first(state.segments).columnBlockIds = { value: ["missing-block"] };
      }),
      mutated(base, (state) => {
        first(state.transactions).pendingSegmentIds = [];
      }),
      mutated(base, (state) => {
        first(state.segments).commitOrdinal = 1;
      }),
      mutated(base, (state) => {
        first(state.transactions).pendingSegmentIds = ["missing-segment"];
      }),
    ];
    expectAtomicRefusal(candidates, blocks);
  });

  it("rejects invalid leases, temp owners, and orphaned persisted counters", () => {
    const core = new RecordCore({ hasBlock: () => false, blockByteLength: () => undefined });
    core.addTable(
      table({
        columns: [
          { id: "value", name: "value", type: "number", nullable: false },
          {
            id: "sequence",
            name: "sequence",
            type: "number",
            nullable: false,
            defaultValue: { kind: "autoincrement" },
          },
        ],
      }),
    );
    core.createLease({
      id: "lease",
      ownerId: "lease-owner",
      kind: "reader",
      manifestVersion: null,
      createdAt: timestamp,
      expiresAt: later,
      revision: 0,
    });
    core.createTempOwner({ ownerId: "temp", createdAt: timestamp, expiresAt: later, revision: 0 });
    const base = core.dump();
    const candidates = [
      mutated(base, (state) => {
        state.leases.push(structuredClone(first(state.leases)));
      }),
      mutated(base, (state) => {
        first(state.leases).kind = "writer" as never;
      }),
      mutated(base, (state) => {
        first(state.leases).manifestVersion = -1;
      }),
      mutated(base, (state) => {
        first(state.leases).expiresAt = timestamp;
      }),
      mutated(base, (state) => {
        first(state.leases).revision = -1;
      }),
      mutated(base, (state) => {
        state.tempOwners.push(structuredClone(first(state.tempOwners)));
      }),
      mutated(base, (state) => {
        first(state.tempOwners).revision = 1;
      }),
      mutated(base, (state) => {
        state.nextRowIds = [["missing-table", 1n]];
      }),
      mutated(base, (state) => {
        state.nextRowIds = [["table", 0n]];
      }),
      mutated(base, (state) => {
        state.nextAutoIncrement = [["table/missing", 1n]];
      }),
      mutated(base, (state) => {
        state.nextAutoIncrement = [
          ["table/sequence", 1n],
          ["table/sequence", 2n],
        ];
      }),
    ];
    expectAtomicRefusal(candidates);
  });

  it("rejects orphaned or inconsistent full-text durable generations", () => {
    const ftsTable = table({
      ftsColumns: {
        value: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "ready",
          buildFromVersion: 0,
        },
      },
    });
    const postings = [{ term: "term", rowIds: [1n], tf: [1] }];
    const base: RecordCoreState = {
      ...emptyState(),
      currentVersion: 0,
      catalogEpoch: 1,
      schemaEpoch: 1,
      manifests: [manifest()],
      tables: [ftsTable],
      ftsBases: [["table/value", { coversVersion: 0, chunks: [postings], totalTokens: 1 }]],
    };
    const candidates = [
      mutated(base, (state) => {
        state.ftsBases.push(structuredClone(first(state.ftsBases)));
      }),
      mutated(base, (state) => {
        state.ftsBases[0] = ["other/value", first(state.ftsBases)[1]];
      }),
      mutated(base, (state) => {
        first(state.ftsBases)[1].coversVersion = 1;
      }),
      mutated(base, (state) => {
        first(state.ftsBases)[1].totalTokens = 2;
      }),
      mutated(base, (state) => {
        state.ftsDeltas = [["other/value", [[0, { postings, totalTokens: 1 }]]]];
      }),
      mutated(base, (state) => {
        state.ftsDeltas = [["table/value", [[0, { postings, totalTokens: 1 }]]]];
      }),
      mutated(base, (state) => {
        state.ftsDeltas = [
          [
            "table/value",
            [
              [1, { postings, totalTokens: 1 }],
              [1, { postings, totalTokens: 1 }],
            ],
          ],
        ];
        state.currentVersion = 1;
        state.manifests.push(manifest({ version: 1, previousVersion: 0 }));
      }),
    ];
    expectAtomicRefusal(candidates);
  });
});
