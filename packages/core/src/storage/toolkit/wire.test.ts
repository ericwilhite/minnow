import { describe, expect, it } from "vitest";
import {
  decodeChunk,
  decodePostingChunk,
  decodeRecordJson,
  decodeSyncCheckpoint,
  encodeChunk,
  encodePostingChunk,
  encodeRecordJson,
  encodeSyncCheckpoint,
  LOG_FORMAT_VERSION,
} from "./wire.js";
import { RecordCore, type RecordCoreState } from "./record-core.js";
import { crc32 } from "../../block-format/index.js";
import {
  MAX_AUTO_INCREMENT_VALUE,
  MAX_ROW_ID_EXCLUSIVE_END,
  StorageFormatVersionError,
} from "../types.js";

function postingEnvelope(payload: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(20 + payload.byteLength);
  new TextEncoder().encodeInto("MNWPOST1", bytes);
  const view = new DataView(bytes.buffer);
  view.setUint32(8, LOG_FORMAT_VERSION, true);
  view.setUint32(12, payload.byteLength, true);
  view.setUint32(16, crc32(payload), true);
  bytes.set(payload, 20);
  return bytes;
}

describe("record JSON codec", () => {
  it("round-trips bigints wherever they appear", () => {
    const value = {
      segment: {
        rowIdStart: 12_345_678_901_234_567_890n,
        rowIdEndExclusive: 18_446_744_073_709_551_616n,
      },
      postings: [{ term: "minnow", rowIds: [1n, 2n, 9_007_199_254_740_993n], tf: [1, 2, 3] }],
      atLeast: 1n,
      plain: { text: "unchanged", count: 7, flag: true, nothing: null },
    };
    expect(decodeRecordJson(encodeRecordJson(value))).toEqual(value);
  });

  it("leaves strings that look like the bigint tag alone", () => {
    const value = { text: '{"$n":"42"}', nested: ["$n", { $n: 42 }] };
    // The object with a numeric $n is not the tag (tag values are strings), so it survives.
    expect(decodeRecordJson(encodeRecordJson(value))).toEqual(value);
  });

  it("rejects noncanonical, negative, oversized, and invalid-UTF8 record values", () => {
    expect(() => encodeRecordJson(undefined)).toThrow(/not JSON-serializable/);
    expect(() => encodeRecordJson({ value: -1n })).toThrow(/unsigned 64-bit/);
    expect(() => encodeRecordJson({ value: (1n << 64n) + 1n })).toThrow(/unsigned 64-bit/);
    expect(() => decodeRecordJson(new TextEncoder().encode('{"$n":"01"}'))).toThrow(
      /canonical bounded unsigned decimal/,
    );
    expect(() => decodeRecordJson(new TextEncoder().encode(`{"$n":"${"9".repeat(100)}"}`))).toThrow(
      /canonical bounded unsigned decimal/,
    );
    expect(() => decodeRecordJson(Uint8Array.of(0xff))).toThrow();
  });
});

describe("chunk envelope", () => {
  it("round-trips a posting chunk", () => {
    const chunk = [{ term: "minnow", rowIds: [1n, 2n], tf: [1, 1] }];
    expect(decodeChunk(encodeChunk(chunk))).toEqual(chunk);
  });

  it("treats truncation at every byte as not-written", () => {
    const bytes = encodeChunk([{ term: "a", rowIds: [1n], tf: [1] }]);
    for (let cut = 0; cut < bytes.byteLength; cut += 1) {
      expect(decodeChunk(bytes.subarray(0, cut))).toBeUndefined();
    }
  });

  it("treats a corrupted payload as not-written", () => {
    const bytes = encodeChunk([{ term: "a", rowIds: [1n], tf: [1] }]).slice();
    bytes[bytes.byteLength - 1] = (bytes[bytes.byteLength - 1] ?? 0) ^ 0xff;
    expect(decodeChunk(bytes)).toBeUndefined();
  });

  it("rejects trailing bytes outside the declared envelope", () => {
    const intact = encodeChunk([{ term: "a", rowIds: [1n], tf: [1] }]);
    const extended = new Uint8Array(intact.byteLength + 1);
    extended.set(intact);
    expect(decodeChunk(extended)).toBeUndefined();
  });

  it("refuses an unknown format version instead of ignoring it", () => {
    const bytes = encodeChunk([{ term: "a", rowIds: [1n], tf: [1] }]).slice();
    new DataView(bytes.buffer).setUint32(8, 999, true);
    const error = (() => {
      try {
        decodeChunk(bytes);
      } catch (caught) {
        return caught;
      }
      return undefined;
    })();
    expect(error).toBeInstanceOf(StorageFormatVersionError);
    expect(error).toMatchObject({
      backend: "opfs",
      location: "envelope/MNWCHNK1",
      actualVersion: 999,
      supportedVersion: LOG_FORMAT_VERSION,
      relation: "newer",
    });
  });

  it("treats the wrong magic as not-written", () => {
    const checkpoint = encodeSyncCheckpoint({ anything: 1 });
    expect(decodeChunk(checkpoint)).toBeUndefined();
  });
});

describe("compact posting chunks", () => {
  it("round-trips sorted bigint locators and unicode terms", () => {
    const postings = [
      { term: "café", rowIds: [1n, 2n, 4_294_967_295n], tf: [1, 3, 2] },
      { term: "paid", rowIds: [7n, 8n, 50n], tf: [1, 1, 1] },
    ];
    expect(decodePostingChunk(encodePostingChunk(postings))).toEqual(postings);
  });

  it("is materially smaller than the legacy JSON representation", () => {
    const postings = Array.from({ length: 100 }, (_, term) => ({
      term: `term-${String(term)}`,
      rowIds: Array.from({ length: 20 }, (_, row) => BigInt(term * 100 + row + 1)),
      tf: Array.from({ length: 20 }, () => 1),
    }));
    expect(encodePostingChunk(postings).byteLength).toBeLessThan(
      encodeChunk(postings).byteLength / 3,
    );
  });

  it("rejects a generic record chunk as a postings payload", () => {
    const generic = encodeChunk([{ term: "wrong-envelope", rowIds: [1n], tf: [1] }]);
    expect(decodePostingChunk(generic)).toBeUndefined();
  });

  it.each([
    ["huge entry count", Uint8Array.of(100), /count exceeds the remaining payload/],
    ["overlong varuint", Uint8Array.of(0x80, 0x00), /varuint is overlong/],
    [
      "uint64 overflow",
      Uint8Array.of(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02),
      /varuint exceeds uint64/,
    ],
    ["invalid UTF-8", Uint8Array.of(1, 1, 0xff, 1, 1, 1), /encoded data was not valid/i],
    ["zero term frequency", Uint8Array.of(1, 1, 0x61, 1, 1, 0), /frequency must be positive/],
  ])("rejects hostile compact posting payloads: %s", (_label, payload, message) => {
    expect(() => decodePostingChunk(postingEnvelope(payload))).toThrow(message);
  });
});

describe("synchronous checkpoint slots", () => {
  it("round-trips synthetic snapshots and retained committed journals at sparse versions", () => {
    const physical = { hasBlock: (id: string) => id === "b1", blockByteLength: () => 8 };
    const table = {
      id: "t1",
      name: "items",
      columns: [{ id: "c1", name: "value", type: "string" as const, nullable: false }],
      managed: false,
      revision: 0,
      createdAt: "2026-08-19T00:00:00.000Z",
    };
    const segment = {
      id: "s1",
      tableId: "t1",
      transactionId: "tx1",
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: { c1: ["b1"] },
      kind: "insert" as const,
      level: 0,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: "2026-08-19T00:00:00.000Z",
    };
    const transaction = {
      id: "tx1",
      ownerId: "owner1",
      expiresAt: "2026-08-19T01:00:00.000Z",
      snapshotVersion: null,
      pendingBlockIds: [] as string[],
      pendingSegmentIds: [] as string[],
      status: "committed" as const,
      revision: 1,
      startedAt: "2026-08-19T00:00:00.000Z",
      updatedAt: "2026-08-19T00:00:00.000Z",
      committedVersion: 7,
    };
    const restored = new RecordCore(physical);
    const restoredState = restored.dump();
    restoredState.currentVersion = 7;
    restoredState.catalogEpoch = 1;
    restoredState.manifests = [
      {
        version: 7,
        previousVersion: null,
        createdAt: "2026-08-19T00:00:00.000Z",
        liveBlockCount: 1,
        liveBlockBytes: 8,
        changedTableIds: [],
      },
    ];
    restoredState.manifestBlocks = [
      {
        blockId: "b1",
        byteLength: 8,
        checksum: 0,
        addedVersion: 7,
        removedVersion: null,
      },
    ];
    restoredState.tables = [table];
    restoredState.segments = [segment];
    restoredState.transactions = [transaction];
    restoredState.nextRowIds = [["t1", 2n]];
    restored.load(restoredState);
    const synthetic = decodeSyncCheckpoint(
      encodeSyncCheckpoint(restored.dump()),
    ) as RecordCoreState;
    const reopened = new RecordCore(physical);
    expect(() => reopened.load(synthetic)).not.toThrow();
    expect(reopened.getCurrentManifest()).toMatchObject({ version: 7, previousVersion: null });

    const retained = structuredClone(synthetic);
    const retainedTransaction = retained.transactions[0];
    if (retainedTransaction === undefined) throw new Error("Missing retained transaction");
    retainedTransaction.pendingBlockIds = ["b1"];
    retainedTransaction.pendingSegmentIds = ["s1"];
    expect(() => reopened.load(retained)).not.toThrow();
    expect(reopened.getTransaction("tx1")?.pendingSegmentIds).toEqual(["s1"]);

    const brokenChain = structuredClone(retained);
    brokenChain.manifests.push({
      version: 9,
      previousVersion: 8,
      createdAt: "2026-08-19T00:01:00.000Z",
      liveBlockCount: 1,
      liveBlockBytes: 8,
      changedTableIds: [],
    });
    brokenChain.currentVersion = 9;
    expect(() => reopened.load(brokenChain)).toThrow(/non-contiguous predecessor/);
  });

  it("round-trips a full record-state dump with bigint counters", () => {
    const core = new RecordCore({ hasBlock: () => true, blockByteLength: () => 8 });
    core.addTable({
      id: "t1",
      name: "items",
      columns: [
        {
          id: "c1",
          name: "id",
          type: "number",
          nullable: false,
          defaultValue: { kind: "autoincrement" },
        },
      ],
      uniqueKeyColumnId: "c1",
      managed: false,
      revision: 0,
      createdAt: "2026-08-19T00:00:00.000Z",
    });
    core.reserveRowIds("t1", 100);
    core.reserveAutoIncrement("t1", "c1", 5, 40n);
    const dumped = core.dump();

    const decoded = decodeSyncCheckpoint(encodeSyncCheckpoint(dumped));
    expect(decoded).toEqual(dumped);

    const reloaded = new RecordCore({ hasBlock: () => true, blockByteLength: () => 8 });
    reloaded.load(dumped);
    expect(reloaded.dump()).toEqual(dumped);
    // The reloaded counters continue where the dump left off, not from 1.
    expect(reloaded.reserveRowIds("t1", 1)).toEqual({ start: 101n, endExclusive: 102n });
    expect(reloaded.reserveAutoIncrement("t1", "c1", 1)).toEqual({
      start: 45n,
      endExclusive: 46n,
    });

    const beforeInvalidLoad = reloaded.dump();
    const invalid = structuredClone(dumped);
    const column = invalid.tables[0]?.columns[0];
    if (column === undefined) throw new Error("Missing test column");
    column.type = "string";
    column.integer = true;
    expect(() => reloaded.load(invalid)).toThrow(/Integer domain requires a number column: id/);
    expect(reloaded.dump()).toEqual(beforeInvalidLoad);
  });

  it("refuses counter overflow before mutation and preserves the final valid reservation", () => {
    const core = new RecordCore({ hasBlock: () => true, blockByteLength: () => 8 });
    core.addTable({
      id: "bounded",
      name: "bounded",
      columns: [
        {
          id: "id",
          name: "id",
          type: "number",
          nullable: false,
          defaultValue: { kind: "autoincrement" },
        },
      ],
      managed: false,
      revision: 0,
      createdAt: "2026-08-19T00:00:00.000Z",
    });
    const state = core.dump();
    state.nextRowIds = [["bounded", MAX_ROW_ID_EXCLUSIVE_END - 1n]];
    state.nextAutoIncrement = [["bounded/id", MAX_AUTO_INCREMENT_VALUE]];
    core.load(state);

    expect(() => core.reserveRowIds("bounded", 2)).toThrow(/numeric range/);
    expect(core.reserveRowIds("bounded", 1)).toEqual({
      start: MAX_ROW_ID_EXCLUSIVE_END - 1n,
      endExclusive: MAX_ROW_ID_EXCLUSIVE_END,
    });
    expect(() =>
      core.reserveAutoIncrement("bounded", "id", 1, MAX_AUTO_INCREMENT_VALUE + 2n),
    ).toThrow(/bump target/);
    expect(() => core.reserveAutoIncrement("bounded", "id", 2)).toThrow(/numeric range/);
    expect(core.reserveAutoIncrement("bounded", "id", 1)).toEqual({
      start: MAX_AUTO_INCREMENT_VALUE,
      endExclusive: MAX_AUTO_INCREMENT_VALUE + 1n,
    });
  });

  it("fails closed on semantic checkpoint corruption without replacing live state", () => {
    const physical = { hasBlock: () => true, blockByteLength: () => 8 };
    const core = new RecordCore(physical);
    core.addTable({
      id: "t1",
      name: "items",
      columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
      uniqueKeyColumnId: "c1",
      managed: false,
      revision: 0,
      createdAt: "2026-08-19T00:00:00.000Z",
    });
    const base = core.dump();
    base.currentVersion = 0;
    base.manifests = [
      {
        version: 0,
        previousVersion: null,
        createdAt: "2026-08-19T00:00:00.000Z",
        liveBlockCount: 1,
        liveBlockBytes: 8,
        changedTableIds: [],
      },
    ];
    base.manifestBlocks = [
      {
        blockId: "live",
        byteLength: 8,
        checksum: 0,
        addedVersion: 0,
        removedVersion: null,
      },
    ];
    base.transactions = [
      {
        id: "tx1",
        ownerId: "owner1",
        expiresAt: "2026-08-19T01:00:00.000Z",
        snapshotVersion: 0,
        pendingBlockIds: ["pending"],
        pendingSegmentIds: ["s1"],
        status: "active",
        schemaEpochGuard: base.schemaEpoch,
        revision: 0,
        startedAt: "2026-08-19T00:00:01.000Z",
        updatedAt: "2026-08-19T00:00:01.000Z",
        committedVersion: null,
      },
    ];
    base.segments = [
      {
        id: "s1",
        tableId: "t1",
        transactionId: "tx1",
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { c1: ["pending"] },
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
        createdAt: "2026-08-19T00:00:01.000Z",
      },
    ];
    base.leases = [
      {
        id: "lease1",
        kind: "reader",
        manifestVersion: 0,
        ownerId: "owner",
        createdAt: "2026-08-19T00:00:00.000Z",
        expiresAt: "2026-08-19T00:01:00.000Z",
        revision: 0,
      },
    ];
    core.load(base);
    const before = core.dump();
    const rejects = (mutate: (state: RecordCoreState) => void, message: RegExp): void => {
      const invalid = structuredClone(base);
      mutate(invalid);
      expect(() => core.load(invalid)).toThrow(message);
      expect(core.dump()).toEqual(before);
    };

    rejects((state) => {
      const manifest = state.manifests[0];
      if (manifest !== undefined) manifest.previousVersion = -1;
    }, /predecessor must be a non-negative whole number/);
    rejects((state) => {
      state.uniqueKeys = [];
    }, /missing unique membership/);
    rejects((state) => {
      state.uniqueKeys.push(["orphan", []]);
    }, /orphan unique membership/);
    rejects((state) => {
      Object.assign(state.tables[0]?.columns[0] ?? {}, { nullable: "no" });
    }, /invalid primitive metadata/);
    rejects((state) => {
      const lease = state.leases[0];
      if (lease !== undefined) lease.manifestVersion = 99;
    }, /no readable manifest/);
    rejects((state) => {
      const transaction = state.transactions[0];
      if (transaction !== undefined) transaction.pendingSegmentIds = ["missing"];
    }, /missing segment/);
    rejects((state) => {
      state.manifestBlocks.push({
        blockId: "pending",
        byteLength: 8,
        checksum: 0,
        addedVersion: 0,
        removedVersion: null,
      });
    }, /live summary is inconsistent/);
    rejects((state) => {
      const transaction = state.transactions[0];
      if (transaction !== undefined) {
        state.transactions.push({
          ...structuredClone(transaction),
          id: "tx2",
          pendingBlockIds: ["pending"],
          pendingSegmentIds: [],
        });
      }
    }, /owned by multiple transactions/);
  });

  it("rejects a corrupted or torn slot", () => {
    const bytes = encodeSyncCheckpoint({ anything: 1 }).slice();
    expect(decodeSyncCheckpoint(bytes.subarray(0, bytes.byteLength - 1))).toBeUndefined();
    bytes[bytes.byteLength - 1] = (bytes[bytes.byteLength - 1] ?? 0) ^ 0xff;
    expect(decodeSyncCheckpoint(bytes)).toBeUndefined();
    expect(decodeSyncCheckpoint(new Uint8Array(0))).toBeUndefined();
  });
});
