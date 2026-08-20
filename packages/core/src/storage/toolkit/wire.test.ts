import { describe, expect, it } from "vitest";
import {
  decodeChunk,
  decodeRecordJson,
  decodeSyncCheckpoint,
  encodeChunk,
  encodeRecordJson,
  encodeSyncCheckpoint,
} from "./wire.js";
import { RecordCore } from "./record-core.js";

describe("record JSON codec", () => {
  it("round-trips bigints wherever they appear", () => {
    const value = {
      segment: { rowIdStart: 12_345_678_901_234_567_890n, rowIdEndExclusive: -3n },
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

  it("refuses a newer format version instead of ignoring it", () => {
    const bytes = encodeChunk([{ term: "a", rowIds: [1n], tf: [1] }]).slice();
    new DataView(bytes.buffer).setUint32(8, 999, true);
    expect(() => decodeChunk(bytes)).toThrow(/newer format/);
  });

  it("treats the wrong magic as not-written", () => {
    const checkpoint = encodeSyncCheckpoint({ anything: 1 });
    expect(decodeChunk(checkpoint)).toBeUndefined();
  });
});

describe("synchronous checkpoint slots", () => {
  it("round-trips a full record-state dump with bigint counters", () => {
    const core = new RecordCore({ hasBlock: () => true, blockByteLength: () => 8 });
    core.addTable({
      id: "t1",
      name: "items",
      columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
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
  });

  it("rejects a corrupted or torn slot", () => {
    const bytes = encodeSyncCheckpoint({ anything: 1 }).slice();
    expect(decodeSyncCheckpoint(bytes.subarray(0, bytes.byteLength - 1))).toBeUndefined();
    bytes[bytes.byteLength - 1] = (bytes[bytes.byteLength - 1] ?? 0) ^ 0xff;
    expect(decodeSyncCheckpoint(bytes)).toBeUndefined();
    expect(decodeSyncCheckpoint(new Uint8Array(0))).toBeUndefined();
  });
});
