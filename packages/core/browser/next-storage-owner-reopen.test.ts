/** Byte accounting uses stored ByteRecord/IndexEntry shapes, not just flat numeric values. */
import "fake-indexeddb/auto";
import { IDBFactory } from "fake-indexeddb";
import { expect, it } from "vitest";
import { openIndexedDb } from "../src/next/storage/idb-like.js";
import { countDatabase } from "./next-storage-owner-reopen.js";

it("counts cloned record bytes and the numeric fields of persisted index entries separately", async () => {
  const raw = await openIndexedDb(new IDBFactory(), "owner-read-byte-accounting");
  try {
    await raw.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
      tx.store("data").put(0, { offset: 0, bytes: new ArrayBuffer(32) });
      tx.store("data-index").put(0, { offset: 0, length: 32 });
    });
    const counts = { payloadBytes: 0, numericBytes: 0, reads: 0 };
    const observed = countDatabase(raw, counts);
    await observed.transaction(["data", "data-index"], "readonly", "relaxed", (tx) => {
      tx.store("data-index").last(null, () => undefined);
      tx.store("data-index").getAll({}, () => undefined);
      tx.store("data-index").getAllKeys({}, () => undefined);
      tx.store("data").get(0, () => undefined);
    });
    // Two 16-byte index values, one 8-byte key and the byte record's 8-byte offset.
    expect(counts).toEqual({ payloadBytes: 32, numericBytes: 48, reads: 4 });
  } finally {
    raw.close();
  }
});
