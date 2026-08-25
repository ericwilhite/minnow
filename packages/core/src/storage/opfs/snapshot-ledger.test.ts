import { describe, expect, it } from "vitest";
import { crc32 } from "../../block-format/checksum.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import type { SnapshotFrame } from "../types.js";
import { OpfsTree } from "./files.js";
import { SnapshotFrameLedger, snapshotLedgerPath } from "./snapshot-ledger.js";

const createdAt = "2026-08-25T00:00:00.000Z";

function metadataFrame(payload = Uint8Array.of(1, 2, 3)): SnapshotFrame {
  return {
    sequence: 0,
    kind: "catalog-page",
    itemCount: 1,
    key: null,
    payload,
    checksum: crc32(payload),
  };
}

function blockFrame(payload = Uint8Array.of(4, 5, 6)): SnapshotFrame {
  return {
    sequence: 1,
    kind: "block",
    itemCount: 1,
    key: "block-id",
    payload,
    checksum: crc32(payload),
  };
}

function rewriteRecordChecksum(bytes: Uint8Array): Uint8Array {
  const next = bytes.slice();
  const view = new DataView(next.buffer, next.byteOffset, next.byteLength);
  view.setUint32(76, 0, true);
  view.setUint32(76, crc32(next), true);
  return next;
}

describe("snapshot frame ledger hardening", () => {
  it("round-trips inline and placed frames, truncates unpublished suffixes, and closes once", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const path = snapshotLedgerPath("export", createdAt).join("/");
    const ledger = await SnapshotFrameLedger.open(tree, "export", createdAt, 0, true);
    const inline = ledger.append(metadataFrame());
    const block = blockFrame();
    const placed = ledger.append(block, {
      extent: 2,
      offset: 8,
      length: block.payload.byteLength,
      checksum: block.checksum,
    });
    ledger.flush();

    expect([...ledger.records()]).toEqual([inline, placed]);
    expect(ledger.read(inline.offset)).toEqual(inline);
    expect(ledger.read(placed.offset)).toEqual(placed);
    expect(ledger.byteLength).toBe(shim.readFileBytes(path)?.byteLength);

    const committed = inline.nextOffset;
    ledger.truncate(committed);
    expect(ledger.byteLength).toBe(committed);
    ledger.close();
    ledger.close();

    const reopened = await SnapshotFrameLedger.open(tree, "export", createdAt, committed, false);
    expect([...reopened.records()]).toEqual([inline]);
    reopened.adoptLength(committed);
    reopened.close();
    expect(() => reopened.read(0)).toThrow(/closed/);
    expect(() => reopened.append(metadataFrame())).toThrow(/closed/);
    expect(() => reopened.flush()).toThrow(/closed/);
    expect(() => reopened.truncate(0)).toThrow(/closed/);
    expect(() => reopened.adoptLength(0)).toThrow(/closed/);
  });

  it("refuses invalid lengths and a committed prefix longer than the physical ledger", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    for (const length of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1]) {
      await expect(
        SnapshotFrameLedger.open(tree, "import", "bad-length", length, true),
      ).rejects.toThrow(RangeError);
    }
    const ledger = await SnapshotFrameLedger.open(tree, "import", "short", 0, true);
    ledger.append(metadataFrame());
    const length = ledger.byteLength;
    ledger.close();
    await expect(
      SnapshotFrameLedger.open(tree, "import", "short", length + 1, false),
    ).rejects.toThrow(/shorter than its committed prefix/);
  });

  it("rejects invalid frames and inconsistent block placements before changing the ledger", async () => {
    const shim = new MemoryOpfs();
    const ledger = await SnapshotFrameLedger.open(
      new OpfsTree(shim.root),
      "completed",
      "append-validation",
      0,
      true,
    );
    const valid = metadataFrame();
    const invalidFrames: SnapshotFrame[] = [
      { ...valid, sequence: -1 },
      { ...valid, sequence: 0.5 },
      { ...valid, kind: "unknown" as SnapshotFrame["kind"] },
      { ...valid, itemCount: 0 },
      { ...valid, payload: [] as unknown as Uint8Array },
      { ...valid, checksum: -1 },
      { ...valid, checksum: 0x1_0000_0000 },
      { ...valid, checksum: valid.checksum ^ 1 },
      { ...valid, key: "metadata-key" },
      { ...blockFrame(), key: null },
      { ...blockFrame(), itemCount: 2 },
      { ...blockFrame(), payload: new Uint8Array() },
    ];
    for (const frame of invalidFrames) expect(() => ledger.append(frame)).toThrow();
    expect(ledger.byteLength).toBe(0);

    const block = blockFrame();
    expect(() => ledger.append(block)).toThrow(/placement is inconsistent/);
    expect(() =>
      ledger.append(valid, { extent: 0, offset: 0, length: 3, checksum: valid.checksum }),
    ).toThrow(/placement is inconsistent/);
    expect(() =>
      ledger.append(block, {
        extent: 0,
        offset: 0,
        length: block.payload.byteLength + 1,
        checksum: block.checksum,
      }),
    ).toThrow(/placement disagrees/);
    expect(() =>
      ledger.append(block, {
        extent: 0,
        offset: 0,
        length: block.payload.byteLength,
        checksum: block.checksum ^ 1,
      }),
    ).toThrow(/placement disagrees/);
    ledger.close();
  });

  it("detects every durable record envelope, body, identity, checksum, and placement corruption", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const id = "corruption";
    const path = snapshotLedgerPath("import", id).join("/");
    let ledger = await SnapshotFrameLedger.open(tree, "import", id, 0, true);
    const appended = ledger.append(metadataFrame());
    const original = shim.readFileBytes(path);
    if (original === undefined) throw new Error("Missing ledger fixture");
    ledger.close();

    const expectCorruption = async (
      mutate: (bytes: Uint8Array, view: DataView) => void,
      error: RegExp,
    ) => {
      const bytes = original.slice();
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      mutate(bytes, view);
      shim.writeFileBytes(path, rewriteRecordChecksum(bytes));
      const reader = await SnapshotFrameLedger.open(tree, "import", id, bytes.byteLength, false);
      expect(() => reader.read(0), error.source).toThrow(error);
      reader.close();
    };

    shim.corruptFileByte(path, 0);
    ledger = await SnapshotFrameLedger.open(tree, "import", id, original.byteLength, false);
    expect(() => ledger.read(0)).toThrow(/marker mismatch/);
    ledger.close();
    shim.writeFileBytes(path, original);
    for (const recordLength of [79, original.byteLength + 1, 17 * 1024 * 1024]) {
      const bytes = original.slice();
      new DataView(bytes.buffer).setUint32(4, recordLength, true);
      shim.writeFileBytes(path, bytes);
      const reader = await SnapshotFrameLedger.open(tree, "import", id, bytes.byteLength, false);
      expect(() => reader.read(0)).toThrow(/record length is invalid/);
      reader.close();
    }
    shim.writeFileBytes(path, original);
    shim.corruptFileByte(path, original.byteLength - 1);
    ledger = await SnapshotFrameLedger.open(tree, "import", id, original.byteLength, false);
    expect(() => ledger.read(0)).toThrow(/record checksum mismatch/);
    ledger.close();

    await expectCorruption(
      (_bytes, view) => view.setBigUint64(8, 0xffff_ffff_ffff_ffffn, true),
      /header is invalid/,
    );
    await expectCorruption(
      (_bytes, view) => view.setUint32(16, 0xffff_ffff, true),
      /header is invalid/,
    );
    await expectCorruption((_bytes, view) => view.setUint32(20, 0, true), /header is invalid/);
    await expectCorruption((_bytes, view) => view.setBigUint64(32, 0n, true), /header is invalid/);
    await expectCorruption((_bytes, view) => view.setUint32(28, 2, true), /header is invalid/);
    await expectCorruption((bytes) => {
      bytes[72] = 1;
    }, /header is invalid/);
    await expectCorruption((_bytes, view) => view.setUint32(24, 1, true), /body length is invalid/);
    await expectCorruption(
      (_bytes, view) => view.setUint32(40, validDifferentChecksum(original), true),
      /payload checksum mismatch/,
    );

    shim.writeFileBytes(path, original);
    ledger = await SnapshotFrameLedger.open(tree, "import", id, original.byteLength, false);
    for (const offset of [-1, 0.5, appended.nextOffset, appended.nextOffset + 1]) {
      expect(() => ledger.read(offset)).toThrow(RangeError);
    }
    expect(() => ledger.read(0, -1)).toThrow(RangeError);
    expect(() => ledger.read(0, appended.nextOffset + 1)).toThrow(RangeError);
    expect(() => ledger.truncate(-1)).toThrow(RangeError);
    expect(() => ledger.truncate(appended.nextOffset + 1)).toThrow(RangeError);
    expect(() => ledger.adoptLength(-1)).toThrow(RangeError);
    expect(() => ledger.adoptLength(appended.nextOffset + 1)).toThrow(RangeError);
    ledger.close();
  });

  it("rejects noncanonical UTF-8, inconsistent block identity, and corrupt placed records", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const id = "placed-corruption";
    const path = snapshotLedgerPath("export", id).join("/");
    const ledger = await SnapshotFrameLedger.open(tree, "export", id, 0, true);
    const frame = blockFrame();
    ledger.append(frame, {
      extent: 3,
      offset: 9,
      length: frame.payload.byteLength,
      checksum: frame.checksum,
    });
    const original = shim.readFileBytes(path);
    if (original === undefined) throw new Error("Missing placed ledger fixture");
    ledger.close();
    const mutate = async (change: (bytes: Uint8Array, view: DataView) => void, error: RegExp) => {
      const bytes = original.slice();
      change(bytes, new DataView(bytes.buffer));
      shim.writeFileBytes(path, rewriteRecordChecksum(bytes));
      const reader = await SnapshotFrameLedger.open(tree, "export", id, bytes.byteLength, false);
      expect(() => reader.read(0)).toThrow(error);
      reader.close();
    };

    await mutate((bytes) => {
      bytes[80] = 0xff;
    }, /key is not canonical UTF-8/);
    await mutate((_bytes, view) => view.setUint32(24, 0, true), /body length is invalid/);
    await mutate((_bytes, view) => view.setUint32(16, 0, true), /frame identity is inconsistent/);
    await mutate((_bytes, view) => view.setUint32(28, 0, true), /body length is invalid/);
    await mutate(
      (_bytes, view) => view.setBigUint64(44, 0xffff_ffff_ffff_ffffn, true),
      /safe integer|placement/i,
    );
    await mutate(
      (_bytes, view) => view.setBigUint64(60, BigInt(frame.payload.byteLength + 1), true),
      /placement disagrees/,
    );
    await mutate(
      (_bytes, view) => view.setUint32(68, frame.checksum ^ 1, true),
      /placement disagrees/,
    );
  });
});

function validDifferentChecksum(encoded: Uint8Array): number {
  const payload = encoded.subarray(80);
  return crc32(payload) ^ 1;
}
