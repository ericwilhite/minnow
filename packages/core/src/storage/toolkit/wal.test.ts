import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsTree } from "../opfs/files.js";
import { MAX_WAL_FRAME_BYTES, WalWriter, replayWalFrames } from "./wal.js";
import { ExtentPool } from "./extents.js";
import { crc32 } from "../../block-format/index.js";
import { readFully, writeFully, type SyncFileHandle } from "./sync-file.js";

async function walHandle(shim: MemoryOpfs): Promise<FileSystemSyncAccessHandle> {
  return new OpfsTree(shim.root).openHandle(["wal"], { create: true });
}

function reportingHandle(report: number): SyncFileHandle {
  return {
    getSize: () => 1,
    read: () => report,
    write: () => report,
    truncate: () => undefined,
    flush: () => undefined,
    close: () => undefined,
  };
}

describe("full sync-file transfers", () => {
  for (const report of [-1, 0.5, Number.NaN, 2]) {
    it(`rejects the invalid transfer count ${String(report)}`, () => {
      const handle = reportingHandle(report);
      expect(() => writeFully(handle, Uint8Array.of(1), 4, "testing an invalid write")).toThrow(
        /Invalid OPFS write result.*file offset 4/,
      );
      expect(() => readFully(handle, Uint8Array.of(0), 9, "testing an invalid read")).toThrow(
        /Invalid OPFS read result.*file offset 9/,
      );
    });
  }

  it.each([
    ["negative", -1, 1],
    ["fractional", 0.5, 1],
    ["NaN", Number.NaN, 1],
    ["infinite", Number.POSITIVE_INFINITY, 1],
    ["overflowing", Number.MAX_SAFE_INTEGER, 1],
  ])("rejects a %s positioned range before calling the handle", (_label, at, length) => {
    let reads = 0;
    let writes = 0;
    const handle: SyncFileHandle = {
      getSize: () => 0,
      read: () => {
        reads += 1;
        return 0;
      },
      write: () => {
        writes += 1;
        return 0;
      },
      truncate: () => undefined,
      flush: () => undefined,
      close: () => undefined,
    };
    expect(() => writeFully(handle, new Uint8Array(length), at, "testing write bounds")).toThrow(
      /Invalid OPFS write range.*non-negative safe-integer byte range/,
    );
    expect(() => readFully(handle, new Uint8Array(length), at, "testing read bounds")).toThrow(
      /Invalid OPFS read range.*non-negative safe-integer byte range/,
    );
    expect({ reads, writes }).toEqual({ reads: 0, writes: 0 });
  });

  it("accepts the last safe byte range and empty range without platform calls", () => {
    let reads = 0;
    let writes = 0;
    const handle: SyncFileHandle = {
      getSize: () => 0,
      read: (buffer) => {
        reads += 1;
        return buffer.byteLength;
      },
      write: (buffer) => {
        writes += 1;
        return buffer.byteLength;
      },
      truncate: () => undefined,
      flush: () => undefined,
      close: () => undefined,
    };
    writeFully(handle, Uint8Array.of(1), Number.MAX_SAFE_INTEGER - 1);
    readFully(handle, Uint8Array.of(1), Number.MAX_SAFE_INTEGER - 1);
    writeFully(handle, new Uint8Array(0), Number.MAX_SAFE_INTEGER);
    readFully(handle, new Uint8Array(0), Number.MAX_SAFE_INTEGER);
    expect({ reads, writes }).toEqual({ reads: 1, writes: 1 });
  });
});

describe("WAL frames", () => {
  it("completes short writes and reads across frame-header boundaries", async () => {
    for (const transferBytes of [1, 4, 11, 12, 13, 31]) {
      const shim = new MemoryOpfs();
      shim.setTransferLimit((_path, _operation, requested) => Math.min(transferBytes, requested));
      const handle = await walHandle(shim);
      const writer = new WalWriter(handle, 0);
      writer.append({ seq: 1, op: "boundary", text: "x".repeat(40) }, false);
      const recovered = replayWalFrames(handle);
      expect(recovered.payloads).toEqual([{ seq: 1, op: "boundary", text: "x".repeat(40) }]);
      expect(recovered.frameEnds).toEqual([writer.byteLength]);
      handle.close();
    }
  });

  it("fails precisely when a short WAL write makes no progress", async () => {
    const shim = new MemoryOpfs();
    shim.setTransferLimit((_path, operation, requested, at) => {
      if (operation === "write" && at >= 5) return 0;
      return Math.min(5, requested);
    });
    const handle = await walHandle(shim);
    const writer = new WalWriter(handle, 0);
    expect(() => writer.append({ seq: 1, op: "stalled" }, false)).toThrow(
      /WAL frame.*wrote 5 of .*stalled at file offset 5/,
    );
    expect(writer.byteLength).toBe(0);
    expect(handle.getSize()).toBe(0);
    handle.close();
  });

  it("reports unexpected EOF when WAL recovery stops making read progress", async () => {
    const shim = new MemoryOpfs();
    const handle = await walHandle(shim);
    new WalWriter(handle, 0).append({ seq: 1, op: "read-stall" }, false);
    shim.setTransferLimit((_path, operation, requested, at) => {
      if (operation === "read" && at >= 5) return 0;
      return Math.min(5, requested);
    });
    expect(() => replayWalFrames(handle)).toThrow(
      /Unexpected EOF.*WAL frame header for recovery.*read 5 of .*EOF at file offset 5/,
    );
    handle.close();
  });

  it("replays appended frames in order and resumes at the tail", async () => {
    const shim = new MemoryOpfs();
    const handle = await walHandle(shim);
    const writer = new WalWriter(handle, 0);
    writer.append({ seq: 1, op: "a", big: 7n }, false);
    writer.append({ seq: 2, op: "b" }, true);

    const { payloads, endOffset } = replayWalFrames(handle);
    expect(payloads).toEqual([
      { seq: 1, op: "a", big: 7n },
      { seq: 2, op: "b" },
    ]);
    expect(endOffset).toBe(writer.byteLength);

    // A new writer at the replayed offset appends frame three, not garbage over frame two.
    const resumed = new WalWriter(handle, endOffset);
    resumed.append({ seq: 3, op: "c" }, false);
    expect(replayWalFrames(handle).payloads).toHaveLength(3);
    handle.close();
  });

  it("stops at a torn tail, whatever byte it tears at", async () => {
    const shim = new MemoryOpfs();
    const handle = await walHandle(shim);
    const writer = new WalWriter(handle, 0);
    writer.append({ seq: 1, op: "a" }, false);
    const intactLength = writer.byteLength;
    writer.append({ seq: 2, op: "b" }, false);
    const fullLength = writer.byteLength;

    for (let cut = intactLength; cut < fullLength; cut += 1) {
      handle.truncate(cut);
      const { payloads, endOffset } = replayWalFrames(handle);
      expect(payloads).toEqual([{ seq: 1, op: "a" }]);
      expect(endOffset).toBe(intactLength);
    }
    handle.close();
  });

  it("reset empties the log for the writer and the replayer alike", async () => {
    const shim = new MemoryOpfs();
    const handle = await walHandle(shim);
    const writer = new WalWriter(handle, 0);
    writer.append({ seq: 1 }, false);
    writer.reset();
    expect(writer.byteLength).toBe(0);
    expect(replayWalFrames(handle).payloads).toEqual([]);
    writer.append({ seq: 2 }, false);
    expect(replayWalFrames(handle).payloads).toEqual([{ seq: 2 }]);
    handle.close();
  });

  it("rejects an oversized frame from a sparse WAL before allocating its payload", () => {
    const header = new Uint8Array(12);
    const view = new DataView(header.buffer);
    view.setUint32(0, 0x4c574e4d, true);
    view.setUint32(4, MAX_WAL_FRAME_BYTES + 1, true);
    let largestRead = 0;
    const handle: SyncFileHandle = {
      getSize: () => 4 * 1024 * 1024 * 1024,
      read: (buffer, options) => {
        largestRead = Math.max(largestRead, buffer.byteLength);
        const at = options.at;
        buffer.set(header.subarray(at, at + buffer.byteLength));
        return buffer.byteLength;
      },
      write: () => 0,
      truncate: () => undefined,
      flush: () => undefined,
      close: () => undefined,
    };
    expect(() => replayWalFrames(handle)).toThrow(/exceeds the .* byte limit/);
    expect(largestRead).toBe(12);
  });
});

describe("extent pool", () => {
  it("completes short extent writes and reads without copying payloads", async () => {
    const shim = new MemoryOpfs();
    shim.setTransferLimit((_path, _operation, requested) => Math.min(3, requested));
    const pool = await ExtentPool.open(new OpfsTree(shim.root), undefined);
    const bytes = Uint8Array.from({ length: 257 }, (_, index) => index & 0xff);
    const placement = await pool.append(bytes, false);
    expect(await pool.read(placement)).toEqual(bytes);
    expect(await pool.contains(placement)).toBe(true);
    expect(await pool.contains({ ...placement, length: placement.length + 1 })).toBe(false);
    pool.close();
  });

  it("shares one held handle across concurrent first reads of a sealed extent", async () => {
    const shim = new MemoryOpfs();
    const pool = await ExtentPool.open(new OpfsTree(shim.root), undefined);
    const first = await pool.append(Uint8Array.of(1, 2, 3), false);
    const second = await pool.append(Uint8Array.of(4, 5, 6), false);
    await pool.append(new Uint8Array(9 * 1024 * 1024), false); // seals their extent

    await expect(Promise.all([pool.read(first), pool.read(second)])).resolves.toEqual([
      Uint8Array.of(1, 2, 3),
      Uint8Array.of(4, 5, 6),
    ]);
    pool.close();
  });

  it("does not evict handles reserved by concurrent reads", async () => {
    const shim = new MemoryOpfs();
    const liveBytes: Array<readonly [number, number]> = [];
    const placements = Array.from({ length: 13 }, (_, extent) => {
      shim.writeFileBytes(`extents/${String(extent).padStart(6, "0")}`, Uint8Array.of(extent));
      liveBytes.push([extent, 1]);
      return { extent, offset: 0, length: 1, checksum: crc32(Uint8Array.of(extent)) };
    });
    const pool = await ExtentPool.open(new OpfsTree(shim.root), {
      nextExtentId: 14,
      tailExtentId: 13,
      tailOffset: 0,
      liveBytes,
    });

    expect(await Promise.all(placements.map((placement) => pool.read(placement)))).toEqual(
      placements.map(({ extent }) => Uint8Array.of(extent)),
    );
    pool.close();
  });

  it("pins a selected sealed placement until a concurrent extent deletion finishes", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    let pauseSealedOpen = false;
    let announceOpen!: () => void;
    let resumeOpen!: () => void;
    const opening = new Promise<void>((resolve) => {
      announceOpen = resolve;
    });
    const resume = new Promise<void>((resolve) => {
      resumeOpen = resolve;
    });
    const files = {
      async openHandle(path: readonly string[], options: { create: boolean }) {
        if (pauseSealedOpen && path.at(-1) === "000000") {
          announceOpen();
          await resume;
        }
        return tree.openHandle(path, options);
      },
      deleteFile: (path: readonly string[]) => tree.deleteFile(path),
    };
    const pool = await ExtentPool.open(files, undefined);
    const selected = await pool.append(Uint8Array.of(1, 2, 3), false);
    await pool.append(new Uint8Array(9 * 1024 * 1024), false); // seal extent zero
    pauseSealedOpen = true;

    const read = pool.read(selected);
    await opening;
    expect(pool.release([selected])).toEqual([0]);
    let deleted = false;
    const deletion = pool.deleteExtent(0).then(() => {
      deleted = true;
    });
    await Promise.resolve();
    expect(deleted).toBe(false);
    resumeOpen();
    await expect(read).resolves.toEqual(Uint8Array.of(1, 2, 3));
    await deletion;
    expect(shim.readFileBytes("extents/000000")).toBeUndefined();
    pool.close();
  });

  it("checksum-scans exactly once per append and verified recovery read", async () => {
    const shim = new MemoryOpfs();
    let scans = 0;
    const pool = await ExtentPool.open(new OpfsTree(shim.root), undefined, {
      _checksumForTests: (bytes) => {
        scans += 1;
        return crc32(bytes);
      },
    });
    const bytes = Uint8Array.from({ length: 1_048_576 }, (_, index) => index & 0xff);
    const placement = await pool.append(bytes, false);
    expect(scans).toBe(1);
    expect(await pool.read(placement)).toEqual(bytes);
    expect(scans).toBe(1); // Ordinary reads rely on the payload format's normal read check.
    expect(await pool.readVerified(placement)).toEqual(bytes);
    expect(scans).toBe(2);
    pool.close();
  });

  it("rejects placement overflow before allocating a read buffer", async () => {
    const pool = await ExtentPool.open(new OpfsTree(new MemoryOpfs().root), undefined);
    await expect(
      pool.read({
        extent: 0,
        offset: Number.MAX_SAFE_INTEGER,
        length: 2,
        checksum: 0,
      }),
    ).rejects.toThrow(/Invalid extent placement/);
    pool.close();
  });

  it("does not publish a placement when an extent write stalls", async () => {
    const shim = new MemoryOpfs();
    shim.setTransferLimit((_path, operation, requested, at) => {
      if (operation === "write" && at >= 7) return 0;
      return Math.min(7, requested);
    });
    const pool = await ExtentPool.open(new OpfsTree(shim.root), undefined);
    await expect(pool.append(new Uint8Array(32), false)).rejects.toThrow(
      /extent 0.*wrote 7 of 32.*file offset 7/,
    );
    expect(pool.meta().tailOffset).toBe(0);
    pool.close();
  });

  it("appends with placements, seals at the size bound, and reads back", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const pool = await ExtentPool.open(tree, undefined);
    const first = await pool.append(new Uint8Array([1, 2, 3]), false);
    const second = await pool.append(new Uint8Array([4, 5]), false);
    expect(first).toMatchObject({ extent: 0, offset: 0, length: 3 });
    expect(second).toMatchObject({ extent: 0, offset: 3, length: 2 });
    expect([...(await pool.read(first))]).toEqual([1, 2, 3]);
    expect([...(await pool.read(second))]).toEqual([4, 5]);

    // A payload that would cross the seal bound rolls to a fresh extent.
    const big = await pool.append(new Uint8Array(9 * 1024 * 1024), false);
    expect(big.extent).toBe(1);
    expect(big.offset).toBe(0);
    expect([...(await pool.read(first))]).toEqual([1, 2, 3]); // sealed extents stay readable
    pool.close();
  });

  it("releases live bytes and deletes drained extents", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const pool = await ExtentPool.open(tree, undefined);
    const a = await pool.append(new Uint8Array(4 * 1024 * 1024), false);
    const b = await pool.append(new Uint8Array(5 * 1024 * 1024), false); // seals extent 0
    expect(b.extent).toBe(1);
    expect(pool.release([a])).toEqual([0]);
    await pool.deleteExtent(0);
    expect(shim.readFileBytes("extents/000000")).toBeUndefined();
    expect((await pool.read(b)).byteLength).toBe(b.length);
    pool.close();
  });

  it("retains drained-extent accounting when physical deletion is refused", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const pool = await ExtentPool.open(tree, undefined);
    const drained = await pool.append(new Uint8Array(4 * 1024 * 1024), false);
    await pool.append(new Uint8Array(5 * 1024 * 1024), false); // seals extent 0
    expect(pool.release([drained])).toEqual([0]);
    shim.setDeleteFault((path) => {
      if (path.endsWith("extents/000000")) {
        throw new DOMException("injected refusal", "NoModificationAllowedError");
      }
    });
    await expect(pool.deleteExtent(0)).rejects.toThrow("injected refusal");
    expect(pool.meta().liveBytes).toContainEqual([0, 0]);
    expect(shim.readFileBytes("extents/000000")).toBeDefined();
    shim.setDeleteFault(null);
    await pool.deleteExtent(0);
    expect(pool.meta().liveBytes).not.toContainEqual([0, 0]);
    expect(shim.readFileBytes("extents/000000")).toBeUndefined();
    pool.close();
  });

  it("fails a release batch atomically on underflow, duplicates, or overlap", async () => {
    const pool = await ExtentPool.open(new OpfsTree(new MemoryOpfs().root), undefined);
    const first = await pool.append(new Uint8Array(4), false);
    const second = await pool.append(new Uint8Array(4), false);
    const before = pool.meta();
    expect(() => pool.release([first, first])).toThrow(/overlapping or duplicate/);
    expect(pool.meta()).toEqual(before);
    expect(() => pool.release([{ ...second, length: 9 }])).toThrow(/underflow/);
    expect(pool.meta()).toEqual(before);
    pool.close();
  });

  it("rolls back unpublished multi-extent batches without leaving physical files", async () => {
    const shim = new MemoryOpfs();
    const pool = await ExtentPool.open(new OpfsTree(shim.root), undefined);
    const published = await pool.append(Uint8Array.of(1, 2, 3), false);
    const before = pool.meta();
    const mark = pool.markBatch();
    await pool.append(new Uint8Array(9 * 1024 * 1024), false);
    await pool.append(new Uint8Array(9 * 1024 * 1024), false);
    expect(shim.readFileBytes("extents/000002")).toBeDefined();
    await pool.rollbackBatch(mark);
    expect(pool.meta()).toEqual(before);
    expect(shim.readFileBytes("extents/000001")).toBeUndefined();
    expect(shim.readFileBytes("extents/000002")).toBeUndefined();
    expect(await pool.read(published)).toEqual(Uint8Array.of(1, 2, 3));
    pool.close();
  });

  it("tracks batch rollback cost by touched extents, not total database extents", async () => {
    const extentCount = 10_000;
    const pool = await ExtentPool.open(new OpfsTree(new MemoryOpfs().root), {
      nextExtentId: extentCount,
      tailExtentId: extentCount - 1,
      tailOffset: 0,
      liveBytes: Array.from({ length: extentCount }, (_, id) => [id, 1] as const),
    });
    const mark = pool.markBatch();
    expect(mark._liveBytesBefore.size).toBe(0);
    await pool.append(Uint8Array.of(1), false);
    expect(mark._liveBytesBefore.size).toBe(1);
    pool.commitBatch(mark);
    pool.close();
  });

  it("rejects duplicate and impossible checkpoint extent metadata", async () => {
    const tree = new OpfsTree(new MemoryOpfs().root);
    await expect(
      ExtentPool.open(tree, {
        nextExtentId: 2,
        tailExtentId: 1,
        tailOffset: 0,
        liveBytes: [
          [0, 1],
          [0, 1],
        ],
      }),
    ).rejects.toThrow(/Invalid OPFS extent live-byte entry/);
    await expect(
      ExtentPool.open(tree, {
        nextExtentId: 1,
        tailExtentId: 1,
        tailOffset: 0,
        liveBytes: [],
      }),
    ).rejects.toThrow(/Invalid OPFS extent checkpoint metadata/);
  });

  it("identifies sealed extents whose dead space exceeds their live payload", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const pool = await ExtentPool.open(tree, undefined);
    const dead = await pool.append(new Uint8Array(6 * 1024 * 1024), false);
    await pool.append(new Uint8Array(2 * 1024 * 1024), false);
    await pool.append(Uint8Array.of(1), false); // seals extent 0
    expect(await pool.fragmentedExtentId()).toBeUndefined();
    pool.release([dead]);
    expect(await pool.fragmentedExtentId()).toBe(0);
    pool.close();
  });

  it("round-trips its meta through a checkpoint and restores replayed placements", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const pool = await ExtentPool.open(tree, undefined);
    const placed = await pool.append(new Uint8Array([9, 9]), false);
    const meta = pool.meta();
    pool.close();

    const reopened = await ExtentPool.open(tree, meta);
    // A placement recorded after the checkpoint replays into the accounting and the tail.
    const later = { extent: meta.tailExtentId, offset: 2, length: 3, checksum: 0 };
    reopened.restorePlacement(later);
    const next = await reopened.append(new Uint8Array([7]), false);
    expect(next.offset).toBe(5); // resumes after the replayed placement, not over it
    expect([...(await reopened.read(placed))]).toEqual([9, 9]);
    reopened.close();
  });
});
