import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsTree } from "../opfs/files.js";
import { WalWriter, replayWalFrames } from "./wal.js";
import { ExtentPool } from "./extents.js";

async function walHandle(shim: MemoryOpfs): Promise<FileSystemSyncAccessHandle> {
  return new OpfsTree(shim.root).openHandle(["wal"], { create: true });
}

describe("WAL frames", () => {
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
});

describe("extent pool", () => {
  it("appends with placements, seals at the size bound, and reads back", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const pool = await ExtentPool.open(tree, undefined);
    const first = await pool.append(new Uint8Array([1, 2, 3]), false);
    const second = await pool.append(new Uint8Array([4, 5]), false);
    expect(first).toEqual({ extent: 0, offset: 0, length: 3 });
    expect(second).toEqual({ extent: 0, offset: 3, length: 2 });
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

  it("round-trips its meta through a checkpoint and restores replayed placements", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const pool = await ExtentPool.open(tree, undefined);
    const placed = await pool.append(new Uint8Array([9, 9]), false);
    const meta = pool.meta();
    pool.close();

    const reopened = await ExtentPool.open(tree, meta);
    // A placement recorded after the checkpoint replays into the accounting and the tail.
    const later = { extent: meta.tailExtentId, offset: 2, length: 3 };
    reopened.restorePlacement(later);
    const next = await reopened.append(new Uint8Array([7]), false);
    expect(next.offset).toBe(5); // resumes after the replayed placement, not over it
    expect([...(await reopened.read(placed))]).toEqual([9, 9]);
    reopened.close();
  });
});
