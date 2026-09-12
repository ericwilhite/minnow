/**
 * Acknowledged-write durability under modelled power loss.
 *
 * The shim keeps every write, so the crash tests elsewhere cover tab death only. These roll
 * every file back to its last flushed content (see power-loss-model.ts) and pin the two
 * documented guarantees:
 *   strict  — does every acknowledged write survive the loss of all unflushed bytes?
 *   relaxed — does recovery accept exactly a consecutive verified prefix (never partial state,
 *             never a refusal to open)?
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { mulberry32 } from "../../testing/seeds.js";
import { OpfsBlockStore } from "./index.js";
import { MinnowDatabase } from "../../engine/database.js";
import { encodeBlock } from "../../block-format/index.js";
import {
  extendSnapshotFrameStreamChecksum,
  snapshotFrameEnvelopeParts,
  snapshotFrameStreamHeaderIdentity,
} from "../snapshot-stream.js";
import type { SnapshotFrame, TableRecord } from "../types.js";
import { PowerLossModel, bytesEqual } from "./power-loss-model.js";

function table(name: string, id = `table-${name}`): TableRecord {
  return {
    id,
    name,
    columns: [{ id: "c1", name: "id", type: "string", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

let ordinal = 0;
async function beginTransaction(store: OpfsBlockStore, label: string): Promise<string> {
  ordinal += 1;
  const transactionId = `tx-${label}-${String(ordinal)}`;
  await store.beginTransaction({
    record: {
      id: transactionId,
      ownerId: `owner-${transactionId}`,
      expiresAt: "2026-08-24T01:00:00.000Z",
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    },
  });
  return transactionId;
}

async function bigBlock(id: string, bytes: number): Promise<{ id: string; bytes: Uint8Array }> {
  return { id, bytes: await encodeBlock({ type: "string", values: ["x".repeat(bytes)] }, "raw") };
}

interface Snapshot {
  rows: Array<{ id: number; v: number }>;
}

/** Drives SQL statements; after each acknowledged statement, power loss + reopen + verify. */
async function sqlLoop(durability: "strict" | "relaxed", keepPrefix: boolean, seed: number) {
  const shim = new MemoryOpfs();
  const model = new PowerLossModel(shim);
  const rng = mulberry32(seed);
  const name = `pl-${durability}-${String(seed)}`;
  const open = async () => {
    const store = await OpfsBlockStore.open({
      name,
      root: shim.root,
      durability,
      checkpointEntries: 7,
    });
    const db = new MinnowDatabase(store, { autoCompact: false, rowsPerBlock: 2 });
    return { store, db };
  };
  let { store, db } = await open();
  await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
  const expected = new Map<number, number>();
  const history: Snapshot[] = [{ rows: [] }];
  const snapshot = (): Snapshot => ({
    rows: [...expected.entries()].sort((a, b) => a[0] - b[0]).map(([id, v]) => ({ id, v })),
  });
  const failures: string[] = [];
  for (let i = 1; i <= 36; i += 1) {
    await db.execute(`INSERT INTO t VALUES (${String(i)}, ${String(i * 10)})`);
    expected.set(i, i * 10);
    history.push(snapshot());
    if (i % 3 === 0) {
      await db.execute(`UPDATE t SET v = v + 1 WHERE id = ${String(i - 1)}`);
      expected.set(i - 1, (expected.get(i - 1) ?? 0) + 1);
      history.push(snapshot());
    }
    if (i % 5 === 0) {
      await db.execute(`DELETE FROM t WHERE id = ${String(i - 2)}`);
      expected.delete(i - 2);
      history.push(snapshot());
    }
    // Tab death, then the device loses power before the OS wrote back its cache.
    store._crashForTests();
    await db.close().catch(() => undefined);
    model.powerLoss(keepPrefix ? (n) => Math.floor(rng() * (n + 1)) : undefined);
    ({ store, db } = await open());
    let rows: Array<{ id: number; v: number }>;
    try {
      rows = (await db.query("SELECT id, v FROM t ORDER BY id")).rows as typeof rows;
    } catch (error) {
      // Relaxed mode may roll the CREATE TABLE itself back; that is the empty prefix state.
      if (durability !== "relaxed" || !String(error).includes("Unknown table")) throw error;
      rows = [];
      await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    }
    const integrity = await store.checkIntegrity({ mode: "full" });
    if (!integrity.ok)
      failures.push(`step ${String(i)}: integrity ${JSON.stringify(integrity.issues)}`);
    if (durability === "strict") {
      if (JSON.stringify(rows) !== JSON.stringify(snapshot().rows)) {
        failures.push(
          `step ${String(i)}: lost acknowledged writes; got ${JSON.stringify(rows)} want ${JSON.stringify(snapshot().rows)}`,
        );
      }
    } else {
      const matches = history.some((h) => JSON.stringify(h.rows) === JSON.stringify(rows));
      if (!matches) failures.push(`step ${String(i)}: not a prefix state ${JSON.stringify(rows)}`);
      // Relaxed may roll back; resynchronize the oracle to what survived so the loop continues.
      expected.clear();
      for (const row of rows) expected.set(row.id, row.v);
      history.length = 0;
      history.push(snapshot());
    }
  }
  await db.close();
  store.close();
  return failures;
}

describe("power loss: strict durability", () => {
  it("every acknowledged SQL statement survives loss of every unflushed byte", async () => {
    expect(await sqlLoop("strict", false, 1)).toEqual([]);
  });

  it("every acknowledged SQL statement survives a torn (partially written-back) suffix", async () => {
    expect(await sqlLoop("strict", true, 2)).toEqual([]);
  });

  it("a multi-extent stage (seal mid-batch) and its commit survive power loss", async () => {
    const shim = new MemoryOpfs();
    const model = new PowerLossModel(shim);
    const name = "pl-strict-seal";
    let store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    await store.addTable(table("wide"));
    const transactionId = await beginTransaction(store, "seal");
    const blocks = await Promise.all(
      Array.from({ length: 40 }, (_, index) => bigBlock(`seal-${String(index)}`, 300 * 1024)),
    );
    const staged = await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks,
      segments: [
        {
          id: "seal-segment",
          tableId: "table-wide",
          transactionId,
          rowCount: 40,
          rowIdStart: 1n,
          rowIdEndExclusive: 41n,
          columnBlockIds: { c1: blocks.map((block) => block.id) },
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    // The batch crossed the 8 MiB seal: two extents exist.
    expect(shim.readFileBytes(`minnowdb/${name}/extents/000001`)).toBeDefined();
    store._crashForTests();
    model.powerLoss();
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    for (const block of blocks) {
      const read = await store.getBlock(block.id);
      expect(read !== undefined && bytesEqual(read, block.bytes)).toBe(true);
    }
    expect((await store.getTransaction(transactionId))?.revision).toBe(staged.revision);
    const committed = await store.commitTransaction({
      transactionId,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "table-wide", limit: 4096 }],
      committedAt: "2026-08-24T00:00:03.000Z",
    });
    store._crashForTests();
    model.powerLoss();
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    expect(await store.getCurrentManifestVersion()).toBe(committed.version);
    expect((await store.checkIntegrity({ mode: "full" })).ok).toBe(true);
    store.close();
  });

  it("GC-driven extent relocation survives power loss with every live block intact", async () => {
    const shim = new MemoryOpfs();
    const model = new PowerLossModel(shim);
    const name = "pl-strict-relocate";
    let store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    await store.addTable(table("retired"));
    await store.addTable(table("retained"));
    const transactionId = await beginTransaction(store, "gc");
    // 30 x 300 KiB retired + 6 x 300 KiB retained: extent 0 seals at 8 MiB and ends < 50% live.
    const retired = await Promise.all(
      Array.from({ length: 30 }, (_, i) => bigBlock(`retired-${String(i)}`, 300 * 1024)),
    );
    const retained = await Promise.all(
      Array.from({ length: 6 }, (_, i) => bigBlock(`retained-${String(i)}`, 300 * 1024)),
    );
    const segment = (id: string, tableId: string, ids: string[], ordinalIndex: number) => ({
      id,
      kind: "insert" as const,
      level: 0,
      logicalOrder: 0,
      commitOrdinal: ordinalIndex,
      rowIdSpans: [],
      tableId,
      transactionId,
      rowCount: ids.length,
      rowIdStart: 1n,
      rowIdEndExclusive: BigInt(ids.length + 1),
      columnBlockIds: { c1: ids },
      createdAt: "2026-08-24T00:00:01.000Z",
    });
    // Interleave so retained blocks sit inside the sealed extent too.
    const interleaved = [...retained.slice(0, 3), ...retired, ...retained.slice(3)];
    let revision = 0;
    for (let start = 0; start < interleaved.length; start += 20) {
      const batch = interleaved.slice(start, start + 20);
      const last = start + 20 >= interleaved.length;
      const updated = await store.stageTransactionArtifacts({
        transactionId,
        expectedRevision: revision,
        blocks: batch,
        segments: last
          ? [
              segment(
                "seg-retired",
                "table-retired",
                retired.map((b) => b.id),
                0,
              ),
              segment(
                "seg-retained",
                "table-retained",
                retained.map((b) => b.id),
                1,
              ),
            ]
          : [],
        updatedAt: "2026-08-24T00:00:02.000Z",
      });
      revision = updated.revision;
    }
    const first = await store.commitTransaction({
      transactionId,
      expectedTransactionRevision: revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [
        { tableId: "table-retired", limit: 4096 },
        { tableId: "table-retained", limit: 4096 },
      ],
      committedAt: "2026-08-24T00:00:03.000Z",
    });
    await store.dropTable({
      tableId: "table-retired",
      expectedTableRevision: 0,
      expectedManifestVersion: first.version,
      expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
      committedAt: "2026-08-24T00:00:05.000Z",
    });
    const job = await store.createGarbageCollectionJob({
      id: "gc-1",
      candidateManifestVersions: [first.version],
      candidateSegmentIds: [],
      candidateBlockIds: retired.map((b) => b.id),
      leaseCutoff: "2026-08-24T00:01:00.000Z",
      createdAt: "2026-08-24T00:01:00.000Z",
    });
    let step = await store.runGarbageCollectionStep({
      jobId: "gc-1",
      expectedRevision: job.revision,
      maxItems: 1000,
      updatedAt: "2026-08-24T00:01:01.000Z",
    });
    for (let guard = 0; step.job.state !== "completed" && guard < 20; guard += 1) {
      step = await store.runGarbageCollectionStep({
        jobId: "gc-1",
        expectedRevision: step.job.revision,
        maxItems: 1000,
        updatedAt: "2026-08-24T00:01:02.000Z",
      });
    }
    expect(step.job.state).toBe("completed");
    const statsBefore = await store.getStorageStats();
    store._crashForTests();
    model.powerLoss();
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    for (const block of retained) {
      const read = await store.getBlock(block.id);
      expect(read !== undefined && bytesEqual(read, block.bytes)).toBe(true);
    }
    const integrity = await store.checkIntegrity({ mode: "full" });
    expect(integrity.issues).toEqual([]);
    const statsAfter = await store.getStorageStats();
    expect(statsAfter.liveBlockCount).toBe(statsBefore.liveBlockCount);
    store.close();
  });

  it("framed snapshot export and import sessions survive power loss at every batch", async () => {
    const shim = new MemoryOpfs();
    const model = new PowerLossModel(shim);
    const name = "pl-strict-snapshot-source";
    let source = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    const db = new MinnowDatabase(source, { autoCompact: false, rowsPerBlock: 2 });
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR)");
    for (let i = 1; i <= 12; i += 1) {
      await db.execute(`INSERT INTO t VALUES (${String(i)}, 'row-${String(i)}')`);
    }
    await db.close();
    const now = Date.now();
    const clock = {
      createdAt: new Date(now).toISOString(),
      expiresAt: new Date(now + 30 * 60 * 1000).toISOString(),
    };
    const ownerId = "backup-owner";
    const session = await source.beginSnapshotFrameExport({ ownerId, ...clock });
    const frameCount = Object.values(session.header.kinds).reduce(
      (total, summary) => total + summary.frameCount,
      0,
    );
    const frames: SnapshotFrame[] = [];
    let checksum = 0;
    let itemCount = 0;
    let storedBytes = 0;
    for (let sequence = 0; sequence < frameCount; sequence += 1) {
      const frame = await source.readSnapshotExportFrame({
        sessionId: session.sessionId,
        ownerId,
        sequence,
        expiresAtCutoff: clock.createdAt,
        expiresAt: clock.expiresAt,
      });
      if (frame === undefined) throw new Error("missing frame");
      frames.push(frame);
      checksum = extendSnapshotFrameStreamChecksum(checksum, snapshotFrameEnvelopeParts(frame));
      itemCount += frame.itemCount;
      storedBytes += frame.payload.byteLength;
      if (sequence % 4 === 1) {
        source._crashForTests();
        model.powerLoss();
        source = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
        // The session must be resumable: replaying the last frame returns identical bytes.
        const replay = await source.readSnapshotExportFrame({
          sessionId: session.sessionId,
          ownerId,
          sequence,
          expiresAtCutoff: clock.createdAt,
          expiresAt: clock.expiresAt,
        });
        expect(replay !== undefined && bytesEqual(replay.payload, frame.payload)).toBe(true);
      }
    }
    await source.closeSnapshotFrameExport({ sessionId: session.sessionId, ownerId });
    const footer = { frameCount, itemCount, storedBytes, checksum };
    const header = session.header;
    source.close();

    const targetName = "pl-strict-snapshot-target";
    let target = await OpfsBlockStore.open({
      name: targetName,
      root: shim.root,
      checkpointEntries: 1_000,
    });
    const identity = snapshotFrameStreamHeaderIdentity(header);
    await target.beginSnapshotFrameImport({ identity, ownerId, header, ...clock });
    const batches: SnapshotFrame[][] = [];
    for (const frame of frames) {
      const last = batches[batches.length - 1];
      if (
        frame.kind !== "block" &&
        last !== undefined &&
        last.every((f) => f.kind !== "block") &&
        last.length < 3
      ) {
        last.push(frame);
      } else {
        batches.push([frame]);
      }
    }
    for (const batch of batches) {
      const result = await target.appendSnapshotImportFrames({
        identity,
        ownerId,
        expiresAtCutoff: clock.createdAt,
        expiresAt: clock.expiresAt,
        frames: batch,
      });
      target._crashForTests();
      model.powerLoss();
      target = await OpfsBlockStore.open({
        name: targetName,
        root: shim.root,
        checkpointEntries: 1_000,
      });
      const resumed = await target.beginSnapshotFrameImport({
        identity,
        ownerId,
        header,
        ...clock,
      });
      expect(resumed.nextSequence).toBe(result.nextSequence);
      expect((await target.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    }
    await target.finishSnapshotFrameImport({
      identity,
      ownerId,
      expiresAtCutoff: clock.createdAt,
      footer,
    });
    target._crashForTests();
    model.powerLoss();
    target = await OpfsBlockStore.open({
      name: targetName,
      root: shim.root,
      checkpointEntries: 1_000,
    });
    const restored = new MinnowDatabase(target, { autoCompact: false, rowsPerBlock: 2 });
    expect((await restored.query("SELECT COUNT(*) AS n FROM t")).rows).toEqual([{ n: 12 }]);
    await restored.close();
    expect((await target.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    target.close();
  });

  it("a temp-page reservation whose file was never flushed is reconciled, not reported", async () => {
    const shim = new MemoryOpfs();
    const model = new PowerLossModel(shim);
    const name = "pl-strict-temp";
    let store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    await store.createTempOwner({
      ownerId: "owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      revision: 0,
    });
    await store.putTempRunPages([
      { ownerId: "owner", runId: "run", pageIndex: 0, bytes: new Uint8Array(4096).fill(7) },
    ]);
    store._crashForTests();
    model.powerLoss();
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    const integrity = await store.checkIntegrity({ mode: "full" });
    expect(integrity.issues).toEqual([]);
    store.close();
  });
});

describe("power loss: zero-filled tail (size persisted, data not)", () => {
  /**
   * The other classic post-power-loss artifact besides a truncated tail: the file's new size
   * reached the disk but the appended page did not, so the tail reads as zeros. The docs say a
   * crash leaves "either a truncated tail frame ... or a torn slot". The WAL reader treats a
   * zero-filled tail as a truncated tail (invisible), never as corruption.
   */
  it("strict: a zero-filled unflushed WAL tail reads as a truncated tail, not corruption", async () => {
    const shim = new MemoryOpfs();
    const model = new PowerLossModel(shim);
    const name = "pl-zero-tail";
    let store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    await store.addTable(table("durable"));
    // Now an append whose flush never happened: write the frame without flushing by faulting
    // the WAL flush (the writer truncates the frame back, so instead emulate the OS: keep the
    // bytes, then zero them at power loss).
    await store.addTable(table("in-flight")).catch(() => undefined);
    store._crashForTests();
    const wal = shim.readFileBytes(`minnowdb/${name}/wal`);
    const durable = model.durableBytes(`minnowdb/${name}/wal`);
    if (wal === undefined || durable === undefined) throw new Error("wal");
    // Emulate: the last frame's bytes were flushed (strict) — so take the durable prefix and
    // append a zero-filled region the size of one more frame that never made it.
    const zeroTail = new Uint8Array(durable.byteLength + 96);
    zeroTail.set(durable);
    shim.writeFileBytes(`minnowdb/${name}/wal`, zeroTail);
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    expect((await store.listTables()).map((t) => t.name)).toContain("durable");
    store.close();
  });
});

describe("power loss: torn checkpoint slot header (magic persisted, rest zero)", () => {
  /**
   * `writeSlot` truncates the older slot to zero and rewrites it. A power loss mid-write can
   * leave the slot's new size with only its first bytes persisted. If those bytes are exactly
   * the 8-byte magic and the version field reads as zero, that must read as a torn slot, not
   * as an unsupported format version (which would stop recovery before redundant-slot
   * fallback). The mirror slot is intact and the WAL was not reset, so nothing is lost.
   */
  it("strict: a torn slot header falls back to the intact mirror instead of refusing the database", async () => {
    const shim = new MemoryOpfs();
    const name = "pl-torn-slot-header";
    let store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 2 });
    await store.addTable(table("a"));
    await store.addTable(table("b")); // checkpoint generation 1 in both slots
    await store.addTable(table("c")); // WAL tail frame
    store._crashForTests();
    // Emulate the crash inside writeSlot(older slot) for generation 2: magic landed, the rest
    // of the page did not (reads as zeros), size already extended.
    const slotPath = `minnowdb/${name}/checkpoint-b`;
    const intact =
      shim.readFileBytes(slotPath) ?? shim.readFileBytes(`minnowdb/${name}/checkpoint-a`);
    if (intact === undefined) throw new Error("slot");
    const torn = new Uint8Array(intact.byteLength);
    torn.set(intact.subarray(0, 8));
    shim.writeFileBytes(slotPath, torn);
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 2 });
    expect((await store.listTables()).map((t) => t.name).sort()).toEqual(["a", "b", "c"]);
    store.close();
  });
});

describe("power loss: relaxed durability", () => {
  it("recovery accepts a consecutive verified prefix (all unflushed bytes lost)", async () => {
    expect(await sqlLoop("relaxed", false, 3)).toEqual([]);
  });

  it("recovery accepts a consecutive verified prefix (torn suffix survives partially)", async () => {
    expect(await sqlLoop("relaxed", true, 4)).toEqual([]);
  });

  it("a durable WAL frame whose extent payload was lost is rolled back, never half-applied", async () => {
    const shim = new MemoryOpfs();
    const model = new PowerLossModel(shim);
    const name = "pl-relaxed-payload";
    let store = await OpfsBlockStore.open({
      name,
      root: shim.root,
      durability: "relaxed",
      checkpointEntries: 1_000,
    });
    await store.addTable(table("r"));
    const transactionId = await beginTransaction(store, "relaxed");
    const block = await bigBlock("relaxed-block", 64 * 1024);
    await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks: [block],
      segments: [],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    store._crashForTests();
    // Keep the WAL entirely (as if its page was written back) but lose the extent bytes.
    model.powerLoss((unflushed, path) => (path.endsWith("/wal") ? unflushed : 0));
    store = await OpfsBlockStore.open({
      name,
      root: shim.root,
      durability: "relaxed",
      checkpointEntries: 1_000,
    });
    expect(await store.getBlock("relaxed-block")).toBeUndefined();
    const record = await store.getTransaction(transactionId);
    // Either the whole stage is gone (journal empty) or the transaction itself never made it.
    expect(record === undefined || record.pendingBlockIds.length === 0).toBe(true);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    store.close();
  });
});
