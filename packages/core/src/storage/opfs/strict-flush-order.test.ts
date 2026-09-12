/**
 * Strict-mode flush ordering, observed at the file layer.
 *
 * The shim reports every write/truncate/create/flush with its path. For each acknowledged
 * operation this checks the documented order:
 *   every extent or snapshot-ledger byte written  ->  flushed  ->  WAL frame written  ->
 *   WAL flushed  ->  the operation resolves.
 * Concretely, at every WAL flush no extent/ledger file may be dirty, and at the moment an
 * operation resolves no WAL write may be pending a flush. Temp pages (reservation-then-file by
 * design) and checkpoint slots are excluded from the dirty rule.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore } from "./index.js";
import { MinnowDatabase } from "../../engine/database.js";
import { encodeBlock } from "../../block-format/index.js";
import type { TableRecord } from "../types.js";

interface Event {
  path: string;
  phase: "create" | "write" | "flush" | "resolve";
  op: string;
}

function checkOrder(events: readonly Event[]): string[] {
  const problems: string[] = [];
  const dirty = new Map<string, string>(); // path -> op that dirtied it
  let walDirtyBy: string | undefined;
  const isPayload = (path: string) => path.includes("/extents/") || path.includes("/snapshots-v1/");
  for (const event of events) {
    const isWal = event.path.endsWith("/wal");
    if (event.phase === "write") {
      // "create" is directory/file creation (no bytes); only byte writes dirty a file.
      if (isWal) walDirtyBy = event.op;
      else if (isPayload(event.path)) dirty.set(event.path, event.op);
    } else if (event.phase === "flush") {
      if (isWal) {
        walDirtyBy = undefined;
        if (dirty.size > 0) {
          problems.push(
            `${event.op}: WAL flushed while payload files were dirty: ${[...dirty.entries()].map(([p, o]) => `${p} (by ${o})`).join(", ")}`,
          );
        }
      } else {
        dirty.delete(event.path);
      }
    } else {
      // resolve
      if (walDirtyBy !== undefined) {
        problems.push(`${event.op}: resolved with an unflushed WAL write (by ${walDirtyBy})`);
      }
    }
  }
  return problems;
}

function table(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "string", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

describe("strict flush order", () => {
  it("every mutation path flushes payload before the WAL frame, and the frame before resolving", async () => {
    const shim = new MemoryOpfs();
    const events: Event[] = [];
    let currentOp = "open";
    shim.setWriteFault((path, phase) => {
      events.push({ path, phase, op: currentOp });
    });
    const name = "flush-order";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 5 });
    const run = async <T>(op: string, work: () => Promise<T>): Promise<T> => {
      currentOp = op;
      const result = await work();
      events.push({ path: "", phase: "resolve", op });
      currentOp = `after:${op}`;
      return result;
    };
    // 1. Store-level ops.
    await run("addTable", () => store.addTable(table("a")));
    await run("beginTransaction", () =>
      store.beginTransaction({
        record: {
          id: "tx",
          ownerId: "owner",
          expiresAt: "2026-08-24T01:00:00.000Z",
          pendingBlockIds: [],
          pendingSegmentIds: [],
          status: "active",
          revision: 0,
          startedAt: "2026-08-24T00:00:00.000Z",
          updatedAt: "2026-08-24T00:00:00.000Z",
          committedVersion: null,
        },
      }),
    );
    const blocks = await Promise.all(
      Array.from({ length: 40 }, async (_, i) => ({
        id: `b-${String(i)}`,
        bytes: await encodeBlock({ type: "string", values: ["x".repeat(300 * 1024)] }, "raw"),
      })),
    );
    const staged = await run("stage(40x300KiB, seals mid-batch)", () =>
      store.stageTransactionArtifacts({
        transactionId: "tx",
        expectedRevision: 0,
        blocks,
        segments: [
          {
            id: "seg",
            tableId: "table-a",
            transactionId: "tx",
            rowCount: 40,
            rowIdStart: 1n,
            rowIdEndExclusive: 41n,
            columnBlockIds: { c1: blocks.map((b) => b.id) },
            kind: "insert",
            level: 0,
            logicalOrder: 0,
            commitOrdinal: 0,
            rowIdSpans: [],
            createdAt: "2026-08-24T00:00:01.000Z",
          },
        ],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    );
    await run("commit", () =>
      store.commitTransaction({
        transactionId: "tx",
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "table-a", limit: 4096 }],
        committedAt: "2026-08-24T00:00:03.000Z",
      }),
    );
    await run("createTempOwner", () =>
      store.createTempOwner({
        ownerId: "owner",
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
        revision: 0,
      }),
    );
    await run("putTempRunPages", () =>
      store.putTempRunPages([
        { ownerId: "owner", runId: "run", pageIndex: 0, bytes: new Uint8Array(1024) },
      ]),
    );
    const now = Date.now();
    const clock = {
      createdAt: new Date(now).toISOString(),
      expiresAt: new Date(now + 30 * 60 * 1000).toISOString(),
    };
    const session = await run("beginSnapshotFrameExport", () =>
      store.beginSnapshotFrameExport({ ownerId: "backup", ...clock }),
    );
    await run("readSnapshotExportFrame", () =>
      store.readSnapshotExportFrame({
        sessionId: session.sessionId,
        ownerId: "backup",
        sequence: 0,
        expiresAtCutoff: clock.createdAt,
        expiresAt: clock.expiresAt,
      }),
    );
    await run("closeSnapshotFrameExport", () =>
      store.closeSnapshotFrameExport({ sessionId: session.sessionId, ownerId: "backup" }),
    );
    // 2. Engine paths (writeTransaction, splits, deletes, updates) plus forced checkpoints.
    const db = new MinnowDatabase(store, { autoCompact: false, rowsPerBlock: 2 });
    await run("CREATE TABLE", () =>
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR)"),
    );
    for (let i = 1; i <= 12; i += 1) {
      await run(`INSERT ${String(i)}`, () =>
        db.execute(`INSERT INTO t VALUES (${String(i)}, '${"v".repeat(2000)}')`),
      );
    }
    await run("UPDATE", () => db.execute("UPDATE t SET v = 'u' WHERE id <= 6"));
    await run("DELETE", () => db.execute("DELETE FROM t WHERE id > 9"));
    await run("scope", () =>
      db.write(async (tx) => {
        for (let i = 100; i < 140; i += 1) {
          await tx.execute(`INSERT INTO t VALUES (${String(i)}, 'scope')`);
        }
      }),
    );
    await db.close();
    store.close();
    // The trace must have seen real work: the WAL and at least one extent were flushed.
    expect(events.some((e) => e.path.endsWith("/wal") && e.phase === "flush")).toBe(true);
    expect(events.some((e) => e.path.includes("/extents/") && e.phase === "flush")).toBe(true);
    expect(checkOrder(events)).toEqual([]);
  });

  it("relaxed mode: payload and WAL frame are written before resolve (no flush promised)", async () => {
    const shim = new MemoryOpfs();
    const events: Event[] = [];
    let currentOp = "open";
    shim.setWriteFault((path, phase) => {
      events.push({ path, phase, op: currentOp });
    });
    const store = await OpfsBlockStore.open({
      name: "relaxed-order",
      root: shim.root,
      durability: "relaxed",
      checkpointEntries: 1_000,
    });
    currentOp = "addTable";
    await store.addTable(table("r"));
    events.push({ path: "", phase: "resolve", op: "addTable" });
    const walWrites = events.filter(
      (e) => e.op === "addTable" && e.path.endsWith("/wal") && e.phase === "write",
    );
    const walFlushes = events.filter(
      (e) => e.op === "addTable" && e.path.endsWith("/wal") && e.phase === "flush",
    );
    expect(walWrites.length).toBeGreaterThan(0);
    expect(walFlushes.length).toBe(0);
    store.close();
  });
});
