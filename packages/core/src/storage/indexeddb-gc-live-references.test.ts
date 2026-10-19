/**
 * Garbage collection against live references on IndexedDB: a pinned older reader across DROP
 * COLUMN (packed and unpacked segments) and across compaction with a delta merge, a second
 * connection's open statement transaction during a pass, a late reader whose version is gone,
 * and a crash between a compaction's commit and its job's publish mark. Each scenario reads the
 * data back through the ordinary API, checks the adapter's integrity report after every pass,
 * and counts raw payloads so a collected-but-needed block cannot hide behind a cache.
 */
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import {
  ENGINE_OPTIONS,
  openEngine,
  readRawStore,
  reopenEngine,
  type OpenedEngine,
} from "./indexeddb-audit-helpers.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { SnapshotManifestMissingError } from "./types.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

async function blockCount(ctx: OpenedEngine): Promise<number> {
  return (await readRawStore(ctx.indexedDB, ctx.name, "blocks")).length;
}

async function expectClean(ctx: OpenedEngine, label: string): Promise<void> {
  expect((await ctx.store.checkIntegrity()).issues, label).toEqual([]);
}

async function sumB(db: MinnowDatabase, version?: number): Promise<{ n: number; s: number }> {
  const result = await db.query("SELECT COUNT(*) AS n, SUM(b) AS s FROM t", {
    memoize: false,
    ...(version === undefined ? {} : { version }),
  });
  return result.rows[0] as { n: number; s: number };
}

function readerLease(manifestVersion: number) {
  return {
    id: "old-reader",
    kind: "reader" as const,
    ownerId: "reader",
    manifestVersion,
    revision: 0,
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 120_000).toISOString(),
  };
}

describe("IndexedDB garbage collection against live references", () => {
  it("DROP COLUMN under a pinned older reader: dropped blocks stay until the pin lifts, packed blocks stay", async () => {
    const ctx = await openEngine();
    await ctx.db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT)");
    await ctx.db.insertBatch(
      "t",
      Array.from({ length: 40 }, (_, index) => ({
        id: index,
        a: `a${String(index)}`,
        b: index,
        c: `c${String(index)}`,
      })),
    );
    for (let id = 40; id < 46; id += 1) {
      // Small commits: packed segments.
      await ctx.db.execute(
        `INSERT INTO t (id, a, b, c) VALUES (${String(id)}, 'a${String(id)}', ${String(id)}, 'c${String(id)}')`,
      );
    }
    const segments = (await readRawStore(ctx.indexedDB, ctx.name, "segments")).map(
      (entry) => entry.value as { packedBlockId?: string },
    );
    expect(segments.some((segment) => segment.packedBlockId !== undefined)).toBe(true);
    const pinnedVersion = await ctx.store.getCurrentManifestVersion();
    if (pinnedVersion === null) throw new Error("no version");
    const before = await sumB(ctx.db);
    await ctx.store.createLease(readerLease(pinnedVersion));
    await ctx.db.execute("ALTER TABLE t DROP COLUMN c");
    await ctx.db.execute("INSERT INTO t (id, a, b) VALUES (100, 'a100', 100)");
    await ctx.db.collectGarbage();
    await expectClean(ctx, "after the first collection under the pin");
    expect(await sumB(ctx.db)).toEqual({ n: before.n + 1, s: before.s + 100 });
    // The rows still read at the pinned version (the catalog is not versioned, so the dropped
    // column itself is no longer addressable there).
    expect(await sumB(ctx.db, pinnedVersion)).toEqual(before);
    const blocksUnderPin = await blockCount(ctx);
    await ctx.store.removeLease({ id: "old-reader", ownerId: "reader" });
    await ctx.db.collectGarbage();
    await expectClean(ctx, "after collection once the pin lifted");
    expect(await blockCount(ctx)).toBeLessThan(blocksUnderPin);
    expect(await sumB(ctx.db)).toEqual({ n: before.n + 1, s: before.s + 100 });
    // Packed segments keep their one block, so the packed rows still read.
    const packedRows = await ctx.db.query(
      "SELECT id, a, b FROM t WHERE id BETWEEN 40 AND 45 ORDER BY id",
      { memoize: false },
    );
    expect(packedRows.rows).toHaveLength(6);
    const cold = await reopenEngine(ctx);
    expect(await sumB(cold.db)).toEqual({ n: before.n + 1, s: before.s + 100 });
    await expectClean(cold, "cold reopen");
    await cold.db.compactTable("t", { minimumLevel0Segments: 1 });
    await cold.db.collectGarbage();
    expect(await sumB(cold.db)).toEqual({ n: before.n + 1, s: before.s + 100 });
    await expectClean(cold, "after compaction and collection");
    await cold.db.close();
    cold.store.close();
  });

  it("compaction with a delta merge under a pinned reader, then a second connection's open transaction", async () => {
    const ctx = await openEngine();
    await ctx.db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
    for (let batch = 0; batch < 6; batch += 1) {
      await ctx.db.insertBatch(
        "t",
        Array.from({ length: 10 }, (_, index) => ({
          id: batch * 10 + index,
          a: `a${String(index)}`,
          b: batch * 10 + index,
        })),
      );
    }
    for (let id = 0; id < 60; id += 7) {
      await ctx.db.execute(`UPDATE t SET b = b + 1000 WHERE id = ${String(id)}`);
    }
    for (let id = 3; id < 60; id += 11)
      await ctx.db.execute(`DELETE FROM t WHERE id = ${String(id)}`);
    const expected = await sumB(ctx.db);
    const pinnedVersion = await ctx.store.getCurrentManifestVersion();
    if (pinnedVersion === null) throw new Error("no version");
    await ctx.store.createLease(readerLease(pinnedVersion));
    expect((await ctx.db.compactTable("t", { minimumLevel0Segments: 1 })).compacted).toBe(true);
    await ctx.db.collectGarbage();
    await expectClean(ctx, "after compaction and collection under the pin");
    expect(await sumB(ctx.db)).toEqual(expected);
    expect(await sumB(ctx.db, pinnedVersion)).toEqual(expected);
    // A second connection holds an open statement transaction with staged rows while the first
    // connection collects: the staged blocks are not reclaimable.
    const store2 = await IndexedDbBlockStore.open({ name: ctx.name, indexedDB: ctx.indexedDB });
    const db2 = new MinnowDatabase(store2, ENGINE_OPTIONS);
    await db2.execute("BEGIN");
    await db2.execute("INSERT INTO t (id, a, b) VALUES (500, 'x', 500)");
    await db2.execute("INSERT INTO t (id, a, b) VALUES (501, 'y', 501)");
    await ctx.store.removeLease({ id: "old-reader", ownerId: "reader" });
    await ctx.db.collectGarbage();
    await db2.execute("COMMIT");
    expect(await sumB(db2)).toEqual({ n: expected.n + 2, s: expected.s + 1001 });
    expect(await sumB(ctx.db)).toEqual({ n: expected.n + 2, s: expected.s + 1001 });
    await expectClean(ctx, "after the other connection committed");
    await ctx.db.collectGarbage();
    await expectClean(ctx, "after a further collection");
    // The pruned version is gone: a late reader fails typed, not wrong.
    const late = await ctx.db
      .query("SELECT COUNT(*) AS n FROM t", { memoize: false, version: pinnedVersion })
      .then(
        () => undefined,
        (error: unknown) => error,
      );
    expect(late).toBeInstanceOf(SnapshotManifestMissingError);
    await db2.close();
    store2.close();
    const cold = await reopenEngine(ctx);
    expect(await sumB(cold.db)).toEqual({ n: expected.n + 2, s: expected.s + 1001 });
    await expectClean(cold, "cold reopen");
    await cold.db.close();
    cold.store.close();
  });

  it("a crash between a compaction's commit and its job's publish mark is recovered, not cancelled", async () => {
    let ctx = await openEngine();
    await ctx.db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
    for (let batch = 0; batch < 4; batch += 1) {
      await ctx.db.insertBatch(
        "t",
        Array.from({ length: 10 }, (_, index) => ({
          id: batch * 10 + index,
          a: `a${String(index)}`,
          b: batch * 10 + index,
        })),
      );
    }
    const expected = await sumB(ctx.db);
    // Abort the first job-store write after the manifest pointer advanced.
    let committed = false;
    let fired = false;
    ctx.instrumented.setHook((info) => {
      if (info.storeName === "catalog" && info.key === "manifest/current") committed = true;
      if (committed && !fired && info.storeName === "gc" && info.method === "put") {
        fired = true;
        return "abort";
      }
      return undefined;
    });
    await ctx.db.compactTable("t", { minimumLevel0Segments: 1 }).catch(() => undefined);
    ctx.instrumented.setHook(undefined);
    expect(fired).toBe(true);
    ctx = await reopenEngine(ctx);
    expect(await sumB(ctx.db)).toEqual(expected);
    await expectClean(ctx, "after the crash and a cold reopen");
    await ctx.db.compactTable("t", { minimumLevel0Segments: 1 });
    await ctx.db.collectGarbage();
    expect(await sumB(ctx.db)).toEqual(expected);
    await expectClean(ctx, "after the resumed compaction and collection");
    await ctx.db.close();
    ctx.store.close();
  });
});
