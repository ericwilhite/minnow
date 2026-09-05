import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";
import { QueryGenerations } from "./query-generations.js";
import type { Manifest } from "../storage/types.js";
import type { LiveQueryPatch } from "./live-patch.js";
import type { QueryRow } from "./query.js";

function manifest(
  version: number,
  previousVersion: number | null,
  changedTableIds: string[],
): Manifest {
  return {
    version,
    previousVersion,
    changedTableIds,
    createdAt: "2026-09-05T00:00:00.000Z",
    liveBlockCount: 0,
    liveBlockBytes: 0,
  };
}

describe("dependency generation cache proofs", () => {
  it("keeps unrelated generations, advances dependencies and handles old snapshots", async () => {
    const history = [manifest(2, 1, ["b"]), manifest(3, 2, ["a"])];
    const versions = new QueryGenerations(async (after) => ({
      records: history.filter((m) => m.version > (after ?? -1)),
      nextCursor: null,
    }));
    const original = await versions.key(["a"], 1);
    expect(await versions.key(["a"], 2)).toBe(original);
    expect(await versions.key(["a"], 3)).not.toBe(original);
    expect(await versions.key(["a"], 1)).not.toBe(await versions.key(["a"], 3));
  });
  it("fails closed on missing history and bounds generation state", async () => {
    const versions = new QueryGenerations(async () => ({
      records: [manifest(3, 2, ["b"])],
      nextCursor: null,
    }));
    const original = await versions.key(["a"], 1);
    expect(await versions.key(["a"], 3)).not.toBe(original);
    const bounded = new QueryGenerations(async () => ({
      records: [
        manifest(
          2,
          1,
          Array.from({ length: 4097 }, (_, i) => String(i)),
        ),
      ],
      nextCursor: null,
    }));
    expect(await bounded.key(["a"], 2)).not.toBe(await bounded.key(["a"], 1));
  });
  it("does not publish partially read generations when storage fails", async () => {
    let failure = true;
    const versions = new QueryGenerations(async (after) => {
      if (after === 1) return { records: [manifest(2, 1, ["a"])], nextCursor: 2 };
      if (failure) throw new Error("storage read failed");
      return { records: [manifest(3, 2, ["b"])], nextCursor: null };
    });
    const original = await versions.key(["a"], 1);
    await expect(versions.key(["a"], 3)).rejects.toThrow("storage read failed");
    expect(await versions.key(["a"], 1)).toBe(original);
    failure = false;
    expect(await versions.key(["a"], 3)).not.toBe(original);
  });
  it("reuses a real memo across unrelated writes and invalidates schema and data changes", async () => {
    const store = new MemoryBlockStore();
    const db = new MinnowDatabase(store);
    const peer = new MinnowDatabase(store);
    try {
      await db.execute("CREATE TABLE audit_memo_a (id INTEGER PRIMARY KEY, x INTEGER)");
      await db.execute("CREATE TABLE audit_memo_b (id INTEGER PRIMARY KEY)");
      await db.execute("INSERT INTO audit_memo_a VALUES (1,10)");
      const peaks: number[] = [];
      const query = () =>
        db.query("SELECT SUM(x) AS n FROM audit_memo_a", {
          onStats: (stats) => peaks.push(stats.peakMemoryBytes),
        });
      expect((await query()).rows).toEqual([{ n: 10 }]);
      await query();
      await peer.execute("INSERT INTO audit_memo_b VALUES (1)");
      expect((await query()).rows).toEqual([{ n: 10 }]);
      expect(peaks.slice(1)).toEqual([0, 0]);
      await peer.execute("UPDATE audit_memo_a SET x = 20");
      expect((await query()).rows).toEqual([{ n: 20 }]);
      expect(peaks.at(-1)).toBeGreaterThan(0);
      await peer.execute("ALTER TABLE audit_memo_a ADD COLUMN extra INTEGER");
      expect((await query()).rows).toEqual([{ n: 20 }]);
      expect(peaks.at(-1)).toBeGreaterThan(0);
    } finally {
      await peer.close();
      await db.close();
    }
  });
});

describe("bounded live maintenance", () => {
  it("rejects an over-budget opening and releases the group's retained state", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    const live = db.liveQueries({ maxRetainedBytes: 1 });
    try {
      await expect(live.subscribe("SELECT 1 AS v", { onChange: () => undefined })).rejects.toThrow(
        "modeled bytes",
      );
      expect(live.stats.retainedBytes).toBe(0);
      expect(() => db.liveQueries({ maxRetainedBytes: -1 })).toThrow();
    } finally {
      live.close();
      await db.close();
    }
  });
  it("delivers patch payloads that reconstruct exact rows without exposing retained objects", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await db.execute("CREATE TABLE audit_patch (id INTEGER PRIMARY KEY, label TEXT)");
    await db.execute("INSERT INTO audit_patch VALUES (1,'a'),(2,'b'),(3,'c')");
    const live = db.liveQueries();
    let rows: QueryRow[] = [];
    const patches: LiveQueryPatch[] = [];
    try {
      await live.subscribePatches("SELECT id,label FROM audit_patch ORDER BY id", {
        onPatch: (patch) => {
          patches.push(patch);
          if (patch.type === "reset") rows = patch.result.rows;
          else {
            const changed = new Map(patch.changedRows.map(({ index, row }) => [index, row]));
            rows = Array.from(patch.retained, (was, index) =>
              was >= 0 ? (rows[was] ?? {}) : (changed.get(index) ?? {}),
            );
          }
        },
      });
      const original = rows[0];
      await db.execute("UPDATE audit_patch SET label='changed' WHERE id=2");
      await live.refresh();
      expect(rows).toEqual((await db.query("SELECT id,label FROM audit_patch ORDER BY id")).rows);
      expect(rows[0]).toBe(original);
      const patch = patches.at(-1);
      expect(patch?.type).toBe("patch");
      if (patch?.type === "patch") expect(patch.changedRows).toHaveLength(1);
      const first = rows[0];
      if (first === undefined) throw new Error("Missing initial row");
      first.label = "consumer mutation";
      await db.execute("DELETE FROM audit_patch WHERE id=2");
      await live.refresh();
      expect((await db.query("SELECT label FROM audit_patch WHERE id=1")).rows).toEqual([
        { label: "a" },
      ]);
      expect(live.stats.retainedBytes).toBeGreaterThan(0);
    } finally {
      live.close();
      expect(live.stats.retainedBytes).toBe(0);
      await db.close();
    }
  });

  it("maintains exact numeric aggregate metadata and reversible removals", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await db.execute(
      "CREATE TABLE audit_live_sum (id INTEGER PRIMARY KEY, g TEXT, x NUMERIC(20,2))",
    );
    await db.execute(
      "INSERT INTO audit_live_sum VALUES (1,'a',10000000000000000),(2,'a',1),(3,'b',NULL)",
    );
    const live = db.liveQueries();
    let result: unknown;
    const sql =
      "SELECT g, COUNT(x) AS n, SUM(x) AS s, AVG(x) AS a FROM audit_live_sum GROUP BY g ORDER BY g";
    try {
      await live.subscribe(sql, {
        onChange: (next) => {
          result = next;
        },
      });
      expect(result).toEqual(await db.query(sql));
      for (const mutation of [
        "DELETE FROM audit_live_sum WHERE id=1",
        "UPDATE audit_live_sum SET g='b', x=2 WHERE id=2",
        "DELETE FROM audit_live_sum WHERE id=2",
        "DELETE FROM audit_live_sum WHERE id=3",
      ]) {
        await db.execute(mutation);
        await live.refresh();
        expect(result, mutation).toEqual(await db.query(sql));
      }
      expect(live.stats.maintained).toBeGreaterThan(0);
    } finally {
      live.close();
      await db.close();
    }
  });
});
