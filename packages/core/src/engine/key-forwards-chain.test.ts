/**
 * Stored key forwards end to end: a chain of rekeys resolves through every hop on every
 * connection, foreign keys follow, a retired key stays reserved until its forward expires and is
 * then free to reuse (one hop at a time, under a controlled clock), and the internal forwards
 * table stays compact and correct across hundreds of rekeys and a reopen.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { KEY_FORWARDS_TABLE } from "./key-forwards.js";
import { allVisibleSegments, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

/** A map entry the model guarantees is present. */
function must<T>(value: T | undefined, what: string): T {
  if (value === undefined) throw new Error(`${what} is missing`);
  return value;
}

const outcome = (p: Promise<unknown>): Promise<string> =>
  p.then(
    () => "ok",
    (error: unknown) => `ERR ${String(error).slice(0, 120)}`,
  );

describe("key forwards", () => {
  it("chain, partial expiry, full expiry and key reuse", async () => {
    let clock = Date.parse("2026-01-01T00:00:00Z");
    const store = new MemoryBlockStore();
    const opts = { keyForwardRetentionMs: 1_000, now: () => new Date(clock) };
    let db = new MinnowDatabase(store, opts);
    const peer = new MinnowDatabase(store, opts);
    const rows = async (conn: MinnowDatabase, sql: string): Promise<unknown> =>
      (await conn.query(sql, { memoize: false })).rows;
    try {
      await db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)");
      await db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))");
      await db.execute("INSERT INTO p VALUES (1, 'one'), (9, 'nine')");
      await db.execute("INSERT INTO c VALUES (1, 1)");
      await db.execute("UPDATE p SET id = 2 WHERE id = 1");
      clock += 600;
      await db.execute("UPDATE p SET id = 3 WHERE id = 2");
      // Both hops resolve, on both connections; the child followed; retired keys are reserved.
      expect(await rows(db, "SELECT * FROM p WHERE id = 1")).toEqual([{ id: 3, v: "one" }]);
      expect(await rows(peer, "SELECT * FROM p WHERE id = 2")).toEqual([{ id: 3, v: "one" }]);
      expect(await rows(db, "SELECT id FROM p WHERE id IN (1, 9) ORDER BY id")).toEqual([
        { id: 3 },
        { id: 9 },
      ]);
      expect(await rows(peer, "SELECT * FROM c")).toEqual([{ id: 1, pid: 3 }]);
      expect(await rows(db, "SELECT c.id, p.v FROM c JOIN p ON p.id = c.pid")).toEqual([
        { id: 1, v: "one" },
      ]);
      expect(await outcome(db.execute("INSERT INTO p VALUES (1, 'again')"))).toMatch(/Duplicate/);
      expect(await outcome(peer.execute("INSERT INTO p VALUES (2, 'again')"))).toMatch(/Duplicate/);
      // Only the first hop expires (it was written 600 ms before the second).
      clock += 700;
      await db.execute("UPDATE p SET v = 'tick' WHERE id = 9");
      await db.collectGarbage();
      expect(await rows(db, "SELECT * FROM p WHERE id = 1")).toEqual([]);
      expect(await rows(peer, "SELECT * FROM p WHERE id = 2")).toEqual([{ id: 3, v: "one" }]);
      await db.execute("INSERT INTO p VALUES (1, 'reused')");
      expect(await outcome(peer.execute("INSERT INTO p VALUES (2, 'again')"))).toMatch(/Duplicate/);
      // Everything expires; a reopened connection agrees.
      clock += 2_000;
      await db.execute("UPDATE p SET v = 'tock' WHERE id = 9");
      await db.collectGarbage();
      await db.close();
      db = new MinnowDatabase(store, opts);
      expect(await rows(db, "SELECT * FROM p WHERE id = 2")).toEqual([]);
      await db.execute("INSERT INTO p VALUES (2, 'reused')");
      expect(await rows(db, "SELECT id, v FROM p ORDER BY id")).toEqual([
        { id: 1, v: "reused" },
        { id: 2, v: "reused" },
        { id: 3, v: "one" },
        { id: 9, v: "tock" },
      ]);
      expect(await rows(peer, "SELECT v FROM p WHERE id = 1")).toEqual([{ v: "reused" }]);
      expect(await rows(peer, "SELECT * FROM c")).toEqual([{ id: 1, pid: 3 }]);
    } finally {
      await peer.close();
      await db.close();
    }
  });

  it("the forwards table stays compact and every hop resolves after hundreds of rekeys", async () => {
    const store = new MemoryBlockStore();
    let db = new MinnowDatabase(store, { rowsPerBlock: 16 });
    try {
      await db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY, v INTEGER)");
      await db.execute(
        `INSERT INTO p VALUES ${Array.from({ length: 50 }, (_, i) => `(${String(i + 1)}, ${String(i)})`).join(",")}`,
      );
      let next = 1000;
      const current = new Map<number, number>();
      for (let i = 1; i <= 50; i += 1) current.set(i, i);
      for (let round = 0; round < 300; round += 1) {
        const original = 1 + (round % 50);
        const from = must(current.get(original), "current key");
        const to = next++;
        await db.execute(`UPDATE p SET id = ${String(to)} WHERE id = ${String(from)}`);
        current.set(original, to);
      }
      // Automatic compaction folds the internal table as it folds any other.
      await new Promise((resolve) => setTimeout(resolve, 300));
      const before = (await allVisibleSegments(db, KEY_FORWARDS_TABLE)).length;
      expect(before).toBeLessThan(100);
      await db.close();
      db = new MinnowDatabase(store, { rowsPerBlock: 16 });
      for (const [original, cur] of current) {
        expect(
          (await db.query(`SELECT id, v FROM p WHERE id = ${String(original)}`, { memoize: false }))
            .rows,
        ).toEqual([{ id: cur, v: original - 1 }]);
        // A middle hop resolves too.
        expect(
          (
            await db.query(`SELECT id FROM p WHERE id = ${String(1000 + (original - 1))}`, {
              memoize: false,
            })
          ).rows,
        ).toEqual([{ id: cur }]);
      }
      expect((await db.query("SELECT COUNT(*) AS n FROM p", { memoize: false })).rows).toEqual([
        { n: 50 },
      ]);
    } finally {
      await db.close();
    }
  });
});
