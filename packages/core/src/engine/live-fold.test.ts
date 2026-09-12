/**
 * Live queries maintained incrementally handle a scope's *folded* commit — an insert+update
 * folded into one inserted row, a delete+insert folded into an upsert segment, an update+delete
 * folded into a delete, an insert+delete that leaves nothing — the same as the unfolded
 * statement sequence would: at most one delivery per commit, rows equal to a fresh execution,
 * and consistent retained-row provenance.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase, type WriteSession } from "./database.js";
import { type QueryResult } from "./query.js";

async function fixture(): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 16 });
  await db.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL, label TEXT NOT NULL)",
  );
  const values = Array.from(
    { length: 40 },
    (_, i) => `(${String(i + 1)}, ${String((i * 7) % 100)}, 'L${String(i % 5)}')`,
  ).join(", ");
  await db.execute(`INSERT INTO items (id, amount, label) VALUES ${values}`);
  return db;
}

const SHAPES = [
  "SELECT id, amount, label FROM items WHERE amount > 20 ORDER BY id",
  "SELECT id, amount FROM items ORDER BY amount DESC, id LIMIT 8",
  "SELECT COUNT(*) AS n, SUM(amount) AS total FROM items",
  "SELECT label, COUNT(*) AS n, SUM(amount) AS total FROM items GROUP BY label ORDER BY label",
];

const SCOPES: Array<[string, (tx: WriteSession) => Promise<void>]> = [
  [
    "insert then update (folds to one insert)",
    async (tx) => {
      await tx.insertBatch("items", [{ id: 41, amount: 1, label: "L1" }]);
      await tx.updateBatch("items", { keys: [41], changes: { amount: [95] } });
    },
  ],
  [
    "delete then insert of a committed key (folds to an upsert segment)",
    async (tx) => {
      await tx.deleteBatch("items", { keys: [5] });
      await tx.insertBatch("items", [{ id: 5, amount: 99, label: "L9" }]);
    },
  ],
  [
    "update then delete (folds to a delete)",
    async (tx) => {
      await tx.updateBatch("items", { keys: [6], changes: { amount: [1_000] } });
      await tx.deleteBatch("items", { keys: [6] });
    },
  ],
  [
    "insert then delete (folds to nothing)",
    async (tx) => {
      await tx.insertBatch("items", [{ id: 42, amount: 1_000, label: "L0" }]);
      await tx.deleteBatch("items", { keys: [42] });
      await tx.updateBatch("items", { keys: [7], changes: { label: ["L4"] } });
    },
  ],
  [
    "upsert then update, and a keyed constant update on a committed row",
    async (tx) => {
      await tx.upsertBatch("items", [{ id: 8, amount: 50, label: "L2" }]);
      await tx.updateBatch("items", { keys: [8], changes: { amount: [51] } });
      await tx.execute("UPDATE items SET amount = 97 WHERE id IN (9, 10)");
      await tx.execute("DELETE FROM items WHERE id = 11");
    },
  ],
  [
    "many folds in one scope crossing a block boundary",
    async (tx) => {
      for (let i = 0; i < 40; i += 1) {
        await tx.execute("UPDATE items SET amount = $1 WHERE id = $2", [
          (i * 13) % 100,
          1 + (i % 20),
        ]);
      }
      for (let i = 0; i < 20; i += 1) {
        await tx.insertBatch("items", [{ id: 100 + i, amount: 30 + i, label: "L3" }]);
      }
      for (let i = 0; i < 10; i += 1) await tx.deleteBatch("items", { keys: [100 + i * 2] });
    },
  ],
];

describe("live maintenance over folded scope commits", () => {
  it("delivers once per commit, equal to fresh execution, with consistent provenance", async () => {
    const db = await fixture();
    const live = db.liveQueries({ sharedResults: true });
    const deliveries = new Map<
      string,
      Array<{ result: QueryResult; retained: Int32Array | undefined }>
    >();
    for (const sql of SHAPES) {
      deliveries.set(sql, []);
      await live.subscribe(sql, {
        onChange: (result, delivery) =>
          deliveries.get(sql)?.push({ result, retained: delivery.retained }),
      });
    }
    const failures: string[] = [];
    for (const [name, body] of SCOPES) {
      const before = new Map([...deliveries].map(([sql, list]) => [sql, list.length]));
      const statsBefore = live.stats;
      await db.write(body);
      await live.refresh();
      for (const sql of SHAPES) {
        const list = deliveries.get(sql) ?? [];
        const added = list.length - (before.get(sql) ?? 0);
        const fresh = (await db.query(sql, { memoize: false })).rows;
        const last = list.at(-1);
        const prev = list.at(-2);
        // A commit that leaves the result unchanged delivers nothing; more than one delivery
        // for one commit would be a double invalidation.
        if (added > 1) failures.push(`${name} / ${sql}: ${String(added)} deliveries`);
        if (JSON.stringify(last?.result.rows) !== JSON.stringify(fresh)) {
          failures.push(
            `${name} / ${sql}\n  live:  ${JSON.stringify(last?.result.rows)}\n  fresh: ${JSON.stringify(fresh)}`,
          );
        }
        if (added === 1 && last?.retained !== undefined && prev !== undefined) {
          for (const [index, from] of [...last.retained].entries()) {
            if (from === -1) continue;
            if (
              JSON.stringify(last.result.rows[index]) !== JSON.stringify(prev.result.rows[from])
            ) {
              failures.push(
                `${name} / ${sql}: retained[${String(index)}]=${String(from)} names a different row`,
              );
            }
          }
        }
      }
      const statsAfter = live.stats;
      if (
        statsAfter.maintained === statsBefore.maintained &&
        statsAfter.reruns === statsBefore.reruns
      ) {
        failures.push(`${name}: no maintenance or rerun counted`);
      }
    }
    // The row-local shape must actually have been patched, not re-executed, for most commits.
    const rowLocal = live.stats.groups.find((g) => g.sql === SHAPES[0]);
    if (rowLocal === undefined || !rowLocal.maintainable || rowLocal.maintained < 4) {
      failures.push(`row-local shape not maintained: ${JSON.stringify(rowLocal)}`);
    }
    live.close();
    await db.close();
    expect(failures, failures.join("\n\n")).toEqual([]);
  });
});
