/**
 * A secondary-index-backed lookup inside a write scope sees the scope's own rows.
 *
 * Index lookups take a different read path from scans, and that path used to answer from the
 * committed segments alone, so a row the scope had inserted or moved was invisible to an
 * equality, IN, range, join, or subquery that went through the index. Every shape below is read
 * inside the scope — before and after a scan forces the pending set to stage — and compared
 * with the committed read after the scope, which is the oracle.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

/**
 * The scope inserts customer 40 at tier 4 and moves customer 1 from `from` to `moved`. Under
 * a unique index `moved` cannot also be 4, so the tiers depend on the mode.
 */
function reads(from: number, moved: number): Array<[string, string, unknown[]]> {
  return [
    [
      "literal equality on the inserted tier",
      "SELECT id FROM customers WHERE tier = 4 ORDER BY id",
      [],
    ],
    [
      "literal equality on the moved-to tier",
      `SELECT id FROM customers WHERE tier = ${String(moved)} ORDER BY id`,
      [],
    ],
    ["parameter equality", "SELECT id FROM customers WHERE tier = $1 ORDER BY id", [moved]],
    [
      "literal IN",
      `SELECT id FROM customers WHERE tier IN (3, 4, ${String(moved)}) ORDER BY id`,
      [],
    ],
    [
      "parameter IN",
      "SELECT id FROM customers WHERE tier IN ($1, $2, $3) ORDER BY id",
      [3, 4, moved],
    ],
    ["moved-away literal", `SELECT id FROM customers WHERE tier = ${String(from)} ORDER BY id`, []],
    ["range", "SELECT id FROM customers WHERE tier >= 3 AND tier <= 5 ORDER BY id", []],
    [
      "join with indexed lookup side",
      `SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.tier = ${String(moved)} ORDER BY o.id`,
      [],
    ],
    [
      "subquery over the index",
      "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tier = 4) ORDER BY id",
      [],
    ],
    [
      "count over the index",
      `SELECT COUNT(*) AS n FROM customers WHERE tier = ${String(moved)}`,
      [],
    ],
  ];
}

describe.each(implementations)("index lookups inside a scope on $name", ({ create }) => {
  it.each([false, true])(
    "see the scope's inserts and moves through a unique=%s index",
    async (unique) => {
      const from = unique ? 100 : 2;
      const moved = unique ? 5 : 4;
      const cases = reads(from, moved);
      for (const rowsPerBlock of [8, 65_536]) {
        const db = new MinnowDatabase(await create(), { rowsPerBlock });
        await db.execute(
          "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, tier INTEGER NOT NULL)",
        );
        await db.execute(
          "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL)",
        );
        await db.execute(
          `CREATE ${unique ? "UNIQUE " : ""}INDEX customers_tier ON customers (tier)`,
        );
        // Enough committed rows that the table spans several blocks at rowsPerBlock 8.
        const values = Array.from(
          { length: 30 },
          (_, i) =>
            `(${String(i + 1)}, 'n${String(i + 1)}', ${String(unique ? 100 + i : 2 + (i % 2))})`,
        ).join(", ");
        await db.execute(`INSERT INTO customers (id, name, tier) VALUES ${values}`);
        await db.execute("INSERT INTO orders (id, customer_id) VALUES (1, 1), (2, 2)");
        const inScope = new Map<string, string>();
        const flushedInScope = new Map<string, string>();
        await db.write(async (tx) => {
          await tx.execute("INSERT INTO customers (id, name, tier) VALUES (40, 'new', 4)");
          await tx.execute(`UPDATE customers SET tier = ${String(moved)} WHERE id = 1`);
          await tx.execute("INSERT INTO orders (id, customer_id) VALUES (3, 40)");
          for (const [label, sql, params] of cases) {
            inScope.set(
              label,
              JSON.stringify((await tx.query(sql, { params: params as never })).rows),
            );
          }
          // Force everything staged and read again.
          await tx.query("SELECT COUNT(*) AS n FROM customers");
          for (const [label, sql, params] of cases) {
            flushedInScope.set(
              label,
              JSON.stringify((await tx.query(sql, { params: params as never })).rows),
            );
          }
        });
        const committed = new Map<string, string>();
        for (const [label, sql, params] of cases) {
          committed.set(
            label,
            JSON.stringify((await db.query(sql, { params: params as never, memoize: false })).rows),
          );
        }
        await db.close();
        const describe_ = (view: Map<string, string>): string[] =>
          [...committed]
            .filter(([label, expected]) => view.get(label) !== expected)
            .map(
              ([label, expected]) =>
                `${label} (rowsPerBlock ${String(rowsPerBlock)}): scope ${view.get(label) ?? "?"} vs committed ${expected}`,
            );
        expect(describe_(inScope), "pending").toEqual([]);
        expect(describe_(flushedInScope), "staged").toEqual([]);
        // The moved row is visible under its new tier and gone from its old one.
        const ids = (label: string): number[] =>
          (JSON.parse(committed.get(label) ?? "[]") as Array<{ id: number }>).map((r) => r.id);
        expect(ids("moved-away literal")).not.toContain(1);
        expect(ids("literal equality on the moved-to tier")).toContain(1);
        expect(ids("literal equality on the inserted tier")).toContain(40);
      }
    },
  );
});
