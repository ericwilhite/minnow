import { IDBFactory } from "fake-indexeddb";
import { afterEach, describe, expect, it } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import { compileQuery, executeRowQuery } from "./query.js";
import { toColumnarBatch } from "./batch.js";
import { pointReadTestHooks } from "./point-read.js";

const stores = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    create: async (): Promise<BlockStore> =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

afterEach(() => {
  pointReadTestHooks.disabled = false;
});

describe.each(stores)("keyed mutation and correlated-query correctness on $name", ({ create }) => {
  it("keeps a correlated keyed count within the selected rows' memory budget", async () => {
    const database = new MinnowDatabase(await create(), { autoCompact: false });
    try {
      await database.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, label TEXT, unused TEXT)",
      );
      await database.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)");
      await database.execute("CREATE INDEX orders_customer ON orders (customer_id)");
      const unused = "x".repeat(16_384);
      await database.insertBatch(
        "customers",
        Array.from({ length: 10_000 }, (_, id) => ({
          id,
          label: `customer-${String(id)}`,
          unused,
        })),
      );
      await database.insertBatch("orders", [
        { id: 1, customer_id: 42 },
        { id: 2, customer_id: 42 },
        { id: 3, customer_id: 43 },
        { id: 4, customer_id: null },
      ]);
      const sql =
        "SELECT c.id, c.label, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS n " +
        "FROM customers c WHERE c.id = ?";
      for (const id of [42, 43, 44, 10_001]) {
        const expected = await database.query(sql, { params: [id], memoize: false });
        let peak = 0;
        const bounded = await database.query(sql, {
          params: [id],
          memoize: false,
          executionMemoryBudgetBytes: 4096,
          spillToStorage: false,
          onStats: (stats) => {
            peak = stats.peakMemoryBytes;
          },
        });
        expect(bounded).toEqual(expected);
        expect(peak).toBeLessThanOrEqual(4096);
        expect(bounded.rows).toEqual(
          id === 10_001
            ? []
            : [{ id, label: `customer-${String(id)}`, n: id === 42 ? 2 : id === 43 ? 1 : 0 }],
        );
      }
    } finally {
      await database.close();
    }
  });

  it("preserves unmatched derived rows, other base predicates and self-join inputs", async () => {
    const database = new MinnowDatabase(await create(), { autoCompact: false });
    const rows = Array.from({ length: 80 }, (_, id) => ({
      id,
      parent: id - 1,
      label: `customer-${String(id)}`,
    }));
    const orders = [
      { id: 1, customer_id: 42 },
      { id: 2, customer_id: 42 },
      { id: 3, customer_id: 43 },
      { id: 4, customer_id: null },
    ];
    try {
      await database.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, parent INTEGER, label TEXT)",
      );
      await database.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)");
      await database.insertBatch("customers", rows);
      await database.insertBatch("orders", orders);
      const inputs = new Map<string, DatabaseRow[]>([
        ["customers", rows],
        ["orders", orders],
      ]);
      for (const sql of [
        "SELECT c.id, x.n FROM customers c LEFT JOIN (SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id) x ON c.id=x.customer_id WHERE c.id=44",
        "SELECT c.id, (SELECT COUNT(*) FROM orders o WHERE o.customer_id=c.id) AS n FROM customers c WHERE c.id=42 AND c.label='other'",
        "SELECT a.id, b.label, (SELECT COUNT(*) FROM orders o WHERE o.customer_id=a.id) AS n FROM customers a JOIN customers b ON a.parent=b.id WHERE a.id=43",
        "SELECT c.id, (SELECT COUNT(*) FROM orders o WHERE o.customer_id=c.id) AS n FROM customers c WHERE c.id IN (42,44,NULL) ORDER BY c.id",
      ]) {
        expect(await database.query(sql, { memoize: false })).toEqual(
          executeRowQuery(compileQuery(sql), inputs),
        );
      }
      await database.execute("UPDATE customers SET label='changed' WHERE id=42");
      await database.execute("DELETE FROM customers WHERE id=43");
      await database.execute("INSERT INTO customers VALUES (43,42,'replacement')");
      const sql =
        "SELECT c.id, c.label, (SELECT COUNT(*) FROM orders o WHERE o.customer_id=c.id) AS n FROM customers c WHERE c.id=?";
      expect((await database.query(sql, { params: [42], memoize: false })).rows).toEqual([
        { id: 42, label: "changed", n: 2 },
      ]);
      expect((await database.query(sql, { params: [43], memoize: false })).rows).toEqual([
        { id: 43, label: "replacement", n: 1 },
      ]);
      await database.compactTable("customers");
      expect((await database.query(sql, { params: [43], memoize: false })).rows).toEqual([
        { id: 43, label: "replacement", n: 1 },
      ]);
    } finally {
      await database.close();
    }
  });

  it("evaluates keyed UPDATE assignments exactly like forced ordinary execution", async () => {
    const run = async (disabled: boolean) => {
      pointReadTestHooks.disabled = disabled;
      const database = new MinnowDatabase(await create(), { autoCompact: false });
      const outcomes: unknown[] = [];
      try {
        await database.execute(
          "CREATE TABLE items (id INTEGER PRIMARY KEY, amount REAL NOT NULL CHECK (amount>=0), label TEXT, flag BOOLEAN)",
        );
        await database.execute(
          "CREATE TABLE audit (item INTEGER, before_value REAL, after_value REAL)",
        );
        await database.insertBatch(
          "items",
          Array.from({ length: 80 }, (_, id) => ({
            id,
            amount: id + 1,
            label: `label-${String(id)}`,
            flag: id % 2 === 0,
          })),
        );
        await database.execute(
          "CREATE TRIGGER changed AFTER UPDATE ON items BEGIN INSERT INTO audit VALUES (NEW.id,OLD.amount,NEW.amount); END",
        );
        const servedBefore = pointReadTestHooks.served;
        for (const sql of [
          "UPDATE items SET amount=amount+1 WHERE id=42 RETURNING id,amount",
          "UPDATE items SET amount=amount*2, label=UPPER(label) WHERE id=42 RETURNING id,amount,label",
          "UPDATE items SET amount=amount-1000 WHERE id=42",
          "UPDATE items SET amount=amount/0 WHERE id=42",
          "UPDATE items SET amount=missing_column WHERE id=999",
          "UPDATE items SET amount=amount+1 WHERE id=42 AND label='other'",
          "UPDATE items SET amount=amount+1 WHERE id=999",
          "DELETE FROM items WHERE id=42",
          "INSERT INTO items VALUES (42,10,'replacement',FALSE)",
          "UPDATE items SET amount=amount+3 WHERE id=42 RETURNING id,amount",
        ]) {
          try {
            const result = await database.execute(sql);
            outcomes.push(
              result.kind === "insert" || result.kind === "update" || result.kind === "delete"
                ? {
                    rowCount: result.rowCount,
                    returnedRows: result.returnedRows,
                    returnedColumns: result.returnedColumns,
                    returnedColumnDomains: result.returnedColumnDomains,
                  }
                : result.kind,
            );
          } catch (error) {
            outcomes.push({ error: (error as Error).name, message: (error as Error).message });
          }
        }
        if (!disabled) expect(pointReadTestHooks.served).toBeGreaterThan(servedBefore);
        outcomes.push(
          (await database.query("SELECT * FROM items ORDER BY id", { memoize: false })).rows,
        );
        outcomes.push(
          (
            await database.query("SELECT * FROM audit ORDER BY item,before_value,after_value", {
              memoize: false,
            })
          ).rows,
        );
        return outcomes;
      } finally {
        await database.close();
      }
    };
    const fast = await run(false);
    expect(fast).toEqual(await run(true));
    expect(fast[0]).toMatchObject({ rowCount: 1, returnedRows: [{ id: 42, amount: 44 }] });
    expect(fast[1]).toMatchObject({
      rowCount: 1,
      returnedRows: [{ id: 42, amount: 88, label: "LABEL-42" }],
    });
    expect(fast[2]).toHaveProperty("error");
    expect(fast[3]).toHaveProperty("error");
    expect(fast[4]).toHaveProperty("error");
    expect(fast[5]).toMatchObject({ rowCount: 0 });
    expect(fast[6]).toMatchObject({ rowCount: 0 });
  });

  it("updates a __proto__ column through the keyed preparation path", async () => {
    const database = new MinnowDatabase(await create(), { autoCompact: false });
    try {
      await database.execute(
        'CREATE TABLE odd (id INTEGER PRIMARY KEY, "__proto__" REAL NOT NULL DEFAULT 9 CHECK ("__proto__">=0))',
      );
      await database.execute("INSERT INTO odd VALUES (1,9)");
      await database.execute("INSERT INTO odd (id) VALUES (2)");
      await database.insertBatch(
        "odd",
        toColumnarBatch({
          columns: { id: [3], ["__proto__"]: undefined },
          rowCount: 1,
        }),
      );
      await database.insertBatch(
        "odd",
        toColumnarBatch({
          columns: { id: [4], ["__proto__"]: [19], absent: undefined },
          rowCount: 1,
        }),
      );

      expect(
        (await database.query("SELECT id FROM odd ORDER BY id", { memoize: false })).rows,
        "after inserts",
      ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
      for (const expected of [10, 11, 12]) {
        if (expected === 12) {
          await database.compactTable("odd");
          expect(
            (await database.query("SELECT id FROM odd ORDER BY id", { memoize: false })).rows,
            "after compact",
          ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
        }
        const servedBefore = pointReadTestHooks.served;
        const result = await database.execute(
          'UPDATE odd SET "__proto__"="__proto__"+1 WHERE id=1 RETURNING "__proto__"',
        );
        expect(pointReadTestHooks.served).toBeGreaterThan(servedBefore);
        expect(result).toMatchObject({ rowCount: 1, returnedRows: [{ ["__proto__"]: expected }] });
        const fast = await database.query('SELECT "__proto__" FROM odd WHERE id=1', {
          memoize: false,
        });
        pointReadTestHooks.disabled = true;
        const ordinary = await database.query('SELECT "__proto__" FROM odd WHERE id=1', {
          memoize: false,
        });
        pointReadTestHooks.disabled = false;
        expect(fast).toEqual(ordinary);
        expect(fast.rows).toEqual([{ ["__proto__"]: expected }]);
      }

      await database.execute('UPDATE odd SET "__proto__"=20 WHERE id=4');
      expect(
        (await database.query("SELECT id FROM odd ORDER BY id", { memoize: false })).rows,
        "after constant update",
      ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
      await database.upsertBatch("odd", [{ id: 4, ["__proto__"]: 21 }]);
      await expect(database.execute('UPDATE odd SET "__proto__"=-1 WHERE id=4')).rejects.toThrow(
        /CHECK/u,
      );
      const all = await database.query('SELECT id,"__proto__" FROM odd ORDER BY id', {
        memoize: false,
      });
      expect(all.rows).toEqual([
        { id: 1, ["__proto__"]: 12 },
        { id: 2, ["__proto__"]: 9 },
        { id: 3, ["__proto__"]: 9 },
        { id: 4, ["__proto__"]: 21 },
      ]);
    } finally {
      await database.close();
    }
  });

  it("preserves a generated __proto__ column through scoped writes and rollback", async () => {
    const database = new MinnowDatabase(await create(), { autoCompact: false });
    try {
      await database.execute(
        'CREATE TABLE generated_names (id INTEGER PRIMARY KEY, value REAL NOT NULL, "__proto__" REAL GENERATED ALWAYS AS (value+1) STORED)',
      );
      await database.execute("INSERT INTO generated_names VALUES (1,9,DEFAULT)");
      await expect(
        database.execute("INSERT INTO generated_names VALUES (2,11,99)"),
      ).rejects.toThrow(/Generated column/u);

      await expect(
        database.execute("INSERT INTO generated_names VALUES (2,11,DEFAULT),(3,12,99)"),
      ).rejects.toThrow(/Generated column/u);
      await database.execute("UPDATE generated_names SET value=value+1 WHERE id=1");
      expect(
        (
          await database.query('SELECT value,"__proto__" FROM generated_names WHERE id=1', {
            memoize: false,
          })
        ).rows,
      ).toEqual([{ value: 10, ["__proto__"]: 11 }]);
      await expect(
        database.execute('UPDATE generated_names SET "__proto__"=0 WHERE id=1'),
      ).rejects.toThrow(/Generated column/u);
      await database.execute("BEGIN");
      await database.execute("UPDATE generated_names SET value=value+2 WHERE id=1");
      expect(
        (
          await database.query('SELECT value,"__proto__" FROM generated_names WHERE id=1', {
            memoize: false,
          })
        ).rows,
      ).toEqual([{ value: 12, ["__proto__"]: 13 }]);
      await database.execute("ROLLBACK");
      expect(
        (await database.query("SELECT id FROM generated_names ORDER BY id", { memoize: false }))
          .rows,
      ).toEqual([{ id: 1 }]);
      await database.compactTable("generated_names");
      expect(
        (
          await database.query('SELECT value,"__proto__" FROM generated_names WHERE id=1', {
            memoize: false,
          })
        ).rows,
      ).toEqual([{ value: 10, ["__proto__"]: 11 }]);
    } finally {
      await database.close();
    }
  });
});
