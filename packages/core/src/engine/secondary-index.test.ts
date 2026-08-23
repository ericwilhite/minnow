import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { UniqueConstraintError } from "./errors.js";
import { compileStatement } from "./query.js";
import { collectFtsPostings } from "../storage/types.js";

function implementations(): Array<{ name: string; create: () => Promise<BlockStore> }> {
  return [
    { name: "memory", create: async () => new MemoryBlockStore() },
    {
      name: "indexeddb",
      create: async () =>
        IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
    },
    {
      name: "opfs",
      create: async () =>
        OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
    },
  ];
}

function sharedImplementations(): Array<{
  name: string;
  create: () => Promise<[BlockStore, BlockStore]>;
}> {
  return [
    {
      name: "memory",
      create: async () => {
        const store = new MemoryBlockStore();
        return [store, store];
      },
    },
    {
      name: "indexeddb",
      create: async () => {
        const indexedDB = new IDBFactory();
        const name = crypto.randomUUID();
        return Promise.all([
          IndexedDbBlockStore.open({ name, indexedDB }),
          IndexedDbBlockStore.open({ name, indexedDB }),
        ]);
      },
    },
    {
      name: "opfs",
      create: async () => {
        const root = new MemoryOpfs().root;
        const name = crypto.randomUUID();
        return Promise.all([
          OpfsBlockStore.open({ name, root }),
          OpfsBlockStore.open({ name, root }),
        ]);
      },
    },
  ];
}

describe("secondary-index SQL", () => {
  it("merges overlapping posting chunks in canonical order with bounded per-term state", () => {
    expect(
      collectFtsPostings([
        [
          { term: "b", rowIds: [3n], tf: [1] },
          { term: "d", rowIds: [5n], tf: [1] },
        ],
        [
          { term: "a", rowIds: [2n], tf: [1] },
          { term: "b", rowIds: [1n, 3n], tf: [2, 4] },
        ],
      ]),
    ).toEqual([
      { term: "a", rowIds: [2n], tf: [1] },
      { term: "b", rowIds: [1n, 3n], tf: [2, 4] },
      { term: "d", rowIds: [5n], tf: [1] },
    ]);
  });

  it("parses composite directions and UNIQUE DDL", () => {
    expect(compileStatement("CREATE INDEX IF NOT EXISTS by_total ON sales (total DESC)")).toEqual({
      kind: "create-index",
      index: "by_total",
      table: "sales",
      columns: [{ name: "total", direction: "desc" }],
      ifNotExists: true,
    });
    expect(compileStatement("DROP INDEX IF EXISTS by_total")).toEqual({
      kind: "drop-index",
      index: "by_total",
      ifExists: true,
    });
    expect(compileStatement("CREATE UNIQUE INDEX by_pair ON sales (sku, total DESC)")).toEqual({
      kind: "create-index",
      index: "by_pair",
      table: "sales",
      columns: [
        { name: "sku", direction: "asc" },
        { name: "total", direction: "desc" },
      ],
      unique: true,
    });
  });

  it("keeps the original single-column API and execution result compatible", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    try {
      await database.execute("CREATE TABLE compatible_index (id INTEGER, value VARCHAR)");
      await database.createIndex("compatible_value", "compatible_index", "value");
      expect(await database.execute("CREATE INDEX compatible_id ON compatible_index(id)")).toEqual({
        kind: "create-index",
        index: "compatible_id",
        table: "compatible_index",
        column: "id",
        columns: ["id"],
        unique: false,
      });
    } finally {
      await database.close();
      store.close();
    }
  });

  for (const implementation of implementations()) {
    it(`${implementation.name} prunes composite equality, IN, and range prefixes`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      try {
        await database.execute(
          "CREATE TABLE inventory (id INTEGER PRIMARY KEY, shop VARCHAR, category VARCHAR, price DOUBLE PRECISION)",
        );
        await database.execute(
          "INSERT INTO inventory VALUES (1, 'north', 'food', 10), (2, 'north', 'food', 20), (3, 'north', 'tools', 15), (4, 'south', 'food', 25), (5, 'north', 'food', 30)",
        );
        await database.execute(
          "CREATE INDEX inventory_lookup ON inventory(shop ASC, category ASC, price DESC)",
        );
        expect(
          await database.explain(
            "SELECT id FROM inventory WHERE shop = 'north' AND category = 'food' AND price >= 15 ORDER BY id",
          ),
        ).toContain("a ready secondary index prunes");
        expect(
          (
            await database.query(
              "SELECT id FROM inventory WHERE shop = 'north' AND category = 'food' AND price >= 15 ORDER BY id",
            )
          ).rows,
        ).toEqual([{ id: 2 }, { id: 5 }]);
        expect(
          (
            await database.query(
              "SELECT id FROM inventory WHERE shop IN ('north', 'south') AND category = 'food' AND price < 26 ORDER BY id",
            )
          ).rows,
        ).toEqual([{ id: 1 }, { id: 2 }, { id: 4 }]);

        await database.execute("UPDATE inventory SET price = 22 WHERE id = 1");
        expect(
          (
            await database.query(
              "SELECT id FROM inventory WHERE shop = 'north' AND category = 'food' AND price BETWEEN 21 AND 23",
            )
          ).rows,
        ).toEqual([{ id: 1 }]);
      } finally {
        await database.close();
        store.close();
      }
    });
  }

  for (const implementation of implementations()) {
    it(`${implementation.name} enforces composite UNIQUE indexes through every mutation`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      try {
        await database.execute(
          "CREATE TABLE accounts (id INTEGER PRIMARY KEY, tenant VARCHAR, email VARCHAR, label VARCHAR)",
        );
        await database.execute(
          "INSERT INTO accounts VALUES (1, 'a', 'one@example.com', 'one'), (2, 'a', 'two@example.com', 'two'), (3, NULL, 'one@example.com', 'nullable')",
        );
        await database.execute("CREATE UNIQUE INDEX account_email ON accounts(tenant, email)");
        await expect(
          database.execute("INSERT INTO accounts VALUES (4, 'a', 'one@example.com', 'duplicate')"),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
        await expect(
          database.execute(
            "INSERT INTO accounts VALUES (4, NULL, 'one@example.com', 'another null')",
          ),
        ).resolves.toMatchObject({ kind: "insert", rowCount: 1 });

        await database.execute("UPDATE accounts SET email = 'moved@example.com' WHERE id = 1");
        await expect(
          database.execute("UPDATE accounts SET email = 'moved@example.com' WHERE id = 2"),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
        await database.execute("DELETE FROM accounts WHERE id = 1");
        await expect(
          database.execute("UPDATE accounts SET email = 'moved@example.com' WHERE id = 2"),
        ).resolves.toMatchObject({ kind: "update", rowCount: 1 });

        await database.execute(
          "INSERT INTO accounts VALUES (2, 'a', 'upserted@example.com', 'changed') ON CONFLICT (id) DO UPDATE SET tenant = EXCLUDED.tenant, email = EXCLUDED.email, label = EXCLUDED.label",
        );
        await expect(
          database.execute(
            "INSERT INTO accounts VALUES (5, 'a', 'upserted@example.com', 'duplicate')",
          ),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
      } finally {
        await database.close();
        store.close();
      }
    });
  }

  for (const implementation of implementations()) {
    it(`${implementation.name} rejects duplicate rows while building a UNIQUE index`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 1 });
      try {
        await database.execute(
          "CREATE TABLE duplicate_values (id INTEGER PRIMARY KEY, code VARCHAR)",
        );
        await database.execute("INSERT INTO duplicate_values VALUES (1, 'x'), (2, 'x')");
        await expect(
          database.execute("CREATE UNIQUE INDEX duplicate_code ON duplicate_values(code)"),
        ).rejects.toThrow("duplicate key");
        expect((await store.getTableByName("duplicate_values"))?.secondaryIndexes).toBeUndefined();
        await expect(
          database.query("SELECT id FROM duplicate_values WHERE code = 'x' ORDER BY id"),
        ).resolves.toMatchObject({ rows: [{ id: 1 }, { id: 2 }] });
        await database.execute("UPDATE duplicate_values SET code = 'y' WHERE id = 2");
        await expect(
          database.execute("CREATE UNIQUE INDEX duplicate_code ON duplicate_values(code)"),
        ).resolves.toMatchObject({ kind: "create-index", unique: true });
      } finally {
        await database.close();
        store.close();
      }
    });
  }

  for (const implementation of implementations()) {
    it(`${implementation.name} builds UNIQUE membership from an uncompacted upsert history`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, autoCompact: false });
      try {
        await database.execute("CREATE TABLE upsert_unique (id INTEGER PRIMARY KEY, code VARCHAR)");
        await database.execute("INSERT INTO upsert_unique VALUES (1, 'old'), (2, 'two')");
        await database.execute(
          "INSERT INTO upsert_unique VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET code = EXCLUDED.code",
        );
        await database.execute("CREATE UNIQUE INDEX upsert_code ON upsert_unique(code)");
        await expect(
          database.execute("INSERT INTO upsert_unique VALUES (3, 'old')"),
        ).resolves.toMatchObject({ kind: "insert", rowCount: 1 });
        await expect(
          database.execute("INSERT INTO upsert_unique VALUES (4, 'new')"),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
      } finally {
        await database.close();
        store.close();
      }
    });
  }

  for (const implementation of sharedImplementations()) {
    it(`${implementation.name} serializes concurrent inserts against one UNIQUE index`, async () => {
      const [firstStore, secondStore] = await implementation.create();
      const first = new MinnowDatabase(firstStore);
      const second = new MinnowDatabase(secondStore);
      try {
        await first.execute(
          "CREATE TABLE concurrent_unique (id INTEGER PRIMARY KEY, code VARCHAR)",
        );
        await first.execute("CREATE UNIQUE INDEX concurrent_code ON concurrent_unique(code)");
        const outcomes = await Promise.allSettled([
          first.execute("INSERT INTO concurrent_unique VALUES (1, 'same')"),
          second.execute("INSERT INTO concurrent_unique VALUES (2, 'same')"),
        ]);
        expect(
          outcomes.filter((outcome) => outcome.status === "fulfilled"),
          implementation.name,
        ).toHaveLength(1);
        expect(
          outcomes.filter((outcome) => outcome.status === "rejected"),
          implementation.name,
        ).toHaveLength(1);
        expect((await first.query("SELECT code FROM concurrent_unique")).rows).toEqual([
          { code: "same" },
        ]);
      } finally {
        await Promise.all([first.close(), second.close()]);
        firstStore.close();
        if (secondStore !== firstStore) secondStore.close();
      }
    });
  }

  it("restarts a writer that began before a UNIQUE index became ready", async () => {
    const store = new MemoryBlockStore();
    const first = new MinnowDatabase(store);
    const second = new MinnowDatabase(store);
    await first.execute("CREATE TABLE stale_unique (id INTEGER PRIMARY KEY, code VARCHAR)");
    await first.execute("INSERT INTO stale_unique VALUES (1, 'taken')");
    const table = await store.getTableByName("stale_unique");
    if (table === undefined) throw new Error("Expected stale_unique table");
    const commit = store.writeTransaction.bind(store);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let entered!: () => void;
    const waiting = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let pause = true;
    store.writeTransaction = async (input) => {
      if (pause && input.changedTableIds?.includes(table.id)) {
        pause = false;
        entered();
        await gate;
      }
      return commit(input);
    };
    try {
      const staleWrite = first.execute("INSERT INTO stale_unique VALUES (2, 'taken')");
      await Promise.race([
        waiting,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("stale writer did not reach commit")), 1_000),
        ),
      ]);
      await Promise.race([
        second.execute("CREATE UNIQUE INDEX stale_code ON stale_unique(code)"),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("concurrent UNIQUE build did not finish")), 1_000),
        ),
      ]);
      release();
      await expect(staleWrite).rejects.toBeInstanceOf(UniqueConstraintError);
      expect((await first.query("SELECT id FROM stale_unique ORDER BY id")).rows).toEqual([
        { id: 1 },
      ]);
    } finally {
      release();
      store.writeTransaction = commit;
      await Promise.all([first.close(), second.close()]);
      store.close();
    }
  });

  it("enforces UNIQUE changes atomically across a write scope and allows a value swap", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    try {
      await database.execute(
        "CREATE TABLE scoped_unique (id INTEGER PRIMARY KEY, code VARCHAR NOT NULL)",
      );
      await database.execute("CREATE UNIQUE INDEX scoped_code ON scoped_unique(code)");
      await database.execute("BEGIN");
      await database.execute("INSERT INTO scoped_unique VALUES (1, 'duplicate')");
      await database.execute("INSERT INTO scoped_unique VALUES (2, 'duplicate')");
      await expect(database.execute("COMMIT")).rejects.toBeInstanceOf(UniqueConstraintError);
      expect((await database.query("SELECT id FROM scoped_unique")).rows).toEqual([]);

      await database.execute("INSERT INTO scoped_unique VALUES (1, 'a'), (2, 'b')");
      await database.updateBatch("scoped_unique", {
        keys: [1, 2],
        changes: { code: ["b", "a"] },
      });
      expect((await database.query("SELECT id, code FROM scoped_unique ORDER BY id")).rows).toEqual(
        [
          { id: 1, code: "b" },
          { id: 2, code: "a" },
        ],
      );
    } finally {
      await database.close();
      store.close();
    }
  });

  it("rolls back source and trigger-derived rows on a secondary UNIQUE conflict", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    try {
      await database.execute(
        "CREATE TABLE trigger_source (id INTEGER PRIMARY KEY, code VARCHAR NOT NULL)",
      );
      await database.execute(
        "CREATE TABLE trigger_audit (source_id INTEGER NOT NULL, code VARCHAR NOT NULL)",
      );
      await database.execute("CREATE UNIQUE INDEX trigger_audit_code ON trigger_audit(code)");
      await database.execute(
        "CREATE TRIGGER copy_code AFTER INSERT ON trigger_source BEGIN " +
          "INSERT INTO trigger_audit (source_id, code) VALUES (NEW.id, NEW.code); END",
      );
      await database.execute("INSERT INTO trigger_source VALUES (1, 'same')");
      await expect(
        database.execute("INSERT INTO trigger_source VALUES (2, 'same')"),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      expect(
        (await database.query("SELECT id, code FROM trigger_source ORDER BY id")).rows,
      ).toEqual([{ id: 1, code: "same" }]);
      expect((await database.query("SELECT source_id, code FROM trigger_audit")).rows).toEqual([
        { source_id: 1, code: "same" },
      ]);
    } finally {
      await database.close();
      store.close();
    }
  });

  for (const implementation of implementations()) {
    it(`${implementation.name} serves mixed-direction ORDER BY from a covering index`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      try {
        await database.execute(
          "CREATE TABLE ranked (category VARCHAR NOT NULL, price DOUBLE PRECISION NOT NULL, payload VARCHAR NOT NULL)",
        );
        await database.execute(
          "INSERT INTO ranked VALUES ('b', 1, 'b1'), ('a', 2, 'a2'), ('a', -3, 'a-3'), ('b', 5, 'b5'), ('a', 7, 'a7')",
        );
        await database.execute("CREATE INDEX ranked_order ON ranked(category ASC, price DESC)");
        expect(
          await database.explain(
            "SELECT category, price FROM ranked ORDER BY category ASC, price DESC",
          ),
        ).toContain("covering secondary-index scan avoids table blocks");
        expect(
          (
            await database.query(
              "SELECT category, price FROM ranked ORDER BY category ASC, price DESC",
            )
          ).rows,
        ).toEqual([
          { category: "a", price: 7 },
          { category: "a", price: 2 },
          { category: "a", price: -3 },
          { category: "b", price: 5 },
          { category: "b", price: 1 },
        ]);
        expect(
          (
            await database.query(
              "SELECT category, price FROM ranked WHERE price >= 2 ORDER BY category DESC, price ASC",
            )
          ).rows,
        ).toEqual([
          { category: "b", price: 5 },
          { category: "a", price: 2 },
          { category: "a", price: 7 },
        ]);
        expect(
          (
            await database.query(
              "SELECT category, price, payload FROM ranked ORDER BY category ASC, price DESC LIMIT 3",
            )
          ).rows,
        ).toEqual([
          { category: "a", price: 7, payload: "a7" },
          { category: "a", price: 2, payload: "a2" },
          { category: "a", price: -3, payload: "a-3" },
        ]);
        expect(
          (
            await database.query(
              "SELECT category, price FROM ranked ORDER BY category ASC, price DESC LIMIT 2 OFFSET 1",
            )
          ).rows,
        ).toEqual([
          { category: "a", price: 2 },
          { category: "a", price: -3 },
        ]);

        // Compaction preserves disjoint source row-ID spans inside a base segment. Rebuild the
        // index over that representation, then append a tail so the inverse locator path proves
        // both generations complete without a bigint-keyed row map.
        await database.execute("INSERT INTO ranked VALUES ('c', 4, 'c4')");
        expect((await database.compactTable("ranked")).compacted).toBe(true);
        await database.execute("DROP INDEX ranked_order");
        await database.execute("CREATE INDEX ranked_order ON ranked(category ASC, price DESC)");
        await database.execute("INSERT INTO ranked VALUES ('a', 6, 'a6')");
        expect(
          (
            await database.query(
              "SELECT category, price FROM ranked ORDER BY category ASC, price DESC LIMIT 3",
            )
          ).rows,
        ).toEqual([
          { category: "a", price: 7 },
          { category: "a", price: 6 },
          { category: "a", price: 2 },
        ]);
      } finally {
        await database.close();
        store.close();
      }
    });
  }

  it("proves a cold covering scan reads no table blocks", async () => {
    class CountingStore extends MemoryBlockStore {
      blockReads = 0;
      override async getBlock(id: string) {
        this.blockReads += 1;
        return super.getBlock(id);
      }
      override async getBlocks(ids: readonly string[]) {
        this.blockReads += ids.length;
        return super.getBlocks(ids);
      }
    }
    const store = new CountingStore();
    const writer = new MinnowDatabase(store, { rowsPerBlock: 2 });
    await writer.execute(
      "CREATE TABLE covered_rows (group_name VARCHAR NOT NULL, score DOUBLE PRECISION NOT NULL, detail VARCHAR NOT NULL)",
    );
    await writer.execute(
      "INSERT INTO covered_rows VALUES ('b', 1, 'one'), ('a', 3, 'three'), ('a', 2, 'two')",
    );
    await writer.execute("CREATE INDEX covered_order ON covered_rows(group_name, score DESC)");
    await writer.close();

    store.blockReads = 0;
    const reader = new MinnowDatabase(store, { rowsPerBlock: 2 });
    expect(
      (
        await reader.query(
          "SELECT group_name, score FROM covered_rows ORDER BY group_name, score DESC",
        )
      ).rows,
    ).toEqual([
      { group_name: "a", score: 3 },
      { group_name: "a", score: 2 },
      { group_name: "b", score: 1 },
    ]);
    expect(store.blockReads).toBe(0);
    await reader.query(
      "SELECT group_name, score, detail FROM covered_rows ORDER BY group_name, score DESC",
    );
    expect(store.blockReads).toBeGreaterThan(0);
    await reader.close();
    store.close();
  });

  it("orders prefix strings exactly and refuses an incomplete nullable covering index", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    try {
      await database.execute(
        "CREATE TABLE prefix_order (name VARCHAR NOT NULL, rank INTEGER NOT NULL)",
      );
      await database.execute(
        "INSERT INTO prefix_order VALUES ('a', 1), ('', 4), ('aa', 2), ('b', 3)",
      );
      await database.execute("CREATE INDEX prefix_order_key ON prefix_order(name DESC, rank ASC)");
      expect(
        (await database.query("SELECT name, rank FROM prefix_order ORDER BY name DESC, rank ASC"))
          .rows,
      ).toEqual([
        { name: "b", rank: 3 },
        { name: "aa", rank: 2 },
        { name: "a", rank: 1 },
        { name: "", rank: 4 },
      ]);
      expect(
        (await database.query("SELECT name, rank FROM prefix_order ORDER BY name ASC, rank DESC"))
          .rows,
      ).toEqual([
        { name: "", rank: 4 },
        { name: "a", rank: 1 },
        { name: "aa", rank: 2 },
        { name: "b", rank: 3 },
      ]);

      await database.execute(
        "CREATE TABLE nullable_order (value VARCHAR, detail VARCHAR NOT NULL)",
      );
      await database.execute(
        "INSERT INTO nullable_order VALUES ('b', 'b'), (NULL, 'null'), ('a', 'a')",
      );
      await database.execute("CREATE INDEX nullable_order_key ON nullable_order(value)");
      expect(
        await database.explain("SELECT value FROM nullable_order ORDER BY value"),
      ).not.toContain("secondary index supplies ORDER BY");
      expect(
        (await database.query("SELECT value FROM nullable_order ORDER BY value")).rows,
      ).toEqual([{ value: null }, { value: "a" }, { value: "b" }]);
    } finally {
      await database.close();
      store.close();
    }
  });

  for (const implementation of implementations()) {
    it(`${implementation.name} does not retain historical secondary UNIQUE keys`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { autoCompact: false });
      try {
        await database.execute(
          "CREATE TABLE bounded_unique (id INTEGER PRIMARY KEY, code VARCHAR NOT NULL)",
        );
        await database.execute("INSERT INTO bounded_unique VALUES (1, 'code-0')");
        await database.execute("CREATE UNIQUE INDEX bounded_code ON bounded_unique(code)");
        for (let revision = 1; revision <= 32; revision += 1) {
          await database.execute(
            `UPDATE bounded_unique SET code = 'code-${String(revision)}' WHERE id = 1`,
          );
        }
        const snapshot = await store.exportSnapshot?.();
        const membership = snapshot?.tables.find(({ record }) => record.name === "bounded_unique")
          ?.secondaryUniqueKeys?.[0];
        expect(membership?.keyTokens).toHaveLength(1);
        await expect(
          database.execute("INSERT INTO bounded_unique VALUES (2, 'code-32')"),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
        await expect(
          database.execute("INSERT INTO bounded_unique VALUES (2, 'code-0')"),
        ).resolves.toMatchObject({ kind: "insert", rowCount: 1 });
      } finally {
        await database.close();
        store.close();
      }
    });
  }

  for (const implementation of implementations()) {
    it(`${implementation.name} keeps equality/range results correct across every mutation`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      try {
        await database.execute(
          "CREATE TABLE sales (id INTEGER PRIMARY KEY, sku VARCHAR, total DOUBLE PRECISION)",
        );
        await database.execute(
          "INSERT INTO sales (id, sku, total) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'b', 30), (4, NULL, 40)",
        );
        await database.execute("CREATE INDEX by_total ON sales (total)");
        await database.execute("CREATE INDEX by_sku ON sales (sku)");
        expect(await database.explain("SELECT id FROM sales WHERE sku = 'b'")).toContain(
          "a ready secondary index prunes the scan to candidate row groups",
        );

        expect(
          (
            await database.query(
              "SELECT id FROM sales WHERE total >= 20 AND total < 40 ORDER BY id",
            )
          ).rows,
        ).toEqual([{ id: 2 }, { id: 3 }]);
        expect(
          (await database.query("SELECT id FROM sales WHERE sku = 'b' ORDER BY id")).rows,
        ).toEqual([{ id: 2 }, { id: 3 }]);
        expect(
          (await database.query("SELECT id FROM sales WHERE sku IN ('a', 'b') ORDER BY id")).rows,
        ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);

        await database.execute("UPDATE sales SET total = 25, sku = 'c' WHERE id = 1");
        expect((await database.query("SELECT id FROM sales WHERE total = 25")).rows).toEqual([
          { id: 1 },
        ]);
        expect((await database.query("SELECT id FROM sales WHERE sku = 'a'")).rows).toEqual([]);
        expect((await database.query("SELECT id FROM sales WHERE sku = 'c'")).rows).toEqual([
          { id: 1 },
        ]);

        await database.execute("DELETE FROM sales WHERE id = 2");
        expect((await database.query("SELECT id FROM sales WHERE sku = 'b'")).rows).toEqual([
          { id: 3 },
        ]);

        await database.execute(
          "INSERT INTO sales (id, sku, total) VALUES (3, 'd', 35) ON CONFLICT (id) DO UPDATE SET sku = EXCLUDED.sku, total = EXCLUDED.total",
        );
        expect((await database.query("SELECT id FROM sales WHERE sku = 'b'")).rows).toEqual([]);
        expect(
          (await database.query("SELECT id FROM sales WHERE total > 30 AND total <= 35")).rows,
        ).toEqual([{ id: 3 }]);

        const table = await store.getTableByName("sales");
        expect(Object.values(table?.secondaryIndexes ?? {}).map((index) => index.state)).toEqual([
          "ready",
          "ready",
        ]);
        expect(await database.execute("DROP INDEX by_sku")).toEqual({
          kind: "drop-index",
          index: "by_sku",
          dropped: true,
        });
        expect(await database.execute("DROP INDEX IF EXISTS by_sku")).toEqual({
          kind: "drop-index",
          index: "by_sku",
          dropped: false,
        });
      } finally {
        await database.close();
        store.close();
      }
    });
  }

  it("enforces catalog-global index names atomically across concurrent engines", async () => {
    for (const implementation of sharedImplementations()) {
      const [firstStore, secondStore] = await implementation.create();
      const first = new MinnowDatabase(firstStore);
      const second = new MinnowDatabase(secondStore);
      try {
        await first.execute("CREATE TABLE first_table (id INTEGER PRIMARY KEY, value INTEGER)");
        await first.execute("CREATE TABLE second_table (id INTEGER PRIMARY KEY, value INTEGER)");
        await first.execute("INSERT INTO first_table VALUES (1, 10)");
        await first.execute("INSERT INTO second_table VALUES (1, 20)");
        const outcomes = await Promise.allSettled([
          first.execute("CREATE INDEX globally_named ON first_table(value)"),
          second.execute("CREATE INDEX globally_named ON second_table(value)"),
        ]);
        expect(
          outcomes.filter((outcome) => outcome.status === "fulfilled"),
          implementation.name,
        ).toHaveLength(1);
        expect(
          outcomes.filter((outcome) => outcome.status === "rejected"),
          implementation.name,
        ).toHaveLength(1);
        const indexes = (await firstStore.listTables()).flatMap((table) =>
          Object.values(table.secondaryIndexes ?? {}),
        );
        expect(indexes.filter((index) => index.name === "globally_named")).toHaveLength(1);
      } finally {
        await Promise.all([first.close(), second.close()]);
        firstStore.close();
        if (secondStore !== firstStore) secondStore.close();
      }
    }
  });

  for (const implementation of implementations()) {
    it(`${implementation.name} takes over a builder abandoned by a dead tab`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      await database.execute("CREATE TABLE abandoned (id INTEGER PRIMARY KEY, value INTEGER)");
      await database.execute("INSERT INTO abandoned VALUES (1, 10), (2, 20), (3, 30)");
      const table = await store.getTableByName("abandoned");
      if (table === undefined) throw new Error("Expected abandoned table");
      await store.updateTable(table.id, table.revision ?? 0, {
        secondaryIndexes: {
          abandoned: {
            name: "abandoned_by_value",
            columnId: table.columns.find((column) => column.name === "value")?.id ?? "",
            storage: "postings-v1",
            storageColumnId: "abandoned-storage",
            locator: "key-hash-v1",
            state: "building",
            buildId: "dead-tab-build",
            buildFromVersion: -1,
          },
        },
      });

      expect(
        (await database.query("SELECT id FROM abandoned WHERE value >= 20 ORDER BY id")).rows,
      ).toEqual([{ id: 2 }, { id: 3 }]);
      await database.close();
      const recovered = await store.getTableByName("abandoned");
      expect(recovered?.secondaryIndexes?.abandoned).toMatchObject({ state: "ready" });
      expect(recovered?.secondaryIndexes?.abandoned?.buildId).toBeUndefined();
      store.close();
    });
  }

  it("maintains row-ID postings for a keyless append-only table", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    try {
      await database.execute("CREATE TABLE events (kind VARCHAR, amount DOUBLE PRECISION)");
      await database.execute("INSERT INTO events VALUES ('sale', 10), ('return', -2)");
      await database.execute("CREATE INDEX by_kind ON events (kind)");
      await database.execute("INSERT INTO events VALUES ('sale', 12), ('other', 1)");
      expect(
        (await database.query("SELECT amount FROM events WHERE kind = 'sale' ORDER BY amount"))
          .rows,
      ).toEqual([{ amount: 10 }, { amount: 12 }]);
    } finally {
      await database.close();
      store.close();
    }
  });

  it("folds an all-day mutation tail instead of retaining one index record per commit", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4, autoCompact: false });
    await database.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)");
    await database.execute("INSERT INTO counters VALUES (1, 0)");
    await database.execute("CREATE INDEX by_value ON counters (value)");
    for (let value = 1; value <= 80; value += 1) {
      await database.update("counters", 1, { value });
    }
    await database.close();

    const table = await store.getTableByName("counters");
    const index = Object.values(table?.secondaryIndexes ?? {})[0];
    expect(index?.state).toBe("ready");
    const candidates = await store.readFtsCandidates(
      table?.id ?? "",
      index?.storageColumnId ?? "",
      [{ term: "c054000000000000", prefix: false }],
      (await store.getCurrentManifestVersion()) ?? -1,
    );
    expect(candidates.deltaChunkCount).toBeLessThanOrEqual(16);
    store.close();
  });

  it("actually avoids non-candidate payload blocks on a cold append-only scan", async () => {
    const store = new MemoryBlockStore();
    let database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    await database.execute("CREATE TABLE events (kind VARCHAR, payload VARCHAR)");
    await database.execute(
      "INSERT INTO events VALUES ('rare', 'p0'), ('common', 'p1'), ('common', 'p2'), ('common', 'p3'), ('common', 'p4'), ('common', 'p5'), ('common', 'p6'), ('common', 'p7')",
    );
    await database.execute("CREATE INDEX by_kind ON events (kind)");
    await database.close();

    let blocksRead = 0;
    const getBlocks = store.getBlocks.bind(store);
    store.getBlocks = async (ids) => {
      blocksRead += ids.length;
      return getBlocks(ids);
    };
    database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    expect((await database.query("SELECT payload FROM events WHERE kind = 'rare'")).rows).toEqual([
      { payload: "p0" },
    ]);
    // A full scan needs all four blocks of both columns (8). The index reads the four cheap
    // anchor headers, then only the candidate row group's two payload/predicate blocks.
    expect(blocksRead).toBeLessThan(8);
    await database.close();
    store.close();
  });

  it("falls back to a scan if a catalog-ready base disappears during the query", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    await database.execute("CREATE TABLE raced (id INTEGER PRIMARY KEY, value INTEGER)");
    await database.execute("INSERT INTO raced VALUES (1, 10), (2, 20), (3, 30)");
    await database.execute("CREATE INDEX raced_by_value ON raced(value)");
    const table = await store.getTableByName("raced");
    const index = Object.values(table?.secondaryIndexes ?? {})[0];
    if (table === undefined || index === undefined) throw new Error("Expected raced index");

    const read = store.readFtsCandidates.bind(store);
    let removed = false;
    store.readFtsCandidates = async (...args) => {
      if (!removed) {
        removed = true;
        await store.removeFtsColumn(table.id, index.storageColumnId);
      }
      return read(...args);
    };
    expect((await database.query("SELECT id FROM raced WHERE value = 20")).rows).toEqual([
      { id: 2 },
    ]);
    await database.close();
    store.close();
  });

  it("builds a high-cardinality base in row-group chunks, never one database-sized value", async () => {
    const store = new MemoryBlockStore();
    let legacyBaseWrites = 0;
    let chunks = 0;
    let largestChunk = 0;
    const legacyWrite = store.writeFtsBase.bind(store);
    store.writeFtsBase = async (...args) => {
      legacyBaseWrites += 1;
      return legacyWrite(...args);
    };
    const chunkWrite = store.writeFtsBaseBuildChunk.bind(store);
    store.writeFtsBaseBuildChunk = async (...args) => {
      chunks += 1;
      largestChunk = Math.max(largestChunk, args[4].length);
      return chunkWrite(...args);
    };
    const database = new MinnowDatabase(store, { rowsPerBlock: 32 });
    try {
      await database.execute("CREATE TABLE samples (value INTEGER, payload VARCHAR)");
      await database.insertBatch(
        "samples",
        Array.from({ length: 1_000 }, (_, value) => ({ value, payload: `p${String(value)}` })),
      );
      await database.execute("CREATE INDEX by_value ON samples (value)");
      expect(legacyBaseWrites).toBe(0);
      expect(chunks).toBeGreaterThan(1);
      expect(largestChunk).toBeLessThanOrEqual(32);
      expect((await database.query("SELECT payload FROM samples WHERE value = 777")).rows).toEqual([
        { payload: "p777" },
      ]);
    } finally {
      await database.close();
      store.close();
    }
  });

  it("keeps the build bounded even when an upsert history needs the superset fallback", async () => {
    const store = new MemoryBlockStore();
    let legacyBaseWrites = 0;
    let largestChunkRows = 0;
    const legacyWrite = store.writeFtsBase.bind(store);
    store.writeFtsBase = async (...args) => {
      legacyBaseWrites += 1;
      return legacyWrite(...args);
    };
    const chunkWrite = store.writeFtsBaseBuildChunk.bind(store);
    store.writeFtsBaseBuildChunk = async (...args) => {
      largestChunkRows = Math.max(
        largestChunkRows,
        args[4].reduce((count, posting) => count + posting.rowIds.length, 0),
      );
      return chunkWrite(...args);
    };
    const database = new MinnowDatabase(store, { rowsPerBlock: 16, autoCompact: false });
    try {
      await database.execute("CREATE TABLE upserted (id INTEGER PRIMARY KEY, value INTEGER)");
      await database.insertBatch(
        "upserted",
        Array.from({ length: 100 }, (_, index) => ({ id: index, value: index })),
      );
      await database.execute(
        "INSERT INTO upserted VALUES (50, 500), (100, 1000) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
      );
      await database.execute("CREATE INDEX upserted_by_value ON upserted(value)");
      expect(legacyBaseWrites).toBe(0);
      expect(largestChunkRows).toBeLessThanOrEqual(16);
      expect((await database.query("SELECT id FROM upserted WHERE value = 50")).rows).toEqual([]);
      expect(
        (await database.query("SELECT id FROM upserted WHERE value >= 500 ORDER BY id")).rows,
      ).toEqual([{ id: 50 }, { id: 100 }]);
    } finally {
      await database.close();
      store.close();
    }
  });

  it("merges index coverage across a multi-statement SQL transaction", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    try {
      await database.execute("CREATE TABLE tx_rows (id INTEGER PRIMARY KEY, value INTEGER)");
      await database.execute("INSERT INTO tx_rows VALUES (1, 10), (2, 20), (3, 30)");
      await database.execute("CREATE INDEX tx_rows_by_value ON tx_rows(value)");
      await database.execute("BEGIN");
      await database.execute("UPDATE tx_rows SET value = 25 WHERE id = 1");
      await database.execute("DELETE FROM tx_rows WHERE id = 2");
      await database.execute("INSERT INTO tx_rows VALUES (4, 40)");
      await database.execute("COMMIT");
      expect(
        (await database.query("SELECT id FROM tx_rows WHERE value >= 20 ORDER BY id")).rows,
      ).toEqual([{ id: 1 }, { id: 3 }, { id: 4 }]);
      expect((await database.query("SELECT id FROM tx_rows WHERE value IN (10, 20)")).rows).toEqual(
        [],
      );
    } finally {
      await database.close();
      store.close();
    }
  });

  it("preserves numeric, boolean, datetime, and Unicode comparison order", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    try {
      await database.execute(
        "CREATE TABLE values_by_type (id INTEGER PRIMARY KEY, amount DOUBLE PRECISION, active BOOLEAN, happened TIMESTAMP, label VARCHAR)",
      );
      await database.insertBatch("values_by_type", [
        {
          id: 1,
          amount: -100.5,
          active: false,
          happened: new Date("1969-12-31T23:59:59.000Z"),
          label: "Zulu",
        },
        {
          id: 2,
          amount: -0,
          active: true,
          happened: new Date("2026-01-01T00:00:00.000Z"),
          label: "apple",
        },
        {
          id: 3,
          amount: 1.25,
          active: true,
          happened: new Date("2030-06-15T12:00:00.000Z"),
          label: "éclair",
        },
      ]);
      const indexes: Array<[string, string]> = [
        ["by_amount", "amount"],
        ["by_active", "active"],
        ["by_happened", "happened"],
        ["by_label", "label"],
      ];
      for (const [name, column] of indexes) {
        await database.execute(`CREATE INDEX ${name} ON values_by_type (${column})`);
      }

      expect(
        (await database.query("SELECT id FROM values_by_type WHERE amount >= -1 ORDER BY id")).rows,
      ).toEqual([{ id: 2 }, { id: 3 }]);
      expect(
        (await database.query("SELECT id FROM values_by_type WHERE active = TRUE ORDER BY id"))
          .rows,
      ).toEqual([{ id: 2 }, { id: 3 }]);
      expect(
        (
          await database.query(
            "SELECT id FROM values_by_type WHERE happened < TIMESTAMP '2027-01-01' ORDER BY id",
          )
        ).rows,
      ).toEqual([{ id: 1 }, { id: 2 }]);
      expect(
        (await database.query("SELECT id FROM values_by_type WHERE label > 'apple' ORDER BY id"))
          .rows,
      ).toEqual([{ id: 3 }]);
    } finally {
      await database.close();
      store.close();
    }
  });

  it("reopens an OPFS index with its committed base and mutation tail intact", async () => {
    const shim = new MemoryOpfs();
    const name = crypto.randomUUID();
    let store = await OpfsBlockStore.open({ name, root: shim.root });
    let database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    await database.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY, quantity INTEGER)");
    await database.execute("INSERT INTO inventory VALUES (1, 2), (2, 7), (3, 12)");
    await database.execute("CREATE INDEX by_quantity ON inventory (quantity)");
    await database.execute("UPDATE inventory SET quantity = 9 WHERE id = 1");
    await database.close();

    store = await OpfsBlockStore.open({ name, root: shim.root });
    database = new MinnowDatabase(store, { rowsPerBlock: 2 });
    expect(
      (await database.query("SELECT id FROM inventory WHERE quantity >= 7 ORDER BY id")).rows,
    ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    expect((await database.query("SELECT id FROM inventory WHERE quantity = 2")).rows).toEqual([]);
    await database.close();
  });

  for (const implementation of implementations()) {
    it(`${implementation.name} snapshot preserves index correctness and rebuildability`, async () => {
      const sourceStore = await implementation.create();
      const source = new MinnowDatabase(sourceStore, { rowsPerBlock: 2 });
      await source.execute("CREATE TABLE snapshot_rows (id INTEGER PRIMARY KEY, value INTEGER)");
      await source.execute("INSERT INTO snapshot_rows VALUES (1, 10), (2, 20), (3, 30)");
      await source.execute("CREATE INDEX snapshot_by_value ON snapshot_rows (value)");
      await source.execute("CREATE UNIQUE INDEX snapshot_unique_value ON snapshot_rows (value)");
      await source.execute("UPDATE snapshot_rows SET value = 25 WHERE id = 2");
      const bytes = await source.exportSnapshot();
      await source.close();

      const restoredStore = new MemoryBlockStore();
      const restored = new MinnowDatabase(restoredStore, { rowsPerBlock: 2 });
      await restored.importSnapshot(bytes);
      expect(
        (await restored.query("SELECT id FROM snapshot_rows WHERE value >= 20 ORDER BY id")).rows,
      ).toEqual([{ id: 2 }, { id: 3 }]);
      expect((await restored.query("SELECT id FROM snapshot_rows WHERE value = 20")).rows).toEqual(
        [],
      );
      await expect(
        restored.execute("INSERT INTO snapshot_rows VALUES (4, 25)"),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      await restored.close();

      const table = await restoredStore.getTableByName("snapshot_rows");
      expect(Object.values(table?.secondaryIndexes ?? {}).map((index) => index.state)).toEqual([
        "ready",
        "invalid",
      ]);
      expect(
        Object.values(table?.secondaryIndexes ?? {}).find(
          (index) => index.name === "snapshot_unique_value",
        )?.uniqueEnforced,
      ).toBe(true);
      restoredStore.close();
    });
  }
});
