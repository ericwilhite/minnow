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
import { compileStatement } from "./query.js";

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

describe("secondary-index SQL", () => {
  it("parses the portable one-column DDL and rejects unsupported uniqueness/composites", () => {
    expect(compileStatement("CREATE INDEX IF NOT EXISTS by_total ON sales (total DESC)")).toEqual({
      kind: "create-index",
      index: "by_total",
      table: "sales",
      column: "total",
      ifNotExists: true,
    });
    expect(compileStatement("DROP INDEX IF EXISTS by_total")).toEqual({
      kind: "drop-index",
      index: "by_total",
      ifExists: true,
    });
    expect(() => compileStatement("CREATE UNIQUE INDEX by_total ON sales (total)")).toThrow(
      "unique key constraint",
    );
    expect(() => compileStatement("CREATE INDEX both ON sales (total, id)")).toThrow("one column");
  });

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
    const shared: Array<{
      name: string;
      create: () => Promise<[BlockStore, BlockStore]>;
    }> = [
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

    for (const implementation of shared) {
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
      await restored.close();

      const table = await restoredStore.getTableByName("snapshot_rows");
      expect(Object.values(table?.secondaryIndexes ?? {})[0]?.state).toBe("ready");
      restoredStore.close();
    });
  }
});
