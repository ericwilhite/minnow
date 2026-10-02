/**
 * Creating a UNIQUE index stages its key set through the store's chunked build — globally
 * ordered chunks of at most MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK keys, one store call each —
 * and publishes it with one atomic finish. The whole key set used to travel as one catalog
 * update, a step that grew with the table on every store and wrote one database-sized frame to
 * the OPFS log. These pin the staged path on the memory, IndexedDB, and OPFS stores: chunk
 * sizes and order, enforcement and reopen afterwards, an empty table, a sort across many runs,
 * and that a failed publication releases its staged keys and is rebuilt.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { OpfsBlockStore } from "../storage/opfs/index.js";
import {
  MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK,
  type AppendUniqueKeyBuildChunkInput,
} from "../storage/types.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

interface Opened {
  store: BlockStore;
  reopen: () => Promise<BlockStore>;
}

const stores: Array<{ name: string; open: () => Promise<Opened> }> = [
  {
    name: "memory",
    open: async () => {
      const store = new MemoryBlockStore();
      return { store, reopen: async () => store };
    },
  },
  {
    name: "indexeddb",
    open: async () => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      const store = await IndexedDbBlockStore.open({ name, indexedDB });
      return { store, reopen: () => IndexedDbBlockStore.open({ name, indexedDB }) };
    },
  },
  {
    name: "opfs",
    open: async () => {
      const shim = new MemoryOpfs();
      const store = await OpfsBlockStore.open({ name: "unique", root: shim.root });
      return { store, reopen: () => OpfsBlockStore.open({ name: "unique", root: shim.root }) };
    },
  },
];

/**
 * `store` behind a proxy that records every staged chunk, refuses the one-shot seed the engine
 * no longer sends, and can fail the first publication. Calls reach the store bound to itself,
 * so adapters with private state and read-only methods are intercepted the same way.
 */
function watchStagedBuild(
  store: BlockStore,
  options: { failFirstFinish?: boolean } = {},
): { store: BlockStore; appended: AppendUniqueKeyBuildChunkInput[]; failedBuilds: string[] } {
  const appended: AppendUniqueKeyBuildChunkInput[] = [];
  const failedBuilds: string[] = [];
  const overrides: Partial<BlockStore> = {
    appendUniqueKeyBuildChunk: async (input) => {
      appended.push(input);
      return store.appendUniqueKeyBuildChunk(input);
    },
    updateTable: async (id, revision, changes) => {
      if (changes.uniqueKeySeed !== undefined) throw new Error("UNIQUE keys sent in one update");
      return store.updateTable(id, revision, changes);
    },
    finishUniqueKeyBuild: async (input) => {
      if (options.failFirstFinish === true && failedBuilds.length === 0) {
        failedBuilds.push(input.buildId);
        throw new Error("injected publication failure");
      }
      return store.finishUniqueKeyBuild(input);
    },
  };
  const watched = new Proxy(store, {
    get(target, property) {
      if (Object.hasOwn(overrides, property)) return overrides[property as keyof BlockStore];
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? (value as () => unknown).bind(target) : value;
    },
  });
  return { store: watched, appended, failedBuilds };
}

async function itemsTable(store: BlockStore, rows: number): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(store, { autoCompact: false });
  await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code VARCHAR, amount INTEGER)");
  for (let start = 0; start < rows; start += 5_000) {
    await db.insertBatch(
      "items",
      Array.from({ length: Math.min(5_000, rows - start) }, (_, index) => ({
        id: start + index,
        // Unordered relative to insertion, so the staged order comes from the build's sort.
        code: `code-${String(((start + index) * 7_919) % 1_000_003)}`,
        amount: index,
      })),
    );
  }
  return db;
}

function close(db: MinnowDatabase, store: BlockStore): Promise<void> {
  return db.close().then(() => store.close());
}

describe.each(stores)("UNIQUE index staged build ($name)", ({ open }) => {
  it("stages ordered bounded chunks, enforces the index, and survives a reopen", async () => {
    const { store: opened, reopen } = await open();
    const { store, appended } = watchStagedBuild(opened);
    const rows = 10_000;
    const db = await itemsTable(store, rows);
    await db.execute("CREATE UNIQUE INDEX items_code ON items (code)");

    expect(appended.length).toBe(Math.ceil(rows / MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK));
    const staged = appended.flatMap((chunk) => [...chunk.keyTokens]);
    expect(staged).toHaveLength(rows);
    for (const chunk of appended) {
      expect(chunk.keyTokens.length).toBeLessThanOrEqual(MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK);
    }
    expect(appended.map((chunk) => chunk.ordinal)).toEqual(appended.map((_, index) => index));
    expect(staged.every((token, index) => index === 0 || (staged[index - 1] ?? "") < token)).toBe(
      true,
    );

    const code = `code-${String((1_234 * 7_919) % 1_000_003)}`;
    expect((await db.query(`SELECT id FROM items WHERE code = '${code}'`)).rows).toEqual([
      { id: 1_234 },
    ]);
    await expect(
      db.execute(`INSERT INTO items (id, code, amount) VALUES (99999, '${code}', 0)`),
    ).rejects.toThrow(/unique|duplicate/i);
    await db.execute("INSERT INTO items (id, code, amount) VALUES (99999, 'fresh', 0)");
    await close(db, opened);

    const again = await reopen();
    const reader = new MinnowDatabase(again, { autoCompact: false });
    await expect(
      reader.execute("INSERT INTO items (id, code, amount) VALUES (100000, 'fresh', 0)"),
    ).rejects.toThrow(/unique|duplicate/i);
    expect((await reader.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: rows + 1 }]);
    await close(reader, again);
  });

  it("publishes an empty table's index with no staged chunks", async () => {
    const { store: opened } = await open();
    const { store, appended } = watchStagedBuild(opened);
    const db = await itemsTable(store, 0);
    await db.execute("CREATE UNIQUE INDEX items_code ON items (code)");
    expect(appended).toEqual([]);
    await db.execute("INSERT INTO items (id, code, amount) VALUES (1, 'a', 0)");
    await expect(
      db.execute("INSERT INTO items (id, code, amount) VALUES (2, 'a', 0)"),
    ).rejects.toThrow(/unique|duplicate/i);
    await close(db, opened);
  });

  it("releases staged keys when publication fails, then rebuilds", async () => {
    const { store: opened } = await open();
    const { store, failedBuilds } = watchStagedBuild(opened, { failFirstFinish: true });
    const db = await itemsTable(store, 6_000);
    await expect(db.execute("CREATE UNIQUE INDEX items_code ON items (code)")).rejects.toThrow(
      /injected publication failure/,
    );
    // Aborted rather than left holding staged keys until its lease runs out, and the failed
    // statement leaves no index behind.
    expect((await opened.getUniqueKeyBuild(failedBuilds[0] ?? ""))?.state).not.toBe("active");
    expect((await opened.getTableByName("items"))?.secondaryIndexes ?? {}).toEqual({});

    await db.execute("CREATE UNIQUE INDEX items_code ON items (code)");
    const code = `code-${String((321 * 7_919) % 1_000_003)}`;
    expect((await db.query(`SELECT id FROM items WHERE code = '${code}'`)).rows).toEqual([
      { id: 321 },
    ]);
    await expect(
      db.execute(`INSERT INTO items (id, code, amount) VALUES (99999, '${code}', 0)`),
    ).rejects.toThrow(/unique|duplicate/i);
    await close(db, opened);
  });
});

describe("UNIQUE index staged build across sort runs", () => {
  it("merges more terms than one sort run into one global order", async () => {
    const opened = new MemoryBlockStore();
    const { store, appended } = watchStagedBuild(opened);
    const rows = 40_000;
    const db = await itemsTable(store, rows);
    await db.execute("CREATE UNIQUE INDEX items_code ON items (code)");
    const staged = appended.flatMap((chunk) => [...chunk.keyTokens]);
    expect(staged).toHaveLength(rows);
    expect(staged).toEqual([...staged].sort());
    expect(new Set(staged).size).toBe(rows);
    await close(db, opened);
  });
});
