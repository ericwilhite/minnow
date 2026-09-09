/** Coordinated autocommit admits all writers; the opt-out still exercises bounded CAS retries. */
import { describe, expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";

const RETRIES = 8;

/**
 * `writers` concurrent one-row inserts, issued through one database or, with
 * `separateInstances`, through one database instance per writer over the same store — the
 * shape of that many tabs.
 */
async function contend(
  store: BlockStore,
  writers: number,
  options: { separateInstances?: boolean; coordinateWrites?: boolean } = {},
): Promise<{ accepted: number; persisted: number; reasons: Set<string> }> {
  const database = new MinnowDatabase(store, {
    maxCommitRetries: RETRIES,
    coordinateWrites: options.coordinateWrites ?? true,
  });
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "value", type: "number" },
    ],
  });
  await database.insertBatch("items", [{ id: 1, value: 0 }]);
  const reasons = new Set<string>();
  const outcomes = await Promise.all(
    Array.from({ length: writers }, (_, index) => {
      const writer =
        options.separateInstances === true
          ? new MinnowDatabase(store, {
              maxCommitRetries: RETRIES,
              coordinateWrites: options.coordinateWrites ?? true,
            })
          : database;
      return writer.insertBatch("items", [{ id: 1_000 + index, value: index }]).then(
        () => 1,
        (error: unknown) => {
          reasons.add(error instanceof Error ? error.message : String(error));
          return 0;
        },
      );
    }),
  );
  const rows = (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false }))
    .rows[0] as { n: number };
  return {
    accepted: outcomes.reduce((total, one) => total + one, 0),
    // Minus the seed row, so this counts only what the contending writers left behind.
    persisted: rows.n - 1,
    reasons,
  };
}

describe("concurrent writes to one table", () => {
  it("lets every writer through on the memory store", async () => {
    for (const writers of [16, 64]) {
      const { accepted, persisted } = await contend(new MemoryBlockStore(), writers);
      expect(accepted, `${String(writers)} writers`).toBe(writers);
      expect(persisted, `${String(writers)} writers`).toBe(writers);
    }
  });

  it("lets every writer through on IndexedDB when they share one database", async () => {
    // One database runs its writes in turn, so none of them ever reads a version another is
    // about to move: the sixteen land like sixteen sequential writes would.
    for (const writers of [16, 64]) {
      const store = await IndexedDbBlockStore.open({
        name: crypto.randomUUID(),
        indexedDB: new IDBFactory(),
      });
      const { accepted, persisted } = await contend(store, writers);
      expect(accepted, `${String(writers)} writers`).toBe(writers);
      expect(persisted, `${String(writers)} writers`).toBe(writers);
    }
  });

  it.each([true, false])(
    "handles separate IndexedDB instances with coordination=%s",
    async (coordinateWrites) => {
      // The fallback remains an explicit conflict, never partial or duplicated data.
      for (const writers of [16, 32, 64]) {
        const store = await IndexedDbBlockStore.open({
          name: crypto.randomUUID(),
          indexedDB: new IDBFactory(),
        });
        const { accepted, persisted, reasons } = await contend(store, writers, {
          separateInstances: true,
          coordinateWrites,
        });
        expect(accepted, `${String(writers)} writers`).toBe(
          coordinateWrites ? writers : RETRIES + 1,
        );
        expect(persisted, `${String(writers)} writers`).toBe(
          coordinateWrites ? writers : RETRIES + 1,
        );
        // A conflict, not a corruption or a quota failure -- the losers must be losing for the
        // reason this test claims they are.
        if (coordinateWrites) expect(reasons.size).toBe(0);
        else expect([...reasons].join(" | ")).toMatch(/Manifest changed/);
      }
    },
  );

  it("loses nothing and duplicates nothing, whichever writers win", async () => {
    const store = await IndexedDbBlockStore.open({
      name: crypto.randomUUID(),
      indexedDB: new IDBFactory(),
    });
    const database = new MinnowDatabase(store, { maxCommitRetries: RETRIES });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "value", type: "number" },
      ],
    });

    const accepted: number[] = [];
    await Promise.all(
      Array.from({ length: 24 }, (_, index) =>
        database.insertBatch("items", [{ id: index, value: index * 10 }]).then(
          () => accepted.push(index),
          () => undefined,
        ),
      ),
    );
    accepted.sort((left, right) => left - right);

    const rows = (
      await database.query("SELECT id, value FROM items ORDER BY id", { memoize: false })
    ).rows as Array<{ id: number; value: number }>;

    // Exactly the accepted writes are present, each once, each with its own value. This is the
    // guarantee that makes the ceiling survivable: a caller can retry the rejections and know
    // precisely which ones they were.
    expect(rows.map((row) => row.id)).toEqual(accepted);
    for (const row of rows) expect(row.value).toBe(row.id * 10);
  });

  it("takes every write when they are issued one at a time", async () => {
    // The contrast that makes the ceiling a property of concurrency rather than of volume: the
    // same twenty-four writes, awaited in turn, all land.
    const store = await IndexedDbBlockStore.open({
      name: crypto.randomUUID(),
      indexedDB: new IDBFactory(),
    });
    const database = new MinnowDatabase(store, { maxCommitRetries: RETRIES });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "value", type: "number" },
      ],
    });
    for (let index = 0; index < 24; index += 1) {
      await database.insertBatch("items", [{ id: index, value: index }]);
    }
    expect(
      (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false })).rows,
    ).toEqual([{ n: 24 }]);
  });

  it("lets every writer through on one OPFS instance", async () => {
    // The same database-level queue as everywhere else; the OPFS store's own serialized appends
    // are what a retrying writer from another instance relies on, below.
    for (const writers of [16, 64]) {
      const store = await OpfsBlockStore.open({
        name: crypto.randomUUID(),
        root: new MemoryOpfs().root,
      });
      const { accepted, persisted } = await contend(store, writers);
      expect(accepted, `${String(writers)} writers`).toBe(writers);
      expect(persisted, `${String(writers)} writers`).toBe(writers);
    }
  });

  it("loses cleanly across two OPFS instances racing on one directory", async () => {
    // Two store instances over one root are two real tabs: the only arbiter between them is the
    // exclusive handle on the command log's next sequence file. Winners must persist exactly,
    // losers must vanish exactly, and both instances must converge on one database.
    const shim = new MemoryOpfs();
    const name = crypto.randomUUID();
    const firstStore = await OpfsBlockStore.open({ name, root: shim.root });
    const secondStore = await OpfsBlockStore.open({ name, root: shim.root });
    const first = new MinnowDatabase(firstStore, { maxCommitRetries: RETRIES });
    const second = new MinnowDatabase(secondStore, { maxCommitRetries: RETRIES });
    await first.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "value", type: "number" },
      ],
    });

    const accepted: number[] = [];
    await Promise.all(
      Array.from({ length: 24 }, (_, index) =>
        (index % 2 === 0 ? first : second)
          .insertBatch("items", [{ id: index, value: index * 10 }])
          .then(
            () => accepted.push(index),
            () => undefined,
          ),
      ),
    );
    accepted.sort((left, right) => left - right);
    expect(accepted.length).toBeGreaterThan(0);

    for (const database of [first, second]) {
      const rows = (
        await database.query("SELECT id, value FROM items ORDER BY id", { memoize: false })
      ).rows as Array<{ id: number; value: number }>;
      expect(rows.map((row) => row.id)).toEqual(accepted);
      for (const row of rows) expect(row.value).toBe(row.id * 10);
    }
  });
});
