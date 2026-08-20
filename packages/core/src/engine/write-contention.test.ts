/**
 * What happens when writes are issued concurrently instead of one after another.
 *
 * Every write commits optimistically: it reads a manifest version, stages its blocks, and
 * publishes only if the manifest has not moved. When it has, the write rebases and tries again,
 * up to `maxCommitRetries` times. Sequential writes never see this. Concurrent ones do, and the
 * behaviour depends on who the writers are.
 *
 * Writers issued through one database do not contend at all: the database runs its simple
 * writes one after another, so each starts from the version the one before it published, and
 * every one of them lands however many are queued — on every store.
 *
 * Writers in different database instances — different tabs, or two instances over one store —
 * do contend, and not in the way "retry a few times" suggests. Each starts from the same
 * manifest version, and every commit that succeeds invalidates the version every other in-flight
 * writer is holding — so one retry is spent per rival that gets there first. After
 * `maxCommitRetries` rivals have won, everyone still waiting is out of budget at once. The
 * ceiling is therefore `maxCommitRetries + 1` writers, *regardless of how many are queued*:
 * sixty-four concurrent instances do not fail more often than sixteen, they simply leave more
 * losers.
 *
 * These tests pin both shapes. The second is not an endorsement — tabs issuing parallel writes
 * to one table will lose most of them past the ceiling, and the durable fix across tabs is to
 * serialize commits rather than let them contend. What the tests guarantee meanwhile is that the
 * losses are *clean*: a rejected write leaves nothing behind, an accepted one is fully present,
 * and the split is deterministic rather than a race.
 */
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
  options: { separateInstances?: boolean } = {},
): Promise<{ accepted: number; persisted: number; reasons: Set<string> }> {
  const database = new MinnowDatabase(store, { maxCommitRetries: RETRIES });
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
          ? new MinnowDatabase(store, { maxCommitRetries: RETRIES })
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

  it("admits exactly maxCommitRetries + 1 instances on IndexedDB, however many are queued", async () => {
    // The number is the point: it does not grow with the queue. A caller who reacts to failures
    // by issuing more parallel writes makes the losses larger, not smaller.
    for (const writers of [16, 32, 64]) {
      const store = await IndexedDbBlockStore.open({
        name: crypto.randomUUID(),
        indexedDB: new IDBFactory(),
      });
      const { accepted, persisted, reasons } = await contend(store, writers, {
        separateInstances: true,
      });
      expect(accepted, `${String(writers)} writers`).toBe(RETRIES + 1);
      expect(persisted, `${String(writers)} writers`).toBe(RETRIES + 1);
      // A conflict, not a corruption or a quota failure -- the losers must be losing for the
      // reason this test claims they are.
      expect([...reasons].join(" | ")).toMatch(/Manifest changed/);
    }
  });

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
