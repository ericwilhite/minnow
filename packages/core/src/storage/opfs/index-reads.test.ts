/**
 * OPFS index lookups used to take the leader's queue for their whole read, because DROP INDEX,
 * a fold's new base, or a repack may reclaim a base's chunks at any time. Behind a large commit
 * or checkpoint that meant waiting seconds for a lookup that needs milliseconds. They now read
 * off the queue — every chunk read is checksummed, so a reclaimed chunk fails instead of reading
 * wrong — and only a failed read runs again on the queue. These pin both halves.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { heavyTestTimeout } from "../../engine/storage-test-helpers.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { RecordCore } from "../toolkit/record-core.js";
import { OpfsBlockStore } from "./index.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

afterEach(() => {
  vi.restoreAllMocks();
});

/** A table whose index has a base in extent files, then a few rows of delta above it. */
async function indexedTable(store: OpfsBlockStore): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  await db.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "amount", type: "number" },
    ],
  });
  await db.insertBatch(
    "items",
    Array.from({ length: 5_000 }, (_, id) => ({ id, amount: id % 50 })),
  );
  await db.execute("CREATE INDEX items_amount ON items (amount)");
  return db;
}

async function lookup(db: MinnowDatabase, amount: number): Promise<number> {
  const result = await db.query("SELECT COUNT(*) AS n FROM items WHERE amount = ?", {
    params: [amount],
    memoize: false,
  });
  return (result.rows[0] as { n: number }).n;
}

describe("OPFS index lookups", () => {
  it("answer while a large commit holds the leader's queue", async () => {
    const store = await OpfsBlockStore.open({ name: "lookup-queue", root: new MemoryOpfs().root });
    const db = await indexedTable(store);
    expect(await lookup(db, 7)).toBe(100);

    // Hold the next large commit inside its preparation, where it keeps the queue.
    let releasePrepare!: () => void;
    const held = new Promise<void>((resolve) => {
      releasePrepare = resolve;
    });
    let entered!: () => void;
    const preparing = new Promise<void>((resolve) => {
      entered = resolve;
    });
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with the mock receiver below.
    const prepare = RecordCore.prototype.prepareCommit;
    vi.spyOn(RecordCore.prototype, "prepareCommit").mockImplementation(async function (
      this: RecordCore,
      ...args: Parameters<RecordCore["prepareCommit"]>
    ) {
      entered();
      await held;
      return prepare.apply(this, args);
    });
    const writing = db.insertBatch(
      "items",
      Array.from({ length: 20_000 }, (_, index) => ({ id: 10_000 + index, amount: 3 })),
    );
    await preparing;
    const table = await store.getTableByName("items");
    const index = Object.values(table?.secondaryIndexes ?? {})[0];
    if (table === undefined || index === undefined) throw new Error("Expected the index");
    const version = (await store.getCurrentManifestVersion()) ?? -1;
    const answered = await Promise.race([
      store
        .readFtsCandidates(
          table.id,
          index.storageColumnId,
          [{ lower: "", lowerInclusive: true }],
          version,
        )
        .then((read) => read.rowIdsByTerm[0]?.length),
      new Promise<undefined>((resolve) => setTimeout(resolve, 5_000)),
    ]);
    expect(answered).toBe(5_000);
    releasePrepare();
    await writing;
    expect(await lookup(db, 3)).toBe(20_100);
    await db.close();
    store.close();
  });

  it("read again on the queue when a chunk read fails partway", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "lookup-retry", root: shim.root });
    const db = await indexedTable(store);
    await db.close();
    store.close();
    // Reopened, nothing is cached; the first extent read is the index chunk, and it makes no
    // progress.
    const reopened = await OpfsBlockStore.open({ name: "lookup-retry", root: shim.root });
    const table = await reopened.getTableByName("items");
    const index = Object.values(table?.secondaryIndexes ?? {})[0];
    if (table === undefined || index === undefined) throw new Error("Expected the index");
    const version = (await reopened.getCurrentManifestVersion()) ?? -1;
    let refused = 0;
    shim.setTransferLimit((path, operation, requested) => {
      if (operation === "read" && path.includes("/extents/") && refused === 0) {
        refused += 1;
        return 0;
      }
      return requested;
    });
    const read = await reopened.readFtsCandidates(
      table.id,
      index.storageColumnId,
      [{ lower: "", lowerInclusive: true }],
      version,
    );
    expect(refused).toBe(1);
    expect(read.overflow).toBe(false);
    expect(read.rowIdsByTerm[0]).toHaveLength(5_000);
    shim.setTransferLimit(null);
    reopened.close();
  });
});
