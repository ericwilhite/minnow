import { IDBFactory } from "fake-indexeddb";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { FaultInjectingBlockStore } from "../testing/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";

/**
 * write(): every mutation in the scope publishes as one atomic commit — all of it or none
 * of it, in every tab — across keyed and keyless tables, with AFTER triggers firing per
 * staged operation exactly as they do for standalone writes.
 */

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

/**
 * Runs one compaction through to publication and returns the manifest version it published,
 * asserting it really published one — a compaction with nothing to do would make the tests
 * that use it vacuous rather than failing them.
 */
async function compactToPublication(
  database: MinnowDatabase,
  store: BlockStore,
  tableName: string,
): Promise<number | null> {
  const before = await store.getCurrentManifestVersion();
  let progress = await database.compactTableStep(tableName, { maxBlocks: 8 });
  while (progress.result === null) {
    progress = await database.compactTableStep(tableName, { maxBlocks: 8 });
  }
  const after = await store.getCurrentManifestVersion();
  expect(after).not.toEqual(before);
  return after;
}

async function bank(store: BlockStore): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
  await database.createTable({
    name: "accounts",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "balance", type: "number" },
    ],
  });
  await database.insertBatch("accounts", { columns: { id: [1, 2], balance: [500, 100] } });
  return database;
}

/** Refuses the exact bug this suite guards: pending bytes are not ordinary store-visible data. */
class PendingOverlayReadGuardStore extends MemoryBlockStore {
  readonly pendingBlockIds = new Set<string>();
  pendingSingleReads = 0;
  committedBulkReadCalls = 0;
  directPendingBatchReads = 0;

  override async stageTransactionArtifacts(
    input: Parameters<MemoryBlockStore["stageTransactionArtifacts"]>[0],
  ) {
    for (const block of input.blocks) this.pendingBlockIds.add(block.id);
    return super.stageTransactionArtifacts(input);
  }

  override async getBlock(id: string): Promise<Uint8Array | undefined> {
    if (this.pendingBlockIds.has(id)) this.pendingSingleReads += 1;
    return super.getBlock(id);
  }

  override async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    if (ids.some((id) => this.pendingBlockIds.has(id))) {
      this.directPendingBatchReads += 1;
      throw new Error("Pending block bypassed the transaction overlay");
    }
    this.committedBulkReadCalls += 1;
    return super.getBlocks(ids);
  }
}

describe("atomic write scopes", () => {
  it("routes pruned headers and projected staged blocks through the transaction overlay", async () => {
    const store = new PendingOverlayReadGuardStore();
    const database = await bank(store);
    await database.write(async (tx) => {
      await tx.insertBatch("accounts", { columns: { id: [30], balance: [11] } });
      // A second stage flushes the first bounded batch to the transaction journal. That makes
      // its bytes durable-but-uncommitted and therefore able to expose a bulk-read bypass.
      await tx.insertBatch("accounts", { columns: { id: [31], balance: [12] } });
      // The key predicate takes the zone-map materializer: it first inspects the staged key
      // header, then decodes the staged projected value. Neither byte exists in the committed
      // manifest yet, so both reads must go through transaction.getBlock rather than store bulk.
      expect((await tx.query("SELECT balance FROM accounts WHERE id = 30")).rows).toEqual([
        { balance: 11 },
      ]);
      expect(store.pendingSingleReads).toBeGreaterThan(0);
      expect(store.committedBulkReadCalls).toBeGreaterThan(0);
      expect(store.directPendingBatchReads).toBe(0);
    });
    await database.close();
  });

  for (const implementation of implementations) {
    it(`${implementation.name} publishes a multi-table transfer as one commit`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.createTable({
        name: "transfers",
        columns: [
          { name: "from_id", type: "number" },
          { name: "to_id", type: "number" },
          { name: "amount", type: "number" },
        ],
      });
      const before = await database.query("SELECT id, balance FROM accounts ORDER BY id");
      const { version } = await database.write(async (tx) => {
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [400] } });
        await tx.updateBatch("accounts", { keys: [2], changes: { balance: [200] } });
        await tx.insertBatch("transfers", {
          columns: { from_id: [1], to_id: [2], amount: [100] },
        });
      });
      expect(version).not.toBeNull();
      // After: everything moved together.
      expect((await database.query("SELECT id, balance FROM accounts ORDER BY id")).rows).toEqual([
        { id: 1, balance: 400 },
        { id: 2, balance: 200 },
      ]);
      expect((await database.query("SELECT COUNT(*) AS n FROM transfers")).rows).toEqual([
        { n: 1 },
      ]);
      // One version earlier: none of it happened.
      const previous = (version ?? 1) - 1;
      expect(
        (
          await database.query("SELECT id, balance FROM accounts ORDER BY id", {
            version: previous,
          })
        ).rows,
      ).toEqual(before.rows);
      expect(
        (await database.query("SELECT COUNT(*) AS n FROM transfers", { version: previous })).rows,
      ).toEqual([{ n: 0 }]);
    });

    it(`${implementation.name} publishes nothing when the scope fails mid-way`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await expect(
        database.write(async (tx) => {
          await tx.updateBatch("accounts", { keys: [1], changes: { balance: [400] } });
          // The second update targets a missing key: the whole scope must vanish.
          await tx.updateBatch("accounts", { keys: [999], changes: { balance: [1] } });
        }),
      ).rejects.toThrow();
      expect((await database.query("SELECT id, balance FROM accounts ORDER BY id")).rows).toEqual([
        { id: 1, balance: 500 },
        { id: 2, balance: 100 },
      ]);
      // An error thrown by the callback itself also aborts everything staged so far.
      await expect(
        database.write(async (tx) => {
          await tx.deleteBatch("accounts", { keys: [2] });
          throw new Error("caller changed its mind");
        }),
      ).rejects.toThrow("caller changed its mind");
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 2 }]);
    });

    it(`${implementation.name} keeps partial conflict updates inside the scope`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
      await database.createTable({
        name: "items",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "score", type: "number" },
          { name: "note", type: "string", nullable: true },
        ],
      });
      await database.insertBatch("items", [{ id: 1, score: 1, note: "kept" }]);

      await expect(
        database.write(async (tx) => {
          const result = await tx.execute(
            "INSERT INTO items (id, score, note) VALUES (?, ?, ?) " +
              "ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score",
            [1, 9, "ignored"],
          );
          expect(result).toMatchObject({ kind: "insert", rowCount: 1 });
          expect(result).not.toHaveProperty("version");
          expect((await tx.query("SELECT score, note FROM items WHERE id = 1")).rows).toEqual([
            { score: 9, note: "kept" },
          ]);
          throw new Error("roll back the scope");
        }),
      ).rejects.toThrow("roll back the scope");

      expect((await database.query("SELECT score, note FROM items WHERE id = 1")).rows).toEqual([
        { score: 1, note: "kept" },
      ]);
    });

    it(`${implementation.name} lets INSERT SELECT read rows staged earlier in the scope`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
      for (const name of ["source_rows", "copied_rows"]) {
        await database.createTable({
          name,
          uniqueKey: "id",
          columns: [
            { name: "id", type: "number" },
            { name: "value", type: "string" },
          ],
        });
      }

      const { version } = await database.write(async (tx) => {
        await tx.execute("INSERT INTO source_rows (id, value) VALUES (?, ?)", [1, "staged"]);
        const copied = await tx.execute(
          "INSERT INTO copied_rows (id, value) SELECT id, value FROM source_rows",
        );
        expect(copied).toMatchObject({ kind: "insert", rowCount: 1 });
        expect((await tx.query("SELECT * FROM copied_rows")).rows).toEqual([
          { id: 1, value: "staged" },
        ]);
      });

      expect(version).not.toBeNull();
      expect((await database.query("SELECT * FROM copied_rows")).rows).toEqual([
        { id: 1, value: "staged" },
      ]);
    });

    it(`${implementation.name} applies DO NOTHING to a key staged earlier in the scope`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.write(async (tx) => {
        await tx.execute("INSERT INTO accounts (id, balance) VALUES (?, ?)", [3, 30]);
        const ignored = await tx.execute(
          "INSERT INTO accounts (id, balance) VALUES (?, ?) ON CONFLICT (id) DO NOTHING",
          [3, 99],
        );
        expect(ignored).toMatchObject({ kind: "insert", rowCount: 0 });
      });
      expect((await database.query("SELECT balance FROM accounts WHERE id = 3")).rows).toEqual([
        { balance: 30 },
      ]);
    });

    it(`${implementation.name} enforces unique keys across and inside scopes`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      // In-scope conflict: inserting one key twice in the same scope.
      await expect(
        database.write(async (tx) => {
          await tx.insertBatch("accounts", { columns: { id: [7], balance: [1] } });
          await tx.insertBatch("accounts", { columns: { id: [7], balance: [2] } });
        }),
      ).rejects.toThrow();
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 2 }]);
      // Insert + delete of independent keys across two keyed tables in one commit.
      await database.createTable({
        name: "labels",
        uniqueKey: "name",
        columns: [
          { name: "name", type: "string" },
          { name: "note", type: "string" },
        ],
      });
      await database.insertBatch("labels", { columns: { name: ["old"], note: ["x"] } });
      const { version } = await database.write(async (tx) => {
        await tx.insertBatch("accounts", { columns: { id: [3], balance: [50] } });
        await tx.insertBatch("labels", { columns: { name: ["fresh"], note: ["y"] } });
        await tx.deleteBatch("labels", { keys: ["old"] });
      });
      expect(version).not.toBeNull();
      expect((await database.query("SELECT name FROM labels")).rows).toEqual([{ name: "fresh" }]);
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 3 }]);
      // Key membership survived the multi-entry commit: re-inserting fresh conflicts,
      // re-inserting old succeeds.
      await expect(
        database.insertBatch("labels", { columns: { name: ["fresh"], note: ["z"] } }),
      ).rejects.toThrow();
      await database.insertBatch("labels", { columns: { name: ["old"], note: ["z"] } });
    });

    it(`${implementation.name} refuses to commit after a caught partial-stage failure`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.createTable({
        name: "audit",
        columns: [{ name: "label", type: "string" }],
      });
      // The body writes the numeric balance into a string column: the insert registers its
      // unique key first, then fails while staging the derivation — partial work landed.
      await database.execute(
        "CREATE TRIGGER bad_audit AFTER INSERT ON accounts BEGIN " +
          "INSERT INTO audit (label) VALUES (NEW.balance); END",
      );
      await expect(
        database.write(async (tx) => {
          try {
            await tx.insertBatch("accounts", { columns: { id: [7], balance: [1] } });
          } catch {
            // Swallowed on purpose: the partial registration cannot be undone in place, so
            // the scope must refuse to publish rather than commit the fragment.
          }
          await expect(
            tx.insertBatch("accounts", { columns: { id: [8], balance: [1] } }),
          ).rejects.toThrow("can only roll back");
        }),
      ).rejects.toThrow("failed mid-stage");
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 2 }]);
      // No phantom key membership survived: the aborted scope's key is still insertable.
      await database.execute("DROP TRIGGER bad_audit");
      await database.insertBatch("accounts", { columns: { id: [7], balance: [1] } });
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 3 }]);
    });

    it(`${implementation.name} keeps the scope usable after a clean validation failure`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      const { version } = await database.write(async (tx) => {
        // Nothing was registered before this rejected, so the scope stays usable.
        await expect(
          tx.updateBatch("accounts", { keys: [999], changes: { balance: [1] } }),
        ).rejects.toThrow();
        await tx.insertBatch("accounts", { columns: { id: [7], balance: [1] } });
      });
      expect(version).not.toBeNull();
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 3 }]);
    });

    it(`${implementation.name} accepts multiple insert batches into one full-text table`, async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
      await database.createTable({
        name: "docs",
        columns: [{ name: "body", type: "string" }],
      });
      await database.insertBatch("docs", { columns: { body: ["seed document"] } });
      await database.buildFtsIndex("docs", "body");
      await database.write(async (tx) => {
        await tx.insertBatch("docs", { columns: { body: ["quick brown fox"] } });
        await tx.insertBatch("docs", { columns: { body: ["quick silver stream"] } });
      });
      // Both batches' full-text deltas merged into the one atomic commit.
      expect(
        (await database.query("SELECT COUNT(*) AS n FROM docs WHERE MATCH(body) AGAINST 'quick'"))
          .rows,
      ).toEqual([{ n: 2 }]);
    });

    it(`${implementation.name} fires AFTER triggers inside the scope's single commit`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.createTable({
        name: "audit",
        columns: [
          { name: "account_id", type: "number" },
          { name: "balance", type: "number" },
        ],
      });
      await database.execute(
        "CREATE TRIGGER account_audit AFTER UPDATE ON accounts BEGIN " +
          "INSERT INTO audit (account_id, balance) VALUES (NEW.id, NEW.balance); END",
      );
      const { version } = await database.write(async (tx) => {
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [450] } });
        await tx.updateBatch("accounts", { keys: [2], changes: { balance: [150] } });
      });
      const audit = await database.query(
        "SELECT account_id, balance FROM audit ORDER BY account_id",
      );
      expect(audit.rows).toEqual([
        { account_id: 1, balance: 450 },
        { account_id: 2, balance: 150 },
      ]);
      // The audit rows share the scope's version: nothing at version - 1.
      expect(
        (
          await database.query("SELECT COUNT(*) AS n FROM audit", {
            version: (version ?? 1) - 1,
          })
        ).rows,
      ).toEqual([{ n: 0 }]);
    });
  }

  for (const implementation of implementations) {
    it(`${implementation.name} reads its own staged writes inside the scope`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.write(async (tx) => {
        // Outside the scope: committed state only. Inside: staged rows overlay it.
        await tx.insertBatch("accounts", { columns: { id: [3], balance: [50] } });
        expect((await tx.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 3 }]);
        expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([
          { n: 2 },
        ]);
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [400] } });
        expect((await tx.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
          { balance: 400 },
        ]);
        await tx.deleteBatch("accounts", { keys: [2] });
        expect((await tx.query("SELECT id, balance FROM accounts ORDER BY id")).rows).toEqual([
          { id: 1, balance: 400 },
          { id: 3, balance: 50 },
        ]);
        // Aggregates and params run through the same doctored read.
        expect(
          (
            await tx.query("SELECT SUM(balance) AS total FROM accounts WHERE balance >= ?", {
              params: [50],
            })
          ).rows,
        ).toEqual([{ total: 450 }]);
      });
      // After commit, the outside view converges on what the scope saw.
      expect((await database.query("SELECT id, balance FROM accounts ORDER BY id")).rows).toEqual([
        { id: 1, balance: 400 },
        { id: 3, balance: 50 },
      ]);
    });

    it(`${implementation.name} updates and deletes rows staged in the same scope`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      await database.write(async (tx) => {
        await tx.insertBatch("accounts", { columns: { id: [10], balance: [5] } });
        // The staged key is updatable in-scope even though it is not committed yet.
        await tx.updateBatch("accounts", { keys: [10], changes: { balance: [7] } });
        expect((await tx.query("SELECT balance FROM accounts WHERE id = 10")).rows).toEqual([
          { balance: 7 },
        ]);
        // A key deleted in-scope is gone for later statements in the same scope.
        await tx.deleteBatch("accounts", { keys: [10] });
        await expect(
          tx.updateBatch("accounts", { keys: [10], changes: { balance: [9] } }),
        ).rejects.toThrow();
      });
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 2 }]);
    });
  }

  it("gives trigger pre-images the scope's view of staged rows", async () => {
    const database = await bank(new MemoryBlockStore());
    await database.createTable({
      name: "audit",
      columns: [
        { name: "account_id", type: "number" },
        { name: "old_balance", type: "number" },
      ],
    });
    await database.execute(
      "CREATE TRIGGER account_update_audit AFTER UPDATE ON accounts BEGIN " +
        "INSERT INTO audit (account_id, old_balance) VALUES (OLD.id, OLD.balance); END",
    );
    await database.write(async (tx) => {
      await tx.insertBatch("accounts", { columns: { id: [30], balance: [11] } });
      // OLD for the staged row must read the staged value, not "missing".
      await tx.updateBatch("accounts", { keys: [30], changes: { balance: [12] } });
    });
    expect((await database.query("SELECT account_id, old_balance FROM audit")).rows).toEqual([
      { account_id: 30, old_balance: 11 },
    ]);
  });

  it("surfaces concurrent commits as explicit conflicts; a retry succeeds", async () => {
    const store = new MemoryBlockStore();
    const database = await bank(store);
    const other = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    await expect(
      database.write(async (tx) => {
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [401] } });
        // A commit from another instance lands while the scope is open.
        await other.insertBatch("accounts", { columns: { id: [50], balance: [1] } });
      }),
    ).rejects.toThrow();
    // The losing scope published nothing; the interloper's row is there.
    expect((await database.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
      { balance: 500 },
    ]);
    expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([{ n: 3 }]);
    // Retrying the scope succeeds against the new state.
    const { version } = await database.write(async (tx) => {
      await tx.updateBatch("accounts", { keys: [1], changes: { balance: [401] } });
    });
    expect(version).not.toBeNull();
    expect((await database.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
      { balance: 401 },
    ]);
  });

  /**
   * Background compaction publishes a manifest of its own, and with `autoCompact` on (the
   * default) a read inside the scope can trigger one. That commit changes no row, so it must
   * not cost the scope its version CAS: the scope rebases over it and commits. The previous
   * behaviour surfaced a spurious "Manifest changed" failure under load.
   */
  for (const implementation of implementations) {
    it(`${implementation.name} commits a scope that a compaction lands under`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      // Enough separate segments that compaction has real work to publish.
      for (let id = 100; id < 112; id += 1) {
        await database.insertBatch("accounts", { columns: { id: [id], balance: [id] } });
      }
      const before = await database.query("SELECT COUNT(*) AS n FROM accounts");

      const { version } = await database.write(async (tx) => {
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [401] } });
        // A data-neutral compaction manifest lands while the scope is open, on the very
        // table the scope is writing.
        await compactToPublication(database, store, "accounts");
        await tx.insertBatch("accounts", { columns: { id: [200], balance: [7] } });
      });

      // The scope published, and published everything.
      expect(version).not.toBeNull();
      expect((await database.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
        { balance: 401 },
      ]);
      expect((await database.query("SELECT balance FROM accounts WHERE id = 200")).rows).toEqual([
        { balance: 7 },
      ]);
      // Compaction stayed data-neutral: one new row on top of the pre-scope count.
      const beforeCount = (before.rows[0] as { n: number }).n;
      expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual([
        { n: beforeCount + 1 },
      ]);
    });

    it(`${implementation.name} pins an idle one-stage scope through compaction and collection`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      for (let id = 100; id < 112; id += 1) {
        await database.insertBatch("accounts", { columns: { id: [id], balance: [id] } });
      }
      const sourceVersion = await store.getCurrentManifestVersion();
      if (sourceVersion === null) throw new Error("Expected a source manifest");

      let signalStaged!: () => void;
      const staged = new Promise<void>((resolve) => {
        signalStaged = resolve;
      });
      let resumeScope!: () => void;
      const maintenanceFinished = new Promise<void>((resolve) => {
        resumeScope = resolve;
      });
      const scopedWrite = database.write(async (tx) => {
        // Exactly one bounded stage remains process-local; only the separate reader lease can
        // protect the pre-scope manifest while user code is idle here.
        await tx.insertBatch("accounts", { columns: { id: [30], balance: [11] } });
        signalStaged();
        await maintenanceFinished;
        expect(
          (await tx.query("SELECT id, balance FROM accounts WHERE id IN (1, 30) ORDER BY id")).rows,
        ).toEqual([
          { id: 1, balance: 500 },
          { id: 30, balance: 11 },
        ]);
      });
      await staged;
      expect(
        (await store.listLeases()).some((lease) => lease.manifestVersion === sourceVersion),
      ).toBe(true);

      await compactToPublication(database, store, "accounts");
      const collection = await database.collectGarbage({
        maxItemsPerStep: 1,
        retainRecentVersions: 0,
      });
      expect(collection.retainedManifestCount).toBeGreaterThan(0);
      expect(await store.getManifest(sourceVersion)).toBeDefined();

      resumeScope();
      const { version } = await scopedWrite;
      expect(version).not.toBeNull();
      expect((await database.query("SELECT balance FROM accounts WHERE id = 30")).rows).toEqual([
        { balance: 11 },
      ]);
      await database.close();
    });

    it(`${implementation.name} pins an idle one-stage SQL transaction through compaction and collection`, async () => {
      const store = await implementation.create();
      const database = await bank(store);
      for (let id = 100; id < 112; id += 1) {
        await database.insertBatch("accounts", { columns: { id: [id], balance: [id] } });
      }
      const sourceVersion = await store.getCurrentManifestVersion();
      if (sourceVersion === null) throw new Error("Expected a source manifest");

      await database.execute("BEGIN");
      // This is exactly one bounded stage, so its artifacts remain process-local. The BEGIN
      // lease is the only durable root for the version the statement read and staged against.
      await database.execute("INSERT INTO accounts (id, balance) VALUES (31, 12)");
      expect(
        (await store.listLeases()).some((lease) => lease.manifestVersion === sourceVersion),
      ).toBe(true);

      await compactToPublication(database, store, "accounts");
      const collection = await database.collectGarbage({
        maxItemsPerStep: 1,
        retainRecentVersions: 0,
      });
      expect(collection.retainedManifestCount).toBeGreaterThan(0);
      expect(await store.getManifest(sourceVersion)).toBeDefined();
      expect(
        (await database.query("SELECT id, balance FROM accounts WHERE id IN (1, 31) ORDER BY id"))
          .rows,
      ).toEqual([
        { id: 1, balance: 500 },
        { id: 31, balance: 12 },
      ]);

      await expect(database.execute("COMMIT")).resolves.toMatchObject({
        kind: "transaction",
        action: "commit",
      });
      expect((await database.query("SELECT balance FROM accounts WHERE id = 31")).rows).toEqual([
        { balance: 12 },
      ]);
      await database.close();
    });
  }

  /**
   * The rebase is narrow on purpose: only a manifest that changed nothing is skipped over. A
   * compaction *and* a genuine concurrent write in the same window still costs the scope the
   * race, so the neutral commit cannot be used to smuggle a real conflict past the CAS.
   */
  it("still loses to a concurrent write that a compaction is mixed in with", async () => {
    const store = new MemoryBlockStore();
    const database = await bank(store);
    for (let id = 100; id < 112; id += 1) {
      await database.insertBatch("accounts", { columns: { id: [id], balance: [id] } });
    }
    const other = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    await expect(
      database.write(async (tx) => {
        await tx.updateBatch("accounts", { keys: [1], changes: { balance: [401] } });
        await compactToPublication(database, store, "accounts");
        await other.insertBatch("accounts", { columns: { id: [50], balance: [1] } });
      }),
    ).rejects.toThrow(/Manifest changed/);
    // The losing scope published nothing.
    expect((await database.query("SELECT balance FROM accounts WHERE id = 1")).rows).toEqual([
      { balance: 500 },
    ]);
  });

  /**
   * Crash atomicity: a fault at the commit boundary must leave the durable store either fully
   * before or fully after the scope, across every table the scope touched plus its trigger
   * derivations and unique-key membership — as observed by a brand-new database instance that
   * shares nothing with the one that crashed.
   */
  for (const implementation of implementations) {
    for (const point of ["beforeTransactionCommit", "afterTransactionCommit"] as const) {
      it(`${implementation.name} survives a fault at ${point} as all-or-nothing`, async () => {
        const inner = await implementation.create();
        let armed = false;
        const store = new FaultInjectingBlockStore(inner, (fired) => {
          if (fired === point && armed) {
            armed = false;
            throw new Error("injected crash");
          }
        });
        const database = await bank(store);
        await database.createTable({
          name: "transfers",
          columns: [
            { name: "from_id", type: "number" },
            { name: "amount", type: "number" },
          ],
        });
        await database.createTable({
          name: "audit",
          columns: [{ name: "account_id", type: "number" }],
        });
        await database.execute(
          "CREATE TRIGGER account_audit AFTER UPDATE ON accounts BEGIN " +
            "INSERT INTO audit (account_id) VALUES (NEW.id); END",
        );
        armed = true;
        const outcome = await database
          .write(async (tx) => {
            await tx.updateBatch("accounts", { keys: [1], changes: { balance: [400] } });
            await tx.insertBatch("accounts", { columns: { id: [9], balance: [1] } });
            await tx.insertBatch("transfers", { columns: { from_id: [1], amount: [100] } });
          })
          .then(
            () => "committed" as const,
            () => "failed" as const,
          );
        // The fault fired (it disarms itself) and the outcome is the documented one for this
        // point: a fault before the commit publishes nothing, while one after it loses only
        // the response — the transaction re-reads its persisted record and reports success.
        expect(armed).toBe(false);
        expect(outcome).toBe(point === "beforeTransactionCommit" ? "failed" : "committed");

        // A fresh instance on the same store sees only durable state.
        const reopened = new MinnowDatabase(inner, { rowsPerBlock: 8, compression: "raw" });
        const balances = (await reopened.query("SELECT id, balance FROM accounts ORDER BY id"))
          .rows;
        const transfers = (await reopened.query("SELECT COUNT(*) AS n FROM transfers")).rows;
        const audit = (await reopened.query("SELECT COUNT(*) AS n FROM audit")).rows;

        if (outcome === "committed") {
          // afterTransactionCommit can lose only the response: the commit itself landed.
          expect(balances).toEqual([
            { id: 1, balance: 400 },
            { id: 2, balance: 100 },
            { id: 9, balance: 1 },
          ]);
          expect(transfers).toEqual([{ n: 1 }]);
          expect(audit).toEqual([{ n: 1 }]);
        } else {
          // Nothing from the scope is visible anywhere — no half-applied table, no orphan
          // trigger row, and the scope's key is free for reuse.
          expect(balances).toEqual([
            { id: 1, balance: 500 },
            { id: 2, balance: 100 },
          ]);
          expect(transfers).toEqual([{ n: 0 }]);
          expect(audit).toEqual([{ n: 0 }]);
          await reopened.insertBatch("accounts", { columns: { id: [9], balance: [7] } });
          expect((await reopened.query("SELECT balance FROM accounts WHERE id = 9")).rows).toEqual([
            { balance: 7 },
          ]);
        }
      });
    }
  }

  it("pins scope reads to the pre-scope snapshot", async () => {
    const store = new MemoryBlockStore();
    const database = await bank(store);
    const other = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    let sawInterloper: number | undefined;
    await expect(
      database.write(async (tx) => {
        await tx.insertBatch("accounts", { columns: { id: [60], balance: [1] } });
        // A commit from another instance lands mid-scope. The scope reads its own pinned
        // snapshot plus its staged rows, so the interloper stays invisible; the scope then
        // loses the version race at commit, which is the documented outcome.
        await other.insertBatch("accounts", { columns: { id: [50], balance: [1] } });
        sawInterloper = (
          (await tx.query("SELECT COUNT(*) AS n FROM accounts WHERE id = 50")).rows[0] as {
            n: number;
          }
        ).n;
      }),
    ).rejects.toThrow();
    expect(sawInterloper).toBe(0);
  });

  it("delivers one live notification and one fresh memo per scope", async () => {
    const database = await bank(new MemoryBlockStore());
    const live = database.liveQueries();
    const changes: unknown[] = [];
    await live.subscribe("SELECT SUM(balance) AS total FROM accounts", {
      onChange: (result) => changes.push(result.rows),
    });
    expect(changes).toEqual([[{ total: 600 }]]);
    const sql = "SELECT SUM(balance) AS total FROM accounts";
    expect((await database.query(sql)).rows).toEqual([{ total: 600 }]);
    await database.write(async (tx) => {
      await tx.updateBatch("accounts", { keys: [1], changes: { balance: [450] } });
      await tx.updateBatch("accounts", { keys: [2], changes: { balance: [151] } });
    });
    await live.refresh();
    // One scope, one maintained result, one notification — never an intermediate state.
    expect(changes).toEqual([[{ total: 600 }], [{ total: 601 }]]);
    expect(live.stats.maintained).toBe(1);
    expect(live.stats.reruns).toBe(0);
    // The memo cannot serve the pre-scope answer: the scope moved the epoch.
    expect((await database.query(sql)).rows).toEqual([{ total: 601 }]);
    await database.write(async (tx) => {
      await tx.deleteBatch("accounts", { keys: [2] });
    });
    expect((await database.query(sql)).rows).toEqual([{ total: 450 }]);
    live.close();
  });

  it("rolls a partial conflict update back through SQL BEGIN and ROLLBACK", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "score", type: "number" },
        { name: "note", type: "string", nullable: true },
      ],
    });
    await database.insertBatch("items", [{ id: 1, score: 1, note: "kept" }]);

    await database.execute("BEGIN");
    const updated = await database.execute(
      "INSERT INTO items (id, score, note) VALUES (1, 9, 'ignored') " +
        "ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score",
    );
    expect(updated).not.toHaveProperty("version");
    expect((await database.query("SELECT score FROM items WHERE id = 1")).rows).toEqual([
      { score: 9 },
    ]);
    await database.execute("ROLLBACK");

    expect((await database.query("SELECT score FROM items WHERE id = 1")).rows).toEqual([
      { score: 1 },
    ]);
  });

  it("poisons a caught multi-stage SQL failure instead of committing its first half", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "allowed_required",
      uniqueKey: "value",
      columns: [{ name: "value", type: "string" }],
    });
    await database.insertBatch("allowed_required", [{ value: "kept" }]);
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "required", type: "string" },
        { name: "score", type: "number" },
        { name: "note", type: "string", nullable: true },
      ],
      foreignKeys: [
        {
          name: "items_required_fkey",
          column: "required",
          parentTable: "allowed_required",
          parentColumn: "value",
          onDelete: "restrict",
        },
      ],
    });
    await database.insertBatch("items", [{ id: 1, required: "kept", score: 1, note: null }]);

    await expect(
      database.write(async (tx) => {
        await tx
          .execute(
            "INSERT INTO items (id, required, score, note) VALUES " +
              "(1, 'kept', 9, 'changed'), (2, 'missing-parent', 3, 'new') " +
              "ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score",
          )
          .catch(() => undefined);
      }),
    ).rejects.toThrow("rolled back");

    expect((await database.query("SELECT id, score, note FROM items")).rows).toEqual([
      { id: 1, score: 1, note: null },
    ]);
  });

  it("stages nothing, publishes nothing", async () => {
    const database = await bank(new MemoryBlockStore());
    const versionBefore = (await database.query("SELECT COUNT(*) AS n FROM accounts")).rows;
    const { result, version } = await database.write(async () => "noop");
    expect(result).toBe("noop");
    expect(version).toBe(0);
    expect((await database.query("SELECT COUNT(*) AS n FROM accounts")).rows).toEqual(
      versionBefore,
    );
  });

  it("rejects session use after the scope ends", async () => {
    const database = await bank(new MemoryBlockStore());
    let leaked: Parameters<Parameters<MinnowDatabase["write"]>[0]>[0] | undefined;
    await database.write(async (tx) => {
      leaked = tx;
    });
    await expect(
      leaked?.insertBatch("accounts", { columns: { id: [9], balance: [1] } }),
    ).rejects.toThrow("The write scope has ended");
  });
});
